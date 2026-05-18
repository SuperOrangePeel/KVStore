// rdma_client.c
//
// RDMA WRITE sender with token/window protocol.
// O_DIRECT pread -> registered buffer -> RDMA_WRITE.
//
// Usage:
//   ./rdma_client <server_ip> <port> <input_file> [buffer_size]
//
// Examples:
//   ./rdma_client 172.16.135.130 2000 test_2g.dat 4M
//
// Build:
//   gcc -O2 -g -Wall -Wextra -o rdma_client rdma_client.c -lrdmacm -libverbs

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define TOKEN_COUNT 4
#define DEFAULT_BUF_SIZE (8ULL * 1024ULL * 1024ULL)
#define ALIGN_SIZE 4096ULL
#define CQ_CAPACITY 1024

#define WRID_RECV_BASE 1000
#define WRID_RDMA_BASE 2000

enum msg_type {
    MSG_TOKEN = 1,
    MSG_ACK   = 2,
    MSG_DONE  = 3,
};

struct ctrl_msg {
    uint32_t type;
    uint32_t slot_id;
    uint32_t len;
    uint32_t write_len;
    uint32_t last;
    uint32_t cap;
    uint64_t seq;
    uint64_t addr;
    uint32_t rkey;
    uint32_t reserved;
};

struct remote_token {
    uint32_t slot_id;
    uint32_t cap;
    uint64_t addr;
    uint32_t rkey;
};

struct rdma_ctx {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    char *data_buf;
    uint64_t buf_size;
    struct ibv_mr *data_mr;

    struct ctrl_msg send_msgs[TOKEN_COUNT];
    struct ibv_mr *send_mr;

    struct ctrl_msg recv_msgs[TOKEN_COUNT];
    struct ibv_mr *recv_mr;

    struct remote_token tokens[TOKEN_COUNT * 4];
    int token_head;
    int token_tail;
    int token_count;
};

static double now_sec(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static void die(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

static uint64_t align_up_u64(uint64_t x, uint64_t align)
{
    return (x + align - 1) & ~(align - 1);
}

static int parse_size(const char *s, uint64_t *out)
{
    if (!s || !*s || !out) return -1;

    errno = 0;
    char *end = NULL;
    unsigned long long value = strtoull(s, &end, 10);
    if (errno != 0 || end == s || value == 0) return -1;

    uint64_t mul = 1;

    if (*end != '\0') {
        if (end[1] != '\0') return -1;

        switch (*end) {
            case 'k':
            case 'K':
                mul = 1024ULL;
                break;
            case 'm':
            case 'M':
                mul = 1024ULL * 1024ULL;
                break;
            case 'g':
            case 'G':
                mul = 1024ULL * 1024ULL * 1024ULL;
                break;
            default:
                return -1;
        }
    }

    if (value > UINT64_MAX / mul) return -1;

    *out = (uint64_t)value * mul;
    return 0;
}

static uint64_t file_size_of(int fd)
{
    struct stat st;

    if (fstat(fd, &st) != 0) die("fstat");

    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "input file is not regular file\n");
        exit(EXIT_FAILURE);
    }

    if (st.st_size <= 0) {
        fprintf(stderr, "input file is empty\n");
        exit(EXIT_FAILURE);
    }

    return (uint64_t)st.st_size;
}

static ssize_t pread_exact_or_eof(int fd, void *buf, size_t len, uint64_t offset)
{
    char *p = (char *)buf;
    size_t got = 0;

    while (got < len) {
        ssize_t n = pread(fd, p + got, len - got, (off_t)(offset + got));
        if (n > 0) {
            got += (size_t)n;
            continue;
        }
        if (n == 0) break;
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }

    return (ssize_t)got;
}

static void wait_cm_event(struct rdma_event_channel *ec,
                          enum rdma_cm_event_type expected)
{
    struct rdma_cm_event *event = NULL;

    if (rdma_get_cm_event(ec, &event)) die("rdma_get_cm_event");

    if (event->event != expected) {
        fprintf(stderr,
                "unexpected CM event: got %s, expected %s\n",
                rdma_event_str(event->event),
                rdma_event_str(expected));
        rdma_ack_cm_event(event);
        exit(EXIT_FAILURE);
    }

    rdma_ack_cm_event(event);
}

static void push_token(struct rdma_ctx *ctx, const struct ctrl_msg *m)
{
    if (ctx->token_count >= (int)(sizeof(ctx->tokens) / sizeof(ctx->tokens[0]))) {
        fprintf(stderr, "[client] token queue overflow\n");
        exit(EXIT_FAILURE);
    }

    struct remote_token *t = &ctx->tokens[ctx->token_tail];

    t->slot_id = m->slot_id;
    t->cap = m->cap;
    t->addr = m->addr;
    t->rkey = m->rkey;

    ctx->token_tail = (ctx->token_tail + 1) %
                      (int)(sizeof(ctx->tokens) / sizeof(ctx->tokens[0]));
    ctx->token_count++;
}

static bool pop_token(struct rdma_ctx *ctx, struct remote_token *out)
{
    if (ctx->token_count == 0) {
        return false;
    }

    *out = ctx->tokens[ctx->token_head];

    ctx->token_head = (ctx->token_head + 1) %
                      (int)(sizeof(ctx->tokens) / sizeof(ctx->tokens[0]));
    ctx->token_count--;

    return true;
}

static void post_recv(struct rdma_ctx *ctx, int recv_idx)
{
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)&ctx->recv_msgs[recv_idx];
    sge.length = sizeof(struct ctrl_msg);
    sge.lkey = ctx->recv_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_RECV_BASE + (uint64_t)recv_idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;

    if (ibv_post_recv(ctx->qp, &wr, &bad)) {
        die("ibv_post_recv");
    }
}

static void post_all_recvs(struct rdma_ctx *ctx)
{
    for (int i = 0; i < TOKEN_COUNT; i++) {
        memset(&ctx->recv_msgs[i], 0, sizeof(ctx->recv_msgs[i]));
        post_recv(ctx, i);
    }
}

static void post_send_done(struct rdma_ctx *ctx,
                           int send_idx,
                           uint64_t seq,
                           const struct remote_token *token,
                           uint32_t valid_len,
                           uint32_t write_len,
                           uint32_t last)
{
    struct ctrl_msg *m = &ctx->send_msgs[send_idx];
    memset(m, 0, sizeof(*m));

    m->type = MSG_DONE;
    m->slot_id = token->slot_id;
    m->len = valid_len;
    m->write_len = write_len;
    m->last = last;
    m->cap = token->cap;
    m->seq = seq;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)m;
    sge.length = sizeof(*m);
    sge.lkey = ctx->send_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uint64_t)send_idx;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_INLINE;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_send_wr *bad = NULL;

    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        die("ibv_post_send DONE");
    }
}

static void post_rdma_write(struct rdma_ctx *ctx,
                            const struct remote_token *token,
                            uint32_t write_len,
                            uint64_t seq)
{
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)ctx->data_buf;
    sge.length = write_len;
    sge.lkey = ctx->data_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_RDMA_BASE + seq;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    wr.wr.rdma.remote_addr = token->addr;
    wr.wr.rdma.rkey = token->rkey;

    struct ibv_send_wr *bad = NULL;

    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        die("ibv_post_send RDMA_WRITE");
    }
}

static int poll_cq_one(struct rdma_ctx *ctx, struct ibv_wc *wc)
{
    while (true) {
        int n = ibv_poll_cq(ctx->cq, 1, wc);

        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq failed\n");
            return -1;
        }

        if (n == 0) {
            continue;
        }

        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            fprintf(stderr,
                    "WR flushed: opcode=%d wr_id=%" PRIu64
                    ". Peer disconnected or QP entered error.\n",
                    wc->opcode,
                    wc->wr_id);
            return -2;
        }

        if (wc->status != IBV_WC_SUCCESS) {
            fprintf(stderr,
                    "CQE failed: status=%s opcode=%d vendor_err=%u wr_id=%" PRIu64 "\n",
                    ibv_wc_status_str(wc->status),
                    wc->opcode,
                    wc->vendor_err,
                    wc->wr_id);
            return -1;
        }

        return 0;
    }
}

static void process_control_recv(struct rdma_ctx *ctx, const struct ibv_wc *wc)
{
    int recv_idx = (int)(wc->wr_id - WRID_RECV_BASE);
    if (recv_idx < 0 || recv_idx >= TOKEN_COUNT) {
        fprintf(stderr, "[client] invalid recv wr_id=%" PRIu64 "\n", wc->wr_id);
        exit(EXIT_FAILURE);
    }

    struct ctrl_msg msg = ctx->recv_msgs[recv_idx];

    memset(&ctx->recv_msgs[recv_idx], 0, sizeof(ctx->recv_msgs[recv_idx]));
    post_recv(ctx, recv_idx);

    if (msg.type != MSG_TOKEN && msg.type != MSG_ACK) {
        fprintf(stderr, "[client] unexpected control msg type=%u\n", msg.type);
        exit(EXIT_FAILURE);
    }

    if (msg.slot_id >= TOKEN_COUNT || msg.addr == 0 || msg.rkey == 0 || msg.cap == 0) {
        fprintf(stderr, "[client] invalid token/ack msg\n");
        exit(EXIT_FAILURE);
    }

    push_token(ctx, &msg);
}

static void setup_qp_and_memory(struct rdma_ctx *ctx, uint64_t buf_size)
{
    if (buf_size == 0 || buf_size > UINT32_MAX) {
        fprintf(stderr, "invalid buffer size: %" PRIu64 "\n", buf_size);
        exit(EXIT_FAILURE);
    }

    if (buf_size % ALIGN_SIZE != 0) {
        fprintf(stderr, "buffer size must be 4096-byte aligned\n");
        exit(EXIT_FAILURE);
    }

    ctx->buf_size = buf_size;

    ctx->pd = ibv_alloc_pd(ctx->id->verbs);
    if (!ctx->pd) die("ibv_alloc_pd");

    ctx->cq = ibv_create_cq(ctx->id->verbs, CQ_CAPACITY, NULL, NULL, 0);
    if (!ctx->cq) die("ibv_create_cq");

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.send_cq = ctx->cq;
    qp_attr.recv_cq = ctx->cq;
    qp_attr.qp_type = IBV_QPT_RC;

    qp_attr.cap.max_send_wr = CQ_CAPACITY;
    qp_attr.cap.max_recv_wr = CQ_CAPACITY;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    //qp_attr.cap.max_inline_data = 128;

    if (rdma_create_qp(ctx->id, ctx->pd, &qp_attr)) {
        die("rdma_create_qp");
    }

    ctx->qp = ctx->id->qp;

    if (posix_memalign((void **)&ctx->data_buf, ALIGN_SIZE, buf_size) != 0) {
        fprintf(stderr, "posix_memalign data_buf failed\n");
        exit(EXIT_FAILURE);
    }

    memset(ctx->data_buf, 0, buf_size);

    ctx->data_mr = ibv_reg_mr(ctx->pd,
                              ctx->data_buf,
                              buf_size,
                              IBV_ACCESS_LOCAL_WRITE);
    if (!ctx->data_mr) die("ibv_reg_mr data");

    ctx->send_mr = ibv_reg_mr(ctx->pd,
                              ctx->send_msgs,
                              sizeof(ctx->send_msgs),
                              IBV_ACCESS_LOCAL_WRITE);
    if (!ctx->send_mr) die("ibv_reg_mr send_msgs");

    ctx->recv_mr = ibv_reg_mr(ctx->pd,
                              ctx->recv_msgs,
                              sizeof(ctx->recv_msgs),
                              IBV_ACCESS_LOCAL_WRITE);
    if (!ctx->recv_mr) die("ibv_reg_mr recv_msgs");
}

static void cleanup_ctx(struct rdma_ctx *ctx)
{
    if (!ctx) return;

    if (ctx->data_mr) ibv_dereg_mr(ctx->data_mr);
    if (ctx->send_mr) ibv_dereg_mr(ctx->send_mr);
    if (ctx->recv_mr) ibv_dereg_mr(ctx->recv_mr);

    free(ctx->data_buf);

    if (ctx->id && ctx->id->qp) rdma_destroy_qp(ctx->id);
    if (ctx->cq) ibv_destroy_cq(ctx->cq);
    if (ctx->pd) ibv_dealloc_pd(ctx->pd);
}

static void run_client(const char *server_ip,
                       const char *port,
                       const char *input_file,
                       uint64_t buf_size)
{
    int in_fd = open(input_file, O_RDONLY | O_DIRECT);
    if (in_fd < 0) die("open input_file O_DIRECT");

    uint64_t file_size = file_size_of(in_fd);

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *id = NULL;
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct addrinfo hints;
    struct addrinfo *res = NULL;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int ret = getaddrinfo(server_ip, port, &hints, &res);
    if (ret) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(ret));
        exit(EXIT_FAILURE);
    }

    if (rdma_resolve_addr(id, NULL, res->ai_addr, 2000)) die("rdma_resolve_addr");
    wait_cm_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED);

    if (rdma_resolve_route(id, 2000)) die("rdma_resolve_route");
    wait_cm_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED);

    freeaddrinfo(res);

    struct rdma_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.id = id;

    setup_qp_and_memory(&ctx, buf_size);
    post_all_recvs(&ctx);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));

    param.responder_resources = 1;
    param.initiator_depth = 1;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_connect(id, &param)) die("rdma_connect");

    wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED);

    printf("[client] connected to %s:%s\n", server_ip, port);
    printf("[client] input file: %s\n", input_file);
    printf("[client] file size: %" PRIu64 " bytes\n", file_size);
    printf("[client] mode: token/window + O_DIRECT pread + RDMA_WRITE\n");
    printf("[client] buffer size: %" PRIu64 " bytes (%.2f MiB)\n",
           buf_size,
           (double)buf_size / (1024.0 * 1024.0));
    printf("[client] token count: %d\n", TOKEN_COUNT);

    uint64_t total_sent = 0;
    uint64_t chunks = 0;
    uint64_t seq = 0;

    double start = now_sec();

    while (total_sent < file_size) {
        struct remote_token token;

        while (!pop_token(&ctx, &token)) {
            struct ibv_wc wc;
            int pr = poll_cq_one(&ctx, &wc);
            if (pr != 0) exit(EXIT_FAILURE);

            if (wc.opcode == IBV_WC_RECV) {
                process_control_recv(&ctx, &wc);
            } else if (wc.opcode == IBV_WC_SEND) {
                continue;
            } else if (wc.opcode == IBV_WC_RDMA_WRITE) {
                continue;
            } else {
                fprintf(stderr, "[client] unexpected opcode=%d\n", wc.opcode);
                exit(EXIT_FAILURE);
            }
        }

        uint64_t remaining = file_size - total_sent;
        uint64_t valid_len_u64 = remaining > buf_size ? buf_size : remaining;

        if (valid_len_u64 > token.cap) {
            valid_len_u64 = token.cap;
        }

        uint64_t write_len_u64 = align_up_u64(valid_len_u64, ALIGN_SIZE);

        if (write_len_u64 > buf_size || write_len_u64 > token.cap) {
            fprintf(stderr, "[client] write_len too large\n");
            exit(EXIT_FAILURE);
        }

        memset(ctx.data_buf, 0, write_len_u64);

        ssize_t nread = pread_exact_or_eof(in_fd,
                                           ctx.data_buf,
                                           (size_t)write_len_u64,
                                           total_sent);
        if (nread < 0) {
            die("pread input_file O_DIRECT");
        }

        if ((uint64_t)nread < valid_len_u64) {
            fprintf(stderr,
                    "[client] short read before EOF: nread=%zd valid=%" PRIu64 "\n",
                    nread,
                    valid_len_u64);
            exit(EXIT_FAILURE);
        }

        uint32_t valid_len = (uint32_t)valid_len_u64;
        uint32_t write_len = (uint32_t)write_len_u64;
        uint32_t last = (total_sent + valid_len >= file_size) ? 1U : 0U;

        post_rdma_write(&ctx, &token, write_len, seq);

        while (true) {
            struct ibv_wc wc;
            int pr = poll_cq_one(&ctx, &wc);
            if (pr != 0) exit(EXIT_FAILURE);

            if (wc.opcode == IBV_WC_RECV) {
                process_control_recv(&ctx, &wc);
                continue;
            }

            if (wc.opcode == IBV_WC_SEND) {
                continue;
            }

            if (wc.opcode == IBV_WC_RDMA_WRITE) {
                break;
            }

            fprintf(stderr, "[client] unexpected opcode=%d\n", wc.opcode);
            exit(EXIT_FAILURE);
        }

        post_send_done(&ctx,
                       (int)(seq % TOKEN_COUNT),
                       seq,
                       &token,
                       valid_len,
                       write_len,
                       last);

        total_sent += valid_len;
        chunks++;
        seq++;

        if ((chunks % 64) == 0 || last) {
            double mib = (double)total_sent / (1024.0 * 1024.0);
            printf("\r[client] sent %.2f MiB", mib);
            fflush(stdout);
        }
    }

    printf("\n[client] sent final chunk\n");

    double end = now_sec();
    double elapsed = end - start;
    if (elapsed <= 0.0) elapsed = 1e-9;

    double mib = (double)total_sent / (1024.0 * 1024.0);
    double mibps = mib / elapsed;

    printf("[client] done\n");
    printf("[client] total bytes: %" PRIu64 "\n", total_sent);
    printf("[client] chunks: %" PRIu64 "\n", chunks);
    printf("[client] elapsed: %.3f sec\n", elapsed);
    printf("[client] throughput: %.2f MiB/s\n", mibps);

    close(in_fd);

    rdma_disconnect(id);

    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(ec, &event) == 0) {
        rdma_ack_cm_event(event);
    }

    cleanup_ctx(&ctx);

    rdma_destroy_id(id);
    rdma_destroy_event_channel(ec);
}

int main(int argc, char **argv)
{
    if (argc != 4 && argc != 5) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <server_ip> <port> <input_file> [buffer_size]\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    uint64_t buf_size = DEFAULT_BUF_SIZE;

    if (argc == 5) {
        if (parse_size(argv[4], &buf_size) != 0) {
            fprintf(stderr, "invalid buffer_size: %s\n", argv[4]);
            return EXIT_FAILURE;
        }
    }

    buf_size = align_up_u64(buf_size, ALIGN_SIZE);

    run_client(argv[1], argv[2], argv[3], buf_size);

    return EXIT_SUCCESS;
}