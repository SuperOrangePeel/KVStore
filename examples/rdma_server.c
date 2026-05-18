// rdma_server.c
//
// RDMA WRITE receiver with token/window protocol.
// O_DIRECT write-file mode + discard mode.
//
// Usage:
//   ./rdma_server <listen_ip> <port> <output_file> [buffer_size]
//   ./rdma_server <listen_ip> <port> --discard [buffer_size]
//
// Examples:
//   ./rdma_server 172.16.135.130 2000 recv_2g.dat 4M
//   ./rdma_server 172.16.135.130 2000 --discard 4M
//
// Build:
//   gcc -O2 -g -Wall -Wextra -o rdma_server rdma_server.c -lrdmacm -libverbs

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

enum msg_type {
    MSG_TOKEN = 1, // server -> client, initial token
    MSG_ACK   = 2, // server -> client, buffer reusable
    MSG_DONE  = 3, // client -> server, RDMA_WRITE done
};

struct ctrl_msg {
    uint32_t type;
    uint32_t slot_id;
    uint32_t len;       // valid file bytes
    uint32_t write_len; // aligned bytes RDMA_WRITE'd / O_DIRECT written
    uint32_t last;
    uint32_t cap;       // buffer capacity
    uint64_t seq;
    uint64_t addr;
    uint32_t rkey;
    uint32_t reserved;
};

struct rdma_slot {
    char *buf;
    struct ibv_mr *mr;

    bool pending;
    uint64_t seq;
    uint32_t len;
    uint32_t write_len;
    uint32_t last;
};

struct rdma_ctx {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    uint64_t buf_size;

    struct rdma_slot slots[TOKEN_COUNT];

    struct ctrl_msg send_msgs[TOKEN_COUNT];
    struct ibv_mr *send_mr;

    struct ctrl_msg recv_msgs[TOKEN_COUNT];
    struct ibv_mr *recv_mr;
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

static int pwrite_all(int fd, const void *buf, size_t len, uint64_t offset)
{
    const char *p = (const char *)buf;
    size_t written = 0;

    while (written < len) {
        ssize_t n = pwrite(fd, p + written, len - written, (off_t)(offset + written));
        if (n > 0) {
            written += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }

    return 0;
}

static void wait_cm_event(struct rdma_event_channel *ec,
                          enum rdma_cm_event_type expected,
                          struct rdma_cm_event **out_event)
{
    struct rdma_cm_event *event = NULL;

    if (rdma_get_cm_event(ec, &event)) {
        die("rdma_get_cm_event");
    }

    if (event->event != expected) {
        fprintf(stderr,
                "unexpected CM event: got %s, expected %s\n",
                rdma_event_str(event->event),
                rdma_event_str(expected));
        rdma_ack_cm_event(event);
        exit(EXIT_FAILURE);
    }

    if (out_event) {
        *out_event = event;
    } else {
        rdma_ack_cm_event(event);
    }
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

static void post_send_ctrl(struct rdma_ctx *ctx, int slot_id, uint32_t type)
{
    struct ctrl_msg *m = &ctx->send_msgs[slot_id];
    memset(m, 0, sizeof(*m));

    m->type = type;
    m->slot_id = (uint32_t)slot_id;
    m->len = (uint32_t)ctx->buf_size;
    m->write_len = (uint32_t)ctx->buf_size;
    m->cap = (uint32_t)ctx->buf_size;
    m->addr = (uint64_t)(uintptr_t)ctx->slots[slot_id].buf;
    m->rkey = ctx->slots[slot_id].mr->rkey;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)m;
    sge.length = sizeof(*m);
    sge.lkey = ctx->send_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uint64_t)slot_id;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_INLINE;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_send_wr *bad = NULL;

    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        die("ibv_post_send ctrl");
    }
}

static void send_initial_tokens(struct rdma_ctx *ctx)
{
    for (int i = 0; i < TOKEN_COUNT; i++) {
        post_send_ctrl(ctx, i, MSG_TOKEN);
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

    for (int i = 0; i < TOKEN_COUNT; i++) {
        if (posix_memalign((void **)&ctx->slots[i].buf, ALIGN_SIZE, buf_size) != 0) {
            fprintf(stderr, "posix_memalign slot failed\n");
            exit(EXIT_FAILURE);
        }

        memset(ctx->slots[i].buf, 0, buf_size);

        ctx->slots[i].mr = ibv_reg_mr(ctx->pd,
                                      ctx->slots[i].buf,
                                      buf_size,
                                      IBV_ACCESS_LOCAL_WRITE |
                                          IBV_ACCESS_REMOTE_WRITE);
        if (!ctx->slots[i].mr) {
            die("ibv_reg_mr slot");
        }

        ctx->slots[i].pending = false;
    }

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

    for (int i = 0; i < TOKEN_COUNT; i++) {
        if (ctx->slots[i].mr) ibv_dereg_mr(ctx->slots[i].mr);
        free(ctx->slots[i].buf);
    }

    if (ctx->send_mr) ibv_dereg_mr(ctx->send_mr);
    if (ctx->recv_mr) ibv_dereg_mr(ctx->recv_mr);

    if (ctx->id && ctx->id->qp) rdma_destroy_qp(ctx->id);
    if (ctx->cq) ibv_destroy_cq(ctx->cq);
    if (ctx->pd) ibv_dealloc_pd(ctx->pd);
}

static int find_pending_seq(struct rdma_ctx *ctx, uint64_t seq)
{
    for (int i = 0; i < TOKEN_COUNT; i++) {
        if (ctx->slots[i].pending && ctx->slots[i].seq == seq) {
            return i;
        }
    }

    return -1;
}

static void run_server(const char *listen_ip,
                       const char *port,
                       const char *output_file,
                       bool discard,
                       uint64_t buf_size)
{
    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *listen_id = NULL;
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP)) {
        die("rdma_create_id");
    }

    struct addrinfo hints;
    struct addrinfo *res = NULL;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    int ret = getaddrinfo(listen_ip, port, &hints, &res);
    if (ret) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(ret));
        exit(EXIT_FAILURE);
    }

    if (rdma_bind_addr(listen_id, res->ai_addr)) die("rdma_bind_addr");
    freeaddrinfo(res);

    if (rdma_listen(listen_id, 1)) die("rdma_listen");

    printf("[server] listening on %s:%s\n", listen_ip, port);
    printf("[server] mode: %s\n", discard ? "discard" : "O_DIRECT write file");
    printf("[server] buffer size: %" PRIu64 " bytes (%.2f MiB)\n",
           buf_size,
           (double)buf_size / (1024.0 * 1024.0));
    printf("[server] token count: %d\n", TOKEN_COUNT);

    struct rdma_cm_event *event = NULL;
    wait_cm_event(ec, RDMA_CM_EVENT_CONNECT_REQUEST, &event);

    struct rdma_cm_id *conn_id = event->id;
    rdma_ack_cm_event(event);

    struct rdma_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.id = conn_id;

    setup_qp_and_memory(&ctx, buf_size);
    post_all_recvs(&ctx);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));

    param.responder_resources = 1;
    param.initiator_depth = 1;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_accept(conn_id, &param)) die("rdma_accept");

    wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, NULL);
    printf("[server] connection established\n");

    int out_fd = -1;
    if (!discard) {
        out_fd = open(output_file, O_CREAT | O_TRUNC | O_WRONLY | O_DIRECT, 0644);
        if (out_fd < 0) die("open output_file O_DIRECT");
    }

    send_initial_tokens(&ctx);

    uint64_t next_write_seq = 0;
    uint64_t total_valid_bytes = 0;
    uint64_t total_aligned_bytes = 0;
    uint64_t chunks = 0;
    bool completed = false;
    bool saw_last = false;

    double start = now_sec();

    while (!completed) {
        struct ibv_wc wc;
        int pr = poll_cq_one(&ctx, &wc);
        if (pr == -2) {
            fprintf(stderr, "\n[server] peer disconnected or QP error\n");
            break;
        }
        if (pr != 0) {
            exit(EXIT_FAILURE);
        }

        if (wc.opcode == IBV_WC_SEND) {
            continue;
        }

        if (wc.opcode != IBV_WC_RECV) {
            fprintf(stderr, "[server] unexpected opcode=%d\n", wc.opcode);
            exit(EXIT_FAILURE);
        }

        int recv_idx = (int)(wc.wr_id - WRID_RECV_BASE);
        if (recv_idx < 0 || recv_idx >= TOKEN_COUNT) {
            fprintf(stderr, "[server] invalid recv wr_id=%" PRIu64 "\n", wc.wr_id);
            exit(EXIT_FAILURE);
        }

        struct ctrl_msg msg = ctx.recv_msgs[recv_idx];

        memset(&ctx.recv_msgs[recv_idx], 0, sizeof(ctx.recv_msgs[recv_idx]));
        post_recv(&ctx, recv_idx);

        if (msg.type != MSG_DONE) {
            fprintf(stderr, "[server] unexpected msg type=%u\n", msg.type);
            exit(EXIT_FAILURE);
        }

        if (msg.slot_id >= TOKEN_COUNT) {
            fprintf(stderr, "[server] invalid slot_id=%u\n", msg.slot_id);
            exit(EXIT_FAILURE);
        }

        if (msg.len > ctx.buf_size || msg.write_len > ctx.buf_size) {
            fprintf(stderr,
                    "[server] invalid length: len=%u write_len=%u buf=%" PRIu64 "\n",
                    msg.len,
                    msg.write_len,
                    ctx.buf_size);
            exit(EXIT_FAILURE);
        }

        if (msg.write_len % ALIGN_SIZE != 0) {
            fprintf(stderr, "[server] write_len not aligned: %u\n", msg.write_len);
            exit(EXIT_FAILURE);
        }

        if (msg.len > msg.write_len) {
            fprintf(stderr, "[server] len > write_len\n");
            exit(EXIT_FAILURE);
        }

        struct rdma_slot *slot = &ctx.slots[msg.slot_id];
        if (slot->pending) {
            fprintf(stderr, "[server] slot %u already pending\n", msg.slot_id);
            exit(EXIT_FAILURE);
        }

        slot->pending = true;
        slot->seq = msg.seq;
        slot->len = msg.len;
        slot->write_len = msg.write_len;
        slot->last = msg.last;

        if (msg.last) {
            saw_last = true;
        }

        while (true) {
            int sid = find_pending_seq(&ctx, next_write_seq);
            if (sid < 0) {
                break;
            }

            struct rdma_slot *s = &ctx.slots[sid];

            if (s->len > 0) {
                if (!discard) {
                    if (pwrite_all(out_fd,
                                   s->buf,
                                   s->write_len,
                                   total_aligned_bytes) != 0) {
                        die("pwrite output O_DIRECT");
                    }
                }

                total_valid_bytes += s->len;
                total_aligned_bytes += s->write_len;
                chunks++;

                if ((chunks % 64) == 0 || s->last) {
                    double mib = (double)total_valid_bytes / (1024.0 * 1024.0);
                    printf("\r[server] received %.2f MiB", mib);
                    fflush(stdout);
                }
            }

            uint32_t was_last = s->last;

            s->pending = false;
            s->seq = 0;
            s->len = 0;
            s->write_len = 0;
            s->last = 0;

            post_send_ctrl(&ctx, sid, MSG_ACK);

            next_write_seq++;

            if (was_last) {
                printf("\n[server] received FIN chunk\n");
                completed = true;
                break;
            }
        }

        if (saw_last && completed) {
            break;
        }
    }

    if (!discard && out_fd >= 0) {
        if (ftruncate(out_fd, (off_t)total_valid_bytes) != 0) {
            die("ftruncate output_file");
        }

        if (fsync(out_fd) != 0) {
            die("fsync output_file");
        }

        close(out_fd);
        out_fd = -1;
    }

    double end = now_sec();
    double elapsed = end - start;
    if (elapsed <= 0.0) elapsed = 1e-9;

    double mib = (double)total_valid_bytes / (1024.0 * 1024.0);
    double mibps = mib / elapsed;

    printf("[server] done%s\n", completed ? "" : " (incomplete)");
    printf("[server] valid bytes: %" PRIu64 "\n", total_valid_bytes);
    printf("[server] aligned bytes: %" PRIu64 "\n", total_aligned_bytes);
    printf("[server] chunks: %" PRIu64 "\n", chunks);
    printf("[server] elapsed: %.3f sec\n", elapsed);
    printf("[server] throughput: %.2f MiB/s\n", mibps);

    rdma_disconnect(conn_id);

    struct rdma_cm_event *disc = NULL;
    if (rdma_get_cm_event(ec, &disc) == 0) {
        rdma_ack_cm_event(disc);
    }

    cleanup_ctx(&ctx);

    rdma_destroy_id(conn_id);
    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(ec);
}

int main(int argc, char **argv)
{
    if (argc != 4 && argc != 5) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <listen_ip> <port> <output_file> [buffer_size]\n"
                "  %s <listen_ip> <port> --discard [buffer_size]\n",
                argv[0],
                argv[0]);
        return EXIT_FAILURE;
    }

    const char *listen_ip = argv[1];
    const char *port = argv[2];
    const char *output_file = argv[3];

    bool discard = false;
    if (strcmp(output_file, "--discard") == 0) {
        discard = true;
        output_file = NULL;
    }

    uint64_t buf_size = DEFAULT_BUF_SIZE;

    if (argc == 5) {
        if (parse_size(argv[4], &buf_size) != 0) {
            fprintf(stderr, "invalid buffer_size: %s\n", argv[4]);
            return EXIT_FAILURE;
        }
    }

    buf_size = align_up_u64(buf_size, ALIGN_SIZE);

    run_server(listen_ip, port, output_file, discard, buf_size);

    return EXIT_SUCCESS;
}