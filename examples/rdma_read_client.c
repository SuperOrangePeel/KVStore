// rdma_read_client.c
//
// RDMA READ initiator/client.
// Receives remote addr/rkey from server, then repeatedly RDMA_READs from server memory.
//
// Build:
//   gcc -O2 -g -Wall -Wextra -o rdma_read_client rdma_read_client.c -lrdmacm -libverbs
//
// Usage:
//   ./rdma_read_client <server_ip> <port> <total_size> [chunk_size] [depth]
//
// Example:
//   ./rdma_read_client 172.28.217.148 2000 2G 1M 100

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_CHUNK_SIZE (1ULL * 1024ULL * 1024ULL)
#define DEFAULT_DEPTH 100
#define MAX_DEPTH 256
#define ALIGN_SIZE 4096ULL
#define CQ_CAPACITY 4096

#define WRID_RECV_INFO 1000
#define WRID_READ_BASE 2000
#define WRID_SEND_DONE 9000

enum msg_type {
    MSG_INFO = 1,
    MSG_DONE = 2,
};

struct ctrl_msg {
    uint32_t type;
    uint32_t reserved0;
    uint64_t addr;
    uint32_t rkey;
    uint32_t reserved1;
    uint64_t size;
};

struct local_slot {
    char *buf;
    struct ibv_mr *mr;
    bool busy;
    uint64_t seq;
    uint32_t len;
};

struct rdma_ctx {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct local_slot slots[MAX_DEPTH];
    int depth;
    uint64_t chunk_size;

    struct ctrl_msg recv_info;
    struct ibv_mr *recv_mr;

    struct ctrl_msg send_done;
    struct ibv_mr *send_mr;

    uint64_t remote_addr;
    uint32_t remote_rkey;
    uint64_t remote_size;
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

static int poll_cq_one(struct rdma_ctx *ctx, struct ibv_wc *wc)
{
    while (true) {
        int n = ibv_poll_cq(ctx->cq, 1, wc);

        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq failed\n");
            return -1;
        }

        if (n == 0) continue;

        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            fprintf(stderr,
                    "WR flushed: opcode=%d wr_id=%" PRIu64 "\n",
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

static void post_recv_info(struct rdma_ctx *ctx)
{
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)&ctx->recv_info;
    sge.length = sizeof(ctx->recv_info);
    sge.lkey = ctx->recv_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_RECV_INFO;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(ctx->qp, &wr, &bad)) {
        die("ibv_post_recv INFO");
    }
}

static void post_send_done(struct rdma_ctx *ctx)
{
    memset(&ctx->send_done, 0, sizeof(ctx->send_done));
    ctx->send_done.type = MSG_DONE;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)&ctx->send_done;
    sge.length = sizeof(ctx->send_done);
    sge.lkey = ctx->send_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_SEND_DONE;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        die("ibv_post_send DONE");
    }
}

static int find_free_slot(struct rdma_ctx *ctx)
{
    for (int i = 0; i < ctx->depth; i++) {
        if (!ctx->slots[i].busy) return i;
    }
    return -1;
}

static struct local_slot *find_slot_by_seq(struct rdma_ctx *ctx, uint64_t seq)
{
    for (int i = 0; i < ctx->depth; i++) {
        if (ctx->slots[i].busy && ctx->slots[i].seq == seq) {
            return &ctx->slots[i];
        }
    }
    return NULL;
}

static void post_rdma_read(struct rdma_ctx *ctx,
                           struct local_slot *slot)
{
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)slot->buf;
    sge.length = slot->len;
    sge.lkey = slot->mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_READ_BASE + slot->seq;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    /*
     * Important:
     * Always read from remote offset 0 first.
     * This matches ib_read_bw style better and avoids range/wrap bugs.
     */
    wr.wr.rdma.remote_addr = ctx->remote_addr;
    wr.wr.rdma.rkey = ctx->remote_rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        die("ibv_post_send RDMA_READ");
    }
}

static void setup_qp_and_memory(struct rdma_ctx *ctx,
                                uint64_t chunk_size,
                                int depth)
{
    if (depth <= 0 || depth > MAX_DEPTH) {
        fprintf(stderr, "invalid depth=%d, max=%d\n", depth, MAX_DEPTH);
        exit(EXIT_FAILURE);
    }

    if (chunk_size == 0 || chunk_size > UINT32_MAX) {
        fprintf(stderr, "invalid chunk_size=%" PRIu64 "\n", chunk_size);
        exit(EXIT_FAILURE);
    }

    if (chunk_size % ALIGN_SIZE != 0) {
        fprintf(stderr, "chunk_size must be 4096-byte aligned\n");
        exit(EXIT_FAILURE);
    }

    ctx->depth = depth;
    ctx->chunk_size = chunk_size;

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
    qp_attr.cap.max_inline_data = 0;

    if (rdma_create_qp(ctx->id, ctx->pd, &qp_attr)) {
        die("rdma_create_qp");
    }

    ctx->qp = ctx->id->qp;

    for (int i = 0; i < depth; i++) {
        if (posix_memalign((void **)&ctx->slots[i].buf, ALIGN_SIZE, chunk_size) != 0) {
            fprintf(stderr, "posix_memalign slot failed\n");
            exit(EXIT_FAILURE);
        }

        memset(ctx->slots[i].buf, 0, chunk_size);

        ctx->slots[i].mr = ibv_reg_mr(ctx->pd,
                                      ctx->slots[i].buf,
                                      chunk_size,
                                      IBV_ACCESS_LOCAL_WRITE |
                                          IBV_ACCESS_REMOTE_READ |
                                          IBV_ACCESS_REMOTE_WRITE);
        if (!ctx->slots[i].mr) die("ibv_reg_mr local slot");

        ctx->slots[i].busy = false;
    }

    ctx->recv_mr = ibv_reg_mr(ctx->pd,
                              &ctx->recv_info,
                              sizeof(ctx->recv_info),
                              IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_READ |
                                  IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->recv_mr) die("ibv_reg_mr recv_info");

    ctx->send_mr = ibv_reg_mr(ctx->pd,
                              &ctx->send_done,
                              sizeof(ctx->send_done),
                              IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_READ |
                                  IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->send_mr) die("ibv_reg_mr send_done");
}

static void cleanup_ctx(struct rdma_ctx *ctx)
{
    if (!ctx) return;

    for (int i = 0; i < ctx->depth; i++) {
        if (ctx->slots[i].mr) ibv_dereg_mr(ctx->slots[i].mr);
        free(ctx->slots[i].buf);
    }

    if (ctx->recv_mr) ibv_dereg_mr(ctx->recv_mr);
    if (ctx->send_mr) ibv_dereg_mr(ctx->send_mr);

    if (ctx->id && ctx->id->qp) rdma_destroy_qp(ctx->id);
    if (ctx->cq) ibv_destroy_cq(ctx->cq);
    if (ctx->pd) ibv_dealloc_pd(ctx->pd);
}

static bool try_submit_one_read(struct rdma_ctx *ctx,
                                uint64_t total_size,
                                uint64_t *submitted_bytes,
                                uint64_t *seq)
{
    if (*submitted_bytes >= total_size) return false;

    int sid = find_free_slot(ctx);
    if (sid < 0) return false;

    struct local_slot *slot = &ctx->slots[sid];

    uint64_t remaining = total_size - *submitted_bytes;
    uint64_t len = remaining > ctx->chunk_size ? ctx->chunk_size : remaining;
    len = align_up_u64(len, ALIGN_SIZE);

    if (len > ctx->chunk_size || len > ctx->remote_size) {
        fprintf(stderr,
                "[client] invalid read len=%" PRIu64
                " chunk=%" PRIu64 " remote_size=%" PRIu64 "\n",
                len,
                ctx->chunk_size,
                ctx->remote_size);
        exit(EXIT_FAILURE);
    }

    slot->busy = true;
    slot->seq = *seq;
    slot->len = (uint32_t)len;

    post_rdma_read(ctx, slot);

    *submitted_bytes += len;
    (*seq)++;

    return true;
}

static void run_client(const char *server_ip,
                       const char *port,
                       uint64_t total_size,
                       uint64_t chunk_size,
                       int depth)
{
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

    setup_qp_and_memory(&ctx, chunk_size, depth);
    post_recv_info(&ctx);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));

    param.responder_resources = 128;
    param.initiator_depth = 128;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_connect(id, &param)) die("rdma_connect");
    wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED);

    printf("[client] connected to %s:%s\n", server_ip, port);

    while (true) {
        struct ibv_wc wc;
        int pr = poll_cq_one(&ctx, &wc);
        if (pr != 0) exit(EXIT_FAILURE);

        if (wc.opcode == IBV_WC_RECV && wc.wr_id == WRID_RECV_INFO) {
            if (ctx.recv_info.type != MSG_INFO) {
                fprintf(stderr, "[client] expected MSG_INFO, got %u\n", ctx.recv_info.type);
                exit(EXIT_FAILURE);
            }

            ctx.remote_addr = ctx.recv_info.addr;
            ctx.remote_rkey = ctx.recv_info.rkey;
            ctx.remote_size = ctx.recv_info.size;
            break;
        }

        fprintf(stderr,
                "[client] unexpected CQE before INFO: opcode=%d wr_id=%" PRIu64 "\n",
                wc.opcode,
                wc.wr_id);
        exit(EXIT_FAILURE);
    }

    printf("[client] remote addr=0x%" PRIx64 " rkey=0x%x size=%" PRIu64 " bytes\n",
           ctx.remote_addr,
           ctx.remote_rkey,
           ctx.remote_size);

    if (chunk_size > ctx.remote_size) {
        fprintf(stderr,
                "[client] chunk_size %" PRIu64 " > remote_size %" PRIu64 "\n",
                chunk_size,
                ctx.remote_size);
        exit(EXIT_FAILURE);
    }

    printf("[client] mode: RDMA_READ memory benchmark\n");
    printf("[client] total size: %" PRIu64 " bytes (%.2f MiB)\n",
           total_size,
           (double)total_size / (1024.0 * 1024.0));
    printf("[client] chunk size: %" PRIu64 " bytes (%.2f MiB)\n",
           chunk_size,
           (double)chunk_size / (1024.0 * 1024.0));
    printf("[client] depth: %d\n", depth);

    uint64_t submitted_bytes = 0;
    uint64_t completed_bytes = 0;
    uint64_t seq = 0;
    uint64_t completed_reads = 0;

    double start = now_sec();

    while (completed_bytes < total_size) {
        while (try_submit_one_read(&ctx, total_size, &submitted_bytes, &seq)) {
            /* fill read pipeline */
        }

        struct ibv_wc wc;
        int pr = poll_cq_one(&ctx, &wc);
        if (pr != 0) exit(EXIT_FAILURE);

        if (wc.opcode == IBV_WC_RDMA_READ) {
            if (wc.wr_id < WRID_READ_BASE) {
                fprintf(stderr, "[client] invalid READ wr_id=%" PRIu64 "\n", wc.wr_id);
                exit(EXIT_FAILURE);
            }

            uint64_t done_seq = wc.wr_id - WRID_READ_BASE;
            struct local_slot *slot = find_slot_by_seq(&ctx, done_seq);
            if (!slot) {
                fprintf(stderr, "[client] READ completion for unknown seq=%" PRIu64 "\n", done_seq);
                exit(EXIT_FAILURE);
            }

            completed_bytes += slot->len;
            completed_reads++;

            slot->busy = false;
            slot->seq = 0;
            slot->len = 0;

            continue;
        }

        fprintf(stderr,
                "[client] unexpected CQE during READ: opcode=%d wr_id=%" PRIu64 "\n",
                wc.opcode,
                wc.wr_id);
        exit(EXIT_FAILURE);
    }

    double end = now_sec();
    double elapsed = end - start;
    if (elapsed <= 0.0) elapsed = 1e-9;

    double mib = (double)completed_bytes / (1024.0 * 1024.0);
    double mibps = mib / elapsed;

    printf("[client] done\n");
    printf("[client] completed bytes: %" PRIu64 "\n", completed_bytes);
    printf("[client] read ops: %" PRIu64 "\n", completed_reads);
    printf("[client] elapsed: %.3f sec\n", elapsed);
    printf("[client] throughput: %.2f MiB/s\n", mibps);

    post_send_done(&ctx);

    while (true) {
        struct ibv_wc wc;
        int pr = poll_cq_one(&ctx, &wc);
        if (pr != 0) break;

        if (wc.opcode == IBV_WC_SEND && wc.wr_id == WRID_SEND_DONE) {
            break;
        }
    }

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
    if (argc != 4 && argc != 5 && argc != 6) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <server_ip> <port> <total_size> [chunk_size] [depth]\n"
                "Example:\n"
                "  %s 172.28.217.148 2000 2G 1M 100\n",
                argv[0],
                argv[0]);
        return EXIT_FAILURE;
    }

    uint64_t total_size = 0;
    if (parse_size(argv[3], &total_size) != 0) {
        fprintf(stderr, "invalid total_size: %s\n", argv[3]);
        return EXIT_FAILURE;
    }

    uint64_t chunk_size = DEFAULT_CHUNK_SIZE;
    if (argc >= 5) {
        if (parse_size(argv[4], &chunk_size) != 0) {
            fprintf(stderr, "invalid chunk_size: %s\n", argv[4]);
            return EXIT_FAILURE;
        }
    }

    int depth = DEFAULT_DEPTH;
    if (argc >= 6) {
        depth = atoi(argv[5]);
        if (depth <= 0 || depth > MAX_DEPTH) {
            fprintf(stderr, "invalid depth: %s, valid range: 1-%d\n", argv[5], MAX_DEPTH);
            return EXIT_FAILURE;
        }
    }

    total_size = align_up_u64(total_size, ALIGN_SIZE);
    chunk_size = align_up_u64(chunk_size, ALIGN_SIZE);

    run_client(argv[1], argv[2], total_size, chunk_size, depth);

    return EXIT_SUCCESS;
}