// rdma_read_server.c
//
// RDMA READ target/server.
// Client reads from this server's registered memory.
//
// Build:
//   gcc -O2 -g -Wall -Wextra -o rdma_read_server rdma_read_server.c -lrdmacm -libverbs
//
// Usage:
//   ./rdma_read_server <listen_ip> <port> [buffer_size]
//
// Example:
//   ./rdma_read_server 172.28.217.148 2000 8M

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

#define DEFAULT_BUF_SIZE (8ULL * 1024ULL * 1024ULL)
#define ALIGN_SIZE 4096ULL
#define CQ_CAPACITY 1024

#define WRID_SEND_INFO 100
#define WRID_RECV_DONE 200

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

struct rdma_ctx {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    char *buf;
    uint64_t buf_size;
    struct ibv_mr *buf_mr;

    struct ctrl_msg send_msg;
    struct ibv_mr *send_mr;

    struct ctrl_msg recv_msg;
    struct ibv_mr *recv_mr;
};

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

static void post_recv_done(struct rdma_ctx *ctx)
{
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)&ctx->recv_msg;
    sge.length = sizeof(ctx->recv_msg);
    sge.lkey = ctx->recv_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_RECV_DONE;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(ctx->qp, &wr, &bad)) {
        die("ibv_post_recv DONE");
    }
}

static void post_send_info(struct rdma_ctx *ctx)
{
    memset(&ctx->send_msg, 0, sizeof(ctx->send_msg));

    ctx->send_msg.type = MSG_INFO;
    ctx->send_msg.addr = (uint64_t)(uintptr_t)ctx->buf;
    ctx->send_msg.rkey = ctx->buf_mr->rkey;
    ctx->send_msg.size = ctx->buf_size;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)&ctx->send_msg;
    sge.length = sizeof(ctx->send_msg);
    sge.lkey = ctx->send_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = WRID_SEND_INFO;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        die("ibv_post_send INFO");
    }
}

static void setup_qp_and_memory(struct rdma_ctx *ctx, uint64_t buf_size)
{
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
    qp_attr.cap.max_inline_data = 0;

    if (rdma_create_qp(ctx->id, ctx->pd, &qp_attr)) {
        die("rdma_create_qp");
    }

    ctx->qp = ctx->id->qp;

    if (posix_memalign((void **)&ctx->buf, ALIGN_SIZE, buf_size) != 0) {
        fprintf(stderr, "posix_memalign buf failed\n");
        exit(EXIT_FAILURE);
    }

    for (uint64_t i = 0; i < buf_size; i++) {
        ctx->buf[i] = (char)(i & 0xff);
    }

    ctx->buf_mr = ibv_reg_mr(ctx->pd,
                             ctx->buf,
                             buf_size,
                             IBV_ACCESS_LOCAL_WRITE |
                                 IBV_ACCESS_REMOTE_READ |
                                 IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->buf_mr) die("ibv_reg_mr buf");

    ctx->send_mr = ibv_reg_mr(ctx->pd,
                              &ctx->send_msg,
                              sizeof(ctx->send_msg),
                              IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_READ |
                                  IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->send_mr) die("ibv_reg_mr send_msg");

    ctx->recv_mr = ibv_reg_mr(ctx->pd,
                              &ctx->recv_msg,
                              sizeof(ctx->recv_msg),
                              IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_READ |
                                  IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->recv_mr) die("ibv_reg_mr recv_msg");
}

static void cleanup_ctx(struct rdma_ctx *ctx)
{
    if (!ctx) return;

    if (ctx->buf_mr) ibv_dereg_mr(ctx->buf_mr);
    if (ctx->send_mr) ibv_dereg_mr(ctx->send_mr);
    if (ctx->recv_mr) ibv_dereg_mr(ctx->recv_mr);

    free(ctx->buf);

    if (ctx->id && ctx->id->qp) rdma_destroy_qp(ctx->id);
    if (ctx->cq) ibv_destroy_cq(ctx->cq);
    if (ctx->pd) ibv_dealloc_pd(ctx->pd);
}

static void run_server(const char *listen_ip,
                       const char *port,
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
    printf("[server] mode: RDMA_READ target\n");
    printf("[server] buffer size: %" PRIu64 " bytes (%.2f MiB)\n",
           buf_size,
           (double)buf_size / (1024.0 * 1024.0));

    struct rdma_cm_event *event = NULL;
    wait_cm_event(ec, RDMA_CM_EVENT_CONNECT_REQUEST, &event);

    struct rdma_cm_id *conn_id = event->id;
    rdma_ack_cm_event(event);

    struct rdma_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.id = conn_id;

    setup_qp_and_memory(&ctx, buf_size);
    post_recv_done(&ctx);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));

    param.responder_resources = 128;
    param.initiator_depth = 128;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_accept(conn_id, &param)) die("rdma_accept");

    wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, NULL);
    printf("[server] connection established\n");

    post_send_info(&ctx);

    bool sent_info = false;
    bool got_done = false;

    while (!got_done) {
        struct ibv_wc wc;
        int pr = poll_cq_one(&ctx, &wc);
        if (pr != 0) break;

        if (wc.opcode == IBV_WC_SEND && wc.wr_id == WRID_SEND_INFO) {
            sent_info = true;
            printf("[server] sent remote addr/rkey to client\n");
            continue;
        }

        if (wc.opcode == IBV_WC_RECV && wc.wr_id == WRID_RECV_DONE) {
            if (ctx.recv_msg.type == MSG_DONE) {
                got_done = true;
                printf("[server] received DONE from client\n");
                break;
            }
        }

        fprintf(stderr,
                "[server] unexpected CQE opcode=%d wr_id=%" PRIu64 "\n",
                wc.opcode,
                wc.wr_id);
    }

    printf("[server] done, sent_info=%d got_done=%d\n",
           sent_info ? 1 : 0,
           got_done ? 1 : 0);

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
    if (argc != 3 && argc != 4) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <listen_ip> <port> [buffer_size]\n"
                "Example:\n"
                "  %s 172.28.217.148 2000 8M\n",
                argv[0],
                argv[0]);
        return EXIT_FAILURE;
    }

    uint64_t buf_size = DEFAULT_BUF_SIZE;

    if (argc == 4) {
        if (parse_size(argv[3], &buf_size) != 0) {
            fprintf(stderr, "invalid buffer_size: %s\n", argv[3]);
            return EXIT_FAILURE;
        }
    }

    buf_size = align_up_u64(buf_size, ALIGN_SIZE);

    run_server(argv[1], argv[2], buf_size);

    return EXIT_SUCCESS;
}