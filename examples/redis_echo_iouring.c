#define _GNU_SOURCE

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define RING_ENTRIES 8192
#define IN_CAP       (1024 * 1024)
#define OUT_CAP      (4 * 1024 * 1024)

enum req_type {
    REQ_ACCEPT = 1,
    REQ_RECV   = 2,
    REQ_SEND   = 3
};

typedef struct conn {
    int fd;
    size_t in_len;
    size_t out_len;
    size_t out_sent;
    int recv_pending;
    int send_pending;
    char in[IN_CAP];
    char out[OUT_CAP];
} conn_t;

typedef struct req {
    enum req_type type;
    conn_t *conn;
} req_t;

static struct io_uring ring;
static int listen_fd = -1;
static int use_sqpoll = 0;

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_sock_opts(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
}

static int create_listener(const char *ip, int port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) die("socket");

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad ip: %s\n", ip);
        exit(1);
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) die("bind");
    if (listen(fd, backlog) < 0) die("listen");

    return fd;
}

static void submit_if_needed(void) {
    int ret = io_uring_submit(&ring);
    if (ret < 0) {
        if (use_sqpoll) {
            fprintf(stderr, "io_uring_submit sqpoll failed: %s\n", strerror(-ret));
        } else {
            fprintf(stderr, "io_uring_submit failed: %s\n", strerror(-ret));
        }
    }
}

static req_t *new_req(enum req_type type, conn_t *conn) {
    req_t *r = malloc(sizeof(*r));
    if (!r) die("malloc req");
    r->type = type;
    r->conn = conn;
    return r;
}

static int ci_eq(const char *a, size_t alen, const char *b) {
    size_t blen = strlen(b);
    if (alen != blen) return 0;

    for (size_t i = 0; i < alen; i++) {
        if (toupper((unsigned char)a[i]) != toupper((unsigned char)b[i])) return 0;
    }

    return 1;
}

static char *find_crlf(char *p, char *end) {
    for (; p + 1 < end; p++) {
        if (p[0] == '\r' && p[1] == '\n') return p;
    }
    return NULL;
}

static int parse_long_line(char *p, char *end, long *v, char **next) {
    char *crlf = find_crlf(p, end);
    if (!crlf) return 0;

    char tmp[64];
    size_t n = (size_t)(crlf - p);
    if (n >= sizeof(tmp)) return -1;

    memcpy(tmp, p, n);
    tmp[n] = '\0';

    char *e = NULL;
    errno = 0;
    long val = strtol(tmp, &e, 10);
    if (errno || !e || *e != '\0') return -1;

    *v = val;
    *next = crlf + 2;
    return 1;
}

static size_t append_reply(char *out, size_t out_len, size_t out_cap,
                           const char *cmd, size_t cmd_len,
                           const char *arg, size_t arg_len) {
    const char *reply = "+OK\r\n";
    char tmp[128];

    if (ci_eq(cmd, cmd_len, "PING")) {
        reply = "+PONG\r\n";
        size_t n = strlen(reply);
        if (out_len + n > out_cap) return out_len;
        memcpy(out + out_len, reply, n);
        return out_len + n;
    }

    if (ci_eq(cmd, cmd_len, "SET") ||
        ci_eq(cmd, cmd_len, "MSET") ||
        ci_eq(cmd, cmd_len, "DEL") ||
        ci_eq(cmd, cmd_len, "SELECT") ||
        ci_eq(cmd, cmd_len, "AUTH")) {
        reply = "+OK\r\n";
        size_t n = strlen(reply);
        if (out_len + n > out_cap) return out_len;
        memcpy(out + out_len, reply, n);
        return out_len + n;
    }

    if (ci_eq(cmd, cmd_len, "GET")) {
        reply = "$1\r\nx\r\n";
        size_t n = strlen(reply);
        if (out_len + n > out_cap) return out_len;
        memcpy(out + out_len, reply, n);
        return out_len + n;
    }

    if (ci_eq(cmd, cmd_len, "INCR") ||
        ci_eq(cmd, cmd_len, "LPUSH") ||
        ci_eq(cmd, cmd_len, "RPUSH") ||
        ci_eq(cmd, cmd_len, "SADD") ||
        ci_eq(cmd, cmd_len, "HSET") ||
        ci_eq(cmd, cmd_len, "ZADD")) {
        reply = ":1\r\n";
        size_t n = strlen(reply);
        if (out_len + n > out_cap) return out_len;
        memcpy(out + out_len, reply, n);
        return out_len + n;
    }

    if (ci_eq(cmd, cmd_len, "LRANGE") ||
        ci_eq(cmd, cmd_len, "SMEMBERS")) {
        reply = "*0\r\n";
        size_t n = strlen(reply);
        if (out_len + n > out_cap) return out_len;
        memcpy(out + out_len, reply, n);
        return out_len + n;
    }

    if (ci_eq(cmd, cmd_len, "ECHO")) {
        int n = snprintf(tmp, sizeof(tmp), "$%zu\r\n", arg_len);
        if (n <= 0) return out_len;
        if (out_len + (size_t)n + arg_len + 2 > out_cap) return out_len;

        memcpy(out + out_len, tmp, (size_t)n);
        out_len += (size_t)n;

        if (arg_len) {
            memcpy(out + out_len, arg, arg_len);
            out_len += arg_len;
        }

        memcpy(out + out_len, "\r\n", 2);
        return out_len + 2;
    }

    reply = "+OK\r\n";
    size_t n = strlen(reply);
    if (out_len + n > out_cap) return out_len;
    memcpy(out + out_len, reply, n);
    return out_len + n;
}

static int parse_one_request(char *buf, size_t len, size_t *consumed,
                             char **cmd, size_t *cmd_len,
                             char **arg, size_t *arg_len) {
    char *p = buf;
    char *end = buf + len;

    *cmd = NULL;
    *cmd_len = 0;
    *arg = NULL;
    *arg_len = 0;
    *consumed = 0;

    if (len == 0) return 0;

    if (*p != '*') {
        char *crlf = find_crlf(p, end);
        if (!crlf) return 0;

        char *sp = p;
        while (sp < crlf && *sp == ' ') sp++;

        char *cmd_start = sp;
        while (sp < crlf && *sp != ' ') sp++;

        *cmd = cmd_start;
        *cmd_len = (size_t)(sp - cmd_start);

        while (sp < crlf && *sp == ' ') sp++;

        *arg = sp;
        *arg_len = (size_t)(crlf - sp);

        *consumed = (size_t)(crlf + 2 - buf);
        return 1;
    }

    p++;

    long argc = 0;
    int r = parse_long_line(p, end, &argc, &p);
    if (r <= 0) return r;
    if (argc <= 0 || argc > 1024) return -1;

    for (long i = 0; i < argc; i++) {
        if (p >= end) return 0;
        if (*p != '$') return -1;

        p++;

        long bulk_len = 0;
        r = parse_long_line(p, end, &bulk_len, &p);
        if (r <= 0) return r;
        if (bulk_len < 0) return -1;

        if (p + bulk_len + 2 > end) return 0;

        if (i == 0) {
            *cmd = p;
            *cmd_len = (size_t)bulk_len;
        } else if (i == 1) {
            *arg = p;
            *arg_len = (size_t)bulk_len;
        }

        p += bulk_len;

        if (p + 2 > end) return 0;
        if (p[0] != '\r' || p[1] != '\n') return -1;

        p += 2;
    }

    *consumed = (size_t)(p - buf);
    return 1;
}

static void process_input(conn_t *c) {
    size_t off = 0;

    while (off < c->in_len) {
        size_t consumed = 0;
        char *cmd = NULL;
        char *arg = NULL;
        size_t cmd_len = 0;
        size_t arg_len = 0;

        size_t before = c->out_len;

        int r = parse_one_request(c->in + off, c->in_len - off,
                                  &consumed, &cmd, &cmd_len, &arg, &arg_len);

        if (r == 0) break;

        if (r < 0 || consumed == 0 || cmd_len == 0) {
            const char *err = "-ERR protocol error\r\n";
            size_t n = strlen(err);
            if (c->out_len + n <= OUT_CAP) {
                memcpy(c->out + c->out_len, err, n);
                c->out_len += n;
            }
            c->in_len = 0;
            return;
        }

        c->out_len = append_reply(c->out, c->out_len, OUT_CAP,
                                  cmd, cmd_len, arg, arg_len);

        if (c->out_len == before) {
            break;
        }

        off += consumed;
    }

    if (off > 0) {
        if (off < c->in_len) {
            memmove(c->in, c->in + off, c->in_len - off);
        }
        c->in_len -= off;
    }
}

static void close_conn(conn_t *c) {
    if (!c) return;
    close(c->fd);
    free(c);
}

static void post_accept(void) {
    req_t *req = new_req(REQ_ACCEPT, NULL);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) die("io_uring_get_sqe accept");

    io_uring_prep_accept(sqe, listen_fd, NULL, NULL, SOCK_NONBLOCK);
    io_uring_sqe_set_data(sqe, req);

    submit_if_needed();
}

static void post_recv(conn_t *c) {
    if (c->recv_pending) return;

    if (c->in_len == IN_CAP) {
        close_conn(c);
        return;
    }

    req_t *req = new_req(REQ_RECV, c);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) die("io_uring_get_sqe recv");

    io_uring_prep_recv(sqe, c->fd, c->in + c->in_len, IN_CAP - c->in_len, 0);
    io_uring_sqe_set_data(sqe, req);

    c->recv_pending = 1;

    submit_if_needed();
}

static void post_send(conn_t *c) {
    if (c->send_pending) return;
    if (c->out_sent >= c->out_len) return;

    req_t *req = new_req(REQ_SEND, c);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) die("io_uring_get_sqe send");

    io_uring_prep_send(sqe,
                       c->fd,
                       c->out + c->out_sent,
                       c->out_len - c->out_sent,
                       MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, req);

    c->send_pending = 1;

    submit_if_needed();
}

static void handle_accept(int res) {
    post_accept();

    if (res < 0) {
        if (-res == EAGAIN || -res == ECONNABORTED || -res == EINTR) return;
        fprintf(stderr, "accept failed: %s\n", strerror(-res));
        return;
    }

    int cfd = res;
    set_nonblock(cfd);
    set_sock_opts(cfd);

    conn_t *c = calloc(1, sizeof(*c));
    if (!c) {
        close(cfd);
        return;
    }

    c->fd = cfd;
    post_recv(c);
}

static void handle_recv(conn_t *c, int res) {
    c->recv_pending = 0;

    if (res <= 0) {
        close_conn(c);
        return;
    }

    c->in_len += (size_t)res;

    process_input(c);

    if (c->out_len > c->out_sent) {
        post_send(c);
    }

    post_recv(c);
}

static void handle_send(conn_t *c, int res) {
    c->send_pending = 0;

    if (res <= 0) {
        close_conn(c);
        return;
    }

    c->out_sent += (size_t)res;

    if (c->out_sent < c->out_len) {
        post_send(c);
        return;
    }

    c->out_sent = 0;
    c->out_len = 0;

    if (c->in_len > 0) {
        process_input(c);
    }

    if (c->out_len > c->out_sent) {
        post_send(c);
    }
}

static void init_ring(int sqpoll) {
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));

    if (sqpoll) {
        p.flags = IORING_SETUP_SQPOLL;
        p.sq_thread_idle = 2000;
    }

    int ret = io_uring_queue_init_params(RING_ENTRIES, &ring, &p);
    if (ret < 0) {
        fprintf(stderr, "io_uring_queue_init_params failed: %s\n", strerror(-ret));
        exit(1);
    }

    use_sqpoll = sqpoll;

    if (use_sqpoll) {
        printf("[io_uring] SQPOLL enabled\n");
    } else {
        printf("[io_uring] normal submit mode\n");
    }
}

int main(int argc, char **argv) {
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Usage: %s <bind_ip> <port> [sqpoll]\n", argv[0]);
        fprintf(stderr, "Example normal: %s 0.0.0.0 6379\n", argv[0]);
        fprintf(stderr, "Example sqpoll: %s 0.0.0.0 6379 sqpoll\n", argv[0]);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int sqpoll = 0;

    if (argc == 4 && strcmp(argv[3], "sqpoll") == 0) {
        sqpoll = 1;
    }

    listen_fd = create_listener(ip, port, 65535);

    init_ring(sqpoll);

    printf("[io_uring] listening on %s:%d\n", ip, port);

    post_accept();

    while (1) {
        struct io_uring_cqe *cqe = NULL;

        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            if (ret == -EINTR) continue;
            fprintf(stderr, "io_uring_wait_cqe failed: %s\n", strerror(-ret));
            break;
        }

        req_t *req = io_uring_cqe_get_data(cqe);
        int res = cqe->res;

        if (!req) {
            io_uring_cqe_seen(&ring, cqe);
            continue;
        }

        if (req->type == REQ_ACCEPT) {
            handle_accept(res);
        } else if (req->type == REQ_RECV) {
            handle_recv(req->conn, res);
        } else if (req->type == REQ_SEND) {
            handle_send(req->conn, res);
        }

        free(req);
        io_uring_cqe_seen(&ring, cqe);
    }

    io_uring_queue_exit(&ring);
    close(listen_fd);

    return 0;
}