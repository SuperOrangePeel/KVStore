#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#define RESP_BUF_SIZE 8192
#define RECV_BUF_SIZE 16384

typedef struct bulk_arg_s {
    const void *data;
    int len;
} bulk_arg_t;

static bulk_arg_t bulk_str(const char *s) {
    bulk_arg_t arg;
    arg.data = s;
    arg.len = (int)strlen(s);
    return arg;
}

static bulk_arg_t bulk_bin(const void *data, int len) {
    bulk_arg_t arg;
    arg.data = data;
    arg.len = len;
    return arg;
}

static int append_bytes(char *buf, int cap, int *off, const void *data, int len) {
    if (len < 0 || *off + len > cap) return -1;
    memcpy(buf + *off, data, (size_t)len);
    *off += len;
    return 0;
}

static int append_fmt(char *buf, int cap, int *off, const char *fmt, ...) {
    va_list ap;
    int n;

    if (*off >= cap) return -1;

    va_start(ap, fmt);
    n = vsnprintf(buf + *off, (size_t)(cap - *off), fmt, ap);
    va_end(ap);

    if (n <= 0 || *off + n > cap) return -1;
    *off += n;
    return 0;
}

static int format_resp(char *buf, int cap, int argc, const bulk_arg_t *argv) {
    int off = 0;

    if (append_fmt(buf, cap, &off, "*%d\r\n", argc) != 0) return -1;
    for (int i = 0; i < argc; i++) {
        if (append_fmt(buf, cap, &off, "$%d\r\n", argv[i].len) != 0) return -1;
        if (append_bytes(buf, cap, &off, argv[i].data, argv[i].len) != 0) return -1;
        if (append_bytes(buf, cap, &off, "\r\n", 2) != 0) return -1;
    }

    return off;
}

static int send_all(int sock, const char *buf, int len) {
    int sent = 0;

    while (sent < len) {
        int n = send(sock, buf + sent, (size_t)(len - sent), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += n;
    }

    return 0;
}

static int recv_response(int sock, char *buf, int cap) {
    int off = 0;

    while (off < cap - 1) {
        fd_set rfds;
        struct timeval tv;
        int ready;

        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);
        tv.tv_sec = off == 0 ? 2 : 0;
        tv.tv_usec = off == 0 ? 0 : 200000;

        ready = select(sock + 1, &rfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (ready == 0) break;

        int n = recv(sock, buf + off, (size_t)(cap - 1 - off), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;
        off += n;
    }

    buf[off] = '\0';
    return off;
}

static int connect_server(const char *ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;

    if (sock < 0) {
        perror("socket");
        exit(1);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sock);
        exit(1);
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        exit(1);
    }

    return sock;
}

static void fail_response(const char *case_name, const char *resp) {
    fprintf(stderr, "[FAIL] %s\nresponse:\n%s\n", case_name, resp ? resp : "<null>");
    exit(1);
}

static void expect_exact(const char *case_name, const char *resp, const char *expected) {
    if (strcmp(resp, expected) != 0) fail_response(case_name, resp);
    printf("[PASS] %s\n", case_name);
}

static void expect_contains(const char *case_name, const char *resp, const char *needle) {
    if (strstr(resp, needle) == NULL) fail_response(case_name, resp);
}

static void run_cmd(int sock, const char *case_name, int argc, const bulk_arg_t *argv, char *resp, int resp_cap) {
    char req[RESP_BUF_SIZE];
    int req_len = format_resp(req, (int)sizeof(req), argc, argv);
    int resp_len;

    if (req_len <= 0) {
        fprintf(stderr, "failed to format request: %s\n", case_name);
        exit(1);
    }
    if (send_all(sock, req, req_len) != 0) {
        perror("send");
        exit(1);
    }
    resp_len = recv_response(sock, resp, resp_cap);
    if (resp_len <= 0) {
        fprintf(stderr, "failed to receive response: %s\n", case_name);
        exit(1);
    }
}

static void test_vector(int sock) {
    char resp[RECV_BUF_SIZE];
    float v1001[2] = {1.0f, 0.0f};
    float v2088[2] = {0.8f, 0.2f};
    float v3000[2] = {0.0f, 1.0f};
    float query[2] = {1.0f, 0.0f};
    const char *value1001 = "下面是一个 C 语言红黑树实现……";
    const char *value2088 = "这里是另一个相关回答……";
    const char *value3000 = "这个答案不太相关。";

    bulk_arg_t createv[] = {
        bulk_str("CREATEV"), bulk_str("qa_index"), bulk_str("DIM"), bulk_str("2"),
        bulk_str("METRIC"), bulk_str("COSINE"), bulk_str("INDEX"), bulk_str("FLAT")
    };
    run_cmd(sock, "CREATEV", 8, createv, resp, sizeof(resp));
    expect_exact("CREATEV", resp, "+OK\r\n");

    bulk_arg_t set1001[] = {bulk_str("SET"), bulk_str("qa:1001"), bulk_str(value1001)};
    bulk_arg_t set3000[] = {bulk_str("SET"), bulk_str("qa:3000"), bulk_str(value3000)};
    run_cmd(sock, "SET qa:1001", 3, set1001, resp, sizeof(resp));
    expect_exact("SET qa:1001", resp, "+OK\r\n");
    run_cmd(sock, "SET qa:3000", 3, set3000, resp, sizeof(resp));
    expect_exact("SET qa:3000", resp, "+OK\r\n");

    bulk_arg_t setv1001[] = {bulk_str("SETV"), bulk_str("qa_index"), bulk_str("qa:1001"), bulk_str("FLOAT32"), bulk_bin(v1001, sizeof(v1001))};
    bulk_arg_t setv2088[] = {bulk_str("SETV"), bulk_str("qa_index"), bulk_str("qa:2088"), bulk_str("FLOAT32"), bulk_bin(v2088, sizeof(v2088)), bulk_str("VALUE"), bulk_str(value2088)};
    bulk_arg_t setv3000[] = {bulk_str("SETV"), bulk_str("qa_index"), bulk_str("qa:3000"), bulk_str("FLOAT32"), bulk_bin(v3000, sizeof(v3000))};
    run_cmd(sock, "SETV qa:1001", 5, setv1001, resp, sizeof(resp));
    expect_exact("SETV qa:1001", resp, "+OK\r\n");
    run_cmd(sock, "SETV qa:2088 VALUE", 7, setv2088, resp, sizeof(resp));
    expect_exact("SETV qa:2088 VALUE", resp, "+OK\r\n");
    run_cmd(sock, "SETV qa:3000", 5, setv3000, resp, sizeof(resp));
    expect_exact("SETV qa:3000", resp, "+OK\r\n");

    bulk_arg_t getv[] = {bulk_str("GETV"), bulk_str("qa_index"), bulk_str("FLOAT32"), bulk_bin(query, sizeof(query)), bulk_str("TOPK"), bulk_str("2")};
    run_cmd(sock, "GETV TOPK", 6, getv, resp, sizeof(resp));
    expect_contains("GETV TOPK count", resp, "*2\r\n");
    expect_contains("GETV TOPK first member", resp, "$7\r\nqa:1001\r\n");
    expect_contains("GETV TOPK second member", resp, "$7\r\nqa:2088\r\n");
    printf("[PASS] GETV TOPK\n");

    bulk_arg_t getv_values[] = {bulk_str("GETV"), bulk_str("qa_index"), bulk_str("FLOAT32"), bulk_bin(query, sizeof(query)), bulk_str("TOPK"), bulk_str("2"), bulk_str("WITHVALUES")};
    run_cmd(sock, "GETV WITHVALUES", 7, getv_values, resp, sizeof(resp));
    expect_contains("GETV WITHVALUES outer array", resp, "*2\r\n*3\r\n");
    expect_contains("GETV WITHVALUES qa:1001", resp, "$7\r\nqa:1001\r\n");
    expect_contains("GETV WITHVALUES value1001", resp, value1001);
    expect_contains("GETV WITHVALUES qa:2088", resp, "$7\r\nqa:2088\r\n");
    expect_contains("GETV WITHVALUES value2088", resp, value2088);
    printf("[PASS] GETV WITHVALUES\n");

    bulk_arg_t vinfo[] = {bulk_str("VINFO"), bulk_str("qa_index")};
    run_cmd(sock, "VINFO", 2, vinfo, resp, sizeof(resp));
    expect_contains("VINFO name", resp, "$4\r\nname\r\n$8\r\nqa_index\r\n");
    expect_contains("VINFO active_count", resp, "$12\r\nactive_count\r\n$1\r\n3\r\n");
    printf("[PASS] VINFO\n");

    bulk_arg_t delv[] = {bulk_str("DELV"), bulk_str("qa_index"), bulk_str("qa:1001")};
    run_cmd(sock, "DELV qa:1001", 3, delv, resp, sizeof(resp));
    expect_exact("DELV qa:1001", resp, "+OK\r\n");

    run_cmd(sock, "GETV after DELV", 7, getv_values, resp, sizeof(resp));
    if (strstr(resp, "qa:1001") != NULL) fail_response("GETV after DELV should not contain qa:1001", resp);
    expect_contains("GETV after DELV qa:2088", resp, "$7\r\nqa:2088\r\n");
    printf("[PASS] GETV after DELV\n");
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <ip> <port>\n", argv[0]);
        return 1;
    }

    int sock = connect_server(argv[1], atoi(argv[2]));
    test_vector(sock);
    close(sock);

    printf("test_vector: all tests passed\n");
    return 0;
}
