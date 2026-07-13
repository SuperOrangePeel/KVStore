#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define RESP_BUF_SIZE 8192
#define RECV_BUF_SIZE 16384

typedef struct bulk_arg_s {
    const void *data;
    int len;
} bulk_arg_t;


typedef struct bench_thread_arg_s {
    const char *ip;
    int port;
    int thread_id;
    int requests;
    int failed;
    char err[256];
} bench_thread_arg_t;

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
        tv.tv_sec = off == 0 ? 180 : 0;
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

static int response_is_error(const char *resp) {
    return resp != NULL && resp[0] == '-';
}


static double now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

static void print_qps_result(const char *name, int n, double total_ms) {
    double total_s = total_ms / 1000.0;
    double avg_ms = n > 0 ? total_ms / (double)n : 0.0;
    double qps = total_ms > 0 ? (double)n * 1000.0 / total_ms : 0.0;
    printf("[BENCH] %s n=%d total=%.3fs avg=%.3fms qps=%.2f\n", name, n, total_s, avg_ms, qps);
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


static void test_qa(int sock) {
    char resp[RECV_BUF_SIZE];
    const char *qa_index = "qa_semantic_index";
    const char *auto_question = "1美元多少美分";
    const char *auto_answer = "1美元等于100美分";
    const char *id_member = "qa:testqa:1";
    const char *id_question = "redis setex 是什么";
    const char *id_answer = "SETEX 用于设置带过期时间的 key。";

    bulk_arg_t setqa_auto[] = {
        bulk_str("SETQA"), bulk_str(qa_index), bulk_str("AUTO"),
        bulk_str("QUESTION"), bulk_str(auto_question),
        bulk_str("ANSWER"), bulk_str(auto_answer)
    };
    run_cmd(sock, "SETQA AUTO", 7, setqa_auto, resp, sizeof(resp));
    if (response_is_error(resp)) {
        printf("[SKIP] SETQA/GETQA embedding service unavailable: %s", resp);
        return;
    }
    expect_contains("SETQA AUTO generated member", resp, "qa:");
    printf("[PASS] SETQA AUTO\n");

    bulk_arg_t setqa_id[] = {
        bulk_str("SETQA"), bulk_str(qa_index), bulk_str("ID"), bulk_str(id_member),
        bulk_str("QUESTION"), bulk_str(id_question),
        bulk_str("ANSWER"), bulk_str(id_answer)
    };
    run_cmd(sock, "SETQA ID", 8, setqa_id, resp, sizeof(resp));
    expect_exact("SETQA ID", resp, "+OK\r\n");

    bulk_arg_t getqa_auto[] = {
        bulk_str("GETQA"), bulk_str(qa_index), bulk_str("QUESTION"), bulk_str(auto_question),
        bulk_str("TOPK"), bulk_str("2")
    };
    run_cmd(sock, "GETQA auto question", 6, getqa_auto, resp, sizeof(resp));
    expect_contains("GETQA auto answer", resp, auto_answer);
    printf("[PASS] GETQA auto question\n");

    bulk_arg_t getqa_id[] = {
        bulk_str("GETQA"), bulk_str(qa_index), bulk_str("QUESTION"), bulk_str(id_question),
        bulk_str("TOPK"), bulk_str("2")
    };
    run_cmd(sock, "GETQA id question", 6, getqa_id, resp, sizeof(resp));
    expect_contains("GETQA id member", resp, id_member);
    expect_contains("GETQA id answer", resp, id_answer);
    printf("[PASS] GETQA id question\n");

    bulk_arg_t delqa[] = {bulk_str("DELQA"), bulk_str(qa_index), bulk_str(id_member)};
    run_cmd(sock, "DELQA qa:testqa:1", 3, delqa, resp, sizeof(resp));
    expect_exact("DELQA qa:testqa:1", resp, "+OK\r\n");

    run_cmd(sock, "GETQA after DELQA", 6, getqa_id, resp, sizeof(resp));
    if (strstr(resp, id_member) != NULL) fail_response("GETQA after DELQA should not contain deleted member", resp);
    printf("[PASS] GETQA after DELQA\n");
}


static void bench_setqa(int sock, int n) {
    char resp[RECV_BUF_SIZE];
    char member[128];
    char question[256];
    char answer[256];
    double start_ms, total_ms;
    const char *qa_index = "qa_bench_index";

    /* Warmup: do not count the first model/runtime hit. */
    bulk_arg_t warmup[] = {
        bulk_str("SETQA"), bulk_str(qa_index), bulk_str("ID"), bulk_str("qa:bench:warmup"),
        bulk_str("QUESTION"), bulk_str("benchmark warmup question"),
        bulk_str("ANSWER"), bulk_str("benchmark warmup answer")
    };
    run_cmd(sock, "BENCH SETQA warmup", 8, warmup, resp, sizeof(resp));
    if (response_is_error(resp)) {
        printf("[SKIP] SETQA QPS embedding service unavailable: %s", resp);
        return;
    }

    start_ms = now_ms();
    for (int i = 0; i < n; i++) {
        snprintf(member, sizeof(member), "qa:bench:setqa:%d", i);
        snprintf(question, sizeof(question), "SETQA benchmark question %d: redis vector semantic cache", i);
        snprintf(answer, sizeof(answer), "SETQA benchmark answer %d", i);
        bulk_arg_t argv[] = {
            bulk_str("SETQA"), bulk_str(qa_index), bulk_str("ID"), bulk_str(member),
            bulk_str("QUESTION"), bulk_str(question),
            bulk_str("ANSWER"), bulk_str(answer)
        };
        run_cmd(sock, "BENCH SETQA", 8, argv, resp, sizeof(resp));
        if (strcmp(resp, "+OK\r\n") != 0) fail_response("BENCH SETQA response", resp);
    }
    total_ms = now_ms() - start_ms;
    print_qps_result("SETQA", n, total_ms);
}

static void bench_getqa(int sock, int n) {
    char resp[RECV_BUF_SIZE];
    char question[256];
    double start_ms, total_ms;
    const char *qa_index = "qa_bench_index";

    /* Warmup GETQA separately because query embedding has its own first-hit cost. */
    bulk_arg_t warmup[] = {
        bulk_str("GETQA"), bulk_str(qa_index), bulk_str("QUESTION"), bulk_str("SETQA benchmark question 0"),
        bulk_str("TOPK"), bulk_str("5")
    };
    run_cmd(sock, "BENCH GETQA warmup", 6, warmup, resp, sizeof(resp));
    if (response_is_error(resp)) {
        printf("[SKIP] GETQA QPS embedding service unavailable: %s", resp);
        return;
    }

    start_ms = now_ms();
    for (int i = 0; i < n; i++) {
        snprintf(question, sizeof(question), "SETQA benchmark question %d", i % 20);
        bulk_arg_t argv[] = {
            bulk_str("GETQA"), bulk_str(qa_index), bulk_str("QUESTION"), bulk_str(question),
            bulk_str("TOPK"), bulk_str("5")
        };
        run_cmd(sock, "BENCH GETQA", 6, argv, resp, sizeof(resp));
        if (response_is_error(resp)) fail_response("BENCH GETQA response", resp);
        expect_contains("BENCH GETQA response array", resp, "*");
    }
    total_ms = now_ms() - start_ms;
    print_qps_result("GETQA", n, total_ms);
}


static void *bench_setqa_worker(void *argp) {
    bench_thread_arg_t *arg = (bench_thread_arg_t *)argp;
    char resp[RECV_BUF_SIZE];
    char member[128];
    char question[256];
    char answer[256];
    const char *qa_index = "qa_batch_index";
    int sock = connect_server(arg->ip, arg->port);

    for (int i = 0; i < arg->requests; i++) {
        snprintf(member, sizeof(member), "qa:batch:setqa:%d:%d", arg->thread_id, i);
        snprintf(question, sizeof(question), "batch SETQA question thread %d request %d", arg->thread_id, i);
        snprintf(answer, sizeof(answer), "batch SETQA answer thread %d request %d", arg->thread_id, i);
        bulk_arg_t argv[] = {
            bulk_str("SETQA"), bulk_str(qa_index), bulk_str("ID"), bulk_str(member),
            bulk_str("QUESTION"), bulk_str(question),
            bulk_str("ANSWER"), bulk_str(answer)
        };
        run_cmd(sock, "BATCH SETQA", 8, argv, resp, sizeof(resp));
        if (strcmp(resp, "+OK\r\n") != 0) {
            snprintf(arg->err, sizeof(arg->err), "BATCH SETQA failed");
            arg->failed = 1;
            break;
        }
    }
    close(sock);
    return NULL;
}

static void *bench_getqa_worker(void *argp) {
    bench_thread_arg_t *arg = (bench_thread_arg_t *)argp;
    char resp[RECV_BUF_SIZE];
    char question[256];
    const char *qa_index = "qa_batch_index";
    int sock = connect_server(arg->ip, arg->port);

    for (int i = 0; i < arg->requests; i++) {
        snprintf(question, sizeof(question), "batch SETQA question thread %d request %d", arg->thread_id, i % arg->requests);
        bulk_arg_t argv[] = {
            bulk_str("GETQA"), bulk_str(qa_index), bulk_str("QUESTION"), bulk_str(question),
            bulk_str("TOPK"), bulk_str("5")
        };
        run_cmd(sock, "BATCH GETQA", 6, argv, resp, sizeof(resp));
        if (response_is_error(resp) || strstr(resp, "*") == NULL) {
            snprintf(arg->err, sizeof(arg->err), "BATCH GETQA failed");
            arg->failed = 1;
            break;
        }
    }
    close(sock);
    return NULL;
}

static void bench_concurrent(const char *ip, int port, const char *name,
                             void *(*worker)(void *), int concurrency, int total_requests) {
    pthread_t tids[16];
    bench_thread_arg_t args[16];
    int per_thread;
    double start_ms, total_ms;

    if (concurrency <= 0 || concurrency > 16 || total_requests <= 0) return;
    per_thread = total_requests / concurrency;
    if (per_thread <= 0) per_thread = 1;
    total_requests = per_thread * concurrency;

    memset(args, 0, sizeof(args));
    start_ms = now_ms();
    for (int i = 0; i < concurrency; i++) {
        args[i].ip = ip;
        args[i].port = port;
        args[i].thread_id = i;
        args[i].requests = per_thread;
        if (pthread_create(&tids[i], NULL, worker, &args[i]) != 0) {
            perror("pthread_create");
            exit(1);
        }
    }
    for (int i = 0; i < concurrency; i++) {
        pthread_join(tids[i], NULL);
        if (args[i].failed) fail_response(args[i].err, "");
    }
    total_ms = now_ms() - start_ms;

    char label[64];
    snprintf(label, sizeof(label), "%s_BATCH_C%d", name, concurrency);
    print_qps_result(label, total_requests, total_ms);
}

int main(int argc, char **argv) {
    int run_bench = 0;
    if (argc != 3 && argc != 4) {
        fprintf(stderr, "Usage: %s <ip> <port> [bench]\n", argv[0]);
        return 1;
    }
    if (argc == 4 && strcmp(argv[3], "bench") == 0) run_bench = 1;

    int sock = connect_server(argv[1], atoi(argv[2]));
    test_vector(sock);
    test_qa(sock);
    if (run_bench) {
        bench_setqa(sock, 20);
        bench_getqa(sock, 20);
    }
    close(sock);

    if (run_bench) {
        bench_concurrent(argv[1], atoi(argv[2]), "SETQA", bench_setqa_worker, 4, 8);
        bench_concurrent(argv[1], atoi(argv[2]), "GETQA", bench_getqa_worker, 4, 8);
    }

    printf("test_vector: all tests passed\n");
    return 0;
}
