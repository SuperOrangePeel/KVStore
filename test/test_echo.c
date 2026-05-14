#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define BUF_SIZE 1048576
#define DEFAULT_COUNT 5000000
#define DEFAULT_BATCH_SIZE 500

static double get_time_diff(struct timeval start, struct timeval end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
}

static int format_echo_set(char *buf, size_t buf_size, int i) {
    return snprintf(buf, buf_size, "SET echo:key:%07d echo_value_content_%07d\r\n", i, i);
}

static int send_all(int sock, const char *buf, int len) {
    int sent = 0;

    while (sent < len) {
        int n = send(sock, buf + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += n;
    }

    return sent;
}

static int connect_server(const char *ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket failed");
        exit(1);
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        perror("inet_pton failed");
        close(sock);
        exit(1);
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect failed");
        close(sock);
        exit(1);
    }

    return sock;
}

static void run_echo_test(const char *ip, int port, int count, int batch_size) {
    int sock = connect_server(ip, port);
    char send_buf[BUF_SIZE];
    char recv_buf[BUF_SIZE];
    int sent_count = 0;
    int confirmed_count = 0;
    struct timeval start_time;
    struct timeval end_time;

    printf("Starting TCP Echo SET-payload Test: %d items, pipeline %d...\n", count, batch_size);
    gettimeofday(&start_time, NULL);

    while (confirmed_count < count) {
        int batch_len = 0;
        int batch_count = 0;
        int recv_len = 0;

        while (sent_count + batch_count < count && batch_count < batch_size) {
            int remaining = (int)sizeof(send_buf) - batch_len;
            int len = format_echo_set(send_buf + batch_len, remaining,
                                      sent_count + batch_count);
            if (len <= 0) {
                fprintf(stderr, "failed to format send message %d\n", sent_count + batch_count);
                close(sock);
                exit(1);
            }
            if (len >= remaining) {
                if (batch_count == 0) {
                    fprintf(stderr, "send buffer is too small\n");
                    close(sock);
                    exit(1);
                }
                break;
            }
            batch_len += len;
            batch_count++;
        }

        if (batch_count == 0) break;

        if (send_all(sock, send_buf, batch_len) < 0) {
            perror("send failed");
            close(sock);
            exit(1);
        }
        sent_count += batch_count;

        while (recv_len < batch_len) {
            int r = recv(sock, recv_buf, sizeof(recv_buf), 0);
            if (r > 0) {
                recv_len += r;
            } else if (r == 0) {
                fprintf(stderr, "server closed connection. confirmed: %d, sent: %d\n",
                        confirmed_count, sent_count);
                close(sock);
                exit(1);
            } else if (errno != EINTR) {
                perror("recv failed");
                close(sock);
                exit(1);
            }
        }

        confirmed_count += batch_count;
    }

    gettimeofday(&end_time, NULL);
    double total_time = get_time_diff(start_time, end_time);
    double qps = total_time > 0 ? confirmed_count / total_time : 0;

    printf("\n--- TCP Echo Results ---\n");
    printf("Total Time:     %.6f seconds\n", total_time);
    printf("Success Count:  %d\n", confirmed_count);
    printf("Actual QPS:     %.2f\n", qps);
    printf("------------------------\n");

    close(sock);
}

int main(int argc, char *argv[]) {
    if (argc < 3 || argc > 5) {
        printf("Usage: %s <ip> <port> [count] [pipeline]\n", argv[0]);
        return -1;
    }

    int count = argc >= 4 ? atoi(argv[3]) : DEFAULT_COUNT;
    int batch_size = argc >= 5 ? atoi(argv[4]) : DEFAULT_BATCH_SIZE;
    if (count <= 0 || batch_size <= 0) {
        fprintf(stderr, "count and pipeline must be positive\n");
        return -1;
    }

    run_echo_test(argv[1], atoi(argv[2]), count, batch_size);
    return 0;
}
