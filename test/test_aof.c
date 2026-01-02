#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>

#define MAX_ITEMS 1048576 
#define BUF_SIZE 65536     // 增加缓冲区以处理批量响应
#define BATCH_SIZE 1000    // 每发送 1000 条指令检查一次返回

// 计算时间差（秒）
double get_time_diff(struct timeval start, struct timeval end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
}

int format_set(char *buf, int i) {
    char key[32], val[64];
    sprintf(key, "key:%07d", i);
    sprintf(val, "value_content_%07d", i);
    return sprintf(buf, "*3\r\n$3\r\nSET\r\n$%ld\r\n%s\r\n$%ld\r\n%s\r\n", 
                   strlen(key), key, strlen(val), val);
}

// 在缓冲区中查找特定字符串并计数
int count_occurrences(const char *buf, int len, const char *pattern) {
    int count = 0;
    const char *ptr = buf;
    int pat_len = strlen(pattern);
    while ((ptr = strstr(ptr, pattern)) != NULL) {
        if (ptr - buf >= len) break;
        count++;
        ptr += pat_len;
    }
    return count;
}

void run_test(const char *ip, int port, int mode, int count) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Connect failed");
        exit(1);
    }

    char send_buf[4096];
    char recv_buf[BUF_SIZE];
    int sent_count = 0;
    int success_count = 0;
    struct timeval start_time, end_time;

    printf("Starting %s Test: %d items...\n", (mode == 1 ? "SET" : "GET"), count);
    gettimeofday(&start_time, NULL);

    while (success_count < count) {
        // 1. 发送阶段：只要没发完，就往死里发，直到达到 BATCH 阈值
        while (sent_count < count && (sent_count - success_count) < BATCH_SIZE) {
            int len = format_set(send_buf, sent_count);
            if (send(sock, send_buf, len, 0) <= 0) break;
            sent_count++;
        }

        // 2. 接收阶段：读取并统计 +OK\r\n 的数量
        int r = recv(sock, recv_buf, sizeof(recv_buf) - 1, MSG_DONTWAIT);
        if (r > 0) {
            recv_buf[r] = '\0';
            success_count += count_occurrences(recv_buf, r, "+OK\r\n");
        } else if (r == 0) {
            printf("\nServer closed connection.\n");
            break;
        }

        // 3. 打印进度
        if (sent_count % 10000 == 0) {
            printf("\rProgress: Sent %d, Confirmed %d", sent_count, success_count);
            fflush(stdout);
        }
    }

    gettimeofday(&end_time, NULL);
    double total_time = get_time_diff(start_time, end_time);

    printf("\n\n--- Test Results ---\n");
    printf("Total Time:     %.3f seconds\n", total_time);
    printf("Sent Commands:  %d\n", sent_count);
    printf("Success (OK):   %d\n", success_count);
    printf("Actual QPS:     %.2f\n", success_count / total_time);
    printf("--------------------\n");

    close(sock);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <ip> <port> <mode: 1-SET> [count]\n", argv[0]);
        return -1;
    }
    run_test(argv[1], atoi(argv[2]), atoi(argv[3]), (argc == 5 ? atoi(argv[4]) : 100000));
    return 0;
}