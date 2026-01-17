#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>

#define MAX_ITEMS 1048576 
#define BUF_SIZE 1048576  // 1 MB     
#define BATCH_SIZE 500    // 适当减小 Batch，防止接收缓冲区溢出

double get_time_diff(struct timeval start, struct timeval end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
}

void reverse_string(char* str) {
    if (str == NULL) return;
    
    int length = strlen(str);
    int start = 0;
    int end = length - 1;
    
    while (start < end) {
        char temp = str[start];
        str[start] = str[end];
        str[end] = temp;
        start++;
        end--;
    }
}

// 构造 HSET 指令
int format_hset(char *buf, int i) {
    char key[32], val[64];
    sprintf(key, "key:%07d", i);
    reverse_string(key);
    sprintf(val, "value_content_%07d", i);
    
    int ret = sprintf(buf, "*3\r\n$4\r\nHSET\r\n$%ld\r\n%s\r\n$%ld\r\n%s\r\n", 
                   strlen(key), key, strlen(val), val);
    //rintf("format_hset ret:%d\n", ret);
    return ret;
}

// 构造 HGET 指令
int format_hget(char *buf, int i) {
    char key[32];
    sprintf(key, "key:%07d", i);
    reverse_string(key);
    return sprintf(buf, "*2\r\n$4\r\nHGET\r\n$%ld\r\n%s\r\n", 
                   strlen(key), key);
}

// 统计缓冲区中出现的字符串次数
int count_confirmed_responses(const char *buf, int len) {
    int count = 0;
    for (int i = 0; i < len; i++) {
        
        if (buf[i] == '+' || buf[i] == '$') {
            count++;
        }
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

    const char* mode_str = (mode == 1 ? "HSET" : "HGET");

    printf("Starting %s Test: %d items...\n", mode_str, count);
    gettimeofday(&start_time, NULL);

    while (success_count < count) {
        // 1. 批量发送
        while (sent_count < count && (sent_count - success_count) < BATCH_SIZE) {
            int len = (mode == 1) ? format_hset(send_buf, sent_count) : format_hget(send_buf, sent_count);
            if (send(sock, send_buf, len, 0) <= 0) break;
            sent_count++;
        }

        // 2. 批量接收并校验
        int r = recv(sock, recv_buf, sizeof(recv_buf) - 1, MSG_DONTWAIT);
        // printf("recv:[%.*s]\n", r, recv_buf);

        if(r <=0 && errno != EAGAIN) {
            perror("Receive failed");
            break;
        }
        if (r > 0) {
            success_count += count_confirmed_responses(recv_buf, r);
        } else if (r == 0) {
            printf("\nServer closed connection. Total handled: %d\n", success_count);
            break;
        }

        // 3. 打印进度
        if (sent_count % 500000 == 0) {
            printf("Progress: Sent %d, Confirmed %d\n", sent_count, success_count);
            fflush(stdout);
        }
    }

    gettimeofday(&end_time, NULL);
    double total_time = get_time_diff(start_time, end_time);

    printf("\n\n--- %s Results ---\n", mode_str);
    printf("Total Time:     %.3f seconds\n", total_time);
    printf("Success Count:  %d\n", success_count);
    printf("Actual QPS:     %.2f\n", success_count / total_time);
    printf("--------------------\n");

    close(sock);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <ip> <port> <mode: 1-HSET, 2-HGET> [count]\n", argv[0]);
        return -1;
    }
    int mode = atoi(argv[3]);
    int count = (argc == 5) ? atoi(argv[4]) : 500000;
    run_test(argv[1], atoi(argv[2]), mode, count);
    return 0;
}