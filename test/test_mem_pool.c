#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <fcntl.h>

#define BUF_SIZE 1048576      
#define BATCH_SIZE 500        

// --- 格式化函数 (Key 8B, Value 16B) ---

int format_hset(char *buf, int i) {
    char key[16], val[32];
    // Key: 8 bytes (e.g. "00000001")
    sprintf(key, "%08d", i);
    // Value: 16 bytes (e.g. "val:000000000001")
    sprintf(val, "%08d", i);
    
    // RESP: $8 for key, $16 for val
    return sprintf(buf, "*3\r\n$4\r\nHSET\r\n$8\r\n%s\r\n$8\r\n%s\r\n", key, val);
}

int format_hdel(char *buf, int i) {
    char key[16];
    sprintf(key, "%08d", i);
    return sprintf(buf, "*2\r\n$4\r\nHDEL\r\n$8\r\n%s\r\n", key);
}

// --- 辅助工具 ---

double get_time_diff(struct timeval start, struct timeval end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
}

int count_responses(const char *buf, int len) {
    int count = 0;
    for (int i = 0; i < len; i++) {
        // 统计所有类型的响应头
        if (buf[i] == '+' || buf[i] == '$' || buf[i] == '-' || buf[i] == ':') {
            count++;
        }
    }
    return count;
}

// --- 核心执行阶段 ---

void run_phase(int sock, const char* phase_name, int start_id, int count, int is_del) {
    char send_buf[4096];
    char recv_buf[BUF_SIZE];
    int sent = 0;
    int confirmed = 0;
    struct timeval start, end;

    printf("\n>>> PHASE: %s [%d items from ID %d] <<<\n", phase_name, count, start_id);
    gettimeofday(&start, NULL);

    while (confirmed < count) {
        // 1. 批量发送
        while (sent < count && (sent - confirmed) < BATCH_SIZE) {
            int len;
            if (is_del) {
                len = format_hdel(send_buf, start_id + sent);
            } else {
                len = format_hset(send_buf, start_id + sent);
            }
            
            int n = send(sock, send_buf, len, 0);
            if (n <= 0) break; // 发送缓冲满
            sent++;
        }

        // 2. 批量接收
        int r = recv(sock, recv_buf, sizeof(recv_buf), MSG_DONTWAIT);
        
        if (r > 0) {
            confirmed += count_responses(recv_buf, r);
        } else if (r == 0) {
            printf("Error: Server closed connection.\n");
            exit(1);
        }

        if (sent % 100000 == 0) {
            printf("   Processing: %d / %d\r", sent, count);
            fflush(stdout);
        }
    }

    // 收尾：确保收齐
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
    while (confirmed < count) {
        int r = recv(sock, recv_buf, sizeof(recv_buf), 0);
        if (r <= 0) break;
        confirmed += count_responses(recv_buf, r);
    }
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    gettimeofday(&end, NULL);
    printf("   Done. Time: %.3fs | QPS: %.2f\n", 
           get_time_diff(start, end), confirmed / get_time_diff(start, end));
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <ip> <port> <count_per_phase>\n", argv[0]);
        return -1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int count = atoi(argv[3]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Connect failed");
        return -1;
    }
    
    // 设置非阻塞
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    printf("=== Test Plan: %d items per phase ===\n", count);
    printf("1. Insert 0 -> %d\n", count);
    printf("2. Delete 0 -> %d\n", count);
    printf("3. Insert %d -> %d (New Data)\n", count, count * 2);
    printf("Press ENTER to start...");
    getchar();

    // Phase 1: 填满
    run_phase(sock, "FILL (0 to N)", 0, count, 0);
    
    printf("\n[CHECK HTOP] Memory should be HIGH. Press ENTER for Delete...");
    getchar();

    // Phase 2: 删空
    run_phase(sock, "DELETE (0 to N)", 0, count, 1);

    printf("\n[CHECK HTOP] Memory should NOT drop (Slab) / Maybe drop (malloc). Press ENTER for Refill...");
    getchar();

    // Phase 3: 填入新数据 (复用测试)
    // 注意：这里我们用 count 作为起始ID，意味着是全新的Key，
    // 强制测试内存池是否复用了之前 delete 掉的 0-N 的空间。
    run_phase(sock, "REFILL (N to 2N)", count, count, 0);

    printf("\n[CHECK HTOP] Slab: RES should be SAME as Phase 1. Malloc: RES might grow.\n");
    
    close(sock);
    return 0;
}