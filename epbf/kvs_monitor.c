#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <bpf/libbpf.h>
#include "kvs_monitor.h"
#include "kvs_monitor.skel.h" // 自动生成

// 与 Python Server 通信的 UDP 协议结构 (紧凑排列)
struct __attribute__((packed)) udp_metric_t {
    uint32_t role;      // 0=Master, 1=Slave
    uint64_t timestamp;
    uint64_t cmd_count;
    uint64_t tcp_srtt;
};

static volatile int stop = 0;
static volatile int rdb_finished = 0; // 标记 RDB 是否完成

void sig_handler(int signo) {
    stop = 1;
}



// RDB RingBuffer 回调函数
int handle_rdb_event(void *ctx, void *data, size_t sz) {
    const struct rdb_event_t *e = data;
    
    printf("\n>>> [RDB SYNC DETECTED] <<<\n");
    printf("PID: %u | Size: %llu Bytes | Time: %llu ms | Speed: %llu MB/s\n",
           e->pid, e->rdb_size, e->duration_ms, e->bandwidth_mb);
    printf("------------------------------------------------\n");
    
    // 标记 RDB 已完成，主循环可以进入下一阶段
    rdb_finished = 1;
    return 0;
}

long get_symbol_offset(const char *binary_path, const char *func_name) {
    char cmd[256];
    char buf[64];
    
    // 构造命令：nm <binary> | grep <func_name> | head -n 1 | awk '{print $1}'
    // 这会返回 16 进制的地址，比如 0000000000456123
    snprintf(cmd, sizeof(cmd), "nm %s | grep '%s' | head -n 1 | awk '{print $1}'", binary_path, func_name);
    
    FILE *fp = popen(cmd, "r");
    if (fp == NULL) {
        perror("Failed to run nm");
        return 0;
    }

    if (fgets(buf, sizeof(buf), fp) == NULL) {
        // 没找到符号
        pclose(fp);
        return 0;
    }
    pclose(fp);

    // 将 16 进制字符串转为 long
    return strtol(buf, NULL, 16);
}

int main(int argc, char **argv) {
    if (argc < 7) {
        printf("Usage: %s <PID> <ROLE: m/s> <monitor rdb> <LOCAL_PORT> <SERVER_IP> <SERVER_PORT>\n", argv[0]);
        return 1;
    }

    int pid = atoi(argv[1]);
    int is_master = (argv[2][0] == 'm'); // 'm' or 's'
    printf("[INFO] Monitoring PID %d as %s\n", pid, is_master ? "MASTER" : "SLAVE");
    int monitor_rdb = atoi(argv[3]);
    int local_port = atoi(argv[4]);
    const char *server_ip = argv[5];
    int server_port = atoi(argv[6]);

    // 1. 准备 UDP Socket
    int sockfd;
    struct sockaddr_in servaddr;
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("Socket creation failed");
        return 1;
    }
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &servaddr.sin_addr);

    // 2. 加载 eBPF
    struct kvs_monitor_bpf *skel = kvs_monitor_bpf__open();
    if (!skel) return -1;
    if (kvs_monitor_bpf__load(skel)) return -1;

    // 3. 配置端口过滤 Map
    uint32_t key = 0;
    uint16_t port_val = (uint16_t)local_port;
    bpf_map__update_elem(skel->maps.target_port_map, &key, sizeof(key), &port_val, sizeof(port_val), BPF_ANY);

    // 4. Attach Hooks (根据角色)
    char binary_path[128];
    snprintf(binary_path, sizeof(binary_path), "/proc/%d/exe", pid);
    //const char *binary_path = "/home/aris/open_source/9.1-kvstore/kvstore"; // 直接指定路径，避免/proc依赖

    skel->links.kprobe_tcp_rcv = bpf_program__attach_kprobe(
        skel->progs.kprobe_tcp_rcv, false, "tcp_rcv_established"); // Kprobes don't need PID/binary_path here

    if (is_master) {
        size_t rdb_sent_offset = get_symbol_offset(binary_path, "_on_rdb_sent"); 
        size_t rdb_ack_offset  = get_symbol_offset(binary_path, "_on_rdb_ack");
        size_t master_inc_offset = get_symbol_offset(binary_path, "kvs_master_propagate_command_to_slaves");


        printf("[INFO] Attaching MASTER hooks...\n");
        // RDB Hooks
        skel->links.uprobe_rdb_start = bpf_program__attach_uprobe(
            skel->progs.uprobe_rdb_start, false, pid, binary_path, rdb_sent_offset);

        if (!skel->links.uprobe_rdb_start) {
            fprintf(stderr, "ERROR: Failed to attach uprobe_rdb_start: %s\n", strerror(errno));
        } else {
            printf("SUCCESS: uprobe_rdb_start attached at symbol _on_rdb_sent\n");
        }

        skel->links.uprobe_rdb_ack = bpf_program__attach_uprobe(
            skel->progs.uprobe_rdb_ack, false, pid, binary_path, rdb_ack_offset);
        // Incremental Hook
        skel->links.uprobe_master_inc = bpf_program__attach_uprobe(
            skel->progs.uprobe_master_inc, false, pid, binary_path, master_inc_offset);
    } else {
        printf("[INFO] Attaching SLAVE hooks...\n");

        size_t slave_inc_offset = get_symbol_offset(binary_path, "_kvs_slave_cmd_logic");
        // Slave 只需要监听增量执行
        skel->links.uprobe_slave_inc = bpf_program__attach_uprobe(
            skel->progs.uprobe_slave_inc, false, pid, binary_path, slave_inc_offset);
        if(!skel->links.uprobe_slave_inc) {
            fprintf(stderr, "ERROR: Failed to attach uprobe_slave_inc: %s\n", strerror(errno));
        } else {
            printf("SUCCESS: uprobe_slave_inc attached at symbol _kvs_slave_cmd_logic\n");
        }
    }
    
    // TCP Hook 全局生效
    //kvs_monitor_bpf__attach(skel);
    

    // 5. 设置 RingBuffer (用于 RDB 事件)
    struct ring_buffer *rb = NULL;
    if(is_master)
        rb = ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_rdb_event, NULL, NULL);


    if ((is_master && (!skel->links.uprobe_rdb_start || !skel->links.uprobe_rdb_ack || !skel->links.uprobe_master_inc)) ||
        (!is_master && !skel->links.uprobe_slave_inc) ||
        !skel->links.kprobe_tcp_rcv) {
        fprintf(stderr, "Failed to attach all necessary BPF programs.\n");
        goto cleanup; // 跳转到清理部分，避免空指针访问
    }

    signal(SIGINT, sig_handler);
    printf("[INFO] Monitoring PID %d. Sending metrics to %s:%d\n", pid, server_ip, server_port);

    // ============================================================
    // 阶段一：等待 RDB 完成 (仅 Master)
    // ============================================================
    if (is_master && monitor_rdb) {
        printf("[PHASE 1] Waiting for RDB sync completion...\n");
        // 循环等待，直到 rdb_finished 被回调置为 1，或者用户按 Ctrl+C
        while (!stop && !rdb_finished) {
            // 轮询 RingBuffer, timeout 100ms
            int cnt = ring_buffer__poll(rb, 100);
            
            // 如果 100ms 内没有事件，可以打印一些等待动画
            printf("."); fflush(stdout);
        }
        if (stop) goto cleanup;
        printf("[PHASE 1] RDB Phase Done. Switching to Incremental Monitor.\n");
    } else {
        printf("[PHASE 1] Slave mode or RDB monitoring disabled, skipping RDB wait.\n");
    }

    // ============================================================
    // 阶段二：增量同步循环上报
    // ============================================================
    printf("[PHASE 2] Streaming Incremental Metrics via UDP...\n");

    while (!stop) {
        sleep(1); // 采样频率 1s

        struct sync_metrics_t stats;
        struct udp_metric_t report;
        
        // 读取 Map 中的计数器
        if (bpf_map__lookup_elem(skel->maps.metrics_map, &key, sizeof(key), &stats, sizeof(stats), 0) == 0) {
            
            // 填充 UDP 包
            report.role = is_master ? 0 : 1;
            report.timestamp = (uint64_t)time(NULL);
            report.cmd_count = stats.cmd_count;
            report.tcp_srtt = stats.tcp_srtt_us;

            // 发送
            sendto(sockfd, &report, sizeof(report), 0, 
                   (const struct sockaddr *)&servaddr, sizeof(servaddr));

            // 本地终端也可以输出一下，方便调试
            printf("\r[%s] Count: %llu | SRTT: %llu us   ", 
                   is_master ? "TX" : "RX", stats.cmd_count, stats.tcp_srtt_us);
            fflush(stdout);
        }
    }

cleanup:
    printf("\nExiting...\n");
    if (sockfd != -1) close(sockfd);
    if (rb) ring_buffer__free(rb);
    if (skel) kvs_monitor_bpf__destroy(skel);
    return 0;
}