#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "kvs_monitor.h"

#define bpf_htons(x) __builtin_bswap16(x)
#define bpf_ntohs(x) __builtin_bswap16(x)
#define bpf_htonl(x) __builtin_bswap32(x)
#define bpf_ntohl(x) __builtin_bswap32(x)

// ---------------- 配置区 ----------------
// 由用户态填充，告诉内核要过滤哪个端口
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, u32);
    __type(value, u16); // 目标 TCP 端口 (Host Byte Order)
} target_port_map SEC(".maps");

// ---------------- 存储区 ----------------
// 1. 增量同步指标 (Map)
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, u32);
    __type(value, struct sync_metrics_t);
} metrics_map SEC(".maps");

// 临时存储开始状态: Key=PID, Value=Context
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1024);
    __type(key, u32);
    __type(value, struct rdb_ctx_t);
} start_map SEC(".maps");

// 上报事件队列
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} rb SEC(".maps");


// ============================================================
//  RDB 监控逻辑 (Hook 你的 _on_rdb_sent)
// ============================================================

// -----------------------------------------------------------
// Hook 1: 开始传输
// signature: _on_rdb_sent(master, conn, trigger)
// -----------------------------------------------------------
SEC("uprobe/_on_rdb_sent")
int BPF_KPROBE(uprobe_rdb_start, void *master, void *conn, int trigger) {
    bpf_printk("BPF: _on_rdb_sent triggered! trigger=%d", trigger);
    // 1. 立即退出非目标 trigger，减少分支复杂度
    if (trigger != 4) 
        return 0;

    u32 pid = bpf_get_current_pid_tgid() >> 32;

    // 2. 重点：改名并强制初始化。
    // 使用 info 而不是 rdb_ctx，确保不和任何宏冲突
    struct rdb_ctx_t info = {0}; 

    // 3. 记录时间
    info.start_ts = bpf_ktime_get_ns();

    // 4. 重点优化：直接读入到结构体成员中，不使用中间变量
    // 这样验证器能更清晰地追踪内存流向
    if (master) {
        bpf_probe_read_user(&info.rdb_size, sizeof(info.rdb_size), master + 88);
    }

    // 5. 更新 Map
    // 这里如果还是报错，我们可以尝试先定义一个局部 key 变量，并确保它和 info 在栈上保持距离
    u32 key_pid = pid; 
    bpf_map_update_elem(&start_map, &key_pid, &info, BPF_ANY);
    
    return 0;
}

// -----------------------------------------------------------
// Hook 2: 传输完成 + ACK
// signature: _on_rdb_ack(master, conn, trigger)
// -----------------------------------------------------------
SEC("uprobe/_on_rdb_ack")
int BPF_KPROBE(uprobe_rdb_ack) {
    bpf_printk("BPF: _on_rdb_ack triggered!");
    u32 pid_val = bpf_get_current_pid_tgid() >> 32;
    
    // 这里的变量也改名，避开 ctx 这个词
    struct rdb_ctx_t *found = bpf_map_lookup_elem(&start_map, &pid_val);
    if (!found) 
        return 0;

    u64 now = bpf_ktime_get_ns();
    u64 delta_ns = now - found->start_ts;

    // 预留空间上报
    struct rdb_event_t *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (e) {
        e->pid = pid_val;
        e->rdb_size = found->rdb_size;
        e->duration_ms = delta_ns / 1000000;
        
        if (e->duration_ms > 0) {
            // 注意：内核里尽量不做复杂的除法，这里简单计算
            e->bandwidth_mb = (found->rdb_size * 1000) / (e->duration_ms * 1024 * 1024);
        } else {
            e->bandwidth_mb = 0;
        }
        bpf_ringbuf_submit(e, 0);
    }

    bpf_map_delete_elem(&start_map, &pid_val);
    return 0;
}

// ============================================================
//  增量同步监控逻辑
// ============================================================

// Master 发送计数
SEC("uprobe/kvs_master_propagate_command_to_slaves")
int BPF_KPROBE(uprobe_master_inc) {
    u32 key = 0;
    struct sync_metrics_t *m = bpf_map_lookup_elem(&metrics_map, &key);
    if (m) __sync_fetch_and_add(&m->cmd_count, 1);
    return 0;
}

// Slave 执行计数
SEC("uprobe/_kvs_slave_cmd_logic")
int BPF_KPROBE(uprobe_slave_inc) {
    
    u32 key = 0;
    struct sync_metrics_t *m = bpf_map_lookup_elem(&metrics_map, &key);
    if (m) __sync_fetch_and_add(&m->cmd_count, 1);
    return 0;
}

// ============================================================
//  网络层监控 (SRTT) - 过滤端口
// ============================================================
SEC("kprobe/tcp_rcv_established")
int BPF_KPROBE(kprobe_tcp_rcv, struct sock *sk) {
    u32 key = 0;
    u16 *target_port = bpf_map_lookup_elem(&target_port_map, &key);
    if (!target_port) return 0;

    // 获取当前 socket 的本地或远端端口
    // 这里简单判断：只要源端口或目的端口匹配即可
    u16 dport = BPF_CORE_READ(sk, __sk_common.skc_dport);
    u16 num   = BPF_CORE_READ(sk, __sk_common.skc_num); // src port
    
    // 内核存储端口是大端序 (Network Byte Order)
    // 我们的 target_port 在用户态已经是 Host Order，这里做一下转换比较
    // 假设是 x86 (小端)，bpf_htons()
    
    // 简化逻辑：如果在监控 TCP 连接延迟
    if (dport == bpf_htons(*target_port) || num == *target_port) {
        struct sync_metrics_t *m = bpf_map_lookup_elem(&metrics_map, &key);
        if (m) {
            struct tcp_sock *tp = (struct tcp_sock *)sk;
            m->tcp_srtt_us = BPF_CORE_READ(tp, srtt_us) >> 3;
        }
    }
    return 0;
}

char LICENSE[] SEC("license") = "GPL";