#ifndef KVS_MONITOR_H
#define KVS_MONITOR_H

typedef unsigned int uint32;
typedef unsigned long long uint64;

// 临时存储 RDB 上下文
struct rdb_ctx_t {
    uint64 start_ts;
    uint64 rdb_size;
};

// RingBuffer 上报的 RDB 事件
struct rdb_event_t {
    uint32 pid;
    uint64 duration_ms;
    uint64 rdb_size;
    uint64 bandwidth_mb;
};

// Map 中的增量指标
struct sync_metrics_t {
    uint64 cmd_count;
    uint64 tcp_srtt_us;
};

#endif