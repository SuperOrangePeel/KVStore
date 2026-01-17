// kvs_event.h
#ifndef KVS_EVENT_H
#define KVS_EVENT_H

#include <stdlib.h>

// 前向声明
struct kvs_event_s;
struct io_uring_cqe;

// 通用回调函数签名
// ctx: 用户数据
// res: io_uring 的结果 (字节数 或 错误码)
// flags: 额外标志
typedef void (*kvs_event_cb)(void *ctx, int res, int flags);

// 事件类型（给框架调度用）
typedef enum {
    KVS_EV_READ = 0,
    KVS_EV_WRITE,
    KVS_EV_ACCEPT,
    KVS_EV_SIGNAL,
    KVS_EV_TIMER,
    KVS_EV_CONNECT
} kvs_event_type_t;

// --- 核心结构体 ---
typedef struct kvs_event_s {
    // 1. 身份标识
    int fd;                 // 关联的文件描述符 (socket/signalfd/timerfd)
    kvs_event_type_t type;  // 事件类型
    
    // 2. 行为
    kvs_event_cb handler;   // 回调函数
    void *ctx;              // 上下文 (通常指向包含这个 event 的父结构体，如 conn)

    // 3. io_uring 提交参数 (按需使用)
    void *buf;
    size_t len;
    
} kvs_event_t;

// 辅助函数：初始化事件
static inline void kvs_event_init(kvs_event_t *ev, int fd, kvs_event_type_t type, kvs_event_cb cb, void *ctx) {
    ev->fd = fd;
    ev->type = type;
    ev->handler = cb;
    ev->ctx = ctx;
}

#endif