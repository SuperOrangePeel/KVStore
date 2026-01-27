#ifndef KVS_LOOP_H
#define KVS_LOOP_H

#include <liburing.h>
#include <sys/socket.h>
#include <netinet/in.h>

// 前向声明
struct kvs_loop_s;

// --- 1. 事件类型定义 (属于 Loop) ---
typedef enum {
    KVS_EV_NONE = 0,
    KVS_EV_READ,        // Socket 读 / File 读
    KVS_EV_WRITE,       // Socket 写 / File 写
    KVS_EV_POLL_IN,     // 可读事件 (poll)
    KVS_EV_POLL_OUT,    // 可写事件 (poll)
    KVS_EV_ACCEPT,      // 监听
    KVS_EV_CONNECT,     // 主动连接
    KVS_EV_SIGNAL,      // 信号
    KVS_EV_TIMER        // 定时器
} kvs_event_type_t;

// 通用回调签名
// ctx: 用户传入的上下文 (通常是 conn 或 task 结构体)
// res: 系统调用返回值 (读写字节数 或 errno)
// flags: cqe->flags (例如 IORING_CQE_F_MORE)
typedef void (*kvs_event_cb)(void *ctx, int res, int flags);

// --- 原子事件结构 (嵌入到业务结构体中) ---
typedef struct kvs_event_s {
    int fd;                 // 关联的文件描述符
    kvs_event_type_t type;  // 事件类型
    kvs_event_cb handler;   // 回调函数
    void *ctx;              // 用户上下文
    //int version;          // 版本号，防止过期事件处理
    
    // 预留给 uring 的参数，避免 malloc
    // void *buf;
    // size_t len;
} kvs_event_t;

// --- 循环引擎 ---
typedef struct kvs_loop_s {
    struct io_uring ring;
    int stop;
} kvs_loop_t;

// API
int kvs_loop_init(kvs_loop_t *loop, int entries);
void kvs_loop_deinit(kvs_loop_t *loop);
void kvs_loop_run(kvs_loop_t *loop);
void kvs_loop_stop(kvs_loop_t *loop);


// --- 提交接口 (只 Prep，不 Submit) ---
int kvs_loop_add_accept(kvs_loop_t *loop, kvs_event_t *ev, struct sockaddr *addr, socklen_t *addrlen);
int kvs_loop_add_send(kvs_loop_t *loop, kvs_event_t *ev, void *buf, size_t len); 
int kvs_loop_add_recv(kvs_loop_t *loop, kvs_event_t *ev, void* buf, size_t len);

int kvs_loop_add_write(kvs_loop_t *loop, kvs_event_t *ev, void *buf, size_t len, off_t offset);
int kvs_loop_add_fsync(kvs_loop_t *loop, kvs_event_t *ev, int fd);
int kvs_loop_add_read(kvs_loop_t *loop, kvs_event_t *ev, void *buf, size_t len);

int kvs_loop_add_timeout(kvs_loop_t *loop, kvs_event_t *ev, struct __kernel_timespec *ts);


void kvs_loop_cancel_event(kvs_loop_t *loop, kvs_event_t *ev);


int kvs_loop_add_poll_in(kvs_loop_t *loop, kvs_event_t *ev);
int kvs_loop_add_poll_out(kvs_loop_t *loop, kvs_event_t *ev);

// 辅助：初始化事件对象
static inline void kvs_event_init(kvs_event_t *ev, int fd, /*int version, */kvs_event_type_t type, kvs_event_cb cb, void *ctx) {
    ev->fd = fd;
    ev->type = type;
    ev->handler = cb;
    ev->ctx = ctx;
   //ev->version = version;
    // ev->buf = NULL;
    // ev->len = 0;
}
#endif // KVS_LOOP_H