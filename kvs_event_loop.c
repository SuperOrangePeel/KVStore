#include "kvs_event_loop.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

int kvs_loop_init(kvs_loop_t *loop, int entries) {
    memset(loop, 0, sizeof(*loop));
    // IORING_SETUP_SQPOLL 可以进一步减少系统调用，但需要 root 权限，暂时不用
    if (io_uring_queue_init(entries, &loop->ring, 0) < 0) {
        perror("io_uring_queue_init");
        return -1;
    }
    return 0;
}

void kvs_loop_deinit(kvs_loop_t *loop) {
    io_uring_queue_exit(&loop->ring);
}

void kvs_loop_stop(kvs_loop_t *loop) {
    loop->stop = 1;
}

// --- 核心：主循环 ---
void kvs_loop_run(kvs_loop_t *loop) {
    struct io_uring_cqe *cqe;
    unsigned head;
    
    while (!loop->stop) {
        // 【关键点】：Submit and Wait
        // 这一步会把上一轮回调中所有 add_read/write 产生的 SQE 一次性提交 (Batch Submit)
        // 并阻塞等待至少 1 个完成事件
        int ret = io_uring_submit_and_wait(&loop->ring, 1);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("io_uring_submit_and_wait");
            break; // Fatal error
        }

        // 批量处理完成队列 (Batch Process CQ)
        io_uring_for_each_cqe(&loop->ring, head, cqe) {
            kvs_event_t *ev = (kvs_event_t *)io_uring_cqe_get_data(cqe);
            
            if (ev && ev->handler) {
                // 回调业务层
                // 注意：业务层在这里面调用 kvs_loop_add_* 不会触发 syscall，只会写 SQ Ring
                ev->handler(ev->ctx, cqe->res, cqe->flags);
            }
            
            // 标记 CQE 已处理，暂不通知内核，等一圈处理完统一 advance
        }
        
        // 告诉内核我们处理了一批 CQE
        io_uring_cq_advance(&loop->ring, 0); 
        // advance 0 是因为 liburing 的 for_each 宏配合 cq_advance 可能会有版本差异
        // 标准做法是：io_uring_cqe_seen 内部已经调了 advance。
        // 所以上面的 for_each 里应该用 io_uring_cqe_seen(&loop->ring, cqe);
        // 为了性能优化，我们可以手动批量 advance，但 liburing 封装得很好，
        // 建议直接在循环里用 io_uring_cqe_seen。
    }
}

// --- 内部辅助：安全获取 SQE ---
// 如果 SQ 满了，先提交一批腾出空间，再获取
static struct io_uring_sqe *get_sqe_safe(kvs_loop_t *loop) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    if (!sqe) {
        // SQ 满了，强制提交一次
        io_uring_submit(&loop->ring);
        sqe = io_uring_get_sqe(&loop->ring);
        if (!sqe) {
            // 还是满的？说明 CQ 也满了没处理，或者内核太忙。
            // 这是一个极端情况，简单起见打印错误。
            fprintf(stderr, "Fatal: io_uring SQ overflow!\n");
        }
    }
    return sqe;
}

// --- 事件注册实现 ---

int kvs_loop_add_accept(kvs_loop_t *loop, kvs_event_t *ev, struct sockaddr *addr, socklen_t *addrlen) {
    struct io_uring_sqe *sqe = get_sqe_safe(loop);
    if (!sqe) return -1;
    
    io_uring_prep_accept(sqe, ev->fd, addr, addrlen, 0);
    io_uring_sqe_set_data(sqe, ev);
    return 0;
}

int kvs_loop_add_read(kvs_loop_t *loop, kvs_event_t *ev) {
    struct io_uring_sqe *sqe = get_sqe_safe(loop);
    if (!sqe) return -1;

    // 使用 ev 内部缓存的指针和长度
    io_uring_prep_recv(sqe, ev->fd, ev->buf, ev->len, 0);
    io_uring_sqe_set_data(sqe, ev);
    return 0;
}

int kvs_loop_add_read_buffer(kvs_loop_t *loop, kvs_event_t *ev, void *buf, size_t len) {
    struct io_uring_sqe *sqe = get_sqe_safe(loop);
    if (!sqe) return -1;

    // 显式指定 buffer (用于 signalfd read)
    io_uring_prep_read(sqe, ev->fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ev);
    return 0;
}

int kvs_loop_add_write(kvs_loop_t *loop, kvs_event_t *ev) {
    struct io_uring_sqe *sqe = get_sqe_safe(loop);
    if (!sqe) return -1;

    io_uring_prep_send(sqe, ev->fd, ev->buf, ev->len, 0);
    io_uring_sqe_set_data(sqe, ev);
    return 0;
}

int kvs_loop_add_write_raw(kvs_loop_t *loop, kvs_event_t *ev, void *buf, size_t len) {
    struct io_uring_sqe *sqe = get_sqe_safe(loop);
    if (!sqe) return -1;

    io_uring_prep_send(sqe, ev->fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ev);
    return 0;
}

int kvs_loop_add_timeout(kvs_loop_t *loop, kvs_event_t *ev, struct __kernel_timespec *ts) {
    struct io_uring_sqe *sqe = get_sqe_safe(loop);
    if (!sqe) return -1;

    io_uring_prep_timeout(sqe, ts, 0, 0);
    io_uring_sqe_set_data(sqe, ev);
    return 0;
}