#ifndef KVS_AOF_ENGINE_H
#define KVS_AOF_ENGINE_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>
#include <liburing.h>



#define AOF_FSYNC_NO 0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2


// 缓冲区大小，建议 16MB ~ 64MB，必须是 2 的幂次方方便位运算优化（这里暂用取模保证通用性）
#define AOF_RING_BUFFER_SIZE (16 * 1024 * 1024) 

// 避免伪共享的宏
#define CACHE_LINE_SIZE 64
#define __cache_aligned __attribute__((aligned(CACHE_LINE_SIZE)))

struct kvs_aof_engine {
    // --- 主线程与后台线程共享区域 ---
    
    // 环形缓冲区内存基地址
    char *buffer; 
    
    // 生产者位置 (Main Thread Write)
    // 使用 atomic 保证内存可见性
    _Atomic uint64_t tail __cache_aligned;

    // 消费者位置 (AOF Thread Read)
    _Atomic uint64_t head __cache_aligned;

    // --- 系统资源 ---
    int aof_fd;           // 目标文件 FD
    int event_fd;         // 用于通知的 eventfd
    uint64_t file_offset; // 当前文件绝对偏移量
    
    pthread_t thread_id;
    struct io_uring ring;
    
    volatile int stop_flag;

    int fsync_policy; // 0: never, 1: always, 2: everysec

    time_t last_fsync_time;
};

/* API 接口 */

// 初始化 AOF 引擎，启动后台线程
int kvs_aof_init(struct kvs_aof_engine *engine, const char *filename, int fsync_policy);

// 销毁引擎，刷盘并关闭
void kvs_aof_destroy(struct kvs_aof_engine *engine);

// 主线程调用：追加数据 (极速，无锁，无 syscall)
// 如果缓冲区满，会短时间自旋等待 (Backpressure)
int kvs_aof_append(struct kvs_aof_engine *engine, const char *data, size_t len);

#endif // KVS_AOF_ENGINE_H