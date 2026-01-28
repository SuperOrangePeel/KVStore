#define _GNU_SOURCE
#include "kvs_aof_engine.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <errno.h>



#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h>
  #define cpu_pause() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
  #define cpu_pause() __asm__ __volatile__("yield" ::: "memory")
#else
  #define cpu_pause() __sync_synchronize()
#endif

// // 辅助函数：计算 min
// static inline size_t min_sz(size_t a, size_t b) { return a < b ? a : b; }

static void *aof_background_worker(void *arg) {
    struct kvs_aof_engine *engine = (struct kvs_aof_engine *)arg;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    uint64_t efd_val; 
    struct iovec iov[2];
    
    // 标记：是否需要 Arm (挂载) EventFD
    // 初始为 1，以便第一次进入时挂载
    int need_arm_eventfd = 1; 

    while (!engine->stop_flag) {
        
        // ==========================================================
        // 阶段 1: 检查是否有数据需要处理
        // ==========================================================
        uint64_t head = atomic_load_explicit(&engine->head, memory_order_acquire);
        uint64_t tail = atomic_load_explicit(&engine->tail, memory_order_acquire);

        // 如果有数据，直接去处理，不要睡觉！
        // 如果没数据，且还没挂载 eventfd，则去挂载并睡觉
        if (head == tail) {
            
            if (need_arm_eventfd) {
                sqe = io_uring_get_sqe(&engine->ring);
                if (!sqe) {
                    // 极罕见：SQ 满了，这不应该发生，如果发生了，忙等一下重试
                    cpu_pause();
                    continue; 
                }
                
                io_uring_prep_read(sqe, engine->event_fd, &efd_val, sizeof(uint64_t), 0);
                io_uring_sqe_set_data(sqe, (void*)1); // Tag 1: EventFD
                io_uring_submit(&engine->ring);
                
                need_arm_eventfd = 0; // 已经挂载了
            }

            // 阻塞等待被唤醒
            // 注意：这里我们只等待 1 个事件。
            // 如果是 eventfd 响了，ret > 0。
            // 此时，因为我们处于 head==tail 分支，理论上没有 pending 的 write。
            int ret = io_uring_wait_cqe(&engine->ring, &cqe);
            if (ret < 0) {
                LOG_ERROR("Wait CQE failed: %d", ret);
                sleep(1); // 避免死循环狂刷日志
                continue;
            }
            
            // 处理唤醒事件
            if ((uint64_t)io_uring_cqe_get_data(cqe) == 1) {
                // 是 EventFD 唤醒的
                need_arm_eventfd = 1; // 消费掉了，下次空闲时需要重新挂载
            }
            io_uring_cqe_seen(&engine->ring, cqe);
            
            // 醒来后，直接回到循环头部重新检查 head/tail
            continue;
        }

        // ==========================================================
        // 阶段 2: 消费数据 (Consumer)
        // ==========================================================
        // 代码执行到这里，意味着 head != tail

        // 计算数据长度
        size_t data_len;
        if (tail > head) {
            data_len = tail - head;
            iov[0].iov_base = engine->buffer + head;
            iov[0].iov_len = data_len;
            
            sqe = io_uring_get_sqe(&engine->ring);
            if(sqe) io_uring_prep_writev(sqe, engine->aof_fd, iov, 1, engine->file_offset);
        } else {
            size_t first = AOF_RING_BUFFER_SIZE - head;
            size_t second = tail;
            data_len = first + second;
            
            iov[0].iov_base = engine->buffer + head;
            iov[0].iov_len = first;
            iov[1].iov_base = engine->buffer;
            iov[1].iov_len = second;

            sqe = io_uring_get_sqe(&engine->ring);
            if(sqe) io_uring_prep_writev(sqe, engine->aof_fd, iov, 2, engine->file_offset);
        }

        if (!sqe) {
            // SQE 满了？处理一下 pending 的请求
            io_uring_submit(&engine->ring);
            continue; // 重试
        }

        io_uring_sqe_set_data(sqe, (void*)2); // Tag 2: Write
        
        // 提交并等待写完 (同步模式，保证顺序)
        io_uring_submit(&engine->ring);
        
        // 等待这个写操作完成
        int ret = io_uring_wait_cqe(&engine->ring, &cqe);
        if (ret < 0) {
             LOG_ERROR("Wait Write CQE failed: %d", ret);
             continue; // 重试
        }

        // 检查是否是我们刚才提交的写
        if ((uint64_t)io_uring_cqe_get_data(cqe) == 2) {
            if (cqe->res > 0) {
                engine->file_offset += cqe->res;
                
                // 更新 head
                uint64_t new_head = (head + cqe->res) % AOF_RING_BUFFER_SIZE;
                atomic_store_explicit(&engine->head, new_head, memory_order_release);
            } else {
                LOG_ERROR("Write Error: %d", cqe->res);
            }
        } else if ((uint64_t)io_uring_cqe_get_data(cqe) == 1) {
            // 极端情况：我们在写的时候，EventFD 也恰好就绪了（或者之前的没消费掉）
            // 标记一下，下次空闲时需要重新挂载
            need_arm_eventfd = 1;
        }

        io_uring_cqe_seen(&engine->ring, cqe);

        // --- 策略处理 (Fsync) ---
        // (保持你之前的 Fsync 逻辑不变)
        if (engine->fsync_policy == AOF_FSYNC_ALWAYS && data_len > 0) { 
            fsync(engine->aof_fd);
        } else if (engine->fsync_policy == AOF_FSYNC_EVERYSEC) {
            time_t now = time(NULL);
            if (now - engine->last_fsync_time >= 1) {
                fsync(engine->aof_fd);
                engine->last_fsync_time = now;
            }
        }
    }
    return NULL;
}

// ... init 和 destroy 函数保持不变 ...

// --- 修正后的 Append 函数 ---
int kvs_aof_append(struct kvs_aof_engine *engine, const char *data, size_t len) {
    if (len > AOF_RING_BUFFER_SIZE / 2) return -1;

    uint64_t tail, head;
    size_t used, free_space;
    int spin_count = 0;

    while (1) {
        tail = atomic_load_explicit(&engine->tail, memory_order_relaxed);
        head = atomic_load_explicit(&engine->head, memory_order_acquire);
        
        if (tail >= head) used = tail - head;
        else used = AOF_RING_BUFFER_SIZE - head + tail;
        
        free_space = AOF_RING_BUFFER_SIZE - used - 1;

        if (free_space >= len) break;

        // --- Backpressure 优化 ---
        // 如果满了，不要死锁 CPU，适当出让
        spin_count++;
        if (spin_count < 1000) {
            cpu_pause(); // 短时间自旋
        } else {
            sched_yield(); // 长时间等待则出让 CPU，防止饿死后台线程
            spin_count = 0;
        }
        
        // 如果你实在不想卡死主线程，可以这里 return -1 (EAGAIN)
        // 但为了数据安全，通常选择阻塞
    }

    size_t contiguous = AOF_RING_BUFFER_SIZE - tail;
    if (len <= contiguous) {
        memcpy(engine->buffer + tail, data, len);
        tail = (tail + len) % AOF_RING_BUFFER_SIZE; // 这里直接算出新 tail
    } else {
        memcpy(engine->buffer + tail, data, contiguous);
        memcpy(engine->buffer, data + contiguous, len - contiguous);
        tail = len - contiguous;
    }

    atomic_store_explicit(&engine->tail, tail, memory_order_release);

    // 唤醒后台
    uint64_t u = 1;
    // 只有当后台可能睡着时才写，但写也没关系，开销很小
    if (write(engine->event_fd, &u, sizeof(uint64_t)) < 0) {
        if (errno != EAGAIN) {
             LOG_ERROR("Failed to write to eventfd: %s", strerror(errno));
        }
    }

    return 0;
}


// --- API 实现 ---

int kvs_aof_init(struct kvs_aof_engine *engine, const char *filename, int fsync_policy) {
    memset(engine, 0, sizeof(*engine));

    // 1. 分配对齐的内存 (Ring Buffer)
    // 使用 posix_memalign 对齐到 4KB，利于 O_DIRECT (如果你以后要开)
    if (posix_memalign((void**)&engine->buffer, 4096, AOF_RING_BUFFER_SIZE) != 0) {
        return -1;
    }

    // 2. 打开文件
    // O_DIRECT 暂时不开，等你整个链路稳定后再开，避免对齐地狱
    engine->aof_fd = open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (engine->aof_fd < 0) return -1;
    
    // 获取当前文件大小，作为 offset 起点
    struct stat st;
    if (fstat(engine->aof_fd, &st) == 0) {
        engine->file_offset = st.st_size;
    }

    // 3. 创建 EventFD
    engine->event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (engine->event_fd < 0) return -1;


    engine->fsync_policy = fsync_policy; // 默认每秒刷盘
    memset(&engine->last_fsync_time, 0, sizeof(engine->last_fsync_time));

    // 4. 初始化 io_uring
    if (io_uring_queue_init(64, &engine->ring, 0) < 0) return -1;

    // 5. 启动后台线程
    if (pthread_create(&engine->thread_id, NULL, aof_background_worker, engine) != 0) {
        return -1;
    }

    atomic_init(&engine->head, 0);
    atomic_init(&engine->tail, 0);
    
    return 0;
}

#if 0
int kvs_aof_append(struct kvs_aof_engine *engine, const char *data, size_t len) {
    // 0. 参数校验
    if (len > AOF_RING_BUFFER_SIZE / 2) {
        // 单条数据过大，RingBuffer 撑不住，建议直接拒绝或特殊处理
        LOG_ERROR("Data too large for AOF buffer");
        return -1;
    }

    // 1. 申请空间 (Claim Space)
    uint64_t tail, head, next_tail;
    size_t used, free_space;

    // Backpressure 自旋循环
    while (1) {
        tail = atomic_load_explicit(&engine->tail, memory_order_relaxed); // 只在当前线程改，relaxed 即可
        head = atomic_load_explicit(&engine->head, memory_order_acquire); // 需要看到消费者的最新进度
        
        // 计算已用空间
        if (tail >= head) {
            used = tail - head;
        } else {
            used = AOF_RING_BUFFER_SIZE - head + tail;
        }
        
        // 留 1 字节避免歧义
        free_space = AOF_RING_BUFFER_SIZE - used - 1;

        if (free_space >= len) {
            break; // 空间够了，跳出循环
        }

        // 空间不够：反压！
        // CPU Pause 指令，告诉 CPU 我在忙等，降低功耗并提升超线程性能
        cpu_pause();
    }

    // 2. 写入数据 (memcpy)
    // tail 是当前写入位置
    size_t contiguous = AOF_RING_BUFFER_SIZE - tail;
    
    if (len <= contiguous) {
        // 一次写完
        memcpy(engine->buffer + tail, data, len);
        next_tail = (tail + len) % AOF_RING_BUFFER_SIZE;
    } else {
        // 分两段写
        memcpy(engine->buffer + tail, data, contiguous);
        memcpy(engine->buffer, data + contiguous, len - contiguous);
        next_tail = len - contiguous;
    }

    // 3. 提交 (Commit)
    // 使用 Release 语义，保证数据写入完成后，tail 的更新才对 Consumer 可见
    atomic_store_explicit(&engine->tail, next_tail, memory_order_release);

    // 4. 通知后台线程 (Notify)
    // 优化：其实不需要每次都写 eventfd。只有当后台线程可能睡着了才写。
    // 但为了代码健壮性，这里先每次都写。由于 eventfd 是聚合的，开销极小。
    if (head == tail) {
        uint64_t u = 1;
        write(engine->event_fd, &u, sizeof(uint64_t));
    }

    return 0;
}
#endif

void kvs_aof_destroy(struct kvs_aof_engine *engine) {
    engine->stop_flag = 1;
    
    // 唤醒一下让他退出
    uint64_t u = 1;
    write(engine->event_fd, &u, sizeof(uint64_t));
    
    pthread_join(engine->thread_id, NULL);
    io_uring_queue_exit(&engine->ring);
    close(engine->event_fd);
    close(engine->aof_fd);
    free(engine->buffer);
}