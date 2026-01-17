#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <liburing.h>

// --- Mock 区域：模拟你的 Server 和容器结构 ---

typedef struct kvs_config_s {
    char rdb_path[256];
} kvs_config_t;

struct kvs_server_s {
    kvs_config_t *conf;
    void *hash;  // 模拟句柄
    void *array; // 模拟句柄
};

// 手动补齐缺失的宏

#ifndef O_DIRECT
#define O_DIRECT 040000
#endif

void kvs_hash_filter(void *inst, int (*cb)(char*, int, char*, int, void*), void *arg);
void kvs_array_filter(void *inst, int (*cb)(char*, int, char*, int, void*), void *arg);

#define RDB_BUF_SIZE (4 * 1024 * 1024) // 4MB
#define MEM_ALIGN 4096                 // 4KB 对齐

typedef struct {
    char *data;
    size_t pos;
    int is_flushing;
} rdb_buffer_t;

struct _kvs_rdb_db_filter_arg {
    int fd;
    off_t file_offset;
    rdb_buffer_t *bufs;
    int cur_buf_idx;
    struct io_uring *ring;
    int ret;
    int db_type; // 当前正在遍历的数据库类型标记
};

// 对齐内存分配
static char* alloc_aligned_buffer(size_t size) {
    void *ptr = NULL;
    if (posix_memalign(&ptr, MEM_ALIGN, size) != 0) return NULL;
    return (char*)ptr;
}

// 等待异步 IO 完成
static int wait_for_io(struct io_uring *ring) {
    struct io_uring_cqe *cqe;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) return ret;
    int res = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    return res; // 返回写入的字节数，负数表示失败
}

/**
 * 刷盘逻辑
 * force_precise: 
 *   - 0: 异步刷整块 (RDB_BUF_SIZE)，由于是 4MB，天然满足 O_DIRECT 对齐要求
 *   - 1: 同步刷最后一块，处理非对齐字节，确保文件结尾不含 0 填充
 */
static int flush_buffer(struct _kvs_rdb_db_filter_arg *arg, int buf_idx, int force_precise) {
    rdb_buffer_t *buf = &arg->bufs[buf_idx];
    if (buf->pos == 0) return 0;

    if (force_precise) {
        // --- 处理文件末尾：解决 O_DIRECT 无法写入非对齐字节的问题 ---
        // 逻辑：补齐到 4K 写入，然后立即 ftruncate 截断文件
        size_t aligned_len = (buf->pos + MEM_ALIGN - 1) & ~(MEM_ALIGN - 1);
        
        // 注意：这里需要确保 buf->data 足够大能容纳补齐后的长度
        // 我们的 RDB_BUF_SIZE 是 4MB，是对齐的，所以安全
        if (aligned_len > buf->pos) {
            memset(buf->data + buf->pos, 0, aligned_len - buf->pos);
        }

        // 执行写入
        if (pwrite(arg->fd, buf->data, aligned_len, arg->file_offset) < 0) {
            return -1;
        }

        // 【关键】截断文件：这是去除末尾 0 填充的唯一工业级手段
        if (ftruncate(arg->fd, arg->file_offset + buf->pos) < 0) {
            return -1;
        }
        
        arg->file_offset += buf->pos;
        buf->pos = 0;
    } else {
        // --- 异步高速刷盘 (必须是对齐的整块) ---
        struct io_uring_sqe *sqe = io_uring_get_sqe(arg->ring);
        if (!sqe) return -1;

        // O_DIRECT 要求：offset 和 length 都必须是对齐的
        // 这里的 buf->pos 预期一定是 RDB_BUF_SIZE
        io_uring_prep_write(sqe, arg->fd, buf->data, buf->pos, arg->file_offset);
        io_uring_submit(arg->ring);

        buf->is_flushing = 1;
        arg->file_offset += buf->pos;
        // 注意：这里不重置 pos，等 wait_for_io 完成后再重置
    }
    return 0;
}

int _kvs_rdb_db_filter(char *key, int len_key, char *value, int len_val, void* arg) {
    struct _kvs_rdb_db_filter_arg* ctx = (struct _kvs_rdb_db_filter_arg*)arg;
    if (ctx->ret != 0) return -1;

    // 协议格式：[TYPE:1][KEY_LEN:4][KEY][VAL_LEN:4][VAL]
    size_t needed = 1 + 4 + len_key + 4 + len_val;
    rdb_buffer_t* cur_buf = &ctx->bufs[ctx->cur_buf_idx];

    // 如果空间不够，刷盘并切换
    if (cur_buf->pos + needed > RDB_BUF_SIZE) {
        // 1. 确保另一个 buffer 已经写完（背压控制）
        int prev_idx = 1 - ctx->cur_buf_idx;
        if (ctx->bufs[prev_idx].is_flushing) {
            if (wait_for_io(ctx->ring) < 0) {
                ctx->ret = -1;
                return -1;
            }
            ctx->bufs[prev_idx].is_flushing = 0;
            ctx->bufs[prev_idx].pos = 0;
        }

        // 2. 异步刷当前 Buffer (它是满的，4MB，对齐)
        if (flush_buffer(ctx, ctx->cur_buf_idx, 0) < 0) {
            ctx->ret = -1;
            return -1;
        }

        // 3. 切换
        ctx->cur_buf_idx = prev_idx;
        cur_buf = &ctx->bufs[ctx->cur_buf_idx];
        // 此时 cur_buf->pos 已经在步骤 1 中重置为 0
    }

    // 写入内存 Buffer
    char *p = cur_buf->data + cur_buf->pos;
    *p++ = (char)ctx->db_type;
    *(int*)p = len_key; p += 4;
    memcpy(p, key, len_key); p += len_key;
    *(int*)p = len_val; p += 4;
    memcpy(p, value, len_val); p += len_val;

    cur_buf->pos = (size_t)(p - cur_buf->data);
    return 0;
}

int kvs_rdb_child_process(struct kvs_server_s *server) {
    struct io_uring ring;
    int ret = 0;
    
    if (io_uring_queue_init(4, &ring, 0) < 0) return -1;

    char rdb_temp_filename[64];
    snprintf(rdb_temp_filename, sizeof(rdb_temp_filename), "dump_%d.rdb", getpid());

    // 使用 O_DIRECT 打开
    int fd = open(rdb_temp_filename, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd < 0) {
        io_uring_queue_exit(&ring);
        return -1;
    }

    rdb_buffer_t bufs[2] = {
        { alloc_aligned_buffer(RDB_BUF_SIZE), 0, 0 },
        { alloc_aligned_buffer(RDB_BUF_SIZE), 0, 0 }
    };

    struct _kvs_rdb_db_filter_arg arg = {
        .fd = fd, .file_offset = 0, .bufs = bufs, 
        .cur_buf_idx = 0, .ring = &ring, .ret = 0
    };

    // --- 遍历开始 ---
    arg.db_type = 1; // Hash
    kvs_hash_filter(server->hash, _kvs_rdb_db_filter, &arg);
    
    arg.db_type = 2; // Array
    kvs_array_filter(server->array, _kvs_rdb_db_filter, &arg);
    
    // --- 遍历结束 ---

    if (arg.ret == 0) {
        // 1. 等待最后一个异步任务完成
        int other_idx = 1 - arg.cur_buf_idx;
        if (bufs[other_idx].is_flushing) {
            wait_for_io(&ring);
        }
        
        // 2. 刷入当前最后残留的 buffer (非对齐刷盘 + ftruncate)
        if (flush_buffer(&arg, arg.cur_buf_idx, 1) < 0) {
            arg.ret = -1;
        }
    }

    // 清理
    fsync(fd);
    close(fd);
    if (arg.ret == 0) {
        rename(rdb_temp_filename, server->conf->rdb_path);
    } else {
        unlink(rdb_temp_filename);
    }

    free(bufs[0].data);
    free(bufs[1].data);
    io_uring_queue_exit(&ring);
    return arg.ret;
}



// 模拟 Hash 遍历：触发两条数据
void kvs_hash_filter(void *inst, int (*cb)(char*, int, char*, int, void*), void *arg) {
    printf("[Mock] Filtering Hash Table...\n");
    cb("key_hash_1", 10, "val_1", 5, arg);
    cb("key_hash_2", 10, "val_2", 5, arg);
}

// 模拟 Array 遍历：触发一条较长的数据，测试跨 Buffer 边界（如果设置小一点的话）
void kvs_array_filter(void *inst, int (*cb)(char*, int, char*, int, void*), void *arg) {
    printf("[Mock] Filtering Array...\n");
    cb("key_array_long", 14, "this_is_a_longer_value_to_test_padding", 38, arg);
}

// --- 验证函数：检查生成的 RDB 文件内容 ---

void verify_rdb(const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) {
        perror("Stat failed");
        return;
    }

    printf("\n=== RDB Verification ===\n");
    printf("File Path: %s\n", path);
    printf("File Size: %ld bytes\n", st.st_size);

    // 理论计算大小：
    // 每条数据：Type(1) + KeyLen(4) + Key + ValLen(4) + Val
    // Hash 1: 1 + 4 + 10 + 4 + 5 = 24
    // Hash 2: 1 + 4 + 10 + 4 + 5 = 24
    // Array 1: 1 + 4 + 14 + 4 + 38 = 61
    // 总计: 24 + 24 + 61 = 109 字节
    long expected_size = 109;

    if (st.st_size == expected_size) {
        printf("RESULT: SUCCESS! File size matches exactly (No trailing zeros).\n");
    } else {
        printf("RESULT: FAILED! Expected %ld, got %ld. (O_DIRECT padding issue?)\n", expected_size, st.st_size);
    }

    // 简单读取并打印前几个字节验证格式
    FILE *fp = fopen(path, "rb");
    unsigned char buf[16];
    fread(buf, 1, 16, fp);
    printf("First 16 bytes (Hex): ");
    for(int i=0; i<16; i++) printf("%02x ", buf[i]);
    printf("\n");
    fclose(fp);
}

// --- Main Test ---

int main() {
    printf("Starting RDB io_uring Test...\n");

    // 1. 准备配置和 Server
    kvs_config_t conf;
    strcpy(conf.rdb_path, "./test_dump.rdb");

    struct kvs_server_s server;
    server.conf = &conf;
    server.hash = (void*)0x123;  // 随便给个地址，Mock 不会解引用它
    server.array = (void*)0x456;

    // 2. 执行 RDB 保存逻辑
    // 注意：如果是生产环境，这里应该在 fork() 出的子进程跑
    // 为了方便单体测试，我们直接跑逻辑
    int ret = kvs_rdb_child_process(&server);

    if (ret == 0) {
        printf("kvs_rdb_child_process returned success.\n");
        // 3. 验证
        verify_rdb(conf.rdb_path);
    } else {
        printf("kvs_rdb_child_process failed with code: %d\n", ret);
    }

    return 0;
}