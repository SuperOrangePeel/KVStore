#include "kvs_persistence.h"
#include "common.h"
#include "kvs_event_loop.h"
#include "logger.h"
#include "kvs_aof_engine.h"

#include <bits/types/struct_iovec.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>


//struct kvs_pers_context_s kvs_aof_ctx = {0}; // global context

#if 0
void _kvs_persistence_fsync_cb(void *ctx, int res, int flags) {
    struct kvs_pers_context_s *pers_ctx = (struct kvs_pers_context_s *)ctx;
    if(pers_ctx == NULL) {
        assert(0);
        return;
    }
    if(res < 0) {
        LOG_ERROR("AOF fsync failed: %s", strerror(-res));
        return;
    }
    //LOG_DEBUG("AOF fsync completed successfully");
    return;
}


#include <string.h>
#include <errno.h>

// 返回 0 表示成功，-1 表示空间不足
static int _kvs_persistence_write_data_to_ring_buffer(struct kvs_pers_context_s *ctx, char* data, size_t data_len) {
    // 1. 计算当前已用空间
    // head == tail 表示空
    // (tail + 1) % size == head 表示满 (保留一个字节区分空满)
    int head = ctx->write_offset_head;
    int tail = ctx->write_offset_tail;
    int capacity = AOF_MAX_BUFFER_SIZE;
    
    int used = (tail >= head) ? (tail - head) : (capacity - head + tail);
    int free_space = capacity - used - 1; // 留一个字节

    if (data_len > (size_t)free_space) {
        // 空间不足，强行进行同步fwrite以及fsync操作

        return -1; 
    }

    // 2. 写入数据
    int first_part = capacity - tail;
    if (data_len <= (size_t)first_part) {
        // 不需要回绕，直接拷贝
        memcpy(ctx->write_buffer + tail, data, data_len);
        ctx->write_offset_tail = (tail + data_len) % capacity;
    } else {
        // 发生回绕，分两段拷贝
        memcpy(ctx->write_buffer + tail, data, first_part);
        memcpy(ctx->write_buffer, data + first_part, data_len - first_part);
        ctx->write_offset_tail = data_len - first_part;
    }

    return 0;
}

// 调用此函数准备 io_uring 任务
void kvs_aof_flush_consume(struct kvs_pers_context_s *ctx) {
    int head = ctx->write_offset_head;
    int tail = ctx->write_offset_tail;
    int capacity = AOF_MAX_BUFFER_SIZE;

    if (head == tail) return; // 没数据

    if(ctx->is_writing_aof) {
        // 正在写入中，先不处理
        return;
    }

    //struct io_uring_sqe *sqe = io_uring_get_sqe(ctx->loop);
    
    if (head < tail) {
        // 物理连续：直接用 prep_write
        int len = tail - head;

        kvs_loop_add_write(ctx->loop, &ctx->aof_fwrite_ev, ctx->write_buffer + head, len, ctx->global_offset);
        //io_uring_prep_write(sqe, ctx->aof_fd, ctx->write_buffer + head, len, ctx->global_offset);
        // 在 context 记录这次发了多少，回调里再动 head
        ctx->last_sent_len = len; 
    } else {
        // 物理不连续：跨越了边界！使用 writev (向量写)
        // 这样不需要 memmove，内核会帮我们把两段数据拼起来写入文件
        int len1 = capacity - head;
        int len2 = tail;
        
        // 这两个 iovec 必须在异步期间保持有效，建议放在 ctx 里
        ctx->aof_iovecs[0].iov_base = ctx->write_buffer + head;
        ctx->aof_iovecs[0].iov_len = len1;
        ctx->aof_iovecs[1].iov_base = ctx->write_buffer;
        ctx->aof_iovecs[1].iov_len = len2;
        
        kvs_loop_add_writev(ctx->loop, &ctx->aof_fwrite_ev, ctx->aof_iovecs, 2, ctx->global_offset);
        //io_uring_prep_writev(sqe, ctx->aof_fd, ctx->aof_iovecs, 2, ctx->global_offset);
        ctx->last_sent_len = len1 + len2;
    }
    
    //sqe->user_data = (uint64_t)ctx;
    //io_uring_submit(ring);
}

void _kvs_persistence_fwrite_cb(void *ctx, int res, int flags) {
    struct kvs_pers_context_s *pers_ctx = (struct kvs_pers_context_s *)ctx;
    if(pers_ctx == NULL) {
        assert(0);
        return;
    }
    if(res < 0) {
        LOG_ERROR("AOF fwrite failed: %s", strerror(-res));
        return;
    }
    if (res >= 0) {
        // res 是实际写入的字节数
        pers_ctx->write_offset_head = (pers_ctx->write_offset_head + res) % pers_ctx->write_buf_size;
        pers_ctx->global_offset += res;
    }
    pers_ctx->is_writing_aof = 0;

    if(pers_ctx->write_offset_head != pers_ctx->write_offset_tail) {
        // 还有数据未写完，继续写
        kvs_aof_flush_consume(pers_ctx);
    }

    return;
}


void _kvs_persistence_timer_cb(void *ctx, int res, int flags) {
    struct kvs_pers_context_s *pers_ctx = (struct kvs_pers_context_s *)ctx;
    if(pers_ctx == NULL) {
        assert(0);
        return;
    }
    
    if(res < 0) {
        LOG_ERROR("persistence timer event error: %s", strerror(-res));
        return;
    }

    // perform periodic tasks
    if(pers_ctx->aof_enabled && pers_ctx->aof_fsync_policy == KVS_AOF_EVERY_SEC) {
        // flush AOF buffer to disk
        kvs_loop_add_fsync(pers_ctx->loop, &pers_ctx->aof_fsync_ev, pers_ctx->aof_fd);
    }

    // re-add timer
    kvs_loop_add_timeout(pers_ctx->loop, &pers_ctx->timer_ev, &pers_ctx->ts);
}

#endif

struct kvs_pers_context_s * kvs_persistence_create(struct kvs_pers_config_s *config) {
    if(config == NULL) {
        assert(0);
        return NULL;
    }

    char* aof_filename = config->aof_filename ? config->aof_filename : "kvstore.aof";
    char* rdb_filename = config->rdb_filename ? config->rdb_filename : "dump.rdb";

    struct kvs_pers_context_s *ctx = (struct kvs_pers_context_s *)kvs_malloc(sizeof(struct kvs_pers_context_s));
    memset(ctx, 0, sizeof(struct kvs_pers_context_s));
    ctx->aof_fsync_policy = config->aof_fsync_policy;
    ctx->loop = config->loop;

    ctx->aof_enabled = config->aof_enabled; // 1: enabled, 0: disabled


    ctx->aof_filename = (char*)kvs_malloc(strlen(aof_filename) + 1);
    if(ctx->aof_filename == NULL) {
        assert(0);
        kvs_free(ctx, sizeof(struct kvs_pers_context_s));
        LOG_FATAL("Failed to allocate memory for aof_filename");
        assert(0); 
        return NULL;
    }
    strncpy(ctx->aof_filename, aof_filename, strlen(aof_filename) + 1);

    // ctx->aof_fd = open(aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    // if(ctx->aof_fd == -1) {
    //     return -1;
    // }
    ctx->aof_fd = -1; // not opened yet
    //ctx->aof_fd = open(ctx->aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    gettimeofday(&ctx->last_fsync_time, NULL);
    ctx->buffer_size = AOF_MAX_BUFFER_SIZE;
    ctx->write_offset = 0;

    ctx->rdb_filename = (char*)kvs_malloc(strlen(rdb_filename) + 1);
    if(ctx->rdb_filename == NULL) {
        assert(0);
        kvs_free(ctx->aof_filename, strlen(ctx->aof_filename) + 1);
        ctx->aof_filename = NULL;
        return NULL;
    }
    strncpy(ctx->rdb_filename, rdb_filename, strlen(rdb_filename) + 1);
    if(ctx->rdb_filename == NULL) {
        assert(0);
        kvs_free(ctx->aof_filename, strlen(ctx->aof_filename) + 1);
        ctx->aof_filename = NULL;
        kvs_free(ctx, sizeof(struct kvs_pers_context_s));
        return NULL;
    }

    // 初始化aof engine
    kvs_aof_init(&ctx->aof_engine, ctx->aof_filename, ctx->aof_fsync_policy);

    // ctx->aof_fwrite_ev.fd = ctx->aof_fd;
    // ctx->aof_fwrite_ev.handler = _kvs_persistence_fwrite_cb; // to be set later
    // ctx->aof_fwrite_ev.ctx = (void *)ctx;

    // ctx->is_writing_aof = 0;

    // ctx->aof_fsync_ev.fd = ctx->aof_fd;
    // ctx->aof_fsync_ev.handler = _kvs_persistence_fsync_cb; // to be set later
    // ctx->aof_fsync_ev.ctx = (void *)ctx;

    // if(ctx->aof_enabled && ctx->aof_fsync_policy == KVS_AOF_EVERY_SEC) {
    //     // setup timer event for periodic fsync
    //     ctx->timer_ev.fd = -1; // not used
    //     ctx->timer_ev.handler = _kvs_persistence_timer_cb;
    //     ctx->timer_ev.ctx = (void *)ctx;
    //     ctx->ts.tv_sec = AOF_FSYNC_INTERVAL_MS / 1000;
    //     ctx->ts.tv_nsec = (AOF_FSYNC_INTERVAL_MS % 1000) * 1000000;

    //     kvs_loop_add_timeout(ctx->loop, &ctx->timer_ev, &ctx->ts);
    // }

    

    //ctx->rdb_fp = NULL;
    ctx->rdb_size = 0;

    return ctx;
}

int kvs_persistence_destroy(struct kvs_pers_context_s *ctx) {
    if(ctx->aof_fd != -1) {
        close(ctx->aof_fd);
        ctx->aof_fd = -1;
    }
    kvs_free(ctx->aof_filename, strlen(ctx->aof_filename) + 1);
    ctx->aof_filename = NULL;
    kvs_free(ctx->rdb_filename, strlen(ctx->rdb_filename) + 1);
    ctx->rdb_filename = NULL;
    kvs_free(ctx, sizeof(struct kvs_pers_context_s));

    kvs_aof_destroy(&ctx->aof_engine);
    return 0;
}

#if 0
int kvs_persistence_write_aof_iouring(struct kvs_pers_context_s *ctx, char *data, size_t data_len) { 
    if(ctx == NULL || data == NULL || data_len == 0 || ctx->aof_filename == NULL) {
        return -1;
    }
    if(ctx->aof_fd == -1) {
        ctx->aof_fd = open(ctx->aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (ctx->aof_fd == -1) {
            return -1;
        }
    }
    
    if(ctx->aof_enabled == 0) {
        // AOF disabled
        return 0;
    }

    int ret = _kvs_persistence_write_data_to_ring_buffer(ctx, data, data_len);
    if(ret == -1) {
        // buffer space not enough, force flush

    } 

    if(ctx->is_writing_aof == 0) {
        ctx->is_writing_aof = 1;
        kvs_aof_flush_consume(ctx);
    }

    return 0;
}
#endif

int kvs_persistence_write_aof(struct kvs_pers_context_s *ctx, char* data, size_t data_len) {
    if(ctx == NULL || data == NULL || data_len == 0 || ctx->aof_filename == NULL) {
        return -1;
    }
    // if(ctx->aof_fd == -1) {
    //     ctx->aof_fd = open(ctx->aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    //     if (ctx->aof_fd == -1) {
    //         return -1;
    //     }
    // }
    //if(ctx->aof_fd == -1) return -1;
    if(ctx->aof_enabled == 0) {
        // AOF disabled
        return 0;
    }

    return kvs_aof_append(&ctx->aof_engine, data, data_len);

#if 0
    if(ctx->write_offset + data_len > ctx->buffer_size) {
        ssize_t bytes_written = write(ctx->aof_fd, ctx->write_buffer, ctx->write_offset);
        if(bytes_written != ctx->write_offset) {
            return -1;
        }
        ctx->write_offset = 0;
    }
    memcpy(ctx->write_buffer + ctx->write_offset, data, data_len);
    ctx->write_offset += data_len;

    // check if need to fsync
    struct timeval current_time;
    gettimeofday(&current_time, NULL);
    if(TIME_SUB_MS(current_time, ctx->last_fsync_time) >= AOF_FSYNC_INTERVAL_MS) {
        if(ctx->write_offset > 0) {

            ssize_t bytes_written = write(ctx->aof_fd, ctx->write_buffer, ctx->write_offset);
            if(bytes_written != ctx->write_offset) {
                printf("AOF write error: %s\n", strerror(errno));
                return -1;
            }
            ctx->write_offset = 0;
        }
        ctx->last_fsync_time = current_time;
        fsync(ctx->aof_fd);   
    }
    // close(ctx->aof_fd);
#endif
}

int kvs_persistence_flush_aof(struct kvs_pers_context_s *ctx) {
    if(ctx == NULL) {
        return -1;
    }
    if(ctx->aof_fd == -1) {
        // not opened yet
        // assert(0);
        return -1;
    }
    if(ctx->write_offset > 0) {
        ssize_t bytes_written = write(ctx->aof_fd, ctx->write_buffer, ctx->write_offset);
        if(bytes_written != ctx->write_offset) {
            printf("AOF flush write error: %s\n", strerror(errno));
            return -1;
        }
        ctx->write_offset = 0;
    }
    fsync(ctx->aof_fd);
    return 0;
}


int kvs_persistence_mmap_load_aof(struct kvs_pers_context_s *aof_ctx, kvs_aof_data_parser_pt data_parser, void* arg) {
    if(!aof_ctx || !data_parser) return -1;

    int aof_fd = open(aof_ctx->aof_filename, O_RDWR);
    if (aof_fd == -1) return -1;

    struct stat st;
    if (fstat(aof_fd, &st) == -1) {
        close(aof_fd);
        return -1;
    }
    size_t file_size = st.st_size;
    if (file_size == 0) {
        close(aof_fd);
        return 0; // empty file
    }

    char* mapped = (char*)mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, aof_fd, 0);
    if (mapped == MAP_FAILED) {
        close(aof_fd);
        return -1;
    }

    size_t offset = 0;
    while (offset < file_size) {
        int parsed_len = 0;
        int ret = data_parser(mapped + offset, file_size - offset, &parsed_len, arg);
        if (ret < 0) {
            // error or need more data
            break;
        }
        offset += parsed_len;
    }


    // crash recovery: truncate corrupted tail
    if (offset < file_size) {
        printf("AOF tail corrupted, truncating from %zu to %zu\n", file_size, offset);
        if (ftruncate(aof_fd, offset) == -1) {
            LOG_ERROR("ftruncate error, %s", strerror(errno));
            munmap(mapped, file_size);
            close(aof_fd);
            return -1;
        }
    }

    munmap(mapped, file_size);
    close(aof_fd);
    return 0;
}

#define LOAD_AOF_FILE_BUFFER_SIZE 1024 // 16384 // 16 * 1024

/*
* @param buffer: suggested size at least 4096 bytes or 64k bytes
* @return number of bytes read, -1 on error
*/
int kvs_persistence_load_aof(struct kvs_pers_context_s *aof_ctx, kvs_aof_data_parser_pt data_parser, void* arg) {
    if(!aof_ctx || !data_parser) return -1;

    int aof_fd = open(aof_ctx->aof_filename, O_RDWR);
    if (aof_fd == -1) return -1;

    char f_buf[LOAD_AOF_FILE_BUFFER_SIZE];
    int leftover = 0;      // 上次剩下的字节数
    off_t total_valid_file_pos = 0; // 累计处理成功的物理位置

    while (1) {
        // 1. 尽量填满缓冲区：从上次剩下的地方开始读
        ssize_t n_read = read(aof_fd, f_buf + leftover, LOAD_AOF_FILE_BUFFER_SIZE - leftover);
        if (n_read < 0) {
            perror("read error");
            close(aof_fd);
            return -1;
        }

        int total_in_buf = leftover + (int)n_read;
        if (total_in_buf == 0) break; // 文件读完了

        int p_idx = 0; // 当前缓冲区解析指针
        
        // 2. 内部循环解析：只要缓冲区里还有数据
        int parsed_len;
        while (1) {
            //int cmd_len = 0; // 这是一个输出参数，由解析器告诉我们这条指令有多长
            
            // 注意：这里传入解析器的必须是起始位置和当前剩余的总长度
            int ret = data_parser(f_buf + p_idx, total_in_buf - p_idx, &parsed_len, arg);

            if (ret >= 0) { 
                // 解析并执行成功
                p_idx += parsed_len;
                total_valid_file_pos += parsed_len;
            } else {
                // ret < 0 说明数据不完整（半条指令）或者格式错误
                break; 
            }
        }

        // 3. 处理残留数据
        leftover = total_in_buf - p_idx;
        if (leftover > 0) {
            memmove(f_buf, f_buf + p_idx, leftover);
        }

        // 4. 如果 read 返回 0，说明文件到底了, 而且解析最后一次还有残留数据，说明格式错误
        if (n_read == 0) break;
    }

    // 5. 崩溃修复：如果文件读完了还剩下 leftover > 0，说明末尾有垃圾
    struct stat st;
    fstat(aof_fd, &st);
    if (st.st_size > total_valid_file_pos) {
        printf("AOF tail corrupted, truncating from %lld to %lld\n", 
                (long long)st.st_size, (long long)total_valid_file_pos);
        if(-1 == ftruncate(aof_fd, total_valid_file_pos)) {
            perror("ftruncate error");
            close(aof_fd);
            return -1;
        }
    }

    close(aof_fd);
    aof_ctx->aof_fd = -1; // reset fd to force reopen next time
    return 0;
}



int _kvs_rdb_item_writer(char *data, int len, void* privdata) {
    FILE *fp = *(FILE **)privdata;

    return fwrite(data, 1, len, fp);
}


int kvs_persistence_save_rdb(struct kvs_pers_context_s *ctx, kvs_storage_iterator_pt iterator, void* iter_arg) {
    // if(ctx->rdb_fp != NULL) {
    //     fclose(ctx->rdb_fp);
    //     ctx->rdb_fp = NULL;
    // }
    char tmp_filename[64];
    snprintf(tmp_filename, sizeof(tmp_filename), "%s_%d.tmp", ctx->rdb_filename, (int)getpid());
    FILE* temp_rdb_fp = fopen(tmp_filename, "wb");
    if(temp_rdb_fp == NULL) {
        return -1;
    }

    iterator(iter_arg, _kvs_rdb_item_writer, (void*)&temp_rdb_fp);


    fflush(temp_rdb_fp);
    fsync(fileno(temp_rdb_fp));
    fclose(temp_rdb_fp);

    //kvs_hash_filter(&global_hash, _rdb_callback, &arg);
    assert(ctx->rdb_filename != NULL);
    if(rename(tmp_filename, ctx->rdb_filename) != 0) {
        perror("rename RDB file");
        return -1;
    }
    // ctx->rdb_fp = NULL;
    return 0;
}

int _kvs_perisistence_load_rdb_filename_mmap(char* rdb_filename, kvs_rdb_item_loader_pt data_loader, void* arg) {
    if(rdb_filename == NULL || data_loader == NULL) {
        return -1;
    }
    FILE* fp = fopen(rdb_filename, "rb");
    if(fp == NULL) {
        return -1;
    }
    struct stat st;
    fstat(fileno(fp), &st);
    size_t file_size = st.st_size;
    if(file_size == 0) {
        fclose(fp);
        return 0; // empty file
    }
    char* mapped = (char*)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fileno(fp), 0);
    if(mapped == MAP_FAILED) {
        fclose(fp);
        return -1;
    }
    size_t offset = 0;
    while(offset < file_size) {
        int processed_bytes = data_loader(mapped + offset, file_size - offset, arg);
        if(processed_bytes < 0) {
            munmap(mapped, file_size);
            fclose(fp);
            return -2;
        }
        offset += processed_bytes;
    }
    munmap(mapped, file_size);
    fclose(fp);
    return 0;
}

int _kvs_persistence_load_rdb_filename(char* rdb_filename, kvs_rdb_item_loader_pt data_loader, void* arg) {
    if(rdb_filename == NULL || data_loader == NULL) {
        return -1;
    }
    FILE* fp = fopen(rdb_filename, "rb");
    if(fp == NULL) {
        return -1;
    }
    int rdb_buf_size = 4096;
    char rdb_buf[rdb_buf_size]; // 4KB
    int rdb_buf_idx = 0;
    while(1) {
        size_t n_read = fread(rdb_buf + rdb_buf_idx, 1, rdb_buf_size - rdb_buf_idx, fp);
        rdb_buf_idx += n_read;
        int processed_bytes = data_loader(rdb_buf, rdb_buf_idx, arg);
        if(processed_bytes < 0) {
            fclose(fp);
            return -2;
        }
        if(processed_bytes < rdb_buf_idx) {
            // move unprocessed data to the beginning
            memmove(rdb_buf, rdb_buf + processed_bytes, rdb_buf_idx - processed_bytes);
        }
        rdb_buf_idx -= processed_bytes;

        if(n_read == 0) {
            if(rdb_buf_idx == 0) {
                break; // all data processed
            } else {
                // still some data left but no more data to read
                printf("%s:%d RDB format error: incomplete data at the end\n", __FILE__, __LINE__);
                fclose(fp);
                return -2;
            }
            if(feof(fp)) {
                break;
            } else if(ferror(fp)) {
                fclose(fp);
                return -2;
            }
        }
    }
    fclose(fp);
    return 0;
}

/*
 *@return 0 success -1 error -2 format error
  */
int kvs_persistence_load_rdb(struct kvs_pers_context_s *ctx, kvs_rdb_item_loader_pt loader, void* arg) {
    if(ctx->rdb_filename == NULL) {
        return -1;        
    }
    // if(ctx->rdb_fp == NULL) {
    //     ctx->rdb_fp = fopen(ctx->rdb_filename, "rb");
    // }

    //return _kvs_persistence_load_rdb_filename(ctx->rdb_filename, loader, arg);
    return _kvs_perisistence_load_rdb_filename_mmap(ctx->rdb_filename, loader, arg);
}

int kvs_persistence_load_rdb_filename(char* rdb_filename, kvs_rdb_item_loader_pt data_loader, void* arg) {
    if(rdb_filename == NULL || data_loader == NULL) {
        return -1;
    }
    //return _kvs_persistence_load_rdb_filename(rdb_filename, data_loader, arg);
    return _kvs_perisistence_load_rdb_filename_mmap(rdb_filename, data_loader, arg);
}


