#include "kvstore.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>

kvs_aof_context_t kvs_aof_ctx = {0}; // global context

int kvs_persistence_init(kvs_aof_context_t *ctx, char* aof_filename) {
    if(aof_filename == NULL) {
        return -1;
    }
    ctx->aof_filename = aof_filename;
    ctx->aof_fd = open(aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if(ctx->aof_fd == -1) {
        return -1;
    }
    gettimeofday(&ctx->last_fsync_time, NULL);
    //ctx->write_buffer = kvs_malloc(AOF_MAX_BUFFER_SIZE); 
    if(ctx->write_buffer == NULL) {
        close(ctx->aof_fd);
        return -1;
    }
    ctx->buffer_size = AOF_MAX_BUFFER_SIZE;
    ctx->write_offset = 0;

    return 0;
}

int kvs_persistence_close(kvs_aof_context_t *ctx) {
    if(ctx->aof_fd != -1) {
        close(ctx->aof_fd);
        ctx->aof_fd = -1;
    }
    return 0;
}


int kvs_persistence_write_aof(kvs_aof_context_t *ctx, char* data, size_t data_len) {
    if(ctx == NULL || data == NULL || data_len == 0 || ctx->aof_filename == NULL) {
        return -1;
    }
    if(ctx->aof_fd == 0) {
        ctx->aof_fd = open(ctx->aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (ctx->aof_fd == -1) {
            return -1;
        }
    }
    if(ctx->aof_fd == -1) return -1;

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
    return 0;
}

#define LOAD_AOF_FILE_BUFFER_SIZE 1024 // 16384 // 16 * 1024

/*
* @param buffer: suggested size at least 4096 bytes or 64k bytes
* @return number of bytes read, -1 on error
*/
int kvs_persistence_load_aof(char *aof_name, kvs_pest_get_exe_one_cmd func) {
    if(!aof_name || !func) return -1;

    int aof_fd = open(aof_name, O_RDWR);
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
        while (1) {
            int cmd_len = 0; // 这是一个输出参数，由解析器告诉我们这条指令有多长
            
            // 注意：这里传入解析器的必须是起始位置和当前剩余的总长度
            int ret = func(f_buf + p_idx, total_in_buf - p_idx, &cmd_len);

            if (ret >= 0) { 
                // 解析并执行成功
                p_idx += cmd_len;
                total_valid_file_pos += cmd_len;
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

        // 4. 如果 read 返回 0，说明文件到底了
        if (n_read == 0) break;
    }

    // 5. 崩溃修复：如果文件读完了还剩下 leftover > 0，说明末尾有垃圾
    struct stat st;
    fstat(aof_fd, &st);
    if (st.st_size > total_valid_file_pos) {
        printf("AOF tail corrupted, truncating from %lld to %lld\n", 
                (long long)st.st_size, (long long)total_valid_file_pos);
        ftruncate(aof_fd, total_valid_file_pos);
    }

    close(aof_fd);
    return 0;
}




/*
 * 
 */
int kvs_rdb_save(char* filename) {
    

}

int kvs_rdb_load(char* filename) {

}

// int kvs_array_persistence_full(kvs_array_t *inst, char* filename) {
//     if(inst == NULL || filename == NULL) {
//         return -1;
//     }
//     int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
//     if(fd == -1) {
//         return -1;
//     }
