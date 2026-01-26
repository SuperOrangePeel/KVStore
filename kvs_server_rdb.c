#include "kvs_network.h"
#include "kvs_types.h"
#define _GNU_SOURCE // 必须在第一行，解锁 O_DIRECT

#include "kvs_server.h"
#include "kvs_hash.h"
#include "kvs_array.h"
#include "kvs_rbtree.h"
#include "kvs_persistence.h"
#include "logger.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <liburing.h>
#include <assert.h>


/*********************************loading rdb method **********************************/


// also used in kvs_slave.c
static int _kvs_server_restore_entry(struct kvs_server_s* server, int data_type, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return -1;
	}
	switch(data_type) {
		case KVS_RDB_ARRAY:
			return kvs_array_resp_set(server->array, key, len_key, value, len_val);
		case KVS_RDB_RBTREE:
			return kvs_rbtree_resp_set(server->rbtree, key, len_key, value, len_val);
		case KVS_RDB_HASH:
			return kvs_hash_resp_set(server->hash, key, len_key, value, len_val);
		default:
			return -1;
	}
	return 0;
}

static int _kvs_server_rdb_item_loader(char* data, int len, void* arg) {
	if(arg == NULL || data == NULL || len <= 0) {
		return -1;
	}
	struct kvs_server_s* server = (struct kvs_server_s*)arg;
	
	if(len < sizeof(char) + sizeof(int)) {
		return -1;
	}
	int data_idx = 0;
	int processed_bytes = 0;
	while(data_idx < len) {
		processed_bytes = data_idx;
		char data_type = data[data_idx];
		data_idx += sizeof(char);
		if(data_idx + sizeof(int) > len) {
			break;
		}
		int len_key = 0;
		memcpy(&len_key, data + data_idx, sizeof(int));
		data_idx += sizeof(int);
		if(len_key <=0) {
			printf("%s:%d invalid len_key %d\n", __FILE__, __LINE__, len_key);
			return -1;
		}
		if(data_idx + len_key + sizeof(int) > len) {
			break;
		}
		char *key = data + data_idx;
		data_idx += len_key;
		int len_val = 0;
		memcpy(&len_val, data + data_idx, sizeof(int));
		data_idx += sizeof(int);
		if(len_val <=0) {
			printf("%s:%d invalid len_val %d\n", __FILE__, __LINE__, len_val);
			return -1;
		}
		if(data_idx + len_val > len) {
			break;
		}
		char *value = data + data_idx;
		data_idx += len_val;


		if(_kvs_server_restore_entry(server, data_type, key, len_key, value, len_val) < 0) {
			return -1;
		}
	}
	
	
	return processed_bytes;
}


int kvs_server_load_rdb(struct kvs_server_s *server) {
	if(server == NULL || server->pers_ctx == NULL) {
		return -1;
	}
    struct stat file_stat;
	if(stat(server->pers_ctx->rdb_filename, &file_stat) != 0){
		// rdb file not exist, skip
		printf("RDB file not exist, skip RDB recovery.\n");
		return KVS_ERR;
	}

	kvs_persistence_load_rdb(server->pers_ctx, _kvs_server_rdb_item_loader, server);
	return 0;
}

/***********************************non-blocking async RDB save using io_uring************************************/

// 手动补齐缺失的宏
#ifndef O_DIRECT
#define O_DIRECT 040000
#endif

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
        LOG_FATAL("Failed to open RDB temp file with O_DIRECT: %s", strerror(errno));
        assert(0);
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
    arg.db_type = KVS_RDB_HASH; // Hash
    kvs_hash_filter(server->hash, _kvs_rdb_db_filter, &arg);
    
    arg.db_type = KVS_RDB_ARRAY; // Array
    kvs_array_filter(server->array, _kvs_rdb_db_filter, &arg);

    arg.db_type = KVS_RDB_RBTREE; // RBTree
    kvs_rbtree_filter(server->rbtree, _kvs_rdb_db_filter, &arg);
    
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
        rename(rdb_temp_filename, server->pers_ctx->rdb_filename);
    } else {
        unlink(rdb_temp_filename);
    }

    free(bufs[0].data);
    free(bufs[1].data);
    io_uring_queue_exit(&ring);
    return arg.ret;
}


kvs_status_t kvs_server_save_rdb_fork(struct kvs_server_s *server) {
    pid_t pid = fork();
    if (pid < 0) {
        LOG_FATAL("fork rdb process failed: %s", strerror(errno));
        assert(0);
        return KVS_ERR;
    } else if (pid == 0) {
        // child process
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGINT);
        sigaddset(&mask, SIGTERM);
        sigprocmask(SIG_UNBLOCK, &mask, NULL);
        close(server->network.server_fd); // close listening socket in child
        int ret = kvs_rdb_child_process(server);
        exit(ret == 0 ? 0 : 1);
    } else {
        server->rdb_child_pid = pid;
        return KVS_OK;
    }
}


int _kvs_server_rdb_conn_cb(struct kvs_conn_s *conn, void* arg) {
    if(conn == NULL) {
        if(conn == NULL) LOG_FATAL("conn is NULL");
        assert(0);
        return 0; // continue
    }
    if(conn->header.user_data == NULL) {
        // server_fd business_ctx is NULL  
        return 0; // continue
    }
    struct kvs_server_s *server = (struct kvs_server_s *)arg;
    struct kvs_ctx_header_s *ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
    

    switch(ctx_header->type) {
        case KVS_CTX_SLAVE_OF_ME:
        {
            struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
            if(slave_ctx->state == KVS_MY_SLAVE_WAIT_BGSAVE_END) {
                LOG_INFO("Notifying slave fd %d to start RDB transfer", conn->_internal.fd);
                kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_BGSAVE_DONE);
            }
            break;
        }
        case KVS_CTX_NORMAL_CLIENT:
        {
            struct kvs_client_context_s *cli_ctx = (struct kvs_client_context_s*)conn->header.user_data;
            if(cli_ctx->state == KVS_CLIENT_STATE_WAIT_BGSAVE) {
                //调用状态机
                kvs_client_state_machine_tick(server, conn);
            }
            // normal client, skip
            break;
        }
        default:
            return 0; // continue
    }
    return 0; // continue
}


int kvs_server_on_rdb_save_finish(struct kvs_server_s *server, kvs_status_t status) {
    if (status != KVS_OK) {
        // 如果失败了，可能需要断开所有等待的 Slave，或者重试
        return KVS_ERR;
    }


    char *rdb_path = server->pers_ctx->rdb_filename;

    // // 1. 打开生成的 RDB 文件（为了发送）
    // // 注意：这里保存 fd，供后续发送使用
    // int rdb_fd = open(rdb_path, O_RDONLY);
    // if (rdb_fd < 0) {
    //     LOG_ERROR("Failed to open RDB for sending: %s", strerror(errno));
    //     return KVS_ERR;
    // }
    
    // // 获取文件大小
    // struct stat st;
    // fstat(rdb_fd, &st);
    // size_t file_size = st.st_size;
    // server->rdb_file_size = file_size;
    // server->rdb_fd = rdb_fd;

    kvs_net_online_conns_filter(&server->network, _kvs_server_rdb_conn_cb, server);
    return KVS_OK;
}





/**********************Blocking method for saving RDB file************************/




typedef struct {
	struct hashtable_s* hash;
	struct _rbtree* rbtree;
	struct kvs_array_s* array;
} server_dbs_t;

typedef struct {
	kvs_rdb_item_writer_pt writer; 
	void* writer_ctx;
	int db_type;
} server_storage_item_filter_ctx_t;

static int _kvs_server_storage_item_filter(char *key, int len_key, char *value, int len_val, void* arg) {
	server_storage_item_filter_ctx_t* ctx = (server_storage_item_filter_ctx_t*)arg;
	kvs_rdb_item_writer_pt writer = ctx->writer;
	void* writer_ctx = ctx->writer_ctx;
	int db_type = ctx->db_type;
	char dt_char = (char)db_type;
	if(writer == NULL) {
		return -1;
	}

	writer(&dt_char, sizeof(char), writer_ctx);
	writer((char*)&len_key, sizeof(int), writer_ctx);
	writer(key, len_key, writer_ctx);
	writer((char*)&len_val, sizeof(int), writer_ctx);
	writer(value, len_val, writer_ctx);
	return 0;
}


static void _kvs_server_storage_item_iterator(void* iter_arg, kvs_rdb_item_writer_pt writer, void* writer_ctx) {

	server_dbs_t *dbs = (server_dbs_t *)iter_arg;
	for(int i = KVS_RDB_START; i < KVS_RDB_END; ++ i) {
		server_storage_item_filter_ctx_t ctx;
		ctx.writer = writer;
		ctx.writer_ctx = writer_ctx;
		ctx.db_type = i;
		switch(i) {
			case KVS_RDB_HASH:
				kvs_hash_filter(dbs->hash, _kvs_server_storage_item_filter, &ctx);
				break;
			case KVS_RDB_ARRAY:
				kvs_array_filter(dbs->array, _kvs_server_storage_item_filter, &ctx);
				break;
			case KVS_RDB_RBTREE:
				kvs_rbtree_filter(dbs->rbtree, _kvs_server_storage_item_filter, &ctx);
				break;
			default:
				assert(0);
		}
	}
}



int kvs_server_save_rdb(struct kvs_server_s *server) {
	if(server->hash == NULL || server->rbtree == NULL || server->array == NULL) {
		return -1;
	}
	int ret = 0;
	// save hash
	server_dbs_t dbs;
	dbs.hash = server->hash;
	dbs.rbtree = server->rbtree;
	dbs.array = server->array;
	ret = kvs_persistence_save_rdb(server->pers_ctx,  _kvs_server_storage_item_iterator, &dbs);
	if(ret != 0) {
		printf("%s:%d save hash rdb failed\n", __FILE__, __LINE__);
		assert(0);
		return -1;
	}
	
	return 0;
}
