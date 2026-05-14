#ifndef __KVS_PERSISTENCE_H__
#define __KVS_PERSISTENCE_H__

#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/uio.h>
#include "kvs_event_loop.h"

#define AOF_FSYNC_INTERVAL_MS 1000 // 1 second
#define AOF_MAX_BUFFER_SIZE 4194304 // 4MB

typedef enum {
    KVS_AOF_NO_FSYNC = 0, 
    KVS_AOF_EVERY_SEC = 1
} kvs_pers_type_t;

typedef enum {
    KVS_AOF_WRITE_ASYNC_IOURING = 0,
    KVS_AOF_WRITE_SYNC_BUFFERED = 1
} kvs_aof_write_mode_t;

typedef void (*kvs_pers_aof_available_pt)(void *arg);

struct kvs_pers_context_s {
    struct kvs_loop_s *loop;
    int aof_enabled;
    kvs_pers_type_t aof_fsync_policy;
    kvs_aof_write_mode_t aof_write_mode;
    int aof_fd;
    char *aof_filename;
    struct timeval last_fsync_time;
    char write_buffer[AOF_MAX_BUFFER_SIZE];
	size_t write_offset;
    size_t buffer_size;
    size_t write_offset_head;
    size_t write_offset_tail;
    size_t last_sent_len;
    off_t global_offset;
    int is_writing_aof;
    struct iovec aof_iovecs[2];
    struct kvs_event_s aof_fwrite_ev;
    struct kvs_event_s aof_fsync_ev;
    struct kvs_event_s timer_ev;
    struct __kernel_timespec ts;
    int aof_fsync_inflight;
    int aof_fsync_pending;
    kvs_pers_aof_available_pt on_aof_available;
    void *on_aof_available_arg;

    //FILE* rdb_fp;
    char *rdb_filename;
    int rdb_policy; // 多少次写操作后触发一次 RDB 快照，默认 100000 次
    size_t rdb_size;
};

struct kvs_pers_config_s {
    struct kvs_loop_s *loop;
    int aof_enabled;
    kvs_pers_type_t aof_fsync_policy; // 0: every sec, 1: every cmd, 2: no fsync
    kvs_aof_write_mode_t aof_write_mode;
    int rdb_policy; // 多少次写操作后触发一次 RDB 快照，默认 100000 次
    char *aof_filename;
    char *rdb_filename;
    kvs_pers_aof_available_pt on_aof_available;
    void *on_aof_available_arg;
};



// return -1 if
typedef int (*kvs_aof_data_parser_pt)(char* msg, int length, int *parsed_len, void* arg);

struct kvs_pers_context_s * kvs_persistence_create(struct kvs_pers_config_s *config);
int kvs_persistence_destroy(struct kvs_pers_context_s *ctx);
int kvs_persistence_write_aof(struct kvs_pers_context_s *ctx, char* data, size_t data_len);
int kvs_persistence_write_aof_iouring(struct kvs_pers_context_s *ctx, char *data, size_t data_len);
int kvs_persistence_write_aof_sync(struct kvs_pers_context_s *ctx, char *data, size_t data_len);
size_t kvs_persistence_aof_available(struct kvs_pers_context_s *ctx);
int kvs_persistence_aof_should_backpressure(struct kvs_pers_context_s *ctx, size_t data_len);
void kvs_persistence_before_sleep(struct kvs_pers_context_s *ctx);
int kvs_persistence_load_aof(struct kvs_pers_context_s *aof_ctx, kvs_aof_data_parser_pt data_parser, void* arg);
int kvs_persistence_flush_aof(struct kvs_pers_context_s *ctx);


typedef int (*kvs_rdb_item_writer_pt)(char* data, int len, void* privdata);
typedef void (*kvs_storage_iterator_pt)(void* iter_arg, kvs_rdb_item_writer_pt writer, void* writer_ctx);

typedef int (*kvs_rdb_item_loader_pt)(char* data, int len, void* arg);

int kvs_persistence_save_rdb(struct kvs_pers_context_s *ctx, kvs_storage_iterator_pt iterator, void* iter_arg);
int kvs_persistence_load_rdb(struct kvs_pers_context_s *ctx, kvs_rdb_item_loader_pt data_loader, void* arg);
int kvs_persistence_load_rdb_filename(char* filename, kvs_rdb_item_loader_pt data_loader, void* arg);

#endif
