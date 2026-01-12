#ifndef __KVS_PERSISTENCE_H__
#define __KVS_PERSISTENCE_H__

#include <stdio.h>
#include <sys/time.h>

#define AOF_FSYNC_INTERVAL_MS 1000 // 1 second
#define AOF_MAX_BUFFER_SIZE 4096 // 4KB

typedef struct kvs_pers_context_s {
    int aof_fd;
    char *aof_filename;
    struct timeval last_fsync_time;
    char write_buffer[AOF_MAX_BUFFER_SIZE];
	size_t write_offset;
    size_t buffer_size;

    //FILE* rdb_fp;
    char *rdb_filename;
    size_t rdb_size;
} kvs_pers_context_t;


// return -1 if
typedef int (*kvs_aof_data_parser_cb)(char* msg, int length, void* arg);

kvs_pers_context_t * kvs_persistence_create(char* aof_filename, char* rdb_filename);
int kvs_persistence_destroy(kvs_pers_context_t *ctx);
int kvs_persistence_write_aof(kvs_pers_context_t *ctx, char* data, size_t data_len);
int kvs_persistence_load_aof(kvs_pers_context_t *aof_ctx, kvs_aof_data_parser_cb data_parser, void* arg);
int kvs_persistence_flush_aof(kvs_pers_context_t *ctx);


typedef int (*kvs_rdb_item_writer_pt)(char* data, int len, void* privdata);
typedef void (*kvs_storage_iterator_pt)(void* iter_arg, kvs_rdb_item_writer_pt writer, void* writer_ctx);

typedef int (*kvs_rdb_item_loader_pt)(char* data, int len, void* arg);

int kvs_persistence_save_rdb(kvs_pers_context_t *ctx, kvs_storage_iterator_pt iterator, void* iter_arg);
int kvs_persistence_load_rdb(kvs_pers_context_t *ctx, kvs_rdb_item_loader_pt data_loader, void* arg);

#endif