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

    FILE* rdb_fd;
    char *rdb_filename;
    size_t rdb_offset;
    size_t rdb_size;
} kvs_pers_context_t;


/*
* @param msg:
*/
typedef int (*kvs_pest_get_exe_one_cmd)(char* msg, int length, int *idx, void* arg);

kvs_pers_context_t * kvs_persistence_create(char* aof_filename, char* rdb_filename);
int kvs_persistence_destroy(kvs_pers_context_t *ctx);
int kvs_persistence_write_aof(kvs_pers_context_t *ctx, char* data, size_t data_len);
int kvs_persistence_load_aof(kvs_pers_context_t *aof_ctx, kvs_pest_get_exe_one_cmd func, void* arg);


typedef void (*kvs_pers_write_rdb_cb)(char *key, int len_key, char *value, int len_val, void* arg);
typedef int (*kvs_pers_rdb_cb)(void* db, kvs_pers_write_rdb_cb callback, void* cb_arg);


int kvs_persistence_save_rdb(kvs_pers_context_t *ctx, kvs_pers_rdb_cb func, void* db);
int kvs_persistence_load_rdb(kvs_pers_context_t *ctx);


#endif