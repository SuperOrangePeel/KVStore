#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdlib.h>
#include <stdint.h>

#if KVS_USE_JEMALLOC
#define KVS_MEM_POOL 0
#else
#define KVS_MEM_POOL 1
#endif

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


void kvs_global_mempool_init();
void kvs_global_mempool_destroy();

void *kvs_malloc(size_t size);
void kvs_free(void *ptr, size_t size);


int kvs_parse_int(char* s, int length, int* offset);
uint64_t kvs_parse_uint64(char* s, int length, int* offset);

uint64_t kvs_generate_token();

struct kvs_session_entry_s {
    uint64_t token;
    void *session_ctx; // 指向具体的会话上下文
    struct kvs_session_entry_s *next; // 链表
};


struct kvs_session_table_s {
    struct kvs_session_entry_s *head;
};

uint64_t kvs_session_register(struct kvs_session_table_s *table, void *session_ctx);
void *kvs_session_match(struct kvs_session_table_s *table, uint64_t token);


void mem_hexdump(const void *addr, size_t len, const char *title);
#endif