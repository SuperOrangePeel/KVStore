#include "common.h"
#include "kvs_mempool.h"

#include <stdint.h>
#include <stdio.h>
#include <stdatomic.h>
#include <time.h>

static kvs_mp_pool_t kvs_mem_pool;

void kvs_global_mempool_init() {
#if KVS_MEM_POOL
    printf("Memory pool enabled.\n");  
    kvs_mempool_create(&kvs_mem_pool);
#else 
    printf("Memory pool disabled.\n");
#endif
}

void kvs_global_mempool_destroy() {
#if KVS_MEM_POOL
    kvs_mempool_destroy(&kvs_mem_pool);
#else 
#endif
}

void *kvs_malloc(size_t size) {
#if KVS_MEM_POOL
	return kvs_mempool_alloc(&kvs_mem_pool, size);
#else
	return malloc(size);
#endif	
}

void kvs_free(void *ptr, size_t size) {
#if KVS_MEM_POOL
	kvs_mempool_free(&kvs_mem_pool, ptr, size);
#else

	free(ptr);
#endif
}


inline int kvs_parse_int(char* s, int length, int* offset) {
    int res = 0;
    int i = *offset;
	//printf("i:%d, s[i]:[%.*s]\n", i, 1, s);
    while (i < length && s[i] >= '0' && s[i] <= '9') {
        res = res * 10 + (s[i] - '0');
		//printf("res:%d, num:%d\n", res, (int)(s[i] - '0'));
        i++;
    }
    *offset = i;
	//printf("res:%d\n", res);
    return res;
}

inline uint64_t kvs_parse_uint64(char* s, int length, int* offset) {
    uint64_t res = 0;
    int i = *offset;
    //printf("i:%d, s[i]:[%.*s]\n", i, 1, s);
    while (i < length && s[i] >= '0' && s[i] <= '9') {
        res = res * 10 + (s[i] - '0');
        //printf("res:%d, num:%d\n", res, (int)(s[i] - '0'));
        i++;
    }
    *offset = i;
    //printf("res:%llu\n", res);
    return res;
}


static atomic_uint_fast32_t g_token_counter = 0;

uint64_t kvs_generate_token() {
    uint64_t now = (uint64_t)time(NULL); // 秒级或毫秒级
    uint32_t count = atomic_fetch_add(&g_token_counter, 1);
    
    // 拼接成 64 位：[32bit timestamp][32bit counter]
    return (now << 32) | count;
}



// 生成 Token 并注册
uint64_t kvs_session_register(struct kvs_session_table_s *table, void *session_ctx) {
    // 1. 生成随机 Token
    uint64_t token = 0;
    // 简单的 64位随机数生成 (生产环境可以用 UUID)
    token =  kvs_generate_token();// ((uint64_t)rand() << 32) | rand();

    // 2. 创建注册表节点
    struct kvs_session_entry_s *entry = kvs_malloc(sizeof(struct kvs_session_entry_s));
    entry->token = token;
    entry->session_ctx = session_ctx;
    
    // 3. 插入链表头 (这里是单线程模型，不用锁；如果是多线程要加锁)
    entry->next = table->head;
    table->head = entry;

    return token;
}

// 查找并移除 Token
void *kvs_session_match(struct kvs_session_table_s *table, uint64_t token) {
    struct kvs_session_entry_s **curr = &table->head;
    struct kvs_session_entry_s *entry;

    while (*curr) {
        entry = *curr;
        if (entry->token == token) {
            // 找到了！
            void *ctx = entry->session_ctx;
            
            // 从链表中移除 (One-time token)
            *curr = entry->next;
            kvs_free(entry, sizeof(struct kvs_session_entry_s));
            return ctx;
        }
        curr = &entry->next;
    }
    return NULL; // 没找到
}