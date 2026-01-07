#include "common.h"
#include "kvs_mempool.h"

#include <stdio.h>

static kvs_mp_pool_t kvs_mem_pool;

void kvs_global_mempool_init() {
#if KVS_MEM_POOL
    printf("Memory pool enabled.\n");  
    kvs_mempool_create(&kvs_mem_pool);
#else 
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