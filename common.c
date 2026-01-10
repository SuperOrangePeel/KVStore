#include "common.h"
#include "kvs_mempool.h"

#include <stdio.h>

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
