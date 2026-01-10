#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdlib.h>

#define KVS_MEM_POOL 1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


void kvs_global_mempool_init();
void kvs_global_mempool_destroy();

void *kvs_malloc(size_t size);
void kvs_free(void *ptr, size_t size);


int kvs_parse_int(char* s, int length, int* offset);
#endif