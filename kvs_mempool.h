#include <stdlib.h>

#define KVS_MP_MIN_SHIFT 3 // 2 ^ 3 = 8 bytes
#define KVS_MP_MAX_SHIFT 12 // 2 ^ 12 = 4096 bytes
#define KVS_MP_CLASS_COUNT (KVS_MP_MAX_SHIFT - KVS_MP_MIN_SHIFT + 1) 
// typedef struct kvs_mp_large_node_s {
//     void *mem;
//     struct kvs_mp_large_node_s *next;
// } kvs_mp_large_node_t;

// typedef struct kvs_mp_node_s {
//     unsigned char *last;
//     unsigned char * end;
//     struct kvs_mp_node_s* next;
//     size_t failed;
// } kvs_mp_node_t;

typedef struct kvs_mp_pool_s {
    //struct kvs_mp_large_node_s *large;
    void *heads[KVS_MP_CLASS_COUNT]; // linked list head for each size class
} kvs_mp_pool_t;


void kvs_mempool_create(kvs_mp_pool_t *pool);
void kvs_mempool_destroy(kvs_mp_pool_t *pool);
void* kvs_mempool_alloc(kvs_mp_pool_t *pool, size_t size);
void kvs_mempool_free(kvs_mp_pool_t *pool, void *p, size_t size);

