#include "kvs_mempool.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>



#define KVS_MP_PAGE_SIZE 4096
#define KVS_MP_MAX_ALLOC_FROM_POOL (1 << KVS_MP_MAX_SHIFT)
#define KVS_MP_ALIGNMENT 32

kvs_mp_pool_t kvs_mem_pool;

// static void *_kvs_mp_alloc_large(kvs_mp_pool_t *pool, size_t size) {
//     void *p = malloc(size);
//     if(p == NULL) return NULL;

//     size_t n = 0;
//     kvs_mp_large_node_t *large;
//     for(large = pool->large; large != NULL; large = large->next) {
//         if(large->mem == NULL) {
//             large->mem = p;
//             return p;
//         }
//         if(n ++ > 3) break; // nginx style
//     }
//     // sizeof(kvs_mp_large_node_t) == 16 then we can use mempool to alloc it
//     large = kvs_mempool_alloc(pool, sizeof(kvs_mp_large_node_t));
//     if(large == NULL) {
//         free(p);
//         return NULL;
//     }
//     large->mem = p;
//     large->next = pool->large;
//     pool->large = large;
//     return p;
// }

// static void _kvs_mp_free_large(kvs_mp_pool_t *pool, void *p) {
//     kvs_mp_large_node_t *large;
//     for(large = pool->large; large != NULL; large = large->next) {
//         if(large->mem == p) {
//             free(large->mem);
//             large->mem = NULL;
//             return;
//         }
//     }
// }

void kvs_mempool_create(kvs_mp_pool_t *pool) {
    assert((1 << KVS_MP_MIN_SHIFT) >= sizeof(void*));
    // kvs_mp_pool_t *pool = NULL;
    // posix_memalign((void**)&pool, KVS_MP_ALIGNMENT, sizeof(kvs_mp_pool_t));

    // if (pool == NULL) {
    //     return NULL;
    // }
    // pool->max_size = max_size < KVS_MP_MAX_ALLOC_FROM_POOL ? max_size : KVS_MP_MAX_ALLOC_FROM_POOL;
    //pool->large = NULL;
    memset(pool->heads, 0, sizeof(pool->heads));
    // return pool;
};


void kvs_mempool_destroy(kvs_mp_pool_t *pool) {
    // kvs_mp_large_node_t *large = pool->large;
    // kvs_mp_large_node_t *tmp_large;
    // while (large) {
    //     tmp_large = large->next;
    //     free(large->mem);
    //     free(large);
    //     large = tmp_large;
    // }
    // free small blocks is not necessary, they will be reclaimed when kvstore process exits
    // free(pool);
}

static inline int _kvs_mp_get_slot_index(size_t size) {
    // 1. 物理极限检查：小于等于 8 字节，统统进第 0 号坑 (8B)
    if (size <= 8) return 0;

    // 2. 超过 4KB，Slab 无法接管
    if (size > KVS_MP_MAX_ALLOC_FROM_POOL) return -1;

    // 3. 核心位运算逻辑 (64位系统使用 __builtin_clzl)
    // 原理：size-1 后计算前导零，确定它是 2 的多少次幂
    // 例如 size=9 -> n=8 (1000) -> 幂是 3 -> shift=4 -> index=1 (16B槽位)
    size_t n = size - 1;
    int shift = 64 - __builtin_clzl(n); 
    
    // 减去基准偏移 (MIN_SHIFT = 3)
    // 8B (shift=3) -> idx 0
    // 16B (shift=4) -> idx 1
    // ...
    // 4096B (shift=12) -> idx 9
    return shift - KVS_MP_MIN_SHIFT;
}

void* kvs_mempool_alloc(kvs_mp_pool_t *pool, size_t size) {
    if(size > KVS_MP_MAX_ALLOC_FROM_POOL) {
        return malloc(size);
    }
    int cls_idx = _kvs_mp_get_slot_index(size);

    void* head = pool->heads[cls_idx];
    if(head) {
        pool->heads[cls_idx] = *((void**)head); // next pointer is stored in the first sizeof(void*) bytes
        return head;
    } else {
        void* p = NULL;
        int ret = posix_memalign(&p, KVS_MP_PAGE_SIZE, KVS_MP_PAGE_SIZE);
        if(ret != 0 || p == NULL) {
            return NULL;
        }
        //memset(p, 0, KVS_MP_PAGE_SIZE);
        if(cls_idx == KVS_MP_CLASS_COUNT - 1) return p;
        size_t block_size = 1 << (cls_idx + KVS_MP_MIN_SHIFT);
        size_t n_blocks = KVS_MP_PAGE_SIZE / block_size;
        if(n_blocks < 2) {
            return p;
        }
        size_t i = 1;
        void* free_mem = (void*)((char*)p + block_size);
        void* curr = free_mem;
        for(i = 1; i < n_blocks; i ++ ) {
            void* next = (i == n_blocks - 1) ? NULL : (void*)((char*)curr + block_size);
            *((void**)curr) = next; // store next pointer
            curr = next;
        }
        pool->heads[cls_idx] = free_mem;
        return p;
    }
   

}


void kvs_mempool_free(kvs_mp_pool_t *pool, void *p, size_t size) {
    if(size > KVS_MP_MAX_ALLOC_FROM_POOL) {
        free(p);
        return;
    }
    int cls_idx = _kvs_mp_get_slot_index(size);
    *(void**)p = pool->heads[cls_idx];
    pool->heads[cls_idx] = p;
};

