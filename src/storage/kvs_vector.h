#ifndef __KVS_VECTOR_H__
#define __KVS_VECTOR_H__

#include <stddef.h>
#include <stdint.h>

#include "kvs_types.h"

typedef enum {
    VEC_METRIC_COSINE = 0,
    VEC_METRIC_IP     = 1,
    VEC_METRIC_L2     = 2,
} vec_metric_t;

typedef enum {
    VEC_INDEX_FLAT = 0,
    VEC_INDEX_HNSW = 1,
} vec_index_type_t;

typedef struct vector_block {
    uint32_t count;      // 当前 block 已使用 slot 数
    uint32_t capacity;   // 一般等于 collection->block_size

    float   *data;       // capacity * dim 个 float
    char   **members;    // capacity 个 member_id，比如 answer:1001
    uint8_t *deleted;    // capacity 个删除标记，0=active, 1=deleted
} vector_block_t;

typedef struct vector_collection {
    char *name;

    uint32_t dim;

    uint32_t block_size;      // 比如 1024 或 4096
    uint32_t block_count;     // 当前已有多少个 block
    uint32_t block_capacity;  // blocks 指针数组容量

    uint64_t next_id;         // 下一个 vector_id
    uint64_t total_count;     // 历史分配过的 slot 数，包括 deleted
    uint64_t active_count;    // 未删除 vector 数
    uint64_t deleted_count;   // lazy deleted 数

    vector_block_t **blocks;

    void *member_to_id;       // member_id -> uint64_t vector_id hash_table

    vec_metric_t metric;
    vec_index_type_t index_type;

    void *index;              // HNSW index，FLAT 阶段可以为 NULL
} vector_collection_t;

typedef struct kvs_vector_store kvs_vector_store_t;

typedef struct kvs_vector_search_result {
    char *member;
    int len_member;
    uint64_t vector_id;
    float score;
} kvs_vector_search_result_t;

typedef struct kvs_vector_info {
    char *name;
    int len_name;
    uint32_t dim;
    uint32_t block_size;
    uint32_t block_count;
    uint64_t total_count;
    uint64_t active_count;
    uint64_t deleted_count;
    vec_metric_t metric;
    vec_index_type_t index_type;
} kvs_vector_info_t;

typedef int (*kvs_vector_collection_iter_cb)(vector_collection_t *collection, void *arg);
typedef int (*kvs_vector_item_iter_cb)(vector_collection_t *collection,
                                       char *member, int len_member,
                                       const float *vector, uint32_t dim,
                                       void *arg);

kvs_vector_store_t *kvs_vector_store_create(uint32_t bucket_count);
void kvs_vector_store_destroy(kvs_vector_store_t *store);

kvs_result_t kvs_vector_createv(kvs_vector_store_t *store,
                                char *name, int len_name,
                                uint32_t dim,
                                vec_metric_t metric,
                                vec_index_type_t index_type);

kvs_result_t kvs_vector_setv(kvs_vector_store_t *store,
                             char *collection_name, int len_collection_name,
                             char *member, int len_member,
                             const void *vector, int len_vector);

kvs_result_t kvs_vector_getv(kvs_vector_store_t *store,
                             char *collection_name, int len_collection_name,
                             const void *query_vector, int len_query_vector,
                             uint32_t topk,
                             kvs_vector_search_result_t *results,
                             uint32_t *result_count);

kvs_result_t kvs_vector_delv(kvs_vector_store_t *store,
                             char *collection_name, int len_collection_name,
                             char *member, int len_member);

kvs_result_t kvs_vector_vinfo(kvs_vector_store_t *store,
                              char *collection_name, int len_collection_name,
                              kvs_vector_info_t *info);

int kvs_vector_store_foreach_collection(kvs_vector_store_t *store,
                                        kvs_vector_collection_iter_cb cb,
                                        void *arg);
int kvs_vector_collection_foreach_item(vector_collection_t *collection,
                                       kvs_vector_item_iter_cb cb,
                                       void *arg);

vector_collection_t *kvs_vector_collection_create(char *name, int len_name,
                                                  uint32_t dim,
                                                  vec_metric_t metric,
                                                  vec_index_type_t index_type,
                                                  uint32_t block_size);
void kvs_vector_collection_destroy(vector_collection_t *collection);

kvs_result_t kvs_vector_set_float32(vector_collection_t *collection,
                                    char *member, int len_member,
                                    const float *vector, uint32_t dim);
kvs_result_t kvs_vector_search_float32(vector_collection_t *collection,
                                       const float *query, uint32_t dim,
                                       uint32_t topk,
                                       kvs_vector_search_result_t *results,
                                       uint32_t *result_count);
kvs_result_t kvs_vector_del(vector_collection_t *collection,
                            char *member, int len_member);
kvs_result_t kvs_vector_info(vector_collection_t *collection,
                             kvs_vector_info_t *info);

#endif
