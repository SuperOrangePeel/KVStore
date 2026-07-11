#include "kvs_vector.h"
#include "common.h"

#include <float.h>
#include <stdint.h>
#include <string.h>

/*
 * Vector storage 模块
 *
 * 这一层只负责向量 collection 的内存组织和检索，不直接处理 RESP 协议。
 * 当前实现的是 FLAT index：写入时顺序追加，检索时线性扫描所有未删除向量，
 * 根据 collection 上配置的 metric 计算分数并维护一个 topK 结果集。
 *
 * 数据组织：
 *   kvs_vector_store_t: collection_name -> vector_collection_t
 *   vector_collection_t: 一个向量集合，保存 dim/metric/index_type/blocks/member_to_id
 *   vector_block_t: 固定容量的向量块，data 为连续 float32 数组
 *
 * HNSW 还没有实现，collection->index 目前作为后续扩展入口保留。
 */

#define KVS_VECTOR_DEFAULT_BUCKETS 1024u
#define KVS_VECTOR_DEFAULT_BLOCK_SIZE 1024u
#define KVS_VECTOR_INITIAL_BLOCK_CAPACITY 4u

/* member_id -> vector_id 哈希表的链表节点。 */
struct vector_member_node {
    char *key;
    int len_key;
    uint64_t id;
    struct vector_member_node *next;
};

/*
 * collection 内部的 member 索引。
 *
 * member_id 是用户传入的业务 id，例如 qa:1001；vector_id 是内部递增 id，
 * 可以通过 vector_id 定位到 block 和 block 内 offset。
 */
struct vector_member_map {
    struct vector_member_node **buckets;
    uint32_t bucket_count;
};

/* vector_store 中 collection_name -> collection 的链表节点。 */
struct vector_collection_node {
    vector_collection_t *collection;
    struct vector_collection_node *next;
};

/*
 * 全局 vector store。
 *
 * server 层持有一个 kvs_vector_store_t，用它管理多个 collection，
 * 不和主 hash 混在一起，避免普通 KV 和向量集合的生命周期/类型语义交叉。
 */
struct kvs_vector_store {
    struct vector_collection_node **buckets;
    uint32_t bucket_count;
};

/* FNV-1a 风格的简单哈希，用于 collection 表和 member 表。 */
static uint32_t kvs_vector_hash(const char *key, int len, uint32_t bucket_count) {
    uint64_t hash = 1469598103934665603ull;

    if (key == NULL || len <= 0 || bucket_count == 0) return 0;

    for (int i = 0; i < len; i++) {
        hash ^= (unsigned char)key[i];
        hash *= 1099511628211ull;
    }

    return (uint32_t)(hash % bucket_count);
}

/* 复制一段不要求以 \0 结尾的二进制安全 key。 */
static char *kvs_vector_memdup(const char *src, int len) {
    char *dst;

    if (src == NULL || len <= 0) return NULL;

    dst = (char *)kvs_malloc((size_t)len);
    if (dst == NULL) return NULL;

    memcpy(dst, src, (size_t)len);
    return dst;
}

/* 创建 collection 内部的 member_id -> vector_id 映射表。 */
static struct vector_member_map *kvs_vector_member_map_create(uint32_t bucket_count) {
    struct vector_member_map *map;

    if (bucket_count == 0) bucket_count = KVS_VECTOR_DEFAULT_BUCKETS;

    map = (struct vector_member_map *)kvs_malloc(sizeof(*map));
    if (map == NULL) return NULL;

    map->buckets = (struct vector_member_node **)kvs_malloc(sizeof(struct vector_member_node *) * bucket_count);
    if (map->buckets == NULL) {
        kvs_free(map, sizeof(*map));
        return NULL;
    }

    memset(map->buckets, 0, sizeof(struct vector_member_node *) * bucket_count);
    map->bucket_count = bucket_count;

    return map;
}

/* 销毁 member 映射表及其所有节点。 */
static void kvs_vector_member_map_destroy(struct vector_member_map *map) {
    if (map == NULL) return;

    for (uint32_t i = 0; i < map->bucket_count; i++) {
        struct vector_member_node *node = map->buckets[i];
        while (node != NULL) {
            struct vector_member_node *next = node->next;
            kvs_free(node->key, (size_t)node->len_key);
            kvs_free(node, sizeof(*node));
            node = next;
        }
    }

    kvs_free(map->buckets, sizeof(struct vector_member_node *) * map->bucket_count);
    kvs_free(map, sizeof(*map));
}

/* 查找 member_id 对应的 vector_id；返回 1 表示存在，0 表示不存在。 */
static int kvs_vector_member_map_get(struct vector_member_map *map, char *key, int len_key, uint64_t *id) {
    uint32_t bucket;
    struct vector_member_node *node;

    if (map == NULL || key == NULL || len_key <= 0) return 0;

    bucket = kvs_vector_hash(key, len_key, map->bucket_count);
    node = map->buckets[bucket];

    while (node != NULL) {
        if (node->len_key == len_key && memcmp(node->key, key, (size_t)len_key) == 0) {
            if (id != NULL) *id = node->id;
            return 1;
        }
        node = node->next;
    }

    return 0;
}

/* 插入或更新 member_id -> vector_id 映射。 */
static int kvs_vector_member_map_set(struct vector_member_map *map, char *key, int len_key, uint64_t id) {
    uint32_t bucket;
    struct vector_member_node *node;

    if (map == NULL || key == NULL || len_key <= 0) return -1;

    bucket = kvs_vector_hash(key, len_key, map->bucket_count);
    node = map->buckets[bucket];

    while (node != NULL) {
        if (node->len_key == len_key && memcmp(node->key, key, (size_t)len_key) == 0) {
            node->id = id;
            return 0;
        }
        node = node->next;
    }

    node = (struct vector_member_node *)kvs_malloc(sizeof(*node));
    if (node == NULL) return -1;

    node->key = kvs_vector_memdup(key, len_key);
    if (node->key == NULL) {
        kvs_free(node, sizeof(*node));
        return -1;
    }

    node->len_key = len_key;
    node->id = id;
    node->next = map->buckets[bucket];
    map->buckets[bucket] = node;

    return 0;
}

/* 从 member 映射表中删除 member_id。 */
static int kvs_vector_member_map_del(struct vector_member_map *map, char *key, int len_key) {
    uint32_t bucket;
    struct vector_member_node *node;
    struct vector_member_node *prev = NULL;

    if (map == NULL || key == NULL || len_key <= 0) return 0;

    bucket = kvs_vector_hash(key, len_key, map->bucket_count);
    node = map->buckets[bucket];

    while (node != NULL) {
        if (node->len_key == len_key && memcmp(node->key, key, (size_t)len_key) == 0) {
            if (prev == NULL) {
                map->buckets[bucket] = node->next;
            } else {
                prev->next = node->next;
            }
            kvs_free(node->key, (size_t)node->len_key);
            kvs_free(node, sizeof(*node));
            return 1;
        }
        prev = node;
        node = node->next;
    }

    return 0;
}

/*
 * 创建一个向量 block。
 *
 * data 是 capacity * dim 个 float 的连续数组，members/deleted 与 slot 一一对应。
 */
static vector_block_t *kvs_vector_block_create(uint32_t capacity, uint32_t dim) {
    vector_block_t *block;

    if (capacity == 0 || dim == 0) return NULL;

    block = (vector_block_t *)kvs_malloc(sizeof(*block));
    if (block == NULL) return NULL;

    block->data = (float *)kvs_malloc(sizeof(float) * capacity * dim);
    block->members = (char **)kvs_malloc(sizeof(char *) * capacity);
    block->deleted = (uint8_t *)kvs_malloc(sizeof(uint8_t) * capacity);

    if (block->data == NULL || block->members == NULL || block->deleted == NULL) {
        if (block->data != NULL) kvs_free(block->data, sizeof(float) * capacity * dim);
        if (block->members != NULL) kvs_free(block->members, sizeof(char *) * capacity);
        if (block->deleted != NULL) kvs_free(block->deleted, sizeof(uint8_t) * capacity);
        kvs_free(block, sizeof(*block));
        return NULL;
    }

    block->count = 0;
    block->capacity = capacity;
    memset(block->members, 0, sizeof(char *) * capacity);
    memset(block->deleted, 0, sizeof(uint8_t) * capacity);

    return block;
}

/* 释放 block 内所有 member 字符串以及 data/members/deleted 三组数组。 */
static void kvs_vector_block_destroy(vector_block_t *block, uint32_t dim) {
    if (block == NULL) return;

    for (uint32_t i = 0; i < block->count; i++) {
        if (block->members[i] != NULL) {
            kvs_free(block->members[i], strlen(block->members[i]) + 1);
        }
    }

    kvs_free(block->data, sizeof(float) * block->capacity * dim);
    kvs_free(block->members, sizeof(char *) * block->capacity);
    kvs_free(block->deleted, sizeof(uint8_t) * block->capacity);
    kvs_free(block, sizeof(*block));
}

/*
 * 确保 collection->blocks 指针数组还有空间。
 *
 * 注意这里扩的是 block 指针数组，不是单个 block 的 slot 容量；
 * 单个 block 满后会新建一个 block。
 */
static int kvs_vector_collection_reserve_blocks(vector_collection_t *collection) {
    vector_block_t **new_blocks;
    uint32_t new_capacity;

    if (collection->block_count < collection->block_capacity) return 0;

    new_capacity = collection->block_capacity == 0 ? KVS_VECTOR_INITIAL_BLOCK_CAPACITY : collection->block_capacity * 2;
    new_blocks = (vector_block_t **)kvs_malloc(sizeof(vector_block_t *) * new_capacity);
    if (new_blocks == NULL) return -1;

    memset(new_blocks, 0, sizeof(vector_block_t *) * new_capacity);
    if (collection->blocks != NULL) {
        memcpy(new_blocks, collection->blocks, sizeof(vector_block_t *) * collection->block_count);
        kvs_free(collection->blocks, sizeof(vector_block_t *) * collection->block_capacity);
    }

    collection->blocks = new_blocks;
    collection->block_capacity = new_capacity;

    return 0;
}

/* 根据内部 vector_id 定位到具体 block 和 block 内 offset。 */
static int kvs_vector_get_slot(vector_collection_t *collection, uint64_t id, vector_block_t **block_out, uint32_t *offset_out) {
    uint64_t block_idx;
    uint32_t offset;
    vector_block_t *block;

    if (collection == NULL || block_out == NULL || offset_out == NULL) return -1;

    block_idx = id / collection->block_size;
    offset = (uint32_t)(id % collection->block_size);

    if (block_idx >= collection->block_count) return -1;

    block = collection->blocks[block_idx];
    if (block == NULL || offset >= block->count) return -1;

    *block_out = block;
    *offset_out = offset;
    return 0;
}

/*
 * 小型 sqrtf 近似实现，避免为 COSINE 计算额外引入 libm 链接依赖。
 * 精度对当前排序用途足够；后续如统一链接 libm，可以替换为 sqrtf。
 */
static float kvs_vector_sqrtf(float value) {
    float x;

    if (value <= 0.0f) return 0.0f;

    x = value > 1.0f ? value : 1.0f;
    for (int i = 0; i < 8; i++) {
        x = 0.5f * (x + value / x);
    }

    return x;
}

/*
 * 计算两个向量的相似度分数。
 *
 * COSINE/IP 分数越大越相似；L2 使用 -distance，使 topK 仍然按分数降序维护。
 */
static float kvs_vector_score(vec_metric_t metric, const float *a, const float *b, uint32_t dim) {
    float dot = 0.0f;
    float na = 0.0f;
    float nb = 0.0f;
    float l2 = 0.0f;

    for (uint32_t i = 0; i < dim; i++) {
        dot += a[i] * b[i];
        if (metric == VEC_METRIC_COSINE) {
            na += a[i] * a[i];
            nb += b[i] * b[i];
        } else if (metric == VEC_METRIC_L2) {
            float d = a[i] - b[i];
            l2 += d * d;
        }
    }

    if (metric == VEC_METRIC_COSINE) {
        if (na <= 0.0f || nb <= 0.0f) return 0.0f;
        return dot / (kvs_vector_sqrtf(na) * kvs_vector_sqrtf(nb));
    }

    if (metric == VEC_METRIC_L2) return -l2;

    return dot;
}

/*
 * 将一个候选结果插入 topK 数组。
 *
 * results 始终按 score 降序排列；topK 较小时这个 O(k) 插入足够简单直接。
 */
static void kvs_vector_topk_insert(kvs_vector_search_result_t *results, uint32_t *count,
                                   uint32_t topk, char *member, uint64_t id, float score) {
    uint32_t pos;

    if (topk == 0 || results == NULL || count == NULL) return;

    if (*count < topk) {
        pos = (*count)++;
    } else if (score > results[topk - 1].score) {
        pos = topk - 1;
    } else {
        return;
    }

    results[pos].member = member;
    results[pos].len_member = (int)strlen(member);
    results[pos].vector_id = id;
    results[pos].score = score;

    while (pos > 0 && results[pos].score > results[pos - 1].score) {
        kvs_vector_search_result_t tmp = results[pos - 1];
        results[pos - 1] = results[pos];
        results[pos] = tmp;
        pos--;
    }
}

/* 在全局 vector store 中按 collection 名称查找 collection。 */
static vector_collection_t *kvs_vector_store_get(kvs_vector_store_t *store, char *name, int len_name) {
    uint32_t bucket;
    struct vector_collection_node *node;

    if (store == NULL || name == NULL || len_name <= 0) return NULL;

    bucket = kvs_vector_hash(name, len_name, store->bucket_count);
    node = store->buckets[bucket];

    while (node != NULL) {
        vector_collection_t *collection = node->collection;
        if (collection != NULL && (int)strlen(collection->name) == len_name &&
            memcmp(collection->name, name, (size_t)len_name) == 0) {
            return collection;
        }
        node = node->next;
    }

    return NULL;
}

/* 创建全局 vector store。 */
kvs_vector_store_t *kvs_vector_store_create(uint32_t bucket_count) {
    kvs_vector_store_t *store;

    if (bucket_count == 0) bucket_count = KVS_VECTOR_DEFAULT_BUCKETS;

    store = (kvs_vector_store_t *)kvs_malloc(sizeof(*store));
    if (store == NULL) return NULL;

    store->buckets = (struct vector_collection_node **)kvs_malloc(sizeof(struct vector_collection_node *) * bucket_count);
    if (store->buckets == NULL) {
        kvs_free(store, sizeof(*store));
        return NULL;
    }

    memset(store->buckets, 0, sizeof(struct vector_collection_node *) * bucket_count);
    store->bucket_count = bucket_count;

    return store;
}

/* 销毁全局 vector store，并级联释放所有 collection。 */
void kvs_vector_store_destroy(kvs_vector_store_t *store) {
    if (store == NULL) return;

    for (uint32_t i = 0; i < store->bucket_count; i++) {
        struct vector_collection_node *node = store->buckets[i];
        while (node != NULL) {
            struct vector_collection_node *next = node->next;
            kvs_vector_collection_destroy(node->collection);
            kvs_free(node, sizeof(*node));
            node = next;
        }
    }

    kvs_free(store->buckets, sizeof(struct vector_collection_node *) * store->bucket_count);
    kvs_free(store, sizeof(*store));
}

/*
 * CREATEV 的 storage 层入口。
 *
 * 当前只接受 VEC_INDEX_FLAT；VEC_INDEX_HNSW 类型已保留，但索引构建逻辑后续再接。
 */
kvs_result_t kvs_vector_createv(kvs_vector_store_t *store, char *name, int len_name,
                                uint32_t dim, vec_metric_t metric, vec_index_type_t index_type) {
    uint32_t bucket;
    struct vector_collection_node *node;
    vector_collection_t *collection;

    if (store == NULL || name == NULL || len_name <= 0 || dim == 0) return KVS_RES_ERR;
    if (index_type != VEC_INDEX_FLAT) return KVS_RES_ERR;
    if (metric != VEC_METRIC_COSINE && metric != VEC_METRIC_IP && metric != VEC_METRIC_L2) return KVS_RES_ERR;
    if (kvs_vector_store_get(store, name, len_name) != NULL) return KVS_RES_EXIST;

    collection = kvs_vector_collection_create(name, len_name, dim, metric, index_type, KVS_VECTOR_DEFAULT_BLOCK_SIZE);
    if (collection == NULL) return KVS_RES_ERR;

    node = (struct vector_collection_node *)kvs_malloc(sizeof(*node));
    if (node == NULL) {
        kvs_vector_collection_destroy(collection);
        return KVS_RES_ERR;
    }

    bucket = kvs_vector_hash(name, len_name, store->bucket_count);
    node->collection = collection;
    node->next = store->buckets[bucket];
    store->buckets[bucket] = node;

    return KVS_RES_OK;
}

/* SETV 的 storage 层入口；当前只支持 FLOAT32，因此长度必须等于 dim * sizeof(float)。 */
kvs_result_t kvs_vector_setv(kvs_vector_store_t *store, char *collection_name, int len_collection_name,
                             char *member, int len_member, const void *vector, int len_vector) {
    vector_collection_t *collection = kvs_vector_store_get(store, collection_name, len_collection_name);

    if (collection == NULL) return KVS_RES_NOT_FOUND;

    /* 协议层传进来的是原始 bulk bytes，这里统一按 float32 向量解释。 */
    if (vector == NULL || len_vector != (int)(collection->dim * sizeof(float))) return KVS_RES_ERR;

    return kvs_vector_set_float32(collection, member, len_member, (const float *)vector, collection->dim);
}

/* GETV 的 storage 层入口；执行向量检索并把 topK 写入调用方提供的 results。 */
kvs_result_t kvs_vector_getv(kvs_vector_store_t *store, char *collection_name, int len_collection_name,
                             const void *query_vector, int len_query_vector, uint32_t topk,
                             kvs_vector_search_result_t *results, uint32_t *result_count) {
    vector_collection_t *collection = kvs_vector_store_get(store, collection_name, len_collection_name);

    if (collection == NULL) return KVS_RES_NOT_FOUND;

    /* query 也必须是完整的 dim 个 float32。 */
    if (query_vector == NULL || len_query_vector != (int)(collection->dim * sizeof(float))) return KVS_RES_ERR;

    return kvs_vector_search_float32(collection, (const float *)query_vector, collection->dim, topk, results, result_count);
}

/* DELV 的 storage 层入口。 */
kvs_result_t kvs_vector_delv(kvs_vector_store_t *store, char *collection_name, int len_collection_name,
                             char *member, int len_member) {
    vector_collection_t *collection = kvs_vector_store_get(store, collection_name, len_collection_name);

    if (collection == NULL) return KVS_RES_NOT_FOUND;
    return kvs_vector_del(collection, member, len_member);
}

/* VINFO 的 storage 层入口。 */
kvs_result_t kvs_vector_vinfo(kvs_vector_store_t *store, char *collection_name, int len_collection_name,
                              kvs_vector_info_t *info) {
    vector_collection_t *collection = kvs_vector_store_get(store, collection_name, len_collection_name);

    if (collection == NULL) return KVS_RES_NOT_FOUND;
    return kvs_vector_info(collection, info);
}

/*
 * 遍历全局 vector_store 中的所有 collection。
 *
 * RDB/full-sync 会用这个接口导出 collection 元信息和 active 向量。这里不暴露
 * kvs_vector_store_t 的内部 bucket 结构给 server 层，保持 storage 模块边界清晰。
 */
int kvs_vector_store_foreach_collection(kvs_vector_store_t *store,
                                        kvs_vector_collection_iter_cb cb,
                                        void *arg) {
    if (store == NULL || cb == NULL) return -1;

    for (uint32_t i = 0; i < store->bucket_count; i++) {
        struct vector_collection_node *node = store->buckets[i];
        while (node != NULL) {
            if (node->collection != NULL && cb(node->collection, arg) != 0) {
                return -1;
            }
            node = node->next;
        }
    }

    return 0;
}

/*
 * 遍历一个 collection 中所有未删除的向量。
 *
 * 当前 FLAT 存储里的向量就是连续 float32；HNSW 后续实现后，只要 collection 的
 * 主数据仍然保留在 blocks 中，RDB 导出逻辑无需变化。
 */
int kvs_vector_collection_foreach_item(vector_collection_t *collection,
                                       kvs_vector_item_iter_cb cb,
                                       void *arg) {
    if (collection == NULL || cb == NULL) return -1;

    for (uint32_t b = 0; b < collection->block_count; b++) {
        vector_block_t *block = collection->blocks[b];
        if (block == NULL) continue;

        for (uint32_t i = 0; i < block->count; i++) {
            float *item;

            if (block->deleted[i] || block->members[i] == NULL) continue;

            item = block->data + ((size_t)i * collection->dim);
            if (cb(collection, block->members[i], (int)strlen(block->members[i]),
                   item, collection->dim, arg) != 0) {
                return -1;
            }
        }
    }

    return 0;
}

/* 创建一个具体 collection，并初始化 member_to_id 映射表。 */
vector_collection_t *kvs_vector_collection_create(char *name, int len_name, uint32_t dim,
                                                  vec_metric_t metric, vec_index_type_t index_type,
                                                  uint32_t block_size) {
    vector_collection_t *collection;

    if (name == NULL || len_name <= 0 || dim == 0) return NULL;
    if (index_type != VEC_INDEX_FLAT) return NULL;

    collection = (vector_collection_t *)kvs_malloc(sizeof(*collection));
    if (collection == NULL) return NULL;

    memset(collection, 0, sizeof(*collection));

    collection->name = (char *)kvs_malloc((size_t)len_name + 1);
    if (collection->name == NULL) {
        kvs_free(collection, sizeof(*collection));
        return NULL;
    }
    memcpy(collection->name, name, (size_t)len_name);
    collection->name[len_name] = '\0';

    collection->dim = dim;
    collection->block_size = block_size == 0 ? KVS_VECTOR_DEFAULT_BLOCK_SIZE : block_size;
    collection->metric = metric;
    collection->index_type = index_type;
    collection->member_to_id = kvs_vector_member_map_create(KVS_VECTOR_DEFAULT_BUCKETS);
    if (collection->member_to_id == NULL) {
        kvs_free(collection->name, (size_t)len_name + 1);
        kvs_free(collection, sizeof(*collection));
        return NULL;
    }

    return collection;
}

/* 销毁 collection 的全部 block、member map 和元信息。 */
void kvs_vector_collection_destroy(vector_collection_t *collection) {
    if (collection == NULL) return;

    for (uint32_t i = 0; i < collection->block_count; i++) {
        kvs_vector_block_destroy(collection->blocks[i], collection->dim);
    }

    if (collection->blocks != NULL) {
        kvs_free(collection->blocks, sizeof(vector_block_t *) * collection->block_capacity);
    }

    kvs_vector_member_map_destroy((struct vector_member_map *)collection->member_to_id);
    kvs_free(collection->name, strlen(collection->name) + 1);
    kvs_free(collection, sizeof(*collection));
}

/*
 * 向 collection 写入或覆盖一个 FLOAT32 向量。
 *
 * 如果 member 已存在，直接覆盖原 slot 的向量数据；如果这个 slot 之前被 lazy delete，
 * 会重新激活并修正 active/deleted 计数。不存在时追加到最后一个 block，满了就新建 block。
 */
kvs_result_t kvs_vector_set_float32(vector_collection_t *collection, char *member, int len_member,
                                    const float *vector, uint32_t dim) {
    uint64_t id;
    vector_block_t *block;
    uint32_t offset;

    if (collection == NULL || member == NULL || len_member <= 0 || vector == NULL) return KVS_RES_ERR;
    if (dim != collection->dim) return KVS_RES_ERR;

    /* member 已存在：覆盖原位置，保持 vector_id 不变。 */
    if (kvs_vector_member_map_get((struct vector_member_map *)collection->member_to_id, member, len_member, &id)) {
        if (kvs_vector_get_slot(collection, id, &block, &offset) != 0) return KVS_RES_ERR;
        memcpy(block->data + ((size_t)offset * collection->dim), vector, sizeof(float) * collection->dim);
        if (block->deleted[offset]) {
            block->deleted[offset] = 0;
            collection->active_count++;
            collection->deleted_count--;
        }
        return KVS_RES_OK;
    }

    /* member 不存在：追加到最后一个 block，最后一个 block 满了就分配新 block。 */
    if (collection->block_count == 0 || collection->blocks[collection->block_count - 1]->count >= collection->block_size) {
        if (kvs_vector_collection_reserve_blocks(collection) != 0) return KVS_RES_ERR;
        block = kvs_vector_block_create(collection->block_size, collection->dim);
        if (block == NULL) return KVS_RES_ERR;
        collection->blocks[collection->block_count++] = block;
    } else {
        block = collection->blocks[collection->block_count - 1];
    }

    offset = block->count++;
    id = collection->next_id++;

    block->members[offset] = (char *)kvs_malloc((size_t)len_member + 1);
    if (block->members[offset] == NULL) return KVS_RES_ERR;
    memcpy(block->members[offset], member, (size_t)len_member);
    block->members[offset][len_member] = '\0';
    memcpy(block->data + ((size_t)offset * collection->dim), vector, sizeof(float) * collection->dim);
    block->deleted[offset] = 0;

    if (kvs_vector_member_map_set((struct vector_member_map *)collection->member_to_id, member, len_member, id) != 0) {
        kvs_free(block->members[offset], (size_t)len_member + 1);
        block->members[offset] = NULL;
        block->deleted[offset] = 1;
        return KVS_RES_ERR;
    }

    collection->total_count++;
    collection->active_count++;

    return KVS_RES_OK;
}

/*
 * FLAT 检索实现。
 *
 * 线性扫描所有 block 的 active slot，计算 query 与每个向量的分数，
 * 再通过 kvs_vector_topk_insert 维护 topK。HNSW 后续会在这里按 index_type 分流。
 */
kvs_result_t kvs_vector_search_float32(vector_collection_t *collection, const float *query, uint32_t dim,
                                       uint32_t topk, kvs_vector_search_result_t *results,
                                       uint32_t *result_count) {
    uint32_t count = 0;

    if (result_count != NULL) *result_count = 0;
    if (collection == NULL || query == NULL || results == NULL || result_count == NULL) return KVS_RES_ERR;
    if (dim != collection->dim || topk == 0) return KVS_RES_ERR;
    if (collection->index_type != VEC_INDEX_FLAT) return KVS_RES_ERR;

    for (uint32_t b = 0; b < collection->block_count; b++) {
        vector_block_t *block = collection->blocks[b];
        if (block == NULL) continue;

        for (uint32_t i = 0; i < block->count; i++) {
            float *item;
            float score;
            uint64_t id;

            /* lazy deleted 的 slot 不参与检索。 */
            if (block->deleted[i] || block->members[i] == NULL) continue;

            item = block->data + ((size_t)i * collection->dim);
            score = kvs_vector_score(collection->metric, query, item, collection->dim);
            id = ((uint64_t)b * collection->block_size) + i;
            kvs_vector_topk_insert(results, &count, topk, block->members[i], id, score);
        }
    }

    *result_count = count;
    return count == 0 ? KVS_RES_NOT_FOUND : KVS_RES_VAL;
}

/*
 * 删除一个 member 对应的向量。
 *
 * 当前采用 lazy delete：只标记 deleted，并从 member_to_id 中移除。
 * block 中的 slot 不立即回收，后续可以增加 compact/rebuild 机制。
 */
kvs_result_t kvs_vector_del(vector_collection_t *collection, char *member, int len_member) {
    uint64_t id;
    vector_block_t *block;
    uint32_t offset;

    if (collection == NULL || member == NULL || len_member <= 0) return KVS_RES_ERR;

    if (!kvs_vector_member_map_get((struct vector_member_map *)collection->member_to_id, member, len_member, &id)) {
        return KVS_RES_NOT_FOUND;
    }

    if (kvs_vector_get_slot(collection, id, &block, &offset) != 0) return KVS_RES_ERR;
    if (block->deleted[offset]) return KVS_RES_NOT_FOUND;

    block->deleted[offset] = 1;
    collection->active_count--;
    collection->deleted_count++;
    kvs_vector_member_map_del((struct vector_member_map *)collection->member_to_id, member, len_member);

    return KVS_RES_OK;
}

/* 填充 collection 的只读元信息，供 VINFO 响应使用。 */
kvs_result_t kvs_vector_info(vector_collection_t *collection, kvs_vector_info_t *info) {
    if (collection == NULL || info == NULL) return KVS_RES_ERR;

    info->name = collection->name;
    info->len_name = (int)strlen(collection->name);
    info->dim = collection->dim;
    info->block_size = collection->block_size;
    info->block_count = collection->block_count;
    info->total_count = collection->total_count;
    info->active_count = collection->active_count;
    info->deleted_count = collection->deleted_count;
    info->metric = collection->metric;
    info->index_type = collection->index_type;

    return KVS_RES_VAL;
}
