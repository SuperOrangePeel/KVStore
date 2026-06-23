#ifndef __KVS_EXPIRE_H__
#define __KVS_EXPIRE_H__

#include <stdint.h>

typedef struct kvs_expire_table_s kvs_expire_table_t;
typedef void (*kvs_expire_callback)(char *key, int len_key, void *arg);

kvs_expire_table_t *kvs_expire_create(int slots);
void kvs_expire_destroy(kvs_expire_table_t *table);
int kvs_expire_set(kvs_expire_table_t *table, const char *key, int len_key,
                   uint64_t ttl_seconds);
void kvs_expire_remove(kvs_expire_table_t *table, const char *key, int len_key);
int kvs_expire_is_expired(kvs_expire_table_t *table, const char *key, int len_key);
void kvs_expire_active_cycle(kvs_expire_table_t *table, int samples,
                             kvs_expire_callback callback, void *arg);

#endif
