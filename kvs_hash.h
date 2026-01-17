#ifndef __KVS_HASH_H__
#define __KVS_HASH_H__

typedef struct hashtable_s kvs_hash_t;

#define KVS_MAX_HASH_SIZE	5242880 // 1048576 // 1024 * 1024

kvs_hash_t *kvs_hash_create(int size);
void kvs_hash_destroy(kvs_hash_t *hash);
int kvs_hash_set(kvs_hash_t *hash, char *key, char *value);
char * kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);

int kvs_hash_resp_set(kvs_hash_t *hash, char *key, int len_key, char *value, int len_val);
int kvs_hash_resp_get(kvs_hash_t *hash, char *key, int len_key, char **value, int *len_val);
int kvs_hash_resp_del(kvs_hash_t *hash, char *key, int len_key);
int kvs_hash_resp_mod(kvs_hash_t *hash, char *key, int len_key, char *value, int len_val);
int kvs_hash_resp_exist(kvs_hash_t *hash, char* key, int len_key);

typedef int(*kvs_hash_item_filter)(char *key, int len_key, char *value, int len_val, void* arg);
int kvs_hash_filter(kvs_hash_t *hash, kvs_hash_item_filter filter, void *arg);

#endif