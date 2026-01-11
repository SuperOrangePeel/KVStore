#ifndef __KVS_ARRAY_H__
#define __KVS_ARRAY_H__

typedef struct kvs_array_item_s {
	char *key;
	char *value;
//#if (KVS_PROTOCOL_SELECT == KVS_RESP)
	int len_key;
	int len_val;
//#endif 
} kvs_array_item_t;

#define KVS_ARRAY_SIZE		1024

typedef struct kvs_array_s {
	kvs_array_item_t *table;
	int idx;
	int total;
} kvs_array_t;

kvs_array_t *kvs_array_create(int size);
void kvs_array_destroy(kvs_array_t *inst);

int kvs_array_set(kvs_array_t *inst, char *key, char *value);
char* kvs_array_get(kvs_array_t *inst, char *key);
int kvs_array_del(kvs_array_t *inst, char *key);
int kvs_array_mod(kvs_array_t *inst, char *key, char *value);
int kvs_array_exist(kvs_array_t *inst, char *key);



int kvs_array_resp_set(kvs_array_t *inst, char *key, int len_key, char *value, int len_val);
int kvs_array_resp_get(kvs_array_t *inst, char *key, int len_key, char **value, int *len_val);
int kvs_array_resp_del(kvs_array_t *inst, char *key, int len_key);
int kvs_array_resp_mod(kvs_array_t *inst, char *key, int len_key, char *value, int len_val);
int kvs_array_resp_exist(kvs_array_t *inst, char* key, int len_key);

typedef void(*kvs_array_item_filter)(char *key, int len_key, char *value, int len_val, void* arg);
void kvs_array_filter(kvs_array_t *inst, kvs_array_item_filter filter, void* filter_ctx);

#endif