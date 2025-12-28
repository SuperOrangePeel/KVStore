


#ifndef __KV_STORE_H__
#define __KV_STORE_H__


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>


#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

#define NETWORK_SELECT		NETWORK_PROACTOR

// protocol
#define KVS_1R1R 	0// one request one response 
#define KVS_RESP	1
#define KVS_PROTOCOL_SELECT KVS_RESP

#define KVS_MAX_TOKENS		128

#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1
#define ENABLE_HASH			1


typedef int (*msg_handler)(char *msg, int length, char *response, int* length_r);


extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

#if (KVS_PROTOCOL_SELECT == KVS_RESP)

#define KVS_RESP_CMD_MAX 1024

typedef struct kvs_resp_cmd_s {
	char* cmd;
	int len_cmd;
	char* key;
	int len_key;
	char* val;
	int len_val;
}kvs_resp_cmd_t;

#endif


#if ENABLE_ARRAY

typedef struct kvs_array_item_s {
	char *key;
	char *value;
#if (KVS_PROTOCOL_SELECT == KVS_RESP)
	int len_key;
	int len_val;
#endif 
} kvs_array_item_t;

#define KVS_ARRAY_SIZE		1024

typedef struct kvs_array_s {
	kvs_array_item_t *table;
	int idx;
	int total;
} kvs_array_t;

int kvs_array_create(kvs_array_t *inst);
void kvs_array_destory(kvs_array_t *inst);

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
#endif


#if ENABLE_RBTREE

#define RED				1
#define BLACK 			2

#define ENABLE_KEY_CHAR		1

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE; // key
#endif

typedef struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;
	KEY_TYPE key;
	void *value;
#if (KVS_PROTOCOL_SELECT == KVS_RESP)
	int len_key;
	int len_val;
#endif 
} rbtree_node;

typedef struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
} rbtree;


typedef struct _rbtree kvs_rbtree_t;

int kvs_rbtree_create(kvs_rbtree_t *inst);
void kvs_rbtree_destory(kvs_rbtree_t *inst);
int kvs_rbtree_set(kvs_rbtree_t *inst, char *key, char *value);
char* kvs_rbtree_get(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_del(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_mod(kvs_rbtree_t *inst, char *key, char *value);
int kvs_rbtree_exist(kvs_rbtree_t *inst, char *key);



#endif


#if ENABLE_HASH

#define MAX_KEY_LEN	128
#define MAX_VALUE_LEN	512
#define MAX_TABLE_SIZE	1024

#define ENABLE_KEY_POINTER	1


typedef struct hashnode_s {
#if ENABLE_KEY_POINTER
	char *key;
	char *value;
#else
	char key[MAX_KEY_LEN];
	char value[MAX_VALUE_LEN];
#endif
	struct hashnode_s *next;
	
} hashnode_t;


typedef struct hashtable_s {

	hashnode_t **nodes; //* change **, 

	int max_slots;
	int count;

} hashtable_t;

typedef struct hashtable_s kvs_hash_t;


int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destory(kvs_hash_t *hash);
int kvs_hash_set(hashtable_t *hash, char *key, char *value);
char * kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);


#endif


void *kvs_malloc(size_t size);
void kvs_free(void *ptr);




#endif



