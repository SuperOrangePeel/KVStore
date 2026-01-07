#include "kvs_hash.h"
#include "common.h"


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>





// Key, Value --> 
// Modify 

//kvs_hash_t global_hash;

#define MAX_KEY_LEN	128
#define MAX_VALUE_LEN	512

#define ENABLE_KEY_POINTER	1


typedef struct hashnode_s {
#if ENABLE_KEY_POINTER
	char *key;
	char *value;
	int len_key;
	int len_val;
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





//Connection 
// 'C' + 'o' + 'n'
static int _hash(char *key, int size) {

	if (!key) return -1;

	int sum = 0;
	int i = 0;

	while (key[i] != 0) {
		sum += key[i];
		i ++;
	}

	return sum % size;

}

// static int _hash_resp(char *key, int len_key, int size) {

// 	if (!key) return -1;

// 	int sum = 0;
// 	int i = 0;

// 	while(i < len_key) {
// 		sum += key[i];
// 		i ++;
// 	}

// 	return sum % size;

// }

// DJB2 hash function 
static unsigned int _hash_resp(char *key, int len_key, int size) {
    if (!key) return 0;

    unsigned int hash = 5381; // 这是一个著名的初始魔数
    int i = 0;

    while (i < len_key) {
        // hash * 33 + key[i]
        // 使用位移 (hash << 5) + hash 代替乘法，速度极快
        hash = ((hash << 5) + hash) + key[i];
        i++;
    }

    // 优化点：如果 size 是 2 的幂，用 & (size - 1) 代替 % size
    return hash % size; 
}

hashnode_t *_create_node(char *key, char *value) {

	hashnode_t *node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
	if (!node) return NULL;
	
#if ENABLE_KEY_POINTER
	char *kcopy = kvs_malloc(strlen(key) + 1);
	if (kcopy == NULL) return NULL;
	memset(kcopy, 0, strlen(key) + 1);
	strncpy(kcopy, key, strlen(key));

	node->key = kcopy;

	char *kvalue = kvs_malloc(strlen(value) + 1);
	if (kvalue == NULL) { 
		kvs_free(kcopy, strlen(key) + 1);
		return NULL;
	}
	memset(kvalue, 0, strlen(value) + 1);
	strncpy(kvalue, value, strlen(value));

	node->value = kvalue;
	
#else
	strncpy(node->key, key, MAX_KEY_LEN);
	strncpy(node->value, value, MAX_VALUE_LEN);
#endif
	node->next = NULL;

	return node;
}

hashnode_t *_create_node_resp(char *key, int len_key, char *value, int len_val) {

	hashnode_t *node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
	if (!node) return NULL;
	
	char *kcopy = kvs_malloc(len_key);
	if (kcopy == NULL) return NULL;
	memset(kcopy, 0, len_key);
	memcpy(kcopy, key, len_key);

	node->key = kcopy;
	node->len_key = len_key;

	char *kvalue = kvs_malloc(len_val);
	if (kvalue == NULL) { 
		kvs_free(kcopy, len_key);
		return NULL;
	}
	memset(kvalue, 0, len_val);
	memcpy(kvalue, value, len_val);

	node->value = kvalue;
	node->len_val = len_val;

	node->next = NULL;

	return node;
}

//
kvs_hash_t *kvs_hash_create(int size) {

	kvs_hash_t *hash = (kvs_hash_t *)kvs_malloc(sizeof(kvs_hash_t));

	hash->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * size);
	if (!hash->nodes) return NULL;

	hash->max_slots = size;
	hash->count = 0; 

	return hash;
}

// 
void kvs_hash_destroy(kvs_hash_t *hash) {

	if (!hash) return;

	int i = 0;
	for (i = 0;i < hash->max_slots;i ++) {
		hashnode_t *node = hash->nodes[i];

		while (node != NULL) { // error

			hashnode_t *tmp = node;
			node = node->next;
			hash->nodes[i] = node;
			
			kvs_free(tmp, sizeof(hashnode_t));
			
		}
	}

	kvs_free(hash->nodes, sizeof(hashnode_t*) * hash->max_slots);
	
}

// 5 + 2

// mp
int kvs_hash_set(kvs_hash_t *hash, char *key, char *value) {

	if (!hash || !key || !value) return -1;

	int idx = _hash(key, KVS_MAX_HASH_SIZE);

	hashnode_t *node = hash->nodes[idx];
#if 1
	while (node != NULL) {
		if (strcmp(node->key, key) == 0) { // exist
			return 1;
		}
		node = node->next;
	}
#endif

	hashnode_t *new_node = _create_node(key, value);
	new_node->next = hash->nodes[idx];
	hash->nodes[idx] = new_node;
	
	hash->count ++;

	return 0;
}


char * kvs_hash_get(kvs_hash_t *hash, char *key) {

	if (!hash || !key) return NULL;

	int idx = _hash(key, KVS_MAX_HASH_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (strcmp(node->key, key) == 0) {
			return node->value;
		}

		node = node->next;
	}

	return NULL;

}


int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value) {

	if (!hash || !key) return -1;

	int idx = _hash(key, KVS_MAX_HASH_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (strcmp(node->key, key) == 0) {
			break;
		}

		node = node->next;
	}

	if (node == NULL) {
		return 1;
	}

	// node --> 
	kvs_free(node->value, strlen(node->value) + 1);

	char *kvalue = kvs_malloc(strlen(value) + 1);
	if (kvalue == NULL) return -2;
	memset(kvalue, 0, strlen(value) + 1);
	strncpy(kvalue, value, strlen(value));

	node->value = kvalue;
	
	return 0;
}

int kvs_hash_count(kvs_hash_t *hash) {
	return hash->count;
}

int kvs_hash_del(kvs_hash_t *hash, char *key) {
	if (!hash || !key) return -2;

	int idx = _hash(key, KVS_MAX_HASH_SIZE);

	hashnode_t *head = hash->nodes[idx];
	if (head == NULL) return -1; // noexist
	// head node
	if (strcmp(head->key, key) == 0) {
		hashnode_t *tmp = head->next;
		hash->nodes[idx] = tmp;
		kvs_free(head->key, head->len_key);
		kvs_free(head->value, head->len_val);
		kvs_free(head, sizeof(hashnode_t));
		hash->count --;
		
		return 0;
	}

	hashnode_t *cur = head;
	while (cur->next != NULL) {
		if (strcmp(cur->next->key, key) == 0) break; // search node
		
		cur = cur->next;
	}

	if (cur->next == NULL) {
		
		return -1;
	}

	hashnode_t *tmp = cur->next;
	cur->next = tmp->next;
#if ENABLE_KEY_POINTER
	kvs_free(tmp->key, tmp->len_key);
	kvs_free(tmp->value, tmp->len_val);
#endif
	kvs_free(tmp, sizeof(hashnode_t));
	
	hash->count --;

	return 0;
}


int kvs_hash_exist(kvs_hash_t *hash, char *key) {

	char *value = kvs_hash_get(hash, key);
	if (!value) return 1;

	return 0;
	
}

/*
 *@return >=0 success -1 error -2 exist
 */
int kvs_hash_resp_set(kvs_hash_t *hash, char *key, int len_key, char *value, int len_val) {

	if (!hash || !key || !value || len_key <=0 || len_val <=0) return -1;

	int idx = _hash_resp(key, len_key, KVS_MAX_HASH_SIZE);
	hashnode_t *node = hash->nodes[idx];
	while(node != NULL) {
		if (node->len_key == len_key && memcmp(node->key, key, len_key) == 0) {
			return -2;
		}
		node = node->next;
	}

	hashnode_t *new_node = _create_node_resp(key, len_key, value, len_val);
	new_node->next = hash->nodes[idx];
	hash->nodes[idx] = new_node;

	hash->count ++;

	return 0;
	
}

/*
 *@return >=0 success -1 error -2 not exist
 */
int kvs_hash_resp_get(kvs_hash_t *hash, char *key, int len_key, char **value, int *len_val) {

	if (!hash || !key || len_key <=0 || !value || !len_val) return -1;

	int idx = _hash_resp(key, len_key, KVS_MAX_HASH_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (node->len_key == len_key && memcmp(node->key, key, len_key) == 0) {
			*value = node->value;
			*len_val = node->len_val;
			return 0;
		}

		node = node->next;
	}

	return -2;
}

/*
 * @return -1 error; 0 success;  -2 not exist
 */
int kvs_hash_resp_del(kvs_hash_t *hash, char *key, int len_key) {
	if (!hash || !key || len_key <=0) return -1;

	int idx = _hash_resp(key, len_key, KVS_MAX_HASH_SIZE);

	hashnode_t *head = hash->nodes[idx];
	if (head == NULL) return -2; // not exist
	// head node
	if (head->len_key == len_key && memcmp(head->key, key, len_key) == 0) {
		hashnode_t *tmp = head->next;
		hash->nodes[idx] = tmp;

		kvs_free(head->key, head->len_key);
		kvs_free(head->value, head->len_val);
		
		kvs_free(head, sizeof(hashnode_t));
		hash->count --;
		return 0;
	}

	hashnode_t *cur = head;
	while (cur->next != NULL) {
		if (cur->next->len_key == len_key && memcmp(cur->next->key, key, len_key) == 0) break; // search node

		cur = cur->next;
	}

	if (cur->next == NULL) return -2;

	hashnode_t *tmp = cur->next;
	cur->next = tmp->next;
	
	kvs_free(tmp->key, tmp->len_key);
	kvs_free(tmp->value, tmp->len_val);
	kvs_free(tmp, sizeof(hashnode_t));
	hash->count --;
	return 0;
}

/*
 * @return >=0 success -1 error -2 not exist
 */
int kvs_hash_resp_mod(kvs_hash_t *hash, char *key, int len_key, char *value, int len_val) {

	if (!hash || !key || len_key <=0 || !value || len_val <=0) return -1;

	int idx = _hash_resp(key, len_key, KVS_MAX_HASH_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (node->len_key == len_key && memcmp(node->key, key, len_key) == 0) {
			break;
		}

		node = node->next;
	}

	if (node == NULL) {
		return -2;
	}

	// node --> 
	kvs_free(node->value, node->len_val);

	char *kvalue = kvs_malloc(len_val);
	if (kvalue == NULL) return -1;
	memset(kvalue, 0, len_val);
	memcpy(kvalue, value, len_val);

	node->value = kvalue;
	node->len_val = len_val;

	return 0;
}


/*
 * @return >=0 success -1 error -2 not exist
 */
int kvs_hash_resp_exist(kvs_hash_t *hash, char* key, int len_key) {

	if(!hash || !key || len_key <=0) return -1;

	int idx = _hash_resp(key, len_key, KVS_MAX_HASH_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (node->len_key == len_key && memcmp(node->key, key, len_key) == 0) {
			return 0;
		}

		node = node->next;
	}

	return -2;

}



void kvs_hash_filter(kvs_hash_t *hash, kvs_item_callback callback, void *arg) {
	if(!hash || !callback) return;

	int i = 0;
	for(i = 0; i < hash->max_slots; ++ i) {
		hashnode_t *node = hash->nodes[i];
		while(node != NULL) {
			callback(node->key, node->len_key, node->value, node->len_val, arg);
			node = node->next;
		}
	}
}

#if 0
int main() {

	kvs_hash_create(&hash);

	kvs_hash_set(&hash, "Teacher1", "King");
	kvs_hash_set(&hash, "Teacher2", "Darren");
	kvs_hash_set(&hash, "Teacher3", "Mark");
	kvs_hash_set(&hash, "Teacher4", "Vico");
	kvs_hash_set(&hash, "Teacher5", "Nick");

	char *value1 = kvs_hash_get(&hash, "Teacher1");
	printf("Teacher1 : %s\n", value1);

	int ret = kvs_hash_mod(&hash, "Teacher1", "King1");
	printf("mode Teacher1 ret : %d\n", ret);
	
	char *value2 = kvs_hash_get(&hash, "Teacher1");
	printf("Teacher2 : %s\n", value1);

	ret = kvs_hash_del(&hash, "Teacher1");
	printf("delete Teacher1 ret : %d\n", ret);

	ret = kvs_hash_exist(&hash, "Teacher1");
	printf("Exist Teacher1 ret : %d\n", ret);

	kvs_hash_destroy(&hash);

	return 0;
}

#endif


