
#include "kvs_array.h"
#include "common.h"


#include <stdio.h>
#include <string.h>

// singleton

//kvs_array_t global_array = {0};

kvs_array_t *kvs_array_create(int size) {
	if(size <= 0) return NULL;
	kvs_array_t *inst = (kvs_array_t *)kvs_malloc(sizeof(kvs_array_t));
	memset(inst, 0, sizeof(kvs_array_t));
	if (inst->table) {
		printf("table has alloc\n");
		return NULL;
	}	
	inst->table = kvs_malloc(size * sizeof(kvs_array_item_t));
	if (!inst->table) {
		return NULL;
	}

	inst->total = 0;

	return inst;
}

void kvs_array_destroy(kvs_array_t *inst) {

	if (!inst) return ;

	if (inst->table) {
		kvs_free(inst->table, KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
	}

}


/*
 * @return: <0, error; =0, success; >0, exist
 */

int kvs_array_set(kvs_array_t *inst, char *key, char *value) {

	if (inst == NULL || key == NULL || value == NULL) return -1;
	if (inst->total == KVS_ARRAY_SIZE) return -1;

	char *str = kvs_array_get(inst, key);
	if (str) {
		return 1; // 
	}

	char *kcopy = kvs_malloc(strlen(key) + 1);
	if (kcopy == NULL) return -2;
	memset(kcopy, 0, strlen(key) + 1);
	strncpy(kcopy, key, strlen(key));

	char *kvalue = kvs_malloc(strlen(value) + 1);
	if (kvalue == NULL) return -2;
	memset(kvalue, 0, strlen(value) + 1);
	strncpy(kvalue, value, strlen(value));

	int i = 0;
	for (i = 0;i < inst->total;i ++) {
		if (inst->table[i].key == NULL) {
			
			inst->table[i].key = kcopy;
			inst->table[i].value = kvalue;
			inst->total ++;
			
			return 0;
		}
	}

	if (i == inst->total && i < KVS_ARRAY_SIZE) {

		inst->table[i].key = kcopy;
		inst->table[i].value = kvalue;
		inst->total ++;
	}

	return 0;
}

char* kvs_array_get(kvs_array_t *inst, char *key) {

	if (inst == NULL || key == NULL) return NULL;

	int i = 0;
	for (i = 0;i < inst->total;i ++) {
		if (inst->table[i].key == NULL) {
			continue;
		}

		if (strcmp(inst->table[i].key, key) == 0) {
			return inst->table[i].value;
		}
	}

	return NULL;
}


/*
 * @return < 0, error;  =0,  success; >0, no exist
 */

int kvs_array_del(kvs_array_t *inst, char *key) {

	if (inst == NULL || key == NULL) return -1;

	int i = 0;
	for (i = 0;i < inst->total;i ++) {

		if (strcmp(inst->table[i].key, key) == 0) {

			kvs_free(inst->table[i].key, inst->table[i].len_key);
			inst->table[i].key = NULL;

			kvs_free(inst->table[i].value, inst->table[i].len_val);
			inst->table[i].value = NULL;
// error: > 1024
			if (inst->total-1 == i) {
				inst->total --;
			}
			

			return 0;
		}
	}

	return i;
}


/*
 * @return : < 0, error; =0, success; >0, no exist 
 */
int kvs_array_mod(kvs_array_t *inst, char *key, char *value) {

	if (inst == NULL || key == NULL || value == NULL) return -1;
// error: > 1024
	if (inst->total == 0) {
		return KVS_ARRAY_SIZE;
	}
	

	int i = 0;
	for (i = 0;i < inst->total;i ++) {

		if (inst->table[i].key == NULL) {
			continue;
		}

		if (strcmp(inst->table[i].key, key) == 0) {

			kvs_free(inst->table[i].value, inst->table[i].len_val);

			char *kvalue = kvs_malloc(strlen(value) + 1);
			if (kvalue == NULL) return -2;
			memset(kvalue, 0, strlen(value) + 1);
			strncpy(kvalue, value, strlen(value));

			inst->table[i].value = kvalue;

			return 0;
		}

	}

	return i;
}


/*
 * @return 0: exist, 1: no exist
 */
int kvs_array_exist(kvs_array_t *inst, char *key) {

	if (!inst || !key) return -1;
	
	char *str = kvs_array_get(inst, key);
	if (!str) {
		return 1; // 
	}
	return 0;
}

/**
 * @return int >=0 success -1 error -2 exist;
 */ 
int kvs_array_resp_set(kvs_array_t *inst, char *key, int len_key, char *value, int len_val) {
	if (inst == NULL || key == NULL || len_key <= 0|| value == NULL || len_val <= 0) return -1;
	//printf("kvs_array_resp_set called\n");
	if (inst->total == KVS_ARRAY_SIZE) return -1;

	//printf("kvs_array_resp_exist before\n");
	int exist = kvs_array_resp_exist(inst, key, len_key);
	if (exist >= 0) {
		return -2;
	}
	//printf("kvs_array_resp_exist after\n");
	char *kcopy = kvs_malloc(len_key);
	if (kcopy == NULL) return -1;
	memset(kcopy, 0, len_key);
	memcpy(kcopy, key, len_key);

	char *kvalue = kvs_malloc(len_val);
	if (kvalue == NULL) return -1;
	memset(kvalue, 0, len_val);
	memcpy(kvalue, value, len_val);

	int i = 0;
	for (i = 0;i < inst->total;i ++) {
		if (inst->table[i].key == NULL) {
			
			inst->table[i].key = kcopy;
			inst->table[i].len_key = len_key;
			inst->table[i].value = kvalue;
			inst->table[i].len_val = len_val;
			inst->total ++;
			
			return i;
		}
	}

	if (i == inst->total && i < KVS_ARRAY_SIZE) {

		inst->table[i].key = kcopy;
		inst->table[i].len_key = len_key;
		inst->table[i].value = kvalue;
		inst->table[i].len_val = len_val;
		inst->total ++;
		
	}
	//printf("insert success: value:[%.*s] (len_val:%d)\n", len_val, value, len_val);

	return i;
}


/**
 * @return int >=0 success -1 error -2 not exist
 */
int kvs_array_resp_get(kvs_array_t *inst, char *key, int len_key, char **value, int *len_val) {

	if (inst == NULL || key == NULL || len_key <= 0 || value == NULL || len_val == NULL) return -1;

	int i = 0;
	for (i = 0;i < inst->total;i ++) {
		if (inst->table[i].key == NULL || inst->table[i].len_key != len_key) {
			continue;
		} 
		
		if(memcmp(inst->table[i].key, key, len_key) == 0) {
			*value = inst->table[i].value;
			*len_val = inst->table[i].len_val;
			return i;
		}
	}

	return -2;
}


/*
 * @return -1 error; >=0 success;  -2 no exist
 */
int kvs_array_resp_del(kvs_array_t *inst, char *key, int len_key) {

	if (inst == NULL || key == NULL || len_key <= 0) return -1;

	int idx = kvs_array_resp_exist(inst, key, len_key);
	if(idx < 0) {
		return idx;
	} else {
		kvs_free(inst->table[idx].key, inst->table[idx].len_key);
		inst->table[idx].key = NULL;
		inst->table[idx].len_key = 0;

		kvs_free(inst->table[idx].value, inst->table[idx].len_val);
		inst->table[idx].value = NULL;
		inst->table[idx].len_val = 0;

		if (inst->total-1 == idx) {
			inst->total --;
		}
	}

	return idx;
}

/*
 * @return >=0 success -1 error -2 not exist
 */
int kvs_array_resp_mod(kvs_array_t *inst, char *key, int len_key, char *value, int len_val) {
	if (inst == NULL || key == NULL || len_key <= 0|| value == NULL || len_val <= 0) return -1;
	int idx = kvs_array_resp_exist(inst, key, len_key);
	if(idx < 0) {
		return idx;
	} else {
		kvs_free(inst->table[idx].value, inst->table[idx].len_val);
		inst->table[idx].value = kvs_malloc(len_val);
		memset(inst->table[idx].value, 0, len_val);
		memcpy(inst->table[idx].value, value, len_val);
		inst->table[idx].len_val = len_val;
	}
	return idx;
}

/*
 * @return >=0 exist -1 error -2 not exist
 */
int kvs_array_resp_exist(kvs_array_t *inst, char* key, int len_key) {
	if(inst == NULL || key == NULL || len_key <= 0) return -1;
	int i = 0;
	for (i = 0;i < inst->total;i ++) {
		if (inst->table[i].key == NULL || inst->table[i].len_key != len_key) {
			continue;
		} 
		
		if(memcmp(inst->table[i].key, key, len_key) == 0) {
			return i;
		}
	}
	return -2;
}

int kvs_array_filter(kvs_array_t *inst, kvs_array_item_filter filter, void* filter_ctx) {
	if(!inst || !filter) return -1;
	int i = 0;
	int ret = 0;
	for(i = 0; i < inst->total; ++ i) {
		if(inst->table[i].key != NULL) {
			ret = filter(inst->table[i].key, inst->table[i].len_key, inst->table[i].value, inst->table[i].len_val, filter_ctx);
			if(ret < 0) {
				return -1;
			}
		}
	}
	return 0;
}