#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "logger.h"


kvs_result_t kvs_server_set(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return KVS_RES_ERR;
	}
	LOG_DEBUG("server set key: %.*s, len_key: %d, value: %.*s, len_val: %d", 
		len_key, key, len_key, len_val, value, len_val);
	int ret = kvs_array_resp_set(server->array, key, len_key, value, len_val);
	if(ret == -2) {
		return KVS_RES_EXIST;
	} else if(ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_get(struct kvs_server_s *server,  char* key, int len_key, char** value_out, int* len_val_out) {
	if(server == NULL || key == NULL || len_key <=0 || value_out == NULL || len_val_out == NULL) {
		return KVS_RES_ERR;
	}
	int ret = kvs_array_resp_get(server->array, key, len_key, value_out, len_val_out);
	if(ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if(ret >= 0) {
		return KVS_RES_VAL;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_del(struct kvs_server_s *server, char* key, int len_key) {
	if(server == NULL || key == NULL || len_key <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_array_resp_del(server->array, key, len_key);
	if(ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if (ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_mod(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_array_resp_mod(server->array, key, len_key, value, len_val);
	if (ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if (ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_exist(struct kvs_server_s *server, char* key, int len_key) {
	if(server == NULL || key == NULL || len_key <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_array_resp_exist(server->array, key, len_key);
	if (ret >= 0) {
		return KVS_RES_EXIST;
	} else if (ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_rset(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_rbtree_resp_set(server->rbtree, key, len_key, value, len_val);
	if(ret == -2) {
		return KVS_RES_EXIST;
	} else if(ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_rget(struct kvs_server_s *server,  char* key, int len_key, char** value_out, int* len_val_out) {
	if(server == NULL || key == NULL || len_key <=0 || value_out == NULL || len_val_out == NULL) {
		return KVS_RES_ERR;
	}
	int ret = kvs_rbtree_resp_get(server->rbtree, key, len_key, value_out, len_val_out);
	if(ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if(ret >= 0) {
		return KVS_RES_VAL;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_rdel(struct kvs_server_s *server, char* key, int len_key) {
	if(server == NULL || key == NULL || len_key <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_rbtree_resp_del(server->rbtree, key, len_key);
	if(ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if (ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_rmod(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_rbtree_resp_mod(server->rbtree, key, len_key, value, len_val);
	if (ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if (ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_rexist(struct kvs_server_s *server, char* key, int len_key) {
	if(server == NULL || key == NULL || len_key <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_rbtree_resp_exist(server->rbtree, key, len_key);
	if (ret >= 0) {
		return KVS_RES_EXIST;
	} else if (ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_hset(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_hash_resp_set(server->hash, key, len_key, value, len_val);
	if(ret == -2) {
		return KVS_RES_EXIST;
	} else if(ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_hget(struct kvs_server_s *server,  char* key, int len_key, char** value_out, int* len_val_out) {
	if(server == NULL || key == NULL || len_key <=0 || value_out == NULL || len_val_out == NULL) {
		return KVS_RES_ERR;
	}
	int ret = kvs_hash_resp_get(server->hash, key, len_key, value_out, len_val_out);
	if(ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if(ret >= 0) {
		return KVS_RES_VAL;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_hdel(struct kvs_server_s *server, char* key, int len_key) {
	if(server == NULL || key == NULL || len_key <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_hash_resp_del(server->hash, key, len_key);
	if(ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if (ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_hmod(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_hash_resp_mod(server->hash, key, len_key, value, len_val);
	if (ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else if (ret >= 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_hexist(struct kvs_server_s *server, char* key, int len_key) {
	if(server == NULL || key == NULL || len_key <=0) {
		return KVS_RES_ERR;
	}
	int ret = kvs_hash_resp_exist(server->hash, key, len_key);
	if (ret >= 0) {
		return KVS_RES_EXIST;
	} else if (ret == -2) {
		return KVS_RES_NOT_FOUND;
	} else {
		return KVS_RES_ERR;
	}
}