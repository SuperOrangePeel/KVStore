#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_expire.h"
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
	if(ret >= 0) {
		kvs_expire_remove(server->expires, key, len_key);
	}
	if(ret == 1) {
		return KVS_RES_EXIST;
	} else if(ret == 0) {
		return KVS_RES_OK;
	} else {
		return KVS_RES_ERR;
	}
}

kvs_result_t kvs_server_hget(struct kvs_server_s *server,  char* key, int len_key, char** value_out, int* len_val_out) {
	if(server == NULL || key == NULL || len_key <=0 || value_out == NULL || len_val_out == NULL) {
		return KVS_RES_ERR;
	}
	if(kvs_expire_is_expired(server->expires, key, len_key)) {
		kvs_server_hdel(server, key, len_key);
		return KVS_RES_NOT_FOUND;
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
	kvs_expire_remove(server->expires, key, len_key);
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
	if(kvs_expire_is_expired(server->expires, key, len_key)) {
		kvs_server_hdel(server, key, len_key);
		return KVS_RES_NOT_FOUND;
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
	if(kvs_expire_is_expired(server->expires, key, len_key)) {
		kvs_server_hdel(server, key, len_key);
		return KVS_RES_NOT_FOUND;
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
kvs_result_t kvs_server_hsetex(struct kvs_server_s *server, char* key, int len_key,
        char* value, int len_val, unsigned long long ttl_seconds) {
    int ret;
    if(server == NULL || key == NULL || len_key <= 0 || value == NULL || len_val <= 0 || ttl_seconds == 0) {
        return KVS_RES_ERR;
    }
    ret = kvs_hash_resp_set(server->hash, key, len_key, value, len_val);
    if(ret < 0 || kvs_expire_set(server->expires, key, len_key, ttl_seconds) < 0) {
        return KVS_RES_ERR;
    }
    return KVS_RES_OK;
}

kvs_result_t kvs_server_createv(struct kvs_server_s *server, char *name, int len_name,
        uint32_t dim, vec_metric_t metric, vec_index_type_t index_type) {
    if(server == NULL || server->vector_store == NULL || name == NULL || len_name <= 0 || dim == 0) {
        return KVS_RES_ERR;
    }
    return kvs_vector_createv(server->vector_store, name, len_name, dim, metric, index_type);
}

kvs_result_t kvs_server_setv(struct kvs_server_s *server, char *collection_name, int len_collection_name,
        char *member, int len_member, const void *vector, int len_vector) {
    if(server == NULL || server->vector_store == NULL || collection_name == NULL || len_collection_name <= 0 ||
       member == NULL || len_member <= 0 || vector == NULL || len_vector <= 0) {
        return KVS_RES_ERR;
    }
    return kvs_vector_setv(server->vector_store, collection_name, len_collection_name,
            member, len_member, vector, len_vector);
}

kvs_result_t kvs_server_getv(struct kvs_server_s *server, char *collection_name, int len_collection_name,
        const void *query_vector, int len_query_vector, uint32_t topk,
        kvs_vector_search_result_t *results, uint32_t *result_count) {
    if(server == NULL || server->vector_store == NULL || collection_name == NULL || len_collection_name <= 0 ||
       query_vector == NULL || len_query_vector <= 0 || topk == 0 || results == NULL || result_count == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_vector_getv(server->vector_store, collection_name, len_collection_name,
            query_vector, len_query_vector, topk, results, result_count);
}

kvs_result_t kvs_server_delv(struct kvs_server_s *server, char *collection_name, int len_collection_name,
        char *member, int len_member) {
    if(server == NULL || server->vector_store == NULL || collection_name == NULL || len_collection_name <= 0 ||
       member == NULL || len_member <= 0) {
        return KVS_RES_ERR;
    }
    return kvs_vector_delv(server->vector_store, collection_name, len_collection_name, member, len_member);
}

kvs_result_t kvs_server_vinfo(struct kvs_server_s *server, char *collection_name, int len_collection_name,
        kvs_vector_info_t *info) {
    if(server == NULL || server->vector_store == NULL || collection_name == NULL || len_collection_name <= 0 || info == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_vector_vinfo(server->vector_store, collection_name, len_collection_name, info);
}
