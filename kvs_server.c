#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_proactor.h"
#include "kvs_rbtree.h"
#include "kvs_persistence.h"
// #include "kvs_handler.h" // decoupling
#include "common.h"
#include "kvs_types.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h> 
#include <assert.h>
#include <fcntl.h>




// struct kvs_server_s *global_server = NULL;

int kvs_server_register_master(struct kvs_server_s *server, int master_fd) {
	struct kvs_conn_s *conn = kvs_proactor_register_fd(server->proactor, master_fd);
	kvs_proactor_set_recv_event(conn);
	return 0;
}

struct kvs_server_s *kvs_server_create(struct kvs_proactor_s *proactor_pt, struct kvs_server_config_s *config_pt) {
	if(proactor_pt == NULL || config_pt == NULL) {
		return NULL;
	}
	kvs_global_mempool_init(); // init global mempool first

	//1. init server struct
    struct kvs_server_s *server = (struct kvs_server_s *)kvs_malloc(sizeof(struct kvs_server_s));
	if(server == NULL) {
		return NULL;
	}
	memset(server, 0, sizeof(struct kvs_server_s));

	//2. init persistence context
    server->pers_ctx = kvs_persistence_create(&config_pt->pers_config);

	// 3. init data structures
	server->array = kvs_array_create(KVS_ARRAY_SIZE);
	if(server->array == NULL) {
		printf("kvs_array_create failed\n");
		assert(0);
	}
	server->hash = kvs_hash_create(KVS_MAX_HASH_SIZE);
	server->rbtree = kvs_rbtree_create();
	
	//printf("AOF recovery completed.\n");

    server->proactor = proactor_pt;

	// 4. init master/slave according to config
	if(config_pt->role & KVS_SERVER_ROLE_MASTER) {
		server->role = KVS_SERVER_ROLE_MASTER;
		server->master = (struct kvs_master_s*)kvs_malloc(sizeof(struct kvs_master_s));
		if(kvs_master_init(server->master, server, &config_pt->master_config) != KVS_OK) {
			printf("kvs_master_init failed\n");
			assert(0);
		}
	} else if(config_pt->role & KVS_SERVER_ROLE_SLAVE) {
		server->role = KVS_SERVER_ROLE_SLAVE;
		server->slave = (struct kvs_slave_s*)kvs_malloc(sizeof(struct kvs_slave_s));
		if(kvs_slave_init(server->slave, server, &config_pt->slave_config) != KVS_OK) {
			printf("kvs_slave_init failed\n");
			assert(0);
		}
		if(kvs_slave_connect_master(server->slave) != KVS_OK) {
			printf("kvs_slave_connect_master failed\n");
			assert(0);
		}
		kvs_server_register_master(server, server->slave->master_fd);
	} else {
		printf("invalid server role\n");
		assert(0);
	}
	
	// 5. init other components if needed
    return server;
}


void kvs_server_destroy(struct kvs_server_s *server) {
	if(server == NULL) {
		return;
	}
	if(server->pers_ctx) {
		kvs_persistence_destroy(server->pers_ctx);
		server->pers_ctx = NULL;
	}
	
	if(server->array) {
		kvs_array_destroy(server->array);
		server->array = NULL;
	}

	if(server->hash) {
		kvs_hash_destroy(server->hash);
		server->hash = NULL;
	}

	if(server->rbtree) {
		kvs_rbtree_destroy(server->rbtree);
		server->rbtree = NULL;
	}
	if(server->role == KVS_SERVER_ROLE_MASTER) {
		kvs_master_deinit(server->master);
		kvs_free(server->master, sizeof(struct kvs_master_s));
		server->master = NULL;
	} 
	if(server->role == KVS_SERVER_ROLE_SLAVE) {
		kvs_slave_deinit(server->slave);
		kvs_free(server->slave, sizeof(struct kvs_slave_s));
		server->slave = NULL;
	}

	kvs_free(server, sizeof(struct kvs_server_s));

	kvs_global_mempool_destroy(); // destroy global mempool
}


typedef struct {
	struct kvs_server_s* server;
	kvs_server_cmd_executor data_parser;
} kvs_server_aof_adapter_t;

static int _kvs_aof_data_parser(char* msg, int length, int *parsed_len, void* arg) {
	kvs_server_aof_adapter_t* adapter = (kvs_server_aof_adapter_t*)arg; 
	if(adapter == NULL) {
		return -1;
	}
	int parsed_length = 0;
	int ret = adapter->data_parser(adapter->server, msg, length, &parsed_length);
	if(ret < 0) {
		return -1; // parser failed
	}
	*parsed_len = parsed_length;
	return 0; // success
}

kvs_status_t kvs_server_storage_recovery(struct kvs_server_s *server, kvs_server_cmd_executor cmd_executor) {
	if(server == NULL || server->pers_ctx == NULL) {
		return KVS_ERR;
	}
	kvs_server_aof_adapter_t adapter;
	adapter.server = server;
	adapter.data_parser = cmd_executor;
	kvs_persistence_load_aof(server->pers_ctx, _kvs_aof_data_parser, &adapter);
	printf("AOF recovery completed.\n");
	return KVS_OK;
}


// also used in kvs_slave.c
static int _kvs_server_restore_entry(struct kvs_server_s* server, int data_type, char* key, int len_key, char* value, int len_val) {
	if(server == NULL || key == NULL || len_key <=0 || value == NULL || len_val <=0) {
		return -1;
	}
	switch(data_type) {
		case KVS_RDB_ARRAY:
			return kvs_array_resp_set(server->array, key, len_key, value, len_val);
		case KVS_RDB_RBTREE:
			return kvs_rbtree_resp_set(server->rbtree, key, len_key, value, len_val);
		case KVS_RDB_HASH:
			return kvs_hash_resp_set(server->hash, key, len_key, value, len_val);
		default:
			return -1;
	}
	return 0;
}

static int _kvs_server_rdb_item_loader(char* data, int len, void* arg) {
	if(arg == NULL || data == NULL || len <= 0) {
		return -1;
	}
	struct kvs_server_s* server = (struct kvs_server_s*)arg;
	
	if(len < sizeof(char) + sizeof(int)) {
		return -1;
	}
	int data_idx = 0;
	int processed_bytes = 0;
	while(data_idx < len) {
		processed_bytes = data_idx;
		char data_type = data[data_idx];
		data_idx += sizeof(char);
		if(data_idx + sizeof(int) > len) {
			break;
		}
		int len_key = 0;
		memcpy(&len_key, data + data_idx, sizeof(int));
		data_idx += sizeof(int);
		if(len_key <=0) {
			printf("%s:%d invalid len_key %d\n", __FILE__, __LINE__, len_key);
			return -1;
		}
		if(data_idx + len_key + sizeof(int) > len) {
			break;
		}
		char *key = data + data_idx;
		data_idx += len_key;
		int len_val = 0;
		memcpy(&len_val, data + data_idx, sizeof(int));
		data_idx += sizeof(int);
		if(len_val <=0) {
			printf("%s:%d invalid len_val %d\n", __FILE__, __LINE__, len_val);
			return -1;
		}
		if(data_idx + len_val > len) {
			break;
		}
		char *value = data + data_idx;
		data_idx += len_val;


		if(_kvs_server_restore_entry(server, data_type, key, len_key, value, len_val) < 0) {
			return -1;
		}
	}
	
	
	return processed_bytes;
}


int kvs_server_load_rdb(struct kvs_server_s *server) {
	if(server == NULL || server->pers_ctx == NULL) {
		return -1;
	}
	kvs_persistence_load_rdb(server->pers_ctx, _kvs_server_rdb_item_loader, server);
	return 0;
}



typedef struct {
	struct hashtable_s* hash;
	struct _rbtree* rbtree;
	struct kvs_array_s* array;
} server_dbs_t;

typedef struct {
	kvs_rdb_item_writer_pt writer; 
	void* writer_ctx;
	int db_type;
} server_storage_item_filter_ctx_t;

static int _kvs_server_storage_item_filter(char *key, int len_key, char *value, int len_val, void* arg) {
	server_storage_item_filter_ctx_t* ctx = (server_storage_item_filter_ctx_t*)arg;
	kvs_rdb_item_writer_pt writer = ctx->writer;
	void* writer_ctx = ctx->writer_ctx;
	int db_type = ctx->db_type;
	char dt_char = (char)db_type;
	if(writer == NULL) {
		return -1;
	}

	writer(&dt_char, sizeof(char), writer_ctx);
	writer((char*)&len_key, sizeof(int), writer_ctx);
	writer(key, len_key, writer_ctx);
	writer((char*)&len_val, sizeof(int), writer_ctx);
	writer(value, len_val, writer_ctx);
	return 0;
}


static void _kvs_server_storage_item_iterator(void* iter_arg, kvs_rdb_item_writer_pt writer, void* writer_ctx) {

	server_dbs_t *dbs = (server_dbs_t *)iter_arg;
	for(int i = KVS_RDB_START; i < KVS_RDB_END; ++ i) {
		server_storage_item_filter_ctx_t ctx;
		ctx.writer = writer;
		ctx.writer_ctx = writer_ctx;
		ctx.db_type = i;
		switch(i) {
			case KVS_RDB_HASH:
				kvs_hash_filter(dbs->hash, _kvs_server_storage_item_filter, &ctx);
				break;
			case KVS_RDB_ARRAY:
				kvs_array_filter(dbs->array, _kvs_server_storage_item_filter, &ctx);
				break;
			case KVS_RDB_RBTREE:
				kvs_rbtree_filter(dbs->rbtree, _kvs_server_storage_item_filter, &ctx);
				break;
			default:
				assert(0);
		}
	}
}



int kvs_server_save_rdb(struct kvs_server_s *server) {
	if(server->hash == NULL || server->rbtree == NULL || server->array == NULL) {
		return -1;
	}
	int ret = 0;
	// save hash
	server_dbs_t dbs;
	dbs.hash = server->hash;
	dbs.rbtree = server->rbtree;
	dbs.array = server->array;
	ret = kvs_persistence_save_rdb(server->pers_ctx,  _kvs_server_storage_item_iterator, &dbs);
	if(ret != 0) {
		printf("%s:%d save hash rdb failed\n", __FILE__, __LINE__);
		assert(0);
		return -1;
	}
	
	return 0;
}


kvs_status_t kvs_server_init_connection(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return -1;
	}
	
	LOG_DEBUG("%s:%d new connection accepted, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);
	conn->bussiness_ctx = kvs_malloc(sizeof(struct kvs_ctx_header_s));
	if(conn->bussiness_ctx == NULL) {
		LOG_FATAL("malloc ctx_header failed");
		assert(0);
	}
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	ctx_header->type = KVS_CTX_NORMAL_CLIENT; // if slave connection, will be changed later when SYNC command is received
	return KVS_OK;
}

kvs_status_t kvs_server_deinit_connection(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	LOG_DEBUG("%s:%d connection closed, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}
	if(ctx_header->type == KVS_CTX_NORMAL_CLIENT) {
		// normal client connection closed
		kvs_free(conn->bussiness_ctx, sizeof(struct kvs_ctx_header_s));
	} else if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
		// master connection closed
		struct kvs_slave_master_context_s* master_ctx = (struct kvs_slave_master_context_s*)conn->bussiness_ctx;

		kvs_free(master_ctx, sizeof(struct kvs_slave_master_context_s));
		assert(0);
	}else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		// slave connection closed
		struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)conn->bussiness_ctx;

		assert(slave_ctx != NULL);
		assert(slave_ctx->slave_idx >= 0);

		kvs_free(slave_ctx, sizeof(struct kvs_master_slave_context_s));
	} else {
		LOG_FATAL("unknown ctx type: %d\n", ctx_header->type);
		assert(0);
		return KVS_ERR;
	}

	return KVS_OK;
}

kvs_status_t kvs_server_change_connection_to_slave(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	
	LOG_DEBUG("%s:%d change connection to slave, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header == NULL) {
		LOG_FATAL("%s:%d ctx_header is NULL\n", __FILE__, __LINE__);
		assert(0);
		return -1;
	}

	if(ctx_header->type == KVS_CTX_NORMAL_CLIENT) {
		// normal client connection changed to slave connection
		kvs_free(ctx_header, sizeof(struct kvs_ctx_header_s));
		struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)kvs_malloc(sizeof(struct kvs_master_slave_context_s));
		memset(slave_ctx, 0, sizeof(struct kvs_master_slave_context_s));
		slave_ctx->header.type = KVS_CTX_SLAVE_OF_ME;
		conn->bussiness_ctx = (void*)slave_ctx;
		slave_ctx->slave_idx = -1; // will be assigned later
	} else {
		LOG_FATAL("%s:%d unknown ctx type: %d\n", __FILE__, __LINE__, ctx_header->type);
		assert(0);
		return -1;
	}

	return KVS_OK;
}




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