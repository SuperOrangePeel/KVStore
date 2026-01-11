#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_persistence.h"
// #include "kvs_handler.h" // decoupling
#include "common.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h> 
#include <assert.h>


enum {
	KVS_RDB_START = 0,
	KVS_RDB_ARRAY = 0,
	KVS_RDB_RBTREE,
	KVS_RDB_HASH,
	KVS_RDB_END
};

struct kvs_server_s *global_server = NULL;

struct kvs_server_s *kvs_server_init(unsigned short port, kvs_on_accept_cb on_accept,
	kvs_on_msg_cb on_msg, kvs_on_send_cb on_send, kvs_on_close_cb on_close) {
	kvs_global_mempool_init(); // init global mempool first

    struct kvs_server_s *server = (struct kvs_server_s *)kvs_malloc(sizeof(struct kvs_server_s));
	if(server == NULL) {
		return NULL;
	}
	memset(server, 0, sizeof(struct kvs_server_s));
    server->pers_ctx = kvs_persistence_create("kvstore.aof", "dump.rdb");
	server->array = kvs_array_create(KVS_ARRAY_SIZE);
	if(server->array == NULL) {
		printf("kvs_array_create failed\n");
		assert(0);
	}
	server->hash = kvs_hash_create(KVS_MAX_HASH_SIZE);
	server->rbtree = kvs_rbtree_create();
	
	//printf("AOF recovery completed.\n");


    server->port = port;
    server->role = -1;
    server->server_fd = -1;
	server->on_msg = on_msg;
	server->on_send = on_send;
	server->on_accept = on_accept;
	server->on_close = on_close;

	server->master.slave_count = 0;
	server->master.max_slave_count = KVS_MAX_SLAVES;

	server->conns = (kvs_conn_t *)kvs_malloc(sizeof(kvs_conn_t) * KVS_MAX_CONNECTS);
	memset(server->conns, 0, sizeof(kvs_conn_t) * KVS_MAX_CONNECTS);
	server->conn_max = KVS_MAX_CONNECTS;
	
	server->uring = NULL;
	
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
	if(server->conns) {
		kvs_free(server->conns, sizeof(kvs_conn_t) * KVS_MAX_CONNECTS);
		server->conns = NULL;
	}
	if(server->master.repl_backlog) {
		kvs_free(server->master.repl_backlog, server->master.repl_backlog_size);
		server->master.repl_backlog = NULL;
	}


	kvs_free(server, sizeof(struct kvs_server_s));
	kvs_global_mempool_destroy();
}





typedef struct {
	struct kvs_server_s* server;
	kvs_server_aof_data_parser_cb data_parser;
} kvs_server_aof_adapter_t;

static int _kvs_aof_data_parser(char* msg, int length, void* arg) {
	kvs_server_aof_adapter_t* adapter = (kvs_server_aof_adapter_t*)arg; 
	if(adapter == NULL) {
		return -1;
	}
	int parsed_length = 0;
	int ret = adapter->data_parser(adapter->server, msg, length, &parsed_length);
	if(ret < 0) {
		return -1; // parser failed
	}
	return parsed_length;
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

void kvs_server_storage_recovery(struct kvs_server_s *server, kvs_server_aof_data_parser_cb aof_data_parser) {
	if(access(server->pers_ctx->aof_filename, F_OK) == 0) {
		kvs_server_aof_adapter_t adapter;
		adapter.server = server;
		adapter.data_parser = aof_data_parser;

		kvs_persistence_load_aof(server->pers_ctx, _kvs_aof_data_parser, &adapter);
		printf("AOF file found. Starting AOF recovery...\n");
	} else {
		if(access(server->pers_ctx->rdb_filename, F_OK) == 0) {
			kvs_server_load_rdb(server);
			printf("RDB file found. Starting RDB recovery...\n");
		} else {
			printf("No AOF or RDB file found. Starting with an empty database.\n");
		}
	}
}




typedef struct {
	struct hashtable_s* hash;
	struct _rbtree* rbtree;
	struct kvs_array_s* array;
} server_dbs;

typedef struct {
	kvs_rdb_item_writer_pt writer; 
	void* writer_ctx;
	int db_type;
} server_storage_item_filter_ctx_t;

static void _kvs_server_storage_item_filter(char *key, int len_key, char *value, int len_val, void* arg) {
	server_storage_item_filter_ctx_t* ctx = (server_storage_item_filter_ctx_t*)arg;
	kvs_rdb_item_writer_pt writer = ctx->writer;
	void* writer_ctx = ctx->writer_ctx;
	int db_type = ctx->db_type;
	char dt_char = (char)db_type;
	if(writer == NULL) {
		return;
	}

	writer(&dt_char, sizeof(char), writer_ctx);
	writer((char*)&len_key, sizeof(int), writer_ctx);
	writer(key, len_key, writer_ctx);
	writer((char*)&len_val, sizeof(int), writer_ctx);
	writer(value, len_val, writer_ctx);
}


static void _kvs_server_storage_item_iterator(void* iter_arg, kvs_rdb_item_writer_pt writer, void* writer_ctx) {

	server_dbs *dbs = (server_dbs *)iter_arg;
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
	server_dbs dbs;
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


