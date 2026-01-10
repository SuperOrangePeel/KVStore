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


// also used in kvs_slave.c
int kvs_server_restore_entry(char data_type, char* key, int len_key, char* value, int len_val, void* arg) {
	struct kvs_server_s* server = (struct kvs_server_s*)arg; // dependency injection !!
	if(server == NULL) {
		return -1;
	}
	switch(data_type) {
		case KVS_RDB_HASH:
			//printf("Restoring hash entry: key=%.*s\n, value=%.*s\n", len_key, key, len_val, value);
			return kvs_hash_resp_set(server->hash, key, len_key, value, len_val);
		// case KVS_RDB_ARRAY:
		// 	return kvs_array_resp_set(server->array, key, len_key, value, len_val);
		// case KVS_RDB_RBTREE:
		// 	return kvs_rbtree_resp_set(server->rbtree, key, len_key, value, len_val);
		default:
			return -1;
	}
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


void kvs_server_storage_recovery(struct kvs_server_s *server, kvs_server_aof_data_parser_cb aof_data_parser) {
	if(access(server->pers_ctx->aof_filename, F_OK) == 0) {
		kvs_server_aof_adapter_t adapter;
		adapter.server = server;
		adapter.data_parser = aof_data_parser;

		kvs_persistence_load_aof(server->pers_ctx, _kvs_aof_data_parser, &adapter);
		printf("AOF file found. Starting AOF recovery...\n");
	} else {
		if(access(server->pers_ctx->rdb_filename, F_OK) == 0) {
			kvs_persistence_load_rdb(server->pers_ctx, kvs_server_restore_entry, server);
			printf("RDB file found. Starting RDB recovery...\n");
		} else {
			printf("No AOF or RDB file found. Starting with an empty database.\n");
		}
	}
}
