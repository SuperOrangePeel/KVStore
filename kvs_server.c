#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_persistence.h"
#include "kvs_handler.h"
#include "common.h"

#include <stdlib.h>
#include <string.h>

kvs_server_t *global_server = NULL;

static int _kvs_aof_recovery(char* msg, int length, int *idx, void* arg) {
	kvs_server_t* server = (kvs_server_t*)arg; // dependency injection !!
	if(server == NULL) {
		return -1;
	}
	return kvs_handler_process_raw_buffer(server, msg, length, idx);
}

kvs_server_t *kvs_server_init(unsigned short port, int role) {
	kvs_global_mempool_init(); // init global mempool first

    kvs_server_t *server = (kvs_server_t *)kvs_malloc(sizeof(kvs_server_t));
	if(server == NULL) {
		return NULL;
	}
	memset(server, 0, sizeof(kvs_server_t));
    server->pers_ctx = kvs_persistence_create("kvstore.aof", "dump.rdb");
	server->array = kvs_array_create(KVS_ARRAY_SIZE);
	server->hash = kvs_hash_create(KVS_MAX_HASH_SIZE);
	server->rbtree = kvs_rbtree_create();
	
	printf("AOF recovery completed.\n");


    server->port = port;
    server->role = role;
    server->server_fd = -1;
	server->on_msg = kvs_protocol;

	server->conns = (kvs_conn_t *)kvs_malloc(sizeof(kvs_conn_t) * KVS_MAX_CONNECTS);
	memset(server->conns, 0, sizeof(kvs_conn_t) * KVS_MAX_CONNECTS);


	// init server before AOF recovery
	kvs_persistence_load_aof(server->pers_ctx, _kvs_aof_recovery, server);
    return server;
}

void kvs_server_destroy(kvs_server_t *server) {
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

	kvs_free(server, sizeof(kvs_server_t));
	kvs_global_mempool_destroy();
}