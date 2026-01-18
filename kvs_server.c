#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_network.h"
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
#include <sys/signalfd.h>
#include <signal.h>



// struct kvs_server_s *global_server = NULL;

int kvs_server_register_master(struct kvs_server_s *server, int master_fd) {
	struct kvs_conn_s *conn = NULL;
	int ret = kvs_net_resigster_fd(&server->network, master_fd, &conn);
	kvs_server_create_conn_type(server, conn, KVS_CTX_MASTER_OF_ME);
	if(ret < 0 || conn == NULL) {
		return -1;
	}
	kvs_net_set_recv_event(conn);
	return 0;
}

int kvs_server_init(struct kvs_server_s *server, struct kvs_server_config_s *config_pt) {
	if(server ==NULL || config_pt == NULL) {
		return -1;
	}
	kvs_global_mempool_init(); // init global mempool first

	//1. init signals and aof timer
	kvs_server_init_signals(server);
	

	//2. init persistence context
    server->pers_ctx = kvs_persistence_create(&config_pt->pers_config);
	kvs_server_init_aof_timer(server);

	// 3. init data structures
	server->array = kvs_array_create(KVS_ARRAY_SIZE);
	if(server->array == NULL) {
		printf("kvs_array_create failed\n");
		assert(0);
	}
	server->hash = kvs_hash_create(KVS_MAX_HASH_SIZE);
	server->rbtree = kvs_rbtree_create();

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
    return 0;
}


int kvs_server_deinit(struct kvs_server_s *server) {
	if(server == NULL) {
		return -1;
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

	kvs_global_mempool_destroy(); // destroy global mempool
	return 0;
}




kvs_status_t kvs_server_init_connection(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return -1;
	}
	
	LOG_DEBUG("%s:%d new connection accepted, fd: %d", __FILE__, __LINE__, conn->_internal.fd);
	kvs_server_create_conn_type(server, conn, KVS_CTX_NORMAL_CLIENT);
	struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->bussiness_ctx;
	cli_ctx->state = KVS_CLIENT_STATE_NORMAL; //不应该在这里设置，应该在状态机里设置。
	return KVS_OK;
}

kvs_status_t kvs_server_deinit_connection(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	//LOG_DEBUG("%s:%d connection closed, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);

	if(conn->bussiness_ctx == NULL) {
		LOG_FATAL("conn bussiness_ctx is NULL");
		assert(0);
		return KVS_ERR;
	}

	//todo: close 
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		//LOG_DEBUG("remove online slave connection, fd: %d\n", conn->_internal.fd);
		// slave connection
		if(server->role != KVS_SERVER_ROLE_MASTER) {
			LOG_FATAL("server is not master, can not have slave connections");
			assert(0);
			return KVS_ERR;
		}
		struct kvs_repl_slave_context_s* slave_ctx = (struct kvs_repl_slave_context_s*)conn->bussiness_ctx;
		if(slave_ctx->state == KVS_REPL_SLAVE_ONLINE) {
			// remove from master slave list
			LOG_DEBUG("remove online slave connection, fd: %d", conn->_internal.fd);
			kvs_master_remove_slave(server->master,  conn);
		} else {
			assert(0);
		}
	} else if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
		LOG_DEBUG("master connection disconnected, fd: %d\n", conn->_internal.fd);
		// master connection
		if(server->role != KVS_SERVER_ROLE_SLAVE) {
			LOG_FATAL("server is not slave, can not have master connection");
			assert(0);
			return KVS_ERR;
		}
	} else if(ctx_header->type == KVS_CTX_NORMAL_CLIENT) {
		LOG_DEBUG("normal client disconnected, fd: %d\n", conn->_internal.fd);
		// normal client
	} else {
		LOG_FATAL("unknown ctx type: %d", ctx_header->type);
		assert(0);
		return KVS_ERR;
	}

	kvs_server_destroy_conn_type(server, conn);

	return KVS_OK;
}

kvs_status_t kvs_server_destroy_conn_type(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}

	switch(ctx_header->type) {
		case KVS_CTX_NORMAL_CLIENT:
			if(ctx_header->type != KVS_CTX_NORMAL_CLIENT) {
				LOG_FATAL("can not destroy normal client from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			kvs_free(ctx_header, sizeof(struct kvs_client_context_s));
			conn->bussiness_ctx = NULL;
			break;
		case KVS_CTX_SLAVE_OF_ME:
			if(ctx_header->type != KVS_CTX_SLAVE_OF_ME) {
				LOG_FATAL("can not destroy slave from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			kvs_free(ctx_header, sizeof(struct kvs_repl_slave_context_s));
			conn->bussiness_ctx = NULL;
			break;
		case KVS_CTX_MASTER_OF_ME:
			if(ctx_header->type != KVS_CTX_MASTER_OF_ME) {
				LOG_FATAL("can not destroy master from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			assert(0); // todo: slave side master connection destroy
		default:
			LOG_FATAL("invalid ctx type: %d", ctx_header->type);
			assert(0);
			return KVS_ERR;
	}

	return KVS_OK;
}

kvs_status_t kvs_server_create_conn_type(struct kvs_server_s *server, struct kvs_conn_s *conn, kvs_ctx_type_t ctx_type) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	
	//LOG_DEBUG("register connection type: %d, fd: %d", ctx_type, conn->_internal.fd);
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header != NULL) {
		LOG_FATAL("ctx_header is not NULL, ctx_head_type: %d", ctx_header->type);
		assert(0);
		return -1;
	}

	switch(ctx_type) {
		case KVS_CTX_NORMAL_CLIENT:
			ctx_header = (struct kvs_ctx_header_s*)kvs_malloc(sizeof(struct kvs_client_context_s));
			if(ctx_header == NULL) {
				LOG_FATAL("malloc ctx_header failed");
				assert(0);
			}
			ctx_header->type = KVS_CTX_NORMAL_CLIENT;
			conn->bussiness_ctx = (void*)ctx_header;
			((struct kvs_client_context_s*)conn->bussiness_ctx)->state = KVS_CLIENT_STATE_NORMAL;
			break;
		case KVS_CTX_SLAVE_OF_ME:
			ctx_header = (struct kvs_ctx_header_s*)kvs_malloc(sizeof(struct kvs_repl_slave_context_s));
			if(ctx_header == NULL) {
				LOG_FATAL("malloc ctx_header failed");
				assert(0);
			}
			ctx_header->type = KVS_CTX_SLAVE_OF_ME;
			conn->bussiness_ctx = (void*)ctx_header;
			conn->bussiness_ctx = (void*)ctx_header;
			((struct kvs_repl_slave_context_s*)conn->bussiness_ctx)->state = KVS_REPL_SLAVE_NONE;
			break;
		case KVS_CTX_MASTER_OF_ME:
			//assert(0); // not used in server side
			ctx_header = (struct kvs_ctx_header_s*)kvs_malloc(sizeof(struct kvs_repl_master_context_s));
			if(ctx_header == NULL) {
				LOG_FATAL("malloc ctx_header failed");
				assert(0);
			}
			ctx_header->type = KVS_CTX_MASTER_OF_ME;
			conn->bussiness_ctx = (void*)ctx_header;
			((struct kvs_repl_master_context_s*)conn->bussiness_ctx)->state = KVS_REPL_MASTER_NONE;
			break;
		default:
			LOG_FATAL("invalid ctx type: %d", ctx_type);
			assert(0);
			return KVS_ERR;
	}

	return KVS_OK;
}

kvs_status_t kvs_server_convert_conn_type(struct kvs_server_s *server, struct kvs_conn_s *conn, kvs_ctx_type_t new_type) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}

	switch(new_type) {
		case KVS_CTX_NORMAL_CLIENT:
			if(ctx_header->type != KVS_CTX_NORMAL_CLIENT) {
				LOG_FATAL("can not convert to normal client from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			// already normal client, do nothing
			break;
		case KVS_CTX_SLAVE_OF_ME:
			if(ctx_header->type != KVS_CTX_NORMAL_CLIENT) {
				LOG_FATAL("can not convert to slave from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			// convert to slave
			kvs_free(ctx_header, sizeof(struct kvs_client_context_s));
			struct kvs_repl_slave_context_s* slave_ctx = (struct kvs_repl_slave_context_s*)kvs_malloc(sizeof(struct kvs_repl_slave_context_s));
			memset(slave_ctx, 0, sizeof(struct kvs_repl_slave_context_s));
			slave_ctx->header.type = KVS_CTX_SLAVE_OF_ME;
			conn->bussiness_ctx = (void*)slave_ctx;
			((struct kvs_repl_slave_context_s*)conn->bussiness_ctx)->state = KVS_REPL_SLAVE_NONE;
			slave_ctx->slave_idx = -1; // will be assigned later
			break;
		default:
			LOG_FATAL("invalid new ctx type: %d", new_type);
			assert(0);
			return KVS_ERR;
	}

	return KVS_OK;
}


/************************kvs server storage recovery using AOF and RDB*****************/


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
	if(ret == KVS_ERR) {
		return -1; // parser failed
	}
	*parsed_len = parsed_length;
	if(ret == KVS_AGAIN) {
		return -1; // need more data
	}
	return 0; // success
}

kvs_status_t kvs_server_aof_recovery(struct kvs_server_s *server, kvs_server_cmd_executor cmd_executor) {
	if(server == NULL || server->pers_ctx == NULL) {
		return KVS_ERR;
	}
	kvs_server_aof_adapter_t adapter;
	adapter.server = server;
	adapter.data_parser = cmd_executor;
	kvs_persistence_load_aof(server->pers_ctx, _kvs_aof_data_parser, &adapter);
	return KVS_OK;
}

kvs_status_t kvs_server_storage_recovery(struct kvs_server_s *server, kvs_server_cmd_executor cmd_executor) {
	if(server == NULL || server->pers_ctx == NULL) {
		return KVS_ERR;
	}
	kvs_status_t ret = KVS_OK;
	// 2. load AOF
	ret = kvs_server_aof_recovery(server, cmd_executor);
	if(ret != KVS_OK) {
		printf("AOF recovery failed.\n");
		return KVS_ERR;
	} else {
		printf("AOF recovery completed.\n");
		return KVS_OK;
	}

	// 1. load RDB
	ret = kvs_server_load_rdb(server);
	if(ret != KVS_OK) {
		printf("RDB recovery failed.\n");
		return KVS_ERR;
	} else {
		printf("RDB recovery completed.\n");
		return KVS_OK;
	}
	
	LOG_DEBUG("recovery failed");
	return KVS_ERR;
}