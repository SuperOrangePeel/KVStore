#include "kvs_server.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_network.h"
#include "kvs_rbtree.h"
#include "kvs_persistence.h"
// #include "kvs_handler.h" // decoupling
#include "common.h"
#include "kvs_rdma_engine.h"
#include "kvs_types.h"
#include "logger.h"
#include "kvs_conn.h"

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
	int ret = kvs_net_register_fd(&server->network, master_fd, &conn);
	kvs_server_create_conn_type(server, (struct kvs_conn_header_s *)conn, KVS_CTX_MASTER_OF_ME);
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
	// 0. init options
	server->use_rdma = config_pt->use_rdma;
	server->rdma_max_chunk_size = config_pt->rdma_max_chunk_size;
	

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
		kvs_rdma_post_listen(&server->rdma_engine); // start RDMA listen for master
	} else if(config_pt->role & KVS_SERVER_ROLE_SLAVE) {
		server->role = KVS_SERVER_ROLE_SLAVE;
		struct kvs_slave_s *slave = server->slave;
		slave = (struct kvs_slave_s*)kvs_malloc(sizeof(struct kvs_slave_s));
		slave->master_ip = config_pt->slave_config.master_ip; // set master_ip
		slave->master_port = config_pt->slave_config.master_port;
		slave->rdb_recv_buffer_count = config_pt->slave_config.rdb_recv_buffer_count;
		if(kvs_slave_init(slave, server, &config_pt->slave_config) != KVS_OK) {
			printf("kvs_slave_init failed\n");
			assert(0);
		}
		server->slave = slave;
		// if(kvs_slave_connect_master(server->slave) != KVS_OK) {
		// 	printf("kvs_slave_connect_master failed\n");
		// 	assert(0);
		// }
		// kvs_server_register_master(server, server->slave->master_fd);
		slave_start_replication(slave);

	} else {
		printf("invalid server role\n");
		assert(0);
	}
	
	// 5. init other components if needed
	if(config_pt->protocol.protocol_parser == NULL || config_pt->protocol.execute_command == NULL) {
		printf("protocol handlers are NULL\n");
		assert(0);
	}
	server->protocol = config_pt->protocol;
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
	if(server->role & KVS_SERVER_ROLE_MASTER) {
		kvs_master_deinit(server->master);
		kvs_free(server->master, sizeof(struct kvs_master_s));
		server->master = NULL;
	} 
	if(server->role & KVS_SERVER_ROLE_SLAVE) {
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
	// conn->ops.name = "kvs_client_ops";
	
	// conn->ops.on_recv = kvs_client_on_recv;
	// conn->ops.on_send = kvs_client_on_send;
	// conn->ops.on_close = kvs_client_on_close;

	
	LOG_DEBUG("%s:%d new connection accepted, fd: %d", __FILE__, __LINE__, conn->_internal.fd);
	kvs_server_create_conn_type(server, conn, KVS_CTX_NORMAL_CLIENT);
	struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->header.user_data;
	cli_ctx->state = KVS_CLIENT_STATE_NORMAL; //不应该在这里设置，应该在状态机里设置。
	// cli_ctx->header.ops.on_recv = kvs_client_on_recv;
	// cli_ctx->header.ops.on_send = kvs_client_on_send;
	// cli_ctx->header.ops.on_close = kvs_client_on_close;
	// cli_ctx->header.ops.name = "kvs_client_ops";

	return KVS_OK;
}

kvs_status_t kvs_server_deinit_connection(struct kvs_server_s *server, struct kvs_conn_s *conn) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	//LOG_DEBUG("%s:%d connection closed, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);

	if(conn->header.user_data == NULL) {
		LOG_FATAL("conn user_data is NULL");
		assert(0);
		return KVS_ERR;
	}

	//todo: close 
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
	if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		//LOG_DEBUG("remove online slave connection, fd: %d\n", conn->_internal.fd);
		// slave connection
		if(server->role != KVS_SERVER_ROLE_MASTER) {
			LOG_FATAL("server is not master, can not have slave connections");
			assert(0);
			return KVS_ERR;
		}
		struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
		if(slave_ctx->state == KVS_MY_SLAVE_ONLINE) {
			// remove from master slave list
			LOG_DEBUG("remove online slave connection, fd: %d", conn->_internal.fd);
			//kvs_master_remove_slave(server->master,  conn);
		} else {
			LOG_DEBUG("slave connection already offline, fd: %d", conn->_internal.fd);
			//assert(0);
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



kvs_status_t kvs_server_destroy_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *conn) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->user_data;
	if(ctx_header == NULL) {
		LOG_WARN("ctx_header is NULL");
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
			conn->user_data = NULL;
			break;
		case KVS_CTX_SLAVE_OF_ME:
			if(ctx_header->type != KVS_CTX_SLAVE_OF_ME) {
				LOG_FATAL("can not destroy slave from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)ctx_header;
			slave_ctx->ref_count --;
			if(slave_ctx->ref_count > 0) {
				LOG_DEBUG("slave_ctx ref_count > 0, not destroying, ref_count: %d", slave_ctx->ref_count);
				return KVS_OK;
			} else if(slave_ctx->ref_count < 0) {
				LOG_FATAL("slave_ctx ref_count < 0, invalid state");
				assert(0);
				return KVS_ERR;
			}
			kvs_free(ctx_header, sizeof(struct kvs_my_slave_context_s));
			conn->user_data = NULL;
			break;
		case KVS_CTX_MASTER_OF_ME:
			if(ctx_header->type != KVS_CTX_MASTER_OF_ME) {
				LOG_FATAL("can not destroy master from type: %d", ctx_header->type);
				assert(0);
				return KVS_ERR;
			}
			//assert(0); // todo: slave side master connection destroy
			kvs_free(ctx_header, sizeof(struct kvs_my_master_context_s));
			conn->user_data = NULL;
			break;
		default:
			LOG_FATAL("invalid ctx type: %d", ctx_header->type);
			assert(0);
			return KVS_ERR;
	}

	return KVS_OK;
}

kvs_status_t kvs_server_share_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *src_conn, struct kvs_conn_header_s *dst_conn) {
	if(server == NULL || src_conn == NULL || dst_conn == NULL) {
		return KVS_ERR;
	}
	
	struct kvs_ctx_header_s* src_ctx_header = (struct kvs_ctx_header_s*)src_conn->user_data;
	if(src_ctx_header == NULL) {
		LOG_FATAL("src_ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}

	if(src_ctx_header->type != KVS_CTX_SLAVE_OF_ME) {
		LOG_FATAL("can not share conn type from type: %d", src_ctx_header->type);
		assert(0);
		return KVS_ERR;
	}

	dst_conn->user_data = src_conn->user_data;
	struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)src_ctx_header;
	slave_ctx->ref_count ++;
	LOG_DEBUG("shared slave_ctx, new ref_count: %d", slave_ctx->ref_count);

	return KVS_OK;
}

kvs_status_t kvs_server_create_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *conn, kvs_ctx_type_t ctx_type) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	
	//LOG_DEBUG("register connection type: %d, fd: %d", ctx_type, conn->_internal.fd);
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->user_data;
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
			conn->user_data = (void*)ctx_header;
			//((struct kvs_client_context_s*)conn->user_data)->state = KVS_CLIENT_STATE_NORMAL;
			struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->user_data;
			cli_ctx->state = KVS_CLIENT_STATE_NORMAL;
			cli_ctx->header.ops.on_recv = kvs_client_on_recv;
			cli_ctx->header.ops.on_send = kvs_client_on_send;
			cli_ctx->header.ops.on_close = kvs_client_on_close;
			cli_ctx->header.ops.name = "kvs_client_ops";

			break;
		case KVS_CTX_SLAVE_OF_ME:
			ctx_header = (struct kvs_ctx_header_s*)kvs_malloc(sizeof(struct kvs_my_slave_context_s));
			if(ctx_header == NULL) {
				LOG_FATAL("malloc ctx_header failed");
				assert(0);
			}
			ctx_header->type = KVS_CTX_SLAVE_OF_ME;
			conn->user_data = (void*)ctx_header;
			//conn->user_data = (void*)ctx_header;
			// ((struct kvs_my_slave_context_s*)conn->user_data)->state = KVS_MY_SLAVE_NONE;
			struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
			slave_ctx->state = KVS_MY_SLAVE_NONE;
			slave_ctx->header.ops.on_recv = kvs_my_slave_on_recv;
			slave_ctx->header.ops.on_send = kvs_my_slave_on_send;
			slave_ctx->header.ops.on_close = kvs_my_slave_on_close;
			slave_ctx->header.ops.name = "kvs_my_slave_ops";
			slave_ctx->ref_count = 1;
			slave_ctx->slave_idx = -1; // will be assigned later
			slave_ctx->master = server->master; // back reference to master
			LOG_WARN("created slave_ctx, ref_count: %d", slave_ctx->ref_count);

			break;
		case KVS_CTX_MASTER_OF_ME:{
			//assert(0); // not used in server side
				struct kvs_my_master_context_s* master_ctx = (struct kvs_my_master_context_s*)kvs_malloc(sizeof(struct kvs_my_master_context_s));
				if(master_ctx == NULL) {
					LOG_FATAL("malloc ctx_header failed");
					assert(0);
				}
				master_ctx->header.type = KVS_CTX_MASTER_OF_ME;
				//conn->user_data = (void*)master_ctx;
				master_ctx->state = KVS_MY_MASTER_NONE;
				master_ctx->header.ops.on_recv = kvs_my_master_on_recv;
				master_ctx->header.ops.on_send = kvs_my_master_on_send;
				master_ctx->header.ops.on_close = kvs_my_master_on_close;
				master_ctx->header.ops.name = "kvs_my_master_ops";
				master_ctx->slave = server->slave; // back reference to slave
				conn->user_data = (void*)master_ctx;
			}
			break;
		default:
			LOG_FATAL("invalid ctx type: %d", ctx_type);
			assert(0);
			return KVS_ERR;
	}

	return KVS_OK;
}

kvs_status_t kvs_server_convert_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *conn, kvs_ctx_type_t new_type) {
	if(server == NULL || conn == NULL) {
		return KVS_ERR;
	}
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->user_data;
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
			conn->user_data = NULL;
			//struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)kvs_malloc(sizeof(struct kvs_my_slave_context_s));
			kvs_server_create_conn_type(server, conn, KVS_CTX_SLAVE_OF_ME);
			// memset(slave_ctx, 0, sizeof(struct kvs_my_slave_context_s));
			// slave_ctx->header.type = KVS_CTX_SLAVE_OF_ME;
			// conn->user_data = (void*)slave_ctx;
			// slave_ctx->state = KVS_MY_SLAVE_NONE;
			// // conn->ops.name = "kvs_my_slave_ops";
			// // conn->ops.on_recv = kvs_my_slave_on_recv;
			// // conn->ops.on_send = kvs_my_slave_on_send;
			// // conn->ops.on_close = kvs_my_slave_on_close;
			// slave_ctx->header.ops.on_recv = kvs_my_slave_on_recv;
			// slave_ctx->header.ops.on_send = kvs_my_slave_on_send;
			// slave_ctx->header.ops.on_close = kvs_my_slave_on_close;
			// slave_ctx->header.ops.name = "kvs_my_slave_ops";
			// slave_ctx->slave_idx = -1; // will be assigned later
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
	
	struct stat file_stat;
	if(stat(server->pers_ctx->aof_filename, &file_stat) != 0){
		// aof file not exist, skip
		printf("AOF file not exist, skip AOF recovery.\n");
		return KVS_ERR;
	}

	kvs_persistence_load_aof(server->pers_ctx, _kvs_aof_data_parser, &adapter);
	return KVS_OK;
}

static kvs_status_t _kvs_server_process_raw_buffer(struct kvs_server_s* server, char* buf, int len, int *parsed_length) {
	if(buf == NULL || len <=0 || parsed_length == NULL) {
		return -1;
	}
	struct kvs_protocol_s* proto = &server->protocol;
	int parsed_length_once = 0;
	struct kvs_handler_cmd_s cmd;
	kvs_result_t result = 0;
	kvs_status_t status = proto->protocol_parser(buf, 
		len, &cmd, &parsed_length_once);
	if(status == KVS_ERR) {
		LOG_ERROR("protocol parse error");
		return KVS_ERR;
	} else if(status == KVS_AGAIN) {
		//LOG_DEBUG("need more data");
		// need more data
		return KVS_AGAIN;
	} else if(status == KVS_OK) {
		//LOG_DEBUG("command_idx: %d", cmd.cmd_idx);
		// process command
		//char *value_out = NULL;
		//int len_val_out = 0;
		result = proto->execute_command(server, &cmd, NULL);
		if(result == KVS_RES_ERR){
			LOG_ERROR("execute command failed");
			assert(0);
		}
	}
	*parsed_length = parsed_length_once;
	return KVS_OK;
}

kvs_status_t kvs_server_storage_recovery(struct kvs_server_s *server) {
	if(server == NULL || server->pers_ctx == NULL) {
		return KVS_ERR;
	}
	kvs_status_t ret = KVS_OK;
	// 1. load AOF
	ret = kvs_server_aof_recovery(server, _kvs_server_process_raw_buffer);
	if(ret != KVS_OK) {
		printf("AOF recovery failed.\n");
		//return KVS_ERR;
	} else {
		printf("AOF recovery completed.\n");
		return KVS_OK;
	}

	// 2. load RDB
	ret = kvs_server_load_rdb(server);
	if(ret != KVS_OK) {
		printf("RDB recovery failed.\n");
		//return KVS_ERR;
	} else {
		printf("RDB recovery completed.\n");
		return KVS_OK;
	}
	
	LOG_DEBUG("recovery failed");
	return KVS_ERR;
}


kvs_status_t kvs_server_msg_pump(struct kvs_conn_s *conn, int *read_size, kvs_cmd_handler_pt handler) {
	int parsed_len = 0;
	int parsed_total_len = 0;
	struct kvs_server_s* server = (struct kvs_server_s*)conn->server_ctx;
	struct kvs_protocol_s *proto = &server->protocol;

	char *r_buffer = conn->r_buffer;
	int r_idx = conn->r_idx;

	struct kvs_handler_cmd_s cmd;
	while(parsed_total_len < conn->r_idx) {
		memset(&cmd, 0, sizeof(struct kvs_handler_cmd_s));
		kvs_status_t status = proto->protocol_parser(r_buffer + parsed_total_len, 
			r_idx - parsed_total_len, 
			&cmd, 
			&parsed_len);
		
		if(status == KVS_ERR) {
			LOG_ERROR("protocol parse error");
			assert(0);
			return KVS_ERR;
		} else if(status == KVS_AGAIN) {
			// need more data
			break;
		}

		parsed_total_len += parsed_len;
		// process command
		kvs_status_t result = handler(server, &cmd, conn);
		if(result == KVS_BREAK) {
			parsed_total_len = conn->r_idx; // 清空缓冲区
			break;
		} else if(result == KVS_ERR) {
			LOG_ERROR("execute command failed");
			assert(0);
			return KVS_ERR;
		} else if(result == KVS_QUIT) {
			// disconnect
			LOG_INFO("client requested to quit");
			assert(0);
			return KVS_QUIT;
		}
		//conn->ops.on_recv(conn, ); // update last active time
	}
	*read_size = parsed_total_len;

	if(conn->s_idx > 0) {
		kvs_net_set_send_event_manual(conn);
	}
	return KVS_OK;
}