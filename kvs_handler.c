#include "kvs_handler.h"
#include "kvs_network.h"

#include "kvs_persistence.h"
#include "kvs_server.h"

#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"


#include "kvs_resp_protocol.h"
#include "kvs_executor.h"
#include "common.h"
#include "kvs_types.h"
#include "kvs_server.h"
#include "logger.h"
#include "kvs_types.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>



kvs_status_t _kvs_handler_format_response(int result, char *value, int len_val, struct kvs_conn_s *conn) {
	if(conn == NULL) return KVS_ERR;
	switch(result) {
		case KVS_RES_OK:
			//kvs_proactor_set_send_event(conn, "+OK\r\n", 5);
			kvs_net_copy_msg_to_send_buf(conn, "+OK\r\n", 5);
			break;
		case KVS_RES_EXIST:
			kvs_net_copy_msg_to_send_buf(conn, "-EXIST\r\n", 9);
			break;
		case KVS_RES_NOT_FOUND:
			kvs_net_copy_msg_to_send_buf(conn, "-NOT FOUND\r\n", 13);
			break;
		case KVS_RES_VAL: {
			if(value == NULL || len_val <= 0) {
				kvs_net_copy_msg_to_send_buf(conn, "-ERROR\r\n", 8);
				break;
			}
			char header[64];
			int header_len = snprintf(header, sizeof(header), "$%d\r\n", len_val);
			kvs_net_copy_msg_to_send_buf(conn, header, header_len);
			kvs_net_copy_msg_to_send_buf(conn, value, len_val);
			kvs_net_copy_msg_to_send_buf(conn, "\r\n", 2);
			break;
		}
		case KVS_RES_UNKNOWN_CMD:
			kvs_net_copy_msg_to_send_buf(conn, "-ERR unknown command\r\n", 22);
			break;
		default:
			LOG_FATAL("unknown result code: %d", result);
			assert(0);
			
	}
	return KVS_OK;
}

int kvs_handler_on_msg(struct kvs_conn_s *conn, int *read_size) {
	if(conn == NULL) return KVS_ERR;
	struct kvs_handler_cmd_s cmd;
	//LOG_DEBUG("connfd:%d", conn->_internal.fd);
	//LOG_DEBUG("received data: %d", conn->r_idx);
	struct kvs_server_s* server = (struct kvs_server_s*)conn->server_ctx;
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(server == NULL || ctx_header == NULL) {
		if(server == NULL)
			LOG_FATAL("server is NULL");
		if(ctx_header == NULL)
			LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}

	struct kvs_master_slave_context_s* slave_ctx = NULL;
	struct kvs_slave_master_context_s* master_ctx = NULL;
	if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
	
		// slave connection
	} else if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
		master_ctx = (struct kvs_slave_master_context_s*)conn->bussiness_ctx;
	} else if(ctx_header->type == KVS_CTX_NORMAL_CLIENT) {
		// normal client
	} else {
		printf("%s:%d unknown ctx type: %d\n", __FILE__, __LINE__, ctx_header->type);
		assert(0);
		return KVS_ERR;
	}


	int parsed_length_once = 0;
	int parsed_total_length = 0;
	kvs_result_t result = 0;
	int msg_len = conn->r_idx;// can not change r_idx in this function
	char *msg = conn->r_buffer;

	while(parsed_total_length < conn->r_idx) {
		//LOG_DEBUG("parsed_total_length:%d, conn->r_idx:%d", parsed_total_length, conn->r_idx);
		//LOG_DEBUG("received data: %.*s", conn->r_idx, conn->r_buffer);
		memset(&cmd, 0, sizeof(struct kvs_handler_cmd_s));
		
		parsed_length_once = 0;
		kvs_status_t status = kvs_protocol(msg + parsed_total_length, 
			msg_len - parsed_total_length, &cmd, &parsed_length_once);
		if(status == KVS_ERR) {
			//LOG_DEBUG("msg: [%.*s], msg length: %d", msg_len, msg, msg_len);
			LOG_DEBUG("parsed_total:%d", parsed_total_length);
			LOG_ERROR("protocol parse error");
			assert(0);
			return KVS_ERR;
		} else if(status == KVS_AGAIN) {
			//LOG_DEBUG("need more data");
			// need more data
			break;
		} else if(status == KVS_OK) {
			//LOG_DEBUG("command_idx: %d", cmd.cmd_idx);
			// process command
			char *value_out = NULL;
			int len_val_out = 0;
			result = kvs_executor_cmd(server, &cmd, conn);
			if(result == KVS_RES_ERR){
				LOG_ERROR("execute command failed");
				assert(0);
			}
		}
		parsed_total_length += parsed_length_once;
		

		if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {

			// master connection
			continue;
		}

		if(server->pers_ctx->aof_enabled && cmd.cmd_type & KVS_CMD_WRITE) {
			// append to AOF file
			kvs_persistence_write_aof(server->pers_ctx, cmd.raw_ptr, cmd.raw_len);
		}
		
		
		// if(result == KVS_RES_SYNC_SLAVE) {
		// 	// special handling for SYNC command
		// 	if(master_ctx == NULL) {
		// 		LOG_ERROR("master_ctx is NULL for SYNC command");
		// 		assert(0);
		// 	}
		// 	int ret = kvs_slave_start_sync_process(conn, server, master_ctx);
		// 	if(ret < 0) {
		// 		LOG_ERROR("start sync process failed");
		// 		return KVS_ERR;
		// 	}
		// 	// after SYNC command, we do not need to send any response
		// 	parsed_total_length += parsed_length_once;
		// 	break; // exit the loop
		// }

		//LOG_DEBUG("parsed_total_length:%d, conn->r_idx:%d", parsed_total_length, conn->r_idx);
		_kvs_handler_format_response(result, cmd.val, cmd.len_val	, conn);
		
		//kvs_net_set_send_event_manual(conn);
	}
	//LOG_DEBUG("read_size:%d", parsed_total_length);
	*read_size = parsed_total_length;
	kvs_net_set_send_event_manual(conn);
	//LOG_DEBUG("total parsed length:%d", parsed_total_length);
	return 0;
}




int kvs_handler_on_send(struct kvs_conn_s *conn, int bytes_sent) {
	if(conn == NULL) return -1;
	assert(conn->w_idx == 0);
	
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		// slave connection
		//return _kvs_handler_init_slave_info(conn);
	} else if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
		// master connection
		// do nothing, just set recv event
		kvs_net_set_recv_event(conn);
	} else {
		// normal client
		kvs_net_set_recv_event(conn);
	}

	return 0;
	
}

int kvs_handler_on_accept(struct kvs_conn_s *conn) {
	if(conn == NULL) return KVS_ERR;
	struct kvs_server_s* server = (struct kvs_server_s*)conn->server_ctx;
	if(server == NULL) {
		LOG_FATAL("server is NULL");
		assert(0);
		return KVS_ERR;
	}
	kvs_server_init_connection(server, conn);

	//kvs_net_set_recv_event(conn);
	return 0;
}



int kvs_handler_on_close(struct kvs_conn_s *conn) {
	if(conn == NULL) return -1;
	struct kvs_server_s* server = (struct kvs_server_s*)conn->server_ctx;
	if(server == NULL) {
		LOG_FATAL("server is NULL");
		assert(0);
		return KVS_ERR;
	}

	kvs_server_deinit_connection(server, conn);
	return 0;
}

int kvs_handler_on_timer(void *global_ctx) {
	if(global_ctx == NULL) return -1;
	struct kvs_server_s *server = (struct kvs_server_s *)global_ctx;
	kvs_persistence_flush_aof(server->pers_ctx);

	return 0;
}


int kvs_handler_process_raw_buffer(struct kvs_server_s* server, char* buf, int len, int *parsed_length) {
	if(buf == NULL || len <=0 || parsed_length == NULL) {
		return -1;
	}
	int parsed_length_once = 0;
	struct kvs_handler_cmd_s cmd;
	kvs_result_t result = 0;
	kvs_status_t status = kvs_protocol(buf, 
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
		char *value_out = NULL;
		int len_val_out = 0;
		result = kvs_executor_cmd(server, &cmd, NULL);
		if(result == KVS_RES_ERR){
			LOG_ERROR("execute command failed");
			assert(0);
		}
	}
	*parsed_length = parsed_length_once;
	return KVS_OK;
}