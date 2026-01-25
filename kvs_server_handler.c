#include "kvs_conn.h"
#include "kvs_server.h"
#include "kvs_network.h"

//#include "kvs_server.h"

#include "common.h"
#include "kvs_types.h"
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
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
	if(server == NULL) {
		LOG_FATAL("server is NULL");
		assert(0);
		return KVS_ERR;
	}
	ctx_header->ops.on_close(conn);

	kvs_server_deinit_connection(server, conn);
	return 0;
}

int kvs_handler_on_msg(struct kvs_conn_s *conn, int *read_size) {
	if(conn == NULL) {
		assert(0);
		return KVS_ERR;
	}
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}
	return ctx_header->ops.on_recv(conn, read_size);
}


int kvs_handler_on_send(struct kvs_conn_s *conn, int bytes_sent) {
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
	if(conn == NULL || ctx_header == NULL) {
		if(conn == NULL)
			LOG_FATAL("conn is NULL");
		if(ctx_header == NULL)
			LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}
	return ctx_header->ops.on_send(conn, bytes_sent);

    return 0;
}


// int kvs_handler_on_rdma_connect_before(struct kvs_rdma_conn_s *conn) {
//     // //struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
// 	// struct kvs_server_s* server = (struct kvs_server_s*)conn->global_ctx;
// 	// // kvs_server_create_conn_type(server, (struct kvs_conn_header_s*)conn, KVS_CTX_MASTER_OF_ME);
// 	// struct kvs_my_master_context_s* master_ctx = (struct kvs_my_master_context_s*)conn->header.user_data;
// 	// if(master_ctx == NULL) {
// 	// 	LOG_FATAL("master_ctx is NULL");
// 	// 	assert(0);
// 	// 	return KVS_ERR;
// 	// }

// 	// struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
// 	// if(ctx_header->type != KVS_CTX_MASTER_OF_ME) {
// 	// 	LOG_FATAL("invalid ctx type: %d", ctx_header->type);
// 	// 	assert(0);
// 	// 	return KVS_ERR;
// 	// }

// 	// kvs_slave_master_state_machine_tick(server->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_RDMA_ESTABLISHED);

//     return 0;
// }

int kvs_handler_on_rdma_connect_request(struct kvs_rdma_conn_s *conn) {
	LOG_DEBUG("RDMA connection request received, priv_len: %d", conn->private_data_len);
    //conn->header.user_data = NULL;
	struct kvs_server_s* server = (struct kvs_server_s*)conn->global_ctx;
	if(server == NULL) {
		LOG_FATAL("server is NULL");
		assert(0);
		return KVS_ERR;
	}
	if(conn->header.user_data != NULL) {
		LOG_FATAL("conn user_data is not NULL in rdma connect request");
		assert(0);
	}
	// todo: 
	// kvs_server_create_conn_type(server, (struct kvs_conn_header_s*)conn, KVS_CTX_SLAVE_OF_ME);
	// struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
	// slave_ctx->state = KVS_MY_SLAVE_WAIT_RDMA_READY;
	// if(slave_ctx == NULL) {
	// 	LOG_FATAL("slave_ctx is NULL");
	// 	assert(0);
	// 	return KVS_ERR;
	// }
	// if(priv_len != sizeof(uint64_t)) {
	// 	LOG_FATAL("invalid priv_len: %d, expected: %lu", priv_len, sizeof(uint64_t));
	// 	assert(0);
	// 	return -1; // -1 to reject connection
	// }

	//memcpy(&slave_ctx->rdma_token, priv_data, sizeof(uint64_t));
	//conn->header.user_data = (void *)priv_data; // store token in user_data temporarily
	//conn->private_data
	kvs_status_t status = kvs_master_slave_state_machine_tick(server->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_RDMA_ESTABLISHED);
	struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
	if(status == KVS_QUIT) {
		LOG_INFO("RDMA connection request rejected by state machine, token: %ld", slave_ctx->rdma_token);
		return -1; // reject connection
	}

	if(status != KVS_OK) {
		LOG_FATAL("Error in RDMA connection request state machine, token: %ld", slave_ctx->rdma_token);
		assert(0);
		return KVS_ERR;
	}

	return 0;
}

int kvs_handler_on_rdma_established(struct kvs_rdma_conn_s *conn) {
	struct kvs_server_s* server = (struct kvs_server_s*)conn->global_ctx;
	if(server == NULL) {
		LOG_FATAL("server is NULL");
		assert(0);
		return KVS_ERR;
	}
	struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->header.user_data;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL, conn type:%d", conn->header.type);
		return KVS_ERR;
	}
	kvs_status_t status = KVS_OK;
	if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {

		status = kvs_slave_master_state_machine_tick(server->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_RDMA_ESTABLISHED);
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		status = kvs_master_slave_state_machine_tick(server->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_RDMA_ESTABLISHED);
	} else {
		LOG_FATAL("invalid ctx type: %d", ctx_header->type);
		assert(0);
		return KVS_ERR;
	}
	if(status == KVS_QUIT) {
		LOG_INFO("RDMA connection closed by state machine, token: %ld", ((struct kvs_my_slave_context_s*)conn->header.user_data)->rdma_token);
		return -1; // close connection
	}
	
	return 0;
}

int kvs_handler_on_rdma_disconnected(struct kvs_rdma_conn_s *conn) {
	struct kvs_server_s* server = (struct kvs_server_s*)conn->global_ctx;
	if(server == NULL) {
		LOG_FATAL("server is NULL");
		assert(0);
		return KVS_ERR;
	}
	LOG_DEBUG("RDMA connection disconnected, cm_id: %p", conn->cm_id);
	kvs_server_destroy_conn_type(server, (struct kvs_conn_header_s*)conn);
	//kvs_master_slave_state_machine_tick(server->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_CONNECTION_CLOSED);
	return 0;
}


void kvs_handler_on_rdma_error(struct kvs_rdma_conn_s *conn, int event_type, int err) {
	LOG_DEBUG("RDMA error on cm_id %p, event_type: %d, err: %d:%s", conn->cm_id, event_type, err, strerror(-err));
	kvs_server_destroy_conn_type((struct kvs_server_s*)conn->global_ctx, (struct kvs_conn_header_s*)conn);
}

int kvs_handler_on_rdma_cq_recv(struct kvs_rdma_conn_s *conn, size_t recv_off_set, int recv_len, int imm_data, void* user_data) {
	struct kvs_ctx_header_s* ctx_header = conn->header.user_data;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}
	LOG_DEBUG("RDMA recv on cm_id %p, offset: %zu, len: %d, imm_data: %d", conn->cm_id, recv_off_set, recv_len, imm_data);
	if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
		struct kvs_my_master_context_s* master_ctx = (struct kvs_my_master_context_s*)ctx_header;
		master_ctx->rdb_recv_buf_offset_cur = recv_off_set;
		master_ctx->rdb_recv_len_cur = recv_len;
		master_ctx->rdb_imm_data_cur = imm_data;
		return kvs_my_master_on_rdma_recv(conn);
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		assert(0); // should not receive anything from slave via RDMA
		return kvs_my_slave_on_rdma_recv(conn);
	} else {
		LOG_FATAL("invalid ctx type: %d", ctx_header->type);
		assert(0);
		return KVS_ERR;
	}

}

int kvs_handler_on_rdma_cq_send(struct kvs_rdma_conn_s *conn, size_t send_off_set, int send_len, void* user_data) {
	struct kvs_ctx_header_s* ctx_header = conn->header.user_data;
	if(ctx_header == NULL) {
		LOG_FATAL("ctx_header is NULL");
		assert(0);
		return KVS_ERR;
	}
	
	LOG_DEBUG("conn type: %d, RDMA send on cm_id %p, offset: %zu", ctx_header->type, conn->cm_id, send_off_set);
	if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
		assert(0); // should not send anything to master via RDMA
		return kvs_my_master_on_rdma_send(conn, send_off_set, send_len);
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		return kvs_my_slave_on_rdma_send(conn, send_off_set, send_len);
	} else {
		LOG_FATAL("invalid ctx type: %d", ctx_header->type);
		assert(0);
		return KVS_ERR;
	}
}