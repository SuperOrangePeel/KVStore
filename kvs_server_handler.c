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
// 	// // kvs_server_create_conn_ctx(server, (struct kvs_conn_header_s*)conn, KVS_CTX_MASTER_OF_ME);
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
	kvs_status_t status = kvs_master_slave_state_machine_tick(server->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_CONNECTED);
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
		status = kvs_slave_master_state_machine_tick(server->slave, 
			(struct kvs_conn_header_s *)conn, 
			KVS_EVENT_WRITE_DONE); // master是accept，所以是write_done
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		status = kvs_master_slave_state_machine_tick(server->master, 
			(struct kvs_conn_header_s *)conn, 
			KVS_EVENT_READ_READY); // slave是connect，所以是read_ready
	} else {
		LOG_FATAL("invalid ctx type: %d", ctx_header->type);
		return KVS_ERR;
	}

	if(status != KVS_OK) {
		LOG_ERROR("Error in RDMA established state machine, conn type:%d, status: %d", ctx_header->type, status);
		return KVS_ERR;
	}

	// if(status == KVS_QUIT) {
	// 	LOG_INFO("RDMA connection closed by state machine, token: %ld", ((struct kvs_my_slave_context_s*)conn->header.user_data)->rdma_token);
	// 	return -1; // close connection
	// }
	
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
	
	kvs_server_destroy_conn_ctx(server, (struct kvs_conn_header_s*)conn);
	//kvs_master_slave_state_machine_tick(server->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_CONNECTION_CLOSED);
	return 0;
}


void kvs_handler_on_rdma_error(struct kvs_rdma_conn_s *conn, int event_type, int err) {
	LOG_DEBUG("RDMA error on cm_id %p, event_type: %d, err: %d:%s", conn->cm_id, event_type, err, strerror(-err));
	kvs_server_destroy_conn_ctx((struct kvs_server_s*)conn->global_ctx, (struct kvs_conn_header_s*)conn);
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
		return kvs_my_master_on_rdma_recv(conn, recv_off_set, recv_len, imm_data);
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		return kvs_my_slave_on_rdma_recv(conn, recv_off_set, recv_len, imm_data);
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
		return kvs_my_master_on_rdma_send(conn, send_off_set, send_len);
	} else if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
		return kvs_my_slave_on_rdma_send(conn, send_off_set, send_len);
	} else {
		LOG_FATAL("invalid ctx type: %d", ctx_header->type);
		assert(0);
		return KVS_ERR;
	}
}