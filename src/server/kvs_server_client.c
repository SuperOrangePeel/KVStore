
#include "kvs_network.h"
#include "kvs_server.h"
#include "kvs_types.h"
#include "logger.h"
#include "kvs_executor.h"
#include "kvs_resp_protocol.h"
#include "kvs_response.h"
#include "kvs_qa.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>


static inline kvs_status_t append_simple_reply(struct kvs_conn_s *conn, const char *reply, int len) {
    if(conn->s_idx + len > conn->s_buf_sz) {
        return KVS_ERR;
    }
    memcpy(conn->s_buffer + conn->s_idx, reply, len);
    conn->s_idx += len;
    return KVS_OK;
}

static inline kvs_status_t append_bulk_reply(struct kvs_conn_s *conn, char *value, int len_val) {
    char header[64];
    int header_len = snprintf(header, sizeof(header), "$%d\r\n", len_val);
    int total_len = header_len + len_val + 2;
    if(value == NULL || len_val <= 0 || header_len <= 0 || conn->s_idx + total_len > conn->s_buf_sz) {
        return KVS_ERR;
    }
    memcpy(conn->s_buffer + conn->s_idx, header, header_len);
    conn->s_idx += header_len;
    memcpy(conn->s_buffer + conn->s_idx, value, len_val);
    conn->s_idx += len_val;
    memcpy(conn->s_buffer + conn->s_idx, "\r\n", 2);
    conn->s_idx += 2;
    return KVS_OK;
}

typedef kvs_status_t (*kvs_client_state_handler_t)(struct kvs_server_s *server, struct kvs_conn_s *conn);

kvs_status_t _kvs_client_state_normal(struct kvs_server_s *server, struct kvs_conn_s *conn) {
    assert(0); // should not reach here for now
    // normal client, do nothing
    return KVS_OK;
}

kvs_status_t _kvs_client_state_wait_bgsave(struct kvs_server_s *server, struct kvs_conn_s *conn) {
    if(server == NULL || conn == NULL) {
        assert(0);
        return KVS_ERR;
    }

    struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->header.user_data;
    if(cli_ctx == NULL || cli_ctx->header.type != KVS_CTX_NORMAL_CLIENT) {
        LOG_FATAL("invalid cli_ctx or ctx type, ctx: %d, type: %d", cli_ctx == NULL ? -1 : 1, 
            cli_ctx == NULL ? -1 : cli_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    // wait bgsave finished

    kvs_format_response(KVS_RES_OK, NULL, 0, conn);
    //kvs_format_response(KVS_RES_OK, NULL, 0, conn);

    kvs_net_set_send_event_manual(conn);
    
    cli_ctx->state = KVS_CLIENT_STATE_NORMAL; // back to normal state
    return KVS_OK;
}

kvs_client_state_handler_t _kvs_server_client_state_handlers[] = {
    [KVS_CLIENT_STATE_NORMAL] = _kvs_client_state_normal, 
    [KVS_CLIENT_STATE_WAIT_BGSAVE] = _kvs_client_state_wait_bgsave, 
    [KVS_CLIENT_STATE_CLOSE_PENDING] = NULL, // KVS_CLIENT_STATE_NORMAL
};

kvs_status_t kvs_client_state_machine_tick(struct kvs_server_s *server, struct kvs_conn_s *conn) {
    if(server == NULL || conn == NULL) return KVS_ERR;
    struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->header.user_data;
    if(cli_ctx == NULL) {
        LOG_FATAL("cli_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    if(cli_ctx->header.type != KVS_CTX_NORMAL_CLIENT) {
        LOG_FATAL("invalid ctx type: %d", cli_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    
    
    kvs_status_t ret = KVS_OK;
    do {
        kvs_client_state_t state = cli_ctx->state;
        if(state < KVS_CLIENT_STATE_NORMAL || state >= KVS_CLIENT_STATE_NUM) {
            LOG_FATAL("invalid client state: %d", state);
            assert(0);
            return KVS_ERR;
        }

        kvs_client_state_handler_t handler = _kvs_server_client_state_handlers[state];
        if(handler == NULL) {
            LOG_FATAL("handler for state %d is NULL", state);
            assert(0);
            return KVS_ERR;
        }

        kvs_status_t ret = handler(server, conn);
        if(ret == KVS_ERR) {
            LOG_FATAL("client state handler for state %d failed", state);
            assert(0);
            return KVS_ERR;
        } else if(ret == KVS_QUIT) {
            // 断开连接
            LOG_INFO("client state handler for state %d requested to quit", state);
            assert(0);
            return KVS_QUIT;
        }
    } while (ret == KVS_STATUS_CONTINUE);

    return KVS_OK;
}




int kvs_server_after_write(struct kvs_server_s *server) {
    if(server == NULL) return -1;
    if(server->pers_ctx->rdb_policy > 0) server->write_command_count++;
    if(server->pers_ctx->rdb_policy > 0 && server->write_command_count >= server->pers_ctx->rdb_policy && server->rdb_child_pid <= 0) {
        if(server->rdb_child_pid <= 0) {
            kvs_server_save_rdb_fork(server);
            server->write_command_count = 0;
        }
    }
    return 0;
}

int kvs_server_write_and_replicate_raw(struct kvs_server_s *server, char *raw, int raw_len) {
    struct kvs_handler_cmd_s tmp;
    if(server == NULL || raw == NULL || raw_len <= 0) return -1;
    if(server->pers_ctx->aof_enabled && kvs_persistence_write_aof(server->pers_ctx, raw, raw_len) < 0) {
        LOG_ERROR("append materialized AOF failed");
        return -1;
    }
    memset(&tmp, 0, sizeof(tmp));
    tmp.cmd_type = KVS_CMD_WRITE;
    tmp.raw_ptr = raw;
    tmp.raw_len = raw_len;
    if(server->role == KVS_SERVER_ROLE_MASTER && server->master != NULL && server->master->slave_count > 0) {
        kvs_master_propagate_command_to_slaves(server->master, &tmp);
    }
    return kvs_server_after_write(server);
}

static kvs_status_t _client_cmd_logic(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    struct kvs_client_context_s* ctx_header = (struct kvs_client_context_s*)conn->header.user_data;
    if(ctx_header == NULL || ctx_header->header.type != KVS_CTX_NORMAL_CLIENT) {
        LOG_FATAL("invalid ctx_header or ctx type, ctx: %d, type: %d", ctx_header == NULL ? -1 : 1, 
            ctx_header == NULL ? -1 : ctx_header->header.type);
        assert(0);
        return KVS_ERR;
    }
    if(server->role == KVS_SERVER_ROLE_SLAVE && cmd->cmd_type & KVS_CMD_WRITE ) {
        // slave can not accept write commands
        kvs_format_response(KVS_RES_ERR, cmd->val, cmd->len_val, conn);
        LOG_WARN("slave server can not accept write commands from client");
        return KVS_OK;
    }

    if(server->pers_ctx->aof_enabled &&
       (cmd->cmd_type & KVS_CMD_WRITE) &&
       kvs_persistence_aof_should_backpressure(server->pers_ctx, (size_t)cmd->raw_len)) {
        kvs_net_pending_conn(conn);
        return KVS_AGAIN;
    }

    if(cmd->cmd_idx == KVS_CMD_SET) {
        kvs_result_t result = kvs_server_hset(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
        if(result == KVS_RES_EXIST) {
            result = KVS_RES_OK;
        }
        if(result != KVS_RES_OK) {
            return KVS_ERR;
        }

        if(kvs_server_write_and_replicate_raw(server, cmd->raw_ptr, cmd->raw_len) != 0) {
            return KVS_ERR;
        }

        return append_simple_reply(conn, "+OK\r\n", sizeof("+OK\r\n") - 1);
    }

    if(cmd->cmd_idx == KVS_CMD_GET) {
        char *value = NULL;
        int len_val = 0;
        kvs_result_t result = kvs_server_hget(server, cmd->key, cmd->len_key, &value, &len_val);
        if(result == KVS_RES_VAL) {
            return append_bulk_reply(conn, value, len_val);
        }
        if(result == KVS_RES_NOT_FOUND) {
            return append_simple_reply(conn, "-NOT FOUND\r\n", sizeof("-NOT FOUND\r\n") - 1);
        }
        return KVS_ERR;
    }

    if(cmd->cmd_idx == KVS_CMD_ECHO) {
        if(cmd->key == NULL || cmd->len_key <= 0) {
            return append_simple_reply(conn, "-ERROR\r\n", sizeof("-ERROR\r\n") - 1);
        }
        return append_bulk_reply(conn, cmd->key, cmd->len_key);
    }

    kvs_result_t result = kvs_executor_cmd(server, cmd, conn);

    if(result == KVS_RES_BLOCKED) {
        return KVS_STATUS_CONTINUE;
    }
    if(result == KVS_RES_SKIP_RESPONSE || result == KVS_RES_RDB_SKIP_RESPONSE){
		return KVS_OK;
    }
    if(result == KVS_RES_SYNC_SLAVE) {
        return KVS_BREAK;
    }
    // if(ctx_header->type == KVS_CTX_MASTER_OF_ME) {
    //     // master connection
    //     return KVS_OK; // do nothing for now
    // }

    if((cmd->cmd_type & KVS_CMD_WRITE) && result == KVS_RES_OK) {
        if(kvs_server_write_and_replicate_raw(server, cmd->raw_ptr, cmd->raw_len) != 0) {
            return KVS_ERR;
        }
    }
    kvs_format_response(result, cmd->val, cmd->len_val, conn);

    return KVS_OK;
}

kvs_status_t kvs_client_process_buffer(struct kvs_conn_s *conn) {
    int parsed_len = 0;
    int parsed_total_len = 0;
    struct kvs_server_s *server = (struct kvs_server_s *)conn->server_ctx;
    char *r_buffer = conn->r_buffer;
    int r_idx = conn->r_idx;
    struct kvs_handler_cmd_s cmd;

    while(parsed_total_len < conn->r_idx) {
        memset(&cmd, 0, sizeof(cmd));
        kvs_status_t status = kvs_resp_parser(r_buffer + parsed_total_len,
                                               r_idx - parsed_total_len,
                                               &cmd,
                                               &parsed_len);
        if(status == KVS_ERR) {
            LOG_ERROR("protocol parse error");
            assert(0);
            return KVS_ERR;
        } else if(status == KVS_AGAIN) {
            break;
        }

        parsed_total_len += parsed_len;
        kvs_status_t result = _client_cmd_logic(server, &cmd, conn);
        if(result == KVS_STATUS_CONTINUE) {
            break;
        } else if(result == KVS_BREAK) {
            parsed_total_len = conn->r_idx;
            break;
        } else if(result == KVS_AGAIN) {
            parsed_total_len -= parsed_len;
            break;
        } else if(result == KVS_ERR) {
            LOG_ERROR("execute command failed");
            assert(0);
            return KVS_ERR;
        } else if(result == KVS_QUIT) {
            LOG_INFO("client requested to quit");
            assert(0);
            return KVS_QUIT;
        }
    }

    if(parsed_total_len > 0) {
        int remain = conn->r_idx - parsed_total_len;
        if(remain > 0) memmove(conn->r_buffer, conn->r_buffer + parsed_total_len, (size_t)remain);
        conn->r_idx = remain;
    }
    if(conn->s_idx > 0) {
        kvs_net_set_send_event_manual(conn);
    }
    return KVS_OK;
}

kvs_status_t kvs_client_on_recv(struct kvs_conn_s *conn, int *read_size) {
    kvs_status_t ret;
    if(conn == NULL || read_size == NULL) return KVS_ERR;
    ret = kvs_client_process_buffer(conn);
    /* kvs_client_process_buffer already compacts conn->r_buffer. */
    *read_size = 0;
    return ret;
}

kvs_status_t kvs_client_on_send(struct kvs_conn_s *conn, int bytes_sent) {
    struct kvs_client_context_s *ctx = conn ? (struct kvs_client_context_s *)conn->header.user_data : NULL;
    (void)bytes_sent;
    if(ctx == NULL || ctx->header.type != KVS_CTX_NORMAL_CLIENT || !(ctx->flags & KVS_CLIENT_BLOCKED_EMBEDDING)) {
        kvs_net_set_recv_event(conn);
    }
    return KVS_OK;
}

void kvs_client_on_close(struct kvs_conn_s *conn) {
    kvs_qa_detach_client(conn);
    LOG_DEBUG("normal client disconnected, fd: %d\n", conn->_internal.fd);
    return; // client close do nothing
}
