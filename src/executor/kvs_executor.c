#include "kvs_executor.h"

#include "kvs_types.h"
#include "common.h"
#include "logger.h"
#include "kvs_server.h"

#include <stddef.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>




kvs_result_t _kvs_exec_set(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_set(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_get(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    
    return kvs_server_get(server, cmd->key, cmd->len_key, &(cmd->val), &(cmd->len_val));
}

kvs_result_t _kvs_exec_del(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_del(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_mod(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_mod(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_exist(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_exist(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_rset(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rset(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_rget(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rget(server, cmd->key, cmd->len_key, &(cmd->val), &(cmd->len_val));
}

kvs_result_t _kvs_exec_rdel(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rdel(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_rmod(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rmod(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_rexist(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rexist(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_hset(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    kvs_result_t result = kvs_server_hset(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
    if(result == KVS_RES_EXIST) {
        result = KVS_RES_OK; // for redis-benchmark compatibility
    }
    return result;
}

kvs_result_t _kvs_exec_hget(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hget(server, cmd->key, cmd->len_key, &(cmd->val), &(cmd->len_val));
}

kvs_result_t _kvs_exec_hdel(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hdel(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_hmod(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hmod(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_hexist(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hexist(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_save(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    if(KVS_OK == kvs_server_save_rdb_fork(server)) {
        struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->header.user_data;
        if(cli_ctx->header.type != KVS_CTX_NORMAL_CLIENT) {
            LOG_FATAL("invalid ctx type: %d", cli_ctx->header.type);
            assert(0);
            return KVS_RES_ERR;
        }
        cli_ctx->state = KVS_CLIENT_STATE_WAIT_BGSAVE; // set client state to wait bgsave

        return KVS_RES_RDB_SKIP_RESPONSE;
    }
    return KVS_RES_ERR;
    //return kvs_server_save_rdb(server);
}

kvs_result_t _kvs_exec_slave_sync(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server->master->slave_count + server->master->syncing_slaves_count >= server->master->max_slave_count) {
        // todo : return more error info to slave 
        return KVS_RES_ERR;
    }
    kvs_server_convert_conn_ctx(server, conn, KVS_CTX_SLAVE_OF_ME);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    slave_ctx->state = KVS_MY_SLAVE_NONE;
    kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_TRIGGER_MANUAL);

    return KVS_RES_SYNC_SLAVE;
    
}

kvs_result_t _kvs_exec_slave_sync_rdma(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server->master->slave_count + server->master->syncing_slaves_count >= server->master->max_slave_count) {
        // todo : return more error info to slave 
        return KVS_RES_ERR;
    }
    kvs_server_convert_conn_ctx(server, conn, KVS_CTX_SLAVE_OF_ME);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    slave_ctx->state = KVS_MY_SLAVE_NONE;
    kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_TRIGGER_MANUAL);

    return KVS_RES_SYNC_SLAVE;
}


kvs_result_t _kvs_exec_echo(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    // just echo the value back
    cmd->val = cmd->key;
    cmd->len_val = cmd->len_key;
    return KVS_RES_VAL;
}

kvs_result_t kvs_executor_cmd(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(cmd == NULL) return KVS_RES_ERR;

    switch(cmd->cmd_idx) {
        case KVS_CMD_SET:
            return _kvs_exec_hset(server, cmd, conn);
        case KVS_CMD_GET:
            return _kvs_exec_hget(server, cmd, conn);
        case KVS_CMD_DEL:
            return _kvs_exec_hdel(server, cmd, conn);
        case KVS_CMD_MOD:
            return _kvs_exec_hmod(server, cmd, conn);
        case KVS_CMD_EXIST:
            return _kvs_exec_hexist(server, cmd, conn);
        case KVS_CMD_RSET:
            return _kvs_exec_rset(server, cmd, conn);
        case KVS_CMD_RGET:
            return _kvs_exec_rget(server, cmd, conn);
        case KVS_CMD_RDEL:
            return _kvs_exec_rdel(server, cmd, conn);
        case KVS_CMD_RMOD:
            return _kvs_exec_rmod(server, cmd, conn);
        case KVS_CMD_REXIST:
            return _kvs_exec_rexist(server, cmd, conn);
        case KVS_CMD_ASET:
            return _kvs_exec_set(server, cmd, conn);
        case KVS_CMD_AGET:
            return _kvs_exec_get(server, cmd, conn);
        case KVS_CMD_ADEL:
            return _kvs_exec_del(server, cmd, conn);
        case KVS_CMD_AMOD:
            return _kvs_exec_mod(server, cmd, conn);
        case KVS_CMD_AEXIST:
            return _kvs_exec_exist(server, cmd, conn);
        case KVS_CMD_SAVE:
            return _kvs_exec_save(server, cmd, conn);
        case KVS_CMD_SLAVE_SYNC:
            return _kvs_exec_slave_sync(server, cmd, conn);
        case KVS_CMD_SLAVE_SYNC_RDMA:
            return _kvs_exec_slave_sync_rdma(server, cmd, conn);
        case KVS_CMD_ECHO:
            return _kvs_exec_echo(server, cmd, conn);
        case KVS_CMD_INVALID:
            return KVS_RES_UNKNOWN_CMD;
        default:
            return KVS_RES_ERR;
    }
}
