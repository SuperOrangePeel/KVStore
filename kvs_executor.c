#include "kvs_executor.h"

#include "kvs_types.h"
#include "common.h"
#include "logger.h"
#include "kvs_server.h"

#include <stddef.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>




typedef kvs_result_t (*cmd_proc_t)(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);

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
    return kvs_server_hset(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
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
    kvs_server_convert_conn_type(server, conn, KVS_CTX_SLAVE_OF_ME);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    slave_ctx->state = KVS_MY_SLAVE_NONE;
    kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_COMMAND_RECEIVED);

    return KVS_RES_SYNC_SLAVE;
    
}

kvs_result_t _kvs_exec_slave_sync_rdma(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server->master->slave_count + server->master->syncing_slaves_count >= server->master->max_slave_count) {
        // todo : return more error info to slave 
        return KVS_RES_ERR;
    }
    kvs_server_convert_conn_type(server, conn, KVS_CTX_SLAVE_OF_ME);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    slave_ctx->state = KVS_MY_SLAVE_NONE;
    kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_COMMAND_RECEIVED);

    return KVS_RES_SYNC_SLAVE;
}

static cmd_proc_t command_table[] = {
    [KVS_CMD_SET] = _kvs_exec_set,
    [KVS_CMD_GET] = _kvs_exec_get,
    [KVS_CMD_DEL] = _kvs_exec_del,
    [KVS_CMD_MOD] = _kvs_exec_mod,
    [KVS_CMD_EXIST] = _kvs_exec_exist,
    // rbtree
    [KVS_CMD_RSET] = _kvs_exec_rset,
    [KVS_CMD_RGET] = _kvs_exec_rget,
    [KVS_CMD_RDEL] = _kvs_exec_rdel,
    [KVS_CMD_RMOD] = _kvs_exec_rmod,
    [KVS_CMD_REXIST] = _kvs_exec_rexist,
    // hash
    [KVS_CMD_HSET] = _kvs_exec_hset,
    [KVS_CMD_HGET] = _kvs_exec_hget,
    [KVS_CMD_HDEL] = _kvs_exec_hdel,
    [KVS_CMD_HMOD] = _kvs_exec_hmod,
    [KVS_CMD_HEXIST] = _kvs_exec_hexist,
    //save
    [KVS_CMD_SAVE] = _kvs_exec_save,
    //slave sync
    [KVS_CMD_SLAVE_SYNC] = _kvs_exec_slave_sync,
    [KVS_CMD_SLAVE_SYNC_RDMA] = _kvs_exec_slave_sync_rdma,
};

kvs_result_t kvs_executor_cmd(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(cmd == NULL) return KVS_RES_ERR;
    if(cmd->cmd_idx == KVS_CMD_INVALID) {
        return KVS_RES_UNKNOWN_CMD;
    }
    if(cmd->cmd_idx < KVS_CMD_START || cmd->cmd_idx >= KVS_CMD_COUNT) {
        return KVS_RES_ERR;
    }

    cmd_proc_t proc = command_table[cmd->cmd_idx];
    if(proc == NULL) {
        return KVS_RES_ERR;
    }
    
    return proc(server, cmd, conn);
}