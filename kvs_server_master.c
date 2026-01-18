#include "kvs_server.h"

#include "kvs_persistence.h"
#include "kvs_types.h"
#include "common.h"
#include "kvs_network.h"
#include "logger.h"

#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

#define KVS_MAX_SLAVES_DEFAULT 128

static inline kvs_status_t _kvs_master_slave_sync_begin(struct kvs_master_s *master, struct kvs_conn_s *slave_conn) {
    if(master == NULL || slave_conn == NULL) {
        assert(0);
        return KVS_ERR;
    }

    // increase syncing slave count
    master->syncing_slaves_count += 1;
    if(master->syncing_slaves_count == 1) {
        // first syncing slave, open rdb fd
        if(master->rdb_fd > 0) {
            close(master->rdb_fd);
        }
        master->rdb_fd = open(master->server->pers_ctx->rdb_filename, O_RDONLY);
        if(master->rdb_fd < 0) {
            LOG_FATAL("open rdb file %s failed: %s", master->server->pers_ctx->rdb_filename, strerror(errno));
            assert(0);
            return KVS_ERR;
        }
        master->is_repl_backlog = 1; // start writing to repl backlog
        master->repl_backlog_idx = 0; // reset repl backlog index
        master->repl_backlog_overflow = 0;

        struct stat rdb_st;
        fstat(master->rdb_fd, &rdb_st);
        master->rdb_size = rdb_st.st_size;
    }
    

    return KVS_OK;
}

static inline kvs_status_t _kvs_master_slave_sync_end(struct kvs_master_s *master, struct kvs_conn_s *slave_conn) {
    if(master == NULL || slave_conn == NULL) {
        assert(0);
        return KVS_ERR;
    }

    
    master->syncing_slaves_count -= 1;
    if(master->syncing_slaves_count < 0) {
        LOG_FATAL("syncing_slaves_count < 0");
        assert(0);
        return KVS_ERR;
    } else if(master->syncing_slaves_count == 0) {
        // all syncing slaves finished, close rdb fd
        if(master->rdb_fd > 0) {
            close(master->rdb_fd);
        }
        master->rdb_fd = -1; 
        master->repl_backlog_idx = 0; // reset backlog
        master->is_repl_backlog = 0; // stop writing to repl backlog
        master->repl_backlog_overflow = 0;
    }

    return KVS_OK;
}

kvs_status_t kvs_master_add_slave(struct kvs_master_s *master, struct kvs_conn_s *conn);
kvs_status_t _kvs_repl_slave_sending_rdb_handler(struct kvs_master_s *master, struct kvs_conn_s *conn);

kvs_status_t kvs_master_init(struct kvs_master_s *master, struct kvs_server_s *server, struct kvs_master_config_s *config) {
    if(master == NULL || server == NULL || config == NULL) return KVS_ERR;
    master->server = server;

    master->slave_count = 0;
    master->slave_count_online = 0;

    // init config
    master->max_slave_count = config-> max_slave_count <= 0 ? KVS_MAX_SLAVES_DEFAULT : config->max_slave_count;
    master->repl_backlog_size = config->repl_backlog_size;

    // init repl backlog
    master->repl_backlog = (char *)kvs_malloc(master->repl_backlog_size);
    if(master->repl_backlog == NULL) {
        LOG_FATAL("malloc repl_backlog failed");
        return KVS_ERR;
    }
    memset(master->repl_backlog, 0, master->repl_backlog_size);
    master->syncing_slaves_count = 0;
    master->repl_backlog = NULL;
    master->repl_backlog_idx = 0;
    master->rdb_fd = -1;
    master->repl_backlog_overflow = 0;

    master->slave_conns = (struct kvs_conn_s **)kvs_malloc(sizeof(struct kvs_conn_s *) * master->max_slave_count);
    if(master->slave_conns == NULL) {
        LOG_FATAL("malloc slave_conns failed\n");
        assert(0);
        return KVS_ERR;
    }
    memset(master->slave_conns, 0, sizeof(struct kvs_conn_s *) * master->max_slave_count);

    return KVS_OK;
}

kvs_status_t kvs_master_deinit(struct kvs_master_s *master) {
    if(master == NULL) return KVS_ERR;

    kvs_free(master->slave_conns, sizeof(struct kvs_conn_s *) * master->max_slave_count); // if ptr is NULL, free does nothing
    master->slave_conns = NULL;
    kvs_free(master->repl_backlog, master->repl_backlog_size);
    master->repl_backlog = NULL;

    if(master->rdb_fd > 0) {
        close(master->rdb_fd);
    }
    master->rdb_fd = -1;

    return KVS_OK;
}



kvs_status_t _kvs_repl_slave_none_handler(struct kvs_master_s *master, struct kvs_conn_s *slave_conn) {
    // 1. check if can accept new slave
    if(master->slave_count >= master->max_slave_count) {
        // slave limit reached
        LOG_WARN("slave limit reached");
        // todo : return more error info to slave
        return KVS_QUIT;
    }
    master->slave_count ++;


    if(slave_conn == NULL) assert(0);
    if(slave_conn->bussiness_ctx == NULL) assert(0);
    struct kvs_repl_slave_context_s* slave_ctx = (struct kvs_repl_slave_context_s*)slave_conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME || slave_ctx->slave_idx != -1) {
        LOG_FATAL("invalid slave state: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    LOG_DEBUG("Adding new slave, current slave count: %d", master->slave_count);
    int ret = 0;
    // 2. start RDB save if not in progress
    if(master->server->rdb_child_pid <= 0) {
        // no RDB save in progress, start a new one
        ret = kvs_server_save_rdb_fork(master->server);
        if(ret != KVS_OK) {
            LOG_FATAL("kvs_server_save_rdb_fork failed");
            assert(0);
            return KVS_ERR;
        }
    }
    slave_ctx->state = KVS_REPL_SLAVE_WAIT_BGSAVE_END;
    

    return KVS_OK;
}


kvs_status_t _kvs_repl_slave_wait_bgsave_end_handler(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(conn == NULL || conn->bussiness_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    struct kvs_repl_slave_context_s *slave_ctx = (struct kvs_repl_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    if(master->server->rdb_child_pid > 0) {
        assert(0);
        return KVS_ERR; // still saving
    }

    // RDB save finished
    
    if(master->rdb_fd > 0 && master->syncing_slaves_count == 0) {
        close(master->rdb_fd);
    }


    //LOG_INFO("RDB save finished, start sending RDB to slave fd %d", conn->_internal.fd);
    // 动作：切换状态，开始发文件
    slave_ctx->state = KVS_REPL_SLAVE_SENDING_RDB;
    return KVS_STATUS_CONTINUE; // trigger sending immediately
}


kvs_status_t _kvs_repl_slave_sending_rdb_handler(struct kvs_master_s *master, struct kvs_conn_s *slave_conn) {
    /************io uring发送 *******/
    if(slave_conn == NULL || slave_conn->bussiness_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    struct kvs_repl_slave_context_s *slave_ctx = (struct kvs_repl_slave_context_s*)slave_conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    size_t *rdb_offset = &slave_ctx->rdb_offset;
    assert(slave_conn->w_idx == 0);

    _kvs_master_slave_sync_begin(master, slave_conn);

    if(*rdb_offset == 0) {
        // first time sending, send rdb size first
        int head_size = snprintf(slave_conn->w_buffer, slave_conn->w_buf_sz, "$%ld\r\n", master->rdb_size);
        slave_conn->w_idx += head_size;
    }


    int pret = pread(master->rdb_fd, slave_conn->w_buffer + slave_conn->w_idx, slave_conn->w_buf_sz - slave_conn->w_idx, *rdb_offset);
    if(pret < 0) {
        printf("%s:%d pread rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
        assert(0);
    } else if(pret == 0) {
        // finish sending rdb file
        LOG_DEBUG("finish sending rdb file to slave");
        // rdb file descriptor will be closed when next rdb save
        // close(conn->server->master.rdb_fd);
        // conn->server->master.rdb_fd = -1;
        slave_ctx->state = KVS_REPL_SLAVE_SENDING_BACKLOG;
        *rdb_offset = 0;
        assert(slave_conn->w_idx == 0);
        return KVS_STATUS_CONTINUE; // continue to send backlog
    } else {
        *rdb_offset += pret;
        LOG_DEBUG("rdb send size: %d", pret);
        slave_conn->w_idx += pret;
        kvs_net_set_send_event_manual(slave_conn);
    }
    /************** rdma发送 **************/
    return KVS_OK;
}

kvs_status_t _kvs_repl_slave_sending_backlog_handler(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(conn == NULL || conn->bussiness_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    
    struct kvs_repl_slave_context_s *slave_ctx = (struct kvs_repl_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    size_t *repl_backlog_offset = &slave_ctx->repl_backlog_offset;
    assert(conn->w_idx == 0);
    size_t to_send = master->repl_backlog_idx - *repl_backlog_offset;

    if(to_send > conn->w_buf_sz) {
        to_send = conn->w_buf_sz;
    }
    if(to_send == 0) {
        // finish sending backlog
        LOG_DEBUG("finish sending backlog to slave");
        slave_ctx->state = KVS_REPL_SLAVE_ONLINE;
        _kvs_master_slave_sync_end(master, conn);
        kvs_master_add_slave(master, conn);
        *repl_backlog_offset = 0;
        assert(conn->w_idx == 0);
        return KVS_STATUS_CONTINUE; // done
    } else {
        memcpy(conn->w_buffer, master->repl_backlog + *repl_backlog_offset, to_send);
        *repl_backlog_offset += to_send;
        conn->w_idx = to_send;
        kvs_net_set_send_event_manual(conn);
    }
    return KVS_OK;
}

kvs_status_t kvs_repl_slave_online_handler(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(conn == NULL || conn->bussiness_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    struct kvs_repl_slave_context_s *slave_ctx = (struct kvs_repl_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    
    assert(conn->w_idx == 0);


    assert(slave_ctx->slave_idx != -1);
    
    // now in online state, do nothing
    return KVS_OK;
}

typedef kvs_status_t (*slave_state_handler_t)(struct kvs_master_s *master, struct kvs_conn_s *slave_conn);

slave_state_handler_t slave_state_handlers[] = {
    [KVS_REPL_SLAVE_NONE] = _kvs_repl_slave_none_handler,
    [KVS_REPL_SLAVE_WAIT_BGSAVE_END] = _kvs_repl_slave_wait_bgsave_end_handler,
    [KVS_REPL_SLAVE_SENDING_RDB] = _kvs_repl_slave_sending_rdb_handler,
    [KVS_REPL_SLAVE_SENDING_BACKLOG] = _kvs_repl_slave_sending_backlog_handler,
    [KVS_REPL_SLAVE_ONLINE] = kvs_repl_slave_online_handler,
    [KVS_REPL_SLAVE_OFFLINE] = NULL
};

kvs_status_t kvs_master_slave_state_machine_tick(struct kvs_master_s *master, struct kvs_conn_s *slave_conn) {
    if(master == NULL || slave_conn == NULL) return KVS_ERR;
    struct kvs_repl_slave_context_s* slave_ctx = (struct kvs_repl_slave_context_s*)slave_conn->bussiness_ctx;
    if(slave_ctx == NULL) {
        LOG_FATAL("slave_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    kvs_status_t ret;

    do {

        kvs_repl_slave_state_t state = slave_ctx->state;
        if(state < KVS_REPL_SLAVE_NONE || state >= KVS_REPL_SLAVE_STATE_NUM) {
            LOG_FATAL("invalid slave state: %d", state);
            assert(0);
            return KVS_ERR;
        }

        slave_state_handler_t handler = slave_state_handlers[state];
        if(handler == NULL) {
            LOG_FATAL("handler for state %d is NULL", state);
            assert(0);
            return KVS_ERR;
        }
        
    
        ret = handler(master, slave_conn); // ignore return value for now
        if(ret == KVS_ERR) {
            // 状态回退
            LOG_FATAL("slave state handler for state %d failed", state);
            assert(0);
            return KVS_ERR;
        } else if(ret == KVS_QUIT) {
            // 断开连接
            LOG_INFO("slave state handler for state %d requested to quit", state);
            assert(0);
            return KVS_QUIT;
        }
    } while (ret == KVS_STATUS_CONTINUE);
   

    return KVS_OK;
}



kvs_status_t kvs_master_add_slave(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    if(conn->bussiness_ctx == NULL) {
        LOG_FATAL("conn bussiness_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    struct kvs_repl_slave_context_s* slave_ctx = (struct kvs_repl_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    if(master->slave_count_online >= master->max_slave_count) {
        LOG_FATAL("slave count exceed max_slave_count: %d", master->max_slave_count);
        assert(0);
        return KVS_ERR;
    }

    slave_ctx->slave_idx = master->slave_count_online;
    master->slave_conns[master->slave_count_online] = conn;
    master->slave_count_online ++;
    LOG_DEBUG("add new slave idx: %d, fd: %d\n", slave_ctx->slave_idx, conn->_internal.fd);
    return KVS_OK;
}

/**
 * @brief Remove a slave connection from master, but not reset the connection
 */
kvs_status_t kvs_master_remove_slave(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    if(conn->bussiness_ctx == NULL) {
        LOG_FATAL("conn bussiness_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    struct kvs_repl_slave_context_s* slave_ctx = (struct kvs_repl_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    int slave_idx = slave_ctx->slave_idx;
    if(slave_idx < 0 || slave_idx >= master->slave_count_online) {
        LOG_FATAL("invalid slave idx: %d, slave_count: %d", slave_idx, master->slave_count_online);
        assert(0);
        return KVS_ERR;
    }

    LOG_DEBUG("remove slave idx: %d, fd: %d\n", slave_idx, master->slave_conns[slave_idx]->_internal.fd);
    // remove the slave connection from the array
    if(slave_idx < master->slave_count_online -1 ) {
        struct kvs_conn_s *last_slave_conn = master->slave_conns[master->slave_count_online - 1];
        master->slave_conns[master->slave_count_online - 1] = NULL;
        master->slave_conns[slave_idx] = last_slave_conn;
        struct kvs_repl_slave_context_s* last_slave_ctx = (struct kvs_repl_slave_context_s*)last_slave_conn->bussiness_ctx;
        if(last_slave_ctx == NULL || last_slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
            LOG_FATAL("invalid last slave ctx");
            assert(0);
            return KVS_ERR;
        }
        last_slave_ctx->slave_idx = slave_idx;
    } else if(slave_idx == master->slave_count_online -1 ) {
        master->slave_conns[slave_idx] = NULL;
    } else {
        LOG_FATAL("invalid slave idx: %d, slave_count: %d", slave_idx, master->slave_count_online);
        assert(0);
        return KVS_ERR;
    }
    master->slave_count_online --;
    master->slave_count --;
    return KVS_OK;
}


kvs_status_t kvs_master_propagate_command_to_slaves(struct kvs_master_s *master, struct kvs_handler_cmd_s *cmd) {
    if(master == NULL || cmd == NULL) {
        return KVS_ERR;
    }
    if(cmd->cmd_type & KVS_CMD_WRITE) {
        // propagate to slaves
        //LOG_DEBUG("propagate command to %d slaves", master->slave_count_online);
        for(int i = 0; i < master->slave_count_online; i++) {
            struct kvs_conn_s *slave_conn = master->slave_conns[i];
            if(slave_conn == NULL) {
                LOG_FATAL("slave_conn is NULL at idx: %d", i);
                assert(0);
                return KVS_ERR;
            }
            // append to slave's send buffer
            if(slave_conn->w_idx + cmd->raw_len > slave_conn->w_buf_sz) {
                LOG_WARN("slave conn fd %d send buffer full, drop command", slave_conn->_internal.fd);
                continue;
            }
            memcpy(slave_conn->w_buffer + slave_conn->w_idx, cmd->raw_ptr, cmd->raw_len);
            slave_conn->w_idx += cmd->raw_len;
            kvs_net_set_send_event_manual(slave_conn);
        }
    }
    return KVS_OK;
}