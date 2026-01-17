#include "kvs_server.h"

#include "kvs_persistence.h"
#include "kvs_types.h"
#include "common.h"
#include "kvs_network.h"
#include "logger.h"

#include <assert.h>
#include <unistd.h>
#include <string.h>

#define KVS_MAX_SLAVES_DEFAULT 128

kvs_status_t kvs_master_init(struct kvs_master_s *master, struct kvs_server_s *server, struct kvs_master_config_s *config) {
    if(master == NULL || server == NULL || config == NULL) return KVS_ERR;
    master->server = server;

    master->slave_count = 0;

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

kvs_status_t kvs_master_slave_connection_init(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    
    LOG_DEBUG("%s:%d convert connection to slave, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);
    struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
    if(ctx_header == NULL) {
        LOG_FATAL("%s:%d ctx_header is NULL\n", __FILE__, __LINE__);
        assert(0);
        return -1;
    }

    if(ctx_header->type == KVS_CTX_NORMAL_CLIENT) {
        // normal client connection changed to slave connection
        kvs_free(ctx_header, sizeof(struct kvs_ctx_header_s));
        struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)kvs_malloc(sizeof(struct kvs_master_slave_context_s));
        memset(slave_ctx, 0, sizeof(struct kvs_master_slave_context_s));
        slave_ctx->header.type = KVS_CTX_SLAVE_OF_ME;
        conn->bussiness_ctx = (void*)slave_ctx;
        slave_ctx->slave_idx = -1; // will be assigned later
    } else {
        LOG_FATAL("%s:%d unknown ctx type: %d\n", __FILE__, __LINE__, ctx_header->type);
        assert(0);
        return -1;
    }

    return KVS_OK;
}

kvs_status_t kvs_master_slave_connection_deinit(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    
    
    LOG_DEBUG("%s:%d slave connection closed, fd: %d\n", __FILE__, __LINE__, conn->_internal.fd);
    struct kvs_ctx_header_s* ctx_header = (struct kvs_ctx_header_s*)conn->bussiness_ctx;
    if(ctx_header == NULL) {
        LOG_FATAL("ctx_header is NULL");
        assert(0);
        return KVS_ERR;
    }
    if(ctx_header->type == KVS_CTX_SLAVE_OF_ME) {
        // slave connection closed
        struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)conn->bussiness_ctx;

        assert(slave_ctx != NULL);
        assert(slave_ctx->slave_idx >= 0);

        kvs_free(slave_ctx, sizeof(struct kvs_master_slave_context_s));
    } else {
        LOG_FATAL("%s:%d unknown ctx type: %d\n", __FILE__, __LINE__, ctx_header->type);
        assert(0);
        return KVS_ERR;
    }

    return KVS_OK;
}

kvs_status_t kvs_master_slave_sync_start(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    // 1. check if can accept new slave
    if(master->slave_count + master->syncing_slaves_count >= master->max_slave_count) {
        // slave limit reached
        LOG_WARN("slave limit reached");
        // todo : return more error info to slave
        return KVS_QUIT;
    }

    if(conn->bussiness_ctx == NULL) assert(0);
    struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME || slave_ctx->slave_idx != -1) {
        printf("%s:%d invalid slave state: %d\n", __FILE__, __LINE__, slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    
    int ret = 0;
    // 2. start RDB save if not in progress
    if(master->server->rdb_child_pid <= 0) {
        // no RDB save in progress, start a new one
        ret = kvs_server_save_rdb_fork(master->server);
        slave_ctx->state = CONN_STATE_SLAVE_WAIT_RDB;
        if(ret != KVS_OK) {
            printf("%s:%d kvs_server_save_rdb_fork failed\n", __FILE__, __LINE__);
            assert(0);
            return KVS_ERR;
        }
        
    }
    slave_ctx->state = CONN_STATE_SLAVE_WAIT_RDB;
    if(master->rdb_fd > 0) {
        close(master->rdb_fd);
    }
    master->rdb_fd = -1; // will be opened in sync tick

    return KVS_OK;
}

kvs_status_t kvs_master_slave_sync_tick(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    if(conn->bussiness_ctx == NULL) assert(0);
    struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    if(slave_ctx->state != CONN_STATE_SLAVE_WAIT_RDB && 
        slave_ctx->state != CONN_STATE_SLAVE_SEND_RDB &&
        slave_ctx->state != CONN_STATE_SLAVE_SEND_REPL) {
        printf("%s:%d invalid slave state: %d\n", __FILE__, __LINE__, slave_ctx->state);
        assert(0);
        return KVS_ERR;
    }
    
    kvs_master_slave_conn_state_t *slave_state = &slave_ctx->state;
    int pret = 0;
    size_t *rdb_offset = &slave_ctx->rdb_offset;
    size_t *repl_backlog_offset = &slave_ctx->repl_backlog_offset;
    switch(*slave_state){
        case CONN_STATE_SLAVE_WAIT_RDB:
            // check if RDB save is finished
            if(master->server->rdb_child_pid > 0) {
                // still saving RDB
                kvs_net_set_recv_event(conn);
                break;
            } else {
                // RDB save finished
                LOG_DEBUG("RDB save finished, start sending RDB to slave");
                master->syncing_slaves_count ++;
                *slave_state = CONN_STATE_SLAVE_SEND_RDB;
                *rdb_offset = 0;
            }
            // fall through
            // no break here
		case CONN_STATE_SLAVE_SEND_RDB:
            if(master->rdb_fd <= 0) {
                // the first slave to send RDB, open the rdb file
                master->rdb_fd = open("dump.rdb", O_RDONLY);
                if(master->rdb_fd < 0) {
                    LOG_FATAL("master->rdb_fd open failed");
                    assert(0);
                }
            }

			pret = pread(master->rdb_fd, conn->w_buffer, conn->w_buf_sz, *rdb_offset);
			if(pret < 0) {
				printf("%s:%d pread rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
				assert(0);
			} else if(pret == 0) {
				// finish sending rdb file
				LOG_DEBUG("finish sending rdb file to slave");
				// rdb file descriptor will be closed when next rdb save
				// close(conn->server->master.rdb_fd);
				// conn->server->master.rdb_fd = -1;
				*repl_backlog_offset = 0;
				goto SLAVE_SEND_REPL_LABEL;
			} else {
				*rdb_offset += pret;
				conn->w_idx = pret;
				kvs_net_set_send_event_manual(conn);
			}
			break;
		case CONN_STATE_SLAVE_SEND_REPL:
		{
SLAVE_SEND_REPL_LABEL:
			if(master->slave_count < 0) {
				LOG_FATAL("invalid master slave count: %d", master->slave_count);
				assert(0);
			}
			if(master->repl_backlog != NULL && master->repl_backlog_idx > 0) {
                int bytes_sent = conn->raw_buf_sent_sz;
				if(bytes_sent > 0 && slave_ctx->state == CONN_STATE_SLAVE_SEND_REPL) {
					*repl_backlog_offset += bytes_sent;
				}
				slave_ctx->state = CONN_STATE_SLAVE_SEND_REPL;
                LOG_DEBUG("[slave state SEND REPL] slave fd: %d, repl_backlog_offset: %zu\n", conn->_internal.fd, *repl_backlog_offset);
				if(*repl_backlog_offset < master->repl_backlog_idx) {
					size_t remaining = master->repl_backlog_idx - *repl_backlog_offset;
					kvs_net_set_send_event_raw_buffer(conn, master->repl_backlog + *repl_backlog_offset, remaining);
					break;
				} else {
					// finish sending replication backlog
					LOG_DEBUG("finish sending replication backlog to slave");
				}
			} else {
				// no replication backlog data
				LOG_DEBUG("no replication backlog data to send to slave\n");
			}

			slave_ctx->state = CONN_STATE_SLAVE_ONLINE;
            LOG_DEBUG("[slave state ONLINE] slave fd: %d\n", conn->_internal.fd);
			master->syncing_slaves_count --;
			assert(master->syncing_slaves_count >= 0);
			if(master->syncing_slaves_count == 0) {
				close(master->rdb_fd);
				master->rdb_fd = -1;
				master->repl_backlog_idx = 0;
			}

			assert(master->slave_count < master->max_slave_count);
            master->slave_conns[master->slave_count] = conn;
            slave_ctx->slave_idx = master->slave_count;
			master->slave_count ++ ;
			LOG_DEBUG("new slave [fd:%d] connected, total slave count: %d\n", conn->_internal.fd, master->slave_count);
			kvs_net_set_recv_event(conn);
		}
			break;
		case CONN_STATE_SLAVE_ONLINE:
            // normal online state
			kvs_net_set_recv_event(conn);
			break;
		default:
			LOG_FATAL("invalid connection state: %d\n", *slave_state);
			assert(0);
    }

    return KVS_OK;

}

/**
 * @brief Remove a slave connection from master, but not reset the connection
 */
kvs_status_t kvs_master_remove_slave(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    if(conn->bussiness_ctx == NULL) assert(0);
    struct kvs_master_slave_context_s* slave_ctx = (struct kvs_master_slave_context_s*)conn->bussiness_ctx;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    int slave_idx = slave_ctx->slave_idx;
    if(slave_idx < 0 || slave_idx >= master->slave_count) {
        LOG_FATAL("invalid slave idx: %d, slave_count: %d", slave_idx, master->slave_count);
        assert(0);
        return KVS_ERR;
    }

    LOG_DEBUG("remove slave idx: %d, fd: %d\n", slave_idx, master->slave_conns[slave_idx]->_internal.fd);
    // remove the slave connection from the array
    if(slave_idx < master->slave_count -1 ) {
        struct kvs_conn_s *last_slave_conn = master->slave_conns[master->slave_count - 1];
        master->slave_conns[master->slave_count - 1] = NULL;
        master->slave_conns[slave_idx] = last_slave_conn;
        struct kvs_master_slave_context_s* last_slave_ctx = (struct kvs_master_slave_context_s*)last_slave_conn->bussiness_ctx;
        if(last_slave_ctx == NULL || last_slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
            LOG_FATAL("invalid last slave ctx");
            assert(0);
            return KVS_ERR;
        }
        last_slave_ctx->slave_idx = slave_idx;
    } else if(slave_idx == master->slave_count -1 ) {
        master->slave_conns[slave_idx] = NULL;
    } else {
        LOG_FATAL("invalid slave idx: %d, slave_count: %d", slave_idx, master->slave_count);
        assert(0);
        return KVS_ERR;
    }
    master->slave_count --;
    return KVS_OK;
}
