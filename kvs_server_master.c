#include "kvs_conn.h"
#include "kvs_server.h"

#include "kvs_persistence.h"
#include "kvs_types.h"
#include "common.h"
#include "kvs_network.h"
#include "logger.h"
#include "kvs_rdma_engine.h"

#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>

#define KVS_MAX_SLAVES_DEFAULT 128
//#define RDMA_MAX_CHUNK_SIZE (16 * 1024 * 1024) // 16MB


kvs_status_t kvs_master_add_slave(struct kvs_master_s *master, struct kvs_conn_s *conn);
kvs_status_t _kvs_repl_slave_sending_rdb_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger);

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
    master->syncing_rdb_slaves_count = 0;
    master->syncing_slaves_count = 0;
    //master->repl_backlog = NULL;
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


    master->rdma_recv_buffer_count = config->rdma_recv_buffer_count;
    master->rdma_recv_buf_size = config->rdma_recv_buf_size;

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


static void _kvs_master_start_repl_buf_saving(struct kvs_master_s *master) {
    master->syncing_slaves_count ++;
    if(master->syncing_slaves_count == 1) {
        master->is_repl_backlog = 1;
        master->repl_backlog_idx = 0;
        master->repl_backlog_overflow = 0;
        LOG_DEBUG("Started repl backlog saving for first syncing slave");
    } else {
        LOG_DEBUG("syncing_slaves_count %d > 1, repl backlog already started", master->syncing_slaves_count);
    }
   
}

static void _kvs_master_stop_repl_buf_saving(struct kvs_master_s *master) {
    master->syncing_slaves_count --;
    if(master->syncing_slaves_count == 0) {
        master->is_repl_backlog = 0;
        master->repl_backlog_idx = 0;
        master->repl_backlog_overflow = 0;
        LOG_DEBUG("Stopped repl backlog saving, no more syncing slaves");
    } else {
        LOG_DEBUG("syncing_slaves_count %d > 0, repl backlog still needed", master->syncing_slaves_count);
    }
}

static kvs_status_t _on_slave_none(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_TRIGGER_MANUAL) {
        LOG_FATAL("invalid trigger for slave none handler: %d", trigger);
        return KVS_ERR;
    }
    // 1. 错误处理
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    
    // 2. 事件完成，处理事件
    if(master->slave_count >= master->max_slave_count) {
        // slave limit reached
        LOG_WARN("slave limit reached");
        return KVS_QUIT; // disconnect slave
    }

    master->slave_count ++; // current number of connected slaves, including syncing and online
    //master->syncing_slaves_count ++ ; // number of slaves in SYNC process
    _kvs_master_start_repl_buf_saving(master);

    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME || slave_ctx->slave_idx != -1) {
        LOG_FATAL("invalid slave state: %d", slave_ctx->header.type);
        return KVS_ERR;
    }

    LOG_DEBUG("Adding new slave, current syncing slave count: %d/%d", master->syncing_slaves_count, master->slave_count);

    
    // 3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_BGSAVE_END;
    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    LOG_INFO("Slave fd %d entered SYNC state, waiting for BGSAVE to complete", slave_conn->_internal.fd);


    // 4. 注册事件
    int ret = 0;
    if(master->syncing_slaves_count == 1) {
        if(master->server->rdb_child_pid <= 0 ) {
            // no RDB save in progress, start a new one
            ret = kvs_server_save_rdb_fork(master->server);
            if(ret != KVS_OK) {
                LOG_FATAL("kvs_server_save_rdb_fork failed");
                return KVS_ERR;
            }

            //_kvs_master_start_repl_buf_saving(master);
            LOG_DEBUG("Started new RDB save and repl backlog for first syncing slave");

            return KVS_OK;
        } else {
            LOG_DEBUG("RDB save already in progress, slave will wait for it to finish");
            return KVS_OK;
        }
    } else if(master->syncing_slaves_count > 1) {
        if(master->server->rdb_child_pid <= 0 ) {
            // 如果同步的从节点数量大于1，且RDB保存已经完成，直接开始传输RDB
            LOG_DEBUG("RDB already finished, notifying slave to start RDB transfer, current syncing_slaves_count: %d", master->syncing_slaves_count);
            return KVS_STATUS_CONTINUE; 
        } else if(master->server->rdb_child_pid > 0 ) {
            LOG_DEBUG("RDB save already in progress, slave will wait for it to finish, current syncing_slaves_count: %d", master->syncing_slaves_count);
            return KVS_OK;
        }
        return KVS_OK;
    } else {
        LOG_FATAL("syncing_slaves_count < 0, syncing_slaves_count: %d", master->syncing_slaves_count);
        return KVS_ERR;
    }
}


#if 0
kvs_status_t _kvs_repl_slave_wait_bgsave_end_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP) {
        LOG_FATAL("invalid conn type for slave wait bgsave end handler: %d", conn->type);
        assert(0);
        return KVS_ERR;
    }
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
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
    
    if(master->rdb_fd > 0 && master->syncing_rdb_slaves_count == 0) {
        close(master->rdb_fd);
    }


    //LOG_INFO("RDB save finished, start sending RDB to slave fd %d", conn->_internal.fd);
    // 动作：切换状态，开始发文件
    slave_ctx->state = KVS_MY_SLAVE_WAIT_SENDING_RDB;
    return KVS_STATUS_CONTINUE; // trigger sending immediately
}
kvs_status_t _kvs_repl_slave_sending_rdb_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(conn->type != KVS_CONN_TCP) {
        LOG_FATAL("invalid conn type for slave sending rdb handler: %d", conn->type);
        assert(0);
        return KVS_ERR;
    }
    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    /************io uring发送 *******/
    if(slave_conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    size_t *rdb_offset = &slave_ctx->rdb_offset;
    assert(slave_conn->s_idx == 0);

    _kvs_master_slave_sync_begin(master);

    if(*rdb_offset == 0) {
        // first time sending, send rdb size first
        int head_size = snprintf(slave_conn->s_buffer, slave_conn->s_buf_sz, "$%ld\r\n", master->rdb_size);
        slave_conn->s_idx += head_size;
    }


    int pret = pread(master->rdb_fd, slave_conn->s_buffer + slave_conn->s_idx, slave_conn->s_buf_sz - slave_conn->s_idx, *rdb_offset);
    if(pret < 0) {
        printf("%s:%d pread rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
        assert(0);
    } else if(pret == 0) {
        // finish sending rdb file
        LOG_DEBUG("finish sending rdb file to slave");
        // rdb file descriptor will be closed when next rdb save
        // close(conn->server->master.rdb_fd);
        // conn->server->master.rdb_fd = -1;
        slave_ctx->state = KVS_MY_SLAVE_SENDING_BACKLOG;
        *rdb_offset = 0;
        assert(slave_conn->s_idx == 0);
        return KVS_STATUS_CONTINUE; // continue to send backlog
    } else {
        *rdb_offset += pret;
        LOG_DEBUG("rdb send size: %d", pret);
        slave_conn->s_idx += pret;
        kvs_net_set_send_event_manual(slave_conn);
    }
    /************** rdma发送 **************/
    return KVS_OK;
}
#endif

static kvs_status_t _kvs_master_open_rdb(struct kvs_master_s *master) {
    if(master == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(master->rdb_fd > 0) {
        LOG_FATAL("rdb_fd already opened: %d", master->rdb_fd);
    }
    master->rdb_fd = open(master->server->pers_ctx->rdb_filename, O_RDWR);
    struct stat rdb_st;
    fstat(master->rdb_fd, &rdb_st);
    master->rdb_size = rdb_st.st_size;

    return KVS_OK;
}

static kvs_status_t _kvs_master_close_rdb(struct kvs_master_s *master) {
    if(master == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(master->rdb_fd > 0) {
        close(master->rdb_fd);
        master->rdb_fd = -1;
    }
    return KVS_OK;
}


static kvs_status_t _on_rdb_sent_begin(struct kvs_master_s *master, struct kvs_rdma_conn_s *conn) {
    master->syncing_rdb_slaves_count += 1;
    if(master->syncing_rdb_slaves_count == 1) {
        _kvs_master_open_rdb(master);
        if(master->server->use_rdma) {
            master->rdb_mmap = mmap(NULL, master->rdb_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, master->rdb_fd, 0);
            if(master->rdb_mmap == MAP_FAILED) {
                LOG_FATAL("mmap RDB file failed: %s", strerror(errno));
                return KVS_ERR;
            }
            madvise(master->rdb_mmap, master->rdb_size, MADV_SEQUENTIAL);
            //struct ibv_mr *mr = kvs_rdma_register_memory(&master->server->network.rdma_engine, master->rdb_mmap, master->rdb_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
            struct kvs_rdma_mr_s *mr = kvs_rdma_register_memory_on_conn(conn, master->rdb_mmap, 
                    master->rdb_size, KVS_RDMA_OP_SEND);
            LOG_DEBUG("Registered RDMA memory for RDB mmap, addr: %p, size: %zu", master->rdb_mmap, master->rdb_size);
            if(mr == NULL) {
                LOG_FATAL("kvs_rdma_register_memory failed");
                return KVS_ERR;
            }
            master->rdb_mr = mr;
        }
    } else {
        LOG_DEBUG("RDB already mmaped for other syncing slaves, current syncing_rdb_slaves_count: %d", master->syncing_rdb_slaves_count);
    }

    return KVS_OK;
}

static kvs_status_t _on_rdb_sent_end(struct kvs_master_s *master) {
    master->syncing_rdb_slaves_count -= 1;
    if(master->syncing_rdb_slaves_count < 0) {
        LOG_FATAL("syncing_rdb_slaves_count < 0");
        return KVS_ERR;
    } else if(master->syncing_rdb_slaves_count == 0) {
        // all syncing slaves finished, close rdb fd
        if(master->server->use_rdma && master->rdb_mmap != NULL) {
            LOG_DEBUG("Deregistering RDMA memory for RDB mmap");
            kvs_rdma_deregister_memory(master->rdb_mr);
            master->rdb_mr = NULL;
            munmap(master->rdb_mmap, master->rdb_size);
            master->rdb_mmap = NULL;

            _kvs_master_close_rdb(master);
        }
    } else {
        LOG_DEBUG("There are still syncing slaves, current syncing_rdb_slaves_count: %d", master->syncing_rdb_slaves_count);
    }

    return KVS_OK;
}

static kvs_status_t _on_bgsave_end(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_BGSAVE_DONE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    if(master->server->rdb_child_pid > 0  ) {
        // slave到这里后，然后client发送save命令，导致rdb_child_pid被重新设置，但是并没有影响slave的rdb传输
        LOG_WARN("RDB save still in progress");
        //return KVS_ERR; // still saving
    }


    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    
    //2 . 事件完成，处理事件
    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    LOG_DEBUG("RDB save finished, start sending RDB to slave fd %d", slave_conn->_internal.fd);

    

    //3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_SENT_RDMA_INFO;
    LOG_INFO("Slave fd %d BGSAVE completed, wait for RDMA info sent", slave_conn->_internal.fd);


    //4. 注册事件
    int fd = open(master->server->pers_ctx->rdb_filename, O_RDONLY);
    struct stat rdb_st;
    fstat(fd, &rdb_st);
    size_t size = rdb_st.st_size;
    master->rdb_size = size;
    close(fd);
    

    int port = master->server->rdma_engine.rdma_port;
    slave_ctx->rdma_token = kvs_session_register(&master->session_table, (void*)slave_ctx); // register session in
    LOG_DEBUG("Sending RDMA FULLRESYNC to slave: port=%d, token=%zu, rdb_size=%zu", 
        port, slave_ctx->rdma_token, size);

    struct kvs_conn_s *conn_tcp = (struct kvs_conn_s *)slave_ctx->header.conn;
    // +FULLRESYNC <rdma_port> <token> <rdb_size>\r\n
    int len = snprintf(conn_tcp->s_buffer, conn_tcp->s_buf_sz, "+FULLRESYNC %d %zu %zu\r\n", port, slave_ctx->rdma_token, size);
    conn_tcp->s_idx += len;

    kvs_net_set_send_event_manual(conn_tcp);
    
    return KVS_OK;
}

static kvs_status_t _on_rdma_info_sent(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_WRITE_DONE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    // 1. 错误处理
   if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    
    //2 . 事件完成，处理事件
    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    LOG_DEBUG("RDMA info sent to slave fd %d, now wait for RDMA connection", slave_conn->_internal.fd);

    //3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_RDMA_CONNECT_REQUEST;

    //4. 注册事件
    return KVS_OK; // 等待slave主动连接RDMA
}


kvs_status_t _kvs_master_alloc_rdma_resources_for_slave(struct kvs_master_s *master, struct kvs_my_slave_context_s *slave_ctx) {
    if(master == NULL || slave_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    LOG_DEBUG("_kvs_master_alloc_rdma_resources_for_slave called for slave idx %d", slave_ctx->slave_idx);
    // allocate RDMA resources for slave
    slave_ctx->rdb_size = master->rdb_size;
    slave_ctx->recv_buf = (char *)kvs_malloc(master->rdma_recv_buf_size * master->rdma_recv_buffer_count);
    memset(slave_ctx->recv_buf, 0, master->rdma_recv_buf_size * master->rdma_recv_buffer_count);
    slave_ctx->recv_buf_sz = master->rdma_recv_buf_size * master->rdma_recv_buffer_count;


    slave_ctx->recv_mr = kvs_rdma_register_memory_on_conn((struct kvs_rdma_conn_s *)slave_ctx->rdma_conn, 
        slave_ctx->recv_buf, master->rdma_recv_buf_size * master->rdma_recv_buffer_count, KVS_RDMA_OP_RECV);

    if(slave_ctx->recv_mr == NULL) {
        LOG_FATAL("kvs_rdma_register_memory failed for slave idx %d", slave_ctx->slave_idx);
        return KVS_ERR;
    }
    return KVS_OK;
}

kvs_status_t _kvs_master_free_rdma_resources_for_slave(struct kvs_master_s *master, struct kvs_my_slave_context_s *slave_ctx) {
    if(master == NULL || slave_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    LOG_DEBUG("_kvs_master_free_rdma_resources_for_slave called for slave idx %d", slave_ctx->slave_idx);
    // free RDMA resources for slave
    if(slave_ctx->recv_mr != NULL) {
        kvs_rdma_deregister_memory(slave_ctx->recv_mr);
        slave_ctx->recv_mr = NULL;
    }
    if(slave_ctx->recv_buf != NULL) {
        kvs_free(slave_ctx->recv_buf, master->rdma_recv_buf_size * master->rdma_recv_buffer_count);
        slave_ctx->recv_buf = NULL;
        slave_ctx->recv_buf_sz = 0;
    }
    return KVS_OK;
}

static kvs_status_t _on_rdma_connect_request(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_CONNECTED) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }

    if(conn->type != KVS_CONN_RDMA || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    if(conn->user_data != NULL) {
        LOG_FATAL("conn user_data already set in rdma connect request");
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件, 收到slave的rdma连接请求 
    struct kvs_rdma_conn_s *rdma_conn = (struct kvs_rdma_conn_s *)conn;
    if(rdma_conn->header.user_data != NULL) {
        LOG_FATAL("rdma_conn user_data already set in rdma connect request handler");
        return KVS_ERR;
    }
    if(rdma_conn->private_data_len != sizeof(uint64_t)) {
        LOG_FATAL("invalid rdma private_data_len: %zu", rdma_conn->private_data_len);
        return KVS_ERR;
    }
    uint64_t *received_token = (uint64_t*)rdma_conn->private_data;
    LOG_DEBUG("Received token from slave: %zu", *received_token);
    struct kvs_my_slave_context_s *matched_ctx = kvs_session_match(&master->session_table, *received_token); // remove token from session table
    if(matched_ctx == NULL) {
        LOG_FATAL("no matching session for token: %lu", *received_token);
        return KVS_QUIT; // rdma_reject connection
    }

    kvs_server_share_conn_ctx(master->server, (struct kvs_conn_header_s *)matched_ctx->header.conn, (struct kvs_conn_header_s *)rdma_conn);
    
    matched_ctx->rdma_conn = rdma_conn;

    // 3. 状态转换
    matched_ctx->state = KVS_MY_SLAVE_WAIT_RDMA_ESTABLISHED;
    LOG_DEBUG("RDMA connection established for slave idx %d, wait for RDMA established event", matched_ctx->slave_idx);

    _on_rdb_sent_begin(master, (struct kvs_rdma_conn_s *)conn); // called when starting to send rdb to a slave
    _kvs_master_alloc_rdma_resources_for_slave(master, matched_ctx);


    for(int i = 0; i < master->rdma_recv_buffer_count; i++) {
        kvs_rdma_post_recv(rdma_conn, matched_ctx->recv_mr, i * master->rdma_recv_buf_size, master->rdma_recv_buf_size, NULL);
    }
    //kvs_rdma_post_recv(rdma_conn, matched_ctx->recv_mr, 0, matched_ctx->recv_buf_sz, NULL);
    // 4. 注册事件 返回0后rdma engine中注册了accept事件
    return KVS_OK; // rdma_accept connection
}


static kvs_status_t _on_rdma_established(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    

    //3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_RDMA_RECV_READY;

    //4. 注册事件
    LOG_DEBUG("RDMA established from slave idx %d", slave_ctx->slave_idx);

    return KVS_OK;
}

static kvs_status_t _on_rdma_recv_ready(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        return KVS_ERR;
    }

    //struct kvs_rdma_conn_s *rdma_conn = (struct kvs_rdma_conn_s *)conn;
    //READY RDB_RECV_BUF_NUM:%d\r\n
    
    int recv_len = slave_ctx->recv_size_cur;
    int head_len = sizeof("+READY") - 1;
    LOG_DEBUG("RDMA RECV READY message from slave idx %d, msg:[%.*s], msg_len:%d, msg_num:[%.*s]", 
        slave_ctx->slave_idx, recv_len, slave_ctx->recv_buf, recv_len, 1, slave_ctx->recv_buf + head_len);
    
    if(memcmp(slave_ctx->recv_buf + slave_ctx->buf_offset_cur, "+READY", head_len) != 0) {
        LOG_DEBUG("Invalid RDMA RECV READY message from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_ctx->recv_buf + slave_ctx->buf_offset_cur);
        assert(0);
        return KVS_ERR;
    }
    int rdb_recv_buf_num = kvs_parse_int(slave_ctx->recv_buf + slave_ctx->buf_offset_cur, recv_len, &head_len);
    slave_ctx->slave_recv_buf_count = rdb_recv_buf_num;
    LOG_DEBUG("Received RDMA RECV READY from slave idx %d, rdb_recv_buf_num: %d", slave_ctx->slave_idx, rdb_recv_buf_num);
    // if(slave_ctx->recv_buf[0] == 'R') {
    //     // RDMA RECV READY received from slave
    //     LOG_DEBUG("Received RDMA RECV READY from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_ctx->recv_buf);
    // } else {
    //     LOG_DEBUG("Invalid RDMA RECV READY message from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_ctx->recv_buf);
    //     assert(0);
    //     return KVS_ERR;
    // }

    // 接受完之后继续接受 循环利用recv buf;
    int off_set = slave_ctx->buf_offset_cur;
    kvs_rdma_post_recv((struct kvs_rdma_conn_s *)conn, slave_ctx->recv_mr, off_set, master->rdma_recv_buf_size, NULL);
    memset(slave_ctx->recv_buf + off_set, 0, master->rdma_recv_buf_size);

    //3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_RDB_SENT;
    LOG_DEBUG("RDMA recv ready from slave idx %d, continuing to send rdb", slave_ctx->slave_idx);

    //4. 注册事件 本次tick延续到下一事件 //
   
    return KVS_STATUS_CONTINUE; // continue to send rdb
}


// rdma_send rdb handler
static kvs_status_t _on_rdb_sent(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    
    // 1. 错误处理
    // if(trigger != KVS_EVENT_CONTINUE && trigger != KVS_EVENT_WRITE_DONE) {
    //     LOG_FATAL("invalid trigger: %d", trigger);
    //     return KVS_ERR;
    // }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件
 
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    size_t *rdb_offset = &slave_ctx->rdb_offset;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    if(trigger == KVS_EVENT_WRITE_DONE) {
        int sent_size = slave_ctx->sent_size_cur;
        *rdb_offset += sent_size;
        LOG_DEBUG("RDMA send completed for slave idx %d, sent size: %d, total sent: %zu/%zu", 
            slave_ctx->slave_idx, sent_size, *rdb_offset, master->rdb_size);

        slave_ctx->rdb_is_sending = 0;
        
        //return KVS_OK;
    } else if(trigger == KVS_EVENT_CONTINUE) {
        //assert(*rdb_offset == 0); // 客户端回应准备好了recv buf也会触发CONTINUE
        LOG_DEBUG("Starting RDMA send for slave idx %d", slave_ctx->slave_idx);
    }



    slave_ctx->master = master; // todo: set back reference to master ？？ should not be set here

    // 3. 状态转换

    // 4. 注册事件


    struct kvs_rdma_conn_s *slave_conn = (struct kvs_rdma_conn_s *)conn;

    if(trigger == KVS_EVENT_READ_READY) {
        //slave_ctx->recv_buf[slave_ctx->recv_size_cur] = '\0';
        if(memcmp(slave_ctx->recv_buf + slave_ctx->buf_offset_cur, "RECV_ACK\r\n", sizeof("RECV_ACK\r\n") - 1) != 0) {
            LOG_FATAL("Invalid RDMA RECV BUF READY message from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_ctx->recv_buf + slave_ctx->buf_offset_cur);
            return KVS_ERR;
        }
        memset(slave_ctx->recv_buf + slave_ctx->buf_offset_cur, 0, master->rdma_recv_buf_size);
        slave_ctx->slave_recv_buf_count ++;
        LOG_DEBUG("Received RDMA RECV BUF READY from slave idx %d, available recv buf count: %d", 
            slave_ctx->slave_idx, slave_ctx->slave_recv_buf_count);
        
        int offset = slave_ctx->buf_offset_cur;
        kvs_rdma_post_recv(slave_conn, slave_ctx->recv_mr, offset, master->rdma_recv_buf_size, NULL); // 循环利用recv buf
        //return KVS_STATUS_CONTINUE; // continue to send rdb
        
        
        // if(slave_ctx->slave_recv_buf_count > 1){
        //     return KVS_OK; ///此时master还没有停止发送rdb，故不需要继续执行下面的发送逻辑了，否则会重复发送
        // } else if(slave_ctx->slave_recv_buf_count == 1){
        //     // 在触发这个读完成事件之前，slave_ctx->slave_recv_buf_count==0，说明master已经停止发送rdb了，故需要继续发送rdb
        //     LOG_DEBUG("Slave idx %d has available recv buffer now, continue sending RDB", slave_ctx->slave_idx);
        // }
    }

    assert(*rdb_offset <= master->rdb_size);
    // 发完了，结束，并准备接受ack
    if(*rdb_offset == master->rdb_size) {
        // finish sending rdb file
        LOG_DEBUG("finish sending rdb file to slave via RDMA");
        _on_rdb_sent_end(master); // called when finishing sending rdb to a slave
        // 3. 状态转换
        slave_ctx->state = KVS_MY_SLAVE_WAIT_RDB_ACK;
        // 4. 注册事件 recv已经post好了
        //kvs_rdma_post_recv(slave_conn, slave_ctx->recv_mr, 0, slave_ctx->recv_buf_sz, NULL);
        return KVS_OK;
    } 

    // 还有数据没发完，但是slave没有准备好接收
    if(slave_ctx->slave_recv_buf_count <= 0) {
        LOG_DEBUG("No available RDMA recv buffer on slave idx %d, waiting for slave to post more recv buffers", slave_ctx->slave_idx);
        //slave_ctx->state = KVS_MY_SLAVE_WAIT_RDB_BUF_RECV_READY;

        
        return KVS_OK; // wait for slave to post more recv buffers
    }
    
    
    
    size_t rdb_max_chunk_size = master->server->rdma_max_chunk_size;
    if(!slave_ctx->rdb_is_sending) {
        slave_ctx->rdb_is_sending = 1;
        if(*rdb_offset + rdb_max_chunk_size < master->rdb_size) {
            // 3. 状态转换 (保持不变)
            // 4. 注册事件
            kvs_rdma_post_send(slave_conn, master->rdb_mr, 0, *rdb_offset, rdb_max_chunk_size, NULL);

            slave_ctx->slave_recv_buf_count --;
            LOG_DEBUG("Posted RDMA send for RDB chunk, offset: %zu, size: %d", *rdb_offset, rdb_max_chunk_size);
        } else{
            // 3. 状态转换 (保持不变)
            // 4. 注册事件
            kvs_rdma_post_send(slave_conn, master->rdb_mr, -1, // imm_data = -1 indicates last chunk
                *rdb_offset, master->rdb_size - *rdb_offset, NULL);
            LOG_DEBUG("Posted RDMA send for RDB chunk, offset: %zu, size: %zu, imm_data: -1", *rdb_offset, master->rdb_size - *rdb_offset);
            //slave_ctx->rdb_sent_done = 1;
        }
    } else {

    }
    
    return KVS_OK; // wait for RDMA send completion
}


// static kvs_status_t _on_slave_rdb_recv_buf_ready(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
//     // 1. 错误处理
//     if(trigger != KVS_EVENT_READ_READY) {
//         LOG_FATAL("invalid trigger: %d", trigger);
//         return KVS_ERR;
//     }
//     if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || master == NULL) {
//         LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data== NULL, master == NULL);
//         return KVS_ERR;
//     }
//     // 2. 事件完成，处理事件
//     struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
//     if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
//         LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
//         return KVS_ERR;
//     }

    
//     // 3. 状态转换
    

//     // 4. 注册事件
//     return KVS_OK;
// }

kvs_status_t _on_rdb_ack(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        return KVS_ERR;
    }

    int off_set = slave_ctx->buf_offset_cur;
    if(memcmp(slave_ctx->recv_buf + off_set, "RECV_ACK\r\n", sizeof("RECV_ACK\r\n") - 1) == 0) {
        LOG_DEBUG("Invalid RDMA RECV BUF READY message from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_ctx->recv_buf + slave_ctx->buf_offset_cur);
        kvs_rdma_post_recv((struct kvs_rdma_conn_s *)conn, slave_ctx->recv_mr, off_set, master->rdma_recv_buf_size, NULL);
        memset(slave_ctx->recv_buf + off_set, 0, master->rdma_recv_buf_size);

        return KVS_OK; // 上个recv buf ack消息可能被重复触发多次，所以这里还会接到RECV_ACK消息，不算错误，而rdma是顺序发送的，所以这里直接返回OK继续等待RDB ACK消息
    }

    
    if(memcmp(slave_ctx->recv_buf + off_set, "ACK_RDB\r\n", sizeof("ACK_RDB\r\n") - 1) == 0) {
        // RDB ACK received from slave
        LOG_DEBUG("Received RDB ACK from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_ctx->recv_buf + off_set);
    } else {
        LOG_FATAL("Invalid RDB ACK message from slave idx %d, msg:[%s], off_set: %d", slave_ctx->slave_idx, slave_ctx->recv_buf + off_set,  off_set);
        return KVS_ERR;
    }

    // 接受完之后继续接受 循环利用recv buf; 虽然这里好像不需要继续接受了，但是为了保持状态机的一致性，还是继续post recv
    kvs_rdma_post_recv((struct kvs_rdma_conn_s *)conn, slave_ctx->recv_mr, off_set, master->rdma_recv_buf_size, NULL);
    memset(slave_ctx->recv_buf + off_set, 0, master->rdma_recv_buf_size);


    //3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_SLAVE_LOAD_ACK;

    LOG_DEBUG("RDB ACK from slave idx %d, continuing to send backlog", slave_ctx->slave_idx);
    //4. 注册事件 本次tick延续到下一事件
    struct kvs_conn_s *tcp_conn = (struct kvs_conn_s *)slave_ctx->header.conn;
    kvs_net_set_recv_event(tcp_conn);

    return KVS_OK; // continue to send backlog
}

kvs_status_t _on_slave_rdb_loaded_ack(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        return KVS_ERR;
    }

    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    if(strncmp(slave_conn->r_buffer, "+RDBLOADED\r\n", sizeof("+RDBLOADED\r\n") - 1) != 0) {
        LOG_FATAL("Invalid RDB LOADED ACK message from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_conn->r_buffer);
        return KVS_ERR;
    }
    slave_ctx->processed_sz_cur = sizeof("+RDBLOADED\r\n") - 1;
    LOG_DEBUG("Received RDB LOADED ACK from slave idx %d, msg:[%s]", slave_ctx->slave_idx, slave_conn->r_buffer);

    _kvs_master_free_rdma_resources_for_slave(master, slave_ctx);
    // rdb事件已经结束，关闭RDMA连接
    if(slave_ctx->rdma_conn) {
        LOG_DEBUG("Closing RDMA sync channel for Slave %d", slave_ctx->slave_idx);
        rdma_disconnect(slave_ctx->rdma_conn->cm_id);
    }

    //3. 状态转换
    slave_ctx->state = KVS_MY_SLAVE_WAIT_BACKLOG_SENT;

    LOG_DEBUG("Slave idx %d loaded RDB, continuing to send backlog", slave_ctx->slave_idx);
    //4. 注册事件 本次tick延续到下一事件
     
    return KVS_STATUS_CONTINUE; // continue to send backlog
}


kvs_status_t _on_backlog_sent(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_WRITE_DONE && trigger != KVS_EVENT_CONTINUE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || master == NULL) {
        LOG_FATAL("invalid conn type: %d or conn user_data == NULL is %d or master == NULL is %d", conn->type, conn->user_data == NULL, master == NULL);
        return KVS_ERR;
    }

    // 2. 事件完成，处理事件
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        return KVS_ERR;
    }

    struct kvs_conn_s *tcp_conn = (struct kvs_conn_s *)slave_ctx->header.conn;
    if(tcp_conn == NULL) {
        LOG_FATAL("tcp_conn is NULL in backlog sent handler for slave idx %d", slave_ctx->slave_idx);
        return KVS_ERR;
    }

    if(trigger == KVS_EVENT_WRITE_DONE) {
        //int sent_size = tcp_conn->s_idx;
        int sent_size = slave_ctx->sent_size_cur;
        slave_ctx->repl_backlog_offset += sent_size;
        LOG_DEBUG("Backlog send completed for slave idx %d, sent size: %d, total sent: %zu/%zu", 
            slave_ctx->slave_idx, sent_size, slave_ctx->repl_backlog_offset, master->repl_backlog_idx);
        tcp_conn->s_idx = 0; // reset send index
    } else if(trigger == KVS_EVENT_CONTINUE) {
        assert(slave_ctx->repl_backlog_offset == 0);
        LOG_DEBUG("Starting backlog send for slave idx %d", slave_ctx->slave_idx);
    }
    
    size_t *repl_backlog_offset = &slave_ctx->repl_backlog_offset;
    assert(tcp_conn->s_idx == 0);
    int to_send = master->repl_backlog_idx - *repl_backlog_offset;
    
    if(to_send > tcp_conn->s_buf_sz) {
        to_send = tcp_conn->s_buf_sz;
    }
    
    if(to_send == 0) {
        // finish sending backlog
        LOG_DEBUG("finish sending backlog to slave");
        _kvs_master_stop_repl_buf_saving(master); // stop saving backlog if all slaves are online
        kvs_master_add_slave(master, tcp_conn);

        // 3. 状态转换
        slave_ctx->state = KVS_MY_SLAVE_ONLINE;

       
        *repl_backlog_offset = 0;
        assert(tcp_conn->s_idx == 0);
        // 4. 注册事件 recv接收客户端命令
        kvs_net_set_recv_event(tcp_conn); 
        return KVS_OK; // done
    } else {
        memcpy(tcp_conn->s_buffer, master->repl_backlog + *repl_backlog_offset, to_send);
        *repl_backlog_offset += to_send;
        tcp_conn->s_idx = to_send;
        // 3. 状态转换 (保持不变)
        // 4. 注册事件
        kvs_net_set_send_event_manual(tcp_conn);
        return KVS_OK;
    }
}

kvs_status_t _on_slave_online(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(trigger == KVS_EVENT_WRITE_DONE) {
        // replication write done event in online state, ignore
        return KVS_OK; 
    }
    //LOG_FATAL("Slave is now ONLINE, but should not trigger any event for now");
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP) {
        LOG_FATAL("invalid conn type for slave online handler: %d", conn->type);
        assert(0);
        return KVS_ERR;
    }
    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    //LOG_DEBUG("Slave idx %d is now ONLINE", slave_ctx->slave_idx);

    //getchar(); // debug pause
    assert(slave_conn->s_idx == 0);


    assert(slave_ctx->slave_idx != -1);
    
    // now in online state, do nothing
    return KVS_OK;
}

typedef kvs_status_t (*slave_state_handler_t)(struct kvs_master_s *master, struct kvs_conn_header_s *slave_conn, kvs_event_trigger_t trigger);


// typedef enum {
//     KVS_MY_SLAVE_NONE = 0,
//     KVS_MY_SLAVE_WAIT_BGSAVE_END, // 等待后台 RDB 进程结束
//     KVS_MY_SLAVE_WAIT_SENT_RDMA_INFO, // 等待发送 RDMA 连接信息
//     KVS_MY_SLAVE_WAIT_RDMA_CONNECT_REQUEST, // 等待 Slave 发起 RDMA 连接请求
//     KVS_MY_SLAVE_WAIT_RDMA_ESTABLISHED, // 等待 RDMA 连接建立完成
//     KVS_MY_SLAVE_WAIT_RDMA_RECV_READY, // 等待 Slave 准备好接收 RDB
//     KVS_MY_SLAVE_WAIT_RDB_SENT,     // 正在发送 RDB 文件流
//     KVS_MY_SLAVE_WAIT_RDB_ACK,    // 等待 Slave RDB 接收完成确认
//     KVS_MY_SLAVE_WAIT_BACKLOG_SENT, // 正在发送 Backlog 积压数据
//     KVS_MY_SLAVE_ONLINE,           // 实时同步状态
//     KVS_MY_SLAVE_OFFLINE,          // 离线状态
//     KVS_MY_SLAVE_STATE_NUM
// } kvs_my_slave_state_t;

slave_state_handler_t slave_state_handlers[] = {
    [KVS_MY_SLAVE_NONE] = _on_slave_none,
    [KVS_MY_SLAVE_WAIT_BGSAVE_END] =  _on_bgsave_end, //_kvs_repl_slave_wait_bgsave_end_handler,
    [KVS_MY_SLAVE_WAIT_SENT_RDMA_INFO] = _on_rdma_info_sent,
    [KVS_MY_SLAVE_WAIT_RDMA_CONNECT_REQUEST] = _on_rdma_connect_request,
    [KVS_MY_SLAVE_WAIT_RDMA_ESTABLISHED] = _on_rdma_established,
    [KVS_MY_SLAVE_WAIT_RDMA_RECV_READY] = _on_rdma_recv_ready,
    [KVS_MY_SLAVE_WAIT_RDB_SENT] = _on_rdb_sent, // _kvs_repl_slave_sending_rdb_handler,
    //[KVS_MY_SLAVE_WAIT_RDB_BUF_RECV_READY] = _on_slave_rdb_recv_buf_ready,
    [KVS_MY_SLAVE_WAIT_RDB_ACK] = _on_rdb_ack,
    [KVS_MY_SLAVE_WAIT_SLAVE_LOAD_ACK] = _on_slave_rdb_loaded_ack,
    [KVS_MY_SLAVE_WAIT_BACKLOG_SENT] = _on_backlog_sent,
    [KVS_MY_SLAVE_ONLINE] = _on_slave_online,
    [KVS_MY_SLAVE_OFFLINE] = NULL
};

/*
* WAIT_BGSAVE, ON_BGSAVE_END
* WAIT_RDMA_CONNECT_REQUEST ,ON_RDMA_CONNECT_REQUEST
* WAIT_RDMA_ESTABLISHED, ON_RDMA_ESTABLISHED
* WAIT_RDMA_RECV_SLAVE_READY, ON_RDMA_RECV_SLAVE_READY
* WAIT_RDMA_SEND, ON_RDB_SEND_DONE
* WAIT_RDMA_RDB_ACK, ON_RDMA_RDB_ACK
* WAIT_BACKLOG_SEND, ON_BACKLOG_SEND_DONE
* ONLINE, OFFLINE
*/

kvs_status_t kvs_master_slave_state_machine_tick(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(master == NULL || conn == NULL) {
        LOG_FATAL("master or conn is NULL");
        return KVS_ERR;
    }
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    // if(slave_ctx == NULL && trigger != KVS_EVENT_RDMA_ESTABLISHED) {
    //     LOG_FATAL("slave_ctx is NULL");
    //     assert(0);
    //     return KVS_ERR;
    // }
    // if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
    //     LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
    //     assert(0);
    //     return KVS_ERR;
    // }

    kvs_status_t ret = KVS_STATUS_CONTINUE;

    if(trigger == KVS_EVENT_CONNECTED) {
        // special case for RDMA connection establishment
        if(conn->type == KVS_CONN_RDMA) {
            return slave_state_handlers[KVS_MY_SLAVE_WAIT_RDMA_CONNECT_REQUEST](master, conn, trigger);
        } else {
            LOG_FATAL("invalid conn type for CONNECTED event: %d", conn->type);
            return KVS_ERR;
        }
    }

    while(ret == KVS_STATUS_CONTINUE) {
        assert(slave_ctx != NULL );
        LOG_DEBUG("slave state machine tick: state=%d, trigger=%d", slave_ctx->state, trigger);
        kvs_my_slave_state_t state = slave_ctx->state;
        if(state < KVS_MY_SLAVE_NONE || state >= KVS_MY_SLAVE_STATE_NUM) {
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
        
    
        ret = handler(master, conn, trigger); // ignore return value for now
        if(ret == KVS_ERR) {
            // 状态回退
            LOG_FATAL("slave state handler for state %d failed", state);
            assert(0);
            return KVS_ERR;
        } else if(ret == KVS_QUIT) {
            // 断开连接
            LOG_INFO("slave state handler for state %d requested to quit", state);
            return -1; // indicate to quit connection
        } else if(ret == KVS_STATUS_CONTINUE) {
            // 继续下一个状态处理
            LOG_DEBUG("slave state handler for state %d returned CONTINUE", state);
            trigger = KVS_EVENT_CONTINUE; // set trigger to CONTINUE for next state
        } else {
            // 正常返回
            //LOG_DEBUG("slave state handler for state %d completed", state);
            return KVS_OK;
        }
    };
   

    return KVS_OK;
}



kvs_status_t kvs_master_add_slave(struct kvs_master_s *master, struct kvs_conn_s *conn) {
    if(master == NULL || conn == NULL) {
        return KVS_ERR;
    }
    if(conn->header.user_data == NULL) {
        LOG_FATAL("conn user_data is NULL");
        assert(0);
        return KVS_ERR;
    }
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
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
    if(conn->header.user_data == NULL) {
        LOG_FATAL("conn user_data is NULL");
        assert(0);
        return KVS_ERR;
    }
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    int slave_idx = slave_ctx->slave_idx;
    if(slave_idx == -1) {
        LOG_WARN("slave idx is -1, slave not in master's slave list");
        return KVS_OK; // not in the list
    }
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
        struct kvs_my_slave_context_s* last_slave_ctx = (struct kvs_my_slave_context_s*)last_slave_conn->header.user_data;
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
            if(slave_conn->s_idx + cmd->raw_len > slave_conn->s_buf_sz) {
                LOG_WARN("slave conn fd %d send buffer full, drop command", slave_conn->_internal.fd);
                continue;
            }
            memcpy(slave_conn->s_buffer + slave_conn->s_idx, cmd->raw_ptr, cmd->raw_len);
            slave_conn->s_idx += cmd->raw_len;
            kvs_net_set_send_event_manual(slave_conn);
        }
    }
    return KVS_OK;
}

kvs_status_t kvs_my_slave_on_recv(struct kvs_conn_s *conn, int *read_size) {
    // currently no need to handle anything from slave
    //assert(0); // should not receive anything from slave
    kvs_master_slave_state_machine_tick(conn->server_ctx->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_READ_READY);
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;

    *read_size = slave_ctx->processed_sz_cur;
    return KVS_OK;
}

kvs_status_t kvs_my_slave_on_send(struct kvs_conn_s *conn, int bytes_sent) {
    struct kvs_server_s *server = conn->server_ctx;


    //LOG_DEBUG("Master sent %d bytes to Slave fd %d", bytes_sent, conn->_internal.fd);
    return kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_WRITE_DONE);
}

void kvs_my_slave_on_close(struct kvs_conn_s *conn) {
    struct kvs_server_s *server = conn->server_ctx;
    struct kvs_master_s *master = server->master;
    if(master == NULL || conn == NULL) {
        return;
    }
    if(conn->header.user_data == NULL) {
        LOG_FATAL("conn user_data is NULL");
        return;
    }
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        return;
    }

    if(slave_ctx->state == KVS_MY_SLAVE_ONLINE) {
        // remove from online list
        kvs_master_remove_slave(master, conn);
    }

    if(slave_ctx->state != KVS_MY_SLAVE_OFFLINE) {
        slave_ctx->state = KVS_MY_SLAVE_OFFLINE;
    //  assert(0); // todo: handle offline state
    } else {
        LOG_WARN("Slave fd %d already offline", conn->_internal.fd);
    }

    LOG_INFO("Slave fd %d disconnected", conn->_internal.fd);
}


kvs_status_t kvs_my_slave_on_rdma_send(struct kvs_rdma_conn_s *conn, size_t send_off_set, int send_len) {

    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    if(slave_ctx == NULL) {
        LOG_FATAL("slave_ctx is NULL in rdma send completion");
        return KVS_ERR;
    }
    
    slave_ctx->sent_size_cur = send_len;
    return kvs_master_slave_state_machine_tick(slave_ctx->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_WRITE_DONE);
}

kvs_status_t kvs_my_slave_on_rdma_recv(struct kvs_rdma_conn_s *conn, size_t recv_off_set, int recv_len, int imm_data) {

    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    if(slave_ctx == NULL) {
        LOG_FATAL("slave_ctx is NULL in rdma recv completion");
        return KVS_ERR;
    }

    slave_ctx->recv_size_cur = recv_len;
    slave_ctx->buf_offset_cur = recv_off_set;
    slave_ctx->imm_data_cur = imm_data;
    return kvs_master_slave_state_machine_tick(slave_ctx->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_READ_READY);
}