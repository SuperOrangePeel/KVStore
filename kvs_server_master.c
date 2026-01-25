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

static inline kvs_status_t _kvs_master_slave_sync_begin(struct kvs_master_s *master) {
    if(master == NULL) {
        assert(0);
        return KVS_ERR;
    }
    LOG_DEBUG("_kvs_master_slave_sync_begin called, syncing_slaves_count: %d", master->syncing_slaves_count);
    // increase syncing slave count
    master->syncing_slaves_count += 1;
    if(master->syncing_slaves_count == 1) {
        // first syncing slave, open rdb fd
        if(master->rdb_fd > 0) {
            close(master->rdb_fd);
            master->rdb_fd = -1;
        }
        master->rdb_fd = open(master->server->pers_ctx->rdb_filename, O_RDONLY);
        struct stat rdb_st;
        fstat(master->rdb_fd, &rdb_st);
        master->rdb_size = rdb_st.st_size;

        //int rdb_max_chunk_size = master->server->rdma_max_chunk_size;

        // register mmap to RDMA engine if needed
        LOG_DEBUG("Master server use_rdma: %d", master->server->use_rdma);
        if(master->server->use_rdma) {
            master->rdb_mmap = mmap(NULL, master->rdb_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, master->rdb_fd, 0);
            if(master->rdb_mmap == MAP_FAILED) {
                LOG_FATAL("mmap RDB file failed: %s", strerror(errno));
                assert(0);
                return KVS_ERR;
            }
            madvise(master->rdb_mmap, rdb_st.st_size, MADV_SEQUENTIAL);
            //struct ibv_mr *mr = kvs_rdma_register_memory(&master->server->network.rdma_engine, master->rdb_mmap, master->rdb_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
            struct kvs_rdma_mr_s *mr = kvs_rdma_register_memory(&master->server->rdma_engine, master->rdb_mmap, 
                    master->rdb_size, KVS_RDMA_OP_SEND);
            LOG_DEBUG("Registered RDMA memory for RDB mmap, addr: %p, size: %zu", master->rdb_mmap, master->rdb_size);
            if(mr == NULL) {
                LOG_FATAL("kvs_rdma_register_memory failed");
                assert(0);
                return KVS_ERR;
            }
            master->rdb_mr = mr;
        }


        if(master->rdb_fd < 0) {
            LOG_FATAL("open rdb file %s failed: %s", master->server->pers_ctx->rdb_filename, strerror(errno));
            assert(0);
            return KVS_ERR;
        }
        master->is_repl_backlog = 1; // start writing to repl backlog
        master->repl_backlog_idx = 0; // reset repl backlog index
        master->repl_backlog_overflow = 0;

        
    }
    

    return KVS_OK;
}

static inline kvs_status_t _kvs_master_slave_sync_end(struct kvs_master_s *master) {
    if(master == NULL) {
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
        if(master->server->use_rdma && master->rdb_mmap != NULL) {
            LOG_DEBUG("Deregistering RDMA memory for RDB mmap");
            kvs_rdma_deregister_memory(master->rdb_mr);
            munmap(master->rdb_mmap, master->rdb_size);
            master->rdb_mmap = NULL;
        }
        if(master->rdb_fd > 0) {
            close(master->rdb_fd);
            master->rdb_fd = -1;
        }
        

        master->rdb_fd = -1; 
        master->repl_backlog_idx = 0; // reset backlog
        master->is_repl_backlog = 0; // stop writing to repl backlog
        master->repl_backlog_overflow = 0;
    }

    return KVS_OK;
}

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



kvs_status_t _kvs_repl_slave_none_handler(struct kvs_master_s *master, struct kvs_conn_header_s *slave_conn, kvs_event_trigger_t trigger) {
   
    if(slave_conn->type != KVS_CONN_TCP) {
        LOG_FATAL("invalid conn type for slave none handler: %d", slave_conn->type);
        assert(0);
        return KVS_ERR;
    }
    //struct kvs_conn_s *conn = (struct kvs_conn_s *)slave_conn;
    // 1. check if can accept new slave
    if(master->slave_count >= master->max_slave_count) {
        // slave limit reached
        LOG_WARN("slave limit reached");
        // todo : return more error info to slave
        return KVS_QUIT;
    }
    master->slave_count ++;


    if(slave_conn == NULL) assert(0);
    if(slave_conn->user_data == NULL) assert(0);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)slave_conn->user_data;
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
    slave_ctx->state = KVS_MY_SLAVE_WAIT_BGSAVE_END;
    

    return KVS_OK;
}


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
    
    if(master->rdb_fd > 0 && master->syncing_slaves_count == 0) {
        close(master->rdb_fd);
    }


    //LOG_INFO("RDB save finished, start sending RDB to slave fd %d", conn->_internal.fd);
    // 动作：切换状态，开始发文件
    slave_ctx->state = KVS_MY_SLAVE_SENDING_RDB;
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

kvs_status_t _kvs_repl_rdma_slave_wait_bgsave_end_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP) {
        LOG_FATAL("invalid conn type for rdma slave wait bgsave end handler: %d", conn->type);
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
    
    if(master->rdb_fd > 0 && master->syncing_slaves_count == 0) {
        close(master->rdb_fd);
    }


    //LOG_INFO("RDB save finished, start sending RDB to slave fd %d", conn->_internal.fd);
    // 动作：切换状态，开始发文件
    //slave_ctx->state = KVS_MY_SLAVE_SENDING_RDB;
    //return KVS_STATUS_CONTINUE; // trigger sending immediately
    //char *ip = conn->_internal.net->rdma.rdma_ip;
    int port = master->server->rdma_engine.rdma_port;

    struct stat rdb_st;
    int fd = open(master->server->pers_ctx->rdb_filename, O_RDONLY);
    if(fd < 0) {
        LOG_FATAL("open rdb file %s failed: %s", master->server->pers_ctx->rdb_filename, strerror(errno));
        assert(0);
        return KVS_ERR;
    }
    fstat(fd, &rdb_st);
    master->rdb_size = rdb_st.st_size;

    close(fd);
    
    // send RDMA info to slave
    // +FULLRESYNC <rdma_port> <token> <rdb_size>\r\n
    //slave_ctx->rdma_token = kvs_generate_token(); // todo: should not access _internal directly !!
    slave_ctx->rdma_token = kvs_session_register(&master->session_table, (void*)slave_ctx); // register session in
    slave_ctx->tcp_conn = (struct kvs_conn_s *)conn;

    LOG_DEBUG("Sending RDMA FULLRESYNC to slave: port=%d, token=%zu, rdb_size=%zu", 
        port, slave_ctx->rdma_token, master->rdb_size);

    struct kvs_conn_s *conn_tcp = (struct kvs_conn_s *)conn;
    int len = snprintf(conn_tcp->s_buffer, conn_tcp->s_buf_sz, "+FULLRESYNC %d %zu %zu\r\n", port, slave_ctx->rdma_token, master->rdb_size);
    conn_tcp->s_idx += len;
    kvs_net_set_send_event_manual(conn_tcp);

    slave_ctx->state = KVS_MY_SLAVE_WAIT_RDMA_READY;
    
    return KVS_OK;
}

kvs_status_t _kvs_repl_begin_wait_slave_recv_ready(struct kvs_master_s *master, struct kvs_my_slave_context_s *slave_ctx) {
    if(master == NULL || slave_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    LOG_DEBUG("_kvs_repl_begin_wait_slave_recv_ready called for slave idx %d", slave_ctx->slave_idx);
    slave_ctx->recv_buf = (char *)kvs_malloc(slave_ctx->master->server->rdma_max_chunk_size);
    slave_ctx->recv_mr = kvs_rdma_register_memory(&master->server->rdma_engine, NULL, slave_ctx->rdb_size, KVS_RDMA_OP_RECV);

    return KVS_OK;
}

kvs_status_t _kvs_repl_end_wait_slave_recv_ready(struct kvs_master_s *master, struct kvs_my_slave_context_s *slave_ctx) {
    if(master == NULL || slave_ctx == NULL) {
        assert(0);
        return KVS_ERR;
    }
    LOG_DEBUG("_kvs_repl_end_wait_slave_recv_ready called for slave idx %d", slave_ctx->slave_idx);
    if(slave_ctx->recv_mr != NULL) {
        kvs_rdma_deregister_memory(slave_ctx->recv_mr);
        slave_ctx->recv_mr = NULL;
    }
    if(slave_ctx->recv_buf != NULL) {
        kvs_free(slave_ctx->recv_buf, master->server->rdma_max_chunk_size);
        slave_ctx->recv_buf = NULL;
    }
    return KVS_OK;
}


kvs_status_t _kvs_repl_wait_rdma_ready_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger == KVS_EVENT_WRITE_DONE) {
        return KVS_OK; // wait for read event
    }
    if(trigger != KVS_EVENT_RDMA_ESTABLISHED) {
        LOG_FATAL("invalid trigger for wait rdma ready handler: %d", trigger);
        assert(0);
        return KVS_ERR;
    }
    if(conn == NULL) {
        assert(0);
        return KVS_ERR;
    }
    //struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    // if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
    //     LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
    //     assert(0);
    //     return KVS_ERR;
    // }
    // if(slave_ctx->rdma_conn != NULL) {
    //     LOG_FATAL("rdma_conn already set in rdma wait rdma ready handler");
    //     assert(0);
    //     return KVS_ERR;
    // }
    if(conn->user_data != NULL) {
        LOG_FATAL("conn user_data already set in rdma wait rdma ready handler");
        assert(0);
        return KVS_ERR;
    }

    struct kvs_rdma_conn_s *rdma_conn = (struct kvs_rdma_conn_s *)conn;
    if(rdma_conn->header.type != KVS_CONN_RDMA) {
        LOG_FATAL("invalid conn type for rdma wait rdma ready handler: %d", rdma_conn->header.type);
        assert(0);
        return KVS_ERR;
    }


    // todo: verify token
    //uint64_t *received_token = (uint64_t*)conn->user_data;//slave_ctx->rdma_token;
    if(rdma_conn->private_data_len != sizeof(uint64_t)) {
        LOG_FATAL("invalid private_data_len: %d, expected: %lu", rdma_conn->private_data_len, sizeof(uint64_t));
        assert(0);
        return KVS_ERR;
    }
    uint64_t *received_token = (uint64_t*)rdma_conn->private_data;
    LOG_DEBUG("Received token from slave: %zu", *received_token);
    
    struct kvs_my_slave_context_s *matched_ctx = kvs_session_match(&master->session_table, *received_token); // remove token from session table
    if(matched_ctx == NULL) {
        LOG_FATAL("no matching session for token: %lu", *received_token);
        assert(0); // todo : close connection
        return KVS_ERR;
    }
    // rdma_conn->header.user_data = matched_ctx;
    // matched_ctx->rdma_conn = rdma_conn;

    struct kvs_rdma_conn_s *rdma_slave_conn = (struct kvs_rdma_conn_s *)conn;
    //rdma_slave_conn->header.user_data = matched_ctx;
    kvs_server_share_conn_type((struct kvs_server_s*)master->server, (struct kvs_conn_header_s*)matched_ctx->tcp_conn, 
        (struct kvs_conn_header_s*)rdma_slave_conn);
    if(rdma_slave_conn->header.user_data != matched_ctx) {
        LOG_FATAL("rdma_slave_conn user_data does not match matched_ctx after sharing conn type");
        assert(0);
        return KVS_ERR;
    }
    matched_ctx->rdma_conn = rdma_slave_conn;

    // RDMA ready received from slave
    //LOG_DEBUG("Received RDMA READY from slave fd %d", conn->_internal.fd);

    //kvs_rdma_post_recv(rdma_conn, , size_t off_set, int len, void *user_data)
    _kvs_repl_begin_wait_slave_recv_ready(master, matched_ctx);
    matched_ctx->state = KVS_MY_SLAVE_WAIT_RECV_READY;
    _kvs_master_slave_sync_begin(master);
    return KVS_OK; // wait for establish RDMA connection
}

kvs_status_t _kvs_repl_wait_slave_recv_ready_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // not used in rdma replication
    _kvs_repl_begin_wait_slave_recv_ready(master, (struct kvs_my_slave_context_s*)conn->user_data);


    return KVS_OK;
}

kvs_status_t _kvs_repl_rdma_slave_sending_rdb_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    /************rdma发送 *******/
    if(conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA) {
        LOG_FATAL("invalid conn type for rdma slave sending rdb handler: %d", conn->type);
        assert(0);
        return KVS_ERR;
    }
    LOG_DEBUG("RDMA slave sending rdb handler called");
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    size_t *rdb_offset = &slave_ctx->rdb_offset;

    struct kvs_rdma_conn_s *slave_conn = (struct kvs_rdma_conn_s *)conn;
    //assert(slave_conn->s_idx == 0);


    if(trigger == KVS_EVENT_WRITE_DONE) {
        // RDMA send completed
        LOG_DEBUG("RDMA send completed for RDB chunk, offset: %zu, size: %d", *rdb_offset, slave_ctx->send_rdb_chunk_size_cur);
        *rdb_offset += slave_ctx->send_rdb_chunk_size_cur;
    }

    // if(slave_conn->type != KVS_CONN_RDMA) {
    //     LOG_FATAL("invalid conn type for rdma rdb sending: %d", slave_conn->type);
    //     assert(0);
    //     return KVS_ERR;
    // }

    assert(*rdb_offset <= master->rdb_size);
    if(*rdb_offset == master->rdb_size) {
        // finish sending rdb file
        LOG_DEBUG("finish sending rdb file to slave via RDMA");
        // rdb file descriptor will be closed when next rdb save
        // close(conn->server->master.rdb_fd);
        // conn->server->master.rdb_fd = -1;
        slave_ctx->state = KVS_MY_SLAVE_WAIT_RDB_ACK;
        //*rdb_offset = 0;
        //assert(slave_conn->s_idx == 0);
        return KVS_OK; // continue to send backlog via TCP
    } 
    slave_ctx->master = master; // set back reference to master
    size_t rdb_max_chunk_size = master->server->rdma_max_chunk_size;
    if(master->rdb_size - *rdb_offset > rdb_max_chunk_size) {
        // send RDMA_MAX_CHUNK_SIZE
        kvs_rdma_post_send(slave_conn, master->rdb_mr, 0, *rdb_offset, rdb_max_chunk_size, NULL);
        LOG_DEBUG("Posted RDMA send for RDB chunk, offset: %zu, size: %d", *rdb_offset, rdb_max_chunk_size);
        //*rdb_offset += rdb_max_chunk_size;
    } else {
        // send remaining
        // imm_data = -1 indicates last chunk
        //getchar();
        kvs_rdma_post_send(slave_conn, master->rdb_mr, -1, *rdb_offset, master->rdb_size - *rdb_offset, NULL);
        LOG_DEBUG("Posted RDMA send for RDB chunk, offset: %zu, size: %zu", *rdb_offset, master->rdb_size - *rdb_offset);
        //*rdb_offset = master->rdb_size;
        return KVS_OK; 
    }
    //kvs_net_rdma_post_send(slave_conn, master->rdb_mr, *rdb_offset, master->rdb_size - *rdb_offset);
    return KVS_OK; // wait for RDMA send completion
}

kvs_status_t _kvs_repl_wait_rdb_ack_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // not used in rdma replication
    
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    struct kvs_conn_s *tcp_conn = slave_ctx->tcp_conn;
    assert(tcp_conn->s_idx == 0);
    kvs_master_slave_state_machine_tick(master, (struct kvs_conn_header_s *)tcp_conn, KVS_EVENT_WRITE_DONE);
    return KVS_OK;
}


kvs_status_t _kvs_repl_slave_sending_backlog_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP) {
        LOG_FATAL("invalid conn type for slave sending backlog handler: %d", conn->type);
        assert(0);
        return KVS_ERR;
    }
    struct kvs_conn_s *slave_conn = (struct kvs_conn_s *)conn;
    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    if(master->repl_backlog_idx == 0) {
        // no backlog to send
        LOG_DEBUG("no backlog to send to slave");
        slave_ctx->state = KVS_MY_SLAVE_ONLINE;
        _kvs_master_slave_sync_end(master);
        kvs_master_add_slave(master, slave_conn);
        assert(slave_conn->s_idx == 0);
        return KVS_STATUS_CONTINUE; // done
    }
    size_t *repl_backlog_offset = &slave_ctx->repl_backlog_offset;
    assert(slave_conn->s_idx == 0);
    int to_send = master->repl_backlog_idx - *repl_backlog_offset;
    
    if(to_send > slave_conn->s_buf_sz) {
        to_send = slave_conn->s_buf_sz;
    }
    
    if(to_send == 0) {
        // finish sending backlog
        LOG_DEBUG("finish sending backlog to slave");
        slave_ctx->state = KVS_MY_SLAVE_ONLINE;
        _kvs_master_slave_sync_end(master);
        kvs_master_add_slave(master, slave_conn);
        *repl_backlog_offset = 0;
        assert(slave_conn->s_idx == 0);
        return KVS_STATUS_CONTINUE; // done
    } else {
        memcpy(slave_conn->s_buffer, master->repl_backlog + *repl_backlog_offset, to_send);
        *repl_backlog_offset += to_send;
        slave_conn->s_idx = to_send;
        kvs_net_set_send_event_manual(slave_conn);
    }
    return KVS_OK;
}

kvs_status_t kvs_repl_slave_online_handler(struct kvs_master_s *master, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(conn == NULL || conn->user_data == NULL) {
        assert(0);
        return KVS_ERR;
    }
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
    assert(slave_conn->s_idx == 0);


    assert(slave_ctx->slave_idx != -1);
    
    // now in online state, do nothing
    return KVS_OK;
}

typedef kvs_status_t (*slave_state_handler_t)(struct kvs_master_s *master, struct kvs_conn_header_s *slave_conn, kvs_event_trigger_t trigger);

slave_state_handler_t slave_state_handlers[] = {
    [KVS_MY_SLAVE_NONE] = _kvs_repl_slave_none_handler,
    [KVS_MY_SLAVE_WAIT_BGSAVE_END] =  _kvs_repl_rdma_slave_wait_bgsave_end_handler, //_kvs_repl_slave_wait_bgsave_end_handler,
    [KVS_MY_SLAVE_WAIT_RDMA_READY] = _kvs_repl_wait_rdma_ready_handler,
    [KVS_MY_SLAVE_WAIT_RECV_READY] = _kvs_repl_wait_slave_recv_ready_handler,
    [KVS_MY_SLAVE_SENDING_RDB] = _kvs_repl_rdma_slave_sending_rdb_handler, // _kvs_repl_slave_sending_rdb_handler,
    [KVS_MY_SLAVE_WAIT_RDB_ACK] = _kvs_repl_wait_rdb_ack_handler,
    [KVS_MY_SLAVE_SENDING_BACKLOG] = _kvs_repl_slave_sending_backlog_handler,
    [KVS_MY_SLAVE_ONLINE] = kvs_repl_slave_online_handler,
    [KVS_MY_SLAVE_OFFLINE] = NULL
};

kvs_status_t kvs_master_slave_state_machine_tick(struct kvs_master_s *master, struct kvs_conn_header_s *slave_conn, kvs_event_trigger_t trigger) {
    if(master == NULL || slave_conn == NULL) return KVS_ERR;
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)slave_conn->user_data;
    if(slave_ctx == NULL && trigger != KVS_EVENT_RDMA_ESTABLISHED) {
        LOG_FATAL("slave_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    // if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
    //     LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
    //     assert(0);
    //     return KVS_ERR;
    // }

    kvs_status_t ret = KVS_STATUS_CONTINUE;

    if(trigger == KVS_EVENT_RDMA_ESTABLISHED && slave_ctx == NULL) {
        // special case for RDMA connection establishment
        ret = slave_state_handlers[KVS_MY_SLAVE_WAIT_RDMA_READY](master, slave_conn, trigger);
        if(ret != KVS_STATUS_CONTINUE) {
            return ret;
        }
        // get the slave_ctx after RDMA established
        slave_ctx = (struct kvs_my_slave_context_s*)slave_conn->user_data;
        if(slave_ctx == NULL) {
            LOG_FATAL("slave_ctx is NULL after RDMA established");
            assert(0);
            return KVS_ERR;
        }
    }

    while(ret == KVS_STATUS_CONTINUE) {
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
        
    
        ret = handler(master, slave_conn, trigger); // ignore return value for now
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
    kvs_master_slave_state_machine_tick(conn->server_ctx->master, conn, KVS_EVENT_READ_READY);
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
        assert(0);
        return;
    }
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    if(slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", slave_ctx->header.type);
        assert(0);
        return;
    }

    if(slave_ctx->state != KVS_MY_SLAVE_OFFLINE) {
        slave_ctx->state = KVS_MY_SLAVE_OFFLINE;
        kvs_master_remove_slave(master, conn);
    } else {
        LOG_WARN("Slave fd %d already offline", conn->_internal.fd);
    }

    LOG_INFO("Slave fd %d disconnected", conn->_internal.fd);
}


kvs_status_t kvs_my_slave_on_rdma_send(struct kvs_rdma_conn_s *conn,   size_t send_off_set, int send_len) {

    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    if(slave_ctx == NULL) {
        LOG_FATAL("slave_ctx is NULL in rdma send completion");
        return KVS_ERR;
    }
    if(slave_ctx->master == NULL) {
        LOG_FATAL("slave_ctx->master is NULL in rdma send completion");
        return KVS_ERR;
    }
    
    slave_ctx->send_rdb_chunk_size_cur = send_len;
    return kvs_master_slave_state_machine_tick(slave_ctx->master, (struct kvs_conn_header_s *)conn, KVS_EVENT_WRITE_DONE);
}

kvs_status_t kvs_my_slave_on_rdma_recv(struct kvs_rdma_conn_s *conn) {
    assert(0); // should not receive anything from slave via RDMA
}