#include "kvs_conn.h"
#include "kvs_event_loop.h"
#include "kvs_network.h"
#include "kvs_rdma_engine.h"
#include "kvs_server.h"

#include "kvs_persistence.h"
#include "common.h"
#include "kvs_types.h"
#include "logger.h"

#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <assert.h>


static int _init_connection(struct kvs_slave_s *slave) {
    if(slave == NULL) return -1;


    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(slave->master_port);
    inet_pton(AF_INET, slave->master_ip, &serv_addr.sin_addr);
    LOG_DEBUG("Connecting to master %s:%d", slave->master_ip, slave->master_port);
    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }
    slave->master_fd = sockfd;
    return sockfd;
}


int _kvs_slave_get_rdma_info(struct kvs_slave_s *slave, int *rdma_port_o, uint64_t *token_o, size_t *rdb_size_o) {

    char response_buf[128];
    ssize_t received = recv(slave->master_fd, response_buf, sizeof(response_buf), 0);
    LOG_DEBUG("Received SYNC response from master: %.*s", (int)received, response_buf);
    if (received <= 0) {
        perror("recv SYNC response");
        return -1;
    }
    int idx = 0;
    if(response_buf[idx] != '+') {
        printf("invalid SYNC response from master\n"); 
        return -1;
    }
    idx ++;
    if(strncmp(&response_buf[idx], "FULLRESYNC ", strlen("FULLRESYNC ")) != 0) {
        printf("invalid SYNC response from master\n"); 
        return -1;
    }
    idx += strlen("FULLRESYNC ");
    int rdma_port = kvs_parse_int(response_buf, received, &idx);
    if(rdma_port <= 0) {
        printf("invalid RDMA port from master: %d\n", rdma_port);
        return -1;
    }
    if(response_buf[idx] != ' ') {
        printf("invalid SYNC response format from master\n"); 
        return -1;
    }
    idx ++; 
    uint64_t token = kvs_parse_uint64(response_buf, received, &idx);
    if(token == 0) {
        printf("invalid RDMA token from master: %lu\n", token);
        return -1;
    }
    if(response_buf[idx] != ' ') {
        printf("invalid SYNC response format from master\n");
        return -1;
    }
    idx ++;
    size_t rdb_size = kvs_parse_int(response_buf, received, &idx);
    if(rdb_size <= 0) {
        printf("invalid RDB size from master: %zu\n", rdb_size);
        return -1;
    }

    *rdma_port_o = rdma_port;
    *token_o = token;
    *rdb_size_o = rdb_size;

    return 0;
}

int _kvs_slave_get_rdma_info_raw_buffer(char *buf, int len, int *rdma_port_o, uint64_t *token_o, size_t *rdb_size_o) {
    if(buf == NULL || len <=0) {
        return -1;
    }
    int idx = 0;
    if(buf[idx] != '+') {
        printf("invalid SYNC response from master\n"); 
        return -1;
    }
    idx ++;
    if(strncmp(&buf[idx], "FULLRESYNC ", strlen("FULLRESYNC ")) != 0) {
        printf("invalid SYNC response from master\n"); 
        return -1;
    }
    idx += strlen("FULLRESYNC ");
    int rdma_port = kvs_parse_int(buf, len, &idx);
    if(rdma_port <= 0) {
        printf("invalid RDMA port from master: %d\n", rdma_port);
        return -1;
    }
    if(buf[idx] != ' ') {
        printf("invalid SYNC response format from master\n"); 
        return -1;
    }
    idx ++; 
    uint64_t token = kvs_parse_uint64(buf, len, &idx);
    if(token == 0) {
        printf("invalid RDMA token from master: %lu\n", token);
        return -1;
    }
    if(buf[idx] != ' ') {
        printf("invalid SYNC response format from master\n");
        return -1;
    }
    idx ++;
    size_t rdb_size = kvs_parse_int(buf, len, &idx);
    if(rdb_size <= 0) {
        printf("invalid RDB size from master: %zu\n", rdb_size);
        return -1;
    }

    *rdma_port_o = rdma_port;
    *token_o = token;
    *rdb_size_o = rdb_size;

    return 0;
}



int _kvs_slave_rdma_connect_master(struct kvs_slave_s *slave, int rdma_port, uint64_t token, size_t rdb_size) {
    if(slave == NULL) return -1;

    LOG_DEBUG("Connecting to master RDMA %s:%d with token %lu", slave->master_ip, rdma_port, token);

    


    //LOG_DEBUG("RDMA connection established to master %s:%d, fd: %d", slave->master_ip, rdma_port, rdma_conn->_internal.fd);

    return 0;
}

int _kvs_slave_sync_rdb_rdma(struct kvs_slave_s *slave) {
    if(slave == NULL) return -1;

    // send SYNC command to master
    const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n";
    ssize_t sent_bytes = send(slave->master_fd, sync_cmd, strlen(sync_cmd), 0);
    if (sent_bytes < 0) {
        perror("send SYNC");
        return -1;
    }

    // +FULLRESYNC <rdma_port> <token> <rdb_size>\r\n
    int rdma_port = 0;
    uint64_t token = 0;
    size_t rdb_size = 0;
    _kvs_slave_get_rdma_info(slave, &rdma_port, &token, &rdb_size);

    _kvs_slave_rdma_connect_master(slave, rdma_port, token, rdb_size);
    


    LOG_DEBUG("RDMA port: %d, token: %lu, RDB size: %zu\n", rdma_port, token, rdb_size);


    return 0;

}

int _kvs_slave_sync_rdb(struct kvs_slave_s *slave) {
    if(slave == NULL) return -1;

    // send SYNC command to master
    const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n";
    ssize_t sent_bytes = send(slave->master_fd, sync_cmd, strlen(sync_cmd), 0);
    if (sent_bytes < 0) {
        perror("send SYNC");
        return -1;
    }

    char tmp_filename[64];
    snprintf(tmp_filename, sizeof(tmp_filename), "slave_sync_%d.rdb", slave->master_fd);
    FILE* tmp_rdb_fp = fopen(tmp_filename, "wb");
    // how can I know the RDB size? server should send it first!
    // $filesize\r\n<filedata>
    if (tmp_rdb_fp == NULL) {
        perror("fopen tmp RDB file");
        return -1;
    }
    char header_buf[64];
    // todo: what if there is no rdb data from master?
    ssize_t received = recv(slave->master_fd, header_buf, sizeof(header_buf), 0);
    if (received <= 0) {
        perror("recv RDB header");
        fclose(tmp_rdb_fp);
        return -1;
    }
    int idx = 0;
    if(header_buf[idx] != '$') {
        printf("invalid RDB header from master\n"); 
        fclose(tmp_rdb_fp);
        return -1;
    }
    idx ++;
    int rdb_size = kvs_parse_int(header_buf, received, &idx);
    if(rdb_size <= 0) {
        printf("invalid RDB size from master: %d\n", rdb_size); 
        fclose(tmp_rdb_fp);
        return -1;
    } 
    if(header_buf[idx] != '\r' || header_buf[idx + 1] != '\n') {
        printf("invalid RDB header format from master\n"); 
        fclose(tmp_rdb_fp);
        return -1;
    }
    idx += 2;
    fwrite(header_buf + idx, 1, received - idx, tmp_rdb_fp);
    int f_received = received - idx;
    printf("RDB size to receive: %d, already received: %d\n", rdb_size, f_received);
    while(f_received < rdb_size) {
        char data_buf[8192];
        ssize_t chunk = recv(slave->master_fd, data_buf, sizeof(data_buf), 0);
        if (chunk < 0) {
            perror("recv RDB data");
            fclose(tmp_rdb_fp);
            return -1;
        } else if (chunk == 0) {
            printf("master closed connection unexpectedly during RDB transfer\n");
            fclose(tmp_rdb_fp);
            return -1;
        }
        fwrite(data_buf, 1, chunk, tmp_rdb_fp);
        f_received += chunk;
    }

    fclose(tmp_rdb_fp);
    if(slave->server->pers_ctx == NULL) {
        printf("slave server pers_ctx is NULL\n");
        assert(0);
        return -1;
    }
    if(rename(tmp_filename, slave->server->pers_ctx->rdb_filename) != 0) {
        perror("rename RDB file");
        return -1;
    }

    kvs_server_load_rdb(slave->server);


    return 0;
}

kvs_status_t kvs_slave_connect_master(struct kvs_slave_s *slave) {

    if (NULL == slave) return KVS_ERR;

    if(_init_connection(slave) < 0) {
        return KVS_ERR;
    }
    LOG_DEBUG("Connected to master %s:%d, fd: %d", slave->master_ip, slave->master_port, slave->master_fd);
#if 1
    if(0 == _kvs_slave_sync_rdb_rdma(slave)) {
        return KVS_OK;
    } else {
        return KVS_ERR;
    }
#else
    if(0 == _kvs_slave_sync_rdb(slave)){
        return KVS_OK;
    } else {
        return KVS_ERR;
    }
#endif
}

kvs_status_t kvs_slave_init(struct kvs_slave_s *slave, struct kvs_server_s *server , struct kvs_slave_config_s *config) {
    if(slave == NULL || config == NULL) return KVS_ERR;

    memset(slave, 0, sizeof(struct kvs_slave_s));
    slave->master_fd = -1;
    // strncpy(slave->master_ip, config->master_ip, strlen(config->master_ip));
    slave->master_ip = config->master_ip;
    slave->master_port = config->master_port;
    slave->rdb_recv_buffer_count = config->rdb_recv_buffer_count;
    //
    // slave->state = KVS_MY_MASTER_NONE;
    slave->server = server;

    return KVS_OK;
}
kvs_status_t kvs_slave_deinit(struct kvs_slave_s *slave) {
    if(slave == NULL) return KVS_ERR;

    if(slave->master_fd > 0) {
        close(slave->master_fd);
        slave->master_fd = -1;
    }

    return KVS_OK;
}



kvs_status_t kvs_my_master_connecting_handler(struct kvs_slave_s *slave, struct kvs_conn_header_s *master_conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_WRITE_DONE) {
        LOG_FATAL("invalid trigger for master connecting handler: %d", trigger);
        assert(0);
        return KVS_ERR;
    }
    if(slave == NULL || master_conn == NULL) return KVS_ERR;
    struct kvs_my_master_context_s *ctx = (struct kvs_my_master_context_s *)master_conn->user_data;
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in connecting handler");
        assert(0);
        return KVS_ERR; 
    }

    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(slave->master_fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        // 连接失败，重试...
        LOG_DEBUG("Failed to connect to master, will retry...");
        assert(0);
        return KVS_AGAIN;
    }

    // 2. 连接成功！状态跃迁
    LOG_DEBUG("Connected to Master via TCP.");
    ctx->state = KVS_MY_MASTER_SENDING_SYNC;

    // // 3. 【核心回答】如何发 SYNC？
    // // 直接在这里构造发送请求！
    // //char *cmd = "SYNC\r\n";
    // // 使用 io_uring 的异步发送接口
    // // 注意：这里要把监听事件改为 POLLIN (等待回复)，或者先 Send 再 wait POLLIN
    // //kvs_loop_send(server.loop, link->fd, cmd, strlen(cmd), link);
    // assert(master_conn->s_idx == 0);
    // master_conn->s_idx = sprintf(master_conn->s_buffer, "%s", "SYNC\r\n");

    // kvs_net_set_send_event_manual(master_conn); 
    
    return KVS_STATUS_CONTINUE;
}


kvs_status_t kvs_my_master_sending_sync_handler(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(slave == NULL || conn == NULL) {
        assert(0);
        return KVS_ERR;
    }

    // send SYNC command to master
    // ssize_t sent_bytes = send(slave->master_fd, sync_cmd, strlen(sync_cmd), 0);
    // if (sent_bytes < 0) {
    //     perror("send SYNC");
    //     return KVS_ERR;
    // }
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    assert(master_conn->s_idx == 0);
    master_conn->s_idx = snprintf(master_conn->s_buffer, master_conn->s_buf_sz, "%s",  "*1\r\n$4\r\nSYNC\r\n");
    kvs_net_set_send_event_manual((struct kvs_conn_s *)master_conn);
    struct kvs_my_master_context_s *ctx = (struct kvs_my_master_context_s *)master_conn->header.user_data;
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in sending sync handler");
        assert(0);
        return KVS_ERR; 
    }
    ctx->state = KVS_MY_MASTER_WAITING_RES;
    kvs_net_set_recv_event(master_conn);
    return KVS_OK;
}

kvs_status_t kvs_my_master_waiting_res_handler(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    LOG_DEBUG("In master waiting response handler, trigger: %d", trigger);
    if(trigger == KVS_EVENT_WRITE_DONE) {
        LOG_DEBUG("Master SYNC command sent, now wait for response");
        return KVS_OK; // wait for read event
    }
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger for master waiting res handler: %d", trigger);
        assert(0);
        return KVS_ERR;
    }
    if(slave == NULL || conn == NULL) {
        assert(0);
        return KVS_ERR;
    }

    struct kvs_my_master_context_s *ctx = (struct kvs_my_master_context_s *)conn->user_data;
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    // +FULLRESYNC <rdma_port> <token> <rdb_size>\r\n
    int rdma_port = 0;
    uint64_t token = 0;
    size_t rdb_size = 0;
    //_kvs_slave_get_rdma_info(slave, &rdma_port, &token, &rdb_size);
    _kvs_slave_get_rdma_info_raw_buffer(master_conn->r_buffer, master_conn->r_idx, &rdma_port, &token, &rdb_size);
    //_kvs_slave_rdma_connect_master(slave, rdma_port, token, rdb_size);
    
    LOG_DEBUG("RDMA port: %d, token: %lu, RDB size: %zu", rdma_port, token, rdb_size);

    ctx->rdma_port = rdma_port;
    ctx->token = token;
    ctx->rdb_size = rdb_size;
    ctx->tcp_conn = master_conn;
    ctx->state = KVS_MY_MASTER_RDMA_CONNECTING;

    return KVS_STATUS_CONTINUE;
}

kvs_status_t kvs_my_master_rdma_connecting(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(slave == NULL || conn == NULL) return KVS_ERR;
    struct kvs_my_master_context_s *ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in rdma connecting handler");
        assert(0);
        return KVS_ERR; 
    }

    // connect to master via RDMA
    //  _kvs_slave_rdma_connect_master(slave, ctx->rdma_port, ctx->token, ctx->rdb_size);
    if(ctx->token == 0) {
        LOG_FATAL("invalid RDMA token 0 in rdma connecting handler");
        assert(0);
        return KVS_ERR; 
    }
    ctx->tcp_conn = (struct kvs_conn_s *)conn;
    // 这里直接把ctx赋值给rdma_conn的user_data
    kvs_rdma_post_connect(&slave->server->rdma_engine, slave->master_ip, ctx->rdma_port, &ctx->token, sizeof(ctx->token), ctx);
    LOG_DEBUG("Posted RDMA connect to master %s:%d with token %lu", slave->master_ip, ctx->rdma_port, ctx->token);

    LOG_DEBUG("RDMA connection request posted to master %p, waiting for established event", ctx);
    ctx->state = KVS_MY_MASTER_RECEIVING_RDB; 
    
    return KVS_OK; // wait for RDMA connection established event
}
// 
//
// ========================= rdma connect ==================
struct kvs_rdb_write_context_s {
    struct kvs_my_master_context_s *master_ctx;
    char *buffer; // registered buffer pointer
    size_t length; // length of data to write
    size_t offset; // rdb buffer offset
    int is_end; // is last chunk
};

static void _kvs_my_master_handle_rdb_fsync_completion(void* ctx, int res, int flags) {
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)ctx;
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in RDB fsync completion");
        assert(0);
        return;
    }
    if(res < 0) {
        LOG_FATAL("RDB fsync failed: %d, %s", res, strerror(-res));
        assert(0);
        return;
    }

    LOG_DEBUG("RDB fsync completed successfully");

    close(master_ctx->rdb_fd);
    master_ctx->rdb_fd = -1;

    // RDB sync complete, transition state
    master_ctx->state = KVS_MY_MASTER_RECEIVING_REPLICATION; 
    
    // todo: notify server to start replication
    return; 
}

static void _kvs_my_master_handle_rdb_write_completion(void* ctx, int res, int flags) {
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in RDB write completion");
        assert(0);
        return;
    }
    struct kvs_rdb_write_context_s *rdb_write_ctx = (struct kvs_rdb_write_context_s *)ctx;
    struct kvs_my_master_context_s *master_ctx = rdb_write_ctx->master_ctx;
    char *buffer = rdb_write_ctx->buffer;
    int write_offset = rdb_write_ctx->offset;
    int write_len = rdb_write_ctx->length;
    int is_end = rdb_write_ctx->is_end;

    kvs_free(rdb_write_ctx, sizeof(struct kvs_rdb_write_context_s));
    if(res < 0) {
        LOG_FATAL("RDB write failed: %d, %s", res, strerror(-res));
        assert(0);
        return;
    }
    if(res == 0) {
        LOG_FATAL("RDB write returned 0 bytes written");
        assert(0);
        return;
    }
    int written = res;
    master_ctx->rdb_offset += written;
    if(is_end) {
        LOG_DEBUG("RDB write completed for last chunk, total written: %zu / %zu", master_ctx->rdb_offset, master_ctx->rdb_size);
        if(master_ctx->rdb_offset != master_ctx->rdb_size) {
            LOG_FATAL("RDB size mismatch on last chunk: expected %zu, got %zu", master_ctx->rdb_size, master_ctx->rdb_offset);
            assert(0);
            return;
        }
        // RDB receiving complete
        LOG_DEBUG("RDB receiving complete, total size: %zu", master_ctx->rdb_offset);
        struct kvs_event_s *fsync_ev = &master_ctx->rdb_fsync_ev;
        fsync_ev->ctx = (void *)master_ctx;
        fsync_ev->fd = master_ctx->rdb_fd;
        fsync_ev->handler = _kvs_my_master_handle_rdb_fsync_completion; // not used
        kvs_loop_add_fsync(&master_ctx->slave->server->loop, &master_ctx->rdb_fsync_ev, master_ctx->rdb_fd);
        return;
    }

    LOG_DEBUG("RDB write completed, total written: %zu / %zu", master_ctx->rdb_offset, master_ctx->rdb_size);
    struct kvs_slave_s *slave = master_ctx->slave;
    
    struct kvs_rdma_conn_s *master_conn = (struct kvs_rdma_conn_s *)master_ctx->rdma_conn;
    if(master_conn == NULL) {
        LOG_FATAL("master_conn is NULL in RDB write completion");
        assert(0);
        return;
    }
    kvs_rdma_post_recv(master_conn, master_ctx->rdb_recv_mr, write_offset, master_ctx->rdb_recv_buf_sz, NULL);
    return ;

    // post more RDMA recv
}

kvs_status_t kvs_my_master_receiving_rdb_handler(struct kvs_slave_s *slave, struct kvs_conn_header_s *master_conn, kvs_event_trigger_t trigger) {
    //getchar(); 
    if(slave == NULL || master_conn == NULL) return KVS_ERR;
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)master_conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in receiving rdb handler");
        assert(0);
        return KVS_ERR; 
    }
    struct kvs_rdma_conn_s *rdma_conn = (struct kvs_rdma_conn_s *)master_conn;
    if(rdma_conn->header.type != KVS_CONN_RDMA) {
        LOG_FATAL("invalid conn type for receiving rdb handler: %d", rdma_conn->header.type);
        assert(0);
        return KVS_ERR;
    }
    if(master_ctx->rdb_recv_mr == NULL){
        // allocate RDB recv MR
        int rdb_max_chunk_size = slave->server->rdma_max_chunk_size;
        char *rdb_recv_buffer = (char *)kvs_malloc(rdb_max_chunk_size * slave->rdb_recv_buffer_count);
        LOG_DEBUG("Allocating RDB recv buffer: %zu bytes (%d chunks of %d bytes)", rdb_max_chunk_size * slave->rdb_recv_buffer_count, slave->rdb_recv_buffer_count, rdb_max_chunk_size);
        master_ctx->rdb_recv_mr = kvs_rdma_register_memory(&slave->server->rdma_engine, rdb_recv_buffer, rdb_max_chunk_size * slave->rdb_recv_buffer_count, KVS_RDMA_OP_RECV);
        if(master_ctx->rdb_recv_mr == NULL) {
            LOG_FATAL("failed to register RDB recv MR");
            assert(0);
            return KVS_ERR;
        }  
        master_ctx->rdb_recv_buffer = rdb_recv_buffer;
        master_ctx->rdb_recv_buf_sz = rdb_max_chunk_size;
        master_ctx->rdb_recv_buffer_count = slave->rdb_recv_buffer_count;
        master_ctx->rdb_offset = 0;
        master_ctx->rdb_fd = open(slave->server->pers_ctx->rdb_filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if(master_ctx->rdb_fd < 0) {
            LOG_FATAL("failed to open RDB file for writing: %s", strerror(errno));
            assert(0);
            return KVS_ERR;
        }
        //LOG_DEBUG("cm_id %p post recv ", rdma_conn->cm_id);
        for(int i = 0; i < master_ctx->rdb_recv_buffer_count; i ++) {
            LOG_DEBUG("Posting RDMA recv for RDB chunk %d at offset %d", i, i * master_ctx->rdb_recv_buf_sz);
            kvs_rdma_post_recv(rdma_conn, master_ctx->rdb_recv_mr, i * master_ctx->rdb_recv_buf_sz, master_ctx->rdb_recv_buf_sz, NULL);
        }
        

        struct kvs_event_s *rdb_write_ev = &master_ctx->rdb_write_ev;

        rdb_write_ev->fd = master_ctx->rdb_fd;
        rdb_write_ev->handler = _kvs_my_master_handle_rdb_write_completion; // not used
        rdb_write_ev->ctx = (void *)master_ctx;

        return KVS_OK; // wait for recv completions
    }
    int write_offset = master_ctx->rdb_recv_buf_offset_cur; // = i * master_ctx->rdb_recv_buf_sz;
    int write_len = master_ctx->rdb_recv_len_cur; // = received_len;
    int imm_data = master_ctx->rdb_imm_data_cur; // = imm_data;
    
    struct kvs_event_s *rdb_write_ev = &master_ctx->rdb_write_ev;
    LOG_DEBUG("RECEIVED RDB chunk: offset %d, length %d, imm_data %d", write_offset, write_len, imm_data);

    struct kvs_rdb_write_context_s *rdb_write_ctx = (struct kvs_rdb_write_context_s *)kvs_malloc(sizeof(struct kvs_rdb_write_context_s));
    rdb_write_ctx->master_ctx = master_ctx;
    rdb_write_ctx->buffer = master_ctx->rdb_recv_buffer; // registered buffer pointer
    rdb_write_ctx->length = write_len;
    rdb_write_ctx->offset = write_offset;
    rdb_write_ctx->is_end = (imm_data == -1) ? 1 : 0;
    rdb_write_ev->ctx = (void *)rdb_write_ctx;
    // if(imm_data == -1) {
    //     // LAST CHUNK
    //     int remaining = master_ctx->rdb_size - master_ctx->rdb_offset;
    //     if(remaining != write_len) {
    //         LOG_ERROR("Invalid last chunk size: %d, remaining: %zu", write_len, remaining);
    //         assert(0);
    //         return KVS_ERR;
    //     }
    //     kvs_loop_add_write(&slave->server->loop, rdb_write_ev, master_ctx->rdb_recv_buffer + write_offset, write_len);
    // }
    kvs_loop_add_write(&slave->server->loop, rdb_write_ev, master_ctx->rdb_recv_buffer + write_offset, write_len);
    

    //getchar(); // for debug

    // receive RDB via RDMA
    // todo: implement RDMA RDB receiving logic here
    //
    return KVS_OK;
}

typedef kvs_status_t (*kvs_repl_master_state_handler_t)(struct kvs_slave_s *slave, struct kvs_conn_header_s *master_conn, kvs_event_trigger_t trigger);

kvs_repl_master_state_handler_t kvs_repl_master_state_handlers[] = {
    [KVS_MY_MASTER_NONE] = NULL,
    [KVS_MY_MASTER_CONNECTING] = kvs_my_master_connecting_handler,
    [KVS_MY_MASTER_SENDING_SYNC] = kvs_my_master_sending_sync_handler,
    [KVS_MY_MASTER_WAITING_RES] = kvs_my_master_waiting_res_handler,
    [KVS_MY_MASTER_RDMA_CONNECTING] = kvs_my_master_rdma_connecting,
    [KVS_MY_MASTER_RECEIVING_RDB] = kvs_my_master_receiving_rdb_handler,
    [KVS_MY_MASTER_RECEIVING_REPLICATION] = NULL,
    [KVS_MY_MASTER_ONLINE] = NULL,
};

kvs_status_t kvs_slave_master_state_machine_tick(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(slave == NULL || conn == NULL) {
        if(slave == NULL) {
            LOG_FATAL("slave is NULL in master state machine tick");
        }
        if(conn == NULL) {
            LOG_FATAL("master_conn is NULL in master state machine tick");
        }
        assert(0);
        return KVS_ERR;
    }

    struct kvs_my_master_context_s* master_ctx = (struct kvs_my_master_context_s*)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }

    if(trigger == KVS_EVENT_RDMA_ESTABLISHED) {
        LOG_DEBUG("RDMA connection established event received in master state machine tick");
    }

    kvs_status_t ret;

    do {
        LOG_DEBUG("Master state machine tick, current state: %d, trigger: %d", master_ctx->state, trigger);
        kvs_my_master_state_t state = master_ctx->state;
        ret = kvs_repl_master_state_handlers[state](slave, conn, trigger);
        if(ret == KVS_ERR) {
            LOG_ERROR("Error in master state handler for state %d", state);
            assert(0);
            return KVS_ERR;
        }
        // continue if state changed
    } while(ret == KVS_STATUS_CONTINUE);

    return KVS_OK;
}


// io_uring 的回调函数
void _on_master_connect_event(void* ctx, int res, int flags) {
    LOG_DEBUG("Master connect event triggered with res: %d, flags: %d", res, flags);
    struct kvs_conn_s *conn = (struct kvs_conn_s *)ctx;
    struct kvs_server_s *server = (struct kvs_server_s *)conn->server_ctx;
    struct kvs_slave_s *slave = (struct kvs_slave_s *)server->slave;
    if(server->role != KVS_SERVER_ROLE_SLAVE) {
        LOG_FATAL("server role is not SLAVE in master connect event");
        assert(0);
        return;
    }
    if(conn == NULL || conn->header.user_data == NULL) {
        LOG_FATAL("conn or conn->header.user_data is NULL in master connect event");
        assert(0);
        return;
    }
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->header.user_data;
    if(master_ctx->header.type != KVS_CTX_MASTER_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", master_ctx->header.type);
        assert(0); 
    }
    if(res < 0) {
        // 处理错误
        LOG_ERROR("Error on master event: %d, %s", res, strerror(-res));
        assert(0);
        return;
    }
    int state = master_ctx->state;
    int events = res; // POLLIN, POLLOUT, etc.
    if(events & POLL_OUT) {
        // 连接成功
        kvs_status_t ret = kvs_repl_master_state_handlers[state](slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_WRITE_DONE);
        if(ret == KVS_ERR) {
            LOG_ERROR("Error in master state handler for state %d", state);
            assert(0);
            return;
        }
    } else {
        LOG_ERROR("Unexpected event type: %d", events);
        assert(0);
        return;
    }
   


#if 0
    switch (link->state) {
        case REP_STATE_CONNECTING:
            if (events & POLLOUT) {
                // 1. 检查连接是否真的成功 (非阻塞 connect 的标准做法)
                int err = 0;
                socklen_t len = sizeof(err);
                getsockopt(link->fd, SOL_SOCKET, SO_ERROR, &err, &len);
                if (err != 0) {
                    // 连接失败，重试...
                    return;
                }

                // 2. 连接成功！状态跃迁
                printf("Connected to Master via TCP.\n");
                link->state = REP_STATE_SENDING_SYNC;

                // 3. 【核心回答】如何发 SYNC？
                // 直接在这里构造发送请求！
                char *cmd = "SYNC\r\n";
                // 使用 io_uring 的异步发送接口
                // 注意：这里要把监听事件改为 POLLIN (等待回复)，或者先 Send 再 wait POLLIN
                kvs_loop_send(server.loop, link->fd, cmd, strlen(cmd), link);
                
                // 此时，你不需要立刻把事件改为 POLLIN，
                // 等 send 完成的回调里（或者 send 提交后），再把 interest 改为 POLLIN
            }
            break;

        case REP_STATE_SENDING_SYNC:
            if (events & POLLOUT) { // 假设这是 send 完成的通知
                 // SYNC 发送完毕，准备接收回复
                 link->state = REP_STATE_WAITING_RES;
                 // 修改 io_uring 监听，只关心 POLLIN (可读)
                 kvs_loop_mod_poll(server.loop, link->fd, POLLIN, link);
            }
            break;
            
        case REP_STATE_WAITING_RES:
            if (events & POLLIN) {
                // 收到 Master 的回复了！
                char buf[1024];
                int n = read(link->fd, buf, sizeof(buf)); // 或者用 io_uring_prep_recv
                
                // 解析 +FULLRESYNC
                if (strncmp(buf, "+FULLRESYNC", 11) == 0) {
                    // 解析 token, port...
                    // 启动 RDMA
                    link->state = REP_STATE_RDMA_CONNECTING;
                    start_rdma_connect(link, token, port);
                }
            }
            break;
    }
#endif
}


// 这是一个非阻塞函数，调用完立即返回
void slave_start_replication(struct kvs_slave_s *slave) {
    // 1. 创建 socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd <= 0) {
        perror("socket");
        assert(0);
        return;
    }
    // todo：【关键】设为非阻塞！
    //fcntl(fd, F_SETFL, O_NONBLOCK);  

    struct sockaddr_in addr = {
        .sin_addr.s_addr = inet_addr(slave->master_ip),
        .sin_family = AF_INET,
        .sin_port = htons(slave->master_port),
    };

    slave->master_fd = fd;
    
    // 2. 发起连接
    int ret = connect(fd, (struct sockaddr*)&addr, sizeof(addr));

    struct kvs_conn_s *master_conn = NULL;
    kvs_net_register_fd(&slave->server->network, slave->master_fd, &master_conn);
    LOG_DEBUG("Registered master fd %d to network , connect %p", master_conn->_internal.fd, master_conn);
    kvs_server_create_conn_type(slave->server, (struct kvs_conn_header_s*)master_conn, KVS_CTX_MASTER_OF_ME);     
    struct kvs_my_master_context_s *ctx = (struct kvs_my_master_context_s*)master_conn->header.user_data;
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL after register fd");
        assert(0);
        return;
    }
    LOG_DEBUG("Registered master connection, fd: %d", slave->master_fd);
    // 3. 处理结果
    if (ret == 0) {
        // 极少见：瞬间连上了（本机连本机可能发生）
        // 直接进入发送 SYNC 状态
        ctx->state = KVS_MY_MASTER_SENDING_SYNC;
        // 直接调用状态机处理发送 SYNC
        
        kvs_slave_master_state_machine_tick(slave, (struct kvs_conn_header_s *)master_conn, KVS_EVENT_WRITE_DONE);
        LOG_DEBUG("Connected to Master immediately via TCP.");
    } else if (errno == EINPROGRESS) {
        // 【这是常态】：正在连接中
        // 我们需要把这个 fd 扔进 io_uring，监听 POLLOUT (可写 = 连接成功)
        if(ctx == NULL) {
            LOG_FATAL("master_ctx is NULL after register fd");
            assert(0);
            return;
        }
        ctx->state = KVS_MY_MASTER_CONNECTING;
        
        // 注册到 io_uring，监听 POLLOUT
        // kvs_loop_add_poll_out(&slave->server->network.loop, fd, ctx, _on_master_ctx_event); 
        ctx->connect_ev.ctx = (void*)master_conn;
        ctx->connect_ev.fd = slave->master_fd;
        ctx->connect_ev.type = KVS_EV_POLL_OUT;
        ctx->connect_ev.handler  = _on_master_connect_event;
        kvs_loop_add_poll_out(&slave->server->loop, &ctx->connect_ev);
        LOG_DEBUG("Connecting to Master via TCP (in progress)...");
    } else {
        perror("Connect failed immediately");
        assert(0);
        kvs_server_destroy_conn_type(slave->server, (struct kvs_conn_header_s*)master_conn);
        return; 
    }
}

// // 在 main() 里的调用位置：
// int main() {
//     // ... init ...
//     if (settings.role == SLAVE) {
//         // 这一步只是埋下一颗种子
//         slave_start_replication(settings.master_ip, settings.master_port);
//     }
//     // 此时连接还没通，但 fd 已经在 io_uring 里排队了
//     kvs_loop_run(); 
// }

kvs_status_t _my_master_cmd_logic(struct kvs_conn_s *conn, struct kvs_handler_cmd_s *cmd) {


}

kvs_status_t kvs_my_master_on_recv(struct kvs_conn_s *conn, int *read_size) {
    
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    LOG_DEBUG("Received data from Master fd %d, recv size:%d", master_conn->_internal.fd, master_conn->r_idx);
    struct kvs_server_s *server = (struct kvs_server_s *)conn->server_ctx;
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)master_conn->header.user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    if(master_ctx->header.type != KVS_CTX_MASTER_OF_ME) {
        LOG_FATAL("invalid ctx type: %d", master_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }

    return kvs_slave_master_state_machine_tick(server->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_READ_READY);
    // if(master_ctx->state == KVS_MY_MASTER_ONLINE)
    // kvs_server_msg_pump(conn, read_size, _my_master_cmd_logic);
}

kvs_status_t kvs_my_master_on_send(struct kvs_conn_s *conn, int bytes_sent) {
    // if (conn->type == KVS_CONN_TCP) {
    //     kvs_net_set_recv_event(conn);
    // }
    kvs_slave_master_state_machine_tick(((struct kvs_server_s *)conn->server_ctx)->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_WRITE_DONE);
    return KVS_OK;
}

void kvs_my_master_on_close(struct kvs_conn_s *conn) {
    struct kvs_server_s *server = conn->server_ctx;
    // master connection
    if(server->role != KVS_SERVER_ROLE_SLAVE) {
        LOG_FATAL("server is not slave, can not have master connection");
        assert(0);
        return;
    }
    LOG_DEBUG("master connection disconnected, fd: %d\n", conn->_internal.fd);
    assert(0);
}

kvs_status_t kvs_my_master_on_rdma_send(struct kvs_rdma_conn_s *conn,  size_t send_off_set, int send_len) {
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->header.user_data;

    return kvs_slave_master_state_machine_tick(master_ctx->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_WRITE_DONE);
}

kvs_status_t kvs_my_master_on_rdma_recv(struct kvs_rdma_conn_s *conn){
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->header.user_data;
    
    return kvs_slave_master_state_machine_tick(master_ctx->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_READ_READY);
} 