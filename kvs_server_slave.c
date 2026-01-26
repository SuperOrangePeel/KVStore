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


// int _kvs_slave_get_rdma_info(struct kvs_slave_s *slave, int *rdma_port_o, uint64_t *token_o, size_t *rdb_size_o) {

//     char response_buf[128];
//     ssize_t received = recv(slave->master_fd, response_buf, sizeof(response_buf), 0);
//     LOG_DEBUG("Received SYNC response from master: %.*s", (int)received, response_buf);
//     if (received <= 0) {
//         perror("recv SYNC response");
//         return -1;
//     }
//     int idx = 0;
//     if(response_buf[idx] != '+') {
//         printf("invalid SYNC response from master\n"); 
//         return -1;
//     }
//     idx ++;
//     if(strncmp(&response_buf[idx], "FULLRESYNC ", strlen("FULLRESYNC ")) != 0) {
//         printf("invalid SYNC response from master\n"); 
//         return -1;
//     }
//     idx += strlen("FULLRESYNC ");
//     int rdma_port = kvs_parse_int(response_buf, received, &idx);
//     if(rdma_port <= 0) {
//         printf("invalid RDMA port from master: %d\n", rdma_port);
//         return -1;
//     }
//     if(response_buf[idx] != ' ') {
//         printf("invalid SYNC response format from master\n"); 
//         return -1;
//     }
//     idx ++; 
//     uint64_t token = kvs_parse_uint64(response_buf, received, &idx);
//     if(token == 0) {
//         printf("invalid RDMA token from master: %lu\n", token);
//         return -1;
//     }
//     if(response_buf[idx] != ' ') {
//         printf("invalid SYNC response format from master\n");
//         return -1;
//     }
//     idx ++;
//     size_t rdb_size = kvs_parse_int(response_buf, received, &idx);
//     if(rdb_size <= 0) {
//         printf("invalid RDB size from master: %zu\n", rdb_size);
//         return -1;
//     }

//     *rdma_port_o = rdma_port;
//     *token_o = token;
//     *rdb_size_o = rdb_size;

//     return idx + 2; // skip \r\n
// }

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

    return idx + 2; // skip \r\n
}



// int _kvs_slave_sync_rdb_rdma(struct kvs_slave_s *slave) {
//     if(slave == NULL) return -1;

//     // send SYNC command to master
//     const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n";
//     ssize_t sent_bytes = send(slave->master_fd, sync_cmd, strlen(sync_cmd), 0);
//     if (sent_bytes < 0) {
//         perror("send SYNC");
//         return -1;
//     }

//     // +FULLRESYNC <rdma_port> <token> <rdb_size>\r\n
//     int rdma_port = 0;
//     uint64_t token = 0;
//     size_t rdb_size = 0;
//     _kvs_slave_get_rdma_info(slave, &rdma_port, &token, &rdb_size);

//     LOG_DEBUG("RDMA port: %d, token: %lu, RDB size: %zu\n", rdma_port, token, rdb_size);


//     return 0;

// }

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

// kvs_status_t kvs_slave_connect_master(struct kvs_slave_s *slave) {

//     if (NULL == slave) return KVS_ERR;

//     if(_init_connection(slave) < 0) {
//         return KVS_ERR;
//     }
//     LOG_DEBUG("Connected to master %s:%d, fd: %d", slave->master_ip, slave->master_port, slave->master_fd);
// #if 1
//     if(0 == _kvs_slave_sync_rdb_rdma(slave)) {
//         return KVS_OK;
//     } else {
//         return KVS_ERR;
//     }
// #else
//     if(0 == _kvs_slave_sync_rdb(slave)){
//         return KVS_OK;
//     } else {
//         return KVS_ERR;
//     }
// #endif
// }

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



static kvs_status_t _on_master_connected(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
   // 1. 错误处理
    // if(trigger != KVS_EVENT_READ_READY) {
    //     LOG_FATAL("invalid trigger: %d", trigger);
    //     return KVS_ERR;
    // }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in connecting handler");
        assert(0);
        return KVS_ERR; 
    }


    //2. 事件完成，处理事件
    if(trigger == KVS_EVENT_WRITE_DONE) {
        // check if connected successfully
        LOG_DEBUG("sync command sent to master.");

        //3. 状态转换
        master_ctx->state = KVS_MY_MASTER_WAIT_SYNC_RESPONSE;
        //4. 注册事件
        struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
        //assert(master_conn->r_idx == 0);
        kvs_net_set_recv_event(master_conn);
        return KVS_OK; // wait for read event
    } else if(trigger == KVS_EVENT_CONNECTED) {
        LOG_DEBUG("TCP connection to master established.");
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

    //3. 状态转换 不需要状态转换，直接发送SYNC命令，等待发送完成事件，再转换状态
    LOG_DEBUG("Connected to Master via TCP.");
    //master_ctx->state = KVS_MY_MATSTER_WAIT_SYNC_RESPONSE;

    //4. 注册事件
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    assert(master_conn->s_idx == 0);
    master_conn->s_idx = snprintf(master_conn->s_buffer, master_conn->s_buf_sz, "%s",  "*1\r\n$4\r\nSYNC\r\n");
    kvs_net_set_send_event_manual((struct kvs_conn_s *)master_conn);

    return KVS_OK; // wait for send completion event
}



static kvs_status_t _on_sync_response(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }


    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    // +FULLRESYNC <rdma_port> <token> <rdb_size>\r\n
    int rdma_port = 0;
    uint64_t token = 0;
    size_t rdb_size = 0;
    //_kvs_slave_get_rdma_info(slave, &rdma_port, &token, &rdb_size);
    int processed_len = _kvs_slave_get_rdma_info_raw_buffer(master_conn->r_buffer, master_conn->r_idx, &rdma_port, &token, &rdb_size);
    master_ctx->processed_sz_cur = processed_len;
    //_kvs_slave_rdma_connect_master(slave, rdma_port, token, rdb_size);
    
    LOG_DEBUG("RDMA port: %d, token: %lu, RDB size: %zu", rdma_port, token, rdb_size);

    master_ctx->rdma_port = rdma_port;
    master_ctx->token = token;
    master_ctx->rdb_size = rdb_size;
    master_ctx->header.conn = master_conn;
   
    //3. 状态转换
    master_ctx->state = KVS_MY_MASTER_WAIT_RDMA_ESTABLISHED;

    // 4. 注册事件

    struct kvs_rdma_conn_s *rdma_conn = NULL;
    kvs_rdma_post_connect(&slave->server->rdma_engine, slave->master_ip, 
        master_ctx->rdma_port, 
        &master_ctx->token, sizeof(master_ctx->token), &rdma_conn);
    kvs_server_share_conn_ctx(slave->server, conn, (struct kvs_conn_header_s*)rdma_conn);
    return KVS_OK; // wait for RDMA connection established event
}


static kvs_status_t _create_master_rdma_rdb_resources(struct kvs_slave_s *slave, struct kvs_my_master_context_s *master_ctx) {
    // allocate
    master_ctx->send_buf = (char *)kvs_malloc(slave->server->rdma_max_chunk_size);
    memset(master_ctx->send_buf, 0, slave->server->rdma_max_chunk_size);
    master_ctx->send_buf_sz = slave->server->rdma_max_chunk_size;
    master_ctx->send_mr = kvs_rdma_register_memory(&slave->server->rdma_engine, master_ctx->send_buf, slave->server->rdma_max_chunk_size, KVS_RDMA_OP_SEND);
    if(master_ctx->send_mr == NULL) {
        kvs_free(master_ctx->send_buf, slave->server->rdma_max_chunk_size);
        master_ctx->send_buf = NULL;
        LOG_FATAL("failed to register send MR");
        return KVS_ERR;
    }


    // allocate RDB recv MR
    int rdb_max_chunk_size = slave->server->rdma_max_chunk_size;
    char *rdb_recv_buffer = (char *)kvs_malloc(rdb_max_chunk_size * slave->rdb_recv_buffer_count);
    LOG_DEBUG("Allocating RDB recv buffer: %zu bytes (%d chunks of %d bytes)", rdb_max_chunk_size * slave->rdb_recv_buffer_count, slave->rdb_recv_buffer_count, rdb_max_chunk_size);
    master_ctx->rdb_recv_mr = kvs_rdma_register_memory(&slave->server->rdma_engine, rdb_recv_buffer, rdb_max_chunk_size * slave->rdb_recv_buffer_count, KVS_RDMA_OP_RECV);
    if(master_ctx->rdb_recv_mr == NULL) {
        LOG_FATAL("failed to register RDB recv MR");
        return KVS_ERR;
    }  
    master_ctx->rdb_recv_buffer = rdb_recv_buffer;
    master_ctx->rdb_recv_buf_sz = rdb_max_chunk_size;
    master_ctx->rdb_recv_buffer_count = slave->rdb_recv_buffer_count;
    master_ctx->rdb_offset = 0;
    master_ctx->rdb_fd = open(slave->server->pers_ctx->rdb_filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if(master_ctx->rdb_fd < 0) {
        LOG_FATAL("failed to open RDB file for writing: %s", strerror(errno));
        return KVS_ERR;
    }
    // struct kvs_event_s *rdb_write_ev = &master_ctx->rdb_write_ev;

    // rdb_write_ev->fd = master_ctx->rdb_fd;
    // rdb_write_ev->handler = _kvs_my_master_handle_rdb_write_completion; // not used
    // rdb_write_ev->ctx = (void *)master_ctx;

    return KVS_OK; 

}

static kvs_status_t _destroy_master_rdma_rdb_resources(struct kvs_slave_s *slave, struct kvs_my_master_context_s *master_ctx) {
    if(master_ctx->send_mr != NULL) {
        kvs_rdma_deregister_memory(master_ctx->send_mr);
        master_ctx->send_mr = NULL;
    }
    if(master_ctx->send_buf != NULL) {
        kvs_free(master_ctx->send_buf, slave->server->rdma_max_chunk_size);
        master_ctx->send_buf = NULL;
    }
    if(master_ctx->rdb_recv_mr != NULL) {
        kvs_rdma_deregister_memory(master_ctx->rdb_recv_mr);
        master_ctx->rdb_recv_mr = NULL;
    }
    if(master_ctx->rdb_recv_buffer != NULL) {
        kvs_free(master_ctx->rdb_recv_buffer, slave->server->rdma_max_chunk_size * slave->rdb_recv_buffer_count);
        master_ctx->rdb_recv_buffer = NULL;
    }
    if(master_ctx->rdb_fd > 0) {
        close(master_ctx->rdb_fd);
        master_ctx->rdb_fd = -1;
    }
    return KVS_OK;
}


// ========================= rdma connect ==================
static kvs_status_t _on_rdma_established(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_WRITE_DONE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in rdma established handler");
        return KVS_ERR;
    }
    
    //2. 事件完成，处理事件
    struct kvs_rdma_conn_s *rdma_conn = (struct kvs_rdma_conn_s *)conn;
    master_ctx->rdma_conn = rdma_conn;
    _create_master_rdma_rdb_resources(slave, master_ctx);

    //3. 状态转换
    master_ctx->state = KVS_MY_MASTER_WAIT_SENT_READY;

    //4. 注册事件

    // 先注册多个RDMA recv，防止RECEIVER_NOT_READY错误
    for(int i = 0; i < master_ctx->rdb_recv_buffer_count; i++) {
        kvs_rdma_post_recv(rdma_conn, master_ctx->rdb_recv_mr, i * master_ctx->rdb_recv_buf_sz, master_ctx->rdb_recv_buf_sz, NULL);
    }

    int send_len = snprintf(master_ctx->send_buf, master_ctx->send_buf_sz, "READY\r\n");
    kvs_rdma_post_send(rdma_conn, master_ctx->send_mr, 0, 0,send_len, NULL);

    return KVS_OK;
}

static kvs_status_t _on_ready_ack_sent(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_WRITE_DONE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in ready ack sent handler");
        return KVS_ERR;
    }
    
    //2. 事件完成，处理事件
    LOG_DEBUG("READY ack sent to master, start receiving RDB via RDMA.");

    //3. 状态转换
    master_ctx->state = KVS_MY_MASTER_WAIT_RECV_RDB; 

    //4. 注册事件
    // RDMA recv already posted in _on_rdma_established

    return KVS_OK;
}

struct kvs_rdb_write_context_s {
    struct kvs_my_master_context_s *master_ctx;
    //char *recv_buffer; // registered buffer pointer
    //size_t length; //
    size_t offset; // rdb buffer offset
    int is_end; // is last chunk
};

static void _kvs_my_master_handle_rdb_fsync_completion(void* ctx, int res, int flags) {
    // 1. 错误处理
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in RDB fsync completion");
        return;
    }
    if(res < 0) {
        LOG_FATAL("RDB fsync failed: %d, %s", res, strerror(-res));
        return;
    }
    // 2. 事件完成，处理事件
    LOG_INFO("RDB fsync completed successfully");

    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)ctx;

    // 3. 状态转换
    master_ctx->state = KVS_MY_MASTER_WAIT_SENT_ACK;
    
    // 4. 注册事件
    char *rdma_send_buf = master_ctx->send_buf;
    int rdma_send_buf_sz = master_ctx->send_buf_sz;
    int send_len = snprintf(rdma_send_buf, rdma_send_buf_sz, "ACK_RDB\r\n");
    struct kvs_rdma_conn_s *master_conn = (struct kvs_rdma_conn_s *)master_ctx->rdma_conn;
    kvs_rdma_post_send(master_conn, master_ctx->send_mr, 0, 0, send_len, NULL);

    return; 
}


static void _kvs_my_master_rdb_fwrite_cb(void* ctx, int res, int flags) {
    if(ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in RDB write completion");
        assert(0);
        return;
    }
    struct kvs_rdb_write_context_s *rdb_write_ctx = (struct kvs_rdb_write_context_s *)ctx;
    struct kvs_my_master_context_s *master_ctx = rdb_write_ctx->master_ctx;
    //char *recv_buffer = rdb_write_ctx->recv_buffer;
    int write_offset = rdb_write_ctx->offset;
    //int write_len = rdb_write_ctx->length;
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
    
    struct kvs_rdma_conn_s *master_conn = (struct kvs_rdma_conn_s *)master_ctx->rdma_conn;
    if(master_conn == NULL) {
        LOG_FATAL("master_conn is NULL in RDB write completion");
        return;
    }

    kvs_rdma_post_recv(master_conn, master_ctx->rdb_recv_mr, write_offset, master_ctx->rdb_recv_buf_sz, NULL);

    // post more RDMA recv
}

static kvs_status_t _on_recv_rdb(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || slave == NULL || conn == NULL) {
        LOG_FATAL("invalid conn type: %d or slave == NULL is %d or conn == NULL is %d", conn->type, slave == NULL, conn == NULL);
        return KVS_ERR;
    }
    //struct kvs_rdma_conn_s *master_conn = (struct kvs_rdma_conn_s *)conn;
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in receiving rdb handler");
        assert(0);
        return KVS_ERR; 
    }
    
    int write_offset = master_ctx->offset_cur; // = i * master_ctx->rdb_recv_buf_sz;
    int write_len = master_ctx->recv_len_cur; // = received_len;
    int imm_data = master_ctx->imm_data_cur; // = imm_data;
    LOG_DEBUG("RDB chunk received via RDMA: offset %d, length %d, imm_data %d", write_offset, write_len, imm_data);
    // imm_data没发过来，不知道为什么？
    
    struct kvs_event_s *rdb_write_ev = &master_ctx->rdb_write_ev;
    LOG_DEBUG("RECEIVED RDB chunk: offset %d, length %d, imm_data %d", write_offset, write_len, imm_data);
    
    struct kvs_rdb_write_context_s *rdb_write_ctx = (struct kvs_rdb_write_context_s *)kvs_malloc(sizeof(struct kvs_rdb_write_context_s));
    rdb_write_ctx->master_ctx = master_ctx;
    //rdb_write_ctx->recv_buffer = master_ctx->rdb_recv_buffer; // registered buffer pointer
    //rdb_write_ctx->length = write_len;
    rdb_write_ctx->offset = write_offset;
    //rdb_write_ctx->is_end = (imm_data == -1) ? 1 : 0;
    rdb_write_ctx->is_end = (master_ctx->rdb_offset + write_len >= master_ctx->rdb_size) ? 1 : 0;
    rdb_write_ev->ctx = (void *)rdb_write_ctx;
    
    mem_hexdump(master_ctx->rdb_recv_buffer + write_offset, write_len, "RDB chunk data dump:");
    // 如果没有赋值fd，就赋值
    if(rdb_write_ev->fd != master_ctx->rdb_fd) {
        rdb_write_ev->fd = master_ctx->rdb_fd;
        rdb_write_ev->handler = _kvs_my_master_rdb_fwrite_cb; // not used
        
    }
    
    kvs_loop_add_write(&slave->server->loop, rdb_write_ev, master_ctx->rdb_recv_buffer + write_offset, write_len);

    return KVS_OK;
}

static kvs_status_t _on_rdma_sent_rdb_ack(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    // 1. 错误处理
    if(trigger != KVS_EVENT_WRITE_DONE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_RDMA || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in sent rdb ack handler");
        return KVS_ERR;
    }

    LOG_DEBUG("RDB ACK sent to master, starting to load RDB into memory.");
    //  加载RDB文件到内存
    kvs_server_load_rdb(slave->server);

    _destroy_master_rdma_rdb_resources(slave, master_ctx); // 此时RDB已经加载到内存，可以释放RDB和rdma相关资源，master收到+RDBLOADED\r\n会关闭RDMA连接

    //3. 状态转换
    master_ctx->state = KVS_MY_MASTER_WAIT_SENT_RDB_LOADED_ACK;
    //4. 注册事件
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)master_ctx->header.conn;
    master_conn->s_idx += snprintf(master_conn->s_buffer, 
        master_conn->s_buf_sz, "+RDBLOADED\r\n");
    kvs_net_set_send_event_manual(master_conn);
    return KVS_OK; // wait for read event
}

static kvs_status_t _on_rdma_sent_rdb_loaded_ack(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    //1. 错误处理
    if(trigger != KVS_EVENT_WRITE_DONE) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    if(master_ctx == NULL) {
        LOG_FATAL("master_ctx is NULL in sent rdb loaded ack handler");
        return KVS_ERR;
    }

    LOG_DEBUG("RDB LOADED ACK sent to master, entering ONLINE state.");

    //3. 状态转换
    master_ctx->state = KVS_MY_MASTER_ONLINE;

    //4. 注册事件
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    kvs_net_set_recv_event(master_conn);
    return KVS_OK;
}

static kvs_status_t _kvs_slave_cmd_logic(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL || conn == NULL) {
        LOG_FATAL("server == NULL is %d, cmd == NULL is %d, conn == NULL is %d", server == NULL, cmd == NULL, conn == NULL);
        return KVS_ERR;
    }

    struct kvs_protocol_s *protocol = &server->protocol;
    if(protocol == NULL) {
        LOG_FATAL("protocol is NULL in cmd logic");
        return KVS_ERR;
    }
    //if(cmd->cmd_type == KVS_CMD_READ)
    LOG_DEBUG("Received replication command from master: %.*s", (int)cmd->len_cmd, cmd->cmd);
    // process command
    kvs_result_t result = protocol->execute_command(server, cmd, conn);
    

    return KVS_OK;
}

static kvs_status_t _on_master_online(struct kvs_slave_s *slave, struct kvs_conn_header_s *conn, kvs_event_trigger_t trigger) {
    //1. 错误处理
    if(trigger != KVS_EVENT_READ_READY) {
        LOG_FATAL("invalid trigger: %d", trigger);
        return KVS_ERR;
    }
    if(conn->type != KVS_CONN_TCP || conn->user_data == NULL || slave == NULL) {
        LOG_FATAL("invalid conn type: %d or conn->user_data == NULL is %d or slave == NULL is %d", conn->type, conn->user_data == NULL, slave == NULL);
        return KVS_ERR;
    }

    LOG_DEBUG("starting to receive replication commands from master.");

    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)conn;
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->user_data;
    master_ctx->processed_sz_cur = 0;
    kvs_server_msg_pump(master_conn, &master_ctx->processed_sz_cur, _kvs_slave_cmd_logic);

}



typedef kvs_status_t (*kvs_repl_master_state_handler_t)(struct kvs_slave_s *slave, struct kvs_conn_header_s *master_conn, kvs_event_trigger_t trigger);

kvs_repl_master_state_handler_t kvs_repl_master_state_handlers[] = {
    [KVS_MY_MASTER_NONE] = NULL,
    [KVS_MY_MASTER_WAIT_CONNECTED] = _on_master_connected,
    //[] = kvs_my_master_sending_sync_handler,
    [KVS_MY_MASTER_WAIT_SYNC_RESPONSE] = _on_sync_response,
    [KVS_MY_MASTER_WAIT_RDMA_ESTABLISHED] = _on_rdma_established,
    [KVS_MY_MASTER_WAIT_SENT_READY] = _on_ready_ack_sent,
    [KVS_MY_MASTER_WAIT_RECV_RDB] = _on_recv_rdb,
    [KVS_MY_MASTER_WAIT_SENT_ACK] = _on_rdma_sent_rdb_ack,
    [KVS_MY_MASTER_WAIT_SENT_RDB_LOADED_ACK] = _on_rdma_sent_rdb_loaded_ack,
    //[KVS_MY_MASTER_WAIT_RECV_REPL] = _on_recv_repl, // 直接在online里接replication command
    [KVS_MY_MASTER_ONLINE] = _on_master_online,
};

/*
 * KVS_MY_MASTER_WAIT_CONNECTED -> on_connected
 * KVS_MY_MASTER_WAIT_SYNC_RESPONSE -> on_sync_response
 * KVS_MY_MASTER_WAIT_RDMA_ESTABLISHED -> on_rdma_established
 * KVS_MY_MASTER_WAIT_SENT_READY -> on_sent_ready
 * KVS_MY_MASTER_WAIT_RECV_RDB -> on_recv_rdb
 * KVS_MY_MASTER_WAIT_SENT_ACK -> on_sent_ack
 * KVS_MY_MASTER_WAIT_RECV_REPL -> on_recv_repl
 * KVS_MY_MASTER_ONLINE -> online
 */

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
    struct kvs_conn_s *master_conn = (struct kvs_conn_s *)ctx;
    if(master_conn == NULL) {
        LOG_FATAL("master_conn is NULL in connect event");
        assert(0);
        return;
    }
    struct kvs_slave_s *slave = (struct kvs_slave_s *)master_conn->server_ctx;
    if(slave == NULL) {
        LOG_FATAL("slave is NULL in connect event");
        assert(0);
        return;
    }
    LOG_DEBUG("Master connect event: res=%d, flags=%d", res, flags);
    kvs_slave_master_state_machine_tick(slave, (struct kvs_conn_header_s *)master_conn, KVS_EVENT_CONNECTED);
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
    kvs_server_create_conn_ctx(slave->server, (struct kvs_conn_header_s*)master_conn, KVS_CTX_MASTER_OF_ME);     
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
        ctx->state = KVS_MY_MASTER_WAIT_CONNECTED;
        // 直接调用状态机处理发送 SYNC
        
        kvs_slave_master_state_machine_tick(slave, (struct kvs_conn_header_s *)master_conn, KVS_EVENT_CONNECTED);
        LOG_DEBUG("Connected to Master immediately via TCP.");
    } else if (errno == EINPROGRESS) {
        // 【这是常态】：正在连接中
        // 我们需要把这个 fd 扔进 io_uring，监听 POLLOUT (可写 = 连接成功)
        if(ctx == NULL) {
            LOG_FATAL("master_ctx is NULL after register fd");
            assert(0);
            return;
        }
        ctx->state = KVS_MY_MASTER_WAIT_CONNECTED;
        
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
        kvs_server_destroy_conn_ctx(slave->server, (struct kvs_conn_header_s*)master_conn);
        return; 
    }
}

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

    kvs_slave_master_state_machine_tick(server->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_READ_READY);

    *read_size = master_ctx->processed_sz_cur;
    return KVS_OK;
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

    master_ctx->offset_cur = send_off_set;
    master_ctx->sent_len_cur = send_len;

    return kvs_slave_master_state_machine_tick(master_ctx->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_WRITE_DONE);
}

kvs_status_t kvs_my_master_on_rdma_recv(struct kvs_rdma_conn_s *conn, size_t recv_off_set, int recv_len, int imm_data) {
    struct kvs_my_master_context_s *master_ctx = (struct kvs_my_master_context_s *)conn->header.user_data;
    master_ctx->imm_data_cur = imm_data;
    master_ctx->offset_cur = recv_off_set;
    master_ctx->recv_len_cur = recv_len;

    return kvs_slave_master_state_machine_tick(master_ctx->slave, (struct kvs_conn_header_s *)conn, KVS_EVENT_READ_READY);
} 