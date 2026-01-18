#include "kvs_server.h"

#include "kvs_persistence.h"
#include "common.h"
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

    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }
    slave->master_fd = sockfd;
    return sockfd;
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

    if(0 == _kvs_slave_sync_rdb(slave)){
        return KVS_OK;
    } else {
        return KVS_ERR;
    }
}

kvs_status_t kvs_slave_init(struct kvs_slave_s *slave, struct kvs_server_s *server , struct kvs_slave_config_s *config) {
    if(slave == NULL || config == NULL) return KVS_ERR;

    memset(slave, 0, sizeof(struct kvs_slave_s));
    slave->master_fd = -1;
    strncpy(slave->master_ip, config->master_ip, strlen(config->master_ip));
    slave->master_port = config->master_port;
    slave->state = KVS_REPL_MASTER_NONE;
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