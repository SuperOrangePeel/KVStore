#include "kvs_network.h"
#include "common.h"
#include "kvs_rdma_engine.h"
#include "kvs_types.h"
#include "kvs_event_loop.h"
#include "logger.h"

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <assert.h>

static inline void _kvs_net_create_buffer(struct kvs_network_s *network, struct kvs_conn_s *conn) {
    if(conn == NULL) return;
    if(conn->r_buffer != NULL || conn->s_buffer != NULL) {
        conn->r_buf_sz = network->read_buffer_size;
        conn->r_idx = 0;
        conn->s_buf_sz = network->write_buffer_size;
        conn->s_idx = 0;
        
        conn->raw_buf_sent_sz = 0;
        // already initialized
        return;
    }
    conn->r_buffer = (char *)kvs_malloc(network->read_buffer_size); // 4KB read buffer
    conn->r_buf_sz = network->read_buffer_size;
    conn->r_idx = 0;
    
    conn->s_buffer = (char *)kvs_malloc(network->write_buffer_size); // 1MB write buffer
    conn->s_buf_sz = network->write_buffer_size;
    conn->s_idx = 0;
    
    conn->raw_buf_sent_sz = 0;
}

static inline void _kvs_net_destroy_buffer(struct kvs_network_s *network, struct kvs_conn_s *conn) {
    conn->r_buf_sz = 0;
    conn->r_idx = 0;
    if(conn->r_buffer) {
        kvs_free(conn->r_buffer, network->read_buffer_size);
        conn->r_buffer = NULL;
    };

    conn->s_buf_sz = 0;
    conn->s_idx = 0;
    if(conn->s_buffer) {
        kvs_free(conn->s_buffer, network->write_buffer_size);
        conn->s_buffer = NULL;
    }
    conn->raw_buf_sent_sz = 0;
}

kvs_status_t kvs_net_get_free_conn(struct kvs_network_s *net, struct kvs_conn_s **out_conn) {
    if(net->free_conn_stack_top < 0) {
        LOG_ERROR("No free connection available");
        return KVS_QUIT;
    }

    int conn_idx = net->free_conn_stack[net->free_conn_stack_top];
    net->free_conn_stack_top --;
    struct kvs_conn_s *conn = &net->conn_pool[conn_idx];
    *out_conn = conn;
    return KVS_OK;
}

static inline int _kvs_net_get_conn_idx(struct kvs_network_s *net, struct kvs_conn_s *conn) {
    if (conn < net->conn_pool || conn >= (net->conn_pool + net->max_conns)) {
        assert(0);
        return -1; // 或者断言失败
    }

    // 2. 指针算术
    return (int)(conn - net->conn_pool);
}

kvs_status_t kvs_net_release_conn(struct kvs_network_s *net, struct kvs_conn_s *conn) {
    if(net->free_conn_stack_top >= net->max_conns - 1) {
        LOG_ERROR("Free connection stack overflow");
        assert(0); // should not happen
        return KVS_ERR;
    }

    int conn_idx = _kvs_net_get_conn_idx(net, conn);
    net->free_conn_stack_top ++;
    net->free_conn_stack[net->free_conn_stack_top] = conn_idx;
    if(net)
    return KVS_OK;
}


static inline void _kvs_net_deinit_conn(struct kvs_network_s *net, struct kvs_conn_s *conn) {
    // 不free buffer，留给下一次复用
    if(conn == NULL) {
        LOG_FATAL("_kvs_net_deinit_conn: conn is NULL");
        assert(0);
        return;
    }

    // 如果连接的fd是最大的，更新 conn_num
   // net->conn_num = conn->_internal.fd + 1 == net->conn_num ? net->conn_num - 1 : net->conn_num;


    // 不要设置为0，没有意义。。。还会导致不明事件。。
    //conn->r_buf_sz = 0;
    //conn->s_buf_sz = 0;
    conn->r_idx = 0;
    conn->s_idx = 0;
    conn->raw_buf_sent_sz = 0;
    conn->_internal.fd = -1;
    //conn->_internal.net = NULL;
    conn->_internal.is_reading = 0;
    conn->_internal.is_writing = 0;
    conn->_internal.is_closed = 1;
    // kvs_loop_cancel_event(&conn->_internal.net->loop, &conn->_internal.read_ev);
    // kvs_loop_cancel_event(&conn->_internal.net->loop, &conn->_internal.write_ev);
}

static void _net_on_write(void *ctx, int res, int flags) {
    struct kvs_conn_s *conn = ctx;
    conn->_internal.is_writing = 0;

    if (res < 0) {
        if(conn->_internal.is_closed) {
            return; // already closed
        }
        // 关闭逻辑：归还连接池，从 Loop 注销
        conn->_internal.net->on_close(conn); // notify upper layer
        LOG_DEBUG("closing connection fd: %d", conn->_internal.fd);
        close(conn->_internal.fd);
        _kvs_net_deinit_conn(conn->_internal.net, conn);
        kvs_net_release_conn(conn->_internal.net, conn);
        return;
    }
    // LOG_DEBUG("write completed, bytes sent: %d, msg left: [%.s]", res, conn->s_idx, conn);
    conn->s_idx -= res;
    
    if (conn->s_idx > 0) {
        // 还有剩余数据未发送，继续发送
        memmove(conn->s_buffer, conn->s_buffer + res, conn->s_idx);
        //kvs_loop_add_send(&conn->_internal.net->loop, &conn->_internal.write_ev, conn->s_buffer, conn->s_idx);
        kvs_net_set_send_event_manual(conn);
    } else {
        // 全部发送完毕，调用业务层回调
        if (conn->_internal.net->on_send) {
            conn->_internal.net->on_send(conn, res);
        }
    }
}

// 内部回调：处理 Read
static void _net_on_read(void *ctx, int res, int flags) {
    //LOG_DEBUG("_net_on_read called, res: %d", res);
    struct kvs_conn_s *conn = (struct kvs_conn_s*)ctx;
    if(conn == NULL) {
        LOG_FATAL("_net_on_read: conn is NULL");
        assert(0);
        return;
    }
    conn->_internal.is_reading = 0;
    if (res <= 0) {
        
        // 关闭逻辑：归还连接池，从 Loop 注销
        if(conn->_internal.is_closed) {
            return; // already closed
        }
        if(conn->_internal.net->on_close == NULL) {
            LOG_FATAL("on_close callback is NULL");
            assert(0);
        }
        LOG_DEBUG("Read error or connection closed, res: %d:%s, closing fd: %d", res, strerror(-res), conn->_internal.fd);
        conn->_internal.net->on_close(conn); // notify upper layer
        //LOG_DEBUG("read error ", conn->_internal.)
        //LOG_DEBUG("closing connection fd: %d, res: %d", conn->_internal.fd, res);
        close(conn->_internal.fd);
        _kvs_net_deinit_conn(conn->_internal.net, conn);
        kvs_net_release_conn(conn->_internal.net, conn);
        return;
    }
    
    // 更新 buffer 指针
    conn->r_idx += res;
    assert(conn->r_idx <= conn->r_buf_sz && conn->r_idx >= 0);
    // 【关键】调用业务层 (L1)
    if (conn->_internal.net->on_msg) {
        int read_size = 0;
        // LOG_DEBUG("on_msg before");
        if(KVS_QUIT == conn->_internal.net->on_msg(conn, &read_size)) {
            //todo: close connection
        }
        int remain_size = conn->r_idx - read_size; 
        if(remain_size < 0) {
            LOG_FATAL("remain_size < 0, read_size: %d, r_idx: %d", read_size, conn->r_idx);
            assert(0);
            remain_size = 0;
        }
        if(remain_size > 0) {
            // 有剩余数据，搬移到缓冲区头部
            memmove(conn->r_buffer, conn->r_buffer + read_size, remain_size);
            // if(conn->r_buffer[0] != '*') {
            //     LOG_DEBUG("read_size:%d", read_size);
            //     LOG_FATAL("after on_msg, invalid protocol: no '*', first byte: %c", conn->r_buffer[0]);
            //     assert(0);
            // }
        }
        conn->r_idx = remain_size;
    }
    
    // 如果没关连接，继续注册读
    //kvs_loop_add_read(&conn->_internal.net->loop, &conn->_internal.read_ev, conn->r_buffer + conn->r_idx, conn->r_buf_sz - conn->r_idx);
    kvs_net_set_recv_event(conn);
}

static inline void _kvs_net_init_conn(struct kvs_network_s *net, struct kvs_conn_s *conn, int client_fd) {
    if(conn == NULL || net == NULL) return;
    conn->_internal.fd = client_fd;
    conn->_internal.net = net;
    conn->_internal.is_closed = 0;
    conn->_internal.is_reading = 0;
    conn->_internal.is_writing = 0;
    _kvs_net_create_buffer(net, conn);

    conn->header.user_data = NULL; // 业务层可以自行设置
    conn->server_ctx = net->server_ctx;
    conn->header.type = KVS_CONN_TCP;
    //conn->_internal.related_conn = NULL;

    //net->conn_num ++

    // 注册事件回调
    kvs_event_init(&conn->_internal.read_ev, client_fd, KVS_EV_READ, _net_on_read, conn);
    kvs_event_init(&conn->_internal.write_ev, client_fd,  KVS_EV_WRITE, _net_on_write, conn);
}

// 内部回调：处理 Accept
static void _net_on_accept(void *ctx, int res, int flags) {
    struct kvs_network_s  *net = ctx;
    
    // 1. 从 Loop 拿到了新的 fd
    int client_fd = res;
    if (client_fd < 0) return; // Error handling
    if(client_fd >= net->max_conns) {
        LOG_ERROR("accepted fd %d exceeds max_conns %d", client_fd, net->max_conns);
        close(client_fd);
        // 重新提交 accept
        kvs_loop_add_accept(net->loop, &net->accept_ev, 
            (struct sockaddr*)&net->client_addr, &net->client_addrlen);
        return;
    }
    
    // 2. 从连接池拿一个空闲对象 (资源管理)
    struct kvs_conn_s *conn = NULL;
    kvs_net_get_free_conn(net, &conn);
    if(conn == NULL) {
        LOG_ERROR("No free connection available, closing accepted fd %d", client_fd);
        close(client_fd);
        // 重新提交 accept
        kvs_loop_add_accept(net->loop, &net->accept_ev, 
            (struct sockaddr*)&net->client_addr, &net->client_addrlen);
        return;
    }

    conn->header.type = KVS_CONN_TCP;

    _kvs_net_init_conn(net, conn, client_fd);
    //conn->type = KVS_CONN_TCP;


    net->on_accept(conn); 
   
    // 4. 告诉 Loop：开始监听读
    kvs_net_set_recv_event(conn);

    // 5. Re-arm accept (io_uring 特性，需重新提交 accept)
    kvs_loop_add_accept(net->loop, &net->accept_ev, 
        (struct sockaddr*)&net->client_addr, &net->client_addrlen);
}



int _kvs_net_init_server(const char *ip, unsigned short port) {
    // 创建 socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    // 设置地址复用
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 绑定地址和端口
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    //server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_addr.s_addr = inet_addr(ip);
    server_addr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(sockfd);
        return -1;
    }

    // 开始监听
    if (listen(sockfd, SOMAXCONN) < 0) {
        perror("listen");
        close(sockfd);
        return -1;
    }

    return sockfd;
}

// int _kvs_net_init_rdma(struct kvs_network_s *net, struct kvs_network_config_s *conf) {
//     // 初始化 RDMA 引擎
//     net->use_rdma = conf->use_rdma;
//     if (net->use_rdma) {
//         struct kvs_rdma_config_s rdma_config = {
//             .server_ip = conf->server_ip,
//             .server_port = conf->rdma_port,
//             .cq_size = conf->cq_size,
//             .max_recv_wr = conf->max_recv_wr,
//             .max_send_wr = conf->max_send_wr,
//             .max_sge = conf->max_sge,
//             .global_ctx = net,
//             .callbacks.on_connect_request = kvs_net_on_rdma_connect_request,
//             .callbacks.on_completion = kvs_net_on_rdma_completion, // 用户可自行设置回调
//             .callbacks.on_established = kvs_net_on_rdma_established,
//             .callbacks.on_disconnected = kvs_net_on_rdma_disconnected,
//             .callbacks.on_error = NULL,
//         }; 
//         kvs_rdma_init_engine(&net->rdma_engine, net->loop, &rdma_config);
//     }
//     return 0;
// }

// 初始化函数
kvs_status_t kvs_net_init(struct kvs_network_s  *net, struct kvs_network_config_s *conf) {
    // 1. 初始化 Loop
    //kvs_loop_init(&net->loop, conf->io_uring_entries);
    net->loop = conf->loop;
    // 2. 分配连接池
    net->max_conns = conf->max_conns;
    net->conn_pool = kvs_malloc(net->max_conns * sizeof(struct kvs_conn_s));
    memset(net->conn_pool, 0, net->max_conns * sizeof(struct kvs_conn_s));
    net->free_conn_stack = kvs_malloc(net->max_conns * sizeof(int));
    memset(net->free_conn_stack, 0, net->max_conns * sizeof(int));
    net->free_conn_stack_top = net->max_conns - 1;
    for(int i=0; i<net->max_conns; i++) {
        net->free_conn_stack[i] = net->max_conns - 1 - i;
        net->conn_pool[i]._internal.fd = -1; // 初始化为无效值
    }

    net->read_buffer_size = conf->read_buffer_size > 0 ? conf->read_buffer_size : KVS_READ_BUF_SZ_DEFAULT;
    net->write_buffer_size = conf->write_buffer_size > 0 ? conf->write_buffer_size : KVS_WRITE_BUF_SZ_DEFAULT;

    net->server_ctx = conf->server_ctx;
    net->on_accept = conf->on_accept;
    net->on_msg = conf->on_msg;
    net->on_send = conf->on_send;
    net->on_close = conf->on_close;
    
    // 3. 启动监听 Socket
    net->server_fd = _kvs_net_init_server(conf->server_ip, conf->port_listen);
    if (net->server_fd < 0) {
        return -1;
    }
    
    // 4. 注册第一个事件：Accept
    kvs_event_init(&net->accept_ev, net->server_fd, KVS_EV_ACCEPT, _net_on_accept, net);
    kvs_loop_add_accept(net->loop, &net->accept_ev, (struct sockaddr*)&net->client_addr, &net->client_addrlen);
    // if(conf->use_rdma) {
    //     _kvs_net_init_rdma(net, conf);
    //     LOG_INFO("KVS Network: RDMA enabled");
    // } else {
    //     LOG_INFO("KVS Network: RDMA disabled, using TCP only");
    // }
    // // 5. 初始化 RDMA 引擎（如果启用）
    
    return 0;
}

kvs_status_t kvs_net_deinit(struct kvs_network_s  *net) {
    if (net->server_fd > 0) {
        close(net->server_fd);
        net->server_fd = -1;
    }
    for(int i = 0; i < net->max_conns; i++) {
        struct kvs_conn_s *conn = &net->conn_pool[i];
        if(conn->_internal.fd > 0 && !conn->_internal.is_closed) {
            // 关闭连接
            close(conn->_internal.fd);
            _kvs_net_deinit_conn(net, conn);
            kvs_net_release_conn(net, conn);
        }
        _kvs_net_destroy_buffer(net, conn);
    }

    //kvs_loop_deinit(net->loop);
    kvs_free(net->conn_pool, sizeof(struct kvs_conn_s) * net->max_conns);
    net->conn_pool = NULL;

    // if(net->use_rdma) {
    //     kvs_rdma_deinit_engine(&net->rdma_engine);
    //     LOG_INFO("KVS Network: RDMA engine deinitialized");
    // }

    return 0;
}

// void kvs_net_start(struct kvs_network_s  *net) {
    
// }

/**
 * @brief 
 * 
 * @param conn 
 * @param send_buf 
 * @param send_buf_sz 
 * @return int -2 : previous data not sent yet
 * @return int -1 : invalid param
 * @return int 0  : success
 */
int kvs_net_set_send_event_raw_buffer(struct kvs_conn_s *conn, char *send_buf, int send_buf_sz) {
    if (conn == NULL || send_buf == NULL || send_buf_sz <=0) {
        return -1;
    }
    if (conn->s_idx > 0) {
        // previous data not sent yet
        LOG_FATAL("connection has pending write data, cannot send raw buffer");
        assert(0);
        return -2;
    }
    
    if(conn->_internal.is_writing == 0){
        // 注册写事件
        kvs_loop_add_send(conn->_internal.net->loop, 
            &conn->_internal.write_ev, send_buf, send_buf_sz);
        conn->_internal.is_writing = 1;
    }
   
    return 0;
}


int kvs_net_copy_msg_to_send_buf(struct kvs_conn_s *conn, char *msg, int msg_sz) {
    if (conn == NULL || msg == NULL || msg_sz <=0) {
        return -1;
    }
    if (msg_sz + conn->s_idx > conn->s_buf_sz) {
        // overflow
        return -2;
    }

    // 复制数据到 conn 的写缓冲区
    memcpy(conn->s_buffer + conn->s_idx, msg, msg_sz);
    conn->s_idx += msg_sz;
    
    return 0;
}
int kvs_net_set_send_event_manual(struct kvs_conn_s *conn) {
    if (conn == NULL) {
        return -1;
    }
    if (conn->s_idx <=0) {
        // nothing to send
        return 0;
    }
    if(conn->_internal.is_writing == 1){
        // already writing
        return -2;
    }

    if(conn->_internal.is_writing == 0){    
        // 注册写事件
        kvs_loop_add_send(conn->_internal.net->loop, 
            &conn->_internal.write_ev, conn->s_buffer, conn->s_idx);
        conn->_internal.is_writing = 1;
    }
    return 0;
}

int kvs_net_set_recv_event(struct kvs_conn_s *conn) {
    if (conn == NULL) {
        return -1;
    }
    if(conn->_internal.is_reading == 1){
        // already reading
        return 0;
    }
    // 注册读事件
    kvs_loop_add_recv(conn->_internal.net->loop, &conn->_internal.read_ev, conn->r_buffer + conn->r_idx, conn->r_buf_sz - conn->r_idx);
    conn->_internal.is_reading = 1;
    return 0;
}

int kvs_net_register_fd(struct kvs_network_s *net, int fd, struct kvs_conn_s **out_conn) {
    if (net == NULL || fd < 0 || out_conn == NULL) {
        return -1;
    }
    struct kvs_conn_s *conn = NULL;
    kvs_net_get_free_conn(net, &conn);

    conn->_internal.net = net;
    conn->header.user_data = NULL; // 业务层可以自行设置
    conn->_internal.fd = fd;
    conn->server_ctx = net->server_ctx;
    conn->header.type = KVS_CONN_TCP;

    _kvs_net_init_conn(net, conn, fd); // register event callbacks


    *out_conn = conn;
    return 0;
}

int kvs_net_online_conns_filter(struct kvs_network_s *net, kvs_conn_filter_cb filter_cb, void* arg) {
    if( net == NULL || filter_cb == NULL) {
        return -1;
    }
    int count = 0;
    for(int i = 0; i < net->max_conns; i++) {
        struct kvs_conn_s *conn = &net->conn_pool[i];
        if(conn->_internal.fd >=0 && !conn->_internal.is_closed) {
            if(filter_cb(conn, arg) >=0) {
                count++;
            }
        }
    }
    return count;
}
