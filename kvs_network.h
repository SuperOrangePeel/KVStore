#ifndef KVS_NETWORK_H
#define KVS_NETWORK_H

#include "kvs_event_loop.h"
#include "kvs_config.h"
#include "kvs_types.h"

#define KVS_READ_BUF_SZ_DEFAULT 4096      // 4KB
#define KVS_WRITE_BUF_SZ_DEFAULT 1048576   // 1MB

// 连接定义 (这是之前讨论过的结构)
 struct kvs_conn_s {
    char* r_buffer;
	int r_buf_sz;
    int r_idx;
	char* w_buffer;
	int w_buf_sz;
    int w_idx;
    
    int raw_buf_sent_sz; // for raw buffer send tracking

	//int state;
	struct kvs_server_s *server_ctx;
    void *bussiness_ctx;
	
    struct{
        int version;
        int fd;
        int is_closed;
        int is_reading; // todo: avoid read/write conflict in uring
        int is_writing; 
       // struct kvs_proactor_s *proactor;
         // 【关键】嵌入事件，供 loop 使用
        kvs_event_t read_ev;
        kvs_event_t write_ev;
        struct kvs_network_s *net; // 回指
    } _internal;
} ;

typedef int (*kvs_on_accept_cb)(struct kvs_conn_s *conn);
typedef int (*kvs_on_msg_cb)(struct kvs_conn_s *conn, int *read_size);
typedef int (*kvs_on_send_cb)(struct kvs_conn_s *conn, int bytes_sent);
typedef int (*kvs_on_close_cb)(struct kvs_conn_s *conn);

// 网络管理器
struct kvs_network_s {
    // 1. 组合 Loop 引擎
    kvs_loop_t loop;
    
    // 2. 管理连接池 (资源)
    struct kvs_conn_s *conns;
    int max_conns;
    int read_buffer_size;
    int write_buffer_size;
    
    // 3. 监听事件 (监听 socket 比较特殊，单独放)
    kvs_event_t accept_ev;
    struct sockaddr_in client_addr;
    socklen_t client_addrlen;
    int server_fd;
    
    // 4. 上层回调 (业务逻辑)
    void *server_ctx; // 传给 handler 用
    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;
};



struct kvs_network_config_s {
    unsigned short port_listen;
    int max_conns;
    int io_uring_entries;
    int read_buffer_size;
    int write_buffer_size;
    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;
    void *server_ctx;
};

int kvs_net_init(struct kvs_network_s *net, struct kvs_network_config_s *conf);
kvs_status_t kvs_net_deinit(struct kvs_network_s  *net);
void kvs_net_start(struct kvs_network_s *net);


int kvs_net_set_send_event_raw_buffer(struct kvs_conn_s *conn, char *send_buf, int send_buf_sz);
int kvs_net_copy_msg_to_send_buf(struct kvs_conn_s *conn, char *msg, int msg_sz);
int kvs_net_set_send_event_manual(struct kvs_conn_s *conn);

int kvs_net_set_recv_event(struct kvs_conn_s *conn);


int kvs_net_resigster_fd(struct kvs_network_s *net, int fd, struct kvs_conn_s **out_conn);
#endif