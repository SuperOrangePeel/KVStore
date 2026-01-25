#ifndef KVS_NETWORK_H
#define KVS_NETWORK_H

#include "kvs_event_loop.h"
#include "kvs_config.h"
#include "kvs_types.h"
#include "kvs_rdma_engine.h"
#include "kvs_conn.h"

#include <infiniband/verbs.h>

#define KVS_READ_BUF_SZ_DEFAULT 4096      // 4KB
#define KVS_WRITE_BUF_SZ_DEFAULT 1048576   // 1MB

// typedef enum {
//     KVS_CONN_TCP = 0,
//     KVS_CONN_RDMA,
// } kvs_conn_type_t;


// struct kvs_net_rdma_conn_s {
//     struct rdma_cm_id *cm_id;
//     struct kvs_network_s *net;
//     // struct ibv_qp *qp; // cm_id already has qp
//     // char *recv_buffer;
//     // char *send_buffer;
//     struct ibv_mr *recv_mr;
//     struct ibv_mr *send_mr;
// };

// 前向声明
struct kvs_conn_s;

// // 定义一套“行为标准” (V-Table)
// struct kvs_conn_ops_s {
//     // 收到数据时怎么办？
//     // Client: 解析命令
//     // Slave: 写入 RDB 文件
//     kvs_status_t (*on_recv)(struct kvs_conn_s *conn, int *read_size);

//     // 可以发送时怎么办？
//     // Client: 发送回复缓冲区
//     // Slave: 发送 RDB/Backlog
//     kvs_status_t (*on_send)(struct kvs_conn_s *conn, int bytes_sent);

//     // 连接关闭时怎么办？
//     void (*on_close)(struct kvs_conn_s *conn);

//     // 额外事件处理（可选）
//     void (*on_extra_event)(struct kvs_conn_s *conn, int event_type, void *data);

//     // rdma_client
//     // void (*on_rdma_addr_resolved)(struct kvs_conn_s *conn);
//     // void (*on_rdma_route_resolved)(struct kvs_conn_s *conn);
//     // // rdma_server
//     // void (*on_rdma_conn_request)(struct kvs_conn_s *conn);

//     // void (*on_rdma_established)(struct kvs_conn_s *conn);
//     // void (*on_rdma_disconnected)(struct kvs_conn_s *conn);
//     // void (*on_rdma_completion)(struct kvs_conn_s *conn, struct ibv_wc *wc);
    
//     // 名字（调试用）
//     const char *name; 
// };

// 连接定义 (这是之前讨论过的结构)
struct kvs_conn_s {
    //kvs_conn_type_t type;
    struct kvs_conn_header_s header;
    char* r_buffer;
	int r_buf_sz;
    int r_idx;
	char* s_buffer;
	int s_buf_sz;
    int s_idx;

    int raw_buf_sent_sz; // for raw buffer send tracking

	//int state;
	struct kvs_server_s *server_ctx;
    //void *user_data;

    //struct kvs_conn_ops_s ops; // 行为接口
	
    
    struct{
        int fd;
        int is_closed;
        int is_reading; // todo: avoid read/write conflict in uring
        int is_writing; 
       // struct kvs_proactor_s *proactor;
         // 【关键】嵌入事件，供 loop 使用
        kvs_event_t read_ev;
        kvs_event_t write_ev;
        struct kvs_network_s *net; // 回指

        //uint64_t token;

        // RDMA 相关
        //struct kvs_conn_s *related_conn; // 关联的 RDMA/TCP 连接
        //struct kvs_net_rdma_conn_s rdma_conn;
       //struct kvs_rdma_conn_s *rdma_conn;
    } _internal;
} ;

typedef int (*kvs_on_accept_cb)(struct kvs_conn_s *conn);
typedef int (*kvs_on_msg_cb)(struct kvs_conn_s *conn, int *read_size);
typedef int (*kvs_on_send_cb)(struct kvs_conn_s *conn, int bytes_sent);
typedef int (*kvs_on_close_cb)(struct kvs_conn_s *conn);





// 网络管理器
struct kvs_network_s {
    // 1. 组合 Loop 引擎
    kvs_loop_t *loop;
    
    // 2. 管理连接池 (资源)
    struct kvs_conn_s *conn_pool;
    //int conn_num;
    int max_conns;
    int read_buffer_size;
    int write_buffer_size;

    int *free_conn_stack;
    int free_conn_stack_top;
    
    // 3. 监听事件 (监听 socket 比较特殊，单独放)
    kvs_event_t accept_ev;
    struct sockaddr_in client_addr;
    socklen_t client_addrlen;
    int server_fd;

    // 4. rdma
    // int use_rdma;
    // struct kvs_rdma_engine_s rdma_engine;
    
    // 5. 上层回调 (业务逻辑)
    void *server_ctx; // 传给 handler 用
    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;
};



struct kvs_network_config_s {
    struct kvs_loop_s *loop;
    const char *server_ip;
    int port_listen;
    int max_conns;
    //int io_uring_entries;
    int read_buffer_size;
    int write_buffer_size;
    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;
    void *server_ctx;

    // int use_rdma; // 是否启用 RDMA
    // //char *rdma_ip;
    // int rdma_port;
    // //int server_port;
    // int cq_size;
    // int max_recv_wr;
    // int max_send_wr;
    // int max_sge;
    //struct kvs_rdma_config_s rdma_config;
};

int kvs_net_init(struct kvs_network_s *net, struct kvs_network_config_s *conf);
kvs_status_t kvs_net_deinit(struct kvs_network_s  *net);
// void kvs_net_start(struct kvs_network_s *net);



int kvs_net_set_send_event_raw_buffer(struct kvs_conn_s *conn, char *send_buf, int send_buf_sz);
int kvs_net_copy_msg_to_send_buf(struct kvs_conn_s *conn, char *msg, int msg_sz);
int kvs_net_set_send_event_manual(struct kvs_conn_s *conn);
int kvs_net_set_recv_event(struct kvs_conn_s *conn);


int kvs_net_register_fd(struct kvs_network_s *net, int fd, struct kvs_conn_s **out_conn);


typedef int (*kvs_conn_filter_cb)(struct kvs_conn_s *conn, void* arg);
int kvs_net_online_conns_filter(struct kvs_network_s *net, kvs_conn_filter_cb filter_cb, void* arg);




/************************rdma methods******************************/

// typedef enum {
//     KVS_NET_RDMA_OP_SEND = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ,
//     KVS_NET_RDMA_OP_RECV = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
// } kvs_rdma_op_t;

// typedef struct kvs_net_memory_register_s {
//     struct ibv_mr *mr;
//     void *addr;
//     size_t length;
//     kvs_rdma_op_t flags;
// } kvs_net_mr_t;

// kvs_status_t kvs_net_rdma_post_send(struct kvs_conn_s *conn, kvs_net_mr_t *mr, size_t off_set, size_t len);
// kvs_status_t kvs_net_rdma_post_recv(struct kvs_conn_s *conn, kvs_net_mr_t *mr, size_t off_set, size_t len);
// kvs_net_mr_t * kvs_net_register_rdma_memory(struct kvs_network_s *net, void *addr, size_t length, kvs_rdma_op_t flags);
// kvs_status_t kvs_net_deregister_rdma_memory(kvs_net_mr_t *mr);


    
/*************************internal methods**************************/
kvs_status_t kvs_net_get_free_conn(struct kvs_network_s *net, struct kvs_conn_s **out_conn);
kvs_status_t kvs_net_release_conn(struct kvs_network_s *net, struct kvs_conn_s *conn) ;

int kvs_net_on_rdma_connect_request(struct rdma_cm_id *cli_id, const void *priv_data, size_t priv_len, void* global_ctx);
int kvs_net_on_rdma_established(struct rdma_cm_id *id, void* global_ctx);
int kvs_net_on_rdma_disconnected(struct rdma_cm_id *id, void* global_ctx);
int kvs_net_on_rdma_completion(struct ibv_wc *wc, void* global_ctx);
#endif