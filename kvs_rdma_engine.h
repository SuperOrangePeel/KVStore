#ifndef __KVS_RDMA_ENGINE_H__
#define __KVS_RDMA_ENGINE_H__

#include "kvs_event_loop.h"
#include "kvs_types.h"
#include "kvs_conn.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>



typedef enum {
    KVS_RDMA_OP_SEND = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ,
    KVS_RDMA_OP_RECV = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
} kvs_rdma_op_t;



struct kvs_rdma_mr_s {
    struct ibv_mr *mr;
    void *addr;
    size_t length;
    kvs_rdma_op_t flags;
    //void *ctx;
};

struct kvs_rdma_conn_s {
    struct kvs_conn_header_s header;
    //void *user_data;
    struct rdma_cm_id *cm_id;

    struct kvs_rdma_engine_s *rdma_engine;
    //struct rdma_cm_id *cm_id;
    void *global_ctx;
    

    const void *private_data;
	uint8_t private_data_len;
};

struct kvs_rdma_cbs_s{

    // // 【客户端专用】地址解析完成 (下一步: resolve_route)
    // void (*on_addr_resolved)(struct rdma_cm_id *id);
    // // 【客户端专用】路由解析完成 (下一步: create_qp -> connect)
    // void (*on_route_resolved)(struct rdma_cm_id *id);
    //【客户端专用】连接建立前触发 (CONNECT)
    //int (*on_connect_before)(struct kvs_rdma_conn_s *conn);
    // 【服务端专用】收到连接请求时触发 (CONNECT_REQUEST)
    // return 0 on success, -1 on reject
    int (*on_connect_request)(struct kvs_rdma_conn_s *conn);
    
    // 当连接彻底建立时触发 (ESTABLISHED)
    int (*on_established)(struct kvs_rdma_conn_s *conn);
    
    // 当断开连接时触发
    int (*on_disconnected)(struct kvs_rdma_conn_s *conn);
    
    // 【通用】错误处理
    void (*on_error)(struct kvs_rdma_conn_s *conn, int event_type, int err);

    //int (*on_completion)(struct ibv_wc *wc, void* global_ctx);
    int (*on_comp_recv)(struct kvs_rdma_conn_s *conn, size_t recv_off_set, int recv_len, int imm_data, void *user_data);
    int (*on_comp_send)(struct kvs_rdma_conn_s *conn, size_t send_off_set, int send_len, void *user_data);
};


struct kvs_rdma_config_s {
    const char *server_ip;
    int server_port;
    int cq_size;
    int max_recv_wr;
    int max_send_wr;
    int max_sge;

    void *global_ctx;
    struct kvs_rdma_cbs_s callbacks;
};


struct kvs_rdma_engine_s {
    const char *rdma_ip;
    int rdma_port;
    struct kvs_loop_s *loop;
    struct ibv_context *verbs;
    struct rdma_event_channel *event_channel;
    struct rdma_cm_id *rdma_conn_id; //connect id
    // const void *private_data;
	// uint8_t private_data_len;

    struct rdma_cm_id *rdma_listen_id; // only for server listen
    struct ibv_comp_channel *comp_channel;
    struct ibv_pd *pd; //唯一的保护域
    struct ibv_cq *cq; //完成队列
    int cq_size;
    struct kvs_event_s cm_event; // cm 事件
    struct kvs_event_s wc_event; // 完成队列事件

    void *global_ctx;

    // RDMA 参数
    int max_recv_wr;
    int max_send_wr;
    int max_sge;

    struct kvs_rdma_cbs_s callbacks;
};

struct kvs_rdma_cq_ctx_s {
    struct kvs_rdma_conn_s *conn;
    void *user_data;
    size_t off_set;
};



int kvs_rdma_init_engine(struct kvs_rdma_engine_s *rdma, struct kvs_loop_s *loop, struct kvs_rdma_config_s *config) ;
int kvs_rdma_deinit_engine(struct kvs_rdma_engine_s *rdma);
struct kvs_rdma_mr_s *kvs_rdma_register_memory(struct kvs_rdma_engine_s *rdma, void *addr, size_t length, int flags);
int kvs_rdma_deregister_memory(struct kvs_rdma_mr_s *mr);

// struct ibv_mr *kvs_rdma_register_memory(struct kvs_rdma_engine_s *rdma, void *addr, size_t length, int flags);
// int kvs_rdma_deregister_memory(struct ibv_mr *mr);


kvs_status_t kvs_rdma_post_listen(struct kvs_rdma_engine_s *rdma);
kvs_status_t kvs_rdma_post_connect(struct kvs_rdma_engine_s *rdma,  const char *server_ip, int server_port, const void *priv_data, uint8_t priv_len, void* user_data);

// kvs_status_t kvs_rdma_post_send(struct ibv_qp *qp, struct ibv_mr *mr, size_t off_set, int len, uint64_t wr_id);
// kvs_status_t kvs_rdma_post_recv(struct ibv_qp *qp, struct ibv_mr *mr, size_t off_set, int len, uint64_t wr_id);

kvs_status_t kvs_rdma_post_send(struct kvs_rdma_conn_s *conn,struct kvs_rdma_mr_s *mr, int imm_data, size_t off_set, int len, void* user_data);
kvs_status_t kvs_rdma_post_recv(struct kvs_rdma_conn_s *conn, struct kvs_rdma_mr_s *mr, size_t off_set, int len, void *user_data);

#endif 