#include "kvs_server.h"

#include "kvs_network.h"
#include "kvs_server.h"
#include "logger.h"
#include "kvs_types.h"
#include "common.h"

#include <assert.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdlib.h>


// static void _init_rdma_cm_id(struct kvs_network_s *net, struct rdma_cm_id *cm_id) {
//     // 分配保护域、完成队列等资源
//     cm_id->pd = net->rdma_engine.pd;
//     cm_id->recv_cq = net->rdma_engine.cq;
//     cm_id->send_cq = net->rdma_engine.cq;

//     // 创建队列对 (QP)
//     struct ibv_qp_init_attr qp_attr;
//     memset(&qp_attr, 0, sizeof(qp_attr));
//     qp_attr.send_cq = net->rdma_engine.cq;
//     qp_attr.recv_cq = net->rdma_engine.cq;
//     qp_attr.qp_type = IBV_QPT_RC; // Reliable Connection
//     qp_attr.cap.max_send_wr = net->rdma_engine.max_send_wr;
//     qp_attr.cap.max_recv_wr = net->rdma_engine.max_recv_wr;
//     qp_attr.cap.max_send_sge = net->rdma_engine.max_sge;
//     qp_attr.cap.max_recv_sge = net->rdma_engine.max_sge;
//     if (rdma_create_qp(cm_id, cm_id->pd, &qp_attr) != 0) {
//         LOG_ERROR("Failed to create RDMA QP");
//         assert(0);
//     }

//     LOG_DEBUG("RDMA connection initialized with QP number: %u\n", cm_id->qp->qp_num);
// }

// static void _deinit_rdma_cm_id(struct rdma_cm_id *cm_id) {
//     if (cm_id->qp) {
//         rdma_destroy_qp(cm_id);
//         cm_id->qp = NULL;
//     }
// }

#if 0
// TODO: Optimize with Hash Map for large scale
struct kvs_conn_s *_find_slave_by_token(struct kvs_network_s *net, uint64_t token) {
    struct kvs_conn_s *conn_pool = net->conn_pool;
    for(int i=0; i<net->max_conns; i++) {
        struct kvs_conn_s *conn = &conn_pool[i];
        if(conn->_internal.token == token && conn->type == KVS_CONN_TCP && conn->_internal.is_closed == 0) {
            return conn;
        }
    }
    return NULL;
}
#endif
#if 0
int kvs_net_on_rdma_connect_request(struct rdma_cm_id *cli_id, const void *priv_data, size_t priv_len, void* global_ctx) {
    // 这里可以根据 priv_data 做一些认证或初始化工作
    LOG_DEBUG("Received RDMA connection request.\n");
    
    struct kvs_network_s *net = (struct kvs_network_s *)cli_id->context;
    uint64_t token = 0;
    if(priv_data != NULL && priv_len == sizeof(uint64_t)) {
        token = *((uint64_t *)priv_data);
        LOG_DEBUG("Received token: %lu\n", token);
    } else {
        LOG_ERROR("Invalid private data in RDMA connection request.\n");
        rdma_reject(cli_id, NULL, 0);
        return -1;
    }

    if(net == NULL || net->on_accept == NULL) {
        LOG_ERROR("Network or on_accept callback is NULL.\n");
        assert(0);
        rdma_reject(cli_id, NULL, 0);
        return -1;
    }


    struct kvs_conn_s *tcp_conn = _find_slave_by_token(net, token);

    if(tcp_conn == NULL) {
        LOG_ERROR("No available connection for RDMA request.\n");
        rdma_reject(cli_id, NULL, 0);
        return -1;
    }
    LOG_DEBUG("Found matching TCP connection for token: %lu, fd: %d\n", token, tcp_conn->_internal.fd);

    _init_rdma_cm_id(net, cli_id);

    struct kvs_conn_s *rdma_conn;
    kvs_status_t status = kvs_net_get_free_conn(net, &rdma_conn);
    if(status != KVS_OK) {
        LOG_ERROR("Failed to get free RDMA connection.\n");
        rdma_reject(cli_id, NULL, 0);
        return -1;
    }
    cli_id->context = (void *)rdma_conn;
    // 关联 RDMA 连接和 TCP 连接
    rdma_conn->_internal.related_conn = tcp_conn;
    tcp_conn->_internal.related_conn = rdma_conn;


    rdma_conn->_internal.rdma_conn.net = net;
    rdma_conn->_internal.rdma_conn.cm_id = cli_id;
    rdma_conn->type = KVS_CONN_RDMA;
    rdma_conn->user_data = tcp_conn->user_data; // 共享用户数据上下文
    rdma_conn->server_ctx = net->server_ctx;

    struct kvs_my_slave_context_s *slave_ctx = (struct kvs_my_slave_context_s *)tcp_conn->user_data;
    if(slave_ctx == NULL || slave_ctx->header.type != KVS_CTX_SLAVE_OF_ME) {
        LOG_FATAL("invalid ctx type in tcp_conn: %d", slave_ctx ? slave_ctx->header.type : -1);
        assert(0);
        rdma_reject(cli_id, NULL, 0);
        return -1;
    }

    // todo: complete closed rdma connection handling
    slave_ctx->rdma_conn = rdma_conn; // 保存 RDMA 连接到上下文中
    slave_ctx->ref_count ++; // 增加引用计数
    //slave_ctx->tcp_conn = tcp_conn; // 保存 TCP 连接到上下文中


    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    if (rdma_accept(cli_id, &conn_param) != 0) {
        LOG_ERROR("Failed to accept RDMA connection.\n");
        assert(0);
        return -1;
    }

    
    // no need to call on_accept here as the tcp_conn is already accepted
    // net->on_accept(rdma_conn); // notify upper layer 
    
    
    return 0;
}
#endif