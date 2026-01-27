#include "kvs_rdma_engine.h"

#include "kvs_event_loop.h"
#include "kvs_types.h"
#include "logger.h"

#include "common.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <arpa/inet.h> 
#include <endian.h>

// static void _on_connection_request(struct rdma_cm_id *cli_cm_id, struct kvs_rdma_engine_s *rdma) {

//     _initialize_connection(cli_cm_id, rdma);

//     struct rdma_conn_param conn_param;
//     memset(&conn_param, 0, sizeof(conn_param));

//     if (0 != rdma_accept(cli_cm_id, &conn_param)) {
//         perror("rdma_accept failed\n");
//         assert(0);
//     }
    
// }

void _rdma_wc_handler(void *ctx, int res, int flags);


static int _kvs_rdma_create_conn_rdma_resources(struct kvs_rdma_conn_s *conn) { 
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct kvs_rdma_engine_s *rdma = conn->rdma_engine;

    conn->comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (conn->comp_channel == NULL) {
        perror("ibv_create_comp_channel failed");
        return -1;
    }

    int flags = fcntl(conn->comp_channel->fd, F_GETFL);
    if(flags < 0) {
        perror("fcntl F_GETFL failed");
        ibv_destroy_comp_channel(conn->comp_channel);
        return -1; 
    }
    if(fcntl(conn->comp_channel->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl F_SETFL failed");
        ibv_destroy_comp_channel(conn->comp_channel);
        return -1;
    }

    conn->pd = ibv_alloc_pd(conn->cm_id->verbs);
    if (conn->pd == NULL) {
        perror("ibv_alloc_pd failed");
        ibv_destroy_comp_channel(conn->comp_channel);
        return -1;
    }

    conn->cq = ibv_create_cq(conn->cm_id->verbs, conn->rdma_engine->cq_size, NULL, conn->comp_channel, 0);
    if (conn->cq == NULL) {
        perror("ibv_create_cq failed");
        ibv_dealloc_pd(conn->pd);
        ibv_destroy_comp_channel(conn->comp_channel);
        return -1;
    }

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = conn->cq;
    qp_attr.recv_cq = conn->cq;
    qp_attr.cap.max_send_wr = rdma->max_send_wr;
    qp_attr.cap.max_recv_wr = rdma->max_recv_wr;
    qp_attr.cap.max_send_sge = rdma->max_sge;
    qp_attr.cap.max_recv_sge = rdma->max_sge;
    qp_attr.cap.max_inline_data = rdma->max_inline_data;
        
    qp_attr.qp_type = IBV_QPT_RC;

    //id->pd = ibv_alloc_pd(id->verbs);

    int ret = rdma_create_qp(conn->cm_id, conn->pd, &qp_attr);
    if (0 != ret) {
       // perror("rdma_create_qp failed");
        LOG_FATAL("rdma_create_qp failed: %s (code: %d)\n", strerror(errno), errno);
        return -1;
    }
    conn->qp = conn->cm_id->qp;
    conn->cm_id->recv_cq = conn->cq;
    conn->cm_id->send_cq = conn->cq;

    if(0 != ibv_req_notify_cq(conn->cq, 0)) {
        perror("ibv_req_notify_cq failed");
        //ibv_destroy_cq(rdma->cq);
        ibv_dealloc_pd(conn->pd);
        ibv_destroy_comp_channel(conn->comp_channel);
        return -1;
    }

    // 注册完成队列事件回调
    
    kvs_event_init(&conn->wc_event, conn->comp_channel->fd, KVS_EV_POLL_IN, _rdma_wc_handler, conn);
    kvs_loop_add_poll_in(conn->rdma_engine->loop, &conn->wc_event);

    LOG_DEBUG("RDMA resources created for cm_id %p, qp %p, cq: %p, recv_cq: %p", cm_id, conn->cm_id->qp, conn->cq, cm_id->recv_cq);

    return 0;
}

static int _kvs_rdma_destroy_conn_rdma_resources(struct kvs_rdma_conn_s *conn) {
    if(conn->cq) {
        ibv_destroy_cq(conn->cq);
        conn->cq = NULL;
    }
    if(conn->pd) {
        ibv_dealloc_pd(conn->pd);
        conn->pd = NULL;
    }
    if(conn->qp) {
        ibv_destroy_qp(conn->qp);
        conn->qp = NULL;
    }
    //kvs_loop_remove_poll(&conn->rdma_engine->loop, &conn->wc_event);
    if(conn->comp_channel) {
        ibv_destroy_comp_channel(conn->comp_channel);
        conn->comp_channel = NULL;
    }

    return 0;
}

static struct kvs_rdma_conn_s *_kvs_rdma_create_conn(struct kvs_rdma_engine_s *rdma) {
    struct kvs_rdma_conn_s *conn = (struct kvs_rdma_conn_s *)kvs_malloc(sizeof(struct kvs_rdma_conn_s));
    memset(conn, 0, sizeof(struct kvs_rdma_conn_s));
    conn->rdma_engine = rdma;
    conn->global_ctx = rdma->global_ctx;
    conn->header.user_data = NULL;
    conn->header.type = KVS_CONN_RDMA;
    return conn;
    //cm_id->context = ctx;
}

static void _kvs_rdma_destroy_conn(struct kvs_rdma_conn_s *conn) {
    if(conn) {
        //_kvs_rdma_destroy_conn_rdma_resources(conn);
        
        kvs_free(conn, sizeof(struct kvs_rdma_conn_s));
    }
}


kvs_status_t kvs_rdma_post_listen(struct kvs_rdma_engine_s *rdma) {
    const char *ip = rdma->rdma_ip;
    int port = rdma->rdma_port;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    rdma_create_id(rdma->event_channel, &rdma->rdma_listen_id, NULL, RDMA_PS_TCP);
    rdma->rdma_listen_id->context = (void *)rdma;
    int ret = rdma_bind_addr(rdma->rdma_listen_id, (struct sockaddr *)&addr);
    if (ret) {
        LOG_ERROR("rdma_bind_addr failed: %s (code: %d)\n", strerror(errno), errno);
        return KVS_ERR;
    }

    ret = rdma_listen(rdma->rdma_listen_id, 10); // backlog = 10
    //rdma->verbs = rdma->rdma_listen_id->verbs;
    if (ret) {
        LOG_ERROR("rdma_listen failed: %s (code: %d)\n", strerror(errno), errno);
        return KVS_ERR;
    }
    //_kvs_rdma_create_global_resources(rdma, rdma->rdma_listen_id->verbs);
    LOG_INFO("RDMA listening on %s:%d\n", ip, port);
    return KVS_OK;
}


kvs_status_t kvs_rdma_post_connect(struct kvs_rdma_engine_s *rdma,  const char *server_ip, int server_port, const void *priv_data, uint8_t priv_len, struct kvs_rdma_conn_s** new_rdma_conn) {
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(server_ip);
    server_addr.sin_port = htons(server_port);

    int ret = rdma_create_id(rdma->event_channel, &rdma->rdma_conn_id, NULL, RDMA_PS_TCP);
    if(ret) {
        LOG_ERROR("rdma_create_id failed: %s (code: %d)\n", strerror(errno), errno);
        return KVS_ERR;
    }
    struct rdma_cm_id *conn_id = rdma->rdma_conn_id;

    ret = rdma_resolve_addr(rdma->rdma_conn_id, NULL, (struct sockaddr *)&server_addr, 2000); // timeout in ms
    if (ret) {
        LOG_ERROR("rdma_resolve_addr failed: %s (code: %d)\n", strerror(errno), errno);
        return KVS_ERR;
    }
    // rdma->private_data = (void *)priv_data;
    // rdma->private_data_len = (uint8_t)priv_len;
    
    struct kvs_rdma_conn_s *conn = (void*)_kvs_rdma_create_conn(rdma);
    conn->private_data = priv_data;
    conn->private_data_len = priv_len;
    conn->cm_id = conn_id;
    //conn->header.user_data = user_data;

    conn_id->context = (void *)conn;

    *new_rdma_conn = conn;
    return KVS_OK;
}

int _kvs_rdma_init_conn(struct kvs_rdma_conn_s *conn) {
    _kvs_rdma_create_conn_rdma_resources(conn);
    return 0;
}

int _kvs_rdma_deinit_conn(struct kvs_rdma_conn_s *conn) {
    _kvs_rdma_destroy_conn_rdma_resources(conn);
    return 0;
}



int _kvs_rdma_on_route_resolved(struct rdma_cm_id *id) {
    //struct kvs_rdma_engine_s *rdma = (struct kvs_rdma_engine_s *)id->context;
    struct kvs_rdma_conn_s *conn = (struct kvs_rdma_conn_s *)id->context;
    conn->cm_id = id;
    
    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.rnr_retry_count = 7; // infinite retry
    conn_param.private_data = conn->private_data;
    conn_param.private_data_len = conn->private_data_len;
   
    //while(1) sleep(1); // wait for established event
    if (0 != rdma_connect(id, &conn_param)) {
        perror("rdma_connect failed");
        assert(0);
        return -1;
    }
  
    return 0;
}

int _kvs_rdma_on_disconnected(struct rdma_cm_id *id) {
    struct kvs_rdma_conn_s *conn = (struct kvs_rdma_conn_s *)id->context;
    _kvs_rdma_deinit_conn(conn);
    _kvs_rdma_destroy_conn(conn);
    return 0;
}

//static int _kvs_rdma_create_global_resources(struct kvs_rdma_engine_s *rdma, struct ibv_context *verbs);


static void _rdma_event_handler(void *ctx, int res, int flags){
    struct kvs_rdma_engine_s *rdma = (struct kvs_rdma_engine_s *)ctx;
    if(res < 0) {
        LOG_ERROR("rdma_event_handler error: %s (code: %d)\n", strerror(-res), res);
        return;
    }

    struct rdma_cm_event *event;
    while (1) {
        int ret = rdma_get_cm_event(rdma->event_channel, &event);
        if(ret == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more events to process
                break;
            } else {
                LOG_ERROR("rdma_get_cm_event failed, %s (code: %d)\n", strerror(errno), errno);
                assert(0);
                break;
            }
        }
        if(event->status < 0) {
            LOG_ERROR("rdma cm event error: %s (code: %d)\n", strerror(-event->status), event->status);
            rdma_ack_cm_event(event);
            continue;
        }

        enum rdma_cm_event_type type = event->event;
        struct rdma_cm_id *id = event->id;
        struct kvs_rdma_conn_s *conn = (struct kvs_rdma_conn_s *)id->context;
        if(event->listen_id != NULL) {
            LOG_DEBUG("RDMA event on listen id.\n");
            // server listen id context is kvs_rdma_engine_s
            // rdma = (struct kvs_rdma_engine_s *)event->listen_id->context;
        }
        bool need_destroy = (type == RDMA_CM_EVENT_DISCONNECTED 
            || type == RDMA_CM_EVENT_ADDR_ERROR 
            || type == RDMA_CM_EVENT_ROUTE_ERROR 
            || type == RDMA_CM_EVENT_CONNECT_ERROR 
            || type == RDMA_CM_EVENT_UNREACHABLE 
            || type == RDMA_CM_EVENT_REJECTED);
        switch(type) {
            case RDMA_CM_EVENT_ADDR_RESOLVED:
                if (0 != rdma_resolve_route(id, 2000)) {
                    perror("rdma_resolve_route failed\n");
                    assert(0);
                }
                LOG_DEBUG("Address resolved.\n");
                break;
            case RDMA_CM_EVENT_ROUTE_RESOLVED:
                _kvs_rdma_init_conn(conn);
                _kvs_rdma_on_route_resolved(event->id);
                LOG_DEBUG("Route resolved.\n");
                
                break;
            case RDMA_CM_EVENT_CONNECT_REQUEST: {
                LOG_DEBUG("Invoking on_connect_request callback.\n");
                //_kvs_rdma_create_global_resources(rdma, event->id->verbs);
                if(event->id == NULL) {
                    LOG_ERROR("rdma_cm_event CONNECT_REQUEST with NULL id");
                    rdma_ack_cm_event(event);
                    assert(0);
                    continue;
                }
                struct kvs_rdma_conn_s *conn = _kvs_rdma_create_conn(rdma);
                event->id->context = (void *)conn;
                conn->cm_id = event->id;
                conn->private_data = event->param.conn.private_data;

                //LOG_DEBUG("Connection request private_data: %d", event->param.conn.private_data_len);
                conn->private_data_len = (uint8_t)event->param.conn.private_data_len;
                _kvs_rdma_init_conn(conn);
                if(rdma->callbacks.on_connect_request) {
                    //LOG_DEBUG("Calling on_connect_request callback. user_data:%p", conn->header.user_data);
                    int ret = rdma->callbacks.on_connect_request(conn);
                    if(ret != 0) {
                        rdma_reject(event->id, NULL, 0);
                        _kvs_rdma_deinit_conn(conn);
                        _kvs_rdma_destroy_conn(conn);
                        LOG_DEBUG("Connection request rejected.\n");
                    } else {
                        struct rdma_conn_param conn_param;
                        memset(&conn_param, 0, sizeof(conn_param));
                        conn_param.responder_resources = 1; // 允许的远程读取/原子操作数
                        conn_param.initiator_depth = 1;
                        conn_param.retry_count = 7;         // 网络丢包重试次数 (7 表示无限重试)
                        //conn_param.rnr_retry = 7;           // 【关键】对端没铺坑时的重试次数 (7 表示无限重试)
                        conn_param.rnr_retry_count = 7;
                        if (0 != rdma_accept(conn->cm_id, &conn_param)) {
                            perror("rdma_accept failed\n");
                            exit(-1);
                        }
                        LOG_DEBUG("Connection request accepted.\n");
                    }
                }


                // _on_connection_request(event->id, event->param.conn.private_data, rdma);
                //LOG_DEBUG("Received connection request.");
            }
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                // Handle established connection
                if(rdma->callbacks.on_established) {
                    rdma->callbacks.on_established(conn);
                }
                LOG_DEBUG("Connection established.\n");
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                
                // Handle disconnection
                if(rdma->callbacks.on_disconnected) {
                    rdma->callbacks.on_disconnected(conn);
                }
                // _kvs_rdma_deinit_conn(conn);
                // _kvs_rdma_destroy_conn(conn);
                LOG_DEBUG("Connection disconnected.");
                break;
            default:
                if (type == RDMA_CM_EVENT_ADDR_ERROR || 
                    type == RDMA_CM_EVENT_ROUTE_ERROR || 
                    type == RDMA_CM_EVENT_CONNECT_ERROR || 
                    type == RDMA_CM_EVENT_UNREACHABLE || 
                    type == RDMA_CM_EVENT_REJECTED) {
                    LOG_DEBUG("Invoking on_error callback for event type %d.\n", type);
                    if (rdma->callbacks.on_error) {
                        rdma->callbacks.on_error(conn, type, event->status);
                    }
                    // _kvs_rdma_deinit_conn(conn);
                    // _kvs_rdma_destroy_conn(conn);
                    //rdma->callbacks.on_error(conn, type, event->status);
                    //_kvs_rdma_on_error(conn->cm_id);
                    
                }
                break;
        }

        rdma_ack_cm_event(event);

        if(need_destroy) {
            rdma_disconnect(conn->cm_id);
            _kvs_rdma_on_disconnected(id);
        }
    }

    kvs_loop_add_poll_in(rdma->loop, &rdma->cm_event);
}

void _rdma_wc_handler(void *ctx, int res, int flags){
    struct kvs_rdma_conn_s *conn = (struct kvs_rdma_conn_s *)ctx;
    struct kvs_rdma_engine_s *rdma = conn->rdma_engine;
    if(res < 0) {
        LOG_ERROR("rdma_event_handler error: %s (code: %d)\n", strerror(-res), res);
        return;
    }
    LOG_INFO("RDMA work completion handler invoked.\n");
    struct ibv_cq *cq;
    void *ctx_wc = NULL;
    struct ibv_wc wc[10];
    while (1) {
        if(conn->comp_channel == NULL) {
            LOG_ERROR("RDMA connection comp_channel is NULL.");
            return;
        }
        int ret = ibv_get_cq_event(conn->comp_channel, &cq, &ctx_wc);
        if(ret == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more events to process
                LOG_DEBUG("No more CQ events to process.\n");
                //kvs_loop_add_poll_in(rdma->loop, &rdma->wc_event);
                break;
            } else {
                LOG_ERROR("ibv_get_cq_event failed, %s (code: %d)\n", strerror(errno), errno);
                assert(0);
                break;
            }
        }

        ibv_ack_cq_events(cq, 1);
        ibv_req_notify_cq(cq, 0);

        int n_wc = 0;
        while ((n_wc =ibv_poll_cq(cq, 10, wc)) > 0) {
            //LOG_DEBUG("Polled %d work completions.", n_wc);
            for (int i = 0; i < n_wc; i++) { 
                struct ibv_wc *cur_wc = &wc[i];
                //struct kvs_rdma_conn_s *conn = (struct kvs_rdma_conn_s *)wc.wr_id;
                struct kvs_rdma_cq_ctx_s *cq_ctx = (struct kvs_rdma_cq_ctx_s *)(uintptr_t)cur_wc->wr_id;
                
                struct kvs_rdma_conn_s *conn = cq_ctx->conn;
                void *user_data = cq_ctx->user_data;
                size_t off_set = cq_ctx->off_set;
                kvs_free(cq_ctx, sizeof(struct kvs_rdma_cq_ctx_s));
                if(conn == NULL) {
                    LOG_ERROR("RDMA connection context is NULL in work completion.");
                    continue;
                }
                //rdma->callbacks.on_completion(&wc, rdma->global_ctx);
                if(cur_wc->status == IBV_WC_SUCCESS) {
                    //LOG_DEBUG("RDMA work completed successfully for wr_id: %lu\n", cur_wc->wr_id);
                } else {
                    LOG_ERROR("RDMA work completion error for wr_id: %lu, status: %s (code: %d)\n", 
                        cur_wc->wr_id, ibv_wc_status_str((enum ibv_wc_status)cur_wc->status), cur_wc->status);
                        continue;
                    
                }
                
                if(cur_wc->opcode == IBV_WC_RECV) {
                    // Handle received data
                    if(rdma->callbacks.on_comp_recv) {
                        rdma->callbacks.on_comp_recv(conn, off_set, cur_wc->byte_len, cur_wc->imm_data, user_data);
                    }
                    //LOG_DEBUG("Receive completed for wr_id: %lu, imm_data: %u\n", cur_wc->wr_id, cur_wc->imm_data);
                } else if (cur_wc->opcode == IBV_WC_SEND) {
                    // Handle send completion
                    if(rdma->callbacks.on_comp_send) {
                        rdma->callbacks.on_comp_send(conn, off_set, cur_wc->byte_len, user_data);
                    }
                    //LOG_DEBUG("Send completed for wr_id: %lu", cur_wc->wr_id);
                }
                    
            }
        }
    }

    kvs_loop_add_poll_in(rdma->loop, &conn->wc_event);
}




int kvs_rdma_init_engine(struct kvs_rdma_engine_s *rdma, struct kvs_loop_s *loop, struct kvs_rdma_config_s *config) {
    // 1. create event channel and set non-blocking
    rdma->event_channel = rdma_create_event_channel();
    if (!rdma->event_channel) {
        perror("rdma_create_event_channel failed");
        return -1;
    }

    int flags = fcntl(rdma->event_channel->fd, F_GETFL);
    if(flags < 0) {
        perror("fcntl F_GETFL failed");
        rdma_destroy_event_channel(rdma->event_channel);
        return -1;
    }
    if(fcntl(rdma->event_channel->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl F_SETFL failed");
        rdma_destroy_event_channel(rdma->event_channel);
        return -1;
    }

    const char *ip = config->server_ip;
    rdma->rdma_ip = ip;
    rdma->rdma_port = config->server_port;

    LOG_DEBUG("Initializing RDMA engine on %s:%d\n", ip, rdma->rdma_port);
    rdma->cq_size = config->cq_size;
    rdma->max_recv_wr = config->max_recv_wr;
    rdma->max_send_wr = config->max_send_wr;
    rdma->max_sge = config->max_sge;

    rdma->callbacks = config->callbacks;
    rdma->loop = loop;
    rdma->global_ctx = config->global_ctx;



    // 2. initialize global resources
    // int num_devices;
    // struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    
    // // select device named "siw0"
    // struct ibv_device *my_dev = NULL;
    // for (int i=0; i<num_devices; i++) {
    //     if (strcmp(ibv_get_device_name(dev_list[i]), "siw0") == 0) {
    //         my_dev = dev_list[i];
    //         break;
    //     }
    // }
    // rdma->verbs = ibv_open_device(my_dev);




    // rdma->cm_event.fd = rdma->event_channel->fd;
    // rdma->wc_event.fd = rdma->comp_channel->fd;

    kvs_event_init(&rdma->cm_event, rdma->event_channel->fd, KVS_EV_POLL_IN, _rdma_event_handler, rdma);
    kvs_loop_add_poll_in(rdma->loop, &rdma->cm_event);

    return 0;
}


int kvs_rdma_deinit_engine(struct kvs_rdma_engine_s *rdma) {
    // if (rdma->cq) {
    //     ibv_destroy_cq(rdma->cq);
    //     rdma->cq = NULL;
    // }
    // if (rdma->pd) {
    //     ibv_dealloc_pd(rdma->pd);
    //     rdma->pd = NULL;
    // }
    // if (rdma->comp_channel) {
    //     ibv_destroy_comp_channel(rdma->comp_channel);
    //     rdma->comp_channel = NULL;
    // }
    if (rdma->event_channel) {
        rdma_destroy_event_channel(rdma->event_channel);
        rdma->event_channel = NULL;
    }
    if (rdma->rdma_listen_id) {
        rdma_destroy_id(rdma->rdma_listen_id);
        rdma->rdma_listen_id = NULL;
    }
    return 0;
}


// struct kvs_rdma_mr_s *kvs_rdma_register_memory(struct kvs_rdma_engine_s *rdma, void *addr, size_t length, int flags) {
//     LOG_DEBUG("Registering memory region: addr=%p, length=%zu, flags=0x%x", addr, length, flags);
//     struct kvs_rdma_mr_s *mr = (struct kvs_rdma_mr_s *)kvs_malloc(sizeof(struct kvs_rdma_mr_s));
//     mr->addr = addr;
//     mr->length = length;
//     if(rdma->pd == NULL) {
//         LOG_FATAL("kvs_rdma_register_memory: rdma pd is NULL");
//         kvs_free(mr, sizeof(struct kvs_rdma_mr_s));
//         return NULL;
//     }
//     mr->mr = ibv_reg_mr(rdma->pd, addr, length, flags);
//     if (!mr->mr) {
//         perror("ibv_reg_mr failed");
//         assert(0);
//         kvs_free(mr, sizeof(struct kvs_rdma_mr_s));
//         return NULL;
//     }

//     return mr;
// }

struct kvs_rdma_mr_s *kvs_rdma_register_memory_on_conn(struct kvs_rdma_conn_s *conn, void *addr, size_t length, int flags) {
    LOG_DEBUG("Registering memory region on conn: addr=%p, length=%zu, flags=0x%x", addr, length, flags);
    struct kvs_rdma_mr_s *mr = (struct kvs_rdma_mr_s *)kvs_malloc(sizeof(struct kvs_rdma_mr_s));
    mr->addr = addr;
    mr->length = length;
    if(conn->cm_id->pd == NULL) {
        LOG_FATAL("kvs_rdma_register_memory_on_conn: conn pd is NULL");
        kvs_free(mr, sizeof(struct kvs_rdma_mr_s));
        return NULL;
    }
    mr->mr = ibv_reg_mr(conn->cm_id->pd, addr, length, flags);
    if (!mr->mr) {
        perror("ibv_reg_mr failed");
        assert(0);
        kvs_free(mr, sizeof(struct kvs_rdma_mr_s));
        return NULL;
    }

    return mr;
}

int kvs_rdma_deregister_memory(struct kvs_rdma_mr_s *mr) {
    if (mr) {
        int ret = ibv_dereg_mr(mr->mr);
        kvs_free(mr, sizeof(struct kvs_rdma_mr_s));
        return ret;
    }
    return 0;
}

// struct ibv_mr *kvs_rdma_register_memory(struct kvs_rdma_engine_s *rdma, void *addr, size_t length, int flags) {
//     struct ibv_mr *mr = ibv_reg_mr(rdma->pd, addr, length, flags);
//     if (!mr) {
//         perror("ibv_reg_mr failed");
//         return NULL;
//     }


//     return mr;
// }

// int kvs_rdma_deregister_memory(struct ibv_mr *mr) {
//     if (mr) {
//         return ibv_dereg_mr(mr);
//     }
//     return 0;
// }

kvs_status_t kvs_rdma_post_send(struct kvs_rdma_conn_s *conn,struct kvs_rdma_mr_s *mr, int imm_data, size_t off_set, int len, void* user_data) {
    if (conn == NULL || mr == NULL) {
        LOG_FATAL("kvs_rdma_post_send: conn == NULL is %d mr == NULL is %d", conn == NULL, mr == NULL);
        return KVS_ERR;
    }

    if(off_set + len > mr->length) {
        LOG_FATAL("kvs_rdma_post_send: off_set + len exceeds mr length, off_set: %zu, len: %zu, mr length: %zu", off_set, len, mr->length);
        return KVS_ERR;
    }
    
    struct ibv_sge sge;
    sge.addr = (uintptr_t)(mr->addr) + off_set;
    sge.length = len;
    sge.lkey = mr->mr->lkey;

    struct kvs_rdma_cq_ctx_s *cq_ctx = (struct kvs_rdma_cq_ctx_s *)kvs_malloc(sizeof(struct kvs_rdma_cq_ctx_s));
    cq_ctx->conn = conn;
    cq_ctx->user_data = user_data;
    cq_ctx->off_set = off_set;

    struct ibv_send_wr send_wr, *bad_send_wr = NULL;
    memset(&send_wr, 0, sizeof(send_wr));
    //mr->ctx = (void *)conn; // set owner to identify the connection in completion

    send_wr.wr_id = (uintptr_t)cq_ctx; // use wr_id to identify the memory region
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.imm_data = imm_data; // optional immediate data

    ibv_post_send(conn->cm_id->qp, &send_wr, &bad_send_wr);
    //LOG_DEBUG("cm_id %p post send on cq %p", conn->cm_id, conn->cm_id->send_cq);

    return KVS_OK;
}

kvs_status_t kvs_rdma_post_send_inline(struct kvs_rdma_conn_s *conn, struct kvs_rdma_mr_s *mr, int imm_data, size_t off_set, int len, void* user_data) {
    if (conn == NULL || mr == NULL) {
        LOG_FATAL("kvs_rdma_post_send_inline: conn == NULL is %d mr == NULL is %d", conn == NULL, mr == NULL);
        return KVS_ERR;
    }
    if(len > conn->rdma_engine->max_inline_data) {
        LOG_FATAL("kvs_rdma_post_send_inline: len %d exceeds max_inline_data %d", len, conn->rdma_engine->max_inline_data);
        return KVS_ERR;
    }
    if(off_set + len > mr->length) {
        LOG_FATAL("kvs_rdma_post_send_inline: off_set + len exceeds mr length, off_set: %zu, len: %zu, mr length: %zu", off_set, len, mr->length);
        return KVS_ERR;
    }
    
    struct ibv_sge sge;
    sge.addr = (uintptr_t)(mr->addr) + off_set;
    sge.length = len;
    sge.lkey = mr->mr->lkey;

    struct kvs_rdma_cq_ctx_s *cq_ctx = (struct kvs_rdma_cq_ctx_s *)kvs_malloc(sizeof(struct kvs_rdma_cq_ctx_s));
    cq_ctx->conn = conn;
    cq_ctx->user_data = user_data;
    cq_ctx->off_set = off_set;

    struct ibv_send_wr send_wr, *bad_send_wr = NULL;
    memset(&send_wr, 0, sizeof(send_wr));
    //mr->ctx = (void *)conn; // set owner to identify the connection in completion

    send_wr.wr_id = (uintptr_t)cq_ctx; // use wr_id to identify the memory region
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.imm_data = imm_data; // optional immediate data

    ibv_post_send(conn->cm_id->qp, &send_wr, &bad_send_wr);
    //LOG_DEBUG("cm_id %p post send inline on cq %p", conn->cm_id, conn->cm_id->send_cq);

    return KVS_OK;
}


kvs_status_t kvs_rdma_post_recv(struct kvs_rdma_conn_s *conn, struct kvs_rdma_mr_s *mr, size_t off_set, int len, void *user_data) {
    if (conn == NULL || mr == NULL) {
        LOG_FATAL("kvs_rdma_post_recv: conn == NULL is %d mr == NULL is %d", conn == NULL, mr == NULL);
        return KVS_ERR;
    }

    if(off_set + len > mr->length) {
        LOG_FATAL("kvs_rdma_post_recv: off_set + len exceeds mr length, off_set: %zu, len: %zu, mr length: %zu", off_set, len, mr->length);
        return KVS_ERR;
    }
    
    struct ibv_sge sge;
    sge.addr = (uintptr_t)(mr->addr) + off_set;
    sge.length = len;
    sge.lkey = mr->mr->lkey;

    struct kvs_rdma_cq_ctx_s *cq_ctx = (struct kvs_rdma_cq_ctx_s *)kvs_malloc(sizeof(struct kvs_rdma_cq_ctx_s));
    cq_ctx->conn = conn;
    cq_ctx->user_data = user_data;
    cq_ctx->off_set = off_set;

    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = (uintptr_t)cq_ctx;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    //conn->qp->recv_cq = conn->cq; // ensure using the correct cq
    //LOG_DEBUG("kvs_rdma_post_recv: cm_id %p, recv_cq: %p, conn->cq: %p", conn->cm_id, conn->cm_id->recv_cq, conn->cq);
    int ret = ibv_post_recv(conn->qp, &recv_wr, &bad_recv_wr);
    if(ret != 0) {
        LOG_FATAL("kvs_rdma_post_recv: ibv_post_recv failed: %s (code: %d)", strerror(errno), errno);
        return KVS_ERR;
    }
    //LOG_DEBUG("cm_id %p post recv on cq %p, conn->cq: %p", conn->cm_id, conn->cm_id->recv_cq, conn->cq);
    return KVS_OK;
}

#if 0
kvs_status_t kvs_rdma_post_send(struct ibv_qp *qp, struct ibv_mr *mr, size_t off_set, int len, uint64_t wr_id) {
    if (qp == NULL || mr == NULL) {
        assert(0);
        return KVS_ERR;
    }

    if(off_set + len > mr->length) {
        LOG_FATAL("kvs_net_rdma_post_send: off_set + len exceeds mr length, off_set: %zu, len: %zu, mr length: %zu", off_set, len, mr->length);
        assert(0);
        return KVS_ERR;
    }
    
    struct ibv_sge sge;
    sge.addr = (uintptr_t)(mr->addr) + off_set;
    sge.length = len;
    sge.lkey = mr->lkey;


    struct ibv_send_wr send_wr, *bad_send_wr = NULL;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.wr_id = wr_id;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.imm_data = len; // optional immediate data
    
    
    ibv_post_send(qp, &send_wr, &bad_send_wr);

    return KVS_OK;
}



kvs_status_t kvs_rdma_post_recv(struct ibv_qp *qp, struct ibv_mr *mr, size_t off_set, int len, uint64_t wr_id) {
    if (qp == NULL || mr == NULL) {
        assert(0);
        return KVS_ERR;
    }

    if(off_set + len > mr->length) {
        LOG_FATAL("kvs_net_rdma_post_recv: off_set + len exceeds mr length, off_set: %zu, len: %zu, mr length: %zu", off_set, len, mr->length);
        assert(0);
        return KVS_ERR;
    }
    
    struct ibv_sge sge;
    sge.addr = (uintptr_t)(mr->addr) + off_set;
    sge.length = len;
    sge.lkey = mr->lkey;

    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = wr_id;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    ibv_post_recv(qp, &recv_wr, &bad_recv_wr);

    return KVS_OK;
}

#endif