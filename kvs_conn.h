#ifndef __KVS_CONN_H__
#define __KVS_CONN_H__

//struct kvs_conn_s;
typedef enum {
    KVS_CONN_TCP = 1,
    KVS_CONN_RDMA,
} kvs_conn_type_t;

struct kvs_conn_header_s {
    void *user_data;
    kvs_conn_type_t type;
};

#endif