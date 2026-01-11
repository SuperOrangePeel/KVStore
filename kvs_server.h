#ifndef __KVS_SERVER_H__
#define __KVS_SERVER_H__
#include <stdlib.h>

#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1
#define ENABLE_HASH 1

#define KVS_MAX_CONNECTS 1024
#define KVS_MAX_SLAVES 128


typedef enum {
    KVS_SERVER_ROLE_MASTER = 0,
    KVS_SERVER_ROLE_SLAVE = 1,
} kvs_server_role_t;

struct kvs_mp_pool_s;
struct hashtable_s;
struct kvs_array_s;
struct _rbtree;
struct kvs_pers_context_s;
struct io_uring;


enum KVS_CONN_STATE {
    CONN_STATE_CMD = 0,
    CONN_STATE_SLAVE_WAIT_RDB,
    CONN_STATE_SLAVE_SEND_RDB,
    CONN_STATE_SLAVE_SEND_REPL,
    CONN_STATE_SLAVE_ONLINE,
    // CONN_STATE_MASTER_
};

typedef struct kvs_conn_s {
	int fd;
	char* r_buffer;
	int r_buf_sz;
    int r_idx;
	char* response;
	int w_buf_sz;
    int w_idx;

	int state;

	struct kvs_server_s *server;

    int is_reading; // todo: avoid read/write conflict in uring
    int is_writing; 

    struct {
        int is_master;
    } master_info;

    struct {
        int is_slave;
        int slave_idx;
	    size_t rdb_size;
        size_t repl_backlog_offset;
        size_t rdb_offset;
    } slave_info;
	
} kvs_conn_t;

typedef int (*kvs_on_accept_cb)(struct kvs_server_s *server, int connfd);
typedef int (*kvs_on_msg_cb)(struct kvs_conn_s *conn);
typedef int (*kvs_on_send_cb)(struct kvs_conn_s *conn, int bytes_sent);
typedef int (*kvs_on_close_cb)(struct kvs_conn_s *conn);

struct kvs_server_s {
    unsigned short port;
    int server_fd;
    struct kvs_conn_s *conns;
    int conn_max;
    int role; // master/slave
    struct io_uring *uring;

    struct kvs_pers_context_s *pers_ctx;
    //kvs_mp_pool_t *mempool; // common.c
    struct hashtable_s *hash;
    struct kvs_array_s *array;
    struct _rbtree *rbtree;

    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;

    struct {
        int slaves_fds[KVS_MAX_SLAVES];
        int slave_count;
        int max_slave_count;
        char *repl_backlog;
        size_t repl_backlog_size;
        size_t repl_backlog_idx;

        int rdb_fd;
        int syncing_slaves_count; // number of slaves in SYNC process
        int repl_backlog_overflow; // 1: overflow, 0: not overflow
    } master;

    struct {
        int master_fd;
        char master_ip[64];
        unsigned short master_port;
        int state; // SYNC_HANDSHAKE, SYNC_RDB, SYNC_AOF, SYNC_CONNECTED
    } slave;



};

struct kvs_server_s *kvs_server_init(unsigned short port, kvs_on_accept_cb on_accept,
	kvs_on_msg_cb on_msg, kvs_on_send_cb on_send, kvs_on_close_cb on_close);

void kvs_server_destroy(struct kvs_server_s *server);

/*
* @return: command processing result code defined in enum KVS_RESPONSE_CODE
*/
typedef int(*kvs_server_aof_data_parser_cb)(struct kvs_server_s* server, char* msg, int length, int* parsed_length);
void kvs_server_storage_recovery(struct kvs_server_s *server, kvs_server_aof_data_parser_cb aof_data_parser);

int kvs_server_restore_entry(char data_type, char* key, int len_key, char* value, int len_val, void* arg);
int kvs_server_save_rdb(struct kvs_server_s *server);
int kvs_server_load_rdb(struct kvs_server_s *server);

#endif