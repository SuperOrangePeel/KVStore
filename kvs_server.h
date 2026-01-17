#ifndef __KVS_SERVER_H__
#define __KVS_SERVER_H__
#include <stdlib.h>
#include <liburing.h>

#include "kvs_proactor.h"
#include "kvs_types.h"
#include "kvs_persistence.h"

#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1
#define ENABLE_HASH 1

#define KVS_MAX_CONNECTS 1024

#define KVS_SERVER_ROLE_MASTER (1 << 0)
#define KVS_SERVER_ROLE_SLAVE (1 << 1)

#define KVS_SERVER_MAX_SLAVES_DEFAULT 128

// typedef enum {
//     KVS_SERVER_ROLE_MASTER = 0,
//     KVS_SERVER_ROLE_SLAVE = 1,
// } kvs_server_role_t;

struct kvs_mp_pool_s;
struct hashtable_s;
struct kvs_array_s;
struct _rbtree;
struct kvs_pers_context_s;
struct io_uring;


struct kvs_master_config_s {
    int max_slave_count;
    size_t repl_backlog_size;
};

struct kvs_slave_config_s {
    char master_ip[64];
    unsigned short master_port;
};

struct kvs_server_config_s {
    int role;
    struct kvs_master_config_s master_config;
    struct kvs_slave_config_s slave_config;
    struct kvs_pers_config_s pers_config;
};

typedef enum {
    SYNC_HANDSHAKE = 0,
    SYNC_RDB,
    SYNC_AOF,
    SYNC_CONNECTED
} kvs_slave_state_t;

typedef enum KVS_CONN_STATE {
    CONN_STATE_SLAVE_WAIT_RDB,
    CONN_STATE_SLAVE_SEND_RDB,
    CONN_STATE_SLAVE_SEND_REPL,
    CONN_STATE_SLAVE_ONLINE,
    // CONN_STATE_MASTER_
} kvs_master_slave_conn_state_t;

typedef enum {
	KVS_RDB_START = 0,
	KVS_RDB_ARRAY = 0,
	KVS_RDB_RBTREE,
	KVS_RDB_HASH,
	KVS_RDB_END
} kvs_server_rdb_dbtype_t;

// context for connections from slaves to me (the master)
// used in struct kvs_conn_s->bussiness_ctx
struct kvs_master_slave_context_s {
    struct kvs_ctx_header_s header;
    kvs_master_slave_conn_state_t state;
    int slave_idx;
    size_t rdb_size;
    size_t repl_backlog_offset;
    size_t rdb_offset;
};

// context for connections from me (the slave) to master
// used in struct kvs_conn_s->bussiness_ctx
struct kvs_slave_master_context_s {
    struct kvs_ctx_header_s header;
    int is_master;
};

struct kvs_slave_s {
    int master_fd;
    char master_ip[64];
    unsigned short master_port;
    kvs_slave_state_t state; // SYNC_HANDSHAKE, SYNC_RDB, SYNC_AOF, SYNC_CONNECTED

    struct kvs_server_s *server;
};

struct kvs_master_s {
    struct kvs_conn_s **slave_conns; // array of slave connections
    int slave_count;
    int max_slave_count;
    char *repl_backlog;
    size_t repl_backlog_size;
    size_t repl_backlog_idx;

    int rdb_fd;
    int syncing_slaves_count; // number of slaves in SYNC process
    int repl_backlog_overflow; // 1: overflow, 0: not overflow
    struct kvs_server_s *server;
};



struct kvs_server_s {
    struct kvs_proactor_s *proactor;

    struct kvs_pers_context_s *pers_ctx;
    //kvs_mp_pool_t *mempool; // common.c
    struct hashtable_s *hash;
    struct kvs_array_s *array;
    struct _rbtree *rbtree;

    struct kvs_master_s* master;
    struct kvs_slave_s* slave;
    pid_t rdb_child_pid;

    int role; // master/slave
};



struct kvs_server_s *kvs_server_create(struct kvs_proactor_s *proactor_pt, struct kvs_server_config_s *config_pt);
void kvs_server_destroy(struct kvs_server_s *server);

/*
* @return: command processing result code defined in enum KVS_RESPONSE_CODE
*/
typedef kvs_status_t(*kvs_server_cmd_executor)(struct kvs_server_s* server, char* msg, int length, int* parsed_length);
kvs_status_t kvs_server_storage_recovery(struct kvs_server_s *server, kvs_server_cmd_executor cmd_executor);

// int kvs_server_restore_entry(char data_type, char* key, int len_key, char* value, int len_val, void* arg);
int kvs_server_save_rdb(struct kvs_server_s *server);
kvs_status_t kvs_server_save_rdb_fork(struct kvs_server_s *server);

int kvs_server_load_rdb(struct kvs_server_s *server);
int kvs_rdb_child_process(struct kvs_server_s *server);

kvs_status_t kvs_server_init_connection(struct kvs_server_s *server, struct kvs_conn_s *conn);
kvs_status_t kvs_server_deinit_connection(struct kvs_server_s *server, struct kvs_conn_s *conn);
kvs_status_t kvs_master_slave_connection_init(struct kvs_master_s *master, struct kvs_conn_s *conn);
kvs_status_t kvs_master_slave_connection_deinit(struct kvs_master_s *master, struct kvs_conn_s *conn);
kvs_status_t kvs_master_slave_sync_start(struct kvs_master_s *master, struct kvs_conn_s *conn);
kvs_status_t kvs_master_slave_sync_tick(struct kvs_master_s *master, struct kvs_conn_s *conn);




kvs_result_t kvs_server_set(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) ;
kvs_result_t kvs_server_get(struct kvs_server_s *server, char* key, int len_key, char** value_out, int* len_val_out) ;
kvs_result_t kvs_server_del(struct kvs_server_s *server, char* key, int len_key) ;
kvs_result_t kvs_server_mod(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) ;
kvs_result_t kvs_server_exist(struct kvs_server_s *server, char* key, int len_key) ;
kvs_result_t kvs_server_rset(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) ;
kvs_result_t kvs_server_rget(struct kvs_server_s *server, char* key, int len_key, char** value_out, int* len_val_out) ;
kvs_result_t kvs_server_rdel(struct kvs_server_s *server, char* key, int len_key) ;
kvs_result_t kvs_server_rmod(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) ;
kvs_result_t kvs_server_rexist(struct kvs_server_s *server, char* key, int len_key) ;
kvs_result_t kvs_server_hset(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) ;
kvs_result_t kvs_server_hget(struct kvs_server_s *server, char* key, int len_key, char** value_out, int* len_val_out) ;
kvs_result_t kvs_server_hdel(struct kvs_server_s *server, char* key, int len_key) ;
kvs_result_t kvs_server_hmod(struct kvs_server_s *server, char* key, int len_key, char* value, int len_val) ;
kvs_result_t kvs_server_hexist(struct kvs_server_s *server, char* key, int len_key) ;





kvs_status_t kvs_master_init(struct kvs_master_s *master, struct kvs_server_s *server, struct kvs_master_config_s *config);
kvs_status_t kvs_master_deinit(struct kvs_master_s *master);
kvs_status_t kvs_master_convert_connection_to_slave(struct kvs_master_s *master, struct kvs_conn_s *conn);
kvs_status_t kvs_master_slave_sync_start(struct kvs_master_s *master, struct kvs_conn_s *conn);





kvs_status_t kvs_slave_init(struct kvs_slave_s *slave, struct kvs_server_s *server,struct kvs_slave_config_s *config);
kvs_status_t kvs_slave_deinit(struct kvs_slave_s *slave);
kvs_status_t kvs_slave_connect_master(struct kvs_slave_s *slave);


#endif