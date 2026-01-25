#ifndef __KVS_SERVER_H__
#define __KVS_SERVER_H__
#include <stdlib.h>
#include <liburing.h>
#include <sys/signalfd.h>
#include <signal.h>

//#include "kvs_proactor.h"
#include "common.h"
#include "kvs_conn.h"
#include "kvs_network.h"
#include "kvs_event_loop.h"
#include "kvs_rdma_engine.h"
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
struct ibv_mr;

typedef kvs_status_t (*kvs_proto_parser_pt)(char* msg, int len, struct kvs_handler_cmd_s *cmd, int *parsed_len);
typedef kvs_result_t (*kvs_proto_executor_pt)(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
typedef kvs_status_t (*kvs_proto_response_pt)(kvs_result_t result, char *value, int len_val, struct kvs_conn_s *conn);

struct kvs_protocol_s {
    kvs_proto_parser_pt protocol_parser;
    kvs_proto_executor_pt execute_command;
    kvs_proto_response_pt format_response;
};

struct kvs_master_config_s {
    int max_slave_count;
    size_t repl_backlog_size;
};

struct kvs_slave_config_s {
    const char *master_ip;
    unsigned short master_port;
    int rdb_recv_buffer_count;
};

struct kvs_server_config_s {
    int role;
    struct kvs_master_config_s master_config;
    struct kvs_slave_config_s slave_config;
    struct kvs_pers_config_s pers_config;
    struct kvs_protocol_s protocol;

    int use_rdma;
    size_t rdma_max_chunk_size;
    //int io_uring_entries;
};


typedef enum {
    KVS_MY_MASTER_NONE = 0,

    // tcp
    KVS_MY_MASTER_CONNECTING,     // 正在 TCP 连接
    KVS_MY_MASTER_SENDING_SYNC,     // 正在发送 SYNC 命令
    KVS_MY_MASTER_WAITING_RES,      // 正在等待 +FULLRESYNC...
    //KVS_REPL_MASTER_HANDSHAKE,      // 正在进行 PING/AUTH 握手

    // rdma
    KVS_MY_MASTER_RDMA_CONNECTING, // 正在 RDMA 建联
    KVS_MY_MASTER_RECEIVING_RDB,   // 正在接收 RDB 文件流
    KVS_MY_MASTER_RECEIVING_REPLICATION, // 正在接收实时增量命令
    KVS_MY_MASTER_ONLINE,          // 握手完成，正在接收实时增量命令
    KVS_MY_MASTER_STATE_NUM
} kvs_my_master_state_t;

// [场景：我是 Master，对方是 Slave] - 管理下发进度
typedef enum {
    KVS_MY_SLAVE_NONE = 0,
    KVS_MY_SLAVE_WAIT_BGSAVE_END, // 等待后台 RDB 进程结束
    KVS_MY_SLAVE_WAIT_RDMA_READY,   // 等待 Slave RDMA 准备就绪
    KVS_MY_SLAVE_WAIT_RECV_READY,   // 等待 Slave 准备接收 RDB
    KVS_MY_SLAVE_SENDING_RDB,     // 正在发送 RDB 文件流
    KVS_MY_SLAVE_WAIT_RDB_ACK,    // 等待 Slave RDB 接收完成确认
    KVS_MY_SLAVE_SENDING_BACKLOG, // 正在发送 Backlog 积压数据
    KVS_MY_SLAVE_ONLINE,           // 实时同步状态
    KVS_MY_SLAVE_OFFLINE,          // 离线状态
    KVS_MY_SLAVE_STATE_NUM
} kvs_my_slave_state_t;

// // 在 kvs_server.h 中定义
// typedef enum {
//     // 0. 初始状态
//     REP_STATE_NONE,
    
//     // 1. TCP 阶段
//     REP_STATE_CONNECTING,      // 正在 TCP 三次握手
//     REP_STATE_SENDING_SYNC,    // 握手完成，正在发 "SYNC\r\n"
//     REP_STATE_WAITING_RES,     // 发完了，正在等 "+FULLRESYNC..."
    
//     // 2. RDMA 阶段
//     REP_STATE_RDMA_CONNECTING, // 收到 Token，正在 RDMA 建联
//     REP_STATE_TRANSFERRING,    // 正在接收 RDB
//     REP_STATE_ONLINE           // 同步完成，进入命令传播
// } kvs_my_master_state_t;

// [场景：普通客户端]
typedef enum {
    KVS_CLIENT_STATE_NORMAL = 0,
    KVS_CLIENT_STATE_WAIT_BGSAVE,   // 客户端发了 SAVE，正在阻塞等待
    KVS_CLIENT_STATE_CLOSE_PENDING,  // 准备关闭（比如发完最后一个回复后）
    KVS_CLIENT_STATE_NUM
} kvs_client_state_t;

typedef enum {
	KVS_RDB_START = 0,
	KVS_RDB_ARRAY = 0,
	KVS_RDB_RBTREE,
	KVS_RDB_HASH,
	KVS_RDB_END
} kvs_server_rdb_dbtype_t;



typedef enum {
    KVS_CTX_IDLE = 0,
    KVS_CTX_NORMAL_CLIENT,  // 普通客户端
    KVS_CTX_SLAVE_OF_ME,    // 我是 Master，这个连接是我的 Slave
    KVS_CTX_MASTER_OF_ME    // 我是 Slave，这个连接是我的 Master
} kvs_ctx_type_t;

// (V-Table)
struct kvs_ops_s {
    kvs_status_t (*on_recv)(struct kvs_conn_s *conn, int *read_size);
    kvs_status_t (*on_send)(struct kvs_conn_s *conn, int bytes_sent);
    void (*on_close)(struct kvs_conn_s *conn);
    void (*on_extra_event)(struct kvs_conn_s *conn, int event_type, void *data);
    const char *name; 
};

struct kvs_ctx_header_s{
    kvs_ctx_type_t type;
	void* next_handler;
	struct kvs_ops_s ops;
};

// context for connections from slaves to me (the master)
// used in struct kvs_conn_s->user_data
struct kvs_my_slave_context_s {
    struct kvs_ctx_header_s header;
    kvs_my_slave_state_t state;

    struct kvs_master_s *master; // back reference to master
    int slave_idx;
    size_t rdb_size;
    size_t repl_backlog_offset;
    size_t rdb_offset;
    char *recv_buf;
    struct kvs_rdma_mr_s *recv_mr;
    uint64_t rdma_token;

    int ref_count; // rdma_conn may share this context with tcp_conn
    struct kvs_rdma_conn_s *rdma_conn; // for rdma connection
    struct kvs_conn_s *tcp_conn; // for tcp connection

    int send_rdb_chunk_size_cur;
};

// context for connections from me (the slave) to master
// used in struct kvs_conn_s->user_data
struct kvs_my_master_context_s {
    struct kvs_ctx_header_s header;
    kvs_my_master_state_t state;
    struct kvs_slave_s *slave; // back reference to master

    int rdma_port;
    uint64_t token;
    size_t rdb_size;
    struct kvs_conn_s *rdma_conn; // for rdma connection
    struct kvs_conn_s *tcp_conn; // for tcp connection
    struct kvs_event_s connect_ev; // for tcp connect event

    char *rdb_recv_buffer;
    int rdb_recv_buffer_count;
    size_t rdb_recv_buf_sz;
    struct kvs_rdma_mr_s *rdb_recv_mr;
    int rdb_fd;
    size_t rdb_offset;
    struct kvs_event_s rdb_write_ev;
    struct kvs_event_s rdb_fsync_ev;
    int rdb_recv_buf_offset_cur;
    int rdb_recv_len_cur;
    int rdb_imm_data_cur;
    
    //int is_post_connect; // whether RDMA connection is established
};


struct kvs_client_context_s {
    struct kvs_ctx_header_s header;
    kvs_client_state_t state;
};

struct kvs_slave_s {
    int master_fd;
    const char *master_ip;
    unsigned short master_port;
    //kvs_my_master_state_t state; // SYNC_HANDSHAKE, SYNC_RDB, SYNC_AOF, SYNC_CONNECTED
    struct kvs_server_s *server;
    int rdb_recv_buffer_count;
};


struct kvs_master_s {
    struct kvs_conn_s **slave_conns; // array of slave connections
    int slave_count;
    int max_slave_count;
    int slave_count_online;
    char *repl_backlog;
    size_t repl_backlog_size;
    int is_repl_backlog; // 1: write command to repl backlog, 0: not write
    size_t repl_backlog_idx;
    int repl_backlog_overflow; // 1: overflow, 0: not overflow

    
    int rdb_fd;

    // RDMA related
    char *rdb_mmap;
    size_t rdb_size;
    //struct ibv_mr *rdb_mr;
    struct kvs_rdma_mr_s *rdb_mr;

    struct kvs_session_table_s session_table;

    int syncing_slaves_count; // number of slaves in SYNC process

    struct kvs_server_s *server;
};




struct kvs_server_s {
    struct kvs_loop_s loop;
    struct kvs_network_s network;
    struct kvs_rdma_engine_s rdma_engine;
    int use_rdma;
    size_t rdma_max_chunk_size;

    struct kvs_pers_context_s *pers_ctx;
    //kvs_mp_pool_t *mempool; // common.c
    struct hashtable_s *hash;
    struct kvs_array_s *array;
    struct _rbtree *rbtree;

    struct kvs_master_s* master;
    struct kvs_slave_s* slave;

    // rdb saving child process pid
    pid_t rdb_child_pid;
    int signal_fd;
    struct signalfd_siginfo signal_info;
    struct kvs_event_s signal_ev;


    struct kvs_event_s aof_timer_ev;    
    struct __kernel_timespec aof_ts;

    struct kvs_protocol_s protocol;


    int rdb_fd;
    off_t rdb_file_size;

    int role; // master/slave
};



int kvs_server_init(struct kvs_server_s *server, struct kvs_server_config_s *config_pt);
int kvs_server_deinit(struct kvs_server_s *server);


/*
* @return: command processing result code defined in enum KVS_RESPONSE_CODE
*/
typedef kvs_status_t(*kvs_server_cmd_executor)(struct kvs_server_s* server, char* msg, int length, int* parsed_length);
kvs_status_t kvs_server_storage_recovery(struct kvs_server_s *server);

// int kvs_server_restore_entry(char data_type, char* key, int len_key, char* value, int len_val, void* arg);
int kvs_server_save_rdb(struct kvs_server_s *server);
kvs_status_t kvs_server_save_rdb_fork(struct kvs_server_s *server);


int kvs_rdb_child_process(struct kvs_server_s *server);

kvs_status_t kvs_server_init_connection(struct kvs_server_s *server, struct kvs_conn_s *conn);
kvs_status_t kvs_server_deinit_connection(struct kvs_server_s *server, struct kvs_conn_s *conn);





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
kvs_status_t kvs_master_slave_state_machine_tick(struct kvs_master_s *master, struct kvs_conn_header_s *slave_conn, kvs_event_trigger_t trigger);
kvs_status_t kvs_master_propagate_command_to_slaves(struct kvs_master_s *master, struct kvs_handler_cmd_s *cmd);




kvs_status_t kvs_slave_init(struct kvs_slave_s *slave, struct kvs_server_s *server,struct kvs_slave_config_s *config);
kvs_status_t kvs_slave_deinit(struct kvs_slave_s *slave);
kvs_status_t kvs_slave_connect_master(struct kvs_slave_s *slave);
kvs_status_t kvs_slave_master_state_machine_tick(struct kvs_slave_s *slave, struct kvs_conn_header_s *master_conn, kvs_event_trigger_t trigger);


kvs_status_t kvs_client_state_machine_tick(struct kvs_server_s *server, struct kvs_conn_s *conn);



int kvs_hander_on_accept(struct kvs_conn_s *conn);
int kvs_handler_on_msg(struct kvs_conn_s *conn, int *read_size);
int kvs_handler_on_send(struct kvs_conn_s *conn, int bytes_sent);
int kvs_handler_on_accept(struct kvs_conn_s *conn);
int kvs_handler_on_close(struct kvs_conn_s *conn);

int kvs_handler_on_rdma_connect_before(struct kvs_rdma_conn_s *conn);
int kvs_handler_on_rdma_connect_request(struct kvs_rdma_conn_s *conn);
int kvs_handler_on_rdma_established(struct kvs_rdma_conn_s *conn);
int kvs_handler_on_rdma_disconnected(struct kvs_rdma_conn_s *conn);
int kvs_handler_on_rdma_cq_recv(struct kvs_rdma_conn_s *conn, size_t recv_off_set, int recv_len, int imm_data, void* user_data);
int kvs_handler_on_rdma_cq_send(struct kvs_rdma_conn_s *conn, size_t send_off_set, int send_len, void* user_data);
void kvs_handler_on_rdma_error(struct kvs_rdma_conn_s *conn, int event_type, int err);


/************************kvs server internal method*****************************/
kvs_status_t kvs_server_create_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *conn, kvs_ctx_type_t ctx_type);
kvs_status_t kvs_server_destroy_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *conn);
kvs_status_t kvs_server_convert_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *conn, kvs_ctx_type_t new_type);
kvs_status_t kvs_server_share_conn_type(struct kvs_server_s *server, struct kvs_conn_header_s *src_conn, struct kvs_conn_header_s *dst_conn);


int kvs_server_init_signals(struct kvs_server_s *server);
int kvs_server_on_rdb_save_finish(struct kvs_server_s *server, kvs_status_t status);
int kvs_server_load_rdb(struct kvs_server_s *server);
kvs_status_t kvs_server_init_aof_timer(struct kvs_server_s *server) ;


kvs_status_t kvs_master_remove_slave(struct kvs_master_s *master, struct kvs_conn_s *conn);


kvs_status_t kvs_client_on_recv(struct kvs_conn_s *conn, int *read_size);
kvs_status_t kvs_client_on_send(struct kvs_conn_s *conn, int bytes_sent);
void kvs_client_on_close(struct kvs_conn_s *conn);

kvs_status_t kvs_my_slave_on_recv(struct kvs_conn_s *conn, int *read_size);
kvs_status_t kvs_my_slave_on_send(struct kvs_conn_s *conn, int bytes_sent);
void kvs_my_slave_on_close(struct kvs_conn_s *conn);
kvs_status_t kvs_my_slave_on_rdma_send(struct kvs_rdma_conn_s *conn,   size_t send_off_set, int send_len);
kvs_status_t kvs_my_slave_on_rdma_recv(struct kvs_rdma_conn_s *conn);

kvs_status_t kvs_my_master_on_recv(struct kvs_conn_s *conn, int *read_size);
kvs_status_t kvs_my_master_on_send(struct kvs_conn_s *conn, int bytes_sent);
void kvs_my_master_on_close(struct kvs_conn_s *conn);
kvs_status_t kvs_my_master_on_rdma_send(struct kvs_rdma_conn_s *conn,  size_t send_off_set, int send_len);
kvs_status_t kvs_my_master_on_rdma_recv(struct kvs_rdma_conn_s *conn);

typedef kvs_status_t (*kvs_cmd_handler_pt)(struct kvs_server_s *s, 
                                           struct kvs_handler_cmd_s *cmd, 
                                           struct kvs_conn_s *conn);

kvs_status_t kvs_server_msg_pump(struct kvs_conn_s *conn, int *read_size, kvs_cmd_handler_pt handler);

void slave_start_replication(struct kvs_slave_s *slave);








#endif