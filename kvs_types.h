#ifndef __KVS_TYPES_H__
#define __KVS_TYPES_H__
 
/* ===========================SYSTEM STATUS CODES================================================ */

// system status codes
typedef enum {
    KVS_ERR = -1,// 系统错误（内存、IO、严重逻辑错误）
    KVS_OK = 0,    // 正常执行
	KVS_BREAK,    // 中断当前循环
    KVS_AGAIN,     // 数据不全，需重试
    KVS_QUIT,       // 需关闭连接
	KVS_STATUS_CONTINUE // 状态机继续
} kvs_status_t;

/* ===========================PROACTOR CONTEXT=================================================== */



//struct kvs_conn_s; 

// // (V-Table)
// struct kvs_ops_s {
//     kvs_status_t (*on_recv)(struct kvs_conn_s *conn, int *read_size);
//     kvs_status_t (*on_send)(struct kvs_conn_s *conn, int bytes_sent);
//     void (*on_close)(struct kvs_conn_s *conn);
//     void (*on_extra_event)(struct kvs_conn_s *conn, int event_type, void *data);
//     const char *name; 
// };

// struct kvs_ctx_header_s{
//     kvs_ctx_type_t type;
// 	void* next_handler;
// 	struct kvs_ops_s ops;
// };

/* ============================RESULT CODES=================================================== */

typedef enum {
	// success 
	KVS_RES_OK = 0,
	KVS_RES_VAL,
	
	// business status
	KVS_RES_NOT_FOUND, 
	KVS_RES_EXIST,

	// error
	KVS_RES_ERR,
	KVS_RES_UNKNOWN_CMD,

	// special control status
	KVS_RES_SYNC_SLAVE,
	KVS_RES_RDB_SKIP_RESPONSE
	
} kvs_result_t;

/* ===========================COMMANDS=================================================== */

typedef enum {
    KVS_CMD_INVALID = -1,
	KVS_CMD_START = 0,
	// array
	KVS_CMD_SET = KVS_CMD_START,
	KVS_CMD_GET,
	KVS_CMD_DEL,
	KVS_CMD_MOD,
	KVS_CMD_EXIST,
	// rbtree
	KVS_CMD_RSET,
	KVS_CMD_RGET,
	KVS_CMD_RDEL,
	KVS_CMD_RMOD,
	KVS_CMD_REXIST,
	// hash
	KVS_CMD_HSET,
	KVS_CMD_HGET,
	KVS_CMD_HDEL,
	KVS_CMD_HMOD,
	KVS_CMD_HEXIST,
	//save
	KVS_CMD_SAVE,
	//slave sync
	KVS_CMD_SLAVE_SYNC,
	KVS_CMD_SLAVE_SYNC_RDMA,
	KVS_CMD_COUNT,
	
} kvs_command_t;

typedef enum {
	KVS_CMD_READ = (1 << 0),
	KVS_CMD_WRITE = (1 << 1),
	KVS_CMD_OTHER = (1 << 2),
} kvs_command_type_t;

struct kvs_handler_cmd_s {
    kvs_command_t cmd_idx; // command index
	kvs_command_type_t cmd_type; // 1: write command, 0: read command
	char *raw_ptr;
	int raw_len;
	
	char* cmd;
	int len_cmd;
	char* key;
	int len_key;
	char* val;
	int len_val;
};

/* ===========================STATE MACHINE EVENT TRIGGERS=================================================== */
typedef enum {
	KVS_EVENT_TRIGGER_MANUAL = 0, // manual trigger 非io事件触发, 如sync命令触发，需要优化
	KVS_EVENT_CONNECTED,    // 连接建立完成
    KVS_EVENT_READ_READY,   // 收到数据了
    KVS_EVENT_WRITE_DONE,   // 数据发完了
	KVS_EVENT_CONTINUE,      // 状态机继续运行
	//KVS_EVENT_RDMA_ESTABLISHED, // 连接建立完成
	KVS_EVENT_BGSAVE_DONE, // 后台 RDB 保存完成
    KVS_EVENT_ERROR         // 出错了
} kvs_event_trigger_t;

#endif