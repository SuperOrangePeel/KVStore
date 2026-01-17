#ifndef __KVS_HANDLER_H__
#define __KVS_HANDLER_H__

#include "kvs_types.h"

// protocol
#define KVS_1R1R 	0// one request one response 
#define KVS_RESP	1
#define KVS_PROTOCOL_SELECT KVS_RESP

#define KVS_PERSISTENCE 1

struct kvs_conn_s;
struct kvs_server_s;


typedef kvs_status_t (*kvs_handler_protocol_pt)(char* msg, int len, struct kvs_handler_cmd_s *cmd, int *parsed_len);
typedef kvs_result_t (*kvs_handler_executor_pt)(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);

struct kvs_handler_s {
    kvs_handler_protocol_pt protocol_parser;
    kvs_handler_executor_pt execute_command;
};





#define KVS_REPL_BACKLOG_SIZE (64 *1024 * 1024) // 1MB

/**
 * @brief Handle incoming message from client
 * 
 * @param conn Connection object
 * @return int 
 */
int kvs_handler_on_msg(struct kvs_conn_s *conn);

int kvs_handler_on_send(struct kvs_conn_s *conn, int bytes_sent);
int kvs_handler_on_accept(struct kvs_conn_s *conn);
int kvs_handler_on_timer(void *global_ctx);
int kvs_handler_on_close(struct kvs_conn_s *conn);

// There are only two hard things in computer science: cache invalidation and naming things. !!

int kvs_handler_process_raw_buffer(struct kvs_server_s* server, char* buf, int len, int *parsed_length);

/**
 * @return enum KVS_RESPONSE_CODE
 */
int kvs_handler_register_master(struct kvs_conn_s *conn, int client_fd, struct kvs_server_s *server);

#endif