#ifndef __KVS_HANDLER_H__
#define __KVS_HANDLER_H__

// protocol
#define KVS_1R1R 	0// one request one response 
#define KVS_RESP	1
#define KVS_PROTOCOL_SELECT KVS_RESP

#define KVS_PERSISTENCE 1

struct kvs_conn_s;
struct kvs_server_s;


int kvs_protocol(struct kvs_conn_s *conn);

// There are only two hard things in computer science: cache invalidation and naming things. !!
/*
 * @return : -1 parser failed
 * process one command from raw buffer, used in AOF recovery
 */
int kvs_handler_process_raw_buffer(struct kvs_server_s* server, char* buf, int len, int *parsed_length);
#endif