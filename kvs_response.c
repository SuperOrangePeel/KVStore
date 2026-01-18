#include "kvs_response.h"

#include "kvs_network.h"
#include "logger.h"
#include "kvs_types.h"

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>


kvs_status_t kvs_format_response(int result, char *value, int len_val, struct kvs_conn_s *conn) {
	if(conn == NULL) return KVS_ERR;
	switch(result) {
		case KVS_RES_OK:
			//kvs_proactor_set_send_event(conn, "+OK\r\n", 5);
			kvs_net_copy_msg_to_send_buf(conn, "+OK\r\n", 5);
			break;
		case KVS_RES_EXIST:
			kvs_net_copy_msg_to_send_buf(conn, "-EXIST\r\n", 9);
			break;
		case KVS_RES_NOT_FOUND:
			kvs_net_copy_msg_to_send_buf(conn, "-NOT FOUND\r\n", 13);
			break;
		case KVS_RES_VAL: {
			if(value == NULL || len_val <= 0) {
				kvs_net_copy_msg_to_send_buf(conn, "-ERROR\r\n", 8);
				break;
			}
			char header[64];
			int header_len = snprintf(header, sizeof(header), "$%d\r\n", len_val);
			kvs_net_copy_msg_to_send_buf(conn, header, header_len);
			kvs_net_copy_msg_to_send_buf(conn, value, len_val);
			kvs_net_copy_msg_to_send_buf(conn, "\r\n", 2);
			break;
		}
		case KVS_RES_UNKNOWN_CMD:
			kvs_net_copy_msg_to_send_buf(conn, "-ERR unknown command\r\n", 22);
			break;
		default:
			LOG_FATAL("unknown result code: %d", result);
			assert(0);
			
	}
	return KVS_OK;
}