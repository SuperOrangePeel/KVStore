#include "kvs_resp_protocol.h"
#include "kvs_types.h"
#include "common.h"

#include <stdio.h>
#include <string.h>
#include <stddef.h>


const char *command[] = {
	"SET", "GET", "DEL", "MOD", "EXIST",
	"RSET", "RGET", "RDEL", "RMOD", "REXIST",
	"HSET", "HGET", "HDEL", "HMOD", "HEXIST",
	"SAVE", "SYNC"
};

const int command_type[] = {
	[KVS_CMD_SET] = KVS_CMD_WRITE,
	[KVS_CMD_GET] = KVS_CMD_READ,
	[KVS_CMD_DEL] = KVS_CMD_WRITE,
	[KVS_CMD_MOD] = KVS_CMD_WRITE,
	[KVS_CMD_EXIST] = KVS_CMD_READ,
	// rbtree
	[KVS_CMD_RSET] = KVS_CMD_WRITE,
	[KVS_CMD_RGET] = KVS_CMD_READ,
	[KVS_CMD_RDEL] = KVS_CMD_WRITE,
	[KVS_CMD_RMOD] = KVS_CMD_WRITE,
	[KVS_CMD_REXIST] = KVS_CMD_READ,
	// hash
	[KVS_CMD_HSET] = KVS_CMD_WRITE,
	[KVS_CMD_HGET] = KVS_CMD_READ,
	[KVS_CMD_HDEL] = KVS_CMD_WRITE,
	[KVS_CMD_HMOD] = KVS_CMD_WRITE,
	[KVS_CMD_HEXIST] = KVS_CMD_READ,
	//save
	[KVS_CMD_SAVE] = KVS_CMD_OTHER,
	//slave sync
	[KVS_SLAVE_SYNC] = KVS_CMD_OTHER,
};

const int command_length[] = {
	3, 3, 3, 3, 5,
	4, 4, 4, 4, 6,
	4, 4, 4, 4, 6,
	4, 4
};



kvs_status_t kvs_protocol(char* msg, int length, struct kvs_handler_cmd_s *cmd_pt, int *parsed_length) {
    if(cmd_pt == NULL || msg == NULL || length <= 0 || parsed_length == NULL /*|| idx < 0*/) return KVS_ERR;
	int idx = 0;
	if(msg[idx] != '*') return KVS_ERR;
	cmd_pt->raw_ptr = &msg[idx];
	int idx_bk = idx;
	//printf("%s:%d command parsering\n", __FILE__, __LINE__);
	idx ++ ;

	int len_arr = kvs_parse_int(msg, length, &idx);
	//printf("len_arr: %d idx:%d length:%d\n", len_arr, idx, length);
	if(idx + 1 >= length) return KVS_AGAIN;
	if(msg[idx] != '\r' || msg[idx + 1] != '\n') return KVS_ERR;
	idx += 2;
	//printf("%s:%d command parsering\n", __FILE__, __LINE__);
	int i = 0;
	for(; i < len_arr; ++ i) {
		// $
		if(idx >= length) return KVS_AGAIN;
		if(msg[idx] != '$') return KVS_ERR;
		idx ++ ;
		// length\r\n
		int len = kvs_parse_int(msg, length, &idx);
		if(idx + 1 >= length) return KVS_AGAIN;
		if(msg[idx] != '\r' || msg[idx + 1] != '\n') return KVS_ERR;
		idx += 2;
		if(idx + len + 1 >= length) return KVS_AGAIN;

		// real value\r\n
		char *str = &msg[idx];
		idx = idx + len;
		if(idx + 1 >= length) return KVS_AGAIN;
		if(msg[idx] != '\r' || msg[idx + 1] != '\n') return KVS_ERR;
		// printf("str:[%.*s], len:%d\n", len, str, len);
		idx += 2;

		if(0 == i) cmd_pt->cmd = str, cmd_pt->len_cmd = len;
		else if(1 == i) cmd_pt->key = str, cmd_pt->len_key = len;
		else if(2 == i) cmd_pt->val = str, cmd_pt->len_val = len;
		else {
			printf("%s:%d error\n", __FILE__, __LINE__);
		}
	}
	
	if(i != len_arr) return KVS_ERR;
	cmd_pt->raw_len = idx-idx_bk;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
		if(cmd_pt->len_cmd != command_length[cmd]) continue;
		if (memcmp(cmd_pt->cmd, command[cmd], cmd_pt->len_cmd) == 0) {
			cmd_pt->cmd_idx = cmd;
			cmd_pt->cmd_type = command_type[cmd];
			break;
		} 
	}
	if(cmd == KVS_CMD_COUNT) {
		// unknown command
		cmd_pt->cmd_idx = KVS_CMD_INVALID;
	}

	
	*parsed_length = idx;
	return KVS_OK;
}
