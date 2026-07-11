#include "kvs_resp_protocol.h"
#include "kvs_types.h"
#include "common.h"
#include "logger.h"

#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <limits.h>


static const int command_type[] = {
	// hash
	[KVS_CMD_SET] = KVS_CMD_WRITE,
	[KVS_CMD_SETEX] = KVS_CMD_WRITE,
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
	// array
	[KVS_CMD_ASET] = KVS_CMD_WRITE,
	[KVS_CMD_AGET] = KVS_CMD_READ,
	[KVS_CMD_ADEL] = KVS_CMD_WRITE,
	[KVS_CMD_AMOD] = KVS_CMD_WRITE,
	[KVS_CMD_AEXIST] = KVS_CMD_READ,
	// vector
	[KVS_CMD_CREATEV] = KVS_CMD_WRITE,
	[KVS_CMD_SETV] = KVS_CMD_WRITE,
	[KVS_CMD_GETV] = KVS_CMD_READ,
	[KVS_CMD_DELV] = KVS_CMD_WRITE,
	[KVS_CMD_VINFO] = KVS_CMD_READ,
	//save
	[KVS_CMD_SAVE] = KVS_CMD_OTHER,
	//slave sync
	[KVS_CMD_SLAVE_SYNC] = KVS_CMD_OTHER,
	[KVS_CMD_SLAVE_SYNC_RDMA] = KVS_CMD_OTHER,

	[KVS_CMD_ECHO] = KVS_CMD_OTHER
};

static inline char cmd_upper(char c) {
	return (c >= 97 && c <= 122) ? (char)(c - 32) : c;
}

static inline int lookup_command(char *cmd, int len, kvs_command_type_t *type) {
	kvs_command_t idx = KVS_CMD_INVALID;
	*type = KVS_CMD_OTHER;

	switch(len) {
		case 3: {
			char c0 = cmd_upper(cmd[0]);
			char c1 = cmd_upper(cmd[1]);
			char c2 = cmd_upper(cmd[2]);
			if(c0 == 83 && c1 == 69 && c2 == 84) idx = KVS_CMD_SET;
			else if(c0 == 71 && c1 == 69 && c2 == 84) idx = KVS_CMD_GET;
			else if(c0 == 68 && c1 == 69 && c2 == 76) idx = KVS_CMD_DEL;
			else if(c0 == 77 && c1 == 79 && c2 == 68) idx = KVS_CMD_MOD;
			break;
		}
		case 4: {
			char c0 = cmd_upper(cmd[0]);
			char c1 = cmd_upper(cmd[1]);
			char c2 = cmd_upper(cmd[2]);
			char c3 = cmd_upper(cmd[3]);
			if(c0 == 69 && c1 == 67 && c2 == 72 && c3 == 79) idx = KVS_CMD_ECHO;
			else if(c0 == 83 && c1 == 69 && c2 == 84 && c3 == 86) idx = KVS_CMD_SETV;
			else if(c0 == 71 && c1 == 69 && c2 == 84 && c3 == 86) idx = KVS_CMD_GETV;
			else if(c0 == 68 && c1 == 69 && c2 == 76 && c3 == 86) idx = KVS_CMD_DELV;
			else if(c0 == 83 && c1 == 65 && c2 == 86 && c3 == 69) idx = KVS_CMD_SAVE;
			else if(c0 == 83 && c1 == 89 && c2 == 78 && c3 == 67) idx = KVS_CMD_SLAVE_SYNC;
			else if((c0 == 82 || c0 == 65) && c1 == 83 && c2 == 69 && c3 == 84) idx = c0 == 82 ? KVS_CMD_RSET : KVS_CMD_ASET;
			else if((c0 == 82 || c0 == 65) && c1 == 71 && c2 == 69 && c3 == 84) idx = c0 == 82 ? KVS_CMD_RGET : KVS_CMD_AGET;
			else if((c0 == 82 || c0 == 65) && c1 == 68 && c2 == 69 && c3 == 76) idx = c0 == 82 ? KVS_CMD_RDEL : KVS_CMD_ADEL;
			else if((c0 == 82 || c0 == 65) && c1 == 77 && c2 == 79 && c3 == 68) idx = c0 == 82 ? KVS_CMD_RMOD : KVS_CMD_AMOD;
			break;
		}
		case 5:
			if(cmd_upper(cmd[0]) == 86 && cmd_upper(cmd[1]) == 73 && cmd_upper(cmd[2]) == 78 && cmd_upper(cmd[3]) == 70 && cmd_upper(cmd[4]) == 79) idx = KVS_CMD_VINFO;
			else if(cmd_upper(cmd[0]) == 69 && cmd_upper(cmd[1]) == 88 && cmd_upper(cmd[2]) == 73 && cmd_upper(cmd[3]) == 83 && cmd_upper(cmd[4]) == 84) idx = KVS_CMD_EXIST;
			else if(cmd_upper(cmd[0]) == 83 && cmd_upper(cmd[1]) == 69 && cmd_upper(cmd[2]) == 84 && cmd_upper(cmd[3]) == 69 && cmd_upper(cmd[4]) == 88) idx = KVS_CMD_SETEX;
			break;
		case 7:
			if(cmd_upper(cmd[0]) == 67 && cmd_upper(cmd[1]) == 82 && cmd_upper(cmd[2]) == 69 && cmd_upper(cmd[3]) == 65 && cmd_upper(cmd[4]) == 84 && cmd_upper(cmd[5]) == 69 && cmd_upper(cmd[6]) == 86) idx = KVS_CMD_CREATEV;
			break;
		case 6: {
			char c0 = cmd_upper(cmd[0]);
			if((c0 == 82 || c0 == 65) && cmd_upper(cmd[1]) == 69 && cmd_upper(cmd[2]) == 88 && cmd_upper(cmd[3]) == 73 && cmd_upper(cmd[4]) == 83 && cmd_upper(cmd[5]) == 84) idx = c0 == 82 ? KVS_CMD_REXIST : KVS_CMD_AEXIST;
			break;
		}
		case 9:
			if(cmd_upper(cmd[0]) == 83 && cmd_upper(cmd[1]) == 89 && cmd_upper(cmd[2]) == 78 && cmd_upper(cmd[3]) == 67 && cmd[4] == 95 && cmd_upper(cmd[5]) == 82 && cmd_upper(cmd[6]) == 68 && cmd_upper(cmd[7]) == 77 && cmd_upper(cmd[8]) == 65) idx = KVS_CMD_SLAVE_SYNC_RDMA;
			break;
	}

	if(idx != KVS_CMD_INVALID) {
		*type = command_type[idx];
	}
	return idx;
}

typedef enum {
	KVS_SET_FAST_NOT_MATCH = 0,
	KVS_SET_FAST_OK,
	KVS_SET_FAST_AGAIN,
	KVS_SET_FAST_ERR
} kvs_set_fast_result_t;

static inline kvs_set_fast_result_t parse_set_bulk(const char **cursor, const char *end,
		char **value_out, int *length_out) {
	const char *p = *cursor;
	int value_len = 0;

	if(p >= end) return KVS_SET_FAST_AGAIN;
	if(*p++ != '$') return KVS_SET_FAST_ERR;
	if(p >= end) return KVS_SET_FAST_AGAIN;
	if(*p < '0' || *p > '9') return KVS_SET_FAST_ERR;

	do {
		int digit = *p++ - '0';
		if(value_len > (INT_MAX - digit) / 10) return KVS_SET_FAST_ERR;
		value_len = value_len * 10 + digit;
	} while(p < end && *p >= '0' && *p <= '9');

	if(p >= end) return KVS_SET_FAST_AGAIN;
	if(*p++ != '\r') return KVS_SET_FAST_ERR;
	if(p >= end) return KVS_SET_FAST_AGAIN;
	if(*p++ != '\n') return KVS_SET_FAST_ERR;
	if((size_t)(end - p) < (size_t)value_len + 2) return KVS_SET_FAST_AGAIN;

	*value_out = (char *)p;
	*length_out = value_len;
	p += value_len;
	if(p[0] != '\r' || p[1] != '\n') return KVS_SET_FAST_ERR;

	*cursor = p + 2;
	return KVS_SET_FAST_OK;
}

static inline kvs_set_fast_result_t try_parse_set_fast(char *msg, int length,
		struct kvs_handler_cmd_s *cmd_pt, int *parsed_length) {
	static const char set_prefix[] = "*3\r\n$3\r\nSET\r\n";
	const int prefix_len = (int)(sizeof(set_prefix) - 1);
	const char *cursor;
	const char *end;
	kvs_set_fast_result_t ret;

	if(length < prefix_len || memcmp(msg, set_prefix, sizeof(set_prefix) - 1) != 0) {
		return KVS_SET_FAST_NOT_MATCH;
	}

	cursor = msg + prefix_len;
	end = msg + length;
	ret = parse_set_bulk(&cursor, end, &cmd_pt->key, &cmd_pt->len_key);
	if(ret != KVS_SET_FAST_OK) return ret;
	ret = parse_set_bulk(&cursor, end, &cmd_pt->val, &cmd_pt->len_val);
	if(ret != KVS_SET_FAST_OK) return ret;

	cmd_pt->raw_ptr = msg;
	cmd_pt->raw_len = (int)(cursor - msg);
	cmd_pt->cmd = msg + 8;
	cmd_pt->len_cmd = 3;
	cmd_pt->cmd_idx = KVS_CMD_SET;
	cmd_pt->cmd_type = KVS_CMD_WRITE;
	cmd_pt->argc = 3;
	cmd_pt->argv[0] = cmd_pt->cmd;
	cmd_pt->argv_len[0] = cmd_pt->len_cmd;
	cmd_pt->argv[1] = cmd_pt->key;
	cmd_pt->argv_len[1] = cmd_pt->len_key;
	cmd_pt->argv[2] = cmd_pt->val;
	cmd_pt->argv_len[2] = cmd_pt->len_val;
	*parsed_length = cmd_pt->raw_len;

	return KVS_SET_FAST_OK;
}


kvs_status_t kvs_resp_parser(char* msg, int length, struct kvs_handler_cmd_s *cmd_pt, int *parsed_length) {
	    if(cmd_pt == NULL || msg == NULL || length <= 0 || parsed_length == NULL /*|| idx < 0*/)  {
			LOG_DEBUG("invalid argument");
			return KVS_ERR;
		}
		kvs_set_fast_result_t fast_ret = try_parse_set_fast(msg, length, cmd_pt, parsed_length);
		if(fast_ret == KVS_SET_FAST_OK) return KVS_OK;
		if(fast_ret == KVS_SET_FAST_AGAIN) return KVS_AGAIN;
		if(fast_ret == KVS_SET_FAST_ERR) return KVS_ERR;

		int idx = 0;
	if(length < 3) return KVS_AGAIN; // at least "*1\r\n$3\r\n"
	if(msg[idx] != '*') {
		LOG_ERROR("msg[0]: %c, length: %d", msg[0], length);
		LOG_DEBUG("invalid protocol: no '*'");
		return KVS_ERR;
	}
	cmd_pt->raw_ptr = &msg[idx];
	int idx_bk = idx;
	//printf("%s:%d command parsering\n", __FILE__, __LINE__);
	idx ++ ;
	
	int len_arr = kvs_parse_int(msg, length, &idx);
	//printf("len_arr: %d idx:%d length:%d\n", len_arr, idx, length);
	if(idx + 1 >= length) return KVS_AGAIN;
	if(msg[idx] != '\r' || msg[idx + 1] != '\n') {
		LOG_DEBUG("invalid protocol: no \\r\\n after array length");
		return KVS_ERR;
	} 
	idx += 2;
	//printf("%s:%d command parsering\n", __FILE__, __LINE__);
	cmd_pt->argc = len_arr;
	if(len_arr > KVS_CMD_MAX_ARGC) return KVS_ERR;
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

		cmd_pt->argv[i] = str;
		cmd_pt->argv_len[i] = len;

		if(0 == i) cmd_pt->cmd = str, cmd_pt->len_cmd = len;
		else if(1 == i) cmd_pt->key = str, cmd_pt->len_key = len;
		else if(2 == i) {
			if(len_arr == 4) cmd_pt->ttl = str, cmd_pt->len_ttl = len;
			else cmd_pt->val = str, cmd_pt->len_val = len;
		} else if(3 == i) cmd_pt->val = str, cmd_pt->len_val = len;
	}
	
	if(i != len_arr) return KVS_ERR;
	cmd_pt->raw_len = idx-idx_bk;

	cmd_pt->cmd_idx = lookup_command(cmd_pt->cmd, cmd_pt->len_cmd, &cmd_pt->cmd_type);

	
	*parsed_length = idx;
	return KVS_OK;
}
