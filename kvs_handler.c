#include "kvs_handler.h"

#include "kvs_persistence.h"
#include "kvs_server.h"

#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"

#include "proactor.h"
#include "common.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
 #include <sys/stat.h>

#if (KVS_PROTOCOL_SELECT == KVS_RESP)

#define KVS_RESP_CMD_MAX 1048576 // 1024 * 1024 = 16MB


typedef struct kvs_resp_cmd_s {
	int cmd_type;
	char *raw_ptr;
	int raw_len;
	
	char* cmd;
	int len_cmd;
	char* key;
	int len_key;
	char* val;
	int len_val;
} kvs_resp_cmd_t;

static kvs_resp_cmd_t kvs_resp_cmds[KVS_RESP_CMD_MAX];

#endif



const char *command[] = {
	"SET", "GET", "DEL", "MOD", "EXIST",
	"RSET", "RGET", "RDEL", "RMOD", "REXIST",
	"HSET", "HGET", "HDEL", "HMOD", "HEXIST",
	"SAVE", "SYNC"
};

const int command_length[] = {
	3, 3, 3, 3, 5,
	4, 4, 4, 4, 6,
	4, 4, 4, 4, 6,
	4, 4
};

enum {
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
	KVS_SLAVE_SYNC,
	
	KVS_CMD_COUNT,
};

static int kvs_write_cmd[] = {
	KVS_CMD_SET,  KVS_CMD_DEL,KVS_CMD_MOD,
	KVS_CMD_RSET, KVS_CMD_RDEL,KVS_CMD_RMOD,
	KVS_CMD_HSET, KVS_CMD_HDEL,KVS_CMD_HMOD,
	-1
};

enum KVS_RESPONSE_CODE {
	KVS_RESP_OK = 0,
	KVS_RESP_ERROR,
	KVS_RESP_EXIST,
	KVS_RESP_NOT_EXIST,
	KVS_RESP_VALUE,
	KVS_RESP_SYNC_SLAVE,
	KVS_RESP_UNKNOWN_CMD
};


// const char *response[] = {
// 	"+OK\r\n",
// 	"-ERROR\r\n",
// 	"+EXIST\r\n",
// 	"+NOT EXIST\r\n",
// 	"$%d\r\n"
// };


int kvs_split_token(char *msg, char *tokens[]) {

	if (msg == NULL || tokens == NULL) return -1;

	int idx = 0;
	char *token = strtok(msg, " ");
	
	while (token != NULL) {
		//printf("idx: %d, %s\n", idx, token);
		
		tokens[idx ++] = token;
		token = strtok(NULL, " ");
	}

	return idx;
}


// SET Key Value
// tokens[0] : SET
// tokens[1] : Key
// tokens[2] : Value
#if (KVS_PROTOCOL_SELECT == KVS_1R1R)
int kvs_execute_one_command(char **tokens, int count, char *response) {

	if (tokens[0] == NULL || count == 0 || response == NULL) return -1;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
		if (strcmp(tokens[0], command[cmd]) == 0) {
			break;
		} 
	}

	int length = 0;
	int ret = 0;
	char *key = tokens[1];
	char *value = tokens[2];

	switch(cmd) {
#if ENABLE_ARRAY
	case KVS_CMD_SET:
		ret = kvs_array_set(cur_array ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_GET: {
		char *result = kvs_array_get(cur_array, key);
		if (result == NULL) {
			length = sprintf(response, "NOT EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_DEL:
		ret = kvs_array_del(cur_array ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_mod(cur_array ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_exist(cur_array ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
#endif
	// rbtree
#if ENABLE_RBTREE
	case KVS_CMD_RSET:
		ret = kvs_rbtree_set(cur_rbtree ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_RGET: {
		char *result = kvs_rbtree_get(&global_rbtree, key);
		if (result == NULL) {
			length = sprintf(response, "NOT EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_RDEL:
		ret = kvs_rbtree_del(&global_rbtree ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_RMOD:
		ret = kvs_rbtree_mod(&global_rbtree ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_REXIST:
		ret = kvs_rbtree_exist(&global_rbtree ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
#endif
#if ENABLE_HASH
	case KVS_CMD_HSET:
		ret = kvs_hash_set(cur_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_HGET: {
		char *result = kvs_hash_get(cur_hash, key);
		if (result == NULL) {
			length = sprintf(response, "NOT EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_HDEL:
		ret = kvs_hash_del(cur_hash ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_mod(cur_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_exist(cur_hash ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
#endif

	default: 
		assert(0);
	}

	return length;
}
#elif(KVS_PROTOCOL_SELECT == KVS_RESP)

int kvs_format_response(int status, char *value, int len_val, char *response, int rsp_buf_len) {
	if (response == NULL) return -1;

	int length = 0;
	switch(status) {
		// todo : what if rsp_buf_len is not enough ?
		case KVS_RESP_OK:
			length = snprintf(response, rsp_buf_len, "+OK\r\n");
			break;
		case KVS_RESP_ERROR:
			length = snprintf(response, rsp_buf_len, "-ERROR\r\n");
			break;
		case KVS_RESP_EXIST:
			length = snprintf(response, rsp_buf_len, "-EXIST\r\n");
			break;
		case KVS_RESP_NOT_EXIST:
			length = snprintf(response, rsp_buf_len, "-NOT EXIST\r\n");
			break;
		case KVS_RESP_VALUE:
			{	
				if(value == NULL || len_val <= 0) {
					length = snprintf(response, rsp_buf_len, "-ERROR\r\n");
					break;
				}
				int r_len1 = snprintf(response, rsp_buf_len, "$%d\r\n", len_val);
				memcpy(response + r_len1, value, len_val);
				int r_len2 = snprintf(response + r_len1 + len_val, rsp_buf_len - r_len1 - len_val, "\r\n");
				length = len_val + r_len1 + r_len2;
			}
			break;
		case KVS_RESP_UNKNOWN_CMD:
			length = snprintf(response, rsp_buf_len, "-ERR unknown command\r\n");
			break;
		default:
			assert(0);
	}

	return length;
}


/*
* @return enum KVS_RESPONSE_CODE
*/
int kvs_execute_one_command(struct kvs_conn_s *conn, kvs_resp_cmd_t *cmd_p, char **value_out, int *len_val_out) {
	struct kvs_server_s *server = conn->server;
	kvs_array_t *cur_array = server->array;
	kvs_hash_t *cur_hash = server->hash;
	kvs_rbtree_t *cur_rbtree = server->rbtree;

	if (cmd_p == NULL) return -1;

	// int cmd = KVS_CMD_START;
	// for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
	// 	if(cmd_p->len_cmd != command_length[cmd]) continue;
	// 	if (memcmp(cmd_p->cmd, command[cmd], cmd_p->len_cmd) == 0) {
	// 		cmd_p->cmd_type = cmd;
	// 		break;
	// 	} 
	// }

	int ret = 0;
	char *key = cmd_p->key;
	int len_key = cmd_p->len_key;
	char *value = cmd_p->val;
	int len_val = cmd_p->len_val;

	// used for GET commands
	char *ret_val = NULL;
	int ret_val_len = 0;

	switch(cmd_p->cmd_type) {
#if ENABLE_ARRAY
	case KVS_CMD_SET:
		//printf("array SET command received, key: %.*s, value: %.*s\n", len_key, key, len_val, value);
		ret = kvs_array_resp_set(cur_array ,key, len_key, value, len_val);
		if (ret == -2) {
			return KVS_RESP_EXIST;
		} else if (ret >=0)  {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		} 
		
		break;
	case KVS_CMD_GET: {
		ret = kvs_array_resp_get(cur_array, key, len_key, &ret_val, &ret_val_len);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if(ret >= 0) {
			if(value_out == NULL || len_val_out == NULL) {
				return KVS_RESP_ERROR;
			}
			*value_out = ret_val;
			*len_val_out =  ret_val_len;
			return KVS_RESP_VALUE;
		} else {
			return KVS_RESP_ERROR;
		} 
		break;
	}
	case KVS_CMD_DEL:
		ret = kvs_array_resp_del(cur_array ,key, len_key);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_resp_mod(cur_array ,key, len_key, value, len_val);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_resp_exist(cur_array ,key, len_key);
		if (ret >= 0) {
			return KVS_RESP_EXIST;
		} else if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
#endif
	// rbtree
#if ENABLE_RBTREE
	case KVS_CMD_RSET:
		ret = kvs_rbtree_resp_set(cur_rbtree ,key, len_key, value, len_val);
		if(ret == -2) {
			return KVS_RESP_EXIST;
		} else if (ret >=0)  {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		} 
		
		break;
	case KVS_CMD_RGET: {
		ret = kvs_rbtree_resp_get(cur_rbtree, key, len_key, &ret_val, &ret_val_len);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if(ret >= 0) {
			if(value_out == NULL || len_val_out == NULL) {
				return KVS_RESP_ERROR;
			}
			*value_out = ret_val;
			*len_val_out =  ret_val_len;
			return KVS_RESP_VALUE;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	}
	case KVS_CMD_RDEL:
		ret = kvs_rbtree_resp_del(cur_rbtree ,key, len_key);
		if( ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_RMOD:
		ret = kvs_rbtree_resp_mod(cur_rbtree ,key, len_key, value, len_val);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
	case KVS_CMD_REXIST:
		ret = kvs_rbtree_resp_exist(cur_rbtree ,key, len_key);
		if( ret >= 0) {
			return KVS_RESP_EXIST;
		} else if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
#endif
#if ENABLE_HASH
	case KVS_CMD_HSET:
		ret = kvs_hash_resp_set(cur_hash ,key, len_key, value, len_val);
		if(ret == -2) {
			return KVS_RESP_EXIST;
		} else if (ret >=0)  {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		
		break;
	case KVS_CMD_HGET: {
		ret = kvs_hash_resp_get(cur_hash, key, len_key, &ret_val, &ret_val_len);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if(ret >= 0) {
			if(value_out == NULL || len_val_out == NULL) {
				return KVS_RESP_ERROR;
			}
			*value_out = ret_val;
			*len_val_out =  ret_val_len;
			return KVS_RESP_VALUE;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	}
	case KVS_CMD_HDEL:
		ret = kvs_hash_resp_del(cur_hash ,key, len_key);
		if( ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_resp_mod(cur_hash ,key, len_key, value, len_val);
		if( ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_resp_exist(cur_hash ,key, len_key);
		if( ret >= 0) {
			return KVS_RESP_EXIST;
		} else if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_SAVE:
		ret = kvs_server_save_rdb(server);
		//ret = kvs_persistence_save_rdb(server->pers_ctx, _kvs_handler_hash_item_iterator, server->hash, KVS_RDB_HASH);
		if(ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_SLAVE_SYNC:
		if(server->master.slave_count + server->master.syncing_slaves_count >= server->master.max_slave_count) {
			// todo : return more error info to slave 
			return KVS_RESP_ERROR;
		}
		conn->slave_info.is_slave = 1;
		//printf("Slave SYNC command received\n");
		conn->state = CONN_STATE_SLAVE_WAIT_RDB;
		assert(server->master.syncing_slaves_count >= 0);
		if(server->master.syncing_slaves_count == 0) {
			ret = kvs_server_save_rdb(server);
			if(ret == 0) {
				assert(server->pers_ctx->rdb_filename != NULL);
				if(conn->server->master.rdb_fd > 0) {
					close(conn->server->master.rdb_fd);
					conn->server->master.rdb_fd = -1;
				}
				conn->server->master.rdb_fd = open(server->pers_ctx->rdb_filename, O_RDONLY);
				if(conn->server->master.rdb_fd < 0) {
					printf("%s:%d open rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
					return KVS_RESP_ERROR;
				}
				conn->state = CONN_STATE_SLAVE_SEND_RDB;
				return KVS_RESP_SYNC_SLAVE;
			} else {
				return KVS_RESP_ERROR;
			}
		} else {
			conn->state = CONN_STATE_SLAVE_SEND_RDB;
			return KVS_RESP_SYNC_SLAVE;
		}
		
		break;
#endif

	default: 
		return KVS_RESP_UNKNOWN_CMD;
		//assert(0);
	}

	return KVS_RESP_ERROR;
}


/*
* @return the index of the start of the next command
*/
int kvs_parse_protocol(kvs_resp_cmd_t *cmd_cur, char* msg, int length/*, int idx*/) {
	if(cmd_cur == NULL || msg == NULL || length <= 0 /*|| idx < 0*/) return -1;
	int idx = 0;
	if(msg[idx] != '*') return -1;
	cmd_cur->raw_ptr = &msg[idx];
	int idx_bk = idx;
	//printf("%s:%d command parsering\n", __FILE__, __LINE__);
	idx ++ ;

	int len_arr = kvs_parse_int(msg, length, &idx);
	//printf("len_arr: %d idx:%d length:%d\n", len_arr, idx, length);
	if(idx + 1 >= length || msg[idx] != '\r' || msg[idx + 1] != '\n') return -1;
	idx += 2;
	//printf("%s:%d command parsering\n", __FILE__, __LINE__);
	int i = 0;
	for(; i < len_arr; ++ i) {
		// $
		if(idx >= length || msg[idx] != '$') return -1;
		idx ++ ;
		// length\r\n
		int len = kvs_parse_int(msg, length, &idx);
		if(idx + 1 >= length || msg[idx] != '\r' || msg[idx + 1] != '\n') return -1;
		idx += 2;
		if(idx + len + 1 >= length) return -1;

		// real value\r\n
		char *str = &msg[idx];
		idx = idx + len;
		if(msg[idx] != '\r' || msg[idx + 1] != '\n') return -1;
		// printf("str:[%.*s], len:%d\n", len, str, len);
		idx += 2;

		if(0 == i) cmd_cur->cmd = str, cmd_cur->len_cmd = len;
		else if(1 == i) cmd_cur->key = str, cmd_cur->len_key = len;
		else if(2 == i) cmd_cur->val = str, cmd_cur->len_val = len;
		else {
			printf("%s:%d error\n", __FILE__, __LINE__);
		}
	}
	
	if(i != len_arr) return -1;
	cmd_cur->raw_len = idx-idx_bk;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
		if(cmd_cur->len_cmd != command_length[cmd]) continue;
		if (memcmp(cmd_cur->cmd, command[cmd], cmd_cur->len_cmd) == 0) {
			cmd_cur->cmd_type = cmd;
			break;
		} 
	}
	if(cmd == KVS_CMD_COUNT) {
		// unknown command
		cmd_cur->cmd_type = -1;
	}

	return idx;

}



/*
 * @param length: length of msg
 * @param idx: start index of msg
 * @return the total parsed length
 */
int kvs_get_multi_cmds(kvs_resp_cmd_t *cmds, int max_cmd_num, char* msg, int length, int *parsed_cmd_count) {
	int cmd_count = 0;
	int cmd_length = 0;
	int idx = 0;
	while(1) {
		kvs_resp_cmd_t *cmd_cur = &cmds[cmd_count];
		cmd_length = kvs_parse_protocol(cmd_cur, msg + idx, length - idx);
		if(cmd_length == -1) {
			// if(idx != length)
			// 	printf("%s:%d command parser failed, remaining %d bytes\n", __FILE__, __LINE__, length - idx);
			break;
		} 
		idx += cmd_length;
		cmd_count ++ ;
		if(cmd_count >= max_cmd_num) break;
	}
	*parsed_cmd_count = cmd_count;
	return idx;
}


/*
 *@return enum KVS_RESPONSE_CODE
 */
int kvs_handler_process_raw_buffer(struct kvs_server_s* server, char* buf, int len, int *parsed_length)
{
	kvs_resp_cmd_t cmd = {0};
	// printf("before next_cmd\n");
	int next_cmd = kvs_parse_protocol(&cmd, buf, len);
	//printf("next_cmd: %d\n", next_cmd);
	if(next_cmd == -1) {
		return -1;
	}
	*parsed_length = next_cmd;
	struct kvs_conn_s dummy_conn;
	memset(&dummy_conn, 0, sizeof(struct kvs_conn_s));
	dummy_conn.server = server;
	dummy_conn.fd = -1; // not used

	char *value_dummy;
	int len_dummy;
	return kvs_execute_one_command(&dummy_conn, &cmd, &value_dummy, &len_dummy);
}


// check if the command is a write command
static inline int kvs_is_write_cmd(int cmd) {
	int i = 0;
	while(kvs_write_cmd[i] != -1) {
		if(kvs_write_cmd[i] == cmd) {
			return 1;
		}
		++ i;
	}
	return 0;
}

int kvs_handler_on_response(struct kvs_conn_s *conn, int bytes_sent) {
	if(conn == NULL) return -1;
	int state = conn->state;
	int pret = 0;
	assert(conn->w_idx == 0);

	size_t *rdb_offset = &conn->slave_info.rdb_offset;
	size_t *repl_backlog_offset = &conn->slave_info.repl_backlog_offset;

	switch(state){
		case CONN_STATE_CMD:
			kvs_proactor_set_recv_event(conn);
			break;
		// case CONN_STATE_SLAVE_WAIT_RDB:
		// 	break;
		case CONN_STATE_SLAVE_SEND_RDB:
			pret = pread(conn->server->master.rdb_fd, conn->response, conn->w_buf_sz, *rdb_offset);
			if(pret < 0) {
				printf("%s:%d pread rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
				assert(0);
			} else if(pret == 0) {
				// finish sending rdb file
				printf("%s:%d finish sending rdb file to slave\n", __FILE__, __LINE__);
				// rdb file descriptor will be closed when next rdb save
				// close(conn->server->master.rdb_fd);
				// conn->server->master.rdb_fd = -1;
				*repl_backlog_offset = 0;
				goto SLAVE_SEND_REPL_LABEL;
			} else {
				*rdb_offset += pret;
				conn->w_idx = pret;
				kvs_proactor_set_send_event_manual(conn);
			}
			break;
		case CONN_STATE_SLAVE_SEND_REPL:
		{
SLAVE_SEND_REPL_LABEL:
			if(conn->server->master.slave_count < 0) {
				printf("%s:%d invalid master slave count: %d\n", __FILE__, __LINE__, conn->server->master.slave_count);
				assert(0);
			}
			if(conn->server->master.repl_backlog != NULL && conn->server->master.repl_backlog_idx > 0) {
				if(bytes_sent > 0 && conn->state == CONN_STATE_SLAVE_SEND_REPL) {
					*repl_backlog_offset += bytes_sent;
				}
				conn->state = CONN_STATE_SLAVE_SEND_REPL;
				if(*repl_backlog_offset < conn->server->master.repl_backlog_idx) {
					size_t remaining = conn->server->master.repl_backlog_idx - *repl_backlog_offset;
					kvs_proactor_set_send_event_raw_buffer(conn, conn->server->master.repl_backlog, remaining);
					break;
				} else {
					// finish sending replication backlog
					printf("%s:%d finish sending replication backlog to slave\n", __FILE__, __LINE__);
				}
			} else {
				// no replication backlog data
				printf("%s:%d no replication backlog data to send to slave\n", __FILE__, __LINE__);
			}

			conn->state = CONN_STATE_SLAVE_ONLINE;
			conn->server->master.syncing_slaves_count --;
			assert(conn->server->master.syncing_slaves_count >= 0);
			if(conn->server->master.syncing_slaves_count == 0) {
				close(conn->server->master.rdb_fd);
				conn->server->master.rdb_fd = -1;
				conn->server->master.repl_backlog_idx = 0;
			}

			assert(conn->server->master.slave_count < KVS_MAX_SLAVES);
			conn->server->master.slaves_fds[conn->server->master.slave_count] = conn->fd;
			conn->slave_info.slave_idx = conn->server->master.slave_count;
			conn->server->master.slave_count ++ ;
			conn->slave_info.is_slave = 1;
			printf("%s:%d new slave [fd:%d] connected, total slave count: %d\n", __FILE__, __LINE__, conn->fd, conn->server->master.slave_count);
			kvs_proactor_set_recv_event(conn);
		}
			break;
		case CONN_STATE_SLAVE_ONLINE:
			kvs_proactor_set_recv_event(conn);
			break;
		default:
			printf("%s:%d invalid connection state: %d\n", __FILE__, __LINE__, state);
			assert(0);
	}

	return 0;
	
}

static inline int _kvs_handler_init_conn(struct kvs_conn_s *conn, int client_fd, struct kvs_server_s *server) {
	if(conn == NULL || client_fd < 0) return -1;
	// memory leak if r_buffer and response are not NULL !!! 
	// do not memset to 0 the whole struct, or the existing buffers will be lost
	// memset(conn, 0, sizeof(struct kvs_conn_s));
	conn->fd = client_fd;
	conn->server = server;
	// may reuse buffers
	if(conn->r_buffer == NULL && conn->r_buf_sz == 0) {
		conn->r_buffer = (char*)kvs_malloc(BUFFER_LENGTH);
		conn->r_buf_sz = BUFFER_LENGTH;
	}
	assert(conn->r_buf_sz == BUFFER_LENGTH);
	memset(conn->r_buffer, 0, conn->r_buf_sz);
	conn->r_idx = 0;
	if(conn->response == NULL) {
		conn->response = (char*)kvs_malloc(BUFFER_LENGTH);
		conn->w_buf_sz = BUFFER_LENGTH;
	}
	assert(conn->w_buf_sz == BUFFER_LENGTH);
	memset(conn->response, 0, conn->w_buf_sz);
	conn->w_idx = 0;
	//conn->state = 0;
	conn->state = CONN_STATE_CMD;
	return 0;
}

int kvs_handler_register_master(struct kvs_conn_s *conn, int client_fd, struct kvs_server_s *server) {
	if(conn == NULL || client_fd < 0) return -1;

	_kvs_handler_init_conn(conn, client_fd, server);
	conn->master_info.is_master = 1;
	conn->slave_info.is_slave = 0;

	// add to uring event loop
	kvs_proactor_set_recv_event(conn);
	return 0;
}

int kvs_handler_register_client(struct kvs_conn_s *conn, int client_fd, struct kvs_server_s *server) {
	if(conn == NULL || client_fd < 0) return -1;

	_kvs_handler_init_conn(conn, client_fd, server);
	conn->master_info.is_master = 0;
	conn->slave_info.is_slave = 0;

	// add to uring event loop
	kvs_proactor_set_recv_event(conn);
	return 0;
}


int kvs_handler_on_accept(struct kvs_server_s *server, int client_fd) {
	if(server == NULL || client_fd <= 0) return -1;

	struct kvs_conn_s *conn = &server->conns[client_fd];
	if(conn == NULL) {
		printf("%s:%d create connection failed\n", __FILE__, __LINE__);
		return -1;
	}

	kvs_handler_register_client(conn, client_fd, server);

	return 0;
}

int static inline _kvs_handler_init_slave_info(struct kvs_conn_s *conn) {
	
	// lazy allocate replication backlog buffer
	if(conn->server->master.repl_backlog == NULL) {
		// allocate replication backlog buffer
		printf("%s:%d allocate repl backlog buffer for slave\n", __FILE__, __LINE__);
		conn->server->master.repl_backlog = (char*)kvs_malloc(KVS_REPL_BACKLOG_SIZE);
		conn->server->master.repl_backlog_size = KVS_REPL_BACKLOG_SIZE;
		conn->server->master.repl_backlog_idx = 0;
	}

	// open rdb file
	if(conn->server->master.rdb_fd <= 0) {
		// if rdb file is not opened, open it
		conn->server->master.rdb_fd = open(conn->server->pers_ctx->rdb_filename, O_RDONLY);
		if(conn->server->master.rdb_fd < 0) {
			printf("%s:%d open rdb file failed\n", __FILE__, __LINE__);
			assert(0);
		}
	}
	
	// get rdb file size
	struct stat rdb_stat;
	if(-1 == fstat(conn->server->master.rdb_fd, &rdb_stat) ) {
		printf("%s:%d fstat rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
		assert(0);
	}
	int rdb_size = rdb_stat.st_size;
	if(rdb_size <= 0) {
		//todo: empty rdb file ?
		printf("%s:%d rdb file size is 0\n", __FILE__, __LINE__);
		//assert(0);
	}
	// reset rdb offset, ready to send rdb file from the beginning
	conn->slave_info.rdb_offset = 0;
	conn->slave_info.rdb_size = rdb_size;
	conn->server->pers_ctx->rdb_size = rdb_size;
	return 0;
}

/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * length_r: length fo response
 * @return : length of processed message
 */
int kvs_handler_on_msg(struct kvs_conn_s *conn) {  
	// todo: state machine for slave
	// todo: response_len avoid buffer overflow
	// if (msg == NULL || length <= 0 || response == NULL || length_r == NULL) return -1;
	if(conn == NULL) return -1;
	char *msg = conn->r_buffer;
	int length = conn->r_idx;// can not change r_idx in this function
	char *response = conn->response;
	int rsp_buf_len = conn->w_buf_sz;
	int* length_r = &conn->w_idx; // w_idx should be the length of response after processing
	int is_write_cmd = 0;

	
	//printf("Debug: length_r pointer address = %p\n", (void*)length_r);
#if (KVS_PROTOCOL_SELECT == KVS_RESP)
	int cmd_count = 0;
	int parsed_length = kvs_get_multi_cmds(kvs_resp_cmds, KVS_RESP_CMD_MAX, msg, length, &cmd_count);
	// if(parsed_length <= 0 || cmd_count <= 0) {
	// 	printf("%s:%d parse commands failed\n", __FILE__, __LINE__);
	// 	return -1;
	// }
	// record the total length of response in response buffer in the following for loop
	//int *length_r = 0;

	// process each command
	for (int cmd_idx = 0; cmd_idx < cmd_count; cmd_idx++) {
		is_write_cmd = kvs_is_write_cmd(kvs_resp_cmds[cmd_idx].cmd_type);
		// if(conn->master_info.is_master) {
		// 	static int master_cmd_count = 0;
		// 	master_cmd_count ++ ;
		// 	if(master_cmd_count % 1000 == 0) {
		// 		printf("%s:%d master processing command count: %d, cmd: [%.*s]\n", __FILE__, __LINE__, master_cmd_count, kvs_resp_cmds[i].len_cmd, kvs_resp_cmds[i].cmd);	
		// 	}
			
		// }

		if(conn->server->role == KVS_SERVER_ROLE_SLAVE && is_write_cmd && !conn->master_info.is_master) {
			// slave server should not accept write commands
			printf("%s:%d slave server reject write command\n", __FILE__, __LINE__);
			//printf("command: [%.*s], cmd_type: %d, is_write_cmd: %d\n", kvs_resp_cmds[i].len_cmd, kvs_resp_cmds[i].cmd, kvs_resp_cmds[i].cmd_type, is_write_cmd);
			//printf("connection fd: %d, is_master: %d\n", conn->fd, conn->master_info.is_master);
			*length_r += kvs_format_response(KVS_RESP_ERROR, NULL, 0, response + *length_r, rsp_buf_len - *length_r);
			continue;
		}

		char *value_out = NULL;
		int len_val_out = 0;
		int status_num = kvs_execute_one_command(conn, &kvs_resp_cmds[cmd_idx], &value_out, &len_val_out);
		if(status_num < 0) {
			printf("%s:%d execute command failed\n", __FILE__, __LINE__);
			assert(0);
		}

		if(conn->master_info.is_master) {
			//printf("%s:%d master command:%.*s\n", __FILE__, __LINE__, kvs_resp_cmds[i].len_cmd, kvs_resp_cmds[i].cmd);
			// master connection does not need response
			continue;
		}

		// special handling for SYNC command
		if(status_num == KVS_RESP_SYNC_SLAVE) {
			conn->server->master.syncing_slaves_count ++ ;
			conn->state = CONN_STATE_SLAVE_SEND_RDB;
			_kvs_handler_init_slave_info(conn);
			if(conn->slave_info.rdb_size <= 0) {
				// there is no RDB file to sends
				printf("%s:%d no RDB file to send to slave\n", __FILE__, __LINE__);
				// directly set to send replication backlog
				conn->state = CONN_STATE_SLAVE_SEND_REPL;
				// todo:
				assert(0);
			}
			assert(conn->w_idx == 0);
			// start to format RDB file sending response
			*length_r += snprintf(conn->response, conn->w_buf_sz, "$%ld\r\n", conn->slave_info.rdb_size);
			assert(conn->slave_info.rdb_offset == 0);
			assert(conn->server->master.rdb_fd > 0);
			int p_ret = pread(conn->server->master.rdb_fd, conn->response + *length_r, conn->w_buf_sz - *length_r, conn->slave_info.rdb_offset);
			if(p_ret < 0) {
				printf("%s:%d pread rdb file failed: %s\n", __FILE__, __LINE__, strerror(errno));
				assert(0);
			} else if(p_ret == 0) {
				printf("%s:%d pread rdb file return 0\n", __FILE__, __LINE__);
				// there is no data in rdb file to read ??? 
				assert(0);
			} else {
				conn->slave_info.rdb_offset += p_ret;
				*length_r += p_ret;
			}
			// set to send RDB file
			
			break; // exit for loop, directly send response, ignore remaining commands !
		}

		
		int r_len = kvs_format_response(status_num, value_out, len_val_out, response + *length_r, rsp_buf_len - *length_r);
		if(*length_r + r_len > rsp_buf_len) {
			// overflow
			printf("%s:%d response buffer overflow\n", __FILE__, __LINE__);
			assert(0);
		}
		*length_r += r_len;

		// persistence for write commands
#if KVS_PERSISTENCE
		if(status_num == KVS_RESP_OK && is_write_cmd) {
			kvs_persistence_write_aof(conn->server->pers_ctx, kvs_resp_cmds[cmd_idx].raw_ptr, kvs_resp_cmds[cmd_idx].raw_len);
			
			if(KVS_SERVER_ROLE_MASTER == conn->server->role) {
				// printf("key: %.*s, cmd: %.*s\n", 
				// 	kvs_resp_cmds[cmd_idx].len_key, kvs_resp_cmds[cmd_idx].key,
				// 	kvs_resp_cmds[cmd_idx].len_cmd, kvs_resp_cmds[cmd_idx].cmd);
				// static int send_count = 0;
				// send_count ++;
				// if(send_count % 1000 == 0) {
				// 	printf("%s:%d master sending to slaves, command count: %d\n", __FILE__, __LINE__, send_count);	
				// }
				int slave_count = conn->server->master.slave_count;
				for(int s_idx = 0; s_idx < slave_count; s_idx ++) {
					
					int slave_fd = conn->server->master.slaves_fds[s_idx];
					assert(slave_fd > 0);
					struct kvs_conn_s *slave_conn = &conn->server->conns[slave_fd];
					if(slave_conn == NULL) {
						printf("%s:%d get slave connection failed for fd: %d\n", __FILE__, __LINE__, slave_fd);
						assert(0);
					}
			
					int set_result = kvs_proactor_set_send_event(slave_conn, kvs_resp_cmds[cmd_idx].raw_ptr, kvs_resp_cmds[cmd_idx].raw_len);
					if(-2 == set_result) {
						printf("%s:%d slave response buffer overflow for fd: %d\n", __FILE__, __LINE__, slave_fd);
						close(slave_fd);
					}
				}
			}
			
			if(conn->server->master.syncing_slaves_count > 0) {
				// append to replication backlog
				if(conn->server->master.repl_backlog_overflow == 0 && conn->server->master.repl_backlog != NULL) {
					if(conn->server->master.repl_backlog_idx + kvs_resp_cmds[cmd_idx].raw_len <= conn->server->master.repl_backlog_size) {
						// enough space
						memcpy(conn->server->master.repl_backlog + conn->server->master.repl_backlog_idx, kvs_resp_cmds[cmd_idx].raw_ptr, kvs_resp_cmds[cmd_idx].raw_len);
						conn->server->master.repl_backlog_idx += kvs_resp_cmds[cmd_idx].raw_len;
					} else {
						conn->server->master.repl_backlog_overflow = 1;
						// todo: handle replication backlog overflow
						printf("%s:%d replication backlog overflow, cannot append more data\n", __FILE__, __LINE__);
					}
				}
			}
		}
#endif 		
	}


	// set to send response
	//set_event_send(conn->server->uring, conn->fd, conn->response, conn->w_idx, 0);
	kvs_proactor_set_send_event_manual(conn);
	
	return parsed_length; // length of processed message

#if 0
	if(test_buffer == 0) {
		printf("recv: [%s]\n", msg);
		memcpy(response, msg, length / 2);
		printf("Debug: length_r pointer address = %p\n", (void*)length_r);
		*length_r = length / 2;
		printf("1.1\n");
		test_buffer = 1;
		printf("1.2\n");
		return length / 2;
	} else {
		memcpy(response, msg, length);
		*length_r = length;
		test_buffer = 0;
		return length;
	}
#endif

	

#endif

#if (KVS_PROTOCOL_SELECT == KVS_1R1R)
	
	//printf("recv %d : %s\n", length, msg);

	char *tokens[KVS_MAX_TOKENS] = {0};

	int count = kvs_split_token(msg, tokens);
	if (count == -1) return -1;

	//memcpy(response, msg, length);

	*length_r = kvs_execute_one_command(tokens, count, response);
	return length;
#endif
}


#endif

int kvs_handler_on_close(struct kvs_conn_s *conn) {
	if(conn == NULL) return -1;
	printf("%s:%d connection closed, fd: %d, rdb_size: %ld\n", __FILE__, __LINE__, conn->fd, conn->slave_info.rdb_size);
	if(conn->master_info.is_master) {
		assert(0 == conn->slave_info.is_slave);
		//printf("%s:%d master connection closed, fd: %d\n", __FILE__, __LINE__, conn->fd);
		// todo: master connection closed
		assert(0);
		return -1;
	}
	
	if(conn->slave_info.is_slave) {
		//printf("%s:%d slave connection closed, fd: %d\n", __FILE__, __LINE__, conn->fd);
		assert(0 == conn->master_info.is_master);
		//printf("%s:%d slave connection closed, fd: %d\n", __FILE__, __LINE__, conn->fd);
		// remove from server's slave fds list
		assert(conn->slave_info.slave_idx >=0);
		printf("%s:%d removing slave connection, fd: %d, slave_idx: %d, slave_count: %d\n", 
			__FILE__, __LINE__, conn->fd, conn->slave_info.slave_idx, conn->server->master.slave_count);
		assert(conn->slave_info.slave_idx < conn->server->master.slave_count);
		int slave_idx = conn->slave_info.slave_idx;
		int last_slave_idx = conn->server->master.slave_count - 1;
		if(slave_idx < last_slave_idx) {
			// move last slave to this position
			int last_slave_fd = conn->server->master.slaves_fds[last_slave_idx];
			conn->server->master.slaves_fds[slave_idx] = last_slave_fd;
			conn->server->conns[last_slave_fd].slave_info.slave_idx = slave_idx;
			conn->server->master.slaves_fds[last_slave_idx] = -1;
			conn->server->master.slave_count -- ;
		} else if(slave_idx == last_slave_idx) {
			conn->server->master.slaves_fds[slave_idx] = -1;
			conn->server->master.slave_count -- ;
		} else {
			assert(0);
		}
		conn->slave_info.slave_idx = -1;
		conn->slave_info.is_slave = 0;
			
	}

	conn->fd = -1;
	conn->r_idx = 0;
	conn->w_idx = 0;


	return 0;
}
