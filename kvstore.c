



#include "kvstore.h"
#include <fcntl.h>

extern kvs_aof_context_t kvs_aof_ctx;

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

#if (KVS_PROTOCOL_SELECT == KVS_RESP)
kvs_resp_cmd_t kvs_resp_cmds[KVS_RESP_CMD_MAX]; // static ?
#endif

void *kvs_malloc(size_t size) {
	return malloc(size);
}

void kvs_free(void *ptr) {
	return free(ptr);
}


const char *command[] = {
	"SET", "GET", "DEL", "MOD", "EXIST",
	"RSET", "RGET", "RDEL", "RMOD", "REXIST",
	"HSET", "HGET", "HDEL", "HMOD", "HEXIST"
};

const int command_length[] = {
	3, 3, 3, 3, 5,
	4, 4, 4, 4, 6,
	4, 4, 4, 4, 6
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
	
	KVS_CMD_COUNT,
};

static int kvs_write_cmd[] = {
	KVS_CMD_SET, KVS_CMD_MOD,
	KVS_CMD_RSET, KVS_CMD_RMOD,
	KVS_CMD_HSET, KVS_CMD_HMOD,
	-1
};

enum {
	KVS_RESP_OK = 0,
	KVS_RESP_ERROR,
	KVS_RESP_EXIST,
	KVS_RESP_NOT_EXIST,
	KVS_RESP_VALUE,
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
		ret = kvs_array_set(&global_array ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_GET: {
		char *result = kvs_array_get(&global_array, key);
		if (result == NULL) {
			length = sprintf(response, "NOT EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_DEL:
		ret = kvs_array_del(&global_array ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_mod(&global_array ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_exist(&global_array ,key);
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
		ret = kvs_rbtree_set(&global_rbtree ,key, value);
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
		ret = kvs_hash_set(&global_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_HGET: {
		char *result = kvs_hash_get(&global_hash, key);
		if (result == NULL) {
			length = sprintf(response, "NOT EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_HDEL:
		ret = kvs_hash_del(&global_hash ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_mod(&global_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NOT EXIST\r\n");
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_exist(&global_hash ,key);
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
#endif


#if (KVS_PROTOCOL_SELECT == KVS_RESP)

int kvs_format_response(int status, char *value, int len_val, char *response) {
	if (response == NULL) return -1;

	int length = 0;
	switch(status) {
		case KVS_RESP_OK:
			length = sprintf(response, "+OK\r\n");
			break;
		case KVS_RESP_ERROR:
			length = sprintf(response, "-ERROR\r\n");
			break;
		case KVS_RESP_EXIST:
			length = sprintf(response, "+EXIST\r\n");
			break;
		case KVS_RESP_NOT_EXIST:
			length = sprintf(response, "+NOT EXIST\r\n");
			break;
		case KVS_RESP_VALUE:
			{
				int r_len1 = sprintf(response, "$%d\r\n", len_val);
				memcpy(response + r_len1, value, len_val);
				int r_len2 = sprintf(response + r_len1 + len_val, "\r\n");
				length = len_val + r_len1 + r_len2;
			}
			break;
		default:
			assert(0);
	}

	return length;
}

//char *response 
int kvs_execute_one_command(kvs_resp_cmd_t *cmd_p, char **value_out, int *len_val_out) {

	if (cmd_p == NULL) return -1;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
		if(cmd_p->len_cmd != command_length[cmd]) continue;
		if (memcmp(cmd_p->cmd, command[cmd], cmd_p->len_cmd) == 0) {
			cmd_p->cmd_type = cmd;
			break;
		} 
	}

	int ret = 0;
	char *key = cmd_p->key;
	int len_key = cmd_p->len_key;
	char *value = cmd_p->val;
	int len_val = cmd_p->len_val;

	switch(cmd) {
#if ENABLE_ARRAY
	case KVS_CMD_SET:
		  
		ret = kvs_array_resp_set(&global_array ,key, len_key, value, len_val);
		if (ret == -2) {
			return KVS_RESP_EXIST;
		} else if (ret >=0)  {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		} 
		
		break;
	case KVS_CMD_GET: {
		char* ret_val;
		int ret_val_len;
		ret = kvs_array_resp_get(&global_array, key, len_key, &ret_val, &ret_val_len);
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
		ret = kvs_array_resp_del(&global_array ,key, len_key);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_resp_mod(&global_array ,key, len_key, value, len_val);
		if (ret == -2) {
			return KVS_RESP_NOT_EXIST;
		} else if (ret >= 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_ERROR;
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_resp_exist(&global_array ,key, len_key);
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
		ret = kvs_rbtree_set(&global_rbtree ,key, value);
		if (ret < 0) {
			return KVS_RESP_ERROR;
		} else if (ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_EXIST;
		} 
		
		break;
	case KVS_CMD_RGET: {
		char *result = kvs_rbtree_get(&global_rbtree, key);
		if (result == NULL) {
			return KVS_RESP_NOT_EXIST;
		} else {
			*value_out = result;
			*len_val_out = strlen(result);
			return KVS_RESP_VALUE;
		}
		break;
	}
	case KVS_CMD_RDEL:
		ret = kvs_rbtree_del(&global_rbtree ,key);
		if (ret < 0) {
			return KVS_RESP_ERROR;
		} else if (ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
	case KVS_CMD_RMOD:
		ret = kvs_rbtree_mod(&global_rbtree ,key, value);
		if (ret < 0) {
			return KVS_RESP_ERROR;
		} else if (ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
	case KVS_CMD_REXIST:
		ret = kvs_rbtree_exist(&global_rbtree ,key);
		if (ret == 0) {
			return KVS_RESP_EXIST;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
#endif
#if ENABLE_HASH
	case KVS_CMD_HSET:
		ret = kvs_hash_set(&global_hash ,key, value);
		if (ret < 0) {
			return KVS_RESP_ERROR;
		} else if (ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_EXIST;
		} 
		
		break;
	case KVS_CMD_HGET: {
		char *result = kvs_hash_get(&global_hash, key);
		if (result == NULL) {
			return KVS_RESP_NOT_EXIST;
		} else {
			*value_out = result;
			*len_val_out = strlen(result);
			return KVS_RESP_VALUE;
		}
		break;
	}
	case KVS_CMD_HDEL:
		ret = kvs_hash_del(&global_hash ,key);
		if (ret < 0) {
			return KVS_RESP_ERROR;
		} else if (ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_mod(&global_hash ,key, value);
		if (ret < 0) {
			return KVS_RESP_ERROR;
		} else if (ret == 0) {
			return KVS_RESP_OK;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_exist(&global_hash ,key);
		if (ret == 0) {
			return KVS_RESP_EXIST;
		} else {
			return KVS_RESP_NOT_EXIST;
		}
		break;
#endif

	default: 
		assert(0);
	}

	return KVS_RESP_ERROR;
}


// >= 0
static inline int kvs_parse_int(char* s, int length, int* offset) {
    int res = 0;
    int i = *offset;
	//printf("i:%d, s[i]:[%.*s]\n", i, 1, s);
    while (i < length && s[i] >= '0' && s[i] <= '9') {
        res = res * 10 + (s[i] - '0');
		//printf("res:%d, num:%d\n", res, (int)(s[i] - '0'));
        i++;
    }
    *offset = i;
	//printf("res:%d\n", res);
    return res;
}


/*
* @return the index of the start of the next command
*/
int kvs_parse_protocol(kvs_resp_cmd_t *cmd_cur, char* msg, int length, int idx) {
	if(cmd_cur == NULL || msg == NULL || length <= 0 || idx < 0) return -1;
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
		else printf("%s:%d error\n", __FILE__, __LINE__);
	}
	
	if(i != len_arr) return -1;
	cmd_cur->raw_len = idx-idx_bk;
	return idx;

}



int kvs_get_multi_cmds(kvs_resp_cmd_t *cmds, int max_cmd_num, char* msg, int length, int *idx) {
	int cmd_count = 0;
	int next_cmd = 0;
	while(1) {
		kvs_resp_cmd_t *cmd_cur = &cmds[cmd_count];
		next_cmd = kvs_parse_protocol(cmd_cur, msg, length, *idx);
		if(next_cmd == -1) {
			if(*idx != length)
				printf("%s:%d command parser failed\n", __FILE__, __LINE__);
			break;
		}
		*idx = next_cmd;
		cmd_count ++ ;
		if(cmd_count >= max_cmd_num) break;
	}
	return cmd_count;
}


/*
 *@return: -1 parser failed
 */
int kvs_get_exe_one_cmd(char* msg, int length, int *idx) {
	kvs_resp_cmd_t cmd = {0};
	// printf("before next_cmd\n");
	int next_cmd = kvs_parse_protocol(&cmd, msg, length, *idx);
	//printf("next_cmd: %d\n", next_cmd);
	if(next_cmd == -1) {
		return -1;
	}
	*idx = next_cmd;
	char *value_dummy;
	int len_dummy;
	return kvs_execute_one_command(&cmd, &value_dummy, &len_dummy);
}

int kvs_recover_aof(char *aof_name) {
	kvs_persistence_load_aof(aof_name, kvs_get_exe_one_cmd);
}

#endif

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

static int total_command = 0;
/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * length_r: length fo response
 * @return : length of processed message
 */
int kvs_protocol(char *msg, int length, char *response, int* length_r) {  
	// todo: response_len avoid buffer overflow
	if (msg == NULL || length <= 0 || response == NULL || length_r == NULL) return -1;
	//printf("Debug: length_r pointer address = %p\n", (void*)length_r);
#if (KVS_PROTOCOL_SELECT == KVS_RESP)
	int idx = 0;
	int cmd_count = kvs_get_multi_cmds(kvs_resp_cmds, KVS_RESP_CMD_MAX, msg, length, &idx);

	int r_len_total = 0;
	//printf("Command count: %d\n", cmd_count);
	int ok_count = 0;
	int error_count = 0;
	// gettimeofday(&kvs_before_time, NULL);
	// struct timeval kvs_after_time;

	for (int i = 0; i < cmd_count; i++) {
		// printf("Command %d:\n", i);
    	// printf("  Cmd: %.*s (len: %d)\n", kvs_resp_cmds[i].len_cmd, kvs_resp_cmds[i].cmd, kvs_resp_cmds[i].len_cmd);
    	// printf("  Key: %.*s (len: %d)\n", kvs_resp_cmds[i].len_key, kvs_resp_cmds[i].key, kvs_resp_cmds[i].len_key);
    	// printf("  Val: %.*s (len: %d)\n", kvs_resp_cmds[i].len_val, kvs_resp_cmds[i].val, kvs_resp_cmds[i].len_val);
		char *value_out = NULL;
		int len_val_out = 0;
		int status_num = kvs_execute_one_command(&kvs_resp_cmds[i], &value_out, &len_val_out);
		int r_len = kvs_format_response(status_num, value_out, len_val_out, response + r_len_total);
		if(status_num == KVS_RESP_OK) ok_count ++;
		else {
			//printf("status num:%d\n", status_num);
			error_count ++;
		}

		if(status_num == KVS_RESP_OK && kvs_is_write_cmd(kvs_resp_cmds[i].cmd_type)) {
			//kvs_persistence_write_aof(&kvs_aof_ctx, kvs_resp_cmds[i].raw_ptr, kvs_resp_cmds[i].raw_len);
		}
		r_len_total += r_len;
		
	}
	//total_command += ok_count;
	//printf("total_command: %d Batch stats: Parsed=%d, Success=%d, Failed=%d\n", total_command, cmd_count, ok_count, error_count);
	
	*length_r = r_len_total;
	
	// idx is the start of a command
	return idx; // length of processed message

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





int init_kvengine(void) {
	
	

#if ENABLE_ARRAY
	memset(&global_array, 0, sizeof(kvs_array_t));
	kvs_array_create(&global_array);
#endif

#if ENABLE_RBTREE
	memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
	kvs_rbtree_create(&global_rbtree);
#endif

#if ENABLE_HASH
	memset(&global_hash, 0, sizeof(kvs_hash_t));
	kvs_hash_create(&global_hash);
#endif


	//kvs_recover_aof("kvs_aof.aof");
	//kvs_persistence_init(&kvs_aof_ctx, "kvs_aof.aof");

	return 0;
}

void dest_kvengine(void) {
	kvs_persistence_close(&kvs_aof_ctx);
#if ENABLE_ARRAY
	kvs_array_destory(&global_array);
#endif
#if ENABLE_RBTREE
	kvs_rbtree_destory(&global_rbtree);
#endif
#if ENABLE_HASH
	kvs_hash_destory(&global_hash);
#endif

}



int main(int argc, char *argv[]) {

	if (argc != 2) return -1;

	int port = atoi(argv[1]);

	init_kvengine();
	
	
#if (NETWORK_SELECT == NETWORK_REACTOR)
	reactor_start(port, kvs_protocol);  //
#elif (NETWORK_SELECT == NETWORK_NTYCO)
	ntyco_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
	proactor_start(port, kvs_protocol);
#endif

	dest_kvengine();

}


