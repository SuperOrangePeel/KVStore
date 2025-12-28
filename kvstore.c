



#include "kvstore.h"
#include <math.h>


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



const char *response[] = {

};


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
int kvs_filter_protocol(char **tokens, int count, char *response) {

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

int kvs_filter_protocol(kvs_resp_cmd_t *cmd_p, char *response) {

	if (cmd_p == NULL || response == NULL) return -1;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
		if(cmd_p->len_cmd != strlen(command[cmd])) continue;;
		if (memcmp(cmd_p->cmd, command[cmd], cmd_p->len_cmd) == 0) {
			break;
		} 
	}

	int length = 0;
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
			length = sprintf(response, "+EXIST\r\n");
		} else if (ret >=0)  {
			length = sprintf(response, "+OK\r\n");
		} else {
			length = sprintf(response, "-ERROR\r\n");
		} 
		
		break;
	case KVS_CMD_GET: {
		char* ret_val;
		int ret_val_len;
		ret = kvs_array_resp_get(&global_array, key, len_key, &ret_val, &ret_val_len);
		if (ret == -2) {
			length = sprintf(response, "+NOT EXIST\r\n");
		} else if(ret >= 0) {
			int r_len1 = sprintf(response, "$%d\r\n", ret_val_len);
			memcpy(response + r_len1, ret_val, ret_val_len);
			int r_len2 = sprintf(response + r_len1 + ret_val_len, "\r\n");
			length = ret_val_len + r_len1 + r_len2;
		} else {
			length = sprintf(response, "-ERROR\r\n");
		} 
		break;
	}
	case KVS_CMD_DEL:
		ret = kvs_array_resp_del(&global_array ,key, len_key);
		if (ret == -2) {
			length = sprintf(response, "+NOT EXIST\r\n");
 		} else if (ret >= 0) {
			length = sprintf(response, "+OK\r\n");
		} else {
			length = sprintf(response, "-ERROR\r\n");
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_resp_mod(&global_array ,key, len_key, value, len_val);
		if (ret == -2) {
			length = sprintf(response, "+NOT EXIST\r\n");
 		} else if (ret >= 0) {
			length = sprintf(response, "+OK\r\n");
		} else {
			length = sprintf(response, "-ERROR\r\n");
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_resp_exist(&global_array ,key, len_key);
		if (ret >= 0) {
			length = sprintf(response, "+EXIST\r\n");
		} else if (ret == -2) {
			length = sprintf(response, "+NOT EXIST\r\n");
		} else {
			length = sprintf(response, "-ERROR\r\n");
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
			length = sprintf(response, "NO EXIST\r\n");
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
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_RMOD:
		ret = kvs_rbtree_mod(&global_rbtree ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_REXIST:
		ret = kvs_rbtree_exist(&global_rbtree ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
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
			length = sprintf(response, "NO EXIST\r\n");
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
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_mod(&global_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_exist(&global_hash ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
#endif

	default: 
		assert(0);
	}

	return length;
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
int kvs_get_one_command(kvs_resp_cmd_t *cmd_cur, char* msg, int length, int idx) {
	if(cmd_cur == NULL || msg == NULL || length <= 0 || idx < 0) return -1;
	if(msg[0] != '*') return -1;
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
		printf("str:[%.*s], len:%d\n", len, str, len);
		idx += 2;

		if(0 == i) cmd_cur->cmd = str, cmd_cur->len_cmd = len;
		else if(1 == i) cmd_cur->key = str, cmd_cur->len_key = len;
		else if(2 == i) cmd_cur->val = str, cmd_cur->len_val = len;
		else printf("%s:%d error\n", __FILE__, __LINE__);
	}
	
	if(i != len_arr) return -1;

	return idx;

}
#endif


/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * length_r: length fo response
 * @return : length of processed message
 */

// static int test_buffer = 0;
// todo: response_len avoid buffer overflow
int kvs_protocol(char *msg, int length, char *response, int* length_r) {  
	
// SET Key Value
// GET Key
// DEL Key

	if (msg == NULL || length <= 0 || response == NULL || length_r == NULL) return -1;
//printf("Debug: length_r pointer address = %p\n", (void*)length_r);

/*
*3\r\n
$3\r\n
SET\r\n
$4\r\n
name\r\n
$7\r\n
Jack Ma\r\n
*/
#if (KVS_PROTOCOL_SELECT == KVS_RESP)
	int idx = 0;
	int cmd_count = 0;

	

	int cmd_ret = 0;
	
	while(1) {
		kvs_resp_cmd_t *cmd_cur = &kvs_resp_cmds[cmd_count];
		cmd_ret = kvs_get_one_command(cmd_cur, msg, length, idx);
		if(cmd_ret == -1) {
			if(idx != length)
				printf("%s:%d command parser failed\n", __FILE__, __LINE__);
			break;
		}
		idx = cmd_ret;
		cmd_count ++ ;
		if(cmd_count >= KVS_RESP_CMD_MAX) break;
	}

	int r_len_total = 0;
	for (int i = 0; i < cmd_count; i++) {
    	printf("Command %d:\n", i);
    	printf("  Cmd: %.*s (len: %d)\n", kvs_resp_cmds[i].len_cmd, kvs_resp_cmds[i].cmd, kvs_resp_cmds[i].len_cmd);
    	printf("  Key: %.*s (len: %d)\n", kvs_resp_cmds[i].len_key, kvs_resp_cmds[i].key, kvs_resp_cmds[i].len_key);
    	printf("  Val: %.*s (len: %d)\n", kvs_resp_cmds[i].len_val, kvs_resp_cmds[i].val, kvs_resp_cmds[i].len_val);
		int r_len = kvs_filter_protocol(&kvs_resp_cmds[i], response + r_len_total);
		r_len_total += r_len;
		
	}
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

	*lenght_r = kvs_filter_protocol(tokens, count, response);
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

	return 0;
}

void dest_kvengine(void) {
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


