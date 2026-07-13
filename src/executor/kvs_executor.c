#include "kvs_executor.h"

#include "kvs_types.h"
#include "common.h"
#include "logger.h"
#include "kvs_server.h"
#include "kvs_network.h"
#include "kvs_qa.h"

#include <stddef.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
static kvs_result_t _kvs_exec_setex(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
static kvs_result_t _kvs_exec_createv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
static kvs_result_t _kvs_exec_setv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
static kvs_result_t _kvs_exec_getv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
static kvs_result_t _kvs_exec_delv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
static kvs_result_t _kvs_exec_vinfo(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);




kvs_result_t _kvs_exec_set(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_set(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_get(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    
    return kvs_server_get(server, cmd->key, cmd->len_key, &(cmd->val), &(cmd->len_val));
}

kvs_result_t _kvs_exec_del(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_del(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_mod(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_mod(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_exist(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_exist(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_rset(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rset(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_rget(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rget(server, cmd->key, cmd->len_key, &(cmd->val), &(cmd->len_val));
}

kvs_result_t _kvs_exec_rdel(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rdel(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_rmod(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rmod(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_rexist(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_rexist(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_hset(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    kvs_result_t result = kvs_server_hset(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
    if(result == KVS_RES_EXIST) {
        result = KVS_RES_OK; // for redis-benchmark compatibility
    }
    return result;
}

kvs_result_t _kvs_exec_hget(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hget(server, cmd->key, cmd->len_key, &(cmd->val), &(cmd->len_val));
}

kvs_result_t _kvs_exec_hdel(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hdel(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_hmod(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hmod(server, cmd->key, cmd->len_key, cmd->val, cmd->len_val);
}

kvs_result_t _kvs_exec_hexist(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    return kvs_server_hexist(server, cmd->key, cmd->len_key);
}

kvs_result_t _kvs_exec_save(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    if(KVS_OK == kvs_server_save_rdb_fork(server)) {
        struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->header.user_data;
        if(cli_ctx->header.type != KVS_CTX_NORMAL_CLIENT) {
            LOG_FATAL("invalid ctx type: %d", cli_ctx->header.type);
            assert(0);
            return KVS_RES_ERR;
        }
        cli_ctx->state = KVS_CLIENT_STATE_WAIT_BGSAVE; // set client state to wait bgsave

        return KVS_RES_RDB_SKIP_RESPONSE;
    }
    return KVS_RES_ERR;
    //return kvs_server_save_rdb(server);
}

kvs_result_t _kvs_exec_slave_sync(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server->master->slave_count + server->master->syncing_slaves_count >= server->master->max_slave_count) {
        // todo : return more error info to slave 
        return KVS_RES_ERR;
    }
    kvs_server_convert_conn_ctx(server, conn, KVS_CTX_SLAVE_OF_ME);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    slave_ctx->state = KVS_MY_SLAVE_NONE;
    kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_TRIGGER_MANUAL);

    return KVS_RES_SYNC_SLAVE;
    
}

kvs_result_t _kvs_exec_slave_sync_rdma(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server->master->slave_count + server->master->syncing_slaves_count >= server->master->max_slave_count) {
        // todo : return more error info to slave 
        return KVS_RES_ERR;
    }
    kvs_server_convert_conn_ctx(server, conn, KVS_CTX_SLAVE_OF_ME);
    struct kvs_my_slave_context_s* slave_ctx = (struct kvs_my_slave_context_s*)conn->header.user_data;
    slave_ctx->state = KVS_MY_SLAVE_NONE;
    kvs_master_slave_state_machine_tick(server->master, conn, KVS_EVENT_TRIGGER_MANUAL);

    return KVS_RES_SYNC_SLAVE;
}


kvs_result_t _kvs_exec_echo(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(server == NULL || cmd == NULL) {
        return KVS_RES_ERR;
    }
    // just echo the value back
    cmd->val = cmd->key;
    cmd->len_val = cmd->len_key;
    return KVS_RES_VAL;
}

kvs_result_t kvs_executor_cmd(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    if(cmd == NULL) return KVS_RES_ERR;

    switch(cmd->cmd_idx) {
        case KVS_CMD_SETQA:
            return kvs_qa_submit_setqa(server, cmd, conn);
        case KVS_CMD_GETQA:
            return kvs_qa_submit_getqa(server, cmd, conn);
        case KVS_CMD_DELQA:
            return kvs_qa_exec_delqa(server, cmd);
        case KVS_CMD_SETQA_APPLY:
            return kvs_qa_exec_apply(server, cmd);
        case KVS_CMD_CREATEV:
            return _kvs_exec_createv(server, cmd, conn);
        case KVS_CMD_SETV:
            return _kvs_exec_setv(server, cmd, conn);
        case KVS_CMD_GETV:
            return _kvs_exec_getv(server, cmd, conn);
        case KVS_CMD_DELV:
            return _kvs_exec_delv(server, cmd, conn);
        case KVS_CMD_VINFO:
            return _kvs_exec_vinfo(server, cmd, conn);
        case KVS_CMD_SETEX:
            return _kvs_exec_setex(server, cmd, conn);
        case KVS_CMD_SET:
            return _kvs_exec_hset(server, cmd, conn);
        case KVS_CMD_GET:
            return _kvs_exec_hget(server, cmd, conn);
        case KVS_CMD_DEL:
            return _kvs_exec_hdel(server, cmd, conn);
        case KVS_CMD_MOD:
            return _kvs_exec_hmod(server, cmd, conn);
        case KVS_CMD_EXIST:
            return _kvs_exec_hexist(server, cmd, conn);
        case KVS_CMD_RSET:
            return _kvs_exec_rset(server, cmd, conn);
        case KVS_CMD_RGET:
            return _kvs_exec_rget(server, cmd, conn);
        case KVS_CMD_RDEL:
            return _kvs_exec_rdel(server, cmd, conn);
        case KVS_CMD_RMOD:
            return _kvs_exec_rmod(server, cmd, conn);
        case KVS_CMD_REXIST:
            return _kvs_exec_rexist(server, cmd, conn);
        case KVS_CMD_ASET:
            return _kvs_exec_set(server, cmd, conn);
        case KVS_CMD_AGET:
            return _kvs_exec_get(server, cmd, conn);
        case KVS_CMD_ADEL:
            return _kvs_exec_del(server, cmd, conn);
        case KVS_CMD_AMOD:
            return _kvs_exec_mod(server, cmd, conn);
        case KVS_CMD_AEXIST:
            return _kvs_exec_exist(server, cmd, conn);
        case KVS_CMD_SAVE:
            return _kvs_exec_save(server, cmd, conn);
        case KVS_CMD_SLAVE_SYNC:
            return _kvs_exec_slave_sync(server, cmd, conn);
        case KVS_CMD_SLAVE_SYNC_RDMA:
            return _kvs_exec_slave_sync_rdma(server, cmd, conn);
        case KVS_CMD_ECHO:
            return _kvs_exec_echo(server, cmd, conn);
        case KVS_CMD_INVALID:
            return KVS_RES_UNKNOWN_CMD;
        default:
            return KVS_RES_ERR;
    }
}

static int kvs_parse_positive_ttl(const char *text, int length, unsigned long long *ttl) {
    unsigned long long value = 0;
    int i;
    if(text == NULL || ttl == NULL || length <= 0) return -1;
    for(i = 0; i < length; ++i) {
        unsigned int digit;
        if(text[i] < '0' || text[i] > '9') return -1;
        digit = (unsigned int)(text[i] - '0');
        if(value > (ULLONG_MAX - digit) / 10) return -1;
        value = value * 10 + digit;
    }
    if(value == 0) return -1;
    *ttl = value;
    return 0;
}

static kvs_result_t _kvs_exec_setex(struct kvs_server_s *server,
        struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    unsigned long long ttl;
    (void)conn;
    if(server == NULL || cmd == NULL || cmd->argc != 4 ||
       kvs_parse_positive_ttl(cmd->ttl, cmd->len_ttl, &ttl) < 0) {
        return KVS_RES_ERR;
    }
    return kvs_server_hsetex(server, cmd->key, cmd->len_key,
            cmd->val, cmd->len_val, ttl);
}


static int kvs_arg_eq(const char *arg, int len, const char *literal) {
    int i;
    if(arg == NULL || literal == NULL || len <= 0) return 0;
    for(i = 0; i < len && literal[i] != '\0'; i++) {
        char a = arg[i];
        char b = literal[i];
        if(a >= 'a' && a <= 'z') a = (char)(a - 32);
        if(b >= 'a' && b <= 'z') b = (char)(b - 32);
        if(a != b) return 0;
    }
    return i == len && literal[i] == '\0';
}

static int kvs_parse_uint32_arg(const char *text, int length, uint32_t *out) {
    unsigned long long value = 0;
    int i;

    if(text == NULL || out == NULL || length <= 0) return -1;

    for(i = 0; i < length; i++) {
        unsigned int digit;
        if(text[i] < '0' || text[i] > '9') return -1;
        digit = (unsigned int)(text[i] - '0');
        if(value > (UINT_MAX - digit) / 10) return -1;
        value = value * 10 + digit;
    }

    if(value == 0) return -1;
    *out = (uint32_t)value;
    return 0;
}

static int kvs_parse_metric_arg(char *arg, int len, vec_metric_t *metric) {
    if(metric == NULL) return -1;
    if(kvs_arg_eq(arg, len, "COSINE")) {
        *metric = VEC_METRIC_COSINE;
        return 0;
    }
    if(kvs_arg_eq(arg, len, "IP")) {
        *metric = VEC_METRIC_IP;
        return 0;
    }
    if(kvs_arg_eq(arg, len, "L2")) {
        *metric = VEC_METRIC_L2;
        return 0;
    }
    return -1;
}

static int kvs_parse_index_arg(char *arg, int len, vec_index_type_t *index_type) {
    if(index_type == NULL) return -1;
    if(kvs_arg_eq(arg, len, "FLAT")) {
        *index_type = VEC_INDEX_FLAT;
        return 0;
    }
    if(kvs_arg_eq(arg, len, "HNSW")) {
        *index_type = VEC_INDEX_HNSW;
        return 0;
    }
    return -1;
}

static const char *kvs_metric_name(vec_metric_t metric) {
    switch(metric) {
        case VEC_METRIC_COSINE: return "COSINE";
        case VEC_METRIC_IP: return "IP";
        case VEC_METRIC_L2: return "L2";
        default: return "UNKNOWN";
    }
}

static const char *kvs_index_name(vec_index_type_t index_type) {
    switch(index_type) {
        case VEC_INDEX_FLAT: return "FLAT";
        case VEC_INDEX_HNSW: return "HNSW";
        default: return "UNKNOWN";
    }
}

static int kvs_append_raw(struct kvs_conn_s *conn, const char *buf, int len) {
    return kvs_net_copy_msg_to_send_buf(conn, (char *)buf, len) == 0 ? 0 : -1;
}

static int kvs_append_bulk(struct kvs_conn_s *conn, const char *buf, int len) {
    char header[64];
    int header_len;

    if(conn == NULL || buf == NULL || len < 0) return -1;

    header_len = snprintf(header, sizeof(header), "$%d\r\n", len);
    if(header_len <= 0 || header_len >= (int)sizeof(header)) return -1;
    if(kvs_append_raw(conn, header, header_len) != 0) return -1;
    if(len > 0 && kvs_append_raw(conn, buf, len) != 0) return -1;
    return kvs_append_raw(conn, "\r\n", 2);
}

static int kvs_append_bulk_cstr(struct kvs_conn_s *conn, const char *str) {
    return kvs_append_bulk(conn, str, (int)strlen(str));
}

static int kvs_append_bulk_u64(struct kvs_conn_s *conn, uint64_t value) {
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%llu", (unsigned long long)value);
    if(len <= 0 || len >= (int)sizeof(buf)) return -1;
    return kvs_append_bulk(conn, buf, len);
}

static int kvs_append_bulk_float(struct kvs_conn_s *conn, float value) {
    char buf[64];
    int len = snprintf(buf, sizeof(buf), "%.9g", value);
    if(len <= 0 || len >= (int)sizeof(buf)) return -1;
    return kvs_append_bulk(conn, buf, len);
}

static kvs_result_t kvs_send_getv_response(struct kvs_server_s *server, struct kvs_conn_s *conn,
        kvs_vector_search_result_t *results, uint32_t result_count, int with_values) {
    char header[64];
    int header_len = snprintf(header, sizeof(header), "*%u\r\n", result_count);

    if(conn == NULL || results == NULL || header_len <= 0 || header_len >= (int)sizeof(header)) return KVS_RES_ERR;
    if(with_values && server == NULL) return KVS_RES_ERR;
    if(kvs_append_raw(conn, header, header_len) != 0) return KVS_RES_ERR;

    for(uint32_t i = 0; i < result_count; i++) {
        char *value = "";
        int len_value = 0;

        if(kvs_append_raw(conn, with_values ? "*3\r\n" : "*2\r\n", 4) != 0) return KVS_RES_ERR;
        if(kvs_append_bulk(conn, results[i].member, results[i].len_member) != 0) return KVS_RES_ERR;
        if(kvs_append_bulk_float(conn, results[i].score) != 0) return KVS_RES_ERR;

        if(with_values) {
            kvs_result_t value_result = kvs_server_hget(server, results[i].member, results[i].len_member,
                    &value, &len_value);
            if(value_result != KVS_RES_VAL) {
                value = "";
                len_value = 0;
            }
            if(kvs_append_bulk(conn, value, len_value) != 0) return KVS_RES_ERR;
        }
    }

    return KVS_RES_SKIP_RESPONSE;
}

static kvs_result_t kvs_send_vinfo_response(struct kvs_conn_s *conn, kvs_vector_info_t *info) {
    if(conn == NULL || info == NULL) return KVS_RES_ERR;

    if(kvs_append_raw(conn, "*18\r\n", 5) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "name") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk(conn, info->name, info->len_name) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "dim") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_u64(conn, info->dim) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "metric") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, kvs_metric_name(info->metric)) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "index") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, kvs_index_name(info->index_type)) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "block_size") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_u64(conn, info->block_size) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "block_count") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_u64(conn, info->block_count) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "total_count") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_u64(conn, info->total_count) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "active_count") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_u64(conn, info->active_count) != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_cstr(conn, "deleted_count") != 0) return KVS_RES_ERR;
    if(kvs_append_bulk_u64(conn, info->deleted_count) != 0) return KVS_RES_ERR;

    return KVS_RES_SKIP_RESPONSE;
}

static kvs_result_t _kvs_exec_createv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    uint32_t dim;
    vec_metric_t metric;
    vec_index_type_t index_type;
    (void)conn;

    if(server == NULL || cmd == NULL || cmd->argc != 8) return KVS_RES_ERR;
    if(!kvs_arg_eq(cmd->argv[2], cmd->argv_len[2], "DIM")) return KVS_RES_ERR;
    if(kvs_parse_uint32_arg(cmd->argv[3], cmd->argv_len[3], &dim) != 0) return KVS_RES_ERR;
    if(!kvs_arg_eq(cmd->argv[4], cmd->argv_len[4], "METRIC")) return KVS_RES_ERR;
    if(kvs_parse_metric_arg(cmd->argv[5], cmd->argv_len[5], &metric) != 0) return KVS_RES_ERR;
    if(!kvs_arg_eq(cmd->argv[6], cmd->argv_len[6], "INDEX")) return KVS_RES_ERR;
    if(kvs_parse_index_arg(cmd->argv[7], cmd->argv_len[7], &index_type) != 0) return KVS_RES_ERR;

    return kvs_server_createv(server, cmd->argv[1], cmd->argv_len[1], dim, metric, index_type);
}

static kvs_result_t _kvs_exec_setv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    kvs_result_t result;
    (void)conn;

    if(server == NULL || cmd == NULL || (cmd->argc != 5 && cmd->argc != 7)) return KVS_RES_ERR;
    if(!kvs_arg_eq(cmd->argv[3], cmd->argv_len[3], "FLOAT32")) return KVS_RES_ERR;

    if(cmd->argc == 7 &&
       !kvs_arg_eq(cmd->argv[5], cmd->argv_len[5], "VALUE") &&
       !kvs_arg_eq(cmd->argv[5], cmd->argv_len[5], "WITHVALUE")) {
        return KVS_RES_ERR;
    }
    if(cmd->argc == 7 && (cmd->argv[6] == NULL || cmd->argv_len[6] <= 0)) return KVS_RES_ERR;

    result = kvs_server_setv(server, cmd->argv[1], cmd->argv_len[1],
            cmd->argv[2], cmd->argv_len[2], cmd->argv[4], cmd->argv_len[4]);
    if(result != KVS_RES_OK) return result;

    if(cmd->argc == 7) {
        result = kvs_server_hset(server, cmd->argv[2], cmd->argv_len[2], cmd->argv[6], cmd->argv_len[6]);
        if(result == KVS_RES_EXIST) {
            result = kvs_server_hmod(server, cmd->argv[2], cmd->argv_len[2], cmd->argv[6], cmd->argv_len[6]);
        }
        if(result != KVS_RES_OK) return result;
    }

    return KVS_RES_OK;
}

static kvs_result_t _kvs_exec_getv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    uint32_t topk;
    uint32_t result_count = 0;
    int with_values = 0;
    kvs_vector_search_result_t *results;
    kvs_result_t result;

    if(server == NULL || cmd == NULL || conn == NULL || (cmd->argc != 6 && cmd->argc != 7)) return KVS_RES_ERR;
    if(!kvs_arg_eq(cmd->argv[2], cmd->argv_len[2], "FLOAT32")) return KVS_RES_ERR;
    if(!kvs_arg_eq(cmd->argv[4], cmd->argv_len[4], "TOPK")) return KVS_RES_ERR;
    if(kvs_parse_uint32_arg(cmd->argv[5], cmd->argv_len[5], &topk) != 0) return KVS_RES_ERR;
    if(cmd->argc == 7) {
        if(!kvs_arg_eq(cmd->argv[6], cmd->argv_len[6], "WITHVALUES")) return KVS_RES_ERR;
        with_values = 1;
    }
    if(topk > 1024) return KVS_RES_ERR;

    results = (kvs_vector_search_result_t *)kvs_malloc(sizeof(kvs_vector_search_result_t) * topk);
    if(results == NULL) return KVS_RES_ERR;
    memset(results, 0, sizeof(kvs_vector_search_result_t) * topk);

    result = kvs_server_getv(server, cmd->argv[1], cmd->argv_len[1],
            cmd->argv[3], cmd->argv_len[3], topk, results, &result_count);
    if(result == KVS_RES_VAL) {
        result = kvs_send_getv_response(server, conn, results, result_count, with_values);
    }

    kvs_free(results, sizeof(kvs_vector_search_result_t) * topk);
    return result;
}

static kvs_result_t _kvs_exec_delv(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    (void)conn;
    if(server == NULL || cmd == NULL || cmd->argc != 3) return KVS_RES_ERR;
    return kvs_server_delv(server, cmd->argv[1], cmd->argv_len[1], cmd->argv[2], cmd->argv_len[2]);
}

static kvs_result_t _kvs_exec_vinfo(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    kvs_vector_info_t info;
    kvs_result_t result;

    if(server == NULL || cmd == NULL || conn == NULL || cmd->argc != 2) return KVS_RES_ERR;

    memset(&info, 0, sizeof(info));
    result = kvs_server_vinfo(server, cmd->argv[1], cmd->argv_len[1], &info);
    if(result == KVS_RES_VAL) {
        result = kvs_send_vinfo_response(conn, &info);
    }
    return result;
}
