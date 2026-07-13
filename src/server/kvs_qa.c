
#include "kvs_qa.h"
#include "kvs_server.h"
#include "kvs_network.h"
#include "common.h"
#include "logger.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

static int qa_arg_eq(const char *arg, int len, const char *lit) {
    int i;
    if(arg == NULL || lit == NULL || len <= 0) return 0;
    for(i = 0; i < len && lit[i] != '\0'; i++) {
        char a = arg[i], b = lit[i];
        if(a >= 'a' && a <= 'z') a = (char)(a - 32);
        if(b >= 'a' && b <= 'z') b = (char)(b - 32);
        if(a != b) return 0;
    }
    return i == len && lit[i] == '\0';
}

static char *qa_memdup(const char *src, int len) {
    char *p;
    if(src == NULL || len <= 0) return NULL;
    p = (char *)malloc((size_t)len + 1);
    if(p == NULL) return NULL;
    memcpy(p, src, (size_t)len);
    p[len] = '\0';
    return p;
}

static void qa_queue_init(qa_task_queue_t *q) {
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->cond, NULL);
}

static void qa_queue_destroy(qa_task_queue_t *q) {
    pthread_mutex_destroy(&q->mu);
    pthread_cond_destroy(&q->cond);
}

static void qa_queue_push(qa_task_queue_t *q, qa_task_t *task) {
    task->next = NULL;
    pthread_mutex_lock(&q->mu);
    if(q->tail) q->tail->next = task;
    else q->head = task;
    q->tail = task;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mu);
}

static qa_task_t *qa_queue_pop_wait(qa_task_queue_t *q) {
    qa_task_t *task;
    pthread_mutex_lock(&q->mu);
    while(q->head == NULL && !q->stop) pthread_cond_wait(&q->cond, &q->mu);
    if(q->stop && q->head == NULL) {
        pthread_mutex_unlock(&q->mu);
        return NULL;
    }
    task = q->head;
    q->head = task->next;
    if(q->head == NULL) q->tail = NULL;
    task->next = NULL;
    pthread_mutex_unlock(&q->mu);
    return task;
}

static qa_task_t *qa_queue_pop_nowait(qa_task_queue_t *q) {
    qa_task_t *task;
    pthread_mutex_lock(&q->mu);
    task = q->head;
    if(task != NULL) {
        q->head = task->next;
        if(q->head == NULL) q->tail = NULL;
        task->next = NULL;
    }
    pthread_mutex_unlock(&q->mu);
    return task;
}

static void qa_queue_stop(qa_task_queue_t *q) {
    pthread_mutex_lock(&q->mu);
    q->stop = 1;
    pthread_cond_broadcast(&q->cond);
    pthread_mutex_unlock(&q->mu);
}

static void qa_task_free(qa_task_t *task) {
    if(task == NULL) return;
    free(task->index);
    free(task->member);
    free(task->question);
    free(task->answer);
    free(task->vector);
    free(task);
}

static void qa_task_error(qa_task_t *task, const char *fmt, ...) {
    va_list ap;
    if(task == NULL) return;
    task->err = 1;
    va_start(ap, fmt);
    vsnprintf(task->err_msg, sizeof(task->err_msg), fmt, ap);
    va_end(ap);
}

static int qa_write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while(len > 0) {
        ssize_t n = write(fd, p, len);
        if(n < 0) {
            if(errno == EINTR) continue;
            return -1;
        }
        if(n == 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int qa_read_all(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    while(len > 0) {
        ssize_t n = read(fd, p, len);
        if(n < 0) {
            if(errno == EINTR) continue;
            return -1;
        }
        if(n == 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int qa_connect_socket(const char *path) {
    int fd;
    struct sockaddr_un addr;
    if(path == NULL || strlen(path) >= sizeof(addr.sun_path)) return -1;
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if(fd < 0) return -1;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if(connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int qa_embed_request(struct kvs_embedding_manager *mgr, int fd, qa_task_t *task) {
    struct embed_req_header req;
    struct embed_resp_header resp;
    if(mgr == NULL || task == NULL || fd < 0) return -1;
    req.magic = KVS_EMBED_MAGIC_REQ;
    req.req_id = task->req_id;
    req.text_len = (uint32_t)task->len_question;
    if(qa_write_all(fd, &req, sizeof(req)) != 0 || qa_write_all(fd, task->question, (size_t)task->len_question) != 0) {
        qa_task_error(task, "embedding socket write failed");
        return -1;
    }
    if(qa_read_all(fd, &resp, sizeof(resp)) != 0) {
        qa_task_error(task, "embedding socket read failed");
        return -1;
    }
    if(resp.magic != KVS_EMBED_MAGIC_RESP || resp.req_id != task->req_id) {
        qa_task_error(task, "embedding bad response header");
        return -1;
    }
    if(resp.status != 0) {
        qa_task_error(task, "embedding service returned status %u", resp.status);
        return -1;
    }
    if(resp.dim == 0 || resp.vector_bytes != resp.dim * sizeof(float) ||
       (mgr->dim > 0 && (int)resp.dim != mgr->dim)) {
        qa_task_error(task, "embedding dimension mismatch: %u", resp.dim);
        return -1;
    }
    task->vector = (float *)malloc(resp.vector_bytes);
    if(task->vector == NULL) {
        qa_task_error(task, "out of memory");
        return -1;
    }
    if(qa_read_all(fd, task->vector, resp.vector_bytes) != 0) {
        qa_task_error(task, "embedding vector read failed");
        return -1;
    }
    task->dim = resp.dim;
    task->len_vector = (int)resp.vector_bytes;
    return 0;
}

static void *qa_worker_main(void *arg) {
    struct kvs_embedding_manager *mgr = (struct kvs_embedding_manager *)arg;
    int fd = -1;
    while(1) {
        qa_task_t *task;
        if(fd < 0) fd = qa_connect_socket(mgr->socket_path);
        task = qa_queue_pop_wait(&mgr->pending_queue);
        if(task == NULL) break;
        if(fd < 0) {
            qa_task_error(task, "embedding socket connect failed: %s", mgr->socket_path);
        } else if(qa_embed_request(mgr, fd, task) != 0) {
            LOG_ERROR("QA embedding request failed: %s", task->err_msg);
            close(fd);
            fd = -1;
        } else {
            LOG_DEBUG("QA embedding request done req_id=%u", task->req_id);
        }
        qa_queue_push(&mgr->done_queue, task);
        uint64_t one = 1;
        ssize_t wn = write(mgr->eventfd, &one, sizeof(one));
        (void)wn;
    }
    if(fd >= 0) close(fd);
    return NULL;
}

static int qa_wait_socket(const char *path, int timeout_ms) {
    int waited = 0;
    while(waited <= timeout_ms) {
        int fd = qa_connect_socket(path);
        if(fd >= 0) {
            close(fd);
            return 0;
        }
        usleep(50000);
        waited += 50;
    }
    return -1;
}

static int qa_spawn_embedding(struct kvs_embedding_manager *mgr) {
    pid_t pid;
    char dimbuf[32];
    if(mgr == NULL || !mgr->enabled) return 0;
    snprintf(dimbuf, sizeof(dimbuf), "%d", mgr->dim > 0 ? mgr->dim : 512);
    unlink(mgr->socket_path);
    pid = fork();
    if(pid < 0) return -1;
    if(pid == 0) {
        sigset_t empty_mask;
        sigemptyset(&empty_mask);
        sigprocmask(SIG_SETMASK, &empty_mask, NULL);
        signal(SIGINT, SIG_DFL);
        signal(SIGTERM, SIG_DFL);
        signal(SIGCHLD, SIG_DFL);

        execl(mgr->server_path, mgr->server_path,
              "--socket", mgr->socket_path,
              "--model", mgr->model,
              "--dim", dimbuf,
              (char *)NULL);
        execlp("python3", "python3", mgr->server_path,
               "--socket", mgr->socket_path,
               "--model", mgr->model,
               "--dim", dimbuf,
               (char *)NULL);
        _exit(127);
    }
    mgr->child_pid = pid;
    return qa_wait_socket(mgr->socket_path, mgr->timeout_ms);
}

char *kvs_qa_next_member(struct kvs_server_s *server, int *len_member) {
    char buf[160];
    int len;
    if(server == NULL || server->qa == NULL || len_member == NULL) return NULL;
    len = snprintf(buf, sizeof(buf), "%s%llu", server->qa->auto_member_prefix,
                   (unsigned long long)server->qa->next_auto_id++);
    if(len <= 0 || len >= (int)sizeof(buf)) return NULL;
    *len_member = len;
    return qa_memdup(buf, len);
}

static void qa_save_auto_id(struct kvs_server_s *server) {
    char buf[32];
    int len;
    kvs_result_t r;
    if(server == NULL || server->qa == NULL) return;
    len = snprintf(buf, sizeof(buf), "%llu", (unsigned long long)server->qa->next_auto_id);
    r = kvs_server_hset(server, server->qa->auto_member_counter_key,
                        (int)strlen(server->qa->auto_member_counter_key), buf, len);
    if(r == KVS_RES_EXIST) {
        kvs_server_hmod(server, server->qa->auto_member_counter_key,
                        (int)strlen(server->qa->auto_member_counter_key), buf, len);
    }
}

void kvs_qa_load_auto_id(struct kvs_server_s *server) {
    char *value = NULL;
    int len_value = 0;
    unsigned long long id = 0;
    if(server == NULL || server->qa == NULL) return;
    if(kvs_server_hget(server, server->qa->auto_member_counter_key,
                       (int)strlen(server->qa->auto_member_counter_key), &value, &len_value) == KVS_RES_VAL) {
        for(int i = 0; i < len_value; i++) {
            if(value[i] < '0' || value[i] > '9') return;
            id = id * 10 + (unsigned long long)(value[i] - '0');
        }
        if(id > server->qa->next_auto_id) server->qa->next_auto_id = id;
    }
}

static uint64_t qa_parse_member_id(struct kvs_server_s *server, char *member, int len_member) {
    uint64_t id = 0;
    int prefix_len;
    if(server == NULL || server->qa == NULL || member == NULL) return 0;
    prefix_len = (int)strlen(server->qa->auto_member_prefix);
    if(len_member <= prefix_len || memcmp(member, server->qa->auto_member_prefix, (size_t)prefix_len) != 0) return 0;
    for(int i = prefix_len; i < len_member; i++) {
        if(member[i] < '0' || member[i] > '9') return 0;
        id = id * 10 + (uint64_t)(member[i] - '0');
    }
    return id;
}

static void qa_maybe_advance_auto_id(struct kvs_server_s *server, char *member, int len_member) {
    uint64_t id = qa_parse_member_id(server, member, len_member);
    if(id > 0 && server->qa != NULL && server->qa->next_auto_id <= id) {
        server->qa->next_auto_id = id + 1;
        qa_save_auto_id(server);
    }
}

static qa_task_t *qa_task_from_common(struct kvs_server_s *server, struct kvs_conn_s *conn,
                                      qa_op_t op, char *index, int len_index,
                                      char *member, int len_member,
                                      char *question, int len_question,
                                      char *answer, int len_answer,
                                      uint32_t topk) {
    qa_task_t *task = (qa_task_t *)calloc(1, sizeof(*task));
    if(task == NULL) return NULL;
    task->server = server;
    task->client = conn;
    task->op = op;
    task->index = qa_memdup(index, len_index);
    task->len_index = len_index;
    task->question = qa_memdup(question, len_question);
    task->len_question = len_question;
    task->topk = topk;
    if(member && len_member > 0) {
        task->member = qa_memdup(member, len_member);
        task->len_member = len_member;
    }
    if(answer && len_answer > 0) {
        task->answer = qa_memdup(answer, len_answer);
        task->len_answer = len_answer;
    }
    if(task->index == NULL || task->question == NULL || (member && task->member == NULL) || (answer && task->answer == NULL)) {
        qa_task_free(task);
        return NULL;
    }
    return task;
}

static int qa_block_client(struct kvs_embedding_manager *mgr, qa_task_t *task) {
    struct kvs_client_context_s *ctx;
    if(mgr == NULL || task == NULL || task->client == NULL) return -1;
    ctx = (struct kvs_client_context_s *)task->client->header.user_data;
    if(ctx == NULL || ctx->header.type != KVS_CTX_NORMAL_CLIENT) return -1;
    ctx->flags |= KVS_CLIENT_BLOCKED_EMBEDDING;
    ctx->blocked_task = task;
    task->client->_internal.is_pending = 1;
    task->req_id = __sync_add_and_fetch(&mgr->next_req_id, 1);
    LOG_DEBUG("QA task queued op=%d req_id=%u", task->op, task->req_id);
    qa_queue_push(&mgr->pending_queue, task);
    return 0;
}

static int qa_parse_uint32(const char *s, int len, uint32_t *out) {
    uint64_t v = 0;
    if(s == NULL || out == NULL || len <= 0) return -1;
    for(int i = 0; i < len; i++) {
        if(s[i] < '0' || s[i] > '9') return -1;
        v = v * 10 + (uint64_t)(s[i] - '0');
        if(v > 1000000) return -1;
    }
    if(v == 0) return -1;
    *out = (uint32_t)v;
    return 0;
}

kvs_result_t kvs_qa_submit_setqa(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    qa_task_t *task;
    char *member = NULL;
    int len_member = 0;
    int auto_mode = 0;
    if(server == NULL || cmd == NULL || conn == NULL || server->embedding == NULL || !server->embedding->enabled) return KVS_RES_ERR;
    if(server->role != KVS_SERVER_ROLE_MASTER) return KVS_RES_ERR;
    if(cmd->argc == 8 && qa_arg_eq(cmd->argv[2], cmd->argv_len[2], "ID") &&
       qa_arg_eq(cmd->argv[4], cmd->argv_len[4], "QUESTION") && qa_arg_eq(cmd->argv[6], cmd->argv_len[6], "ANSWER")) {
        member = cmd->argv[3]; len_member = cmd->argv_len[3];
    } else if(cmd->argc == 7 && qa_arg_eq(cmd->argv[2], cmd->argv_len[2], "AUTO") &&
              qa_arg_eq(cmd->argv[3], cmd->argv_len[3], "QUESTION") && qa_arg_eq(cmd->argv[5], cmd->argv_len[5], "ANSWER")) {
        auto_mode = 1;
        member = kvs_qa_next_member(server, &len_member);
        if(member == NULL) return KVS_RES_ERR;
        qa_save_auto_id(server);
    } else {
        return KVS_RES_ERR;
    }
    task = qa_task_from_common(server, conn, QA_OP_SETQA, cmd->argv[1], cmd->argv_len[1],
                               member, len_member,
                               auto_mode ? cmd->argv[4] : cmd->argv[5], auto_mode ? cmd->argv_len[4] : cmd->argv_len[5],
                               auto_mode ? cmd->argv[6] : cmd->argv[7], auto_mode ? cmd->argv_len[6] : cmd->argv_len[7], 0);
    if(auto_mode) free(member);
    if(task == NULL) return KVS_RES_ERR;
    task->set_mode = auto_mode ? QA_SET_AUTO : QA_SET_ID;
    if(qa_block_client(server->embedding, task) != 0) {
        qa_task_free(task);
        return KVS_RES_ERR;
    }
    return KVS_RES_BLOCKED;
}

kvs_result_t kvs_qa_submit_getqa(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn) {
    uint32_t topk;
    qa_task_t *task;
    if(server == NULL || cmd == NULL || conn == NULL || server->embedding == NULL || !server->embedding->enabled) return KVS_RES_ERR;
    if(cmd->argc != 6 || !qa_arg_eq(cmd->argv[2], cmd->argv_len[2], "QUESTION") ||
       !qa_arg_eq(cmd->argv[4], cmd->argv_len[4], "TOPK") || qa_parse_uint32(cmd->argv[5], cmd->argv_len[5], &topk) != 0) {
        return KVS_RES_ERR;
    }
    task = qa_task_from_common(server, conn, QA_OP_GETQA, cmd->argv[1], cmd->argv_len[1],
                               NULL, 0, cmd->argv[3], cmd->argv_len[3], NULL, 0, topk);
    if(task == NULL) return KVS_RES_ERR;
    if(qa_block_client(server->embedding, task) != 0) {
        qa_task_free(task);
        return KVS_RES_ERR;
    }
    return KVS_RES_BLOCKED;
}

kvs_result_t kvs_server_setqa_apply(struct kvs_server_s *server, char *index, int len_index,
        char *member, int len_member, char *question, int len_question,
        char *answer, int len_answer, const void *vector, int len_vector) {
    kvs_result_t r;
    char *qkey;
    int len_qkey;
    if(server == NULL || index == NULL || member == NULL || question == NULL || answer == NULL || vector == NULL) return KVS_RES_ERR;
    r = kvs_server_hset(server, member, len_member, answer, len_answer);
    if(r == KVS_RES_EXIST) r = kvs_server_hmod(server, member, len_member, answer, len_answer);
    if(r != KVS_RES_OK) return r;
    len_qkey = len_member + 2;
    qkey = (char *)malloc((size_t)len_qkey);
    if(qkey == NULL) return KVS_RES_ERR;
    memcpy(qkey, member, (size_t)len_member);
    memcpy(qkey + len_member, ":q", 2);
    r = kvs_server_hset(server, qkey, len_qkey, question, len_question);
    if(r == KVS_RES_EXIST) r = kvs_server_hmod(server, qkey, len_qkey, question, len_question);
    free(qkey);
    if(r != KVS_RES_OK) return r;
    r = kvs_server_setv(server, index, len_index, member, len_member, vector, len_vector);
    if(r == KVS_RES_NOT_FOUND) {
        /* first-use convenience: create FLAT/COSINE collection with vector dim */
        uint32_t dim = (uint32_t)(len_vector / (int)sizeof(float));
        if(dim > 0 && kvs_server_createv(server, index, len_index, dim, VEC_METRIC_COSINE, VEC_INDEX_FLAT) == KVS_RES_OK) {
            r = kvs_server_setv(server, index, len_index, member, len_member, vector, len_vector);
        }
    }
    if(r == KVS_RES_OK) qa_maybe_advance_auto_id(server, member, len_member);
    return r;
}

kvs_result_t kvs_qa_exec_apply(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd) {
    if(server == NULL || cmd == NULL || cmd->argc != 9) return KVS_RES_ERR;
    if(!qa_arg_eq(cmd->argv[3], cmd->argv_len[3], "QUESTION") ||
       !qa_arg_eq(cmd->argv[5], cmd->argv_len[5], "ANSWER") ||
       !qa_arg_eq(cmd->argv[7], cmd->argv_len[7], "FLOAT32")) return KVS_RES_ERR;
    return kvs_server_setqa_apply(server, cmd->argv[1], cmd->argv_len[1], cmd->argv[2], cmd->argv_len[2],
                                  cmd->argv[4], cmd->argv_len[4], cmd->argv[6], cmd->argv_len[6],
                                  cmd->argv[8], cmd->argv_len[8]);
}

kvs_result_t kvs_qa_exec_delqa(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd) {
    kvs_result_t r;
    char *qkey;
    int len_qkey;
    if(server == NULL || cmd == NULL || cmd->argc != 3) return KVS_RES_ERR;
    r = kvs_server_delv(server, cmd->argv[1], cmd->argv_len[1], cmd->argv[2], cmd->argv_len[2]);
    (void)kvs_server_hdel(server, cmd->argv[2], cmd->argv_len[2]);
    len_qkey = cmd->argv_len[2] + 2;
    qkey = (char *)malloc((size_t)len_qkey);
    if(qkey != NULL) {
        memcpy(qkey, cmd->argv[2], (size_t)cmd->argv_len[2]);
        memcpy(qkey + cmd->argv_len[2], ":q", 2);
        (void)kvs_server_hdel(server, qkey, len_qkey);
        free(qkey);
    }
    return (r == KVS_RES_OK || r == KVS_RES_NOT_FOUND) ? KVS_RES_OK : r;
}

static int qa_append_bulk_len(char *p, const char *data, int len) {
    int n = sprintf(p, "$%d\r\n", len);
    memcpy(p + n, data, (size_t)len);
    memcpy(p + n + len, "\r\n", 2);
    return n + len + 2;
}

int kvs_qa_build_apply_resp(char *index, int len_index, char *member, int len_member,
                            char *question, int len_question, char *answer, int len_answer,
                            const void *vector, int len_vector, char **raw_out, int *raw_len_out) {
    const char *cmd = "SETQA.APPLY", *kwq = "QUESTION", *kwa = "ANSWER", *kwf = "FLOAT32";
    int lens[9] = {11, len_index, len_member, 8, len_question, 6, len_answer, 7, len_vector};
    const char *datas[9] = {cmd, index, member, kwq, question, kwa, answer, kwf, (const char *)vector};
    int total = 32;
    char *raw, *p;
    if(raw_out == NULL || raw_len_out == NULL) return -1;
    for(int i = 0; i < 9; i++) total += 32 + lens[i];
    raw = (char *)malloc((size_t)total);
    if(raw == NULL) return -1;
    p = raw;
    p += sprintf(p, "*9\r\n");
    for(int i = 0; i < 9; i++) p += qa_append_bulk_len(p, datas[i], lens[i]);
    *raw_out = raw;
    *raw_len_out = (int)(p - raw);
    return 0;
}

static int qa_send_error(struct kvs_conn_s *conn, const char *msg) {
    char buf[320];
    int len = snprintf(buf, sizeof(buf), "-ERR %s\r\n", msg ? msg : "embedding failed");
    if(len <= 0 || len >= (int)sizeof(buf)) return -1;
    return kvs_net_copy_msg_to_send_buf(conn, buf, len);
}

static int qa_send_get_results(struct kvs_server_s *server, struct kvs_conn_s *conn, qa_task_t *task) {
    kvs_vector_search_result_t *results;
    uint32_t count = 0;
    kvs_result_t r;
    char head[64];
    int hlen;
    if(task->topk == 0 || task->topk > 1024) return -1;
    results = (kvs_vector_search_result_t *)calloc(task->topk, sizeof(*results));
    if(results == NULL) return -1;
    r = kvs_server_getv(server, task->index, task->len_index, task->vector, task->len_vector, task->topk, results, &count);
    if(r != KVS_RES_VAL) count = 0;
    hlen = snprintf(head, sizeof(head), "*%u\r\n", count);
    kvs_net_copy_msg_to_send_buf(conn, head, hlen);
    for(uint32_t i = 0; i < count; i++) {
        char *value = "";
        int len_value = 0;
        kvs_net_copy_msg_to_send_buf(conn, "*3\r\n", 4);
        char bulk[64];
        int bl = snprintf(bulk, sizeof(bulk), "$%d\r\n", results[i].len_member);
        kvs_net_copy_msg_to_send_buf(conn, bulk, bl);
        kvs_net_copy_msg_to_send_buf(conn, results[i].member, results[i].len_member);
        kvs_net_copy_msg_to_send_buf(conn, "\r\n", 2);
        bl = snprintf(bulk, sizeof(bulk), "$%.0d%g\r\n", 0, results[i].score);
        char score[64]; int sl = snprintf(score, sizeof(score), "%.9g", results[i].score);
        bl = snprintf(bulk, sizeof(bulk), "$%d\r\n", sl);
        kvs_net_copy_msg_to_send_buf(conn, bulk, bl);
        kvs_net_copy_msg_to_send_buf(conn, score, sl);
        kvs_net_copy_msg_to_send_buf(conn, "\r\n", 2);
        if(kvs_server_hget(server, results[i].member, results[i].len_member, &value, &len_value) != KVS_RES_VAL) {
            value = ""; len_value = 0;
        }
        bl = snprintf(bulk, sizeof(bulk), "$%d\r\n", len_value);
        kvs_net_copy_msg_to_send_buf(conn, bulk, bl);
        if(len_value > 0) kvs_net_copy_msg_to_send_buf(conn, value, len_value);
        kvs_net_copy_msg_to_send_buf(conn, "\r\n", 2);
    }
    free(results);
    return 0;
}

static void qa_resume_task(struct kvs_embedding_manager *mgr, qa_task_t *task) {
    struct kvs_server_s *server = mgr->server;
    struct kvs_conn_s *conn = task->client;
    struct kvs_client_context_s *ctx = conn ? (struct kvs_client_context_s *)conn->header.user_data : NULL;
    if(conn == NULL || conn->_internal.is_closed || ctx == NULL) {
        qa_task_free(task);
        return;
    }
    LOG_DEBUG("QA resume task op=%d err=%d", task->op, task->err);
    if(task->err) {
        qa_send_error(conn, task->err_msg);
    } else if(task->op == QA_OP_SETQA) {
        kvs_result_t r = kvs_server_setqa_apply(server, task->index, task->len_index, task->member, task->len_member,
                                                task->question, task->len_question, task->answer, task->len_answer,
                                                task->vector, task->len_vector);
        if(r == KVS_RES_OK) {
            char *raw = NULL; int raw_len = 0;
            if(kvs_qa_build_apply_resp(task->index, task->len_index, task->member, task->len_member,
                                       task->question, task->len_question, task->answer, task->len_answer,
                                       task->vector, task->len_vector, &raw, &raw_len) == 0) {
                kvs_server_write_and_replicate_raw(server, raw, raw_len);
                free(raw);
            }
            if(task->set_mode == QA_SET_AUTO) {
                char header[64]; int hl = snprintf(header, sizeof(header), "$%d\r\n", task->len_member);
                kvs_net_copy_msg_to_send_buf(conn, header, hl);
                kvs_net_copy_msg_to_send_buf(conn, task->member, task->len_member);
                kvs_net_copy_msg_to_send_buf(conn, "\r\n", 2);
            } else {
                kvs_net_copy_msg_to_send_buf(conn, "+OK\r\n", 5);
            }
        } else {
            qa_send_error(conn, "SETQA apply failed");
        }
    } else {
        qa_send_get_results(server, conn, task);
    }
    ctx->flags &= ~KVS_CLIENT_BLOCKED_EMBEDDING;
    ctx->blocked_task = NULL;
    conn->_internal.is_pending = 0;
    kvs_net_set_send_event_manual(conn);
    if(conn->r_idx > 0) kvs_client_process_buffer(conn);
    qa_task_free(task);
}

static void qa_event_handler(void *ctx, int res, int flags) {
    struct kvs_embedding_manager *mgr = (struct kvs_embedding_manager *)ctx;
    uint64_t v;
    (void)res; (void)flags;
    if(mgr == NULL) return;
    while(read(mgr->eventfd, &v, sizeof(v)) == sizeof(v)) {}
    LOG_DEBUG("QA eventfd triggered");
    while(1) {
        qa_task_t *task = qa_queue_pop_nowait(&mgr->done_queue);
        if(task == NULL) break;
        qa_resume_task(mgr, task);
    }
    kvs_loop_add_poll_in(&mgr->server->loop, &mgr->event_ev);
}

int kvs_qa_init(struct kvs_server_s *server, const struct kvs_server_config_s *conf) {
    struct kvs_qa_manager *qa;
    struct kvs_embedding_manager *mgr;
    if(server == NULL || conf == NULL) return -1;
    qa = (struct kvs_qa_manager *)calloc(1, sizeof(*qa));
    if(qa == NULL) return -1;
    qa->next_auto_id = conf->qa_auto_member_start;
    qa->start_auto_id = conf->qa_auto_member_start;
    strncpy(qa->auto_member_prefix, conf->qa_auto_member_prefix, sizeof(qa->auto_member_prefix) - 1);
    strncpy(qa->auto_member_counter_key, conf->qa_auto_member_counter_key, sizeof(qa->auto_member_counter_key) - 1);
    server->qa = qa;

    mgr = (struct kvs_embedding_manager *)calloc(1, sizeof(*mgr));
    if(mgr == NULL) return -1;
    mgr->enabled = conf->embedding_enabled;
    mgr->worker_threads = conf->embedding_worker_threads > 0 ? conf->embedding_worker_threads : 1;
    mgr->timeout_ms = conf->embedding_timeout_ms > 0 ? conf->embedding_timeout_ms : 3000;
    mgr->dim = conf->embedding_dim > 0 ? conf->embedding_dim : 512;
    mgr->server = server;
    strncpy(mgr->socket_path, conf->embedding_socket_path, sizeof(mgr->socket_path) - 1);
    strncpy(mgr->server_path, conf->embedding_server_path, sizeof(mgr->server_path) - 1);
    strncpy(mgr->model, conf->embedding_model, sizeof(mgr->model) - 1);
    qa_queue_init(&mgr->pending_queue);
    qa_queue_init(&mgr->done_queue);
    server->embedding = mgr;
    if(!mgr->enabled) return 0;
    mgr->eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if(mgr->eventfd < 0) return -1;
    if(qa_spawn_embedding(mgr) != 0) {
        LOG_ERROR("embedding service did not become ready: %s", mgr->socket_path);
        mgr->enabled = 0;
        if(mgr->eventfd >= 0) {
            close(mgr->eventfd);
            mgr->eventfd = -1;
        }
        return 0;
    }
    mgr->threads = (pthread_t *)calloc((size_t)mgr->worker_threads, sizeof(pthread_t));
    if(mgr->threads == NULL) return -1;
    for(int i = 0; i < mgr->worker_threads; i++) pthread_create(&mgr->threads[i], NULL, qa_worker_main, mgr);
    kvs_event_init(&mgr->event_ev, mgr->eventfd, KVS_EV_POLL_IN, qa_event_handler, mgr);
    kvs_loop_add_poll_in(&server->loop, &mgr->event_ev);
    return 0;
}

static void qa_stop_embedding_child(struct kvs_embedding_manager *mgr) {
    int status;
    if(mgr == NULL || mgr->child_pid <= 0) return;

    kill(mgr->child_pid, SIGTERM);
    for(int i = 0; i < 20; i++) {
        pid_t r = waitpid(mgr->child_pid, &status, WNOHANG);
        if(r == mgr->child_pid || (r < 0 && errno == ECHILD)) {
            mgr->child_pid = -1;
            return;
        }
        usleep(50000);
    }

    LOG_WARN("embedding child %d did not exit after SIGTERM, killing", mgr->child_pid);
    kill(mgr->child_pid, SIGKILL);
    waitpid(mgr->child_pid, &status, 0);
    mgr->child_pid = -1;
}

void kvs_qa_deinit(struct kvs_server_s *server) {
    struct kvs_embedding_manager *mgr;
    if(server == NULL) return;
    mgr = server->embedding;
    if(mgr != NULL) {
        qa_queue_stop(&mgr->pending_queue);
        qa_stop_embedding_child(mgr);
        if(mgr->threads) {
            for(int i = 0; i < mgr->worker_threads; i++) pthread_join(mgr->threads[i], NULL);
            free(mgr->threads);
        }
        if(mgr->eventfd >= 0) close(mgr->eventfd);
        unlink(mgr->socket_path);
        qa_queue_destroy(&mgr->pending_queue);
        qa_queue_destroy(&mgr->done_queue);
        free(mgr);
        server->embedding = NULL;
    }
    if(server->qa) {
        free(server->qa);
        server->qa = NULL;
    }
}

void kvs_qa_detach_client(struct kvs_conn_s *conn) {
    struct kvs_client_context_s *ctx;
    if(conn == NULL || conn->header.user_data == NULL) return;
    ctx = (struct kvs_client_context_s *)conn->header.user_data;
    if(ctx->header.type != KVS_CTX_NORMAL_CLIENT) return;
    ctx->flags &= ~KVS_CLIENT_BLOCKED_EMBEDDING;
    ctx->blocked_task = NULL;
}
