#ifndef __KVS_QA_H__
#define __KVS_QA_H__

#include <stdint.h>
#include <pthread.h>
#include <sys/types.h>
#include "kvs_types.h"
#include "kvs_event_loop.h"

#define KVS_EMBED_MAGIC_REQ  0x45515231u
#define KVS_EMBED_MAGIC_RESP 0x45535031u

typedef enum {
    QA_OP_SETQA = 0,
    QA_OP_GETQA = 1
} qa_op_t;

typedef enum {
    QA_SET_ID = 0,
    QA_SET_AUTO = 1
} qa_set_mode_t;

struct kvs_server_s;
struct kvs_conn_s;
struct kvs_server_config_s;

struct embed_req_header {
    uint32_t magic;
    uint32_t req_id;
    uint32_t text_len;
};

struct embed_resp_header {
    uint32_t magic;
    uint32_t req_id;
    uint32_t status;
    uint32_t dim;
    uint32_t vector_bytes;
};

typedef struct qa_task {
    qa_op_t op;
    qa_set_mode_t set_mode;
    struct kvs_server_s *server;
    struct kvs_conn_s *client;

    char *index;
    int len_index;
    char *member;
    int len_member;
    char *question;
    int len_question;
    char *answer;
    int len_answer;

    uint32_t topk;
    float *vector;
    uint32_t dim;
    int len_vector;

    int err;
    char err_msg[256];
    uint32_t req_id;
    struct qa_task *next;
} qa_task_t;

typedef struct qa_task_queue {
    qa_task_t *head;
    qa_task_t *tail;
    pthread_mutex_t mu;
    pthread_cond_t cond;
    int stop;
} qa_task_queue_t;

struct kvs_embedding_manager {
    int enabled;
    int worker_threads;
    int timeout_ms;
    int dim;
    char socket_path[256];
    char server_path[256];
    char model[256];
    pid_t child_pid;
    pthread_t *threads;
    qa_task_queue_t pending_queue;
    qa_task_queue_t done_queue;
    int eventfd;
    uint64_t eventfd_value;
    kvs_event_t event_ev;
    struct kvs_server_s *server;
    uint32_t next_req_id;
};

struct kvs_qa_manager {
    uint64_t next_auto_id;
    uint64_t start_auto_id;
    char auto_member_prefix[64];
    char auto_member_counter_key[128];
};

int kvs_qa_init(struct kvs_server_s *server, const struct kvs_server_config_s *conf);
void kvs_qa_deinit(struct kvs_server_s *server);
void kvs_qa_load_auto_id(struct kvs_server_s *server);
char *kvs_qa_next_member(struct kvs_server_s *server, int *len_member);
void kvs_qa_detach_client(struct kvs_conn_s *conn);

kvs_result_t kvs_qa_submit_setqa(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
kvs_result_t kvs_qa_submit_getqa(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
kvs_result_t kvs_qa_exec_delqa(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd);
kvs_result_t kvs_qa_exec_apply(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd);

int kvs_qa_build_apply_resp(char *index, int len_index, char *member, int len_member,
                            char *question, int len_question, char *answer, int len_answer,
                            const void *vector, int len_vector, char **raw_out, int *raw_len_out);

#endif
