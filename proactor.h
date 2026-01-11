#ifndef __KVS_PROACTOR_H__
#define __KVS_PROACTOR_H__

#include <stdlib.h>

#define ENTRIES_LENGTH		1024
#define BUFFER_LENGTH		1048576 // 1024 * 1024

struct kvs_server_s;
struct io_uring;
struct kvs_conn_s;

int kvs_proactor_init(struct kvs_server_s *server);
int kvs_proactor_destroy(struct kvs_server_s *server);

int kvs_proactor_start(struct kvs_server_s *server);
// int set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);
// int set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);

int kvs_proactor_set_send_event_raw_buffer(struct kvs_conn_s *conn, char *send_buf, int send_buf_sz);
int kvs_proactor_set_send_event(struct kvs_conn_s *conn, char *msg, int msg_sz);
int kvs_proactor_set_send_event_manual(struct kvs_conn_s *conn);
int kvs_proactor_set_recv_event(struct kvs_conn_s *conn);

#endif