#ifndef __KVS_PROACTOR_H__
#define __KVS_PROACTOR_H__

#include <stdlib.h>

#define ENTRIES_LENGTH		1024
#define BUFFER_LENGTH		4096// 1048576 // 1024 * 1024

struct kvs_server_s;
struct io_uring;


int kvs_proactor_init(struct kvs_server_s *server);
int kvs_proactor_destroy(struct kvs_server_s *server);

int kvs_proactor_start(struct kvs_server_s *server);
int set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);
int set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);

#endif