#ifndef __KVS_SERVER_H__
#define __KVS_SERVER_H__
#include <stdlib.h>

#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1
#define ENABLE_HASH 1

#define KVS_MAX_CONNECTS 1024


typedef enum {
    KVS_SERVER_ROLE_MASTER = 0,
    KVS_SERVER_ROLE_SLAVE = 1,
} kvs_server_role_t;

struct kvs_mp_pool_s;
struct hashtable_s;
struct kvs_array_s;
struct _rbtree;
struct kvs_pers_context_s;


typedef struct kvs_conn_s {
	int fd;
	char* r_buffer;
	int r_total;
    int r_idx;
	char* response;
	int w_total;
    int w_idx;

	int state;

	struct kvs_server_s *server;
	int rdb_fd;
	size_t rdb_offset;
	size_t rdb_size;
	
	
} kvs_conn_t;



typedef struct kvs_server_s {
    unsigned short port;
    int server_fd;
    struct kvs_conn_s *conns;
    int role; // master/slave

    struct kvs_pers_context_s *pers_ctx;
    //kvs_mp_pool_t *mempool;
    struct hashtable_s *hash;
    struct kvs_array_s *array;
    struct _rbtree *rbtree;

    int (*on_msg)(struct kvs_conn_s *conn);


} kvs_server_t;

kvs_server_t *kvs_server_init(unsigned short port, int role);
void kvs_server_destroy(kvs_server_t *server);

#endif