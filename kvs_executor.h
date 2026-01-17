#ifndef __KVS_EXECUTOR_H__
#define __KVS_EXECUTOR_H__

#include "kvs_types.h"
#include "kvs_server.h"

struct kvs_conn_s;

kvs_result_t kvs_executor_cmd(struct kvs_server_s *server, struct kvs_handler_cmd_s *cmd, struct kvs_conn_s *conn);
#endif