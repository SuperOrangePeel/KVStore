#ifndef __KVS_RESPONSE_H__
#define __KVS_RESPONSE_H__

#include "kvs_types.h"

struct kvs_conn_s;

kvs_status_t kvs_format_response(kvs_result_t result, char *value, int len_val, struct kvs_conn_s *conn);

#endif 