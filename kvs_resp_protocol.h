#ifndef __KVS_RESP_PROTOCOL_H__
#define __KVS_RESP_PROTOCOL_H__

#include "kvs_types.h"

struct kvs_handler_cmd_s;
kvs_status_t kvs_protocol(char* msg, int length, struct kvs_handler_cmd_s *cmd_pt, int *parsed_length);

#endif