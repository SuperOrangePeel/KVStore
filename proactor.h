#ifndef __KVS_PROACTOR_H__
#define __KVS_PROACTOR_H__

#include <stdlib.h>

#define ENTRIES_LENGTH		1024
#define BUFFER_LENGTH		4096// 1048576 // 1024 * 1024

struct kvs_server_s;



int proactor_start(struct kvs_server_s *server);

#endif