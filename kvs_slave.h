#ifndef __KVS_SLAVE_H__
#define __KVS_SLAVE_H__

struct kvs_server_s;

int kvs_slave_connect_master(struct kvs_server_s *server);

#endif