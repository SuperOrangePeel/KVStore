#ifndef __ECHO_H__
#define __ECHO_H__


struct kvs_conn_s;

int echo_on_accept(struct kvs_conn_s *conn);

int echo_on_msg(struct kvs_conn_s *conn, int *read_size);
int echo_on_send(struct kvs_conn_s *conn, int bytes_sent);

int echo_on_close(struct kvs_conn_s *conn);



#endif