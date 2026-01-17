#ifndef __KVS_PROACTOR_H__
#define __KVS_PROACTOR_H__

#include <stdlib.h>
#include <liburing.h>

#define ENTRIES_LENGTH		1024
#define BUFFER_SIZE_DEFAULT 1048576 // 1MB


// kvs_proactor.h
typedef enum {
    KVS_IO_ACCEPT = 0,
    KVS_IO_READ,
    KVS_IO_WRITE,
    KVS_IO_TIMER,
    KVS_IO_SIGNAL,  // 信号
    KVS_IO_AOF      // AOF 落盘
} kvs_io_type_t;

// 2. 定义基类 (Base Class)
// 所有的 io_uring user_data 最终都指向这个结构体
typedef struct kvs_io_event_s {
    kvs_io_type_t type;
    
    // 统一的回调接口：res 是 cqe->res (读写字节数/错误码)
    void (*handler)(struct kvs_io_event_s *ev, int res);
} kvs_io_event_t;

struct io_uring;
struct kvs_conn_s;

typedef int (*kvs_on_accept_cb)(struct kvs_conn_s *conn);
typedef int (*kvs_on_msg_cb)(struct kvs_conn_s *conn);
typedef int (*kvs_on_send_cb)(struct kvs_conn_s *conn, int bytes_sent);
typedef int (*kvs_on_close_cb)(struct kvs_conn_s *conn);
// kvs_proactor_s.global_ctx is passed as the first argument to on_timer callback
typedef int (*kvs_on_timer_cb)(void *global_ctx);

struct kvs_proactor_s {
    unsigned short port;
    int server_fd;
    struct kvs_conn_s *conns;
    int conn_num;
    int conn_max;

    struct io_uring *uring;

    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;
    kvs_on_timer_cb on_timer;
    int read_buffer_size;
    int write_buffer_size;

    struct __kernel_timespec ts;

    void* global_ctx;
};  

struct kvs_conn_s {
	char* r_buffer;
	int r_buf_sz;
    int r_idx;
	char* w_buffer;
	int w_buf_sz;
    int w_idx;
    
    int raw_buf_sent_sz; // for raw buffer send tracking

	//int state;
	// struct kvs_server_s *server;

    // user data pointers
    void *global_ctx;
    void *bussiness_ctx;
	
    struct{
        int fd;
        int is_reading; // todo: avoid read/write conflict in uring
        int is_writing; 
        struct kvs_proactor_s *proactor;
        kvs_io_event_t read_ev;
        kvs_io_event_t write_ev;
        
        kvs_io_event_t accept_ev; // 专门给 accept 用 (监听 socket 只需要这一个)
    } _internal;
};





struct kvs_proactor_options_s {
    unsigned short port;
    int conn_max;

    kvs_on_accept_cb on_accept;
    kvs_on_msg_cb on_msg;
    kvs_on_send_cb on_send;
    kvs_on_close_cb on_close;
    kvs_on_timer_cb on_timer;
    int read_buffer_size;
    int write_buffer_size;

    void* global_ctx;
};

int kvs_proactor_init(struct kvs_proactor_s *proactor, struct kvs_proactor_options_s *options);
int kvs_proactor_deinit(struct kvs_proactor_s *proactor);

int kvs_proactor_start(struct kvs_proactor_s *proactor);
// int set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);
// int set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);

int kvs_proactor_set_send_event_raw_buffer(struct kvs_conn_s *conn, char *send_buf, int send_buf_sz);
int kvs_proactor_set_send_event(struct kvs_conn_s *conn, char *msg, int msg_sz);
int kvs_proactor_set_send_event_manual(struct kvs_conn_s *conn);
int kvs_proactor_set_recv_event(struct kvs_conn_s *conn);

struct kvs_conn_s *kvs_proactor_register_fd(struct kvs_proactor_s *proactor, int fd);

#endif