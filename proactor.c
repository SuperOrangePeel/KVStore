#include "proactor.h"
#include "kvs_server.h"


#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <assert.h>


#define EVENT_ACCEPT   		0
#define EVENT_READ			1
#define EVENT_WRITE			2
#define EVENT_WRITE_BUFFER 	3

// #define KVS_CONNS_INST(fd) (fd - 3)

struct conn_info {
	int fd;
	int event;
};


int p_init_server(unsigned short port) {	

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);	

	int reuse = 1;  // 非 0 值表示启用该选项，0 表示禁用
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(sockfd);
        return -1;
    }

	struct sockaddr_in serveraddr;	
	memset(&serveraddr, 0, sizeof(struct sockaddr_in));	
	serveraddr.sin_family = AF_INET;	
	serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);	
	serveraddr.sin_port = htons(port);	


	if (-1 == bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(struct sockaddr))) {		
		perror("bind");		
		return -1;	
	}	

	listen(sockfd, 10);
	
	return sockfd;
}


static int _set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_READ,
	};
	
	io_uring_prep_recv(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


static int _set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_WRITE,
	};
	
	io_uring_prep_send(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}

static int _set_event_send_raw_buffer(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_WRITE_BUFFER,
	};
	
	io_uring_prep_send(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
}



int set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr,
					socklen_t *addrlen, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_ACCEPT,
	};
	
	io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


// typedef int (*msg_handler)(struct kvs_conn_s *conn);
// static msg_handler kvs_handler;

//#define CONNECT_NUM_MAX 12

// kvs_conn_t kvs_conns[CONNECT_NUM_MAX];

int kvs_proactor_init(struct kvs_server_s *server) {
	if(server == NULL) {
		printf("kvs_proactor_init: server is NULL\n");
		return -1;
	}
	struct io_uring_params params;
	memset(&params, 0, sizeof(params));

	struct io_uring *ring = (struct io_uring *)malloc(sizeof(struct io_uring));
	io_uring_queue_init_params(ENTRIES_LENGTH, ring, &params);
	server->uring = ring;

	return 0;
}

int kvs_proactor_destroy(struct kvs_server_s *server) {
	if(server == NULL || server->uring == NULL) {
		return -1;
	}
	io_uring_queue_exit(server->uring);
	free(server->uring);
	server->uring = NULL;
	return 0;
}


int kvs_proactor_start(struct kvs_server_s *server) {
	assert(server->on_msg != NULL);
	assert(server->on_send != NULL);
	assert(server->on_accept != NULL);
	assert(server->conns != NULL);
	assert(server->uring != NULL);

	int sockfd = p_init_server(server->port);
	if(sockfd < 0) {
		printf("Failed to init server at port %d\n", server->port);
		return -1;
	}
	server->server_fd = sockfd;
	//kvs_handler = server->on_msg;

	struct io_uring *ring = server->uring;

	
#if 0
	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	accept(sockfd, (struct sockaddr*)&clientaddr, &len);
#else

	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	
	set_event_accept(ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
	
#endif

	//char buffer[BUFFER_LENGTH] = {0};
	//char response[BUFFER_LENGTH] = {0};

	printf("Server started at port %d\n", server->port);
	while (1) {

		// io_uring_submit(ring);


		// struct io_uring_cqe *cqe;
		// io_uring_wait_cqe(ring, &cqe);

		// struct io_uring_cqe *cqes[128];
		// int nready = io_uring_peek_batch_cqe(ring, cqes, 128);  // epoll_wait

		// int i = 0;
		// for (i = 0;i < nready;i ++) 
		
		struct io_uring_cqe *cqe;
		unsigned head;
		int count = 0;


		int ret = io_uring_submit_and_wait(ring, 1);
		if (ret < 0) {
			if (errno == EINTR) continue;
			else {
				perror("io_uring_submit_and_wait");
				return -1;
			}
		}

		// go through all completed events using the macro provided by liburing
		io_uring_for_each_cqe(ring, head, cqe) {
			count ++ ;
			struct io_uring_cqe *entries = cqe;
			struct conn_info result;
			memcpy(&result, &entries->user_data, sizeof(struct conn_info));
			struct kvs_conn_s *cur_conn = &server->conns[result.fd];

			if (result.event == EVENT_ACCEPT) {

				set_event_accept(ring, result.fd, (struct sockaddr*)&clientaddr, &len, 0);
				//printf("set_event_accept\n"); //

				int connfd = entries->res;
				if(connfd < 0) {
					printf("%s:%d accept error\n", __FILE__, __LINE__);
					continue;
				}
				server->on_accept(server, connfd);
				
			} else if (result.event == EVENT_READ) {  
				int r_size = entries->res;
				cur_conn->is_reading = 0;
				if (r_size == 0) {
					if(server->on_close != NULL) {
						//printf("%s:%d connection closed, fd: %d\n", __FILE__, __LINE__, result.fd);
						server->on_close(cur_conn);
					}
					close(result.fd);
				} else if (r_size > 0) {
					int* r_buffer_len = &cur_conn->r_idx;
					char* r_buffer = cur_conn->r_buffer;


					*r_buffer_len += r_size;
					//cur_conn->r_idx += r_size;
					
					int length_resp = 0;
					// process the received data
					int length_processed = server->on_msg(cur_conn);
					assert(length_processed >= 0);
					// move the unprocessed data to the beginning of the read buffer
					if(length_processed > *r_buffer_len) {
						//error
						printf("%s:%d error\n", __FILE__, __LINE__);
						assert(0);
					} else if(length_processed == *r_buffer_len) {
						*r_buffer_len = 0;
					} else {
						*r_buffer_len -= length_processed;
						memmove(r_buffer, r_buffer + length_processed, *r_buffer_len);
					}

					if(cur_conn->is_reading == 0) {
						// set recv event again
						_set_event_recv(ring, result.fd, r_buffer + *r_buffer_len, cur_conn->r_buf_sz - *r_buffer_len, 0);
						cur_conn->is_reading = 1;
					}
				} else {
					// todo: error handling
					printf("%s:%d recv error,%d: %s, close_fd:%d\n", __FILE__, __LINE__, entries->res, strerror(entries->res), result.fd);
					server->on_close(cur_conn);
					close(result.fd);
					// error
				}
			}  else if (result.event == EVENT_WRITE) {
  //

				int ret = entries->res;
				cur_conn->is_writing = 0;
				assert(cur_conn->w_idx >= 0);
				if(ret < 0) {
					// todo:error handling
					
					printf("%s:%d send error\n", __FILE__, __LINE__);
					close(result.fd);
					assert(0);
					continue;
				} 
				if(ret > cur_conn->w_idx) {
					// error
					printf("%s:%d send more than w_idx, ret: %d, w_idx: %d\n", __FILE__, __LINE__, ret, cur_conn->w_idx);
					printf("buffer[%.*s]\n", cur_conn->w_idx, cur_conn->response);
					assert(0);
				}

				
				if(ret < cur_conn->w_idx) {
					if(ret > 0) {

						// not send all data
						memmove(cur_conn->response, cur_conn->response + ret, cur_conn->w_idx - ret);
						cur_conn->w_idx -= ret;
						
					}
					// ret == 0, need to resend all data
					_set_event_send(ring, result.fd, cur_conn->response, cur_conn->w_idx, 0);
					cur_conn->is_writing = 1;

					
					continue;
				} else if (ret == cur_conn->w_idx) {
					// sent all data
					cur_conn->w_idx -= ret;
					assert(cur_conn->w_idx == 0);
				}
				
				assert(cur_conn->w_idx == 0);

				//printf("_set_event_send ret: %d, %s\n", ret, buffer);
				// char* r_buffer = cur_conn->r_buffer;
				// int current_len = cur_conn->r_idx;
				server->on_send(cur_conn, ret);
				//_set_event_recv(ring, result.fd, r_buffer + current_len, BUFFER_LENGTH - current_len, 0);
				
			} else if(result.event == EVENT_WRITE_BUFFER) {
				int ret = entries->res;
				cur_conn->is_writing = 0;
				if(ret < 0) {
					close(result.fd);
					printf("%s:%d send error\n", __FILE__, __LINE__);
					assert(0);
				}

				server->on_send(cur_conn, ret);
			}
			
		}
		// advance the completion queue to mark these events as processed
		io_uring_cq_advance(ring, count);
	}
	return 0;
}


int kvs_proactor_set_send_event_raw_buffer(struct kvs_conn_s *conn, char *send_buf, int send_buf_sz) {
	if(conn == NULL || send_buf == NULL || send_buf_sz <=0) {
		return -1;
	}

	if(conn->is_writing == 0){	
		_set_event_send_raw_buffer(conn->server->uring, conn->fd, send_buf, send_buf_sz, 0);
		conn->is_writing = 1;
	}
	return 0;
}



/*
 * @return 0 if success, -1 if error, -2 if overflow
 */
int kvs_proactor_set_send_event(struct kvs_conn_s *conn, char *msg, int msg_sz) {
	if(conn == NULL || msg == NULL || msg_sz <=0) {
		return -1;
	}
	if(msg_sz + conn->w_idx > conn->w_buf_sz) {
		// overflow
		printf("%s:%d response buffer overflow\n", __FILE__, __LINE__);
		return -2;
	}
	memcpy(conn->response + conn->w_idx, msg, msg_sz);
	
	conn->w_idx += msg_sz;
	

	if(conn->is_writing == 0){	
		_set_event_send(conn->server->uring, conn->fd, conn->response, conn->w_idx, 0);
		conn->is_writing = 1;
	}
	return 0;
}

int kvs_proactor_set_send_event_manual(struct kvs_conn_s *conn) {
	if(conn == NULL) {
		return -1;
	}
	if(conn->w_idx <=0) {
		// nothing to send
		return 0;
	}

	if(conn->is_writing == 0){	
		_set_event_send(conn->server->uring, conn->fd, conn->response, conn->w_idx, 0);
		conn->is_writing = 1;
	}
	return 0;
}

/*
 * @return 0 if success, -1 if error -2 if overflow
 */
int kvs_proactor_set_recv_event(struct kvs_conn_s *conn) {
	if(conn == NULL ) {
		return -1;
	}
	if(conn->r_buffer == NULL || conn->r_buf_sz <=0) {
		return -1;
	}
	if(conn->r_idx >= conn->r_buf_sz) {
		// overflow
		printf("%s:%d recv buffer overflow, fd:%d\n", __FILE__, __LINE__, conn->fd);
		printf("BUFFER: [%.*s]", conn->r_idx, conn->r_buffer);
		return -2;
	}
	if(conn->is_reading) {
		//printf("%s:%d recv event already set, fd:%d\n", __FILE__, __LINE__,  conn->fd);
		// already reading
		return 0;
	}
	conn->is_reading = 1;
	_set_event_recv(conn->server->uring, conn->fd, conn->r_buffer + conn->r_idx, conn->r_buf_sz - conn->r_idx, 0);
	return 0;
}