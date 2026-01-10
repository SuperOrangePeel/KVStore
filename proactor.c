#include "proactor.h"
#include "kvs_server.h"


#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>


#define EVENT_ACCEPT   	0
#define EVENT_READ		1
#define EVENT_WRITE		2

// #define KVS_CONNS_INST(fd) (fd - 3)

struct conn_info {
	int fd;
	int event;
};


int p_init_server(unsigned short port) {	

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);	
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


int set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_READ,
	};
	
	io_uring_prep_recv(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


int set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_WRITE,
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
				// todo: on_accept callback
				// struct kvs_conn_s *init_conn = &server->conns[connfd];
				// kvs_init_conn(init_conn, connfd, server);
				// printf("Accepted a new connection: %d\n", connfd); //
				// set_event_recv(ring, connfd, init_conn->r_buffer, init_conn->r_buf_sz, 0);
				server->on_accept(server, connfd);
				
			} else if (result.event == EVENT_READ) {  //
				//printf("EVENT_READ fd: %d\n", result.fd);
				int r_size = entries->res;
				if (r_size == 0) {
					// if(server->on_close) {
					// 	server->on_close(cur_conn);
					// }
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
					} else if(length_processed == *r_buffer_len) {
						*r_buffer_len = 0;
					} else {
						*r_buffer_len -= length_processed;
						//fwrite(node->val, 1, node->len_val, stdout);
						//fflush(stdout);
						//printf("unprocessed data size: %d, buffer size: %d\n", *r_buffer_len, cur_conn->r_buf_sz);
						memmove(r_buffer, r_buffer + length_processed, *r_buffer_len);
					}

					// todo: set recv again
					// int remain = cur_conn->r_buf_sz - cur_conn->r_idx;
					// if (remain > 0) {
					// 	// 如果是特殊的“发RDB”状态，可能暂时不读，这里可以加个简单判断
					// 	// if (conn->state != SEND_RDB) 
					// 	set_event_recv(server->uring, cur_conn->fd, cur_conn->r_buffer + cur_conn->r_idx, remain, 0);
					// } else {
					// 	// 满了！这是网络层要处理的流控 (Backpressure)
					// 	printf("Buffer full, pause reading.\n");
					// }

					//set_event_send(ring, result.fd, w_buffer, length_resp, 0);
				} else {
					// error
				}
			}  else if (result.event == EVENT_WRITE) {
  //

				int ret = entries->res;
				if(ret < 0) {
					// todo:error handling
					close(result.fd);
					continue;
				} 

				if(ret < cur_conn->w_idx) {
					// not send all data
					//memmove(cur_conn->response, cur_conn->response + ret, cur_conn->w_idx - ret);
					
					set_event_send(ring, result.fd, cur_conn->response + ret, cur_conn->w_idx - ret, 0);
					cur_conn->w_idx -= ret;
					continue;
				} else if (ret == cur_conn->w_idx) {
					// sent all data
					cur_conn->w_idx = 0;
				} else {
					// error
					printf("%s:%d error in send\n", __FILE__, __LINE__);
					assert(0);
				}
				
				assert(cur_conn->w_idx == 0);

				//printf("set_event_send ret: %d, %s\n", ret, buffer);
				// char* r_buffer = cur_conn->r_buffer;
				// int current_len = cur_conn->r_idx;
				server->on_send(cur_conn);
				//set_event_recv(ring, result.fd, r_buffer + current_len, BUFFER_LENGTH - current_len, 0);
				
			}
			
		}
		// advance the completion queue to mark these events as processed
		io_uring_cq_advance(ring, count);
	}
	return 0;
}


