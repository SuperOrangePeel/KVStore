


#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>


#define EVENT_ACCEPT   	0
#define EVENT_READ		1
#define EVENT_WRITE		2

#define KVS_CONNS_INST(fd) (fd - 3)

extern int kvs_protocol(char *msg, int length, char *response, int rsp_buf_len, int* length_r);



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



#define ENTRIES_LENGTH		1024
#define BUFFER_LENGTH		1048576 // 1024 * 1024

int set_event_recv(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_READ,
	};
	
	io_uring_prep_recv(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


int set_event_send(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags) {

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


typedef int (*msg_handler)(char *msg, int length, char *response, int rsp_buf_len, int* length_r);
static msg_handler kvs_handler;

#define CONNECT_NUM_MAX 5

typedef struct kvs_conn_s {
	int fd;
	char r_buffer[BUFFER_LENGTH];
	// int r_idx;
	int r_total;
	char response[BUFFER_LENGTH];
} kvs_conn_t;

kvs_conn_t kvs_conns[CONNECT_NUM_MAX];

void kvs_init_conn(int connfd) {
	kvs_conns[KVS_CONNS_INST(connfd)].fd = connfd;
	char *r_buffer = kvs_conns[KVS_CONNS_INST(connfd)].r_buffer;
	char *w_buffer = kvs_conns[KVS_CONNS_INST(connfd)].response;
	memset(r_buffer, 0, BUFFER_LENGTH);
	memset(w_buffer, 0, BUFFER_LENGTH);
	//kvs_conns[KVS_CONNS_INST(connfd)].r_idx = 0;
	kvs_conns[KVS_CONNS_INST(connfd)].r_total = 0;
}




int proactor_start(unsigned short port, msg_handler handler) {

	int sockfd = p_init_server(port);
	kvs_handler = handler;

	struct io_uring_params params;
	memset(&params, 0, sizeof(params));

	struct io_uring ring;
	io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);

	
#if 0
	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	accept(sockfd, (struct sockaddr*)&clientaddr, &len);
#else

	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	
	set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
	
#endif

	//char buffer[BUFFER_LENGTH] = {0};
	//char response[BUFFER_LENGTH] = {0};


	while (1) {

		// io_uring_submit(&ring);


		// struct io_uring_cqe *cqe;
		// io_uring_wait_cqe(&ring, &cqe);

		// struct io_uring_cqe *cqes[128];
		// int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);  // epoll_wait

		// int i = 0;
		// for (i = 0;i < nready;i ++) 
		
		struct io_uring_cqe *cqe;
		unsigned head;
		int count = 0;

		// 1. 核心武器：提交并等待
		// 这个函数会把之前所有 set_event 产生的 SQE 一次性提交
		// 并且至少等待 1 个事件完成才返回
		int ret = io_uring_submit_and_wait(&ring, 1);
		if (ret < 0) {
			if (errno == EINTR) continue;
			break;
		}

		// 2. 遍历所有已完成的事件
		// 使用 io_uring 提供的标准宏，既安全又高效
		io_uring_for_each_cqe(&ring, head, cqe) {
			count ++ ;
			struct io_uring_cqe *entries = cqe;
			struct conn_info result;
			memcpy(&result, &entries->user_data, sizeof(struct conn_info));

			if (result.event == EVENT_ACCEPT) {

				set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
				//printf("set_event_accept\n"); //

				int connfd = entries->res;
				
				kvs_init_conn(connfd);
				
				char* r_buffer = kvs_conns[KVS_CONNS_INST(connfd)].r_buffer;
				set_event_recv(&ring, connfd, r_buffer, BUFFER_LENGTH, 0);

				
			} else if (result.event == EVENT_READ) {  //

				int r_size = entries->res;
				if (r_size == 0) {
					close(result.fd);
				} else if (r_size > 0) {
					int* r_buffer_len = &kvs_conns[KVS_CONNS_INST(result.fd)].r_total;
					char* r_buffer = kvs_conns[KVS_CONNS_INST(result.fd)].r_buffer;
					char* w_buffer = kvs_conns[KVS_CONNS_INST(result.fd)].response;
					*r_buffer_len += r_size;
					//int kvs_protocol(char *msg, int length, char *response);

					int length_resp = 0;
					// process the received data
					int length_processed = kvs_handler(r_buffer, *r_buffer_len, w_buffer, BUFFER_LENGTH, &length_resp);
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
						printf("unprocessed data: [%.*s]\n", *r_buffer_len, r_buffer + length_processed);
						memmove(r_buffer, r_buffer + length_processed, *r_buffer_len);
					}

					set_event_send(&ring, result.fd, w_buffer, length_resp, 0);
				} else {
					// error
				}
			}  else if (result.event == EVENT_WRITE) {
  //

				int ret = entries->res;

				//printf("set_event_send ret: %d, %s\n", ret, buffer);
				char* r_buffer = kvs_conns[KVS_CONNS_INST(result.fd)].r_buffer;
				int current_len = kvs_conns[KVS_CONNS_INST(result.fd)].r_total;
				set_event_recv(&ring, result.fd, r_buffer + current_len, BUFFER_LENGTH - current_len, 0);
				
			}
			
		}

		io_uring_cq_advance(&ring, count);
	}

}


