#include "kvs_proactor.h"
#include "logger.h"


#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <assert.h>
#include <sys/signalfd.h>


#define EVENT_ACCEPT   		0
#define EVENT_READ			1
#define EVENT_WRITE			2
#define EVENT_WRITE_BUFFER 	3
#define EVENT_TIMER			4

#define KVS_CONN_MAX_DEFAULT 32


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

static int _set_event_timeout(struct io_uring *ring, struct kvs_proactor_s *proactor, int sec, int nsec) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	proactor->ts.tv_sec = sec;
	proactor->ts.tv_nsec = nsec;

	struct conn_info accept_info = {
		.fd = -1,
		.event = EVENT_TIMER,
	};
	io_uring_prep_timeout(sqe, &proactor->ts, 0, 0);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
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

int _kvs_proactor_add_conn(struct kvs_proactor_s *proactor, int fd) {
	if(proactor == NULL || fd < 0 || fd >= proactor->conn_max) {
		return -1;
	}
	struct kvs_conn_s *conn = &proactor->conns[fd];
	memset(conn, 0, sizeof(struct kvs_conn_s));
	conn->_internal.fd = fd;
	conn->_internal.proactor = proactor;
	conn->r_buf_sz = proactor->read_buffer_size;
	conn->r_buffer = (char *)malloc(conn->r_buf_sz);
	conn->r_idx = 0;
	conn->w_buf_sz = proactor->write_buffer_size;
	conn->w_buffer = (char *)malloc(conn->w_buf_sz);
	conn->w_idx = 0;
	conn->global_ctx = proactor->global_ctx;

	proactor->conn_num = fd > proactor->conn_num ? fd + 1 : proactor->conn_num;
	return 0;
}

int _kvs_proactor_remove_conn(struct kvs_proactor_s *proactor, int fd) {
	if(proactor == NULL || fd < 0 || fd >= proactor->conn_max) {
		return -1;
	}
	struct kvs_conn_s *conn = &proactor->conns[fd];
	conn->_internal.fd = -1;
	conn->_internal.proactor = NULL;
	if(conn->r_buffer) {
		free(conn->r_buffer);
		conn->r_buffer = NULL;
	}
	if(conn->w_buffer) {
		free(conn->w_buffer);
		conn->w_buffer = NULL;
	}
	memset(conn, 0, sizeof(struct kvs_conn_s));

	proactor->conn_num = fd == proactor->conn_num - 1 ? proactor->conn_num - 1 : proactor->conn_num;
	return 0;
}

// typedef int (*msg_handler)(struct kvs_conn_s *conn);
// static msg_handler kvs_handler;

//#define CONNECT_NUM_MAX 12

// kvs_conn_t kvs_conns[CONNECT_NUM_MAX];

int kvs_proactor_init(struct kvs_proactor_s *proactor, struct kvs_proactor_options_s *options) {
	if(proactor == NULL) {
		printf("kvs_proactor_init: server is NULL\n");
		return -1;
	}
	struct io_uring_params params;
	memset(&params, 0, sizeof(params));

	struct io_uring *ring = (struct io_uring *)malloc(sizeof(struct io_uring));
	io_uring_queue_init_params(ENTRIES_LENGTH, ring, &params);
	proactor->uring = ring;

	proactor->port = options->port;
	if(options->conn_max <=0 ) {
		options->conn_max = KVS_CONN_MAX_DEFAULT;
	} else {
		proactor->conn_max = options->conn_max;
	}
	proactor->conn_num = 0;
	proactor->conns = (struct kvs_conn_s *)malloc(sizeof(struct kvs_conn_s) * proactor->conn_max);
	memset(proactor->conns, 0, sizeof(struct kvs_conn_s) * proactor->conn_max);
	proactor->on_accept = options->on_accept;
	proactor->on_msg = options->on_msg;
	proactor->on_send = options->on_send;
	proactor->on_close = options->on_close;
	proactor->on_timer = options->on_timer;
	proactor->global_ctx = options->global_ctx;
	proactor->read_buffer_size = options->read_buffer_size >0 ? options->read_buffer_size : BUFFER_SIZE_DEFAULT;
	proactor->write_buffer_size = options->write_buffer_size >0 ? options->write_buffer_size : BUFFER_SIZE_DEFAULT;

	return 0;
}

int kvs_proactor_deinit(struct kvs_proactor_s *proactor) {
	if(proactor == NULL || proactor->uring == NULL) {
		return -1;
	}
	io_uring_queue_exit(proactor->uring);
	free(proactor->uring);
	proactor->uring = NULL;
	free(proactor->conns);
	proactor->conns = NULL;
	return 0;
}

int _kvs_proactor_event_accept(struct kvs_proactor_s *proactor, struct io_uring_cqe *u_entry, struct io_uring *ring, struct sockaddr_in *clientaddr, socklen_t *len) {
	struct conn_info result;
	memcpy(&result, &u_entry->user_data, sizeof(struct conn_info));
	set_event_accept(ring, result.fd, (struct sockaddr*)clientaddr, len, 0);
	//printf("set_event_accept\n"); //
	int connfd = u_entry->res;
	
	if(connfd < 0) {
		printf("%s:%d accept error\n", __FILE__, __LINE__);
		return -1;
	}
	if(connfd >= proactor->conn_max) {
		printf("%s:%d connfd %d exceed max %d\n", __FILE__, __LINE__, connfd, proactor->conn_max);
		close(connfd);
		return -1;
	}
	_kvs_proactor_add_conn(proactor, connfd);
	proactor->on_accept(&proactor->conns[connfd]);
	_set_event_recv(ring, connfd, proactor->conns[connfd].r_buffer, proactor->conns[connfd].r_buf_sz, 0);
	proactor->conns[connfd]._internal.is_reading = 1;
	return 0;
}

int _kvs_proactor_event_read(struct kvs_proactor_s *proactor, struct io_uring_cqe *entry_u, struct kvs_conn_s *cur_conn, struct io_uring *ring) {
	struct conn_info result;
	memcpy(&result, &entry_u->user_data, sizeof(struct conn_info));
	int r_size = entry_u->res;
	cur_conn->_internal.is_reading = 0;
	if (r_size == 0) {
		if(proactor->on_close != NULL) {
			//printf("%s:%d connection closed, fd: %d\n", __FILE__, __LINE__, result.fd);
			proactor->on_close(cur_conn);
		}
		close(result.fd);
	} else if (r_size > 0) {
		int* r_buffer_len = &cur_conn->r_idx;
		char* r_buffer = cur_conn->r_buffer;
		//LOG_DEBUG("fd:%d current r_idx: %d, r_size: %d", result.fd, *r_buffer_len, r_size);


		*r_buffer_len += r_size;
		//LOG_DEBUG("fd:%d current r_idx: %d, r_size: %d", result.fd, *r_buffer_len, r_size);
		//cur_conn->r_idx += r_size;
		
		int length_resp = 0;
		// process the received data
		int length_processed = proactor->on_msg(cur_conn);
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

		if(cur_conn->_internal.is_reading == 0) {
			// set recv event again
			_set_event_recv(ring, result.fd, r_buffer + *r_buffer_len, cur_conn->r_buf_sz - *r_buffer_len, 0);
			cur_conn->_internal.is_reading = 1;
		}
	} else {
		if((-entry_u->res) == ECONNRESET)
			close(result.fd);
		else 
			printf("%s:%d recv error,%d: %s, close_fd:%d\n", __FILE__, __LINE__, entry_u->res, strerror(entry_u->res), result.fd);
		proactor->on_close(cur_conn);
		
		// error
	}
	return 0;
}

int _kvs_proactor_event_write(struct kvs_proactor_s *proactor, struct io_uring_cqe *entry_u, struct kvs_conn_s *cur_conn, struct io_uring *ring) {
	struct conn_info result;
	memcpy(&result, &entry_u->user_data, sizeof(struct conn_info));
	int ret = entry_u->res;
	cur_conn->_internal.is_writing = 0;
	assert(cur_conn->w_idx >= 0);
	if(ret < 0) {
		// todo:error handling
		
		printf("%s:%d send error, %d: %s\n", __FILE__, __LINE__, ret, strerror(-ret));
		close(result.fd);
		// assert(0);
		return -1;
	} 
	if(ret > cur_conn->w_idx) {
		// error
		printf("%s:%d send more than w_idx, ret: %d, w_idx: %d\n", __FILE__, __LINE__, ret, cur_conn->w_idx);
		printf("buffer[%.*s]\n", cur_conn->w_idx, cur_conn->w_buffer);
		assert(0);
	}

	
	if(ret < cur_conn->w_idx) {
		if(ret > 0) {

			// not send all data
			memmove(cur_conn->w_buffer, cur_conn->w_buffer + ret, cur_conn->w_idx - ret);
			cur_conn->w_idx -= ret;
			
		}
		// ret == 0, need to resend all data
		_set_event_send(ring, result.fd, cur_conn->w_buffer, cur_conn->w_idx, 0);
		cur_conn->_internal.is_writing = 1;

		
		return 0;
	} else if (ret == cur_conn->w_idx) {
		// sent all data
		cur_conn->w_idx -= ret;
		assert(cur_conn->w_idx == 0);
	}
	
	assert(cur_conn->w_idx == 0);

	//printf("_set_event_send ret: %d, %s\n", ret, buffer);
	// char* r_buffer = cur_conn->r_buffer;
	// int current_len = cur_conn->r_idx;
	proactor->on_send(cur_conn, ret);
	//_set_event_recv(ring, result.fd, r_buffer + current_len, BUFFER_LENGTH - current_len, 0);	
	return 0;
}

int _kvs_proactor_event_write_buffer(struct kvs_proactor_s *proactor, struct io_uring_cqe *entry_u, struct kvs_conn_s *cur_conn, struct io_uring *ring) {
	struct conn_info result;
	memcpy(&result, &entry_u->user_data, sizeof(struct conn_info));
	int ret = entry_u->res;
	cur_conn->_internal.is_writing = 0;
	if(ret < 0) {
		close(result.fd);
		printf("%s:%d send error\n", __FILE__, __LINE__);
		assert(0);
	};
	cur_conn->raw_buf_sent_sz = ret;
	proactor->on_send(cur_conn, ret);
	return 0;
}

int kvs_proactor_start(struct kvs_proactor_s *proactor) {
	assert(proactor->on_msg != NULL);
	assert(proactor->on_send != NULL);
	assert(proactor->on_accept != NULL);
	assert(proactor->conns != NULL);
	assert(proactor->uring != NULL);

	int sockfd = p_init_server(proactor->port);
	if(sockfd < 0) {
		printf("Failed to init server at port %d\n", proactor->port);
		return -1;
	}
	proactor->server_fd = sockfd;
	//kvs_handler = server->on_msg;

	struct io_uring *ring = proactor->uring;



	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	
	set_event_accept(ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
	_set_event_timeout(ring, proactor, 1, 0); // 1 second timeout
	

	LOG_INFO("kvs_proactor_start: server started at port %d, listenfd:%d", proactor->port, sockfd);
	while (1) {
		
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
			//LOG_DEBUG("kvs_proactor_start: event received, count: %d", count);
			count ++ ;
			struct io_uring_cqe *entry_u = cqe;
			struct conn_info result;
			memcpy(&result, &entry_u->user_data, sizeof(struct conn_info));
			struct kvs_conn_s *cur_conn = &proactor->conns[result.fd];

			switch(result.event) {
				case EVENT_ACCEPT:
					_kvs_proactor_event_accept(proactor, entry_u, ring, &clientaddr, &len);
					break;
				case EVENT_READ:
					_kvs_proactor_event_read(proactor, entry_u, cur_conn, ring);
					break;
				case EVENT_WRITE:
					_kvs_proactor_event_write(proactor, entry_u, cur_conn, ring);
					break;
				case EVENT_WRITE_BUFFER:
					_kvs_proactor_event_write_buffer(proactor, entry_u, cur_conn, ring);
					break;
				case EVENT_TIMER:
					// timer event
					if(proactor->on_timer != NULL)
						proactor->on_timer(proactor->global_ctx);
					_set_event_timeout(ring, proactor, 1, 0); // 1 second timeout
					break;
				default:
					// unknown event
					printf("%s:%d unknown event\n", __FILE__, __LINE__);
					assert(0);

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

	if(conn->_internal.is_writing == 0){	
		_set_event_send_raw_buffer(conn->_internal.proactor->uring, conn->_internal.fd, send_buf, send_buf_sz, 0);
		conn->_internal.is_writing = 1;
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
		LOG_WARN("connection write buffer overflowed");
		LOG_DEBUG("msg_sz: %d, conn->w_idx:%d, conn->w_buf_sz:%d", msg_sz, conn->w_idx, conn->w_buf_sz);
		return -2;
	}
	memcpy(conn->w_buffer + conn->w_idx, msg, msg_sz);
	
	conn->w_idx += msg_sz;
	

	if(conn->_internal.is_writing == 0){	
		_set_event_send(conn->_internal.proactor->uring, conn->_internal.fd, conn->w_buffer, conn->w_idx, 0);
		conn->_internal.is_writing = 1;
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

	if(conn->_internal.is_writing == 0){	
		_set_event_send(conn->_internal.proactor->uring, conn->_internal.fd, conn->w_buffer, conn->w_idx, 0);
		conn->_internal.is_writing = 1;
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
		LOG_WARN("connection recv buffer overflowed, fd:%d", conn->_internal.fd);
		return -2;
	}
	if(conn->_internal.is_reading) {
		//printf("%s:%d recv event already set, fd:%d\n", __FILE__, __LINE__,  conn->fd);
		// already reading
		return 0;
	}
	conn->_internal.is_reading = 1;
	_set_event_recv(conn->_internal.proactor->uring, conn->_internal.fd, conn->r_buffer + conn->r_idx, conn->r_buf_sz - conn->r_idx, 0);
	return 0;
}

struct kvs_conn_s *kvs_proactor_register_fd(struct kvs_proactor_s *proactor, int fd) {
	_kvs_proactor_add_conn(proactor, fd);
	return &proactor->conns[fd];
}