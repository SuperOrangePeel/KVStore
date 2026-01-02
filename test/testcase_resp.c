

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>


#define MAX_MSG_LENGTH		1024
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int send_msg(int connfd, char *msg, int length) {

	int res = send(connfd, msg, length, 0);
	if (res < 0) {
		perror("send");
		exit(1);
	}
	return res;
}

int recv_msg(int connfd, char *msg, int length) {

	int res = recv(connfd, msg, length, 0);
	if (res < 0) {
		perror("recv");
		exit(1);
	}
	return res;

}


int format_hset(char *buf, int i) {
    char key[32], val[64];
    sprintf(key, "key:%07d", i);
    sprintf(val, "value_content_%07d", i);
    return sprintf(buf, "*3\r\n$4\r\nHSET\r\n$%ld\r\n%s\r\n$%ld\r\n%s\r\n", 
                   strlen(key), key, strlen(val), val);
}

int format_hget(char *buf, int i) {
	char key[32];
	sprintf(key, "key:%07d", i);
	return sprintf(buf, "*2\r\n$4\r\nHGET\r\n$%ld\r\n%s\r\n", 
				   strlen(key), key);
}

int format_hexist(char *buf, int i) {
	char key[32];
	sprintf(key, "key:%07d", i);
	return sprintf(buf, "*2\r\n$6\r\nHEXIST\r\n$%ld\r\n%s\r\n", 
				   strlen(key), key);
}

int format_hmod(char *buf, int i) {
	char key[32], val[64];
	sprintf(key, "key:%07d", i);
	sprintf(val, "modified_value_%07d", i);
	return sprintf(buf, "*3\r\n$4\r\nHMOD\r\n$%ld\r\n%s\r\n$%ld\r\n%s\r\n", 
				   strlen(key), key, strlen(val), val);
}

int format_hdel(char *buf, int i) {
	char key[32];
	sprintf(key, "key:%07d", i);
	return sprintf(buf, "*2\r\n$4\r\nHDEL\r\n$%ld\r\n%s\r\n", 
				   strlen(key), key);
}


void testcase(int connfd, char *msg, char *pattern, char *casename) {

	if (!msg || !pattern || !casename) return ;

	send_msg(connfd, msg, strlen(msg));

	char result[MAX_MSG_LENGTH] = {0};
	recv_msg(connfd, result, MAX_MSG_LENGTH);

	if (strcmp(result, pattern) == 0) {
		//printf("==> PASS ->  %s\n", casename);
	} else {
		printf("==> FAILED -> %s, '%s' != '%s' \n", casename, result, pattern);
		exit(1);
	}

}



int connect_tcpserver(const char *ip, unsigned short port) {

	int connfd = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof(struct sockaddr_in));

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = inet_addr(ip);
	server_addr.sin_port = htons(port);

	if (0 !=  connect(connfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_in))) {
		perror("connect");
		return -1;
	}
	
	return connfd;
	
}


void hash_testcase(int connfd) {
	char* msg_buf = (char*)malloc(512);
	char* resp_buf = (char*)malloc(512);

	int count = 50000;
	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);
	int i = 0;
	// HSET test
	for (i = 0;i < count;i ++) {
		int msg_len = format_hset(msg_buf, i);
		testcase(connfd, msg_buf, "+OK\r\n", "HSET-Test");
	}
	// HGET test
	for (i = 0;i < count;i ++) {
		int msg_len = format_hget(msg_buf, i);
		int val_len = sprintf(resp_buf, "$%ld\r\nvalue_content_%07d\r\n", strlen("value_content_XXXXXXX"), i);
		testcase(connfd, msg_buf, resp_buf, "HGET-Test");
	}
	// HEXIST test
	for (i = 0;i < count;i ++) {
		int msg_len = format_hexist(msg_buf, i);
		testcase(connfd, msg_buf, "+EXIST\r\n", "HEXIST-Test");
	}
	// HMOD test
	for (i = 0;i < count;i ++) {
		int msg_len = format_hmod(msg_buf, i);
		testcase(connfd, msg_buf, "+OK\r\n", "HMOD-Test");
	}
	// HDEL test
	for (i = 0;i < count;i ++) {
		int msg_len = format_hdel(msg_buf, i);
		testcase(connfd, msg_buf, "+OK\r\n", "HDEL-Test");
	}
	struct timeval tv_end;
	gettimeofday(&tv_end, NULL);
	int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms
	printf("hash testcase --> time_used: %d, qps: %d\n", time_used, 50000 * 1000 / time_used);

	free(msg_buf);
	free(resp_buf);
}

//har *msg_set = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nJack Ma\r\n";
// char *msg_get = "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n";
void array_test_set_get_a_blog_test(int connfd) {
	char* file_name = "4. 设计模式.md";
	int blg_fd = open(file_name, O_RDONLY);
	
	struct stat blg_stat;
	fstat(blg_fd, &blg_stat);
	int blg_sz = blg_stat.st_size;

	//send cmd
	const char *set_cmd = "*3\r\n$3\r\nSET\r\n";
	int s_size = send(connfd, set_cmd, (int)strlen(set_cmd), 0);

	//send key
	char send_buf[256];
	memset(send_buf, 0, 256);
	int m_sz = sprintf(send_buf, "$%d\r\n%s\r\n", (int)strlen(file_name), file_name);
	send(connfd, send_buf, m_sz, 0);

	//send val
	
	memset(send_buf, 0, 256);
	m_sz = sprintf(send_buf, "$%d\r\n", blg_sz);
	send(connfd, send_buf, m_sz, 0);
	int sf_ret = sendfile(connfd, blg_fd, NULL, blg_sz);
	if(sf_ret == -1) {
		printf("sendfile failed: %s\n", strerror(errno));
		return;
	}
	printf("sendfile: %d\n", sf_ret);
	send(connfd, "\r\n", 2, 0);

	
	// send get command
	m_sz = sprintf(send_buf, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", (int)strlen(file_name), file_name);

	send(connfd, send_buf, m_sz, 0);

	
	char recv_buf[2048];
	memset(recv_buf, 0, 2048);
	int r_size = recv(connfd, recv_buf, 2048, 0);
	printf("received blog:[%*s]\n", r_size, recv_buf);

	
	
}



// testcase 192.168.243.131  2000
int main(int argc, char *argv[]) {

	if (argc != 3) {
		printf("arg error\n");
		return -1;
	}

	char *ip = argv[1];
	int port = atoi(argv[2]);
	//int mode = atoi(argv[3]);

	int connfd = connect_tcpserver(ip, port);
    
    // char *msg_set = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nJack Ma\r\n";
    // //char *msg_set = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nJack Ma\r\n*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$9\r\nJack Ma\r\n\r\n";
    // char *msg_get = "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n";
    
    // testcase(connfd, msg_set, "+OK\r\n", "resp_set");
    // testcase(connfd, msg_get, "$7\r\nJack Ma\r\n", "resp_get");

    // char *msg_set_get = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nJack Ma\r\n*2\r\n$3\r\nGET\r\n$4\r\nname\r\n";
    
	// testcase(connfd, msg_set_get, "+EXIST\r\n$7\r\nJack Ma\r\n", "msg_set_get");

	// char *msg_del = "*2\r\n$3\r\nDEL\r\n$4\r\nname\r\n";

	// testcase(connfd, msg_del, "+OK\r\n", "msg_set_get");

	//array_test_set_get_a_blog_test(connfd);
	hash_testcase(connfd);

	//sendfile();

	// if (mode == 0) {
	// 	rbtree_testcase_1w(connfd);
	// } else if (mode == 1) {
	// 	rbtree_testcase_3w(connfd);
	// } else if (mode == 2) {
	// 	array_testcase_1w(connfd);
	// } else if (mode == 3) {
	// 	hash_testcase(connfd);
	// }

	return 0;
	
}



