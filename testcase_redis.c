

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




void testcase(int connfd, char *msg, char *pattern, char *casename) {

	if (!msg || !pattern || !casename) return ;

	send_msg(connfd, msg, strlen(msg));

	char result[MAX_MSG_LENGTH] = {0};
	recv_msg(connfd, result, MAX_MSG_LENGTH);

	if (strcmp(result, pattern) == 0) {
		printf("==> PASS ->  %s\n", casename);
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


void array_testcase(int connfd) {

	testcase(connfd, "SET Teacher King", "OK\r\n", "SET-Teacher");
	testcase(connfd, "GET Teacher", "King\r\n", "GET-Teacher");
	testcase(connfd, "MOD Teacher Darren", "OK\r\n", "MOD-Teacher");
	testcase(connfd, "GET Teacher", "Darren\r\n", "GET-Teacher");
	testcase(connfd, "EXIST Teacher", "EXIST\r\n", "GET-Teacher");
	testcase(connfd, "DEL Teacher", "OK\r\n", "DEL-Teacher");
	testcase(connfd, "GET Teacher", "NO EXIST\r\n", "GET-Teacher");
	testcase(connfd, "MOD Teacher KING", "NO EXIST\r\n", "MOD-Teacher");
	testcase(connfd, "EXIST Teacher", "NO EXIST\r\n", "GET-Teacher");

}

void array_testcase_1w(int connfd) {

	int count = 10000;
	int i = 0;

	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	for (i = 0;i < count;i ++) {

		testcase(connfd, "SET Teacher King", "OK\r\n", "SET-Teacher");
		testcase(connfd, "GET Teacher", "King\r\n", "GET-Teacher");
		testcase(connfd, "MOD Teacher Darren", "OK\r\n", "MOD-Teacher");
		testcase(connfd, "GET Teacher", "Darren\r\n", "GET-Teacher");
		testcase(connfd, "EXIST Teacher", "EXIST\r\n", "GET-Teacher");
		testcase(connfd, "DEL Teacher", "OK\r\n", "DEL-Teacher");
		testcase(connfd, "GET Teacher", "NO EXIST\r\n", "GET-Teacher");
		testcase(connfd, "MOD Teacher KING", "NO EXIST\r\n", "MOD-Teacher");
		testcase(connfd, "EXIST Teacher", "NO EXIST\r\n", "GET-Teacher");

	}

	struct timeval tv_end;
	gettimeofday(&tv_end, NULL);

	int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

	printf("array testcase --> time_used: %d, qps: %d\n", time_used, 90000 * 1000 / time_used);

}


void rbtree_testcase(int connfd) {

	testcase(connfd, "RSET Teacher King", "OK\r\n", "RSET-Teacher");
	testcase(connfd, "RGET Teacher", "King\r\n", "RGET-King-Teacher");
	testcase(connfd, "RMOD Teacher Darren", "OK\r\n", "RMOD-D-Teacher");
	testcase(connfd, "RGET Teacher", "Darren\r\n", "RGET-Darren-Teacher");
	testcase(connfd, "REXIST Teacher", "EXIST\r\n", "REXIST-Teacher");
	testcase(connfd, "RDEL Teacher", "OK\r\n", "RDEL-Teacher");
	testcase(connfd, "RGET Teacher", "NO EXIST\r\n", "RGET-K-Teacher");
	testcase(connfd, "RMOD Teacher KING", "NO EXIST\r\n", "RMOD-K-Teacher");
	testcase(connfd, "REXIST Teacher", "NO EXIST\r\n", "REXIST-Teacher");

}

void rbtree_testcase_1w(int connfd) {

	int count = 10000;
	int i = 0;

	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	for (i = 0;i < count;i ++) {

		testcase(connfd, "RSET Teacher King", "OK\r\n", "RSET-Teacher");
		testcase(connfd, "RGET Teacher", "King\r\n", "RGET-King-Teacher");
		testcase(connfd, "RMOD Teacher Darren", "OK\r\n", "RMOD-D-Teacher");
		testcase(connfd, "RGET Teacher", "Darren\r\n", "RGET-Darren-Teacher");
		testcase(connfd, "REXIST Teacher", "EXIST\r\n", "REXIST-Teacher");
		testcase(connfd, "RDEL Teacher", "OK\r\n", "RDEL-Teacher");
		testcase(connfd, "RGET Teacher", "NO EXIST\r\n", "RGET-K-Teacher");
		testcase(connfd, "RMOD Teacher KING", "NO EXIST\r\n", "RMOD-K-Teacher");
		testcase(connfd, "REXIST Teacher", "NO EXIST\r\n", "REXIST-Teacher");

	}

	struct timeval tv_end;
	gettimeofday(&tv_end, NULL);

	int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

	printf("rbtree testcase --> time_used: %d, qps: %d\n", time_used, 90000 * 1000 / time_used);
	
}


void rbtree_testcase_3w(int connfd) {

	int count = 10000;
	int i = 0;

	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	for (i = 0;i < count;i ++) {

		char cmd[128] = {0};
		snprintf(cmd, 128, "RSET Teacher%d King%d", i, i);
		testcase(connfd, cmd, "OK\r\n", "RSET-Teacher");
	}

	for (i = 0;i < count;i ++) {

		char cmd[128] = {0};
		snprintf(cmd, 128, "RGET Teacher%d", i);

		char result[128] = {0};
		snprintf(result, 128, "King%d\r\n", i);
		
		testcase(connfd, cmd, result, "RGET-King-Teacher");
	}

	for (i = 0;i < count;i ++) {

		char cmd[128] = {0};
		snprintf(cmd, 128, "RMOD Teacher%d King%d", i, i);
		testcase(connfd, cmd, "OK\r\n", "RGET-King-Teacher");
	}

	struct timeval tv_end;
	gettimeofday(&tv_end, NULL);

	int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

	printf("rbtree testcase --> time_used: %d, qps: %d\n", time_used, 30000 * 1000 / time_used);

}

void hash_testcase(int connfd) {

	testcase(connfd, "HSET Teacher King", "OK\r\n", "HSET-Teacher");
	testcase(connfd, "HGET Teacher", "King\r\n", "HGET-King-Teacher");
	testcase(connfd, "HMOD Teacher Darren", "OK\r\n", "HMOD-D-Teacher");
	testcase(connfd, "HGET Teacher", "Darren\r\n", "HGET-Darren-Teacher");
	testcase(connfd, "HEXIST Teacher", "EXIST\r\n", "HEXIST-Teacher");
	testcase(connfd, "HDEL Teacher", "OK\r\n", "HDEL-Teacher");
	testcase(connfd, "HGET Teacher", "NO EXIST\r\n", "HGET-K-Teacher");
	testcase(connfd, "HMOD Teacher KING", "NO EXIST\r\n", "HMOD-K-Teacher");
	testcase(connfd, "HEXIST Teacher", "NO EXIST\r\n", "HEXIST-Teacher");

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

	array_test_set_get_a_blog_test(connfd);


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



