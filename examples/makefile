all:
	gcc -O2 -Wall -Wextra -o file_generator file_generator.c
	gcc -O2 -g -Wall -Wextra -o rdma_server rdma_server.c -lrdmacm -libverbs
	gcc -O2 -g -Wall -Wextra -o rdma_client rdma_client.c -lrdmacm -libverbs
	gcc -O2 -g -Wall -Wextra -o sendfile_server sendfile_server.c
	gcc -O2 -g -Wall -Wextra -o sendfile_client sendfile_client.c
	gcc -O2 -g -Wall -Wextra -o redis_echo_epoll redis_echo_epoll.c
	gcc -O2 -g -Wall -Wextra -o redis_echo_iouring redis_echo_iouring.c -luring

clean:
	rm -f file_generator 
	rm -f rdma_server rdma_client
	rm -f sendfile_server sendfile_client
	rm -f redis_echo_epoll redis_echo_iouring
