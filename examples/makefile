all:
	gcc -O2 -Wall -Wextra -o file_generator file_generator.c
	gcc -O2 -g -Wall -Wextra -o rdma_server rdma_server.c -lrdmacm -libverbs
	gcc -O2 -g -Wall -Wextra -o rdma_client rdma_client.c -lrdmacm -libverbs
	gcc -O2 -g -Wall -Wextra -o sendfile_server sendfile_server.c
	gcc -O2 -g -Wall -Wextra -o sendfile_client sendfile_client.c

clean:
	rm -f file_generator 
	rm -f rdma_server rdma_client
	rm -f sendfile_server sendfile_client
