


#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "kvstore.h"

/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * @return : length of response
 */

int kvs_protocol(char *msg, int length, char *response) {

	printf("recv %d : %s\n", length, msg);

	memcpy(response, msg, length);

	return strlen(response);
}



int main(int argc, char *argv[]) {

	if (argc != 2) return -1;

	int port = atoi(argv[1]);
	
#if (NETWORK_SELECT == NETWORK_REACTOR)
	reactor_start(port, kvs_protocol);  //
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
	ntyco_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
	proactor_start(port, kvs_protocol);
#endif
}


