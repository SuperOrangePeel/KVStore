#include "kvstore.h"
#include "kvs_server.h"
#include "proactor.h"

extern kvs_server_t *global_server;

#include <fcntl.h>

int main(int argc, char *argv[]) {

	if (argc != 2) return -1;

	int port = atoi(argv[1]);

	global_server = kvs_server_init(port, KVS_SERVER_ROLE_MASTER);

	
	
#if (NETWORK_SELECT == NETWORK_REACTOR)
	reactor_start(port, kvs_protocol);  //
#elif (NETWORK_SELECT == NETWORK_NTYCO)
	ntyco_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
	proactor_start(global_server);
#endif

	kvs_server_destroy(global_server);

	return 0;

}


