#include "kvstore.h"
#include "kvs_server.h"
#include "kvs_slave.h"
#include "proactor.h"
#include "kvs_handler.h"

extern struct kvs_server_s *global_server;

#include <fcntl.h>

int main(int argc, char *argv[]) {

	if (argc < 2) return -1;

	int port = atoi(argv[1]);
	

	global_server = kvs_server_init(port, kvs_handler_on_accept, 
		kvs_handler_on_msg, kvs_handler_on_response, kvs_handler_on_close);

	kvs_proactor_init(global_server);

	if(argc == 2) {
		global_server->role = KVS_SERVER_ROLE_MASTER;

		kvs_server_storage_recovery(global_server, kvs_handler_process_raw_buffer);

		#if (NETWORK_SELECT == NETWORK_REACTOR)
			reactor_start(port, kvs_handler_on_msg);  //
		#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_start(port, kvs_handler_on_msg);
		#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			kvs_proactor_start(global_server);
		#endif

	} else if(argc == 5 && strcmp(argv[2], "slave") == 0){
	    // ./kvstore 2001 slave 127.0.0.1 2000
		global_server->role = KVS_SERVER_ROLE_SLAVE;
		const char *master_ip = argv[3];
		int master_port = atoi(argv[4]);
		strcpy(global_server->slave.master_ip, master_ip);
		global_server->slave.master_port = master_port;
		printf("Start as slave, master %s:%d\n", master_ip, master_port);

		kvs_slave_connect_master(global_server);

		int master_fd = global_server->slave.master_fd;

		assert(master_fd > 0);
		
		kvs_handler_register_master(&global_server->conns[master_fd], master_fd, global_server);

		// no need to recovery from AOF/RDB, slave has already synced from master
		// slave then will recv the replication commands from master
		//printf("RDB sync from master completed.\n");

		#if (NETWORK_SELECT == NETWORK_REACTOR)
			reactor_start(port, kvs_handler_on_msg);  //
		#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_start(port, kvs_handler_on_msg);
		#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			kvs_proactor_start(global_server);
		#endif
	}

	printf("args error\n");
	kvs_proactor_destroy(global_server);
	kvs_server_destroy(global_server);

	return 0;

}


