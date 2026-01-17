#include "kvs_server.h"
#include "kvs_proactor.h"
#include "kvs_handler.h"
#include "kvs_config.h"
#include "logger.h"

#include <string.h>
#include <stdio.h>

struct kvs_server_s *global_server = NULL;

int main(int argc, char *argv[]) {

	if (argc < 2) return -1;
    logger_set_level(LOG_DEBUG);
    //logger_set_level(LOG_INFO);


    // const char *config_file = (argc > 1) ? argv[1] : "kvs.toml";

    // kvs_config_t config;
    
    // if (kvs_config_load(&config, config_file) != 0) {
    //     LOG_WARN("Failed to load config, using defaults or exiting...");
    //     return -1;
    // }


    int port = atoi(argv[1]);

    struct kvs_proactor_s proactor;
    memset(&proactor, 0, sizeof(proactor));
    struct kvs_proactor_options_s proactor_options = {
        .conn_max = 32,
        .global_ctx = (void*)&global_server,  // link to server
        .port = port,
        .on_accept = kvs_handler_on_accept,
        .on_msg = kvs_handler_on_msg,
        .on_send = kvs_handler_on_send,
        .on_close = kvs_handler_on_close,
        .on_timer = kvs_handler_on_timer,
        .read_buffer_size = 4096,
        .write_buffer_size = 1048576, // 1MB
    };
    kvs_proactor_init(&proactor, &proactor_options);

	if(argc == 2) {
		struct kvs_server_config_s server_config = {
            .role = KVS_SERVER_ROLE_MASTER,
            .master_config.max_slave_count = KVS_SERVER_MAX_SLAVES_DEFAULT,
            .master_config.repl_backlog_size = 1024 * 1024, // 1MB
            .pers_config.aof_enabled = 0,
            .pers_config.aof_filename = "kvstore.aof",
            .pers_config.rdb_filename = "dump.rdb",
        };

        global_server = kvs_server_create(&proactor, &server_config);
        LOG_DEBUG("kvs_server create");
        kvs_server_storage_recovery(global_server, kvs_handler_process_raw_buffer); // recovery from AOF/RDB

        kvs_proactor_start(&proactor);

	} else if(argc == 5 && strcmp(argv[2], "slave") == 0){
        const char *master_ip = argv[3];
		int master_port = atoi(argv[4]);

        struct kvs_server_config_s server_config = {
            .role = KVS_SERVER_ROLE_SLAVE,
            .slave_config.master_port = master_port,
            .pers_config.aof_enabled = 0,
            .pers_config.rdb_filename = "dump.rdb",
        };
        strcpy(server_config.slave_config.master_ip, master_ip);

        printf("Start as slave, master %s:%d\n", master_ip, master_port);

	    global_server = kvs_server_create(&proactor, &server_config);
        // slave no need to recovery from AOF/RDB
		
		//kvs_handler_register_master(&global_server->conns[master_fd], master_fd, global_server);

		// no need to recovery from AOF/RDB, slave has already synced from master
		// slave then will recv the replication commands from master
		//printf("RDB sync from master completed.\n");
			
		kvs_proactor_start(&proactor);
	}
    // #if (NETWORK_SELECT == NETWORK_REACTOR)
	// 		reactor_start(port, kvs_handler_on_msg);  //
    // #elif (NETWORK_SELECT == NETWORK_NTYCO)
    //     ntyco_start(port, kvs_handler_on_msg);
    // #elif (NETWORK_SELECT == NETWORK_PROACTOR)
    //     kvs_proactor_start(global_server);
    // #endif

    

	printf("args error\n");
	kvs_proactor_deinit(&proactor);
	kvs_server_destroy(global_server);

	return 0;

}
