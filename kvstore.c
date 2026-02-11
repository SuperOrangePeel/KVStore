#include "kvs_event_loop.h"
#include "kvs_rdma_engine.h"
#include "kvs_server.h"
//#include "kvs_proactor.h"
#include "kvs_config.h"
#include "logger.h"
#include "kvs_network.h"
#include "kvs_resp_protocol.h"
#include "kvs_executor.h"
#include "kvs_response.h"
#include "kvs_persistence.h"


#include <linux/mount.h>
#include <string.h>
#include <stdio.h>

#define KVS_REDIS_BENCHMARK_SET_COMPATIBILITY

//struct kvs_server_s *global_server = NULL;

struct kvs_server_s global_server;

int get_logger_info(const char *level_str, LogLevel *out_level) {
    if (strcmp(level_str, "debug") == 0) {
        *out_level = LOG_DEBUG;
    } else if (strcmp(level_str, "info") == 0) {
        *out_level = LOG_INFO;
    } else if (strcmp(level_str, "warn") == 0) {
        *out_level = LOG_WARN;
    } else if (strcmp(level_str, "error") == 0) {
        *out_level = LOG_ERROR;
    } else if (strcmp(level_str, "fatal") == 0) {
        *out_level = LOG_FATAL;
    } else {
        return -1; // unknown level
    }
    return 0;
}

int main(int argc, char *argv[]) {

	if (argc > 2) return -1;
    //logger_set_level(LOG_DEBUG);
    //logger_set_level(LOG_INFO);
    


    const char *config_file = (argc > 1) ? argv[1] : "kvs.toml";

    kvs_config_t config;
    
    if (kvs_config_load(&config, config_file) != 0) {
        LOG_WARN("Failed to load config, using defaults or exiting...");
        return -1;
    }

    LogLevel log_level;
    get_logger_info(config.log_level, &log_level);
    logger_set_level(log_level);
    // int port = atoi(argv[1]);

    // const char * server_ip = "172.16.135.130";

    int io_uring_entries = 1024;
    kvs_loop_init(&global_server.loop, io_uring_entries);
    
    struct kvs_network_config_s net_conf = {
        .loop = &global_server.loop,
        .server_ip = config.bind_ip,
        .port_listen = config.port,
        .max_conns = config.max_tcp_connections,
        //.io_uring_entries = 1024,
        .read_buffer_size = 1048576,
        .write_buffer_size = 1048576, // 1MB

        .on_accept = kvs_handler_on_accept,
        .on_msg = kvs_handler_on_msg,
        .on_send = kvs_handler_on_send,
        .on_close = kvs_handler_on_close,
        .server_ctx = (void*)&global_server,

        // //.rdma_ip = "192.168.31.16",
        // .rdma_port = 2001,
        // .cq_size = 256,
        // .max_recv_wr = 128,
        // .max_send_wr = 128,
        // .max_sge = 1,
        // .use_rdma = 1,
    };

    kvs_net_init(&global_server.network, &net_conf);


    struct kvs_rdma_config_s rdma_conf = {
        .server_ip = config.bind_ip,
        .server_port = config.rdma_port,
        .cq_size = 256,
        .max_recv_wr = 128,
        .max_send_wr = 128,
        .max_sge = 1,
        .max_inline_data = 64, // 操作系统可以直接把小于等于64字节的数据内联到WR中发送出去，不用额外进行DMA操作
        .global_ctx = (void*)&global_server,
        //.callbacks.on_connect_before = kvs_handler_on_rdma_connect_before,
        .callbacks.on_connect_request = kvs_handler_on_rdma_connect_request,
        .callbacks.on_established = kvs_handler_on_rdma_established,
        .callbacks.on_disconnected = kvs_handler_on_rdma_disconnected,
        .callbacks.on_error =  kvs_handler_on_rdma_error, // kvs_handler_on_rdma_error,

        .callbacks.on_comp_recv = kvs_handler_on_rdma_cq_recv,
        .callbacks.on_comp_send = kvs_handler_on_rdma_cq_send,
    };


    kvs_rdma_init_engine(&global_server.rdma_engine, &global_server.loop, &rdma_conf);

    //kvs_proactor_init(&proactor, &proactor_options);
    //size_t rdma_max_chunk_size = 1024 ; // 60KB
    int rdma_reponse_buf_size = 512; // 512B
    size_t rdma_recv_buf_count = 4; // 4 buffers for receiving RDB data


    struct kvs_server_config_s server_config = {
        .role = (config.master_ip[0] == '\0') ? KVS_SERVER_ROLE_MASTER : KVS_SERVER_ROLE_SLAVE,
        //.io_uring_entries = 1024,

        .master_config.max_slave_count = config.max_slave_count,
        .master_config.repl_backlog_size = config.repl_backlog_size,
        .master_config.rdma_recv_buffer_count = rdma_recv_buf_count * 2, // double buffers for each slave, 防止有除了有回复rdb传输的ack，还有其他数据导致RNR
        .master_config.rdma_recv_buf_size = rdma_reponse_buf_size,
        .slave_config.master_ip = config.master_ip,
        .slave_config.master_port = config.master_port,
        .slave_config.rdb_recv_buffer_count = rdma_recv_buf_count,
        .slave_config.rdma_send_buf_size = rdma_reponse_buf_size,

        .pers_config.aof_enabled = config.aof_enabled,
        .pers_config.aof_filename = config.aof_path,
        .pers_config.rdb_filename = config.rdb_path,
        .pers_config.aof_fsync_policy = config.aof_fsync_policy, 
        .protocol.protocol_parser = kvs_resp_parser,
        .protocol.execute_command = kvs_executor_cmd,
        .protocol.format_response = kvs_format_response,
        .use_rdma = 1,
        .rdma_max_chunk_size = config.rdma_max_chunk_size, 
    };

    if(config.master_ip[0] == '\0') {
        // Master mode
        server_config.role = KVS_SERVER_ROLE_MASTER;
        kvs_server_init(&global_server, &server_config);
        kvs_server_storage_recovery(&global_server); // recovery from AOF/RDB
        LOG_INFO("Start as Master");
    } else {
        // Slave mode
        server_config.role = KVS_SERVER_ROLE_SLAVE;
        server_config.pers_config.aof_enabled = 0; // slave no need to fsync AOF
        kvs_server_init(&global_server, &server_config);
        LOG_INFO("Start as Slave, master %s:%d", config.master_ip, config.master_port);
    }


    kvs_loop_run(&global_server.loop);

    
    kvs_net_deinit(&global_server.network);
    kvs_rdma_deinit_engine(&global_server.rdma_engine);

    kvs_server_deinit(&global_server);

    kvs_loop_deinit(&global_server.loop);



#if 0
	if(argc == 2) {
        //rdma_conf.server_port = 2001;
      
		struct kvs_server_config_s server_config = {
            .role = KVS_SERVER_ROLE_MASTER,
            //.io_uring_entries = 1024,

            .master_config.max_slave_count = KVS_SERVER_MAX_SLAVES_DEFAULT,
            .master_config.repl_backlog_size = 1024 * 1024, // 1MB
            .master_config.rdma_recv_buffer_count = rdma_recv_buf_count * 2, // double buffers for each slave, 防止有除了有回复rdb传输的ack，还有其他数据导致RNR
            .master_config.rdma_recv_buf_size = rdma_reponse_buf_size,
            .pers_config.aof_enabled = 0,
            .pers_config.aof_filename = "kvstore.aof",
            .pers_config.rdb_filename = "dump.rdb",
            .pers_config.aof_fsync_policy = KVS_AOF_EVERY_SEC, // slave no need to fsync AOF
            .protocol.protocol_parser = kvs_resp_parser,
            .protocol.execute_command = kvs_executor_cmd,
            .protocol.format_response = kvs_format_response,
            .use_rdma = 1,
            .rdma_max_chunk_size = rdma_max_chunk_size, 
        };

        kvs_server_init(&global_server, &server_config);
        LOG_DEBUG("kvs_server create");
        kvs_server_storage_recovery(&global_server); // recovery from AOF/RDB

        //kvs_net_start(&global_server.network);
        LOG_DEBUG("Start as master ip:port %s:%d\n", config.bind_ip, config.port);
	} else if(argc == 5 && strcmp(argv[2], "slave") == 0){
        //rdma_conf.server_port = 2002;
        kvs_rdma_init_engine(&global_server.rdma_engine, &global_server.loop, &rdma_conf);


        // ./kvstore 2001 slave 127.0.0.1 2000
        const char *master_ip = argv[3];
		int master_port = atoi(argv[4]);

        struct kvs_server_config_s server_config = {
            .role = KVS_SERVER_ROLE_SLAVE,
            //.io_uring_entries = 1024,
            .slave_config.master_ip = master_ip,
            .slave_config.master_port = master_port,
            .slave_config.rdb_recv_buffer_count = rdma_recv_buf_count,
            .slave_config.rdma_send_buf_size = rdma_reponse_buf_size,

            .pers_config.aof_enabled = 0,
            .pers_config.rdb_filename = "dump.rdb",
            .pers_config.aof_fsync_policy = KVS_AOF_EVERY_SEC, // slave no need to fsync AOF
            .protocol.protocol_parser = kvs_resp_parser,
            .protocol.execute_command = kvs_executor_cmd,
            .protocol.format_response = kvs_format_response,
            .use_rdma = 1,
            .rdma_max_chunk_size = rdma_max_chunk_size, 
        };
        //strcpy(server_config.slave_config.master_ip, master_ip);

        

	    kvs_server_init(&global_server, &server_config);
        // slave no need to recovery from AOF/RDB
		
		//kvs_handler_register_master(&global_server->conns[master_fd], master_fd, global_server);

		// no need to recovery from AOF/RDB, slave has already synced from master
		// slave then will recv the replication commands from master
		//printf("RDB sync from master completed.\n");
			
		//kvs_net_start(&global_server.network);
        LOG_DEBUG("Start as slave, master %s:%d\n", master_ip, master_port);
	} else {
        LOG_ERROR("args error\n");
        return -1;
    }

#endif

    
    // #if (NETWORK_SELECT == NETWORK_REACTOR)
	// 		reactor_start(port, kvs_handler_on_msg);  //
    // #elif (NETWORK_SELECT == NETWORK_NTYCO)
    //     ntyco_start(port, kvs_handler_on_msg);
    // #elif (NETWORK_SELECT == NETWORK_PROACTOR)
    //     kvs_proactor_start(global_server);
    // #endif

    

	
	

	return 0;

}
