#include "kvs_event_loop.h"
#include "kvs_network.h"
#include "kvs_server.h"
#include "kvs_types.h"
#include "kvs_event_loop.h"

#include <liburing.h>
#include <assert.h>

// kvs_status_t kvs_server_timer_tick(struct kvs_server_s *server) {
//     if(server == NULL) {
//         return KVS_ERR;
//     }

//     // 1. master repl timer tick
//     if(server->role == KVS_SERVER_ROLE_MASTER && server->master != NULL) {
//         kvs_master_timer_tick(server->master);
//     }

//     // 2. slave repl timer tick
//     if(server->role == KVS_SERVER_ROLE_SLAVE && server->slave != NULL) {
//         kvs_slave_timer_tick(server->slave);
//     }

//     return KVS_OK;
// }

void _kvs_server_aof_timer_cb(void *ctx, int res, int flags) {
    struct kvs_server_s *server = (struct kvs_server_s *)ctx;
    if(server == NULL) {
        assert(0);
        return;
    }
    kvs_persistence_flush_aof(server->pers_ctx);
    kvs_loop_add_timeout(&server->loop, &server->aof_timer_ev, &server->aof_ts);
}


kvs_status_t kvs_server_init_aof_timer(struct kvs_server_s *server) {
    if(server == NULL) {
        return KVS_ERR;
    }
    server->aof_timer_ev.type = KVS_EV_TIMER;
    
    server->aof_timer_ev.handler = _kvs_server_aof_timer_cb;
    server->aof_timer_ev.ctx= (void*)server;
    server->aof_ts.tv_sec = 1; // 1 second
    server->aof_ts.tv_nsec = 0;
    

    kvs_loop_add_timeout(&server->loop, &server->aof_timer_ev, &server->aof_ts);
    return KVS_OK;
}