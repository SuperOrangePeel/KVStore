
#include "kvs_network.h"
#include "kvs_server.h"
#include "kvs_types.h"
#include "logger.h"
#include "kvs_response.h" // todo: 解耦！！

#include <assert.h>


typedef kvs_status_t (*kvs_client_state_handler_t)(struct kvs_server_s *server, struct kvs_conn_s *conn);

kvs_status_t _kvs_client_state_normal(struct kvs_server_s *server, struct kvs_conn_s *conn) {
    assert(0); // should not reach here for now
    // normal client, do nothing
    return KVS_OK;
}

kvs_status_t _kvs_client_state_wait_bgsave(struct kvs_server_s *server, struct kvs_conn_s *conn) {
    if(server == NULL || conn == NULL) {
        assert(0);
        return KVS_ERR;
    }

    struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->bussiness_ctx;
    if(cli_ctx == NULL || cli_ctx->header.type != KVS_CTX_NORMAL_CLIENT) {
        LOG_FATAL("invalid cli_ctx or ctx type, ctx: %d, type: %d", cli_ctx == NULL ? -1 : 1, 
            cli_ctx == NULL ? -1 : cli_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    // wait bgsave finished


    kvs_format_response(KVS_RES_OK, NULL, 0, conn);

    kvs_net_set_send_event_manual(conn);
    
    cli_ctx->state = KVS_CLIENT_STATE_NORMAL; // back to normal state
    return KVS_OK;
}

kvs_client_state_handler_t _kvs_server_client_state_handlers[] = {
    [KVS_CLIENT_STATE_NORMAL] = _kvs_client_state_normal, 
    [KVS_CLIENT_STATE_WAIT_BGSAVE] = _kvs_client_state_wait_bgsave, 
    [KVS_CLIENT_STATE_CLOSE_PENDING] = NULL, // KVS_CLIENT_STATE_NORMAL
};

kvs_status_t kvs_server_client_state_machine_tick(struct kvs_server_s *server, struct kvs_conn_s *conn) {
    if(server == NULL || conn == NULL) return KVS_ERR;
    struct kvs_client_context_s* cli_ctx = (struct kvs_client_context_s*)conn->bussiness_ctx;
    if(cli_ctx == NULL) {
        LOG_FATAL("cli_ctx is NULL");
        assert(0);
        return KVS_ERR;
    }
    if(cli_ctx->header.type != KVS_CTX_NORMAL_CLIENT) {
        LOG_FATAL("invalid ctx type: %d", cli_ctx->header.type);
        assert(0);
        return KVS_ERR;
    }
    
    
    kvs_status_t ret = KVS_OK;
    do {
        kvs_client_state_t state = cli_ctx->state;
        if(state < KVS_CLIENT_STATE_NORMAL || state >= KVS_CLIENT_STATE_NUM) {
            LOG_FATAL("invalid client state: %d", state);
            assert(0);
            return KVS_ERR;
        }

        kvs_client_state_handler_t handler = _kvs_server_client_state_handlers[state];
        if(handler == NULL) {
            LOG_FATAL("handler for state %d is NULL", state);
            assert(0);
            return KVS_ERR;
        }

        kvs_status_t ret = handler(server, conn);
        if(ret == KVS_ERR) {
            LOG_FATAL("client state handler for state %d failed", state);
            assert(0);
            return KVS_ERR;
        } else if(ret == KVS_QUIT) {
            // 断开连接
            LOG_INFO("client state handler for state %d requested to quit", state);
            assert(0);
            return KVS_QUIT;
        }
    } while (ret == KVS_STATUS_CONTINUE);

    return KVS_OK;
}
