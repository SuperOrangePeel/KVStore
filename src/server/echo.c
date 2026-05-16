#include "echo.h"
#include "kvs_network.h"
#include "kvs_resp_protocol.h"
#include "logger.h"

#include <stdio.h>
#include <string.h>

#define ECHO_RESP_OK "+OK\r\n"
#define ECHO_RESP_OK_LEN ((int)(sizeof(ECHO_RESP_OK) - 1))

int echo_on_accept(struct kvs_conn_s *conn) {
    conn->header.user_data = NULL; // 不需要特定的上下文
    //printf("Echo server: New connection accepted.\n");
    return 0; // 接受连接成功
}

int echo_on_msg(struct kvs_conn_s *conn, int *read_size) {
    *read_size = 0;
    int parsed_total_len = 0;

    while (parsed_total_len < conn->r_idx) {
        struct kvs_handler_cmd_s cmd;
        int parsed_len = 0;

        if (conn->s_buf_sz - conn->s_idx < ECHO_RESP_OK_LEN) {
            break;
        }

        memset(&cmd, 0, sizeof(cmd));
        kvs_status_t status = kvs_resp_parser(conn->r_buffer + parsed_total_len,
                                              conn->r_idx - parsed_total_len,
                                              &cmd,
                                              &parsed_len);
        if (status == KVS_AGAIN) {
            break;
        }
        if (status == KVS_ERR) {
            LOG_ERROR("echo resp parse error");
            return KVS_ERR;
        }

        memcpy(conn->s_buffer + conn->s_idx, ECHO_RESP_OK, ECHO_RESP_OK_LEN);
        conn->s_idx += ECHO_RESP_OK_LEN;
        parsed_total_len += parsed_len;
    }

    *read_size = parsed_total_len;
    if (conn->s_idx > 0) {
        kvs_net_set_send_event_manual(conn);
    }
    return KVS_OK;
}

int echo_on_send(struct kvs_conn_s *conn, int bytes_sent) {
    // 发送完成后，我们不需要做额外的处理，因为数据已经在 echo_on_msg 中准备好了
    return 0; // 发送处理成功
}

int echo_on_close(struct kvs_conn_s *conn) {
    // 连接关闭时，我们不需要做额外的处理
    //printf("Echo server: Connection closed.\n");
    return 0; // 关闭处理成功
}
