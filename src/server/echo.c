#include "echo.h"
#include "kvs_network.h"
#include <stdio.h>
#include <string.h>

int echo_on_accept(struct kvs_conn_s *conn) {
    conn->header.user_data = NULL; // 不需要特定的上下文
    printf("Echo server: New connection accepted.\n");
    return 0; // 接受连接成功
}

int echo_on_msg(struct kvs_conn_s *conn, int *read_size) {
    *read_size = 0;

    // 这里我们简单地将收到的数据原封不动地发送回去
    // 1. 从连接的读缓冲区获取数据
    char *msg = conn->r_buffer;
    int msg_sz = conn->r_idx; // 当前读缓冲区中的数据大小

    // 2. 将数据复制到发送缓冲区，最多复制当前发送缓冲区还能容纳的大小
    int writable_sz = conn->s_buf_sz - conn->s_idx;
    int copy_sz = msg_sz < writable_sz ? msg_sz : writable_sz;
    if (copy_sz <= 0) {
        kvs_net_set_send_event_manual(conn);
        return KVS_OK;
    }
    memcpy(conn->s_buffer + conn->s_idx, msg, copy_sz);
    conn->s_idx += copy_sz;

    // 3. 设置发送事件
    kvs_net_set_send_event_manual(conn);


    *read_size = copy_sz; // 告诉网络层我们已经处理了多少数据
    return 0; // 消息处理成功
}

int echo_on_send(struct kvs_conn_s *conn, int bytes_sent) {
    // 发送完成后，我们不需要做额外的处理，因为数据已经在 echo_on_msg 中准备好了
    return 0; // 发送处理成功
}

int echo_on_close(struct kvs_conn_s *conn) {
    // 连接关闭时，我们不需要做额外的处理
    printf("Echo server: Connection closed.\n");
    return 0; // 关闭处理成功
}
