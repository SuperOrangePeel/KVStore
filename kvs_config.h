
#ifndef __KVS_CONFIG_H__
#define __KVS_CONFIG_H__

#include <stdint.h>

// 默认值定义
#define KVS_DEFAULT_PORT 6379
#define KVS_DEFAULT_BACKLOG 512
#define KVS_DEFAULT_IO_URING_ENTRIES 4096
#define KVS_DEFAULT_LOG_LEVEL "info"

typedef struct {
    // --- 网络配置 ---
    char bind_ip[64];
    int port;
    int backlog;
    int io_uring_entries; // io_uring 队列深度

    // --- 通用配置 ---
    char log_level[16];   // debug, info, warn, error
    int worker_threads;   // 虽然你是单线程 Proactor，预留给未来 Worker 线程池

    // --- 持久化配置 ---
    int aof_enabled;
    char aof_path[256];
    int aof_fsync_policy; // 0: no, 1: always, 2: everysec

    int rdb_compression;
    char rdb_path[256];

    // --- 主从复制 ---
    // 如果 replicaof_ip 不为空，则是 Slave
    char replicaof_ip[64];
    int replicaof_port;
    char master_auth[128]; // Master 密码

} kvs_config_t;

// 初始化并加载配置
// return: 0 on success, -1 on error
int kvs_config_load(kvs_config_t *conf, const char *filename);

// 打印配置信息 (调试用)
void kvs_config_dump(kvs_config_t *conf);

#endif // __KVS_CONFIG_H__