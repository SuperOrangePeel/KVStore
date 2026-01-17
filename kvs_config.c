#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "kvs_config.h"
#include "./deps/tomlc99/toml.h" // 根据你的实际路径修改

// --- 宏定义：极速读取助手 ---

// 读取字符串，如果不存在使用默认值，并安全拷贝
#define CONF_GET_STR(table, key, target, default_val) do { \
    toml_datum_t d = toml_string_in(table, key); \
    if (d.ok) { \
        strncpy(target, d.u.s, sizeof(target) - 1); \
        free(d.u.s); \
    } else { \
        strncpy(target, default_val, sizeof(target) - 1); \
    } \
} while(0)

// 读取整数，如果不存在使用默认值
#define CONF_GET_INT(table, key, target, default_val) do { \
    toml_datum_t d = toml_int_in(table, key); \
    if (d.ok) { \
        target = (int)d.u.i; \
    } else { \
        target = default_val; \
    } \
} while(0)

// 读取布尔值
#define CONF_GET_BOOL(table, key, target, default_val) do { \
    toml_datum_t d = toml_bool_in(table, key); \
    if (d.ok) { \
        target = d.u.b; \
    } else { \
        target = default_val; \
    } \
} while(0)


int kvs_config_load(kvs_config_t *conf, const char *filename) {
    FILE *fp;
    char errbuf[200];

    // 1. 打开文件
    if ((fp = fopen(filename, "r")) == NULL) {
        fprintf(stderr, "Cannot open config file %s: %s\n", filename, strerror(errno));
        return -1;
    }

    // 2. 解析 TOML
    toml_table_t *root = toml_parse_file(fp, errbuf, sizeof(errbuf));
    fclose(fp);

    if (!root) {
        fprintf(stderr, "TOML parse error: %s\n", errbuf);
        return -1;
    }

    // 3. 读取 Section: [server]
    toml_table_t *server = toml_table_in(root, "server");
    if (server) {
        CONF_GET_STR(server, "bind_ip", conf->bind_ip, "0.0.0.0");
        CONF_GET_INT(server, "port", conf->port, KVS_DEFAULT_PORT);
        CONF_GET_INT(server, "backlog", conf->backlog, KVS_DEFAULT_BACKLOG);
        CONF_GET_STR(server, "log_level", conf->log_level, KVS_DEFAULT_LOG_LEVEL);
        CONF_GET_INT(server, "io_uring_entries", conf->io_uring_entries, KVS_DEFAULT_IO_URING_ENTRIES);
    } else {
        // 使用默认值填充
        strcpy(conf->bind_ip, "0.0.0.0");
        conf->port = KVS_DEFAULT_PORT;
        conf->backlog = KVS_DEFAULT_BACKLOG;
        strcpy(conf->log_level, KVS_DEFAULT_LOG_LEVEL);
        conf->io_uring_entries = KVS_DEFAULT_IO_URING_ENTRIES;
    }

    // 4. 读取 Section: [persistence]
    toml_table_t *persist = toml_table_in(root, "persistence");
    if (persist) {
        CONF_GET_BOOL(persist, "aof_enabled", conf->aof_enabled, 0);
        CONF_GET_STR(persist, "aof_path", conf->aof_path, "./kvs.aof");
        CONF_GET_STR(persist, "rdb_path", conf->rdb_path, "./kvs.rdb");
    } else {
        conf->aof_enabled = 0;
        strcpy(conf->aof_path, "./kvs.aof");
        strcpy(conf->rdb_path, "./kvs.rdb");
    }

    // 5. 读取 Section: [replication]
    toml_table_t *repl = toml_table_in(root, "replication");
    if (repl) {
        CONF_GET_STR(repl, "replicaof_ip", conf->replicaof_ip, "");
        CONF_GET_INT(repl, "replicaof_port", conf->replicaof_port, 6379);
    } else {
        conf->replicaof_ip[0] = '\0'; // 空字符串代表 Master
    }

    // 6. 清理 TOML 内存
    toml_free(root);
    return 0;
}

void kvs_config_dump(kvs_config_t *conf) {
    printf("=== Server Configuration ===\n");
    printf("Bind: %s:%d\n", conf->bind_ip, conf->port);
    printf("Log Level: %s\n", conf->log_level);
    printf("Role: %s\n", conf->replicaof_ip[0] == '\0' ? "Master" : "Slave");
    if (conf->replicaof_ip[0] != '\0') {
        printf("Master Host: %s:%d\n", conf->replicaof_ip, conf->replicaof_port);
    }
    printf("AOF: %s (Path: %s)\n", conf->aof_enabled ? "Yes" : "No", conf->aof_path);
    printf("============================\n");
}