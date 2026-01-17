#ifndef KV_LOGGER_H
#define KV_LOGGER_H



// 日志级别定义
typedef enum {
    LOG_DEBUG = 0,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_FATAL
} LogLevel;

// 全局配置，由 main 函数或 config 模块修改
extern LogLevel g_log_level;

// 设置日志级别
void logger_set_level(LogLevel level);

// 内部函数，不要直接调用
void log_internal(int level, const char* file, int line, const char* fmt, ...);

// 对外宏接口
// 使用 do-while(0) 包裹是为了保证宏在 if-else 语句中的安全性
#define LOG_DEBUG(fmt, ...) \
    do { if (g_log_level <= LOG_DEBUG) log_internal(LOG_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_INFO(fmt, ...) \
    do { if (g_log_level <= LOG_INFO) log_internal(LOG_INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_WARN(fmt, ...) \
    do { if (g_log_level <= LOG_WARN) log_internal(LOG_WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_ERROR(fmt, ...) \
    do { if (g_log_level <= LOG_ERROR) log_internal(LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_FATAL(fmt, ...) \
    do { \
        log_internal(LOG_FATAL, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        abort(); \
    } while(0)

#endif // KV_LOGGER_H