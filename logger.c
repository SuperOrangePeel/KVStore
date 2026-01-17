#include "logger.h"
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>

LogLevel g_log_level = LOG_DEBUG; // 默认级别

void logger_set_level(LogLevel level) {
    g_log_level = level;
}

void log_internal(int level, const char* file, int line, const char* fmt, ...) {
    // 颜色代码
    const char* colors[] = {
        "\x1b[36m", // DEBUG - Cyan
        "\x1b[32m", // INFO  - Green
        "\x1b[33m", // WARN  - Yellow
        "\x1b[31m", // ERROR - Red
        "\x1b[35m"  // FATAL - Magenta
    };
    const char* reset_color = "\x1b[0m";
    const char* level_strs[] = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"};

    // 获取时间
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm_info;
    // 使用 localtime_r 替代 localtime，保证线程安全（虽然你现在是单线程，但要养成好习惯）
    localtime_r(&tv.tv_sec, &tm_info);
    
    char time_buffer[32];
    strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S", &tm_info);

    // 格式化输出头部： [时间] [级别] [文件:行]
    // 输出到 stderr
    fprintf(stderr, "%s[%s.%06ld] [%s] [%s:%d]%s ", 
            colors[level], 
            time_buffer, tv.tv_usec, 
            level_strs[level], 
            file, line, 
            reset_color);

    // 处理变参消息
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);

    fprintf(stderr, "\n");
    // 既然是日志，建议手动 flush，防止崩溃时日志留在缓冲区丢了
    // 虽然 stderr 通常无缓冲，但在重定向时行为不确定
    fflush(stderr); 
}