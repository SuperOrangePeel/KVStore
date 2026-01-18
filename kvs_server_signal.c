#include "kvs_server.h"
#include "kvs_types.h"
#include "logger.h"
#include "kvs_network.h"
#include "kvs_event_loop.h"

#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <string.h>


#include <sys/wait.h>

static void _kvs_server_check_child_exit(struct kvs_server_s *server) {
    int statloc;
    pid_t pid;

    // 循环调用 wait3，防止同时有多个子进程退出（虽然 RDB 场景一般就一个）
    // WNOHANG: 非阻塞模式，这是关键！
    while ((pid = wait3(&statloc, WNOHANG, NULL)) > 0) {
        
        // 检查是不是 RDB 子进程
        if (pid == server->rdb_child_pid) {
            
            // 标记子进程已结束
            server->rdb_child_pid = -1;

            // 检查退出状态：是正常退出(exit 0) 还是 被杀/报错
            if (WIFEXITED(statloc) && WEXITSTATUS(statloc) == 0) {
                LOG_INFO("Background RDB saved successfully.");
                
                // 【核心】RDB 成功，触发广播逻辑
                // 这里调用 server的propagate 去通知所有等待的 Slave 和client
                kvs_server_on_rdb_save_finish(server, KVS_OK);
                
            } else {
                LOG_WARN("Background RDB failed.");
                assert(0);
                kvs_server_on_rdb_save_finish(server, KVS_ERR);
            }
        } else {
            LOG_WARN("Unknown child process %d exited.", pid);
        }
    }
}

void kvs_server_on_signal(void *ctx, int res, int flags) {
    struct kvs_server_s *server = ctx;

    if (res < 0) {
        LOG_ERROR("Read signalfd failed: %s", strerror(-res));
        return; // 甚至可以 exit
    }

    // 解析读取到的数据
    struct signalfd_siginfo *fdsi = &server->signal_info;
    
    switch (fdsi->ssi_signo) {
        case SIGINT:
        case SIGTERM:
            LOG_INFO("Received SHUTDOWN signal, exiting...");
            server->network.loop.stop = 1; // 让主循环退出
            break;

        case SIGCHLD:
            LOG_INFO("Received SIGCHLD, child process exited.");
            // 核心逻辑：去检查是不是 RDB 子进程挂了
            _kvs_server_check_child_exit(server); 
            break;
    }

    // 【重要】io_uring 的读是一次性的，必须重新提交！
    // 就像 listen fd 每次 accept 完要重新 add_accept 一样
    kvs_loop_add_read(&server->network.loop, 
                             &server->signal_ev, 
                             &server->signal_info, 
                             sizeof(struct signalfd_siginfo));
}


int kvs_server_init_signals(struct kvs_server_s *server) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);  // Ctrl+C
    sigaddset(&mask, SIGTERM); // kill 命令
    sigaddset(&mask, SIGCHLD); // 子进程退出 (RDB 完成)

    // 【关键动作】屏蔽信号！
    // 如果不屏蔽，信号来了会直接中断你的程序，signalfd 就读不到东西了。
    // 这行代码必须在主线程最开始执行。
    if (sigprocmask(SIG_BLOCK, &mask, NULL) == -1) {
        perror("sigprocmask");
        return -1;
    }

    // 创建 signalfd
    int sfd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (sfd < 0) {
        perror("signalfd");
        return -1;
    }
    
    server->signal_fd = sfd; // 保存起来

    // 注册到 Proactor (L0 层)
    // 这里复用我们之前设计的 add_read_buffer 接口，或者专门的 add_signal 接口
    // 注意：需要分配一个 event 结构体来承载上下文
    kvs_event_init(&server->signal_ev, sfd, KVS_EV_SIGNAL, kvs_server_on_signal, server);
    
    // 提交第一个读请求
    // 我们需要读 sizeof(struct signalfd_siginfo) 这么多字节
    return kvs_loop_add_read(&server->network.loop, 
                                    &server->signal_ev, 
                                    &server->signal_info, // 这是一个预分配的 struct signalfd_siginfo
                                    sizeof(struct signalfd_siginfo));
}