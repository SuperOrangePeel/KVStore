# 9.1 Kvstore
## run
```shell
git clone https://github.com/cktan/tomlc99.git deps/tomlc99
git clone https://github.com/wangbojing/NtyCo.git deps/NtyCo
mkdir test_slave

make

sudo modprobe siw
sudo rdma link add siw0 type siw netdev <网卡名>

./kvstore ./kvs.toml
cd test_slave

```

## 架构
![image](./images/architecture.png)
![image](https://disk.0voice.com/p/py)
## 测试环境
- 宿主机环境：
    - 硬件：macOS M3PRO 
    - 软件：Tahoe 26.2 
- 虚拟机环境 
    - 虚拟机载体：VMware Fusion Professional Version 13.6.1 (24319021) 
    - linux版本：Ubuntu 24.04.3 LTS
    - 分配内存：4GB
    - 分配核心：6 processer cores


## 性能测试
### redis-benchmark
测试策略：使用redis-benchmark测试，测试500w次SET命令，关闭持久化。
redis结果如下：
```shell
# redis
$ redis-benchmark -h 127.0.0.1 -p 6379 -t set -n 5000000 -P 500 -q
SET: 3927729.75 requests per second, p50=5.631 msec  

# kvstore
$ redis-benchmark -h 172.16.135.130 -p 2000 -t set -n 5000000 -P 500 -q
SET: 7363770.00 requests per second, p50=2.919 msec
```


```shell
# redis
$ redis-benchmark -h 127.0.0.1 -p 6379 -t set -n 5000000 -P 1 -q
SET: 275118.31 requests per second, p50=0.095 msec 

# kvstore
$ redis-benchmark -h 172.16.135.130 -p 2000 -t set -n 5000000 -P 1 -q
SET: 228592.34 requests per second, p50=0.119 msec     
```
在高并发下 QPS 达到 736万，超越 Redis (392万) 约 87%，充分释放 io_uring 的批量处理能力。
在单指令交互（Pipeline=1）场景下，性能与 Redis 处于同一数量级，延迟仅有微小差距。

### 自定义测试脚本
测试策略：进行500w次管道化的SET操作，一次性写入500w个不同的KEY，并开启持久化。

```shell
$ sudo perf record  -p 681498 -g -- sleep 10
$ ./test/test_hash 127.0.0.1 2000 1 50000000

+   27.86%    14.08%  kvstore  kvstore            [.] kvs_hash_resp_set                                           
+   11.80%    11.77%  kvstore  [kernel.kallsyms]  [k] __wake_up    
```
业务代码本身只占27.86%CPU时间

```shell
$ ./test/test_hash 127.0.0.1 2000 1 50000000

--- HSET Results ---
Total Time:     2.526 seconds
Success Count:  5000000
Actual QPS:     1979670.37
--------------------
```


### 内存性能测试
测试策略：进行500w次管道化的SET操作，一次性写入500w个不同的KEY。
```shell
key:11bytes value:21bytes  500w HSET
malloc      | 2023916.21 qps | 620M VIRT | 545M RES
kvs_mempool | 1984553.03 qps | 469M VIRT | 394M RES
jemalloc    | 1989839.09 qps | 509M VIRT | 402M RES
```

```shell
key:8bytes value:8bytes    500w HSET
malloc      | 2199975.10 qps | 620M VIRT | 545M RES
kvs_mempool | 2060713.57 qps | 316M VIRT | 241M RES
jemalloc    | 2151554.65 qps | 335M VIRT | 246M RES
```

### 持久化
测试策略：进行500w次管道化的SET操作，一次性写入500w个不同的KEY。

```shell
# alway 策略结果：
--- SET Results ---
Total Time:     2.621 seconds
Success Count:  5000000
Actual QPS:     1907322.44
--------------------

# very second 策略结果：
--- SET Results ---
Total Time:     2.546 seconds
Success Count:  5000000
Actual QPS:     1963904.23
--------------------
```


## 主从同步
### RDMA
分布式 KV 在主从全量同步时，TB 级的 RDB 文件传输会导致严重的 CPU 占用和网络 IO 瓶颈，传统 TCP 模式下内核态与用户态的多次拷贝显著增加了同步时长。

KVstore实现了一种高性能、零拷贝的 RDB 传输机制，并解决了 RDMA 在高速传输中极易触发的 RNR (Receiver Not Ready) 硬件错误。

基于 RDMA CMA/Verbs 开发了纯异步传输引擎，利用 Zero-Copy 绕过内核协议栈。
设计并实现了一套基于信用的反压机制 (Backpressure)。Slave 动态向 Master 授权“信用额度（Buffer 数量）”，Master 严格按额度发送。
有效解决了RNR错误的发生

### EBPF
使用ebpf监测主从增量同步的进度
```shell
$ git clone https://github.com/libbpf/libbpf-bootstrap.git
$ cp ./ebpf/kvs* ./libbpf-bootstrap/examples/c/
# 在./libbpf-bootstrap/examples/c/makefile的APPS中加上kvs_monitor
$ make kvs_monitor

$ python3 ./ebpf/monitor_server.py
$ ./kvstore
$ sudo ./kvs_monitor <PID> m 0 2000 127.0.0.1 9090
$ ./test_slave/kvstore
$ sudo ./kvs_monitor <PID> s 0 2004 127.0.0.1 9090
$ ./test/test_hash <ip> <port> 1 500000
```
![image](./images/MONITOR.png)

### 面试题
1. 为什么会实现kvstore，使用场景在哪里？
2. reactor, ntyco, io_uring的三种网络模型的性能差异？
3. 多线程的kvstore该如何改进？
4. 私有协议如何设计会更加安全可靠？
5. 协议改进以后，对已有的代码有哪些改变？
6. kv引擎实现了哪些？
7. 每个kv引擎的使用场景，以及性能差异？
8. 测试用例如何实现？并且保证代码覆盖率超过90%
9. 网络并发量如何？qps如何？
10. 能够跟哪些系统交互使用？




README 电脑各项配置信息、虚拟机的系统版本 编译步骤，测试方案与可行性，性能数据

实现配置文件，包含端口ip，日志级别，持久化方案，主从同步 等的配置，在配置文件中配置

aof、rdb文件落盘用io_uring,加载持久化文件用mmap

主从同步用ebpf

发rdb文件，调研rdma实现。