# 9.1 Kvstore
## run
```shell
mkdir build 
cd build
cmake ..
make

sudo modprobe siw
sudo rdma link add siw0 type siw netdev <网卡名>

./kvstore ./kvs.toml
```

## 架构
![image](./images/architecture.png)

## 测试环境
- 宿主机环境：
    - 硬件：macOS M3PRO 
    - 软件：Tahoe 26.2 
- 虚拟机环境 
    - 虚拟机载体：VMware Fusion Professional Version 13.6.1 (24319021) 
    - linux版本：Ubuntu 24.04.3 LTS
    - 分配内存：4GB
    - 分配核心：6 processer cores
- redis version: Redis 6.2.21 (578ac274/1) 64 bit


## 性能测试

### redis-benchmark set

1. 单客户端 / 单线程
```shell
redis-benchmark -h <ip> -p <port>  -t set -n 5000000 -q -P 10
```
| **Pipeline** | **Redis QPS** | **Redis p50** | **KVStore QPS** | **KVStore p50** | **KVStore / Redis** |
| ------------ | ------------- | ------------- | --------------- | --------------- | ------------------- |
| 1            | 307,749.12    | 0.087 ms      | 275,178.88      | 0.095 ms        | 0.89×               |
| 10           | 2,606,882.25  | 0.151 ms      | 2,270,663.25    | 0.111 ms        | 0.87×               |
| 20           | 3,387,534.00  | 0.247 ms      | 4,088,307.25    | 0.127 ms        | 1.21×               |
| 40           | 3,968,254.00  | 0.439 ms      | 7,072,135.50    | 0.143 ms        | 1.78×               |
| 80           | 4,201,680.50  | 0.847 ms      | 10,729,614.00   | 0.207 ms        | 2.55×               |
| 160          | 4,690,431.50  | 1.567 ms      | 10,482,180.00   | 0.503 ms        | 2.24×               |

2. 10 clients + 10 benchmark threads
```shell
redis-benchmark -h <ip> -p <port>  -t set -n 5000000 -q -c 10 --threads 10 -P 10
```
| **Pipeline** | **Redis QPS** | **Redis p50** | **KVStore QPS** | **KVStore p50** | **KVStore / Redis** |
| ------------ | ------------- | ------------- | --------------- | --------------- | ------------------- |
| 1            | 212,666.42    | 0.047 ms      | 201,881.53      | 0.047 ms        | 0.95×               |
| 10           | 1,331,912.62  | 0.071 ms      | 1,816,860.38    | 0.055 ms        | 1.36×               |
| 20           | 1,996,007.88  | 0.095 ms      | 3,328,894.75    | 0.055 ms        | 1.67×               |
| 40           | 2,496,255.50  | 0.135 ms      | 4,990,020.00    | 0.063 ms        | 2.00×               |
| 80           | 3,324,468.25  | 0.207 ms      | 6,657,789.50    | 0.079 ms        | 2.00×               |
| 160          | 3,987,241.00  | 0.351 ms      | 9,960,160.00    | 0.119 ms        | 2.50×               |

3. echo
```shell
redis-benchmark -h <ip> -p <port>  -n 5000000 -P 1 -q echo hello
```

| 系统 | 命令 | Pipeline | 请求数 | QPS | p50 延迟 | 相对 Redis |
|---|---|---:|---:|---:|---:|---:|
| Redis | `ECHO hello` | 1 | 500,000 | 293,255.12 | 0.087 ms | 1.00× |
| KVStore | `ECHO hello` | 1 | 500,000 | 274,423.72 | 0.095 ms | 0.94× |

### rdb
```shell
redis-benchmark -h 172.16.135.130 -p 2000 -t set -n 5000000 -P 10
```

| **save 触发阈值** | **SET QPS**  | **相比 1000 提升** | **相比 1000000 下降** |
| ----------------- | ------------ | ------------------ | --------------------- |
| 1000              | 1,893,222.25 | baseline           | 15.06%                |
| 10000             | 1,926,040.12 | +1.73%             | 13.60%                |
| 100000            | 2,091,175.25 | +10.45%            | 6.19%                 |
| 1000000           | 2,229,157.25 | +17.74%            | baseline              |

### aof

```shell
redis-benchmark -h <ip> -p <port>  -t set -n 5000000 -q -P 20
```

| **系统** | **关闭 AOF QPS** | **开启 AOF everysec QPS** | **下降 QPS** | **下降比例** | **保留性能** |
| -------- | ---------------- | ------------------------- | ------------ | ------------ | ------------ |
| KVStore  | 4,111,842.25     | 3,295,979.00              | 815,863.25   | 19.84%       | 80.16%         |
| Redis    | 3,092,146.00     | 1,952,362.25              | 1,139,783.75 | 36.86%       | 63.14%       |

### rdma vs. sendfile 

| **方案**                                  | **MTU** | **文件大小** | **Buffer / Chunk** | **Client 吞吐**   | **Server 吞吐** | **耗时** |
| ----------------------------------------- | ------- | ------------ | ------------------  | ----------------- | --------------- | -------- |
| RDMA WRITE + O_DIRECT read + token/window | 1500    | 4096 MiB     | 256 MiB           | **1977.71 MiB/s** | 1977.69 MiB/s   | 2.071 s  |
| RDMA WRITE + O_DIRECT read + token/window | 9000    | 4096 MiB     | 256 MiB               | **2057.96 MiB/s** | 2057.95 MiB/s   | 1.990 s  |
| TCP sendfile                              | 1500    | 4096 MiB     | sendfile chunk        | **1704.92 MiB/s** | 1704.95 MiB/s   | 2.402 s  |
| TCP sendfile                              | 9000    | 4096 MiB     | sendfile chunk        | **1865.38 MiB/s** | 1865.65 MiB/s   | 2.196 s  |


RDMA WRITE 的接收端数据直接写入 server 预注册的内存 buffer，server 不需要像 TCP 那样通过 recv() 从 socket receive buffer 拷贝到用户态 buffer；


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


### 性能测试分析


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
$ git clone --recursive https://github.com/libbpf/libbpf-bootstrap.git
$ cp ./ebpf/kvs* ./libbpf-bootstrap/examples/c/
$ cd ./libbpf-bootstrap/examples/c/
# 在./libbpf-bootstrap/examples/c/makefile的APPS中加上kvs_monitor
$ make kvs_monitor
$ cd ../../../

$ python3 ./ebpf/monitor_server.py
$ cd  ./libbpf-bootstrap/examples/c/
$ sudo ./kvs_monitor <PID> m 0 2000 127.0.0.1 9090
$ sudo ./kvs_monitor <PID> s 0 2004 127.0.0.1 9090
$ ./test/test_hash <ip> <port> 1 500000
```
![image](./images/MONITOR.png)


