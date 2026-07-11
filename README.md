# 9.1 Kvstore
## run
```shell
apt install -y liburing-dev
apt install -y libibverbs-dev librdmacm-dev

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


- 阿里云双实例
    - linux版本：Ubuntu 24.04.4 LTS
    - CPU： AMD EPYC 9T95 192-Core Processor
    - 分配内存：16G
    - 分配核心：4 processer cores
- redis version: Redis server v=7.0.15



## 性能测试
在macOS中，用宿主机中的redis-benmark测试虚拟机中的软件
在阿里云双实例中，用一个实例中
### redis-benchmark set

1. 单客户端 / 单线程
```shell
redis-benchmark -h <ip> -p <port> -t set -n 5000000 -q -P <pipeline>
```
虚拟机环境：
| Pipeline | Redis QPS            | Redis p50 | KVStore QPS          | KVStore p50 | KVStore / Redis |
|----------|----------------------|-----------|----------------------|-------------|-----------------|
| 1        | 48,600.31 req/s      | 1.007 ms  | 48,894.97 req/s      | 1.007 ms    | 1.01x           |
| 10       | 446,787.59 req/s     | 1.127 ms  | 445,196.34 req/s     | 1.095 ms    | 1.00x           |
| 20       | 838,785.44 req/s     | 1.119 ms  | 965,064.62 req/s     | 1.015 ms    | 1.15x           |
| 40       | 880,902.00 req/s     | 1.959 ms  | 1,146,526.00 req/s   | 1.535 ms    | 1.30x           |
| 80       | 1,402,524.50 req/s   | 2.295 ms  | 1,799,855.88 req/s   | 1.903 ms    | 1.28x           |
| 160      | 1,786,990.62 req/s   | 3.687 ms  | 2,458,210.50 req/s   | 2.775 ms    | 1.38x           |

阿里云双实例：
| Pipeline | Redis QPS            | Redis p50 | KVStore QPS           | KVStore p50 | KVStore / Redis |
|----------|----------------------|-----------|-----------------------|-------------|-----------------|
| 1        | 246,791.70 req/s     | 0.127 ms  | 254,452.92 req/s      | 0.119 ms    | 1.03x           |
| 10       | 1,903,311.75 req/s   | 0.231 ms  | 2,361,832.75 req/s    | 0.127 ms    | 1.24x           |
| 20       | 2,535,497.00 req/s   | 0.351 ms  | 4,212,300.00 req/s    | 0.143 ms    | 1.66x           |
| 40       | 3,045,067.00 req/s   | 0.607 ms  | 7,082,153.50 req/s    | 0.159 ms    | 2.33x           |
| 80       | 3,408,316.25 req/s   | 1.103 ms  | 11,636,928.00 req/s   | 0.191 ms    | 3.41x           |
| 160      | 3,619,254.50 req/s   | 2.103 ms  | 16,825,574.00 req/s   | 0.263 ms    | 4.65x           |


2. 单客户端 / 单线程 / -r
```shell
# -r代表在1~keylen范围内随机key，如果不加入随机key，那么会一直插入同一个key
redis-benchmark -h <ip> -p <port>  -t set -n 5000000 -q -r 5000000 -P 10
```
虚拟机环境：
| **Pipeline** | **Redis QPS** | **Redis p50** | **KVStore QPS** | **KVStore p50** | **KVStore / Redis** |
| -------- | ------------- | ------------- | --------------- | --------------- | -------- |
| 10       | 1,161,440.25  | 0.367 ms      | 2,374,169.00    | 0.127 ms        | 2.04×    |
| 20       | 1,402,524.50  | 0.623 ms      | 3,511,236.00    | 0.207 ms        | 2.50×    |
| 40       | 1,435,956.25  | 1.247 ms      | 4,401,408.50    | 0.375 ms        | 3.07×    |
| 80       | 1,532,332.25  | 2.359 ms      | 4,775,549.00    | 0.735 ms        | 3.12×    |
| 160      | 1,549,426.75  | 4.663 ms      | 4,950,495.00    | 1.463 ms        | 3.20×    |

阿里云双实例：
| Pipeline | Redis QPS            | Redis p50 | KVStore QPS          | KVStore p50 | KVStore / Redis |
|----------|----------------------|-----------|----------------------|-------------|-----------------|
| 1        | 251,546.03 req/s     | 0.119 ms  | 253,421.17 req/s     | 0.119 ms    | 1.01x           |
| 10       | 1,050,640.88 req/s   | 0.399 ms  | 2,049,180.25 req/s   | 0.175 ms    | 1.95x           |
| 20       | 1,234,567.88 req/s   | 0.687 ms  | 2,771,618.50 req/s   | 0.303 ms    | 2.24x           |
| 40       | 1,360,914.50 req/s   | 1.263 ms  | 3,518,648.75 req/s   | 0.519 ms    | 2.59x           |
| 80       | 1,434,720.25 req/s   | 2.383 ms  | 3,782,148.25 req/s   | 0.991 ms    | 2.64x           |
| 160      | 1,484,119.88 req/s   | 4.655 ms  | 4,061,738.50 req/s   | 1.863 ms    | 2.74x           |


2. 8 clients + 8 benchmark threads
```shell
redis-benchmark -h <ip> -p <port>  -t set -n 5000000 -q -r 5000000 -c 8 --threads 8 -P <pipeline>
```
虚拟机环境：
| Pipeline | Redis QPS            | Redis p50 | KVStore QPS          | KVStore p50 | KVStore / Redis |
|----------|----------------------|-----------|----------------------|-------------|-----------------|
| 1        | 35,698.99 req/s      | 0.215 ms  | 36,350.42 req/s      | 0.215 ms    | 1.02x           |
| 10       | 285,551.09 req/s     | 0.255 ms  | 333,111.25 req/s     | 0.231 ms    | 1.17x           |
| 20       | 512,610.22 req/s     | 0.303 ms  | 624,609.62 req/s     | 0.247 ms    | 1.22x           |
| 40       | 666,133.75 req/s     | 0.455 ms  | 869,414.00 req/s     | 0.335 ms    | 1.31x           |
| 80       | 999,600.19 req/s     | 0.567 ms  | 1,248,127.75 req/s   | 0.439 ms    | 1.25x           |
| 160      | 1,050,420.12 req/s   | 1.015 ms  | 1,425,313.62 req/s   | 0.743 ms    | 1.36x           |

阿里云双实例：
| Pipeline | Redis QPS            | Redis p50 | KVStore QPS          | KVStore p50 | KVStore / Redis |
|----------|----------------------|-----------|----------------------|-------------|-----------------|
| 1        | 153,657.05 req/s     | 0.055 ms  | 153,775.19 req/s     | 0.055 ms    | 1.00x           |
| 10       | 999,600.19 req/s     | 0.071 ms  | 1,175,364.25 req/s   | 0.063 ms    | 1.18x           |
| 20       | 1,175,364.25 req/s   | 0.119 ms  | 1,665,001.62 req/s   | 0.087 ms    | 1.42x           |
| 40       | 1,331,557.88 req/s   | 0.199 ms  | 2,220,248.50 req/s   | 0.127 ms    | 1.67x           |
| 80       | 1,426,126.62 req/s   | 0.367 ms  | 2,853,881.25 req/s   | 0.191 ms    | 2.00x           |
| 160      | 1,425,720.00 req/s   | 0.695 ms  | 3,328,894.75 req/s   | 0.319 ms    | 2.33x           |


3. echo
```shell
redis-benchmark -h <ip> -p <port>  -n 500000 -P 1 -q echo hello
```
虚拟机环境：
| 系统 | 命令 | Pipeline | QPS | p50 延迟 | 相对 Redis |
|---|---|---:|---:|---:|---:|
| Redis | `ECHO hello` | 1 | 47,637.20 req/s | 1.023 ms | 1.00× |
| KVStore | `ECHO hello` | 1 | 48,909.32 req/s | 1.007 ms | 1.03× |


阿里云双实例：
| 系统 | 命令 | Pipeline | 请求数 | QPS | p50 延迟 | 相对 Redis |
|---|---|---:|---:|---:|---:|---:|
| Redis | `ECHO hello` | 1 | 500,000 | 256,226.30 req/s | 0.119 ms | 1.00× |
| KVStore | `ECHO hello` | 1 | 500,000 | 251,838.40 req/s | 0.119 ms | 0.98× |

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

阿里云双实例：
```shell
redis-benchmark -h <ip> -p <port>  -t set -n 5000000 -q -P <pipeline>
```

| Pipeline | Redis No AOF QPS    | Redis AOF everysec QPS | Redis 下降 | Redis No AOF p50 | Redis AOF p50 | KVStore No AOF QPS  | KVStore AOF everysec QPS | KVStore 下降 | KVStore No AOF p50 | KVStore AOF p50 |
|----------|---------------------|------------------------|------------|------------------|---------------|---------------------|--------------------------|--------------|--------------------|-----------------|
| 1        | 246,791.70 req/s    | 240,615.97 req/s       | -2.50%     | 0.127 ms         | 0.127 ms      | 254,452.92 req/s    | 241,931.58 req/s         | -4.92%       | 0.119 ms           | 0.127 ms        |
| 10       | 1,903,311.75 req/s  | 1,215,362.25 req/s     | -36.14%    | 0.231 ms         | 0.375 ms      | 2,361,832.75 req/s  | 2,354,049.00 req/s       | -0.33%       | 0.127 ms           | 0.127 ms        |
| 20       | 2,535,497.00 req/s  | 1,428,571.38 req/s     | -43.66%    | 0.351 ms         | 0.655 ms      | 4,266,211.50 req/s  | 4,212,300.00 req/s       | -1.26%       | 0.135 ms           | 0.143 ms        |
| 40       | 3,045,067.00 req/s  | 1,555,210.00 req/s     | -48.93%    | 0.607 ms         | 1.231 ms      | 7,082,153.50 req/s  | 7,042,253.50 req/s       | -0.56%       | 0.159 ms           | 0.159 ms        |
| 80       | 3,408,316.25 req/s  | 1,642,036.12 req/s     | -51.82%    | 1.103 ms         | 2.359 ms      | 11,636,928.00 req/s | 11,611,975.00 req/s      | -0.21%       | 0.191 ms           | 0.191 ms        |
| 160      | 3,619,254.50 req/s  | 1,688,618.75 req/s     | -53.34%    | 2.103 ms         | 4.615 ms      | 16,825,574.00 req/s | 16,688,918.00 req/s      | -0.81%       | 0.263 ms           | 0.263 ms        |

```shell
redis-benchmark -h <ip> -p <port>  -t set -n 5000000  -r 5000000 -q -P <pipeline>
```
| Pipeline | Redis No AOF QPS    | Redis AOF everysec QPS | Redis 下降 | Redis No AOF p50 | Redis AOF p50 | KVStore No AOF QPS | KVStore AOF everysec QPS | KVStore 下降 | KVStore No AOF p50 | KVStore AOF p50 |
|----------|---------------------|------------------------|------------|------------------|---------------|--------------------|--------------------------|--------------|--------------------|-----------------|
| 1        | 251,546.03 req/s    | 247,586.03 req/s       | -1.57%     | 0.119 ms         | 0.127 ms      | 253,421.17 req/s   | 248,880.03 req/s         | -1.79%       | 0.119 ms           | 0.127 ms        |
| 10       | 1,050,640.88 req/s  | 803,987.81 req/s       | -23.48%    | 0.399 ms         | 0.551 ms      | 2,049,180.25 req/s | 1,868,460.25 req/s       | -8.82%       | 0.175 ms           | 0.223 ms        |
| 20       | 1,234,567.88 req/s  | 917,599.56 req/s       | -25.67%    | 0.687 ms         | 0.967 ms      | 2,771,618.50 req/s | 2,467,917.00 req/s       | -10.96%      | 0.303 ms           | 0.351 ms        |
| 40       | 1,360,914.50 req/s  | 974,848.88 req/s       | -28.37%    | 1.263 ms         | 1.847 ms      | 3,518,648.75 req/s | 2,929,115.50 req/s       | -16.75%      | 0.519 ms           | 0.631 ms        |
| 80       | 1,434,720.25 req/s  | 1,003,613.06 req/s     | -30.05%    | 2.383 ms         | 3.615 ms      | 3,782,148.25 req/s | 3,267,974.00 req/s       | -13.59%      | 0.991 ms           | 1.151 ms        |
| 160      | 1,484,119.88 req/s  | 1,038,206.00 req/s     | -30.05%    | 4.655 ms         | 6.999 ms      | 4,061,738.50 req/s | 3,376,097.25 req/s       | -16.88%      | 1.863 ms           | 2.271 ms        |


### rdma vs. sendfile 
#### 阿里云
测试环境为阿里云 eRDMA 双实例，RDMA 设备为 `erdma_0`，链路层为 Ethernet。TCP 测试使用内网 IP，RDMA 测试通过 RDMA CM 建立连接。MTU为8500。

| 测试项 | 测试方式 | 数据量 / 参数 | 平均吞吐 | 约合带宽 |
|---|---|---:|---:|---:|
| TCP `iperf3` | 单连接 TCP | 10s | - | 26.3 Gbit/s |
| TCP `sendfile` | client `sendfile()`，server `recv discard` | 4 GiB | 3359.68 MiB/s | 28.18 Gbit/s |
| RDMA WRITE | memory buffer → RDMA WRITE → server discard | 2–4 GiB | 约 3100–3160 MiB/s | 约 26 Gbit/s |
| RDMA READ  | RDMA READ，1MiB chunk  | 4 GiB | 约 3161.44 MiB/s | 约 26.52 Gbit/s |
| `ib_read_bw`  | RDMA READ，1MiB × 4096 | 4 GiB | 3161.43 MB/s | 25.29 Gbit/s |


| **测试方式**         | **参数**                      | **平均吞吐**                 |
| -------------------- | ----------------------------- | ---------------------------- |
| `ib_read_bw`         | `-R --report_gbits -D 5`      | 25.90 Gb/s                   |
| `ib_write_bw`        | `-R --report_gbits -D 5 -q 4` | 25.91 Gb/s                   |
| TCP sendfile discard | 15 GiB 数据量                 | 3160.30 MiB/s，约 26.51 Gb/s |
| TCP iperf3           | 单连接，10s                   | 26.2 Gb/s                    |

#### 本地虚拟机
测试环境说明：
- 测试方式：两台虚拟机之间传输 `test_2g.dat`
- 文件大小：2048 MiB
- RDMA 实现：Soft-iWARP（siw）
- 传输模式：RDMA_WRITE + token/window + O_DIRECT pread
- RDMA buffer size：8 MiB
- token count：4
- chunk 数量：256
- MTU：9000


| 测试项 | 测试方式 | 数据量 / 参数 | 平均吞吐 | 约合带宽 |
|---|---|---:|---:|---:|
| TCP `iperf3` | 单连接 TCP | 10s | 约 59.5 MiB/s | 499 Mbit/s |
| TCP `sendfile` | client `sendfile()`，server `recv discard` | 2 GiB | 59.03 MiB/s | 约 495 Mbit/s |
| RDMA WRITE，MTU 1500 | `O_DIRECT pread` → RDMA WRITE → server discard，token/window | 2 GiB，8 MiB buffer，token=4 | 53.46 MiB/s | 约 448 Mbit/s |
| RDMA READ | RDMA READ memory, 1 MiB chunk，depth=100 | 2 GiB | 51.93 MiB/s | 约 436 Mbit/s |
| RDMA WRITE，MTU 9000 | `O_DIRECT pread` → RDMA WRITE → server discard，token/window | 2 GiB，8 MiB buffer，token=4 | 52.75 MiB/s | 约 442 Mbit/s |

| MTU | 耗时 | 吞吐量 | 说明 |
|---:|---:|---:|---|
| 1500 | 38.306 s | 53.46 MiB/s | 原始测试结果 |
| 9000 | 38.824 s | 52.75 MiB/s | 调整 MTU 后结果 |

### 内存池

```
key:11bytes value:21bytes  500w SET
./test_hash 172.16.135.130 2000 1 5000000 500
```
| **内存池**  | **SET QPS** | **插入前 VIRT** | **插入前 RES** | **插入后 VIRT** | **插入后 RES** | **删除后 VIRT** | **删除后 RES** |
| ----------- | ------------ | --------------- | -------------- | --------------- | -------------- | --------------- | -------------- |
| malloc      | 2,442,669.33 | 74.71 MiB       | 47.52 MiB      | 610 MiB         | 581 MiB        | 610 MiB         | 581 MiB        |
| jemalloc    | 2,639,330.24 | 104 MiB         | 49.93 MiB      | 573 MiB         | 438 MiB        | 573 MiB         | 58.82MiB    |
| kvs_mempool | 2,795,660.69 | 74.71 MiB       | 47.54 MiB      | 459 MiB         | 430 MiB        | 459 MiB         | 430 MiB        |


## EBPF
### tc forwards
```shell
./build/ebpf/tc_forwarder <ifname> <master_port> <listen_ip> <listen_port>\n


./build/kvstore ./build/kvs.toml
sudo ./build/ebpf/tc_forwarder ens161 2000 172.16.135.130 3000
./test_slave/kvstore ./test_slave/kvs.toml
./test/test_hash 172.16.135.130 2000 1 2000000
./test/test_hash 172.16.135.130 2004 2 2000000
```

```
./test/test_hash 172.16.135.130 2000 1 200000 1 
```
tc qps :     38520.92
nornmal qpas : 60874.50


### 可观测
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




```shell
CREATEV qa DIM 768 METRIC COSINE INDEX FLAT
SETV qa answer:1001 FLOAT32 <3072 bytes vector> value <value>
GETV qa FLOAT32 <3072 bytes query_vector> TOPK 5
DELV qa answer:1001
VINFO qa
```