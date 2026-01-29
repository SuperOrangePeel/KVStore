import socket
import struct
import time
import os

# 配置
BIND_IP = "0.0.0.0"
BIND_PORT = 9090

# 协议格式: Role(I), Timestamp(Q), Count(Q), SRTT(Q)
# I = unsigned int (4 bytes), Q = unsigned long long (8 bytes)
# Total size = 4 + 8 + 8 + 8 = 28 bytes
STRUCT_FMT = "<IQQQ" 

# 内存状态
state = {
    'master': {'ts': 0, 'count': 0, 'srtt': 0},
    'slave':  {'ts': 0, 'count': 0, 'srtt': 0}
}

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_dashboard():
    clear_screen()
    now = time.time()
    
    m = state['master']
    s = state['slave']
    
    # 计算 Lag
    lag = m['count'] - s['count']
    if lag < 0: lag = 0 # 防止 slave 重启导致计数重置
    
    # 判断存活 (超过 3 秒没心跳视为 Down)
    m_status = "ONLINE" if (now - m['ts'] < 3 and m['ts'] > 0) else "OFFLINE"
    s_status = "ONLINE" if (now - s['ts'] < 3 and s['ts'] > 0) else "OFFLINE"

    print("================================================================")
    print("             KV-STORE DISTRIBUTED MONITOR DASHBOARD             ")
    print("================================================================")
    print(f"Server Time: {time.strftime('%H:%M:%S')}")
    print("----------------------------------------------------------------")
    
    # 打印 Master 状态
    print(f" [MASTER] Status: {m_status}")
    print(f"    Sent Commands : {m['count']:,}")
    print(f"    Network SRTT  : {m['srtt']} us")
    
    print("----------------------------------------------------------------")
    
    # 打印 Slave 状态
    print(f" [SLAVE ] Status: {s_status}")
    print(f"    Exec Commands : {s['count']:,}")
    print(f"    Last Report   : {time.strftime('%H:%M:%S', time.localtime(s['ts'])) if s['ts'] > 0 else 'Never'}")

    print("================================================================")
    
    # 核心指标：同步积压
    color = "\033[92m" # Green
    if lag > 1000: color = "\033[93m" # Yellow
    if lag > 10000: color = "\033[91m" # Red
    
    print(f" SYNC LAG (Backlog): {color}{lag:,} ops\033[0m")
    
    if m_status == "ONLINE" and s_status == "ONLINE":
        print(f" SYNC HEALTH: {'HEALTHY' if lag < 1000 else 'CONGESTED'}")
    else:
        print(f" SYNC HEALTH: \033[91mBROKEN LINK\033[0m")
        
    print("================================================================")

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BIND_IP, BIND_PORT))
    print(f"Dashboard Server listening on UDP {BIND_PORT}...")

    while True:
        # 阻塞接收数据
        data, addr = sock.recvfrom(1024)
        
        if len(data) < 28:
            continue
            
        try:
            # 解包
            role_id, ts, count, srtt = struct.unpack(STRUCT_FMT, data[:28])
            
            # 更新状态
            role = 'master' if role_id == 0 else 'slave'
            state[role]['ts'] = time.time() # 使用收到包的时间作为最新时间
            state[role]['count'] = count
            state[role]['srtt'] = srtt
            
            # 刷新屏幕
            print_dashboard()
            
        except Exception as e:
            print(f"Error parsing packet: {e}")

if __name__ == "__main__":
    main()