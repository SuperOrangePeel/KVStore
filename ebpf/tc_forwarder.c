#include <arpa/inet.h>
#include <errno.h>
#include <net/if.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include "tc_forwarder.skel.h"

#define MAX_PAYLOAD 1400

struct tc_forwarder_config {
    unsigned short master_port;
    unsigned short _pad;
};

struct payload_event {
    unsigned int len;
    unsigned char data[MAX_PAYLOAD];
};

static volatile sig_atomic_t exiting;
static int slave_fd = -1;
static int listen_fd = -1;
static unsigned long long forwarded_packets;
static unsigned long long forwarded_bytes;

static void handle_signal(int signo)
{
    (void)signo;
    exiting = 1;
}

static int listen_for_slave(const char *ip, unsigned short port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid listen ip: %s\n", ip);
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind slave listener");
        close(fd);
        return -1;
    }

    if (listen(fd, 1) < 0) {
        perror("listen slave");
        close(fd);
        return -1;
    }

    return fd;
}

static int accept_slave(int fd)
{
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);
    int conn_fd;

    do {
        conn_fd = accept(fd, (struct sockaddr *)&peer, &peer_len);
    } while (conn_fd < 0 && errno == EINTR && !exiting);

    if (conn_fd < 0) {
        perror("accept slave");
        return -1;
    }

    char ip[INET_ADDRSTRLEN];
    const char *ip_str = inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
    printf("slave connected from %s:%u\n", ip_str ? ip_str : "?", ntohs(peer.sin_port));
    return conn_fd;
}

static int write_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("send slave");
            return -1;
        }
        if (n == 0)
            return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static void print_payload_preview(const struct payload_event *event)
{
    unsigned int preview_len = event->len < 80 ? event->len : 80;

    //printf("[packet %llu] len=%u preview=\"", forwarded_packets, event->len);
    for (unsigned int i = 0; i < preview_len; i++) {
        unsigned char c = event->data[i];
        if (c == '\r')
            printf("\\r");
        else if (c == '\n')
            printf("\\n");
        else if (c >= 32 && c <= 126)
            putchar(c);
        else
            printf("\\x%02x", c);
    }
    if (event->len > preview_len)
        printf("...");
    printf("\"\n");
}

static int handle_payload(void *ctx, void *data, size_t data_sz)
{
    (void)ctx;
    if (data_sz < sizeof(struct payload_event))
        return 0;

    const struct payload_event *event = data;
    if (event->len == 0 || event->len > MAX_PAYLOAD)
        return 0;

    forwarded_packets++;
    forwarded_bytes += event->len;
    // if (forwarded_packets <= 10 || forwarded_packets % 10000 == 0) {
    //     print_payload_preview(event);
    //     printf("[stats] forwarded_packets=%llu forwarded_bytes=%llu\n",
    //            forwarded_packets, forwarded_bytes);
    // }

    if (write_all(slave_fd, event->data, event->len) < 0)
        exiting = 1;
    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s <ifname> <master_port> <listen_ip> <listen_port>\n"
            "Example: %s eth0 2000 0.0.0.0 3000\n",
            prog, prog);
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc != 5) {
        usage(argv[0]);
        return 1;
    }

    const char *ifname = argv[1];
    unsigned short master_port = (unsigned short)atoi(argv[2]);
    const char *listen_ip = argv[3];
    unsigned short listen_port = (unsigned short)atoi(argv[4]);

    int ifindex = if_nametoindex(ifname);
    if (ifindex == 0) {
        fprintf(stderr, "unknown interface: %s\n", ifname);
        return 1;
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    listen_fd = listen_for_slave(listen_ip, listen_port);
    if (listen_fd < 0)
        return 1;

    printf("waiting for slave on %s:%u\n", listen_ip, listen_port);
    slave_fd = accept_slave(listen_fd);
    if (slave_fd < 0) {
        close(listen_fd);
        return 1;
    }

    struct tc_forwarder_bpf *skel = tc_forwarder_bpf__open_and_load();
    if (!skel) {
        fprintf(stderr, "failed to open/load BPF skeleton\n");
        close(slave_fd);
        close(listen_fd);
        return 1;
    }

    struct tc_forwarder_config cfg = {
        .master_port = master_port,
    };
    unsigned int key = 0;
    if (bpf_map_update_elem(bpf_map__fd(skel->maps.config_map), &key, &cfg, BPF_ANY) < 0) {
        perror("bpf_map_update_elem");
        tc_forwarder_bpf__destroy(skel);
        close(slave_fd);
        close(listen_fd);
        return 1;
    }

    struct bpf_tc_hook hook = {
        .sz = sizeof(hook),
        .ifindex = ifindex,
        .attach_point = BPF_TC_INGRESS,
    };
    struct bpf_tc_opts opts = {
        .sz = sizeof(opts),
        .prog_fd = bpf_program__fd(skel->progs.tc_forwarder_ingress),
    };

    int err = bpf_tc_hook_create(&hook);
    if (err && err != -EEXIST) {
        fprintf(stderr, "failed to create TC hook: %s\n", strerror(-err));
        tc_forwarder_bpf__destroy(skel);
        close(slave_fd);
        close(listen_fd);
        return 1;
    }

    err = bpf_tc_attach(&hook, &opts);
    if (err) {
        fprintf(stderr, "failed to attach TC program: %s\n", strerror(-err));
        bpf_tc_hook_destroy(&hook);
        tc_forwarder_bpf__destroy(skel);
        close(slave_fd);
        close(listen_fd);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(skel->maps.payload_events), handle_payload, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "failed to create ring buffer\n");
        bpf_tc_detach(&hook, &opts);
        bpf_tc_hook_destroy(&hook);
        tc_forwarder_bpf__destroy(skel);
        close(slave_fd);
        close(listen_fd);
        return 1;
    }

    printf("forwarding TCP payloads from %s:%u to connected slave\n", ifname, master_port);

    while (!exiting) {
        err = ring_buffer__poll(rb, 100);
        if (err == -EINTR)
            break;
        if (err < 0) {
            fprintf(stderr, "ring_buffer__poll failed: %d\n", err);
            break;
        }
    }

    ring_buffer__free(rb);
    bpf_tc_detach(&hook, &opts);
    bpf_tc_hook_destroy(&hook);
    tc_forwarder_bpf__destroy(skel);
    close(slave_fd);
    close(listen_fd);
    return 0;
}
