#include "vmlinux.h"
#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

char LICENSE[] SEC("license") = "GPL";

#define ETH_P_IP 0x0800
#define IPPROTO_TCP 6
#define MAX_PAYLOAD 1400
#define TC_ACT_OK 0

struct tc_forwarder_config {
    __u16 master_port;
    __u16 _pad;
};

struct payload_event {
    __u32 len;
    __u8 data[MAX_PAYLOAD];
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct tc_forwarder_config);
} config_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24);
} payload_events SEC(".maps");

SEC("tc")
int tc_forwarder_ingress(struct __sk_buff *skb)
{
    __u32 key = 0;
    struct tc_forwarder_config *cfg = bpf_map_lookup_elem(&config_map, &key);
    if (!cfg || cfg->master_port == 0)
        return TC_ACT_OK;

    __u32 off = 0;
    struct ethhdr eth;
    if (bpf_skb_load_bytes(skb, off, &eth, sizeof(eth)) < 0)
        return TC_ACT_OK;
    if (bpf_ntohs(eth.h_proto) != ETH_P_IP)
        return TC_ACT_OK;

    off += sizeof(eth);
    struct iphdr iph;
    if (bpf_skb_load_bytes(skb, off, &iph, sizeof(iph)) < 0)
        return TC_ACT_OK;
    if (iph.protocol != IPPROTO_TCP)
        return TC_ACT_OK;

    __u32 ihl = iph.ihl * 4;
    if (ihl < sizeof(iph) || ihl > 60)
        return TC_ACT_OK;

    struct tcphdr tcph;
    if (bpf_skb_load_bytes(skb, off + ihl, &tcph, sizeof(tcph)) < 0)
        return TC_ACT_OK;
    if (bpf_ntohs(tcph.dest) != cfg->master_port)
        return TC_ACT_OK;

    __u32 doff = tcph.doff * 4;
    if (doff < sizeof(tcph) || doff > 60)
        return TC_ACT_OK;

    __u32 ip_len = bpf_ntohs(iph.tot_len);
    __u32 hdr_len = ihl + doff;
    if (ip_len <= hdr_len)
        return TC_ACT_OK;

    __u32 payload_len = ip_len - hdr_len;
    if (payload_len > MAX_PAYLOAD)
        payload_len = MAX_PAYLOAD;

    __u32 payload_off = sizeof(eth) + hdr_len;
    __u32 skb_len = skb->len;
    if (payload_off >= skb_len)
        return TC_ACT_OK;

    __u32 avail = skb_len - payload_off;
    if (avail > MAX_PAYLOAD)
        avail = MAX_PAYLOAD;
    if (payload_len > avail)
        payload_len = avail;

    payload_len &= 0x7ff;
    if (payload_len == 0 || payload_len > MAX_PAYLOAD)
        return TC_ACT_OK;

    struct payload_event *event = bpf_ringbuf_reserve(&payload_events, sizeof(*event), 0);
    if (!event)
        return TC_ACT_OK;

    event->len = payload_len;
    if (bpf_skb_load_bytes(skb, payload_off, event->data, payload_len) < 0) {
        bpf_ringbuf_discard(event, 0);
        return TC_ACT_OK;
    }

    bpf_ringbuf_submit(event, 0);
    return TC_ACT_OK;
}
