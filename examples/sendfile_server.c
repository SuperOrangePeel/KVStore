// sendfile_server.c
//
// TCP file receiver for sendfile benchmark.
//
// Usage:
//   ./sendfile_server <listen_ip> <port> <output_file>
//   ./sendfile_server <listen_ip> <port> --discard
//
// Example:
//   ./sendfile_server 0.0.0.0 7472 recv_2g.dat
//   ./sendfile_server 0.0.0.0 7472 --discard
//
// Build:
//   gcc -O2 -g -Wall -Wextra -o sendfile_server sendfile_server.c

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define RECV_BUF_SIZE (1024 * 1024 * 128)
#define BACKLOG 16

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static void die(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

static int write_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t written = 0;

    while (written < len) {
        ssize_t n = write(fd, p + written, len - written);

        if (n > 0) {
            written += (size_t)n;
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        return -1;
    }

    return 0;
}

static int create_listen_socket(const char *ip, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket");
    }

    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        die("setsockopt SO_REUSEADDR");
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid listen ip: %s\n", ip);
        exit(EXIT_FAILURE);
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        die("bind");
    }

    if (listen(fd, BACKLOG) < 0) {
        die("listen");
    }

    return fd;
}

static uint64_t ntohll_u64(uint64_t x)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)ntohl((uint32_t)(x & 0xffffffffULL)) << 32) |
           ntohl((uint32_t)(x >> 32));
#else
    return x;
#endif
}

static int recv_all(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t got = 0;

    while (got < len) {
        ssize_t n = recv(fd, p + got, len - got, 0);

        if (n > 0) {
            got += (size_t)n;
            continue;
        }

        if (n == 0) {
            return -1;
        }

        if (errno == EINTR) {
            continue;
        }

        return -1;
    }

    return 0;
}

static void run_server(const char *listen_ip,
                       uint16_t port,
                       const char *output_file,
                       int discard)
{
    int listen_fd = create_listen_socket(listen_ip, port);

    printf("[server] listening on %s:%u\n", listen_ip, port);

    int client_fd = accept(listen_fd, NULL, NULL);
    if (client_fd < 0) {
        die("accept");
    }

    printf("[server] client connected\n");

    /*
     * Protocol:
     * client first sends uint64_t file_size in network byte order.
     */
    uint64_t net_file_size = 0;
    if (recv_all(client_fd, &net_file_size, sizeof(net_file_size)) != 0) {
        fprintf(stderr, "[server] failed to receive file size\n");
        close(client_fd);
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    uint64_t expected_size = ntohll_u64(net_file_size);

    printf("[server] expected file size: %" PRIu64 " bytes\n", expected_size);
    printf("[server] mode: %s\n", discard ? "discard" : "write file");

    int out_fd = -1;

    if (!discard) {
        out_fd = open(output_file, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (out_fd < 0) {
            die("open output_file");
        }
    }

    char *buf = NULL;
    int ret = posix_memalign((void **)&buf, 4096, RECV_BUF_SIZE);
    if (ret != 0) {
        fprintf(stderr, "posix_memalign failed: %s\n", strerror(ret));
        exit(EXIT_FAILURE);
    }

    uint64_t total = 0;
    double start = now_sec();

    while (total < expected_size) {
        size_t want = RECV_BUF_SIZE;
        uint64_t remaining = expected_size - total;
        if (remaining < want) {
            want = (size_t)remaining;
        }

        ssize_t n = recv(client_fd, buf, want, 0);

        if (n > 0) {
            if (!discard) {
                if (write_all(out_fd, buf, (size_t)n) != 0) {
                    die("write output");
                }
            }

            total += (uint64_t)n;

            if ((total % (256ULL * 1024ULL * 1024ULL)) < (uint64_t)n ||
                total == expected_size) {
                double mib = (double)total / (1024.0 * 1024.0);
                printf("\r[server] received %.2f MiB", mib);
                fflush(stdout);
            }

            continue;
        }

        if (n == 0) {
            fprintf(stderr, "\n[server] peer closed early\n");
            break;
        }

        if (errno == EINTR) {
            continue;
        }

        die("recv");
    }

    printf("\n");

    if (!discard) {
        if (fsync(out_fd) != 0) {
            die("fsync");
        }
        close(out_fd);
    }

    double end = now_sec();
    double elapsed = end - start;
    if (elapsed <= 0.0) {
        elapsed = 1e-9;
    }

    double mib = (double)total / (1024.0 * 1024.0);
    double mibps = mib / elapsed;

    printf("[server] done\n");
    printf("[server] total bytes: %" PRIu64 "\n", total);
    printf("[server] elapsed: %.3f sec\n", elapsed);
    printf("[server] throughput: %.2f MiB/s\n", mibps);

    free(buf);
    close(client_fd);
    close(listen_fd);
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <listen_ip> <port> <output_file>\n"
                "  %s <listen_ip> <port> --discard\n"
                "\n"
                "Example:\n"
                "  %s 0.0.0.0 7472 recv_2g.dat\n"
                "  %s 0.0.0.0 7472 --discard\n",
                argv[0],
                argv[0],
                argv[0],
                argv[0]);
        return EXIT_FAILURE;
    }

    const char *listen_ip = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);

    int discard = 0;
    const char *output_file = argv[3];

    if (strcmp(argv[3], "--discard") == 0) {
        discard = 1;
        output_file = NULL;
    }

    run_server(listen_ip, port, output_file, discard);

    return EXIT_SUCCESS;
}