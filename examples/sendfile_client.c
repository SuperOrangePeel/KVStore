// sendfile_client.c
//
// TCP file sender using sendfile().
//
// Usage:
//   ./sendfile_client <server_ip> <port> <input_file>
//
// Example:
//   ./sendfile_client 192.168.1.10 7472 test_2g.dat
//
// Build:
//   gcc -O2 -g -Wall -Wextra -o sendfile_client sendfile_client.c

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
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define SENDFILE_CHUNK (64 * 1024 * 1024)

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

static uint64_t file_size_of(int fd)
{
    struct stat st;
    if (fstat(fd, &st) != 0) {
        die("fstat");
    }

    return (uint64_t)st.st_size;
}

static uint64_t htonll_u64(uint64_t x)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl((uint32_t)(x & 0xffffffffULL)) << 32) |
           htonl((uint32_t)(x >> 32));
#else
    return x;
#endif
}

static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, 0);

        if (n > 0) {
            sent += (size_t)n;
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        return -1;
    }

    return 0;
}

static int connect_to_server(const char *server_ip, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket");
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, server_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid server ip: %s\n", server_ip);
        exit(EXIT_FAILURE);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        die("connect");
    }

    return fd;
}

static void run_client(const char *server_ip,
                       uint16_t port,
                       const char *input_file)
{
    int in_fd = open(input_file, O_RDONLY);
    if (in_fd < 0) {
        die("open input_file");
    }

    uint64_t file_size = file_size_of(in_fd);

    int sock_fd = connect_to_server(server_ip, port);

    printf("[client] connected to %s:%u\n", server_ip, port);
    printf("[client] input file: %s\n", input_file);
    printf("[client] file size: %" PRIu64 " bytes\n", file_size);

    /*
     * Send file size first.
     */
    uint64_t net_file_size = htonll_u64(file_size);
    if (send_all(sock_fd, &net_file_size, sizeof(net_file_size)) != 0) {
        die("send file size");
    }

    uint64_t total_sent = 0;
    off_t offset = 0;

    double start = now_sec();

    while (total_sent < file_size) {
        uint64_t remaining = file_size - total_sent;
        size_t chunk = remaining > SENDFILE_CHUNK
                           ? SENDFILE_CHUNK
                           : (size_t)remaining;

        ssize_t n = sendfile(sock_fd, in_fd, &offset, chunk);

        if (n > 0) {
            total_sent += (uint64_t)n;

            if ((total_sent % (256ULL * 1024ULL * 1024ULL)) < (uint64_t)n ||
                total_sent == file_size) {
                double mib = (double)total_sent / (1024.0 * 1024.0);
                printf("\r[client] sent %.2f MiB", mib);
                fflush(stdout);
            }

            continue;
        }

        if (n == 0) {
            fprintf(stderr, "\n[client] sendfile returned 0 unexpectedly\n");
            break;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }

        die("sendfile");
    }

    printf("\n");

    /*
     * Gracefully close write side.
     */
    shutdown(sock_fd, SHUT_WR);

    double end = now_sec();
    double elapsed = end - start;
    if (elapsed <= 0.0) {
        elapsed = 1e-9;
    }

    double mib = (double)total_sent / (1024.0 * 1024.0);
    double mibps = mib / elapsed;

    printf("[client] done\n");
    printf("[client] total bytes: %" PRIu64 "\n", total_sent);
    printf("[client] elapsed: %.3f sec\n", elapsed);
    printf("[client] throughput: %.2f MiB/s\n", mibps);

    close(sock_fd);
    close(in_fd);
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <server_ip> <port> <input_file>\n"
                "\n"
                "Example:\n"
                "  %s 192.168.1.10 7472 test_2g.dat\n",
                argv[0],
                argv[0]);
        return EXIT_FAILURE;
    }

    const char *server_ip = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    const char *input_file = argv[3];

    run_client(server_ip, port, input_file);

    return EXIT_SUCCESS;
}