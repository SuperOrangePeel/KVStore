// file_generator.c
//
// Generate a test file for TCP sendfile / RDMA transfer benchmark.
//
// Default:
//   file name: test_2g.dat
//   file size: 2G
//
// Usage:
//   ./file_generator
//   ./file_generator <file_path>
//   ./file_generator <file_path> <size>
//
// Size examples:
//   2G
//   512M
//   4096K
//   1048576

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_FILE_PATH "test_2g.dat"
#define DEFAULT_FILE_SIZE ((uint64_t)2 * 1024 * 1024 * 1024ULL)
#define WRITE_BLOCK_SIZE  (4 * 1024 * 1024)

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s\n"
            "  %s <file_path>\n"
            "  %s <file_path> <size>\n"
            "\n"
            "Size examples:\n"
            "  2G\n"
            "  512M\n"
            "  4096K\n"
            "  1048576\n",
            prog, prog, prog);
}

static int parse_size(const char *s, uint64_t *out)
{
    if (!s || !*s || !out) {
        return -1;
    }

    errno = 0;

    char *end = NULL;
    unsigned long long value = strtoull(s, &end, 10);

    if (errno != 0 || end == s) {
        return -1;
    }

    uint64_t multiplier = 1;

    if (*end != '\0') {
        if (end[1] != '\0') {
            return -1;
        }

        switch (*end) {
            case 'k':
            case 'K':
                multiplier = 1024ULL;
                break;

            case 'm':
            case 'M':
                multiplier = 1024ULL * 1024ULL;
                break;

            case 'g':
            case 'G':
                multiplier = 1024ULL * 1024ULL * 1024ULL;
                break;

            default:
                return -1;
        }
    }

    if (value > UINT64_MAX / multiplier) {
        return -1;
    }

    *out = (uint64_t)value * multiplier;
    return 0;
}

static double now_sec(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }

    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static void fill_pattern(uint8_t *buf, size_t len, uint64_t block_index)
{
    /*
     * Deterministic pattern.
     * This avoids all-zero sparse-file style confusion and is useful for
     * optional checksum/debugging later.
     */
    uint64_t seed = 0x9e3779b97f4a7c15ULL ^ block_index;

    for (size_t i = 0; i < len; i++) {
        seed ^= seed << 7;
        seed ^= seed >> 9;
        seed ^= seed << 8;
        buf[i] = (uint8_t)(seed & 0xff);
    }
}

static int write_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
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

int main(int argc, char **argv)
{
    const char *file_path = DEFAULT_FILE_PATH;
    uint64_t file_size = DEFAULT_FILE_SIZE;

    if (argc > 3) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    if (argc >= 2) {
        file_path = argv[1];
    }

    if (argc >= 3) {
        if (parse_size(argv[2], &file_size) != 0 || file_size == 0) {
            fprintf(stderr, "Invalid file size: %s\n", argv[2]);
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    printf("Generating file:\n");
    printf("  path: %s\n", file_path);
    printf("  size: %" PRIu64 " bytes", file_size);

    if (file_size % (1024ULL * 1024ULL * 1024ULL) == 0) {
        printf(" (%" PRIu64 " GiB)", file_size / (1024ULL * 1024ULL * 1024ULL));
    } else if (file_size % (1024ULL * 1024ULL) == 0) {
        printf(" (%" PRIu64 " MiB)", file_size / (1024ULL * 1024ULL));
    }

    printf("\n");

    int fd = open(file_path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        perror("open");
        return EXIT_FAILURE;
    }

    uint8_t *buf = NULL;

    int ret = posix_memalign((void **)&buf, 4096, WRITE_BLOCK_SIZE);
    if (ret != 0) {
        fprintf(stderr, "posix_memalign failed: %s\n", strerror(ret));
        close(fd);
        return EXIT_FAILURE;
    }

    double start = now_sec();

    uint64_t remaining = file_size;
    uint64_t offset = 0;
    uint64_t block_index = 0;

    while (remaining > 0) {
        size_t chunk = remaining > WRITE_BLOCK_SIZE
                           ? WRITE_BLOCK_SIZE
                           : (size_t)remaining;

        fill_pattern(buf, chunk, block_index);

        if (write_all(fd, buf, chunk) != 0) {
            perror("write");
            free(buf);
            close(fd);
            return EXIT_FAILURE;
        }

        remaining -= chunk;
        offset += chunk;
        block_index++;

        if ((block_index % 64) == 0 || remaining == 0) {
            double percent = (double)offset * 100.0 / (double)file_size;
            printf("\rProgress: %6.2f%%  %" PRIu64 "/%" PRIu64 " bytes",
                   percent, offset, file_size);
            fflush(stdout);
        }
    }

    printf("\n");

    if (fsync(fd) != 0) {
        perror("fsync");
        free(buf);
        close(fd);
        return EXIT_FAILURE;
    }

    double end = now_sec();
    double elapsed = end - start;

    if (elapsed <= 0.0) {
        elapsed = 1e-9;
    }

    double mib = (double)file_size / (1024.0 * 1024.0);
    double mibps = mib / elapsed;

    printf("Done.\n");
    printf("Elapsed: %.3f sec\n", elapsed);
    printf("Write throughput: %.2f MiB/s\n", mibps);

    free(buf);
    close(fd);

    return EXIT_SUCCESS;
}