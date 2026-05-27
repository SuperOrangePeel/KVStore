#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>

#include <jemalloc/jemalloc.h>

static size_t get_rss_kb() {
    std::ifstream file("/proc/self/status");
    std::string line;

    while (std::getline(file, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            size_t value_kb = 0;
            std::string unit;
            iss >> key >> value_kb >> unit;
            return value_kb;
        }
    }

    return 0;
}

static void print_rss(const char* msg) {
    size_t rss_kb = get_rss_kb();
    double rss_mib = rss_kb / 1024.0;
    std::printf("%-35s RSS = %.2f MiB (%zu kB)\n", msg, rss_mib, rss_kb);
}

static void jemalloc_purge_all() {
    unsigned narenas = 0;
    size_t sz = sizeof(narenas);

    if (mallctl("arenas.narenas", &narenas, &sz, nullptr, 0) != 0) {
        std::fprintf(stderr, "mallctl arenas.narenas failed\n");
        return;
    }

    for (unsigned i = 0; i < narenas; ++i) {
        char cmd[64];
        std::snprintf(cmd, sizeof(cmd), "arena.%u.purge", i);
        mallctl(cmd, nullptr, nullptr, nullptr, 0);
    }
}

struct KVObj {
    char key[16];
    char value[32];
    void* next;
};

int main(int argc, char** argv) {
    size_t n = 5000000;

    if (argc >= 2) {
        n = std::strtoull(argv[1], nullptr, 10);
    }

    std::printf("jemalloc small object RES test\n");
    std::printf("object count: %zu\n", n);
    std::printf("object size : %zu bytes\n\n", sizeof(KVObj));

    print_rss("before allocation");

    std::vector<KVObj*> ptrs;
    ptrs.reserve(n);

    print_rss("after vector reserve");

    for (size_t i = 0; i < n; ++i) {
        KVObj* p = static_cast<KVObj*>(std::malloc(sizeof(KVObj)));
        if (!p) {
            std::fprintf(stderr, "malloc failed at %zu\n", i);
            return 1;
        }

        std::memset(p->key, 'k', sizeof(p->key));
        std::memset(p->value, 'v', sizeof(p->value));
        p->next = nullptr;

        ptrs.push_back(p);
    }

    print_rss("after malloc + write");

    for (KVObj* p : ptrs) {
        std::free(p);
    }

    print_rss("after free all objects");

    jemalloc_purge_all();

    print_rss("after jemalloc purge");

    std::vector<KVObj*>().swap(ptrs);

    print_rss("after vector release");

    jemalloc_purge_all();

    print_rss("after final purge");

    sleep(2);

    return 0;
}