#include "kvs_expire.h"
#include "common.h"

#include <stdint.h>
#include <string.h>
#include <time.h>

struct kvs_expire_entry_s {
    char *key;
    int len_key;
    uint64_t deadline_ns;
    struct kvs_expire_entry_s *next;
};

struct kvs_expire_table_s {
    struct kvs_expire_entry_s **buckets;
    int slots;
    int count;
    uint64_t random_state;
};

static uint64_t kvs_expire_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static unsigned int kvs_expire_hash(const char *key, int len_key) {
    unsigned int hash = 2166136261u;
    int i;
    for(i = 0; i < len_key; ++i) {
        hash ^= (unsigned char)key[i];
        hash *= 16777619u;
    }
    return hash;
}

static uint64_t kvs_expire_random(kvs_expire_table_t *table) {
    uint64_t x = table->random_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    table->random_state = x;
    return x;
}

static struct kvs_expire_entry_s **kvs_expire_find(kvs_expire_table_t *table,
        const char *key, int len_key) {
    unsigned int bucket = kvs_expire_hash(key, len_key) % (unsigned int)table->slots;
    struct kvs_expire_entry_s **entry = &table->buckets[bucket];
    while(*entry != NULL) {
        if((*entry)->len_key == len_key && memcmp((*entry)->key, key, (size_t)len_key) == 0) {
            break;
        }
        entry = &(*entry)->next;
    }
    return entry;
}

kvs_expire_table_t *kvs_expire_create(int slots) {
    kvs_expire_table_t *table;
    if(slots <= 0) return NULL;
    table = kvs_malloc(sizeof(*table));
    if(table == NULL) return NULL;
    table->buckets = kvs_malloc(sizeof(*table->buckets) * (size_t)slots);
    if(table->buckets == NULL) {
        kvs_free(table, sizeof(*table));
        return NULL;
    }
    memset(table->buckets, 0, sizeof(*table->buckets) * (size_t)slots);
    table->slots = slots;
    table->random_state = kvs_expire_now_ns() | 1u;
    return table;
}

void kvs_expire_destroy(kvs_expire_table_t *table) {
    int i;
    if(table == NULL) return;
    for(i = 0; i < table->slots; ++i) {
        struct kvs_expire_entry_s *entry = table->buckets[i];
        while(entry != NULL) {
            struct kvs_expire_entry_s *next = entry->next;
            kvs_free(entry->key, (size_t)entry->len_key);
            kvs_free(entry, sizeof(*entry));
            entry = next;
        }
    }
    kvs_free(table->buckets, sizeof(*table->buckets) * (size_t)table->slots);
    kvs_free(table, sizeof(*table));
}

int kvs_expire_set(kvs_expire_table_t *table, const char *key, int len_key,
                   uint64_t ttl_seconds) {
    struct kvs_expire_entry_s **found;
    uint64_t now;
    if(table == NULL || key == NULL || len_key <= 0 || ttl_seconds == 0) return -1;
    now = kvs_expire_now_ns();
    if(ttl_seconds > (UINT64_MAX - now) / UINT64_C(1000000000)) return -1;
    found = kvs_expire_find(table, key, len_key);
    if(*found == NULL) {
        *found = kvs_malloc(sizeof(**found));
        if(*found == NULL) return -1;
        (*found)->key = kvs_malloc((size_t)len_key);
        if((*found)->key == NULL) {
            kvs_free(*found, sizeof(**found));
            *found = NULL;
            return -1;
        }
        memcpy((*found)->key, key, (size_t)len_key);
        (*found)->len_key = len_key;
        (*found)->next = NULL;
        table->count++;
    }
    (*found)->deadline_ns = now + ttl_seconds * UINT64_C(1000000000);
    return 0;
}

void kvs_expire_remove(kvs_expire_table_t *table, const char *key, int len_key) {
    struct kvs_expire_entry_s **found;
    if(table == NULL || key == NULL || len_key <= 0) return;
    found = kvs_expire_find(table, key, len_key);
    if(*found != NULL) {
        struct kvs_expire_entry_s *entry = *found;
        *found = entry->next;
        kvs_free(entry->key, (size_t)entry->len_key);
        kvs_free(entry, sizeof(*entry));
        table->count--;
    }
}

int kvs_expire_is_expired(kvs_expire_table_t *table, const char *key, int len_key) {
    struct kvs_expire_entry_s **found;
    if(table == NULL || key == NULL || len_key <= 0) return 0;
    found = kvs_expire_find(table, key, len_key);
    return *found != NULL && (*found)->deadline_ns <= kvs_expire_now_ns();
}

void kvs_expire_active_cycle(kvs_expire_table_t *table, int samples,
                             kvs_expire_callback callback, void *arg) {
    int i;
    if(table == NULL || callback == NULL || samples <= 0 || table->count == 0) return;
    for(i = 0; i < samples; ++i) {
        unsigned int bucket = (unsigned int)(kvs_expire_random(table) % (uint64_t)table->slots);
        struct kvs_expire_entry_s *entry = table->buckets[bucket];
        if(entry != NULL && entry->deadline_ns <= kvs_expire_now_ns()) {
            callback(entry->key, entry->len_key, arg);
        }
    }
}
