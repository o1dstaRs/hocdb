#include "../hocdb.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>

#define TICKER "TEST_C_FILTER"
#define DATA_DIR "b_c_test_filter_syntax"

void cleanup() {
    // Simple recursive removal for test dir (system specific, but works on mac/linux)
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", DATA_DIR);
    system(cmd);
}

int main() {
    cleanup();

    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"event", HOCDB_TYPE_I64}
    };

    printf("Initializing DB...\n");
    HOCDBHandle db = hocdb_init(TICKER, DATA_DIR, schema, 3, 0, 0, 0, 0);
    if (!db) {
        fprintf(stderr, "Failed to init DB\n");
        return 1;
    }

    printf("Appending data...\n");
    // 1. event = 0
    struct { int64_t ts; double p; int64_t e; } r1 = {100, 1.0, 0};
    hocdb_append(db, &r1, sizeof(r1));
    // 2. event = 1
    struct { int64_t ts; double p; int64_t e; } r2 = {200, 2.0, 1};
    hocdb_append(db, &r2, sizeof(r2));
    // 3. event = 2
    struct { int64_t ts; double p; int64_t e; } r3 = {300, 3.0, 2};
    hocdb_append(db, &r3, sizeof(r3));

    // Query with filter using helper
    printf("Querying with filter { event: 1 }...\n");
    
    int64_t event_idx = hocdb_get_field_index(db, "event");
    if (event_idx < 0) {
        fprintf(stderr, "Field 'event' not found\n");
        return 1;
    }

    HOCDBFilter filter;
    filter.field_index = (size_t)event_idx;
    filter.type = HOCDB_TYPE_I64;
    filter.val_i64 = 1;

    size_t out_len = 0;
    void* data = hocdb_query(db, 0, 1000, &filter, 1, &out_len);
    
    if (!data) {
        fprintf(stderr, "Query returned NULL\n");
        return 1;
    }

    size_t count = out_len / sizeof(r1);
    printf("Results count: %zu\n", count);

    if (count != 1) {
        fprintf(stderr, "Expected 1 result, got %zu\n", count);
        return 1;
    }

    struct { int64_t ts; double p; int64_t e; }* res = data;
    printf("Result: TS=%lld, Event=%lld\n", res->ts, res->e);

    if (res->e != 1) {
        fprintf(stderr, "Expected event 1, got %lld\n", res->e);
        return 1;
    }

    hocdb_free(data);
    hocdb_close(db);
    cleanup();

    printf("✅ C Filter Syntax Test Passed!\n");
    return 0;
}
