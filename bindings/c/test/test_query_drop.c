/**
 * Test for query() and drop() functions in C API
 */
#include "hocdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

// Define record structure matching schema
struct __attribute__((packed)) Trade {
    int64_t timestamp;
    double price;
    double volume;
    bool is_buy;
};

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int dir_has_files(const char *dir_path) {
    DIR *dir = opendir(dir_path);
    if (!dir) return 0;

    struct dirent *entry;
    int count = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] != '.') {
            count++;
        }
    }
    closedir(dir);
    return count;
}

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    system(cmd);
}

int test_query() {
    printf("Testing query()...\n");

    const char *test_dir = "../../../b_c_test_query";
    rm_rf(test_dir);

    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64},
        {"is_buy", HOCDB_TYPE_BOOL}
    };

    HOCDBHandle db = hocdb_init("QUERY_TEST", test_dir, schema, 4, 0, 0, 0, 0);
    if (!db) {
        printf("FAIL: Failed to initialize database\n");
        return 1;
    }

    // Append some trades
    struct Trade trades[] = {
        {1620000000, 100.0, 10.0, true},
        {1620000001, 200.0, 20.0, false},
        {1620000002, 300.0, 30.0, true},
        {1620000003, 400.0, 40.0, false},
        {1620000004, 500.0, 50.0, true}
    };

    for (int i = 0; i < 5; i++) {
        if (hocdb_append(db, &trades[i], sizeof(struct Trade)) != 0) {
            printf("FAIL: Append failed at %d\n", i);
            hocdb_close(db);
            rm_rf(test_dir);
            return 1;
        }
    }
    hocdb_flush(db);

    // Test 1: Query without filters
    size_t len;
    void *data = hocdb_query(db, 1620000000, 1620000005, NULL, 0, &len);
    if (!data) {
        printf("FAIL: Query without filters returned NULL\n");
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }

    size_t count = len / sizeof(struct Trade);
    if (count != 5) {
        printf("FAIL: Expected 5 records, got %zu\n", count);
        hocdb_free(data);
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }
    printf("  Query without filters: %zu records (OK)\n", count);
    hocdb_free(data);

    // Test 2: Query with time range
    data = hocdb_query(db, 1620000001, 1620000003, NULL, 0, &len);
    if (!data) {
        printf("FAIL: Range query returned NULL\n");
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }

    count = len / sizeof(struct Trade);
    if (count != 2) {
        printf("FAIL: Range query expected 2 records, got %zu\n", count);
        hocdb_free(data);
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }
    printf("  Query with range [1, 3): %zu records (OK)\n", count);
    hocdb_free(data);

    // Test 3: Query with filter (is_buy = true)
    HOCDBFilter filter = {
        .field_index = 3,  // is_buy field
        .type = HOCDB_TYPE_BOOL,
        .val_bool = true
    };
    data = hocdb_query(db, 1620000000, 1620000005, &filter, 1, &len);
    if (!data) {
        printf("FAIL: Filtered query returned NULL\n");
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }

    count = len / sizeof(struct Trade);
    if (count != 3) {
        printf("FAIL: Filtered query expected 3 buy orders, got %zu\n", count);
        hocdb_free(data);
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }
    printf("  Query with filter (is_buy=true): %zu records (OK)\n", count);
    hocdb_free(data);

    // Test 4: Query with price filter
    HOCDBFilter price_filter = {
        .field_index = 1,  // price field
        .type = HOCDB_TYPE_F64,
        .val_f64 = 300.0
    };
    data = hocdb_query(db, 1620000000, 1620000005, &price_filter, 1, &len);
    if (!data) {
        printf("FAIL: Price filtered query returned NULL\n");
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }

    count = len / sizeof(struct Trade);
    if (count != 1) {
        printf("FAIL: Price filter expected 1 record, got %zu\n", count);
        hocdb_free(data);
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }
    printf("  Query with filter (price=300.0): %zu records (OK)\n", count);
    hocdb_free(data);

    hocdb_close(db);
    rm_rf(test_dir);
    printf("PASS: query() tests\n\n");
    return 0;
}

int test_drop() {
    printf("Testing drop()...\n");

    const char *test_dir = "../../../b_c_test_drop";
    rm_rf(test_dir);

    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64}
    };

    HOCDBHandle db = hocdb_init("DROP_TEST", test_dir, schema, 2, 0, 0, 0, 0);
    if (!db) {
        printf("FAIL: Failed to initialize database\n");
        return 1;
    }

    // Append some data
    struct __attribute__((packed)) {
        int64_t timestamp;
        double price;
    } record = {1620000000, 50000.0};

    hocdb_append(db, &record, sizeof(record));
    record.timestamp = 1620000001;
    hocdb_append(db, &record, sizeof(record));
    hocdb_flush(db);

    // Verify data directory exists
    if (!file_exists(test_dir)) {
        printf("FAIL: Data directory should exist after writes\n");
        hocdb_close(db);
        return 1;
    }

    int files_before = dir_has_files(test_dir);
    if (files_before == 0) {
        printf("FAIL: Data files should exist before drop\n");
        hocdb_close(db);
        rm_rf(test_dir);
        return 1;
    }
    printf("  Files before drop: %d\n", files_before);

    // Drop the database
    hocdb_drop(db);

    // Verify data files are deleted
    int files_after = dir_has_files(test_dir);
    printf("  Files after drop: %d\n", files_after);

    if (files_after > 0) {
        printf("FAIL: Data files should be deleted after drop\n");
        rm_rf(test_dir);
        return 1;
    }

    rm_rf(test_dir);
    printf("PASS: drop() test\n\n");
    return 0;
}

int main() {
    printf("=== C Bindings Query and Drop Tests ===\n\n");

    int result = 0;
    result |= test_query();
    result |= test_drop();

    if (result == 0) {
        printf("All tests PASSED!\n");
    } else {
        printf("Some tests FAILED!\n");
    }

    return result;
}
