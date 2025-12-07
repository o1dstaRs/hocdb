#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../hocdb.h"

// Helper to clean up directory
void cleanup_dir(const char* path) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    system(cmd);
}

typedef struct {
    int64_t timestamp;
    double value;
} TestRecord;

int main() {
    printf("Running C Auto-Increment Recovery Test...\n");

    const char* ticker = "TEST_C_RECOVERY";
    const char* dir = "test_c_recovery_data";
    cleanup_dir(dir);

    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"value", HOCDB_TYPE_F64}
    };

    // 1. Create and fill ring buffer
    {
        // Max size for 3 records: Header(12) + 3 * 16 = 60
        HOCDBHandle db = hocdb_init(ticker, dir, schema, 2, 60, 1, 1, 1);
        assert(db != NULL);

        TestRecord r1 = {0, 1.1};
        TestRecord r2 = {0, 2.2};
        TestRecord r3 = {0, 3.3};
        TestRecord r4 = {0, 4.4};

        // Write 3 records (0, 1, 2)
        assert(hocdb_append(db, &r1, sizeof(TestRecord)) == 0);
        assert(hocdb_append(db, &r2, sizeof(TestRecord)) == 0);
        assert(hocdb_append(db, &r3, sizeof(TestRecord)) == 0);

        // Write 4th record (should wrap and overwrite 0)
        // Timestamp should be 3
        assert(hocdb_append(db, &r4, sizeof(TestRecord)) == 0);

        hocdb_close(db);
    }

    // 2. Re-open and verify recovery
    {
        HOCDBHandle db = hocdb_init(ticker, dir, schema, 2, 60, 1, 1, 1);
        assert(db != NULL);

        // Next append should be timestamp 4
        TestRecord r5 = {0, 5.5};
        assert(hocdb_append(db, &r5, sizeof(TestRecord)) == 0);

        // Load and verify
        size_t len_bytes = 0;
        TestRecord* data = (TestRecord*)hocdb_load(db, &len_bytes);
        assert(data != NULL);
        
        size_t num_records = len_bytes / sizeof(TestRecord);
        printf("Loaded %zu bytes (%zu records):\n", len_bytes, num_records);
        
        if (num_records != 3) {
            printf("ERROR: Expected 3 records, got %zu\n", num_records);
        }
        assert(num_records == 3);

        for (size_t i = 0; i < num_records; i++) {
            printf("  [%zu] ts=%lld, val=%.1f\n", i, data[i].timestamp, data[i].value);
        }

        if (num_records != 3) {
             printf("ERROR: Expected 3 records, got %zu. Aborting.\n", num_records);
             exit(1);
        }

        // Expected order: 3, 4, 5 (timestamps)
        // Values: 3.3, 4.4, 5.5
        assert(data[0].timestamp == 3);
        assert(data[0].value == 3.3);

        assert(data[1].timestamp == 4);
        assert(data[1].value == 4.4);

        assert(data[2].timestamp == 5);
        assert(data[2].value == 5.5);

        hocdb_free(data);
        hocdb_close(db);
    }

    cleanup_dir(dir);
    printf("✅ C Auto-Increment Recovery Test Passed\n");
    return 0;
}
