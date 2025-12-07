#include "../hocdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

typedef struct {
    int64_t timestamp;
    double usd;
    double volume;
} TradeData;

double get_time_diff(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
}

int main() {
    printf("Testing HOCDB C bindings...\n");
    
    // Clean up any previous test data
    system("rm -rf b_c_test_data/bench");
    system("mkdir -p b_c_test_data");
    
    // Define Schema
    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };
    
    // Initialize the database
    HOCDBHandle db = hocdb_init("TEST_C", "b_c_test_data/bench", schema, 3, 0, 1, 0);
    if (!db) {
        printf("Failed to initialize database\n");
        return 1;
    }
    
    // Test 1: Append performance
    printf("\n1. Testing append performance...\n");
    struct timespec start, end;
    
    clock_gettime(CLOCK_MONOTONIC, &start);
    const int num_records = 1000000;  // 1M records
    TradeData record;
    
    for (int i = 0; i < num_records; i++) {
        record.timestamp = 1600000000 + i;
        record.usd = 50000.0 + (i % 1000) * 0.01;
        record.volume = 1.0 + (i % 100) * 0.01;
        
        int result = hocdb_append(db, &record, sizeof(TradeData));
        if (result != 0) {
            printf("Failed to append record %d\n", i);
            hocdb_close(db);
            return 1;
        }
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    double append_time = get_time_diff(start, end);
    double append_ops = num_records / append_time;
    
    printf("Appended %d records in %.4f seconds\n", num_records, append_time);
    printf("Append performance: %.0f ops/sec\n", append_ops);
    
    // Flush to ensure data is written
    hocdb_flush(db);
    
    // Test 2: Load performance (Zero-Copy)
    printf("\n2. Testing zero-copy load performance...\n");
    
    clock_gettime(CLOCK_MONOTONIC, &start);
    size_t byte_len = 0;
    void* data_ptr = hocdb_load(db, &byte_len);
    if (!data_ptr) {
        printf("Failed to load data\n");
        hocdb_close(db);
        return 1;
    }
    
    size_t count = byte_len / sizeof(TradeData);
    TradeData* data = (TradeData*)data_ptr;
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    double load_time = get_time_diff(start, end);
    
    printf("Loaded %zu bytes (%zu records) in %.6f seconds\n", byte_len, count, load_time);
    printf("Load performance: instantaneous (zero-copy)\n");
    
    // Verify data was loaded correctly
    printf("First record: ts=%ld, usd=%.2f, vol=%.2f\n", 
           (long)data[0].timestamp, data[0].usd, data[0].volume);
    printf("Last record: ts=%ld, usd=%.2f, vol=%.2f\n", 
           (long)data[count-1].timestamp, data[count-1].usd, data[count-1].volume);
    
    // Free the loaded data (zero-copy memory management)
    hocdb_free(data_ptr);
    
    // Test 3: Small data verification
    printf("\n3. Testing small dataset for accuracy...\n");
    
    system("rm -rf b_c_test_data/small");
    
    // Create a small database for verification
    HOCDBHandle small_db = hocdb_init("SMALL_C", "b_c_test_data/small", schema, 3, 0, 1, 0);
    if (!small_db) {
        printf("Failed to initialize small database\n");
        hocdb_close(db);
        return 1;
    }
    
    // Add a few known records
    TradeData r1 = {100, 1.1, 10.1};
    TradeData r2 = {200, 2.2, 20.2};
    TradeData r3 = {300, 3.3, 30.3};
    
    hocdb_append(small_db, &r1, sizeof(TradeData));
    hocdb_append(small_db, &r2, sizeof(TradeData));
    hocdb_append(small_db, &r3, sizeof(TradeData));
    hocdb_flush(small_db);
    
    // Load and verify
    size_t small_byte_len = 0;
    void* small_data_ptr = hocdb_load(small_db, &small_byte_len);
    size_t small_count = small_byte_len / sizeof(TradeData);
    TradeData* small_data = (TradeData*)small_data_ptr;
    
    if (!small_data || small_count != 3) {
        printf("Failed to load or wrong number of records: %zu\n", small_count);
        hocdb_close(small_db);
        hocdb_close(db);
        return 1;
    }
    
    if (small_data[0].timestamp != 100 || small_data[0].usd != 1.1 || small_data[0].volume != 10.1) {
        printf("Data verification failed for first record\n");
        hocdb_free(small_data_ptr);
        hocdb_close(small_db);
        hocdb_close(db);
        return 1;
    }
    
    if (small_data[2].timestamp != 300 || small_data[2].usd != 3.3 || small_data[2].volume != 30.3) {
        printf("Data verification failed for last record\n");
        hocdb_free(small_data_ptr);
        hocdb_close(small_db);
        hocdb_close(db);
        return 1;
    }
    
    hocdb_free(small_data_ptr);
    hocdb_close(small_db);
    printf("Small dataset verification passed!\n");
    
    // Close the main database
    hocdb_close(db);
    
    printf("\nC bindings test completed successfully!\n");
    return 0;
}