#include "../hocdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

// Define a struct that matches our schema for easier packing
typedef struct {
    int64_t timestamp;
    double usd;
    double volume;
} TradeData;

int main() {
    printf("Testing basic HOCDB C bindings functionality...\n");
    
    // Clean up any previous test data
    system("rm -rf b_c_test_data/simple");
    system("mkdir -p b_c_test_data");
    
    // Define Schema
    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };
    
    // Test 1: Initialize database
    printf("1. Testing initialization...\n");
    // hocdb_init(ticker, path, schema, schema_len, max_size, overwrite, flush)
    HOCDBHandle db = hocdb_init("SIMPLE_TEST", "b_c_test_data/simple", schema, 3, 1024*1024, 1, 1, 0);
    if (!db) {
        printf("Failed to initialize database\n");
        return 1;
    }
    printf("Database initialized successfully\n");
    
    // Test 2: Append a single record
    printf("2. Testing append...\n");
    TradeData record = {100, 1.1, 10.1};
    int result = hocdb_append(db, &record, sizeof(TradeData));
    if (result != 0) {
        printf("Failed to append record: %d\n", result);
        hocdb_close(db);
        return 1;
    }
    printf("Record appended successfully\n");
    
    // Flush (although flush_on_write=1, explicit flush shouldn't hurt)
    result = hocdb_flush(db);
    if (result != 0) {
        printf("Failed to flush: %d\n", result);
        hocdb_close(db);
        return 1;
    }
    printf("Database flushed successfully\n");
    
    // Test 3: Load data (zero-copy)
    printf("3. Testing load (zero-copy)...\n");
    size_t byte_len = 0;
    void* data_ptr = hocdb_load(db, &byte_len);
    if (!data_ptr) {
        printf("Failed to load data\n");
        hocdb_close(db);
        return 1;
    }
    
    size_t count = byte_len / sizeof(TradeData);
    TradeData* data = (TradeData*)data_ptr;
    
    printf("Loaded %zu bytes (%zu records)\n", byte_len, count);
    if (count > 0) {
        printf("First record: ts=%ld, usd=%.2f, vol=%.2f\n", 
               (long)data[0].timestamp, data[0].usd, data[0].volume);
        
        if (data[0].timestamp != 100 || data[0].usd != 1.1 || data[0].volume != 10.1) {
            printf("Data verification failed!\n");
            hocdb_free(data_ptr);
            hocdb_close(db);
            return 1;
        }
    } else {
        printf("Expected at least 1 record!\n");
        hocdb_free(data_ptr);
        hocdb_close(db);
        return 1;
    }
    
    // Free the loaded data
    hocdb_free(data_ptr);
    printf("Data freed successfully\n");
    
    // Test 4: Close database
    printf("4. Testing close...\n");
    hocdb_close(db);
    printf("Database closed successfully\n");
    
    printf("\nBasic C bindings test completed successfully!\n");
    return 0;
}