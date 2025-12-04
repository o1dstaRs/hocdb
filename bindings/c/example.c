#include "hocdb.h"
#include <stdio.h>
#include <stdint.h>

int main() {
    printf("Initializing HOCDB C bindings example...\n");
    
    // Initialize the database - using the simpler init function
    HOCDBHandle db = hocdb_init("C_EXAMPLE", "example_data_c");
    if (!db) {
        printf("Failed to initialize database\n");
        return 1;
    }
    
    // Add some sample data
    printf("Adding sample records...\n");
    for (int i = 0; i < 5; i++) {
        int64_t timestamp = 1000 + i * 1000;  // Starting at 1000, adding 1000 per record
        double usd = 100.0 + i * 10.0;
        double volume = 1000.0 + i * 100.0;
        
        int result = hocdb_append(db, timestamp, usd, volume);
        if (result != 0) {
            printf("Failed to append record %d\n", i);
            hocdb_close(db);
            return 1;
        }
        
        printf("Added: ts=%ld, usd=%.2f, vol=%.2f\n", (long)timestamp, usd, volume);
    }
    
    // Flush to ensure data is written
    int flush_result = hocdb_flush(db);
    if (flush_result != 0) {
        printf("Failed to flush database\n");
        hocdb_close(db);
        return 1;
    }
    printf("Data flushed to disk.\n");
    
    // Load data with zero-copy
    printf("\nLoading data with zero-copy...\n");
    size_t length = 0;
    const TradeData* data = hocdb_load(db, &length);
    if (!data) {
        printf("Failed to load data\n");
        hocdb_close(db);
        return 1;
    }
    
    printf("Loaded %zu records:\n", length);
    for (size_t i = 0; i < length; i++) {
        printf("  Record %zu: ts=%ld, usd=%.2f, vol=%.2f\n", 
               i, (long)data[i].timestamp, data[i].usd, data[i].volume);
    }
    
    // Free the loaded data (zero-copy memory management)
    hocdb_free((void*)data);
    printf("\nData freed.\n");
    
    // Close the database
    hocdb_close(db);
    printf("\nHOCDB C example completed successfully!\n");
    
    return 0;
}