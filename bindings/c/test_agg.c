#include "hocdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

typedef struct {
    int64_t timestamp;
    double value;
} TradeData;

int main() {
    printf("Testing C aggregation bindings...\n");
    
    system("rm -rf b_c_test_data/agg");
    system("mkdir -p b_c_test_data/agg");
    
    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"value", HOCDB_TYPE_F64}
    };
    
    HOCDBHandle db = hocdb_init("TEST_C_AGG", "b_c_test_data/agg", schema, 2, 1024*1024, 1, 1);
    if (!db) return 1;
    
    printf("Appending data...\n");
    TradeData d1 = {100, 10.0};
    TradeData d2 = {200, 20.0};
    TradeData d3 = {300, 30.0};
    
    hocdb_append(db, &d1, sizeof(TradeData));
    hocdb_append(db, &d2, sizeof(TradeData));
    hocdb_append(db, &d3, sizeof(TradeData));
    hocdb_flush(db);
    
    printf("Testing getLatest...\n");
    double val;
    int64_t ts;
    if (hocdb_get_latest(db, 1, &val, &ts) != 0) {
        printf("getLatest failed\n");
        return 1;
    }
    printf("Latest: value=%.2f, timestamp=%ld\n", val, (long)ts);
    assert(val == 30.0);
    assert(ts == 300);
    
    printf("Testing getStats...\n");
    HOCDBStats stats;
    if (hocdb_get_stats(db, 0, 400, 1, &stats) != 0) {
        printf("getStats failed\n");
        return 1;
    }
    printf("Stats: min=%.2f, max=%.2f, sum=%.2f, count=%lu, mean=%.2f\n", 
           stats.min, stats.max, stats.sum, (unsigned long)stats.count, stats.mean);
           
    assert(stats.count == 3);
    assert(stats.min == 10.0);
    assert(stats.max == 30.0);
    assert(stats.sum == 60.0);
    assert(stats.mean == 20.0);
    
    hocdb_close(db);
    printf("C Aggregation Test Passed!\n");
    return 0;
}
