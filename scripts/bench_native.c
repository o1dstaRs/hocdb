/* Fixed-work benchmark shared by old and new libraries. Assertions also
 * validate every workload; compile without NDEBUG. Emits one JSON object. */
#define _POSIX_C_SOURCE 200809L
#include "hocdb.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <time.h>

typedef struct {
    int64_t timestamp;
    double open, high, low, close, volume;
} Record;

static double seconds(void) {
    struct timespec t;
    assert(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
    return t.tv_sec + t.tv_nsec * 1e-9;
}

static Record record_at(int64_t i) {
    double p = 100.0 + i * 0.00001 + (i % 100) * 0.01;
    return (Record){i, p, p + 1, p - 1, p, 10 + i % 100};
}

int main(int argc, char **argv) {
    assert(argc == 2);
    const size_t n = 1000000;
    CField schema[] = {{"timestamp", 1}, {"open", 2}, {"high", 2},
                       {"low", 2}, {"close", 2}, {"volume", 2}};
    HOCDBHandle db = hocdb_init("FIXED", argv[1], schema, 6,
                               64 + (int64_t)n * sizeof(Record), 0, 0, 0);
    assert(db);
    double started = seconds();
    for (size_t i = 0; i < n; i++) {
        Record r = record_at((int64_t)i);
        assert(hocdb_append(db, &r, sizeof(r)) == 0);
    }
    assert(hocdb_flush(db) == 0);
    double write = n / (seconds() - started);

    uint64_t checksum = 0;
    const size_t load_reps = 100;
    started = seconds();
    for (size_t i = 0; i < load_reps; i++) {
        size_t bytes = 0;
        Record *data = hocdb_load(db, &bytes);
        assert(data && bytes == n * sizeof(Record));
        assert(data[0].timestamp == 0 && data[n - 1].timestamp == (int64_t)n - 1);
        checksum += (uint64_t)data[n - 1].timestamp;
        hocdb_free(data);
    }
    double load = n * load_reps / (seconds() - started);

    const size_t queries = 100000;
    uint32_t rng = 42;
    started = seconds();
    for (size_t i = 0; i < queries; i++) {
        rng = rng * 1664525u + 1013904223u;
        int64_t first = rng % (n - 1000);
        size_t bytes = 0;
        Record *data = hocdb_query(db, first, first + 200, NULL, 0, &bytes);
        assert(data && bytes == 200 * sizeof(Record));
        assert(data[0].timestamp == first && data[199].timestamp == first + 199);
        checksum += (uint64_t)data[199].timestamp;
        hocdb_free(data);
    }
    double query = queries / (seconds() - started);

    const size_t filtered_queries = 10000;
    started = seconds();
    for (size_t i = 0; i < filtered_queries; i++) {
        rng = rng * 1664525u + 1013904223u;
        int64_t first = rng % (n - 1000);
        HOCDBFilter filter = {.field_index = 0, .type = HOCDB_TYPE_I64, .val_i64 = first + 100};
        size_t bytes = 0;
        Record *data = hocdb_query(db, first, first + 1000, &filter, 1, &bytes);
        assert(data && bytes == sizeof(Record) && data[0].timestamp == first + 100);
        checksum += (uint64_t)data[0].timestamp;
        hocdb_free(data);
    }
    double filtered = filtered_queries / (seconds() - started);

    HOCDBStats stats;
    started = seconds();
    for (size_t i = 0; i < 100; i++) {
        assert(hocdb_get_stats(db, 0, (int64_t)n, 4, 0, &stats) == 0);
        assert(stats.count == n && stats.min == 100.0 && stats.max > stats.min);
    }
    double aggregate = n * 100 / (seconds() - started);

    HOCDBIndicatorColumns cols = {1, 2, 3, 4, 5, -1, -1, -1};
    HOCDBIndicatorSpec specs[] = {
        {.kind = HOCDB_IND_SMA, .period = 20, .field_index = -1, .field_index2 = -1},
        {.kind = HOCDB_IND_EMA, .period = 20, .field_index = -1, .field_index2 = -1},
        {.kind = HOCDB_IND_RSI, .period = 14, .field_index = -1, .field_index2 = -1},
    };
    started = seconds();
    for (size_t i = 0; i < 10; i++) {
        HOCDBIndicatorResult result;
        assert(hocdb_indicators(db, 0, (int64_t)n, &cols, specs, 3, 0, 0, &result) == 0);
        assert(result.n_rows == n && result.n_outputs == 3);
        assert(result.values[n - 1] > 100);
        hocdb_indicators_free(&result);
    }
    double indicators = n * 10 / (seconds() - started);

    HOCDBSnapshot snapshot;
    started = seconds();
    for (size_t i = 0; i < 1000; i++) {
        assert(hocdb_snapshot(db, &cols, 2500, 0, 252, &snapshot) == 0);
        assert(snapshot.bars == 2500 && snapshot.timestamp == (int64_t)n - 1);
    }
    double snapshots = 1000 / (seconds() - started);
    hocdb_close(db);

    db = hocdb_init("FLUSH", argv[1], schema, 6, 0, 0, 1, 0);
    assert(db);
    started = seconds();
    for (size_t i = 0; i < 50000; i++) {
        Record r = record_at((int64_t)i);
        assert(hocdb_append(db, &r, sizeof(r)) == 0);
    }
    double flush = 50000 / (seconds() - started);
    hocdb_close(db);

    db = hocdb_init("AUTO", argv[1], schema, 6, 64 + (int64_t)n * sizeof(Record), 0, 0, 1);
    assert(db);
    started = seconds();
    for (size_t i = 0; i < n; i++) {
        Record r = record_at((int64_t)i);
        assert(hocdb_append(db, &r, sizeof(r)) == 0);
    }
    assert(hocdb_flush(db) == 0);
    double auto_write = n / (seconds() - started);
    double latest;
    int64_t timestamp;
    assert(hocdb_get_latest(db, 4, &latest, &timestamp) == 0);
    assert(timestamp == (int64_t)n && latest == record_at((int64_t)n - 1).close);
    assert(hocdb_verify(db) == 1);
    hocdb_close(db);

    const size_t capacity = 65535;
    db = hocdb_init("RING", argv[1], schema, 6, 64 + (int64_t)capacity * sizeof(Record), 1, 0, 0);
    assert(db);
    for (size_t i = 0; i < n; i++) {
        Record r = record_at((int64_t)i);
        assert(hocdb_append(db, &r, sizeof(r)) == 0);
    }
    assert(hocdb_flush(db) == 0);
    started = seconds();
    for (size_t i = 0; i < queries; i++) {
        rng = rng * 1664525u + 1013904223u;
        int64_t first = n - capacity + rng % (capacity - 1000);
        size_t bytes = 0;
        Record *data = hocdb_query(db, first, first + 200, NULL, 0, &bytes);
        assert(data && bytes == 200 * sizeof(Record));
        assert(data[0].timestamp == first && data[199].timestamp == first + 199);
        checksum += (uint64_t)data[199].timestamp;
        hocdb_free(data);
    }
    double ring_query = queries / (seconds() - started);
    hocdb_close(db);

    printf("{\"write_records_s\":%.3f,\"load_records_s\":%.3f,"
           "\"queries_s\":%.3f,\"filtered_queries_s\":%.3f,\"ring_queries_s\":%.3f,\"aggregate_records_s\":%.3f,"
           "\"indicator_rows_s\":%.3f,\"snapshots_s\":%.3f,"
           "\"flush_writes_s\":%.3f,\"auto_write_records_s\":%.3f,\"checksum\":%" PRIu64 "}\n",
           write, load, query, filtered, ring_query, aggregate, indicators, snapshots, flush, auto_write, checksum);
    return 0;
}
