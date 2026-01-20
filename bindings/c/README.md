# HOCDB C Bindings

C bindings for HOCDB - The World's Most Performant Time-Series Database.

## Building

```bash
# Build the C bindings (shared library and headers)
zig build c-bindings
```

This creates:
- Shared library: `zig-out/lib/libhocdb_c.dylib` (macOS) / `libhocdb_c.so` (Linux) / `libhocdb_c.dll` (Windows)
- Headers: `zig-out/include/hocdb.h` and `zig-out/include/hocdb_cpp.h`

## Quick Start

```c
#include "hocdb.h"
#include <stdio.h>
#include <string.h>

int main() {
    // Define schema
    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64},
        {"active", HOCDB_TYPE_BOOL}
    };

    // Initialize database
    HOCDBHandle db = hocdb_init(
        "BTC_USD",           // ticker
        "data",              // path
        schema,              // schema array
        4,                   // schema length
        0,                   // max_file_size (0 = default)
        0,                   // overwrite_on_full (0 = false)
        0,                   // flush_on_write (0 = false)
        0                    // auto_increment (0 = false)
    );

    if (!db) {
        fprintf(stderr, "Failed to initialize database\n");
        return 1;
    }

    // Create and append a record
    struct __attribute__((packed)) {
        int64_t timestamp;
        double price;
        double volume;
        bool active;
    } record = {1620000000, 50000.0, 1.5, true};

    hocdb_append(db, &record, sizeof(record));
    hocdb_flush(db);

    // Get statistics
    HOCDBStats stats;
    hocdb_get_stats(db, 1620000000, 1620000100, 1, 0, &stats);
    printf("Price: min=%.2f, max=%.2f, mean=%.2f\n", stats.min, stats.max, stats.mean);

    // Get latest value
    double latest_val;
    int64_t latest_ts;
    hocdb_get_latest(db, 1, &latest_val, &latest_ts);
    printf("Latest price: %.2f at %lld\n", latest_val, latest_ts);

    // Close
    hocdb_close(db);
    return 0;
}
```

## Compiling

```bash
# macOS
gcc -Izig-out/include -Lzig-out/lib -lhocdb_c -o my_app my_app.c

# Linux
gcc -Izig-out/include -Lzig-out/lib -Wl,-rpath,./zig-out/lib -lhocdb_c -o my_app my_app.c
```

## API Reference

### Field Types

```c
#define HOCDB_TYPE_I64    1   // Signed 64-bit integer (8 bytes)
#define HOCDB_TYPE_F64    2   // 64-bit floating point (8 bytes)
#define HOCDB_TYPE_U64    3   // Unsigned 64-bit integer (8 bytes)
#define HOCDB_TYPE_STRING 5   // Fixed 128-byte string
#define HOCDB_TYPE_BOOL   6   // Boolean (1 byte)
```

### Data Structures

```c
// Schema field definition
typedef struct {
    const char *name;
    int type;
} CField;

// Database handle
typedef void *HOCDBHandle;

// Filter for queries
typedef struct {
    size_t field_index;
    int type;
    int64_t val_i64;
    double val_f64;
    uint64_t val_u64;
    char val_string[128];
    bool val_bool;
} HOCDBFilter;

// Statistics result
typedef struct {
    double min;
    double max;
    double sum;
    uint64_t count;
    double mean;
    double p50;    // 50th percentile (if requested)
    double p90;    // 90th percentile (if requested)
    double p95;    // 95th percentile (if requested)
    double p99;    // 99th percentile (if requested)
} HOCDBStats;

// Flag for percentile computation
#define HOCDB_STATS_PERCENTILES 1
```

---

### `hocdb_init`

Initialize the database with a dynamic schema.

```c
HOCDBHandle hocdb_init(
    const char *ticker,        // Ticker symbol / database name
    const char *path,          // Directory path for data files
    const CField *schema,      // Array of field definitions
    size_t schema_len,         // Number of fields
    int64_t max_file_size,     // Max file size (0 for default)
    int overwrite_on_full,     // Ring buffer mode (1 = true)
    int flush_on_write,        // Flush on every write (1 = true)
    int auto_increment         // Auto-increment timestamp (1 = true)
);
```

**Returns:** Database handle, or `NULL` on failure.

**Example:**
```c
CField schema[] = {
    {"timestamp", HOCDB_TYPE_I64},
    {"price", HOCDB_TYPE_F64},
    {"volume", HOCDB_TYPE_F64}
};

// Basic initialization
HOCDBHandle db = hocdb_init("BTC_USD", "data", schema, 3, 0, 0, 0, 0);

// With ring buffer (100MB, overwrite when full)
HOCDBHandle db = hocdb_init("BTC_USD", "data", schema, 3,
                            100*1024*1024, 1, 0, 0);
```

---

### `hocdb_append`

Append a raw record to the database.

```c
int hocdb_append(HOCDBHandle handle, const void *data, size_t len);
```

**Parameters:**
- `handle`: Database handle
- `data`: Pointer to packed record data
- `len`: Size of record in bytes

**Returns:** `0` on success, `-2` for invalid record size, `-3` for non-monotonic timestamp.

**Example:**
```c
// Define a packed struct matching the schema
struct __attribute__((packed)) Trade {
    int64_t timestamp;
    double price;
    double volume;
};

struct Trade trade = {1620000000, 50000.0, 1.5};
int result = hocdb_append(db, &trade, sizeof(trade));
if (result != 0) {
    fprintf(stderr, "Append failed: %d\n", result);
}
```

---

### `hocdb_flush`

Force buffered data to be written to disk.

```c
int hocdb_flush(HOCDBHandle handle);
```

**Returns:** `0` on success, non-zero on failure.

---

### `hocdb_load`

Load all records from the database.

```c
void *hocdb_load(HOCDBHandle handle, size_t *out_len);
```

**Parameters:**
- `handle`: Database handle
- `out_len`: Output parameter for total bytes loaded

**Returns:** Pointer to data (must be freed with `hocdb_free`), or `NULL` on failure.

**Example:**
```c
size_t len;
void *data = hocdb_load(db, &len);
if (data) {
    size_t record_size = sizeof(struct Trade);
    size_t count = len / record_size;
    struct Trade *trades = (struct Trade *)data;

    for (size_t i = 0; i < count; i++) {
        printf("Trade %zu: price=%.2f\n", i, trades[i].price);
    }

    hocdb_free(data);  // IMPORTANT: Free the memory
}
```

---

### `hocdb_query`

Query records within a timestamp range with optional filters.

```c
void *hocdb_query(
    HOCDBHandle handle,
    int64_t start_ts,           // Start timestamp (inclusive)
    int64_t end_ts,             // End timestamp (exclusive)
    const HOCDBFilter *filters, // Array of filters (can be NULL)
    size_t filters_len,         // Number of filters
    size_t *out_len             // Output: bytes returned
);
```

**Returns:** Pointer to data (must be freed with `hocdb_free`), or `NULL` on failure/empty.

**Example:**
```c
// Query without filters
size_t len;
void *data = hocdb_query(db, 1620000000, 1620001000, NULL, 0, &len);

// Query with filter
HOCDBFilter filter = {
    .field_index = 1,        // 'price' field
    .type = HOCDB_TYPE_F64,
    .val_f64 = 50000.0
};
void *data = hocdb_query(db, 1620000000, 1620001000, &filter, 1, &len);

if (data) {
    // Process data...
    hocdb_free(data);
}
```

---

### `hocdb_get_stats`

Get statistics for a specific field within a time range.

```c
int hocdb_get_stats(
    HOCDBHandle handle,
    int64_t start_ts,
    int64_t end_ts,
    size_t field_index,
    uint32_t flags,           // Use HOCDB_STATS_PERCENTILES for percentiles
    HOCDBStats *out_stats
);
```

**Returns:** `0` on success, non-zero on failure.

**Example:**
```c
HOCDBStats stats;

// Basic stats
hocdb_get_stats(db, 1620000000, 1620001000, 1, 0, &stats);
printf("Price: min=%.2f, max=%.2f, mean=%.2f, count=%lu\n",
       stats.min, stats.max, stats.mean, stats.count);

// With percentiles
hocdb_get_stats(db, 1620000000, 1620001000, 1, HOCDB_STATS_PERCENTILES, &stats);
printf("P50=%.2f, P99=%.2f\n", stats.p50, stats.p99);
```

---

### `hocdb_get_latest`

Get the most recent value and timestamp for a specific field.

```c
int hocdb_get_latest(
    HOCDBHandle handle,
    size_t field_index,
    double *out_val,
    int64_t *out_ts
);
```

**Returns:** `0` on success, non-zero on failure.

**Example:**
```c
double value;
int64_t timestamp;
if (hocdb_get_latest(db, 1, &value, &timestamp) == 0) {
    printf("Latest price: %.2f at %lld\n", value, timestamp);
}
```

---

### `hocdb_get_field_index`

Get the index of a field by name.

```c
int64_t hocdb_get_field_index(HOCDBHandle handle, const char *field_name);
```

**Returns:** Field index, or `-1` if not found.

**Example:**
```c
int64_t price_idx = hocdb_get_field_index(db, "price");
if (price_idx >= 0) {
    // Use price_idx for queries
}
```

---

### `hocdb_free`

Free memory allocated by `hocdb_load` or `hocdb_query`.

```c
void hocdb_free(void *ptr);
```

**IMPORTANT:** Always call this to free memory returned by `hocdb_load` and `hocdb_query`.

---

### `hocdb_close`

Close the database and release resources.

```c
void hocdb_close(HOCDBHandle handle);
```

---

### `hocdb_drop`

Close the database and delete all data files.

```c
void hocdb_drop(HOCDBHandle handle);
```

**WARNING:** This permanently deletes all data.

---

## Complete Example

```c
#include "hocdb.h"
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

// Define record structure matching schema
struct __attribute__((packed)) Trade {
    int64_t timestamp;
    double price;
    double volume;
    bool is_buy;
};

int main() {
    // Define schema
    CField schema[] = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64},
        {"is_buy", HOCDB_TYPE_BOOL}
    };

    // Initialize with ring buffer
    HOCDBHandle db = hocdb_init(
        "ETH_USD", "market_data",
        schema, 4,
        100*1024*1024,  // 100MB
        1,              // overwrite_on_full
        0, 0
    );

    if (!db) {
        fprintf(stderr, "Failed to initialize\n");
        return 1;
    }

    // Append some trades
    struct Trade trades[] = {
        {1620000000, 2500.0, 10.0, true},
        {1620000001, 2501.5, 5.0, false},
        {1620000002, 2502.0, 15.0, true}
    };

    for (int i = 0; i < 3; i++) {
        if (hocdb_append(db, &trades[i], sizeof(struct Trade)) != 0) {
            fprintf(stderr, "Append failed\n");
        }
    }
    hocdb_flush(db);

    // Query buy orders only
    HOCDBFilter filter = {
        .field_index = 3,          // is_buy field
        .type = HOCDB_TYPE_BOOL,
        .val_bool = true
    };

    size_t len;
    void *data = hocdb_query(db, 1620000000, 1620001000, &filter, 1, &len);
    if (data) {
        size_t count = len / sizeof(struct Trade);
        printf("Found %zu buy orders\n", count);
        hocdb_free(data);
    }

    // Get price statistics with percentiles
    HOCDBStats stats;
    hocdb_get_stats(db, 1620000000, 1620001000, 1, HOCDB_STATS_PERCENTILES, &stats);
    printf("Price: min=%.2f, max=%.2f, mean=%.2f, p99=%.2f\n",
           stats.min, stats.max, stats.mean, stats.p99);

    // Get latest price
    double latest_price;
    int64_t latest_ts;
    hocdb_get_latest(db, 1, &latest_price, &latest_ts);
    printf("Latest price: %.2f at %lld\n", latest_price, latest_ts);

    // Load all data
    data = hocdb_load(db, &len);
    if (data) {
        printf("Loaded %zu bytes (%zu records)\n",
               len, len / sizeof(struct Trade));
        hocdb_free(data);
    }

    // Clean up
    hocdb_close(db);
    // Or use hocdb_drop(db) to also delete data files

    return 0;
}
```

## Memory Management

**IMPORTANT:** The C API allocates memory for query results. You must:

1. Always call `hocdb_free()` on pointers returned by `hocdb_load()` and `hocdb_query()`
2. Do not use the returned pointer after calling `hocdb_free()`
3. Do not call `free()` directly - use `hocdb_free()` to ensure proper deallocation

## Testing

```bash
# Compile and run tests
gcc -Izig-out/include -Lzig-out/lib -lhocdb_c -o test_c bindings/c/test/simple_test.c
./test_c
```
