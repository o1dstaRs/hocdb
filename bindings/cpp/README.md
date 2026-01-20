# HOCDB C++ Bindings

Modern, RAII-compliant C++ bindings for HOCDB - The World's Most Performant Time-Series Database.

## Building

The C++ bindings use the same shared library as the C bindings.

```bash
# Build the shared library and headers
zig build c-bindings
```

This generates:
- `zig-out/lib/libhocdb_c.dylib` (macOS) / `.so` (Linux) / `.dll` (Windows)
- `zig-out/include/hocdb.h` (C header)
- `zig-out/include/hocdb_cpp.h` (C++ header)

## Quick Start

```cpp
#include "hocdb_cpp.h"
#include <iostream>
#include <vector>

int main() {
    // Define schema
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64},
        {"active", HOCDB_TYPE_BOOL}
    };

    // Initialize database
    hocdb::Database db("BTC_USD", "data", schema);

    // Define record struct (must match schema layout)
    struct __attribute__((packed)) Trade {
        int64_t timestamp;
        double price;
        double volume;
        bool active;
    };

    // Append records
    db.append(Trade{1620000000, 50000.0, 1.5, true});
    db.append(Trade{1620000001, 50100.0, 2.0, true});
    db.flush();

    // Get statistics
    auto stats = db.getStats(1620000000, 1620001000, "price");
    std::cout << "Price: min=" << stats.min << ", max=" << stats.max << std::endl;

    // Get latest value
    auto [value, timestamp] = db.getLatest("price");
    std::cout << "Latest: " << value << " at " << timestamp << std::endl;

    return 0;
}  // Database automatically closed by destructor
```

## Compiling

```bash
# C++17 or later required
g++ -std=c++17 -Izig-out/include -Lzig-out/lib -lhocdb_c -o my_app main.cpp

# On Linux, add rpath
g++ -std=c++17 -Izig-out/include -Lzig-out/lib -Wl,-rpath,./zig-out/lib -lhocdb_c -o my_app main.cpp
```

## API Reference

### Field Types

```cpp
HOCDB_TYPE_I64    // Signed 64-bit integer (8 bytes)
HOCDB_TYPE_F64    // 64-bit floating point (8 bytes)
HOCDB_TYPE_U64    // Unsigned 64-bit integer (8 bytes)
HOCDB_TYPE_STRING // Fixed 128-byte string
HOCDB_TYPE_BOOL   // Boolean (1 byte)
```

### Data Structures

```cpp
namespace hocdb {
    // Field definition for schema
    struct Field {
        std::string name;
        int type;  // HOCDB_TYPE_*
    };

    // Filter value variant (for map-based queries)
    using FilterValue = std::variant<int64_t, double, uint64_t, std::string, bool>;
}

// Statistics (from C API)
struct HOCDBStats {
    double min, max, sum;
    uint64_t count;
    double mean;
    double p50, p90, p95, p99;  // Percentiles (if requested)
};
```

---

### `hocdb::Database`

#### Constructor

```cpp
Database(
    const std::string& ticker,
    const std::string& path,
    const std::vector<Field>& schema,
    int64_t max_file_size = 0,      // 0 = default
    bool overwrite_on_full = true,   // Ring buffer mode
    bool flush_on_write = false,
    bool auto_increment = false
);
```

**Example:**
```cpp
std::vector<hocdb::Field> schema = {
    {"timestamp", HOCDB_TYPE_I64},
    {"price", HOCDB_TYPE_F64},
    {"volume", HOCDB_TYPE_F64}
};

// Basic initialization
hocdb::Database db("BTC_USD", "data", schema);

// With ring buffer (100MB, overwrite when full)
hocdb::Database db("BTC_USD", "data", schema, 100*1024*1024, true);

// With auto-increment timestamps
hocdb::Database db("BTC_USD", "data", schema, 0, true, false, true);
```

---

#### `append(const T& record)` (template)

Append a struct record to the database.

```cpp
struct __attribute__((packed)) Trade {
    int64_t timestamp;
    double price;
    double volume;
};

Trade trade{1620000000, 50000.0, 1.5};
db.append(trade);
```

#### `append(const void* data, size_t len)`

Append raw bytes to the database.

```cpp
db.append(&trade, sizeof(trade));
```

**Throws:** `hocdb::Exception` on failure (invalid size, non-monotonic timestamp)

---

#### `flush()`

Force buffered data to be written to disk.

```cpp
db.flush();
```

**Throws:** `hocdb::Exception` on failure

---

#### `load()`

Load all records from the database.

```cpp
std::vector<uint8_t> data = db.load();

// Parse records
size_t record_size = db.get_record_size();
size_t count = data.size() / record_size;

auto* trades = reinterpret_cast<Trade*>(data.data());
for (size_t i = 0; i < count; i++) {
    std::cout << "Price: " << trades[i].price << std::endl;
}
```

**Returns:** `std::vector<uint8_t>` containing raw record data

**Throws:** `hocdb::Exception` on failure

---

#### `query(start_ts, end_ts, filters)` (with HOCDBFilter)

Query records with low-level filter structs.

```cpp
std::vector<HOCDBFilter> filters;
HOCDBFilter f;
f.field_index = 1;
f.type = HOCDB_TYPE_F64;
f.val_f64 = 50000.0;
filters.push_back(f);

auto data = db.query(1620000000, 1620001000, filters);
```

#### `query(start_ts, end_ts, filters)` (with map)

Query records with convenient map-based filters.

```cpp
std::map<std::string, hocdb::FilterValue> filters = {
    {"price", 50000.0},
    {"active", true}
};

auto data = db.query(1620000000, 1620001000, filters);
```

**Returns:** `std::vector<uint8_t>` containing matching records

---

#### `getStats(start_ts, end_ts, field_index, compute_percentiles)`

Get statistics for a field by index.

```cpp
HOCDBStats stats = db.getStats(1620000000, 1620001000, 1, false);
std::cout << "Min: " << stats.min << ", Max: " << stats.max << std::endl;

// With percentiles
HOCDBStats stats = db.getStats(1620000000, 1620001000, 1, true);
std::cout << "P99: " << stats.p99 << std::endl;
```

#### `getStats(start_ts, end_ts, field_name, compute_percentiles)`

Get statistics for a field by name.

```cpp
HOCDBStats stats = db.getStats(1620000000, 1620001000, "price", true);
```

**Returns:** `HOCDBStats` struct

**Throws:** `hocdb::Exception` if field not found or operation fails

---

#### `getLatest(field_index)` / `getLatest(field_name)`

Get the most recent value and timestamp for a field.

```cpp
// By index
auto [value, timestamp] = db.getLatest(1);

// By name
auto [value, timestamp] = db.getLatest("price");

std::cout << "Latest: " << value << " at " << timestamp << std::endl;
```

**Returns:** `std::pair<double, int64_t>` (value, timestamp)

**Throws:** `hocdb::Exception` if field not found or operation fails

---

#### `close()`

Explicitly close the database (optional, destructor handles this).

```cpp
db.close();
```

---

#### `drop()`

Close the database and delete all data files.

```cpp
// WARNING: This permanently deletes all data!
db.drop();
```

---

#### `is_valid()`

Check if the database handle is valid.

```cpp
if (db.is_valid()) {
    // Database is open
}
```

---

#### `get_record_size()`

Get the size of a single record in bytes.

```cpp
size_t record_size = db.get_record_size();
```

---

## Exception Handling

The C++ bindings throw `hocdb::Exception` on errors:

```cpp
try {
    hocdb::Database db("BTC_USD", "data", schema);
    db.append(trade);
} catch (const hocdb::Exception& e) {
    std::cerr << "HOCDB error: " << e.what() << std::endl;
}
```

---

## Complete Example

```cpp
#include "hocdb_cpp.h"
#include <iostream>
#include <vector>
#include <map>

// Define record struct (packed to match schema exactly)
struct __attribute__((packed)) Trade {
    int64_t timestamp;
    double price;
    double volume;
    bool is_buy;
};

int main() {
    // Define schema
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64},
        {"is_buy", HOCDB_TYPE_BOOL}
    };

    try {
        // Initialize with ring buffer
        hocdb::Database db("ETH_USD", "market_data", schema,
                          100*1024*1024,  // 100MB
                          true);          // overwrite_on_full

        // Append trades
        db.append(Trade{1620000000, 2500.0, 10.0, true});
        db.append(Trade{1620000001, 2501.5, 5.0, false});
        db.append(Trade{1620000002, 2502.0, 15.0, true});
        db.flush();

        // Query buy orders only
        std::map<std::string, hocdb::FilterValue> filters = {
            {"is_buy", true}
        };
        auto buy_data = db.query(1620000000, 1620001000, filters);
        size_t buy_count = buy_data.size() / db.get_record_size();
        std::cout << "Buy orders: " << buy_count << std::endl;

        // Get price statistics with percentiles
        auto stats = db.getStats(1620000000, 1620001000, "price", true);
        std::cout << "Price: min=" << stats.min
                  << ", max=" << stats.max
                  << ", mean=" << stats.mean
                  << ", p99=" << stats.p99 << std::endl;

        // Get latest price
        auto [latest_price, latest_ts] = db.getLatest("price");
        std::cout << "Latest: " << latest_price << " at " << latest_ts << std::endl;

        // Load all data
        auto all_data = db.load();
        size_t total = all_data.size() / db.get_record_size();
        std::cout << "Total records: " << total << std::endl;

        // Iterate over records
        auto* trades = reinterpret_cast<Trade*>(all_data.data());
        for (size_t i = 0; i < total; i++) {
            std::cout << "  [" << i << "] price=" << trades[i].price
                      << ", is_buy=" << trades[i].is_buy << std::endl;
        }

    } catch (const hocdb::Exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
```

## RAII and Resource Management

- Database is automatically closed when the `Database` object goes out of scope
- Move semantics are supported (`Database(Database&&)`, `operator=(Database&&)`)
- Copy operations are deleted (each handle should be unique)
- Memory returned by `load()` and `query()` is managed as `std::vector`

## Testing

```bash
# Compile and run tests
g++ -std=c++17 -Izig-out/include -Lzig-out/lib -lhocdb_c -o cpp_test bindings/cpp/test/test.cpp
./cpp_test
```
