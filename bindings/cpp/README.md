# C++ Bindings

The HOCDB C++ bindings provide a modern, RAII-compliant wrapper around the C API.

## Building

The C++ bindings use the same shared library as the C bindings.

```bash
# Build the shared library and headers
zig build c-bindings
```

This generates:
- `zig-out/lib/libhocdb_c.dylib` (or .so/.dll)
- `zig-out/include/hocdb.h` (C header)
- `zig-out/include/hocdb_cpp.h` (C++ header)

## Usage

Include the header and link against the library:

```cpp
#include "hocdb_cpp.h"
#include <vector>

int main() {
    // Define schema
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };

    // Initialize DB
    hocdb::Database db("TICKER", "data_dir", schema);

    // Append data (using a struct matching the schema)
    struct Trade { int64_t ts; double price; double vol; };
    db.append(Trade{100, 50000.0, 1.5});

    // Flush to disk
    db.flush();

    // Get Stats (using field name)
    auto stats = db.getStats(0, 200, "usd");
    // stats.min, stats.max, etc.

    // Get Latest Value (using field name)
    auto latest = db.getLatest("usd");
    // latest.first (value), latest.second (timestamp)

    // Load data (Zero-Copy with RAII)
    auto data = hocdb::load_with_raii<Trade>(db);
    
    // data is a std::span-like object (pointer + size)
    // Memory is automatically freed when 'data' goes out of scope
    
    return 0;
}
```

## Compiling

```bash
g++ -std=c++17 -Izig-out/include -Lzig-out/lib -lhocdb_c -o my_app main.cpp
```

## Running Tests

```bash
# Compile and run tests
zig c++ -o cpp_test bindings/cpp/test.cpp -I zig-out/include -L zig-out/lib -lhocdb_c -std=c++17
./cpp_test
```
