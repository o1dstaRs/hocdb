<p align="center">
  <img src="assets/hocdb_trademark_256.png" alt="HOCDB Logo" width="256">
</p>

# HOCDB: Universal High-Performance Time-Series Database

<p align="center">
  <a href="https://github.com/o1dstaRs/hocdb/actions/workflows/test.yml">
    <img src="https://github.com/o1dstaRs/hocdb/actions/workflows/test.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://ziglang.org/download/">
    <img src="https://img.shields.io/badge/Zig-0.15.2-orange.svg?logo=zig&logoColor=white" alt="Zig Version">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License">
  </a>
  <img src="https://img.shields.io/badge/platform-linux%20%7C%20macos-lightgrey" alt="Platform">
</p>

> **The World's Most Performant Time-Series Database.**
> *Built for speed. Built for scale. Built for victory.*

HOCDB is a high-performance time-series database engine. While originally built for the **hyper-specialized** needs of high-frequency crypto trading (extreme throughput, low latency), its flexible schema system and cross-language bindings make it a **universal** solution for any time-series workload requiring maximum efficiency.

**Supported Languages:**
*   **Zig** (Native)
*   **C**
*   **C++**
*   **Python**
*   **Node.js**
*   **Bun**
*   **Go**

---

## Performance
HOCDB is designed to be the fastest database you will ever use.

| Metric | Performance |
| :--- | :--- |
| **Write Throughput** | **~18,500,000 ops/sec** (Buffered) |
| **Read Throughput** | **~535,000,000 ops/sec** (Full Load) |
| **Aggregation Speed** | **~437,000,000 records/sec** |
| **Write Latency (p99)** | **42 nanoseconds** |
| **Bandwidth** | **~330 MB/sec** (Write) / **~12 GB/sec** (Read) |

*Benchmarks run on Apple Silicon (M-series).*

### Running Benchmarks
To reproduce these benchmarks on your machine:
```bash
# Run the full benchmark suite (Write, Read, Aggregation)
zig build bench -Doptimize=ReleaseFast
```

---

## Architecture & Design
Why is HOCDB so fast?

### 1. Fixed-Size Records
Unlike JSON or CSV, HOCDB uses a binary format with fixed-size records. This allows **O(1) random access** to any record by index, eliminating parsing overhead and enabling instant lookups.

### 2. Append-Only Log
Data is written sequentially to the end of the file. This maximizes disk I/O bandwidth by avoiding random seeks during writes, making it ideal for high-throughput data ingestion.

### 3. Zero-Serialization
Data is laid out in memory exactly as it is on disk. Reading data involves simply mapping or reading bytes directly into a struct, with **zero CPU cycles spent on deserialization**.

### 4. Ring Buffer
HOCDB supports an optional circular buffer mode. When the file reaches its size limit, it automatically wraps around and overwrites the oldest data, ensuring constant disk usage without manual maintenance.

### 5. Zig Core
The core engine is written in **Zig**, providing manual memory control, no garbage collection pauses, and direct access to SIMD intrinsics for aggregation.

---

---

## Schema & Data Types
HOCDB uses a dynamic schema system defined at runtime. You must define your schema when initializing the database.

### Supported Types
| Type | Description | Size |
| :--- | :--- | :--- |
| `i64` | Signed 64-bit integer | 8 bytes |
| `f64` | 64-bit floating point | 8 bytes |
| `u64` | Unsigned 64-bit integer | 8 bytes |
| `bool` | Boolean | 1 byte |
| `string` | Fixed-length string | 128 bytes |

### ⚠️ Requirements
*   **Timestamp Field**: Every schema **MUST** contain a field named `timestamp` of type `i64`. This is used for indexing, binary search, and time-range queries.
*   **Field Order**: The order of fields in your schema definition must match the order in your struct/binary layout.

---

## Getting Started

### Prerequisites
*   **Zig 0.15.2** (for building the core)
*   Your language of choice (C, C++, Python, Node, Bun, Go)

### Build & Test
```bash
# Run all core tests (with summary)
zig build test --summary all

# Run full verification suite (Core + All Bindings)
./verify_all.sh

# Run benchmarks
zig build bench -- -Doptimize=ReleaseFast
```

### Build Core & Bindings
```bash
# Build everything (Core, C/C++ libs, Python bindings, Go bindings)
zig build
zig build c-bindings
zig build python-bindings
zig build go-bindings
```

### Installation as Zig Package
You can use HOCDB as a standard Zig library in your own project.

1. **Add Dependency**:
   ```bash
   zig fetch --save https://github.com/o1dstaRs/hocdb/archive/refs/heads/main.tar.gz
   # OR for local development:
   # zig fetch --save ../path/to/hocdb
   ```

2. **Configure `build.zig`**:
   ```zig
   pub fn build(b: *std.Build) void {
       const target = b.standardTargetOptions(.{});
       const optimize = b.standardOptimizeOption(.{});

       const hocdb_dep = b.dependency("hocdb", .{
           .target = target,
           .optimize = optimize,
       });

       const exe = b.addExecutable(.{
           .name = "my-app",
           .root_source_file = b.path("src/main.zig"),
           .target = target,
           .optimize = optimize,
       });

       exe.root_module.addImport("hocdb", hocdb_dep.module("hocdb"));
       b.installArtifact(exe);
   }
   ```

---

## Usage Examples

### ⚡ Zig
Usage as a library (imported via `build.zig`).

```zig
const std = @import("std");
const hocdb = @import("hocdb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // Define Schema
    var fields = std.ArrayList(hocdb.FieldInfo).init(allocator);
    defer fields.deinit();
    try fields.append(.{ .name = "timestamp", .type = .i64 });
    try fields.append(.{ .name = "price", .type = .f64 });
    try fields.append(.{ .name = "active", .type = .bool });
    try fields.append(.{ .name = "ticker", .type = .string });

    const schema = hocdb.Schema{ .fields = fields.items };

    // Initialize DB
    var db = try hocdb.DynamicTimeSeriesDB.init("BTC_USD", "data", allocator, schema, .{});
    // defer db.deinit(); // Use drop() to delete, or deinit() to just close

    // Append Data
    var ticker_buf: [128]u8 = undefined;
    @memset(&ticker_buf, 0);
    std.mem.copyForwards(u8, &ticker_buf, "BTC");

    try db.append(.{
        .timestamp = 1620000000,
        .price = 50000.0,
        .active = true,
        .ticker = ticker_buf,
    });
    try db.flush();

    // Query with Filter
    var filters = std.ArrayList(hocdb.Filter).init(allocator);
    defer filters.deinit();
    
    var filter_val_str: [128]u8 = undefined;
    @memset(&filter_val_str, 0);
    std.mem.copyForwards(u8, &filter_val_str, "BTC");
    
    try filters.append(.{
        .field_index = 3, // Index of 'ticker' field
        .value = .{ .string = filter_val_str }
    });

    const results = try db.query(1620000000, 1620000100, filters.items, allocator);
    defer allocator.free(results);

    // Aggregation
    const stats = try db.getStats(1620000000, 1620000100, 1); 
    std.debug.print("Min: {d}, Max: {d}\n", .{ stats.min, stats.max });

    // Drop Database (Close & Delete)
    try db.drop();
}
```

### 🐍 Python
High-performance Python bindings using `ctypes`.

```bash
# Build bindings
zig build python-bindings
```

```python
from bindings.python.hocdb import HOCDB, Field, Type

# Define Schema
schema = [
    Field("timestamp", Type.I64),
    Field("price", Type.F64),
    Field("active", Type.Bool),
    Field("ticker", Type.String)
]

# Initialize
db = HOCDB("BTC_USD", "data", schema)

# Append
db.append({
    "timestamp": 1620000000, 
    "price": 50000.0, 
    "active": True, 
    "ticker": "BTC"
})

# Query with Filter
filters = {"ticker": "BTC"}
results = db.query(1620000000, 1620000100, filters)

# Aggregation
stats = db.get_stats(1620000000, 1620000100, 1)
print(f"Min: {stats.min}, Max: {stats.max}")

# Drop
db.drop()
```

### 🚀 Node.js
N-API bindings for maximum performance.

```bash
cd bindings/node && npm install
```

```javascript
const hocdb = require('./bindings/node');

// Async API (Recommended)
async function run() {
    const db = await hocdb.dbInitAsync("BTC_USD", "data", [
        { name: "timestamp", type: "i64" },
        { name: "price", type: "f64" },
        { name: "active", type: "bool" },
        { name: "ticker", type: "string" }
    ]);

    await db.append({
        timestamp: 1620000000n,
        price: 50000.0,
        active: true,
        ticker: "BTC"
    });

    // Query with Filter
    const results = await db.query(1620000000n, 1620000100n, { ticker: "BTC" });

    // Aggregation
    const stats = await db.getStats(1620000000n, 1620000100n, 1);
    console.log(`Min: ${stats.min}, Max: ${stats.max}`);

    // Drop
    await db.drop();
}

run();
```

### 🥟 Bun
Native FFI bindings for Bun.

```typescript
import { HOCDBAsync } from "./bindings/bun/index.ts";

const db = new HOCDBAsync("BTC_USD", "./data", [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "active", type: "bool" },
    { name: "ticker", type: "string" }
]);

await db.append({
    timestamp: 1620000000n,
    price: 50000.0,
    active: true,
    ticker: "BTC"
});

// Query with Filter
const results = await db.query(1620000000n, 1620000100n, { ticker: "BTC" });

// Aggregation
const stats = await db.getStats(1620000000n, 1620000100n, 1);
console.log(stats);

// Drop
await db.drop();
```

### 🇨 C / C++
Direct access to the core engine.

```cpp
#include "hocdb_cpp.h"

int main() {
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"active", HOCDB_TYPE_BOOL},
        {"ticker", HOCDB_TYPE_STRING}
    };
    
    hocdb::Database db("BTC_USD", "data", schema);
    
    // Append (using raw bytes or helper struct)
    // ... (append logic depends on struct layout)

    // Query with Filter
    std::map<std::string, hocdb::FilterValue> filters;
    filters["ticker"] = "BTC";
    
    auto query_data = hocdb::query_with_raii<Trade>(db, 1620000000, 1620000100, filters);

    // Drop
    db.drop();
}
```

### 🐹 Go
Idiomatic Go bindings using CGO.

```bash
# Build bindings
zig build go-bindings
```

```go
package main

import (
    "fmt"
    "hocdb"
)

func main() {
    schema := []hocdb.Field{
        {Name: "timestamp", Type: hocdb.TypeI64},
        {Name: "price", Type: hocdb.TypeF64},
        {Name: "active", Type: hocdb.TypeBool},
        {Name: "ticker", Type.TypeString},
    }

    db, _ := hocdb.New("BTC_USD", "data", schema, hocdb.Options{})
    
    // Append
    record, _ := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, true, "BTC")
    db.Append(record)

    // Query with Filter
    filters := map[string]interface{}{
        "ticker": "BTC",
    }
    data, _ := db.Query(1620000000, 1620000100, filters)
    fmt.Printf("Queried %d bytes\n", len(data))

    // Drop
    db.Drop()
}
```

---

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_file_size` | Number | 2GB | Maximum size of the data file in bytes. |
| `overwrite_on_full` | Boolean | `true` | Whether to overwrite old data when the file is full (Ring Buffer). |
| `flush_on_write` | Boolean | `false` | Whether to flush to disk after every write. Ensures durability but reduces performance. |
| `auto_increment` | Boolean | `false` | Automatically assign monotonically increasing timestamps to new records. Overwrites the timestamp field. |

---

*Built with ❤️ and ⚡ by the Heroes of Crypto AI Team.*