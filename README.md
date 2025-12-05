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

---

## Usage Examples

### ⚡ Zig
Direct usage of the core library.

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
    try fields.append(.{ .name = "volume", .type = .f64 });

    const schema = hocdb.Schema{ .fields = fields.items };

    // Initialize DB
    var db = try hocdb.DynamicTimeSeriesDB.init("BTC_USD", "data", allocator, schema, .{});
    defer db.deinit();

    // Append Data
    try db.append(.{
        .timestamp = 1620000000,
        .price = 50000.0,
        .volume = 1.5,
    });
    try db.flush();

    // Query Range (Zero-Copy)
    const results = try db.query(1620000000, 1620000100, allocator);
    defer allocator.free(results); // Frees the slice, data is zero-copy mapped

    // Aggregation (Native Speed)
    // Calculate stats for 'price' (index 1)
    const stats = try db.getStats(1620000000, 1620000100, 1); 
    std.debug.print("Min: {d}, Max: {d}, Mean: {d}\n", .{ stats.min, stats.max, stats.mean });

    // Get Latest Value
    const latest = try db.getLatest(1);
    std.debug.print("Latest Price: {d} @ {d}\n", .{ latest.value, latest.timestamp });
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
    Field("volume", Type.F64)
]

# Initialize
db = HOCDB("BTC_USD", "data", schema)

# Append
db.append({"timestamp": 1620000000, "price": 50000.0, "volume": 1.5})

# Load (Zero-Copy)
data = db.load()
print(f"Loaded {len(data)} records")

# Query Range
results = db.query(1620000000, 1620000100)

# Aggregation
stats = db.get_stats(1620000000, 1620000100, 1) # Field index 1 (price)
print(f"Min: {stats.min}, Max: {stats.max}, Mean: {stats.mean}")

latest = db.get_latest(1)
print(f"Latest: {latest.value}")
```

### 🚀 Node.js
N-API bindings for maximum performance.

```bash
cd bindings/node && npm install
```

```javascript
const hocdb = require('./bindings/node');
const db = hocdb.dbInit("BTC_USD", "data", {
    max_file_size: 1024 * 1024 * 1024,
    flush_on_write: false
});

hocdb.dbAppend(db, 1620000000, 50000.0, 1.5);
const buffer = hocdb.dbLoad(db); // Zero-Copy ArrayBuffer

// Query Range
const results = db.query(1620000000n, 1620000100n);

// Aggregation
const stats = db.getStats(1620000000n, 1620000100n, 1);
console.log(`Min: ${stats.min}, Max: ${stats.max}`);

const latest = db.getLatest(1);
console.log(`Latest: ${latest.value}`);
```

### 🥟 Bun
Native FFI bindings for Bun.

```typescript
import { HOCDB } from "./bindings/bun/index.ts";

const db = new HOCDB("BTC_USD", "./data", schema);
db.append(1620000000, 50000.0, 1.5);

// Query Range
const results = db.query(1620000000n, 1620000100n);
// Aggregation
const stats = db.getStats(1620000000n, 1620000100n, 1);
console.log(stats);

const latest = db.getLatest(1);
console.log(latest);
```

### 🇨 C / C++
Direct access to the core engine.

```cpp
#include "hocdb_cpp.h"

int main() {
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64}
    };
    
    hocdb::Database db("BTC_USD", "data", schema);
    db.append(1620000000, 50000.0);
    
    // RAII Zero-Copy Load
    auto data = hocdb::load_with_raii<Trade>(db);
    // Query Range
    auto query_data = hocdb::query_with_raii<Trade>(db, 1620000000, 1620000100);
    // Aggregation
    auto stats = db.getStats(1620000000, 1620000100, 1); // Index 1
    std::cout << "Min: " << stats.min << std::endl;

    auto [val, ts] = db.getLatest(1);
    std::cout << "Latest: " << val << std::endl;
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
    }

    db, _ := hocdb.New("BTC_USD", "data", schema, hocdb.Options{})
    defer db.Close()

    // Append
    record, _ := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0)
    db.Append(record)

    // Query Range
    data, _ := db.Query(1620000000, 1620000100)
    fmt.Printf("Queried %d bytes\n", len(data))

    // Aggregation
    stats, _ := db.GetStats(1620000000, 1620000100, 1)
    fmt.Printf("Min: %f\n", stats.Min)
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