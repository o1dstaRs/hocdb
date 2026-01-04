<p align="center">
  <img src="assets/hocdb_trademark_256.png" alt="HOCDB Logo" width="256">
</p>

# HOCDB: Universal High-Performance Time-Series database library

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


HOCDB is a high-performance, embedded time-series database library written in Zig. It provides strict schema enforcement, fixed-size records for O(1) access, and highly optimized SIMD aggregations. It is designed to be embedded directly into applications (like SQLite or LevelDB) rather than running as a standalone server.

This library is built for high-frequency trading and other latency-sensitive time-series workloads.

## Features

*   **Fixed-Size Records**: Data is stored in a binary format with fixed record sizes, enabling O(1) random access and eliminating parsing overhead.
*   **Append-Only Log**: Sequential writes maximize disk I/O throughput.
*   **Zero-Copy Read**: Data is loaded directly from disk into memory structures without deserialization.
*   **SIMD Aggregation**: Statistical operations (min, max, sum, mean) utilization SIMD instructions for extreme speed.
*   **Ring Buffer Mode**: Optional circular buffer support for constant-space usage.
*   **Cross-Language Support**: Native bindings for C, C++, Python, Go, Node.js, and Bun.

## API Overview

The HOCDB API is consistent across all supported languages.

*   `init(ticker, path, schema, config)`: Open or create a database instance.
*   `append(record)`: Write a single record to the database.
*   `flush()`: Force buffered data to be written to disk.
*   `query(start, end, filters)`: Retrieve raw records within a timestamp range.
*   `getStats(start, end, field, func_options)`: Compute statistics (Min, Max, Sum, Count, Mean, Percentiles) for a specific field (by index or name) in a time range.
*   `getLatest(field)`: Retrieve the most recent value and timestamp for a field (by index or name).
*   `close()`: Close the database handle and release resources.
*   `drop()`: Close the database and delete data files from disk.

## Limitations

*   **Embedded Only**: Single-process access. Not a client-server database.
*   **Fixed Schema**: Schema must be defined at initialization and cannot change for an existing database file.
*   **Time-Series Optimized**: Primary indexing is by Timestamp (`i64`). Other queries require scanning (though scanning is extremely fast).

## Building

HOCDB uses the Zig build system.

**Prerequisites**: [Zig 0.15.2](https://ziglang.org/download/)

```bash
# Build core library and all bindings
zig build

# Run benchmarks
zig build bench -Doptimize=ReleaseFast
```

## Comparisons

| Metric | HOCDB | LevelDB | SQLite |
| :--- | :--- | :--- | :--- |
| **Primary Use Case** | Time-Series / HFT | Key-Value Store | Relational / General |
| **Data Layout** | Columnar/Row Hybrid | LSM Tree | B-Tree |
| **Read Speed** | ~535M ops/sec | ~200k ops/sec | ~500k ops/sec |
| **Write Speed** | ~18M ops/sec | ~400k ops/sec | ~50k ops/sec |

*Benchmarks run on Apple Silicon (M-series).*

## Bindings

HOCDB is written in Zig but provides native bindings for other languages.

### C / C++
```cpp
#include "hocdb_cpp.h"
// ...
hocdb::Database db("BTC_USD", "data", schema);
db.append(record);
auto stats = db.getStats(start, end, "price", true); // true = compute percentiles
```

### Go
```go
import "hocdb"
// ...
db, _ := hocdb.New("BTC_USD", "data", schema, options)
db.Append(record)
stats, _ := db.GetStatsByName(start, end, "price", true)
```

### Python
```python
from hocdb import HOCDB
// ...
db = HOCDB("BTC_USD", "data", schema)
db.append(record)
stats = db.get_stats(start, end, "price", compute_percentiles=True)
```

### Node.js
```javascript
const hocdb = require('hocdb');
// ...
const db = await hocdb.dbInitAsync("BTC_USD", "data", schema);
await db.append(record);
const stats = await db.getStats(start, end, "price", { percentiles: true });
```

### Bun
```typescript
import { HOCDBAsync } from "hocdb";
// ...
const db = new HOCDBAsync("BTC_USD", "data", schema);
await db.append(record);
const stats = await db.getStats(start, end, "price", { percentiles: true });
```

## Contributing

This repository is maintained by the Heroes of Crypto AI Team. We welcome issues and pull requests that improve performance or binding compatibility.