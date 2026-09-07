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
*   **Built-in Indicators & Analytics**: 83 technical, microstructure, pair, label and session indicators (SMA/EMA ladders, RSI, MACD, Bollinger, ATR, ADX, Ichimoku, Supertrend, VWAP, ...), tick→bar resampling, risk statistics (Sharpe, Sortino, drawdown, VaR, Hurst) and a one-shot ~100-field snapshot, all computed in-engine with SIMD kernels and verified against TA-Lib. See [INDICATORS.md](INDICATORS.md).

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

## Indicators & Analytics

Indicators are computed inside the engine from the stored columns in one pass,
so an agent can fetch a complete market picture with a single call. The same
surface exists in every binding (see the binding READMEs for the exact
signatures and [INDICATORS.md](INDICATORS.md) for the full catalogue,
conventions and performance numbers):

*   `indicators(specs, range | tail, options)`: any set of indicators as aligned series in one pass (`{ kind: "rsi", period: 14 }`, `{ kind: "macd" }`, `{ kind: "sma", period: 10, field: "volume" }`, ...). Warm-up is handled automatically (`lookback: "auto"`), and `bucket` aggregates ticks into OHLCV bars first.
*   `snapshot(options)`: ~100 named values for the latest bar (SMA 5…200, EMA 9…200, RSI, Stochastics, MACD, PPO, CCI, ADX/DI, Aroon, PSAR, Supertrend, Ichimoku, Bollinger, Keltner, Donchian, ATR, OBV, VWAP, MFI, returns, z-score, drawdown, Sharpe, ...) — the "give me everything" call for an LLM trading agent, ~0.5 ms.
*   `summary(range, field, periods_per_year)`: 29 risk/performance statistics (total/annualised return, volatility, Sharpe, Sortino, Calmar, max drawdown & duration, VaR/CVaR 95, win rate, profit factor, skew, kurtosis, autocorrelation, Hurst exponent, mean-reversion half-life).
*   `ohlcv(range, bucket, price, volume, side)`: OHLCV bars aggregated from raw records, with buy volume when the aggressor side is stored.
*   `pairIndicators(other, specs, ...)`: the same indicators over two databases aligned on time (ratio, spread z-score, relative strength, correlation, beta to a benchmark).
*   `snapshotMulti(buckets)`: the snapshot for several bar sizes (1m / 5m / 1h / 1d) from one read.
*   `health(...)`: gaps, stale feed, outlier and volume sanity checks. `evaluate(decisions, ...)`: hit rate, PnL, Sharpe and drawdown of the agent's own decisions.
*   Microstructure kinds on tick data (spread, order-flow imbalance, tick pressure, trade intensity, Amihud illiquidity, realised volatility), look-ahead labels (forward returns with MFE/MAE, triple-barrier) and session-anchored kinds (session VWAP, session range, opening range, pivots).

```ts
// Bun: 5-minute bars from 1-minute records, indicators for the last 200 bars
const res = db.indicatorsTail(200, [
  { kind: "ema", period: 21 }, { kind: "macd" }, { kind: "bbands" }, { kind: "atr" },
], { bucket: 300 });
res.columns["ema_21"]; res.columns["macd_hist"]; res.columns["bbands_upper"];

const snap = db.snapshot({ periodsPerYear: 252 });
snap.rsi_14; snap.supertrend_dir; snap.bb_percent_b; snap.sharpe_20;
```

```python
# Python
res = db.indicators([{"kind": "rsi"}, {"kind": "supertrend"}], tail=500)
risk = db.summary(start_ts, end_ts, "close", periods_per_year=252)
snap = db.snapshot(periods_per_year=252)
```

Every kernel is checked against TA-Lib 0.7 (or explicit numpy references for
indicators TA-Lib lacks) in `src/test_indicators_golden.zig`, and against
naive scalar implementations on million-row series in `src/test_indicators.zig`.

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

# Run tests with summary
zig build test --summary all

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

## Installation as Zig Package

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
    const stats = try db.getStatsByName(1620000000, 1620000100, "price");
    std.debug.print("Min: {d}, Max: {d}\n", .{ stats.min, stats.max });

    // Get Latest Value
    const latest = try db.getLatestByName("price");
    std.debug.print("Latest value: {d}, Timestamp: {d}\n", .{ latest.value, latest.timestamp });

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
stats = db.get_stats(1620000000, 1620000100, "price")
print(f"Min: {stats.min}, Max: {stats.max}")

# Get Latest Value
latest = db.get_latest("price")
print(f"Latest value: {latest.value}, Timestamp: {latest.timestamp}")

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
    const stats = await db.getStats(1620000000n, 1620000100n, "price");
    console.log(`Min: ${stats.min}, Max: ${stats.max}`);

    // Get Latest Value
    const latest = await db.getLatest("price");
    console.log(`Latest value: ${latest.value}, Timestamp: ${latest.timestamp}`);

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
const stats = await db.getStats(1620000000n, 1620000100n, "price", { percentiles: true });
console.log(stats);

// Get Latest Value
const latest = await db.getLatest("price");
console.log(latest);

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

    // Aggregation
    auto stats = db.getStatsByName(1620000000, 1620000100, "price");
    // stats.min, stats.max, etc.

    // Get Latest Value
    auto latest = db.getLatestByName("price");
    // latest.first (value), latest.second (timestamp)

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

    // Aggregation
    stats, err := db.GetStatsByName(1620000000, 1620000100, "price", false)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Min: %f, Max: %f\n", stats.Min, stats.Max)

    // Get Latest Value
    latest, err := db.GetLatestByName("price")
    if err != nil {
        panic(err)
    }
    fmt.Printf("Latest value: %f, Timestamp: %d\n", latest.Value, latest.Timestamp)

    // Drop
    db.Drop()
}
```

## Contributing

This repository is maintained by the Heroes of Crypto AI Team. We welcome issues and pull requests that improve performance or binding compatibility.
## Durability, readers and operations

HOCDB files carry a 64-byte header with the writer's *committed* cursor,
updated atomically after every flush. That single word gives you both crash
safety and multi-process reads:

*   **Single writer, lock-free readers.** A writer takes an exclusive lock and
    a second writer fails immediately with `DatabaseLocked`. Any number of
    other processes can attach with `openReader` / `hocdb_open_reader`: they
    take no lock, see exactly the data the writer has flushed, and pick up
    new commits on every read (or explicitly with `refresh()`). Readers follow
    compaction and rollover automatically. This is how an ingestion process
    and a trading agent share one database.
*   **fsync policy** per database: `none`, `on_close` (default), `on_flush`, or
    `interval` (at most every *N* ms). `sync()` forces a flush and fsync.
*   **Crash recovery.** On the next writer open, records written after the
    last commit are adopted when they are complete and in timestamp order;
    torn or out-of-order bytes are truncated. Both counts are in the metrics.
*   **Checksums.** A CRC32C of the committed data is maintained incrementally
    and stored in the header; `verify()` recomputes it, `verify_on_open`
    refuses a corrupted file. (Not available for ring buffers.)
*   **Retention and rollover.** `compact(min_ts)` / `retain_last(n)` rewrite
    the file atomically; `retention_span` does it automatically once the
    history exceeds the span by 25%. `rollover()` (or `rollover_size`) archives
    the file as `<ticker>.<first_ts>-<last_ts>.bin` and continues with an empty
    one; archives are ordinary databases.
*   **Metrics.** Appends, flushes, commits, fsyncs (count and latency), reads
    (count, p50/p99 latency, records read), refreshes, recovery and checksum
    counters, compactions, rollovers, ingest lag (wall clock and, with
    `timestamp_unit_ns`, in record time), committed records, file size.
*   **Legacy files** (`HOC1`, 12-byte header) are migrated in place the first
    time a writer opens them. Ring-buffer capacity is
    `max_file_size = 64 + N × record_size`.

```python
writer = HOCDB("BTCUSD", "./data", schema, fsync="interval", fsync_interval_ms=500,
               retention_span=30 * 86_400_000_000, timestamp_unit_ns=1_000)
# ... in another process:
reader = HOCDB.open_reader("BTCUSD", "./data", schema)
snap = reader.snapshot(bucket=60_000_000)      # always the latest committed bars
lag_ms = reader.metrics()["ingest_lag_wall_ns"] / 1e6
```

See the binding READMEs for the exact signatures.

## Calendars, backtesting and universe features

* **Trading calendars** (`calendar` option: `crypto`, `fx`, `nyse`, `nasdaq`, `lse`, `cme`, or a custom
  definition): session-anchored indicators follow real exchange sessions and holidays, data health
  measures gaps in trading time, and annualisation (`periods_per_year`) is derived automatically.
* **Signal backtester**: a target-position series over the rows of an indicator window gives an equity
  curve with costs, slippage, stops, a trade list and full statistics, plus walk-forward splits.
* **Universe features**: momentum / volatility ranks, correlation matrix, market factor and betas,
  dispersion and breadth over a watch-list of databases in one call.

See `INDICATORS.md` for the semantics and every binding's README for the calls.
