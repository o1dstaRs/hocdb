<p align="center">
  <img src="assets/hocdb_trademark_256.png" alt="HOCDB Logo" width="256">
</p>

# HOCDB: Time Series Database and bindings

> **The World's Most Performant (by any measurement) Time-Series Database.**
> *Built for speed. Built for scale. Built for victory.*

## The Genesis
Two months ago, the **Heroes of Crypto AI (HOCAI)** team began a journey to master DEX trading on the Base network. Our goal was simple: develop a robust algorithm for our Bitcoin treasury assets.

Initial results were promising, but the reality of the crypto market—a chaotic, volatile beast—quickly humbled our models. We realized that to survive and thrive, we needed to stress-test our logic against thousands of market scenarios: **STABLE**, **VOLATILE**, **GROWING**, and **DROPPING** assets over intense intervals.

We had the data. We had the logic. But we hit a wall: **Velocity.**

### The Bottleneck
Speed is the only currency that matters in backtesting. To iterate effectively, we needed to run millions of simulations in seconds. Our existing infrastructure became an anchor:

*   **InfluxDB 1.8**: Choked on heavy write loads.
*   **InfluxDB 2.7**: Solved write stability but regressed 3x in read speed.
*   **QuestDB**: A significant improvement, but still too slow for the sheer scale of our ambition.

We realized we were trying to fit a square peg into a round hole. General-purpose databases are designed for everyone, which means they are optimized for no one.

### The Solution
We chose to build exactly what we needed. **HOCDB** is the result of that decision. It is not a general-purpose store; it is a hyper-specialized weapon designed for one thing: **extreme performance**.

By embracing our specific constraints—sequential, evenly distributed data—we unlocked performance that general-purpose DBs can only dream of.

---

## Performance
HOCDB is designed to saturate your hardware.

| Metric | Performance |
| :--- | :--- |
| **Write Throughput** | **~13,100,000 ops/sec** (Buffered) |
| **Read Throughput** | **~169,000,000 ops/sec** (Full Load) |
| **Aggregation Speed** | **~158,000,000 records/sec** (Mean/Vol) |
| **Write Latency (p99)** | **42 nanoseconds** |
| **Bandwidth** | **~300 MB/sec** (Write) / **~3.8 GB/sec** (Read) |

*Benchmarks run on Apple Silicon (M-series).*

---

## Design Philosophy
*   **Zig-Powered**: Written in Zig (v0.15.2) for manual memory control and zero hidden allocations.
*   **Generic & Type-Safe**: Define your own data structure (`struct`). HOCDB adapts to *your* data, not the other way around.
*   **Append-Only**: Strictly sequential writes ensure maximum disk throughput and data integrity.
*   **Schema Validation**: Every file is protected by a 64-bit schema hash. If your data structure changes, HOCDB prevents you from loading incompatible data.
*   **Instant Loading**: The `load()` method maps the entire dataset directly into memory, enabling lightning-fast backtesting iterations.

## File Format
Simplicity is the ultimate sophistication. Our binary format is lean and mean:

```text
+----------------+----------------+-------------------------------------+
| Magic (4 bytes)| Hash (8 bytes) | Data Records (N * sizeof(T))        |
+----------------+----------------+-------------------------------------+
| "HOC1"         | Wyhash(T)      | [Struct T] [Struct T] [Struct T]... |
+----------------+----------------+-------------------------------------+
```

---

## Getting Started

### Prerequisites
*   **Zig 0.15.2**

### Usage
HOCDB is designed to be embedded directly into your Zig application.

```zig
const std = @import("std");
const hocdb = @import("hocdb");

// 1. Define your data structure
const TradeData = struct {
    timestamp: i64,
    price: f64,
    volume: f64,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // 2. Initialize the DB
    const DB = hocdb.TimeSeriesDB(TradeData);
    var db = try DB.init("BTC_USD", "data");
    defer db.deinit();

    // 3. Write data (Buffered for speed)
    try db.append(.{ .timestamp = 100, .price = 50000.0, .volume = 1.5 });

    // 4. Load everything into memory
    const data = try db.load(allocator);
    defer allocator.free(data);
    
    std.debug.print("Loaded {d} records.\n", .{data.len});
}
```

### Commands
```bash
# Run the test suite
zig build test

# Run the example
zig build run

# Run the performance benchmark
zig build bench
```

## Bindings

HOCDB provides high-performance bindings for Node.js and Bun, featuring **Zero-Copy** data loading.

### Node.js
```bash
cd bindings/node
npm install
npm test
```

```javascript
const hocdb = require('./bindings/node');
const db = hocdb.dbInit("BTC_USD", "data");

// Append (11M+ ops/sec)
hocdb.dbAppend(db, 1620000000, 50000.0, 1.5);

// Load (Zero-Copy, Instant)
const buffer = hocdb.dbLoad(db); 
// buffer is an ArrayBuffer backed by Zig memory
```

### Bun
```bash
cd bindings/bun
bun install
bun run test.ts
```

```typescript
import { HOCDB } from "./bindings/bun/index.ts"; // or index.js

const db = new HOCDB("BTC_USD", "data");
db.append(1620000000, 50000.0, 1.5);
const data = db.load(); // Returns Float64Array (Zero-Copy)
```

---

*Built with ❤️ and ⚡ by the Heroes of Crypto AI Team.*

---

<img src="https://cdn-images-1.medium.com/max/1600/1*C87EjxGeMPrkTuVRVWVg4w.png" width="225"></img>