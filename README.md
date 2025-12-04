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

HOCDB is a hyper-specialized, high-performance time-series database designed for extreme throughput and low latency. Originally built for crypto trading backtesting, it has evolved into a universal engine capable of saturating modern hardware.

**Supported Languages:**
*   **Zig** (Native)
*   **C**
*   **C++**
*   **Python**
*   **Node.js**
*   **Bun**

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

---

## Design Philosophy
*   **Universal Core**: The core engine is written in **Zig** for manual memory control and zero hidden allocations, exposed via a stable C ABI.
*   **Zero-Copy Access**: Load millions of records instantly. HOCDB maps data directly into memory, allowing your application to access it without copying.
*   **Dynamic Schema**: Define your own data structure. HOCDB adapts to *your* data, not the other way around.
*   **Append-Only**: Strictly sequential writes ensure maximum disk throughput and data integrity.
*   **Ring Buffer**: Optional circular buffer mode to automatically overwrite old data when the file limit is reached.

---

## Getting Started

### Prerequisites
*   **Zig 0.15.2** (for building the core)
*   Your language of choice (C, C++, Python, Node, Bun)

### Build Core & Bindings
```bash
# Build everything (Core, C/C++ libs, Python bindings)
zig build
zig build c-bindings
zig build python-bindings
```

---

## Language Bindings

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
```

### 🥟 Bun
Native FFI bindings for Bun.

```typescript
import { HOCDB } from "./bindings/bun/index.ts";

const db = new HOCDB("BTC_USD", "./data", schema);
db.append(1620000000, 50000.0, 1.5);
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
}
```

---

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_file_size` | Number | 2GB | Maximum size of the data file in bytes. |
| `overwrite_on_full` | Boolean | `true` | Whether to overwrite old data when the file is full (Ring Buffer). |
| `flush_on_write` | Boolean | `false` | Whether to flush to disk after every write. Ensures durability but reduces performance. |

---

*Built with ❤️ and ⚡ by the Heroes of Crypto AI Team.*