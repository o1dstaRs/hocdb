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

    // Writer configuration (see "Durability, readers and operations")
    enum class FsyncPolicy { None = 0, OnClose = 1, OnFlush = 2, Interval = 3 };
    struct Config {
        int64_t  max_file_size     = 0;      // 0 = default (2 GiB)
        bool     overwrite_on_full = false;  // ring buffer
        bool     flush_on_write    = false;
        bool     auto_increment    = false;
        FsyncPolicy fsync          = FsyncPolicy::OnClose;
        uint32_t fsync_interval_ms = 0;      // Interval policy; 0 = 1000
        bool     verify_on_open    = false;
        int64_t  retention_span    = 0;      // timestamp units; 0 = off
        uint64_t rollover_size     = 0;      // bytes; 0 = off
        bool     auto_migrate      = true;
        uint64_t timestamp_unit_ns = 0;      // ns per timestamp unit; 0 = unknown
        uint64_t index_stride      = 0;      // 0 = 1024
    };
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

#### Constructors and `openReader()`

```cpp
// Writer, basic options (the remaining options keep their defaults)
Database(
    const std::string& ticker,
    const std::string& path,
    const std::vector<Field>& schema,
    int64_t max_file_size = 0,      // 0 = default
    bool overwrite_on_full = true,   // Ring buffer mode
    bool flush_on_write = false,
    bool auto_increment = false
);

// Writer, full configuration (fsync policy, retention, rollover, ...)
Database(
    const std::string& ticker,
    const std::string& path,
    const std::vector<Field>& schema,
    const hocdb::Config& config
);

// Lock-free reader of a database another process writes (read methods only)
static Database Database::openReader(
    const std::string& ticker,
    const std::string& path,
    const std::vector<Field>& schema
);
```

Both constructors open the database as its **writer** (through `hocdb_init_ex`)
and take an exclusive lock; a second writer on the same file fails at once.
A failed open throws `hocdb::Exception` whose message ends with the engine's
error name (`DatabaseLocked`, `SchemaMismatch`, `ChecksumMismatch`,
`LegacyFormatNeedsMigration`, ...). See
[Durability, readers and operations](#durability-readers-and-operations).

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

// Full configuration: fsync after every flush, 30-day retention (microsecond timestamps)
hocdb::Config cfg;
cfg.fsync = hocdb::FsyncPolicy::OnFlush;
cfg.retention_span = 30LL * 86'400'000'000;
cfg.timestamp_unit_ns = 1'000;
hocdb::Database db("BTC_USD", "data", schema, cfg);

// Reader in another process (no lock; sees what the writer has flushed)
hocdb::Database reader = hocdb::Database::openReader("BTC_USD", "data", schema);
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

## Indicators & analytics

HOCDB computes technical indicators, OHLCV bars and risk/performance
analytics inside the storage engine: 83 indicator kinds (moving averages,
momentum, trend, volatility, volume, statistics, tick microstructure,
pair/spread kinds, look-ahead labels and session-anchored kinds), two-database
pair indicators, data-quality health checks, decision evaluation and
multi-timeframe snapshots. The C++ wrapper exposes them as `hocdb::Database`
methods that return standard containers; indicator kinds and fields are given
by name and resolved at runtime, and every C result is freed by an RAII guard
even if copying it out throws. See [../../INDICATORS.md](../../INDICATORS.md)
for the full kind table, defaults, output names and conventions.

```cpp
namespace hocdb {
    struct IndicatorSpec {          // one indicator request (aggregate; brace-init)
        std::string kind;           // "sma", "rsi", "macd", ... (case-insensitive)
        uint32_t period = 0, period2 = 0, period3 = 0, period4 = 0; // 0 = default
        double param = 0, param2 = 0; // k / multiplier / periods-per-year / session length ...
        std::string field, field2;  // DB field to run on ("" = close); field2 = second series (correl, beta, ratio, ...)
        std::string label;          // output column name override
    };
    struct IndicatorColumns { std::string open, high, low, close, volume, bid, ask, side; }; // "" = absent
    struct IndicatorOptions {
        std::optional<IndicatorColumns> columns; // nullopt = auto-detect
        std::optional<size_t> lookback;          // nullopt = auto
        int64_t bucket = 0;
    };
    struct PairOptions : IndicatorOptions {
        std::optional<IndicatorColumns> other_columns; // roles of the other database (nullopt = auto-detect)
    };
    struct IndicatorResult {
        std::vector<int64_t> timestamps;
        std::vector<std::string> names;             // one per output column
        std::vector<std::vector<double>> outputs;   // outputs[k] is the series names[k]
        size_t n_rows = 0;
        const std::vector<double>& column(const std::string& name) const; // throws on unknown name
    };
    struct Bars { std::vector<int64_t> timestamps;
                  std::vector<double> open, high, low, close, volume, count, buy_volume; }; // buy_volume: only with a side field
    struct Decision { int64_t timestamp; double direction; double size = 1; int64_t horizon = 0; };
    struct EvaluationResult { HOCDBEvaluation stats; std::vector<double> entry, exit, net_return; };
}

// hocdb::Database
IndicatorResult indicators(const std::vector<IndicatorSpec>& specs, int64_t start_ts, int64_t end_ts,
                           const IndicatorOptions& options = {});
IndicatorResult indicatorsTail(size_t n, const std::vector<IndicatorSpec>& specs,
                               const IndicatorOptions& options = {});
IndicatorResult pairIndicators(Database& other, const std::vector<IndicatorSpec>& specs,
                               int64_t start_ts, int64_t end_ts, const PairOptions& options = {});
IndicatorResult pairIndicatorsTail(Database& other, size_t n, const std::vector<IndicatorSpec>& specs,
                                   const PairOptions& options = {});
Bars ohlcv(int64_t start_ts, int64_t end_ts, int64_t bucket, const std::string& price_field,
           const std::string& volume_field = "", const std::string& side_field = "");
HOCDBSummary summary(int64_t start_ts, int64_t end_ts, const std::string& field, double periods_per_year = 0);
std::map<std::string, double> summaryMap(int64_t start_ts, int64_t end_ts, const std::string& field,
                                         double periods_per_year = 0);
HOCDBHealth health(int64_t start_ts, int64_t end_ts, const std::string& price_field,
                   const std::string& volume_field = "", int64_t gap_threshold = 0, double outlier_threshold = 0);
std::map<std::string, double> healthMap(/* same arguments */);
EvaluationResult evaluate(const std::vector<Decision>& decisions, const std::string& price_field,
                          int64_t default_horizon = 0, double cost_bps = 0);
static std::map<std::string, double> evaluationMap(const HOCDBEvaluation& stats);
HOCDBSnapshot snapshot(const IndicatorColumns* cols = nullptr, size_t bars = 0, int64_t bucket = 0,
                       double periods_per_year = 0);
std::map<std::string, double> snapshotMap(const IndicatorColumns* cols = nullptr, size_t bars = 0,
                                          int64_t bucket = 0, double periods_per_year = 0);
std::vector<HOCDBSnapshot> snapshotMulti(const std::vector<int64_t>& buckets,
                                         const std::vector<double>& periods_per_year = {}, size_t bars = 0,
                                         const IndicatorColumns* cols = nullptr);
std::vector<std::map<std::string, double>> snapshotMultiMap(/* same arguments */);
static std::vector<std::string> indicatorKinds();                      // "sma", "ema", ..., "pivots" (83 kinds)
static std::vector<std::string> indicatorOutputs(const std::string& kind); // {"macd","signal","hist"}; {"value"} for single-output kinds
static bool indicatorIsLookahead(const std::string& kind);             // true for forward_return / triple_barrier
static size_t indicatorWarmup(const IndicatorSpec& spec);              // recommended warm-up rows
```

All methods throw `hocdb::Exception` on an unknown kind or field, a missing
column, a duplicate output column name, a per-spec `field` override combined
with `bucket > 0`, a session kind without `param`, or any other C API error.

### Batch of indicators by name

```cpp
#include "hocdb_cpp.h"

// Schema: timestamp, open, high, low, close, volume
// (all f64 except the i64 timestamp; columns are auto-detected by name)
std::vector<hocdb::IndicatorSpec> specs = {
    {"sma", 20},
    {"ema", 50},
    {"rsi", 14},
    {"macd"},                              // defaults 12/26/9
    {"bbands", 20, 0, 0, 0, 2.0},          // period 20, k = 2.0
    {"atr", 14},
    {"sma", 10, 0, 0, 0, 0, 0, "volume", "", "vol_sma"}, // SMA of the volume field, custom label
};

auto res = db.indicators(specs, start_ts, end_ts);   // lookback = auto, bucket = 0
// res.names: sma_20, ema_50, rsi_14, macd, macd_signal, macd_hist,
//            bbands_20_upper, bbands_20_middle, bbands_20_lower,
//            bbands_20_percent_b, bbands_20_bandwidth, atr_14, vol_sma
const auto& rsi = res.column("rsi_14");
const auto& hist = res.column("macd_hist");
for (size_t i = 0; i < res.n_rows; ++i) {
    if (!std::isnan(rsi[i]) && rsi[i] < 30 && hist[i] > 0) {
        std::cout << res.timestamps[i] << " oversold + MACD turning up\n";
    }
}

// Last 5 rows only, no explicit range:
auto tail = db.indicatorsTail(5, {{"rsi", 14}, {"supertrend"}});
```

### Snapshot for an LLM / trading agent

`snapshotMap()` returns ~100 named values for the latest bar (SMA/EMA
ladders, RSI, MACD, Bollinger, ATR, ADX, Ichimoku, VWAP, returns, volatility,
Sharpe, ...) in one call, decoded generically through the C introspection
functions, so it needs no code change when the library adds fields.

```cpp
// 5-minute bars -> 365 * 24 * 12 periods per year
auto snap = db.snapshotMap(nullptr, 0, 0, 365.0 * 24 * 12);

std::ostringstream prompt;
prompt << "Market state at " << static_cast<int64_t>(snap.at("timestamp")) << ":\n";
for (const auto& [name, value] : snap) {
    if (name != "timestamp" && name != "bars") {
        prompt << "  " << name << " = " << value << "\n";   // NaN = not enough history
    }
}
// prompt.str() -> feed to the model

// Typed access when you prefer the struct from hocdb.h:
HOCDBSnapshot s = db.snapshot();
if (s.rsi_14 < 30 && s.close < s.bb_lower && s.supertrend_dir > 0) { /* ... */ }

// Several bar sizes from one read (bucket order; periods_per_year per bucket):
auto multi = db.snapshotMultiMap({60'000'000, 300'000'000}, {525600, 105120}, 500);
std::cout << "1m rsi=" << multi[0].at("rsi_14") << " 5m rsi=" << multi[1].at("rsi_14") << "\n";
```

### Tick data -> bars: `bucket` and `ohlcv()`

With `bucket > 0` the raw records are first aggregated into OHLCV bars of
`bucket` timestamp units and the indicators run on the bars. `ohlcv()` returns
the bars themselves; give it a side field (1 = buy, 0 = sell) to also get the
per-bar buy volume.

```cpp
// Schema: timestamp (us), price, size, bid, ask, side
// "price" is used as close and "size" as volume automatically
hocdb::IndicatorOptions opt;
opt.bucket = 60'000'000;                     // 1-minute bars from ticks

auto bars = db.indicatorsTail(200, {{"ema", 20}, {"atr", 14}, {"vwap"}}, opt);

// The bars as candles (buy_volume is filled only when a side field is given):
hocdb::Bars candles = db.ohlcv(start_ts, end_ts, 60'000'000, "price", "size", "side");
for (size_t i = 0; i < candles.timestamps.size(); ++i) {
    std::cout << candles.timestamps[i] << " O=" << candles.open[i] << " H=" << candles.high[i]
              << " L=" << candles.low[i] << " C=" << candles.close[i]
              << " V=" << candles.volume[i] << " buy=" << candles.buy_volume[i]
              << " n=" << candles.count[i] << "\n";
}
hocdb::Bars plain = db.ohlcv(start_ts, end_ts, 60'000'000, "price", "size"); // plain.buy_volume.empty()

// Risk / performance statistics of a field (29 values):
HOCDBSummary sum = db.summary(start_ts, end_ts, "price", 252.0);
std::cout << "sharpe=" << sum.sharpe << " max_dd=" << sum.max_drawdown << "\n";
auto sum_map = db.summaryMap(start_ts, end_ts, "price", 252.0); // same values as name -> double
```

### Microstructure, sessions and labels on ticks

The `bid`, `ask` and `side` column roles (auto-detected by name, or set in
`IndicatorColumns`) feed the tick-level kinds: `spread` (abs, bps) needs
bid/ask; `order_flow`, `tick_pressure` and per-bar buy volume need `side`;
`trade_intensity` (`param` = timestamp units per second, default 1e6),
`amihud` and `realized_vol` (`param` = periods per year) work on any series.

```cpp
auto t = db.indicatorsTail(100, {
    {"spread"},                                 // spread_abs, spread_bps
    {"order_flow", 10},                         // order_flow_10_net, order_flow_10_imbalance
    {"trade_intensity", 10, 0, 0, 0, 1e6},      // ..._trades_per_sec, ..._volume_per_sec
    {"session_vwap", 0, 0, 0, 0, 600e6},        // param = session length (mandatory), param2 = offset
    {"forward_return", 5},                      // label: forward_return_5_ret / _max / _min
});
```

- **Session-anchored kinds** (`session_vwap`, `session_range`,
  `opening_range`, `pivots`) require `param` = session length in timestamp
  units (`param2` = session offset); without it the call throws.
- **Labels are look-ahead by design.** `forward_return` (`period` = horizon)
  and `triple_barrier` (`period` = horizon, `param` = up fraction, `param2` =
  down fraction) use *future* rows and are NaN at the end of every window.
  Use them as training targets, never as features for the same row.
  `hocdb::Database::indicatorIsLookahead(kind)` tells the two apart.

### Pairs: two databases

`pairIndicators()` / `pairIndicatorsTail()` run the specs on this database
with another open database as the second series (its close column). On ticks
the other database is as-of joined onto this one's rows (latest row at or
before each timestamp); with `bucket > 0` both are resampled and inner-joined
on bar timestamps. `series` / `series2` pass the two inputs through; `ratio`,
`ratio_zscore`, `rel_strength`, `correl` and `beta` combine them.
Per-spec `field2` is not used here (it would be ignored) and is rejected.

```cpp
hocdb::Database eth("ETH", dir, tick_schema), btc("BTC", dir, tick_schema);

hocdb::PairOptions po;
po.bucket = 10'000'000;                          // 10-second bars, inner-joined
// po.other_columns = hocdb::IndicatorColumns{"", "", "", "mid", "qty"}; // roles of `btc` if they differ
auto pr = eth.pairIndicators(btc, {{"series"}, {"series2"}, {"ratio"}, {"ratio_zscore", 60}, {"correl", 30}},
                             start_ts, end_ts, po);
const auto& z = pr.column("ratio_zscore_60");
```

### Health, evaluation and multi-timeframe snapshots

```cpp
// Data quality: count, first/last ts, gaps above 5 s, |log return| outliers above 5 %, zero / negative volume
HOCDBHealth h = db.health(INT64_MIN, INT64_MAX, "price", "size", 5'000'000, 0.05);   // or healthMap(...)
// Decisions -> hit rate, PnL, Sharpe, ...; entry at ts, exit at ts + horizon, 5 bps per side
auto ev = db.evaluate({{ts, +1, 1000, 60'000'000}, {ts2, -1, 500}}, "price", 120'000'000, 5.0);
std::cout << ev.stats.hit_rate << " " << ev.net_return[0] << "\n";   // NaN where not evaluated
auto ev_map = hocdb::Database::evaluationMap(ev.stats);              // 20 fields as name -> double
// Snapshots for 1-minute and 5-minute bars from one read (see above): db.snapshotMulti(...)
```

### Options

| Option | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `columns` | `std::optional<IndicatorColumns>` | auto-detect | Field names for the open/high/low/close/volume/bid/ask/side roles (`""` = absent). Auto-detection uses fields literally named `open`/`high`/`low`/`close`/`volume`/`bid`/`ask`/`side`; a field named `price` is used as close when there is no `close`, and `size` or `qty` as volume when there is no `volume`. Only close is mandatory; other roles are needed only by the indicators that use them (ATR needs high/low, OBV needs volume, spread needs bid/ask, order_flow needs volume + side, ...). |
| `lookback` | `std::optional<size_t>` | auto | Extra records (bars when `bucket > 0`) read before the window so that the first in-window values are converged. `nullopt` = recommended per-spec warm-up; `0` = none (NaN warm-up inside the window). |
| `bucket` | `int64_t` | `0` | `0` = one row per record; `> 0` = aggregate records into OHLCV bars of that many timestamp units first (tick -> bar). Per-spec `field` overrides are rejected with `bucket > 0`. |
| `other_columns` | `std::optional<IndicatorColumns>` | auto-detect | `PairOptions` only: the column roles of the other database, resolved against *its* schema. |

`snapshot()` / `snapshotMap()` take the columns (pointer, `nullptr` = auto),
the number of bars (`0` = recommended 2500), `bucket` and `periods_per_year`
positionally; `snapshotMulti()` / `snapshotMultiMap()` take the bucket list,
the matching `periods_per_year` list (empty = 0), the number of bars and the
columns, and return one snapshot per bucket in bucket order.

### Output naming

- The label of a spec is `spec.label` if given, otherwise the kind name plus
  `_<period>` when a period is given: `sma_20`, `rsi_14`, `macd`, `obv`.
- Single-output kinds use the label as the column name.
- Multi-output kinds append the output name: `macd_signal`, `macd_hist`,
  `bbands_20_upper`, `adx_14_plus_di`, `spread_bps`, `order_flow_10_imbalance`;
  the output named like the kind itself is just the label (`macd`, `adx_14`).
- Two specs producing the same column name are rejected; set `label` to
  disambiguate (e.g. two `sma_20` on different fields).

`IndicatorResult::names` holds the columns in spec order, and
`IndicatorResult::column(name)` looks a series up by name.

### NaN warm-up

A value that is not yet defined (the indicator has not seen `period` rows, or
`lookback = 0` was requested) is `NaN`; it is never converted to another
sentinel. With the default automatic lookback the first in-window values are
already converged whenever enough history exists before the window. Use
`hocdb::Database::indicatorWarmup(spec)` to get the recommended warm-up row
count of a spec. Label kinds are NaN at the *end* of the window instead
(they need future rows).

## Durability, readers and operations

Data files carry a 64-byte header (`HOC2`) holding the writer's *committed*
cursor, updated atomically on every flush, and a CRC32C of the committed data.
That one word gives a single writer, any number of lock-free readers, crash
recovery and in-place maintenance. Legacy `HOC1` files are migrated in place
the first time a writer opens them (`Config::auto_migrate`, default on).

### Options (`hocdb::Config`)

`hocdb::Config` is an aggregate with defaults for every field; set the ones you
need and pass it to `Database(ticker, path, schema, const Config&)`. The
four-option constructor goes through the same path with the remaining options
at their defaults.

| Option | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `max_file_size` | `int64_t` | `0` (2 GiB) | Maximum file size in bytes, including the 64-byte header. |
| `overwrite_on_full` | `bool` | `false` | Ring buffer: overwrite the oldest records when the file is full (`true` in the legacy constructor's default). |
| `flush_on_write` | `bool` | `false` | Commit after every `append()`. |
| `auto_increment` | `bool` | `false` | The database assigns the timestamps. |
| `fsync` | `hocdb::FsyncPolicy` | `OnClose` | When the data file is fsynced: `None` (never, the OS decides), `OnClose` (once on close), `OnFlush` (after every flush / commit), `Interval` (at most every `fsync_interval_ms`, and on close). `sync()` forces a flush + fsync at any time. |
| `fsync_interval_ms` | `uint32_t` | `0` (= 1000) | Period of the `Interval` policy. |
| `verify_on_open` | `bool` | `false` | Recompute the CRC32C when opening; the open fails with `ChecksumMismatch` if the data is corrupted. |
| `retention_span` | `int64_t` | `0` (off) | Keep only the last span of history, in timestamp units (`last_ts - span`); the file is compacted automatically once the excess exceeds 25%. |
| `rollover_size` | `uint64_t` | `0` (off) | Archive the file automatically once it exceeds this many bytes (see `rollover()`). |
| `auto_migrate` | `bool` | `true` | Rewrite legacy `HOC1` files to the current format when a writer opens them. Disable it to get `LegacyFormatNeedsMigration` instead. |
| `timestamp_unit_ns` | `uint64_t` | `0` (unknown) | Nanoseconds per timestamp unit (`1'000` for microseconds, `1'000'000'000` for seconds); enables the `ingest_lag_record_ns` metric. |
| `index_stride` | `uint64_t` | `0` (= 1024) | Records per timestamp-index entry. |

`sizeof(HOCDBConfig)` (the C struct behind `hocdb::Config`) is 72 bytes.

### Single writer, lock-free readers

A writer holds an exclusive lock on its file; a second writer fails immediately
with an `hocdb::Exception` mentioning `DatabaseLocked` instead of blocking.
Readers are opened with `Database::openReader(ticker, path, schema)`: they take
no lock, and every read method (`query`, `load`, `getStats`, `getLatest`,
`indicators`, `pairIndicators`, `ohlcv`, `summary`, `snapshot`,
`snapshotMulti`, `health`, `evaluate`, ...) re-reads the writer's committed
cursor, so a reader sees exactly the records the writer has **flushed** and
follows compaction and rollover on its own. `refresh()` picks up the latest
commit explicitly (a no-op on writers), `isReadOnly()` is `true`, and writes
(`append`, `sync`, `compact`, `retainLast`, `rollover`) throw an `Exception`
saying the handle is a read-only reader (`flush()` on a reader is a no-op).
This is the ingestion-process / agent-process pattern:

```cpp
// Ingestion process: the single writer
hocdb::Config cfg;
cfg.fsync = hocdb::FsyncPolicy::Interval;
cfg.fsync_interval_ms = 500;
cfg.retention_span = 30LL * 86'400'000'000;   // keep 30 days (microsecond timestamps)
cfg.timestamp_unit_ns = 1'000;                 // so ingest lag is also reported in record time
hocdb::Database writer("BTCUSD", "./data", schema, cfg);
for (const Trade& t : feed) {
    writer.append(t);
}
writer.flush();   // commit: readers see the records from here on

// Trading agent (another process): a lock-free reader
hocdb::Database reader = hocdb::Database::openReader("BTCUSD", "./data", schema);
auto snap = reader.snapshotMap(nullptr, 0, 60'000'000);      // always the latest committed bars
auto [price, ts] = reader.getLatest("price");
double lag_ms = reader.metricsMap().at("ingest_lag_wall_ns") / 1e6;
```

### sync / verify / compact / retainLast / rollover / metrics (writers)

```cpp
writer.sync();                                   // flush + fsync now, whatever the policy
bool ok = writer.verify();                       // CRC32C of the committed data: false = corrupted
                                                 // (throws "checksum unavailable" for ring buffers / legacy files)
writer.compact(now - 7 * DAY);                   // keep timestamp >= min_ts; the file is rewritten atomically
writer.retainLast(1'000'000);                    // keep only the last n records
std::string archive = writer.rollover();         // "./data/BTCUSD.<first_ts>-<last_ts>.bin"; the live file is empty again
hocdb::Database old(std::filesystem::path(archive).stem().string(), "./data", schema);   // archives are ordinary databases

HOCDBMetrics m = writer.metrics();               // raw struct (uint64_t / int64_t fields, see hocdb.h)
std::cout << m.appends << " appends, worst fsync " << m.fsync_ns_max << " ns, p99 read "
          << m.read_ns_p99 << " ns, " << m.committed_records << " committed\n";
std::map<std::string, double> mm = writer.metricsMap();   // the same 30 fields as name -> double
writer.metricsReset();                           // zero the counters; state fields are kept
int version = writer.formatVersion();            // 2 = current, 1 = legacy file
size_t header = hocdb::Database::headerSize();   // 64
```

`metrics()` returns the `HOCDBMetrics` struct; `metricsMap()` decodes the same
fields generically through the `hocdb_metrics_field_*` introspection functions
(like `summaryMap()`), so it keeps working if the header and the library
disagree on the struct layout. Its integer counters are converted to `double`,
which is exact up to 2^53 (about 9.0e15): plenty for every counter and for the
nanosecond timestamps. The 30 fields are `appends`, `bytes_written`, `flushes`,
`commits`, `fsyncs`, `fsync_ns_total`, `fsync_ns_max`, `reads`, `read_ns_total`,
`read_ns_max`, `read_ns_last`, `read_ns_p50`, `read_ns_p99`, `records_read`,
`refreshes`, `recovered_tail_records`, `dropped_tail_bytes`, `crc_failures`,
`compactions`, `rollovers`, `migrations`, `last_append_wall_ns`,
`last_commit_wall_ns`, `last_record_ts`, `ingest_lag_wall_ns` (now minus the
last commit for readers / last append for writers), `ingest_lag_record_ns`
(needs `timestamp_unit_ns`), `committed_records`, `file_size`,
`format_version` and `read_only`. Metrics are available on readers too.

### Recovery and checksums

On the next writer open, records written after the last commit are adopted when
they are complete and in timestamp order; torn or out-of-order bytes are
truncated, and both counts appear in the metrics (`recovered_tail_records`,
`dropped_tail_bytes`). A CRC32C of the committed data is kept in the header:
`verify()` recomputes it (`false` on a mismatch; the file stays readable),
`Config::verify_on_open` makes the open fail with `ChecksumMismatch` instead,
and ring buffers, legacy files and a just-adopted tail (flush first) have no
checksum, so `verify()` throws an `Exception` mentioning "checksum unavailable".

### Ring-buffer capacity

With `overwrite_on_full` a file of `max_file_size` bytes holds
`(max_file_size - 64) / record_size` records, so size it as

```
max_file_size = hocdb::Database::headerSize() + N * record_size     // headerSize() == 64
```

to keep exactly the last `N` records:

```cpp
hocdb::Config ring;
ring.max_file_size = static_cast<int64_t>(hocdb::Database::headerSize() + 100'000 * sizeof(Trade));
ring.overwrite_on_full = true;
hocdb::Database db("BTCUSD", "./data", schema, ring);   // never more than 100'000 records
```

### Errors

| Situation | What you get |
| :--- | :--- |
| Open fails | `hocdb::Exception` ending with the engine's error name (`hocdb_last_error()`): `DatabaseLocked`, `SchemaMismatch`, `ChecksumMismatch`, `LegacyFormatNeedsMigration`, ... `Database::lastError()` returns the same name. |
| Write on a reader | `hocdb::Exception`: `append failed: this handle is a read-only reader (opened with Database::openReader); only the writer can modify the database` (same for `sync`, `compact`, `retainLast`, `rollover`). |
| `verify()` without a checksum | `hocdb::Exception`: `verify failed: checksum unavailable (...)`. |
| Layout mismatch | `metrics()` throws when `sizeof(HOCDBMetrics)` differs from `hocdb_metrics_size()`; `metricsMap()` still works. |

## RAII and Resource Management

- Database is automatically closed when the `Database` object goes out of scope
- Move semantics are supported (`Database(Database&&)`, `operator=(Database&&)`)
- Copy operations are deleted (each handle should be unique)
- Memory returned by `load()` and `query()` is managed as `std::vector`

## Testing

```bash
# Compile and run tests (as verify_all.sh does)
clang++ -std=c++17 bindings/cpp/test/test.cpp -o test_binaries/test_cpp_wrapper -I bindings/c -I bindings/cpp -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_wrapper
clang++ -std=c++17 bindings/cpp/test/test_indicators.cpp -o test_binaries/test_cpp_indicators -I bindings/c -I bindings/cpp -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_indicators
# Durability, readers, maintenance and metrics
clang++ -std=c++17 bindings/cpp/test/test_storage.cpp -o test_binaries/test_cpp_storage -I bindings/c -I bindings/cpp -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_storage
```

## Calendars, backtesting and universe features

A trading calendar teaches a database when its market is open. With one attached
(and a known timestamp unit), session-anchored indicators follow real exchange
sessions, `health` measures gaps in trading time, and `periods_per_year` is
derived automatically. Built-in calendars: `crypto` (24/7), `fx`
(Sunday 17:00 to Friday 17:00 New York), `nyse`, `nasdaq`, `lse`, `cme`;
custom ones are registered at runtime. The signal backtester turns a
target-position series over the rows of an indicator window into an equity
curve with costs, slippage, stops, a trade list and performance statistics.
The universe call computes cross-sectional features over a watch-list of
databases in one shot. See `INDICATORS.md` for the exact semantics.

```cpp
hocdb::Config cfg;
cfg.calendar = hocdb::calendarId("nyse");
cfg.timestamp_unit_ns = 1000;
hocdb::Database db("AAPL", "./data", schema, cfg);
db.calendar();                  // 3
db.periodsPerYear(60'000'000);  // 98280

// session kinds with param 0 follow the exchange sessions
std::vector<hocdb::IndicatorSpec> specs{{"session_vwap"}, {"pivots"}};
auto res = db.indicatorsTail(390, specs, opts);

auto params = hocdb::backtestDefaults();
params.initial_equity = 100'000;
params.cost_bps = 5;
params.stop_loss = 0.02;
params.position_mode = static_cast<uint64_t>(hocdb::PositionMode::Fraction);
hocdb::BacktestOutputs outs;
outs.equity = true;
outs.max_trades = 1000;
auto run = db.backtest(target, start_ts, end_ts, 60'000'000, &params, outs, &cols);
run.result.sharpe;  run.trades[0].exit_reason;

auto splits = hocdb::walkForwardSplits(close.size(), 5, 0.6, true);
auto per_window = hocdb::backtestSplits(ts, &open, &high, &low, close, target, splits, &params);

auto u = hocdb::universe({&db_a, &db_b, &db_c}, &cols, 500, 60'000'000);
u.rows[0].rank_mom_mid;  u.summary.breadth_up;  u.correlation(0, 1);
```

Free calendar functions: `calendarId`, `calendarName`, `calendarSession`,
`calendarSessionForDay`, `calendarIsOpen`, `calendarOpenSeconds`,
`calendarSessionsBetween`, `calendarPeriodsPerYear`, `calendarToLocal`,
`daysFromCivil`, `civilFromDays`, `calendarDefine`; `Database::setCalendar`,
`calendar`, `setTimestampUnit`, `timestampUnit`, `periodsPerYear`, `backtest`,
`backtestTail`. `backtestResultMap`, `tradeMap`, `universeRowMap` and
`universeSummaryMap` decode the C structs into `std::map<std::string, double>`.
