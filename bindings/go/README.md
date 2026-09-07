# HOCDB Go Bindings

Go bindings for HOCDB - The World's Most Performant Time-Series Database.

## Prerequisites

Before using the Go bindings, build the C library:

```bash
zig build c-bindings
```

This creates the necessary C library in `zig-out/lib/` that the Go bindings link against.

## Installation

```bash
# If using Go workspace (Go 1.18+)
go work init
go work use .
go work use ./bindings/go

# Or reference directly in your project
go mod edit -replace=hocdb=./bindings/go
```

## Quick Start

```go
package main

import (
    "fmt"
    "hocdb"
)

func main() {
    // Define schema
    schema := []hocdb.Field{
        {Name: "timestamp", Type: hocdb.TypeI64},
        {Name: "price", Type: hocdb.TypeF64},
        {Name: "volume", Type: hocdb.TypeF64},
        {Name: "active", Type: hocdb.TypeBool},
    }

    // Create database instance
    db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{})
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create and append records
    record, _ := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5, true)
    db.Append(record)

    record, _ = hocdb.CreateRecordBytes(schema, int64(1620000001), 50100.0, 2.0, true)
    db.Append(record)

    db.Flush()

    // Get statistics
    stats, _ := db.GetStatsByName(1620000000, 1620001000, "price", false)
    fmt.Printf("Price: min=%.2f, max=%.2f, mean=%.2f\n", stats.Min, stats.Max, stats.Mean)

    // Get latest value
    latest, _ := db.GetLatestByName("price")
    fmt.Printf("Latest: %.2f at %d\n", latest.Value, latest.Timestamp)
}
```

## API Reference

### Field Types

```go
const (
    TypeI64    FieldType = 1  // Signed 64-bit integer
    TypeF64    FieldType = 2  // 64-bit floating point
    TypeU64    FieldType = 3  // Unsigned 64-bit integer
    TypeString FieldType = 5  // Fixed 128-byte string
    TypeBool   FieldType = 6  // Boolean (1 byte)
)
```

### Data Structures

```go
// Field defines a field in the database schema
type Field struct {
    Name string
    Type FieldType
}

// Options contains configuration options for the database (hocdb_init_ex)
type Options struct {
    MaxFileSize   int64  // Max file size (0 = default 2 GiB); ring buffers: HeaderSize() + N * recordSize
    OverwriteFull bool   // Ring buffer mode
    FlushOnWrite  bool   // Flush on every write
    AutoIncrement bool   // Auto-increment timestamps

    // Durability, maintenance and metrics (see "Durability, readers and operations")
    Fsync              string // "" = hocdb.FsyncOnClose; FsyncNone, FsyncOnFlush, FsyncInterval ("0".."3" accepted too)
    FsyncIntervalMs    uint32 // FsyncInterval period (0 = 1000)
    VerifyOnOpen       bool   // Recompute the checksum on open; mismatch -> New fails with ChecksumMismatch
    RetentionSpan      int64  // Drop records older than last - span (timestamp units); 0 = off
    RolloverSize       uint64 // Archive the file above this many bytes; 0 = off
    DisableAutoMigrate bool   // Keep legacy HOC1 files untouched (default: migrate in place)
    TimestampUnitNs    uint64 // ns per timestamp unit for the ingest-lag metrics; 0 = unknown
    IndexStride        uint64 // Records per index entry (0 = 1024)
}

// Stats represents statistics for a field in a time range
type Stats struct {
    Min   float64
    Max   float64
    Sum   float64
    Count uint64
    Mean  float64
    P50   float64  // 50th percentile (if requested)
    P90   float64  // 90th percentile
    P95   float64  // 95th percentile
    P99   float64  // 99th percentile
}

// Latest represents the latest value and timestamp for a field
type Latest struct {
    Value     float64
    Timestamp int64
}

// Filter represents a filter condition for queries
type Filter struct {
    FieldIndex int
    Value      interface{}
}
```

---

### `New(ticker, path string, schema []Field, options Options) (*DB, error)`

Open or create a database as its writer. A writer holds an exclusive lock on the file: a second `New` on the same ticker and path fails immediately. A failed open returns an error that ends with the engine's error name (`DatabaseLocked`, `SchemaMismatch`, `ChecksumMismatch`, `LegacyFormatNeedsMigration`, ...).

```go
schema := []hocdb.Field{
    {Name: "timestamp", Type: hocdb.TypeI64},
    {Name: "price", Type: hocdb.TypeF64},
    {Name: "volume", Type: hocdb.TypeF64},
}

// Basic initialization
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{})
if err != nil {
    // e.g. `failed to open HOCDB "BTC_USD" in "./data": DatabaseLocked`
}

// With ring buffer (100MB, overwrite when full)
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{
    MaxFileSize:   100 * 1024 * 1024,
    OverwriteFull: true,
})

// With auto-increment timestamps
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{
    AutoIncrement: true,
})

// Durable ingestion: fsync after every flush, archive above 1 GiB, microsecond timestamps
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{
    Fsync:           hocdb.FsyncOnFlush,
    RolloverSize:    1 << 30,
    TimestampUnitNs: 1_000_000,
})
```

---

### `OpenReader(ticker, path string, schema []Field) (*DB, error)`

Attach to a database another process writes, without taking any lock. The returned `*DB` supports every read method (`Load`, `Query`, `GetStats`, `GetLatest`, indicators, `Snapshot`, `Health`, `Evaluate`, ...) plus `Refresh` and `IsReadOnly`; every read sees the writer's latest commit. `Append`, `Sync`, `Compact`, `RetainLast` and `Rollover` return an error wrapping `hocdb.ErrReadOnly`.

```go
r, err := hocdb.OpenReader("BTC_USD", "./data", schema)
if err != nil {
    panic(err)
}
defer r.Close()
latest, _ := r.GetLatestByName("price")
```

---

### `CreateRecordBytes(schema []Field, values ...interface{}) ([]byte, error)`

Create raw bytes for a record based on the schema. This helper converts Go values to the required binary format.

```go
schema := []hocdb.Field{
    {Name: "timestamp", Type: hocdb.TypeI64},
    {Name: "price", Type: hocdb.TypeF64},
    {Name: "volume", Type: hocdb.TypeF64},
}

record, err := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
if err != nil {
    panic(err)
}
```

**Supported value types:**
- `TypeI64`: `int64`, `int`, `int32`
- `TypeF64`: `float64`, `float32`, `int`
- `TypeU64`: `uint64`, `uint`, non-negative `int`
- `TypeString`: `string` (padded to 128 bytes)
- `TypeBool`: `bool`

---

### `Append(data []byte) error`

Append raw record data to the database.

```go
record, _ := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
err := db.Append(record)
if err != nil {
    // Handle error (invalid size, non-monotonic timestamp)
}
```

---

### `Flush() error`

Force buffered data to be written to disk.

```go
err := db.Flush()
```

---

### `Load() ([]byte, error)`

Load all records from the database.

```go
data, err := db.Load()
if err != nil {
    panic(err)
}
fmt.Printf("Loaded %d bytes\n", len(data))
```

---

### `Query(startTs, endTs int64, filters interface{}) ([]byte, error)`

Query records within a timestamp range with optional filters.

**Filter formats:**
- `[]Filter`: Array of Filter structs
- `map[string]interface{}`: Map of field name to value

```go
// Query without filters
data, err := db.Query(1620000000, 1620001000, nil)

// Query with map filter
filters := map[string]interface{}{
    "price": 50000.0,
    "active": true,
}
data, err := db.Query(1620000000, 1620001000, filters)

// Query with Filter slice
filters := []hocdb.Filter{
    {FieldIndex: 1, Value: 50000.0},
}
data, err := db.Query(1620000000, 1620001000, filters)
```

---

### `GetStats(startTs, endTs int64, fieldIndex int, computePercentiles bool) (*Stats, error)`

Get statistics for a field by index.

```go
// Basic stats
stats, err := db.GetStats(1620000000, 1620001000, 1, false)
fmt.Printf("Min: %.2f, Max: %.2f, Mean: %.2f\n", stats.Min, stats.Max, stats.Mean)

// With percentiles
stats, err := db.GetStats(1620000000, 1620001000, 1, true)
fmt.Printf("P99: %.2f\n", stats.P99)
```

### `GetStatsByName(startTs, endTs int64, fieldName string, computePercentiles bool) (*Stats, error)`

Get statistics for a field by name.

```go
stats, err := db.GetStatsByName(1620000000, 1620001000, "price", true)
```

---

### `GetLatest(fieldIndex int) (*Latest, error)`

Get the latest value and timestamp for a field by index.

```go
latest, err := db.GetLatest(1)
fmt.Printf("Latest: %.2f at %d\n", latest.Value, latest.Timestamp)
```

### `GetLatestByName(fieldName string) (*Latest, error)`

Get the latest value and timestamp for a field by name.

```go
latest, err := db.GetLatestByName("price")
```

---

### `Close()`

Close the database and free resources.

```go
db.Close()
```

---

### `Drop()`

Close the database and delete all data files.

```go
// WARNING: This permanently deletes all data!
db.Drop()
```

---

## Durability, readers and operations

Data files carry a 64-byte header (`HOC2`) with an atomically committed write cursor and a CRC32C checksum of the committed data; legacy `HOC1` files are migrated in place the first time a writer opens them. A writer holds an exclusive lock on its file, so a second `New` on the same ticker and path fails immediately with an error naming `DatabaseLocked`. Records become visible to readers when the writer commits them (`Flush`, `Sync`, or every append with `FlushOnWrite`); when the committed bytes also reach stable storage is decided by the fsync policy.

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `Fsync string` | `""` = `hocdb.FsyncOnClose` | `hocdb.FsyncNone` (`"none"`): never fsync, the OS decides. `hocdb.FsyncOnClose` (`"on_close"`): once on `Close`. `hocdb.FsyncOnFlush` (`"on_flush"`): after every flush / commit. `hocdb.FsyncInterval` (`"interval"`): at most once per `FsyncIntervalMs`, and on close. The numbers `"0"`..`"3"` are accepted as well; anything else makes `New` return an error. |
| `FsyncIntervalMs uint32` | `0` = 1000 | Period of `FsyncInterval` in milliseconds. |
| `VerifyOnOpen bool` | `false` | Recompute the checksum when opening. A mismatch makes `New` fail with `ChecksumMismatch` instead of opening the corrupted file. |
| `RetentionSpan int64` | `0` = off | Automatic compaction: drop records older than `lastTimestamp - RetentionSpan` (timestamp units) once the excess exceeds 25% of the span. |
| `RolloverSize uint64` | `0` = off | Automatic rollover: archive the file as `<ticker>.<first_ts>-<last_ts>.bin` once it grows above this many bytes and continue with an empty file. |
| `DisableAutoMigrate bool` | `false` | Keep legacy `HOC1` files untouched; opening one then fails with `LegacyFormatNeedsMigration`. (A `DisableAutoMigrate` flag was chosen over an `AutoMigrate *bool` so that the zero `Options{}` keeps the engine default of migrating.) |
| `TimestampUnitNs uint64` | `0` = unknown | Nanoseconds per timestamp unit (`1e9` seconds, `1e6` milliseconds, `1e3` microseconds). Enables the `ingest_lag_record_ns` metric. |
| `IndexStride uint64` | `0` = 1024 | Records per entry of the timestamp index. |

`MaxFileSize`, `OverwriteFull`, `FlushOnWrite` and `AutoIncrement` are unchanged; `New` now calls `hocdb_init_ex` with all of them.

### Ring-buffer capacity

The header takes `hocdb.HeaderSize()` (64) bytes, so a ring buffer of `MaxFileSize = HeaderSize() + N * recordSize` holds exactly N records:

```go
recordSize := 8 + 8 // timestamp + value
ring, err := hocdb.New("TICKS", "./data", schema, hocdb.Options{
    MaxFileSize:   int64(hocdb.HeaderSize() + 50*recordSize), // exactly the last 50 records
    OverwriteFull: true,
})
```

Ring buffers (and legacy files) carry no checksum: `Verify` returns an error wrapping `hocdb.ErrChecksumUnavailable`.

### Readers: an ingestion process and an agent process

Only one writer per file; any number of readers in other processes (or goroutines) attach with `OpenReader`. Readers take no lock, see only committed data, follow files the writer compacts or rolls over, and need the current file format (legacy files are migrated the first time a writer opens them).

```go
// Ingestion process: the single writer
w, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{
    Fsync:           hocdb.FsyncInterval, // fsync at most every 500 ms
    FsyncIntervalMs: 500,
    RolloverSize:    1 << 30,     // archive above 1 GiB
    TimestampUnitNs: 1_000_000,   // microsecond timestamps
})
if err != nil {
    panic(err) // e.g. ... DatabaseLocked when another ingester is running
}
defer w.Close()
for tick := range ticks {
    rec, _ := hocdb.CreateRecordBytes(schema, tick.Timestamp, tick.Price, tick.Volume)
    if err := w.Append(rec); err != nil {
        log.Println(err)
    }
    if tick.EndOfBatch {
        w.Flush() // commit: readers can see the batch from here on
    }
}
```

```go
// Agent process: a lock-free reader
r, err := hocdb.OpenReader("BTC_USD", "./data", schema)
if err != nil {
    panic(err)
}
defer r.Close()

snap, _ := r.Snapshot(nil)             // every read picks up the writer's latest commit
latest, _ := r.GetLatestByName("price")
_ = r.Refresh()                        // explicit refresh, e.g. before a batch of reads
fmt.Println(r.IsReadOnly())            // true

if err := r.Append(rec); errors.Is(err, hocdb.ErrReadOnly) {
    // "append failed: database is read-only: this handle is a reader opened with OpenReader"
}
m, _ := r.Metrics()                    // m["read_only"] == 1, m["refreshes"], m["read_ns_p99"], ...
```

### Sync, verify, compact, retain, rollover

```go
if err := w.Sync(); err != nil { ... }   // flush + fsync now, whatever the policy

ok, err := w.Verify()                    // recompute the CRC32C of the committed data
switch {
case errors.Is(err, hocdb.ErrChecksumUnavailable): // ring buffer or legacy file
case err != nil:
case !ok:                                // MISMATCH: the file is corrupted
}

w.Compact(minTs)                         // keep records with timestamp >= minTs
w.RetainLast(1_000_000)                  // keep the last n records

archive, err := w.Rollover()             // "./data/BTC_USD.<first_ts>-<last_ts>.bin"; the writer continues with an empty file
old, err := hocdb.New(strings.TrimSuffix(filepath.Base(archive), ".bin"), "./data", schema, hocdb.Options{})
```

Compaction, retention and rollover rewrite or replace the file; open readers follow automatically and timestamps stay monotonic across rolled-over files. On a reader these calls return an error wrapping `hocdb.ErrReadOnly`; `Refresh` is a no-op on a writer.

### Metrics

```go
m, err := w.Metrics() // map[string]int64, 30 fields
fmt.Println(m["appends"], m["committed_records"], m["fsyncs"], m["fsync_ns_max"],
    m["ingest_lag_wall_ns"], m["read_ns_p99"], m["file_size"], m["format_version"])
w.MetricsReset()      // counters back to zero; the state fields keep their values
w.FormatVersion()     // 2 (1 = legacy HOC1, only with DisableAutoMigrate)
hocdb.HeaderSize()    // 64
```

Fields (all `int64`; the `uint64` counters fit): `appends`, `bytes_written`, `flushes`, `commits`, `fsyncs`, `fsync_ns_total`, `fsync_ns_max`, `reads`, `read_ns_total`, `read_ns_max`, `read_ns_last`, `read_ns_p50`, `read_ns_p99`, `records_read`, `refreshes`, `recovered_tail_records`, `dropped_tail_bytes`, `crc_failures`, `compactions`, `rollovers`, `migrations`, `last_append_wall_ns`, `last_commit_wall_ns`, `last_record_ts`, `ingest_lag_wall_ns` (now - last commit for readers, now - last append for writers), `ingest_lag_record_ns` (now - last record time; needs `TimestampUnitNs`), `committed_records`, `file_size`, `format_version`, `read_only`. The names come from the C library's introspection API (`hocdb_metrics_field_name`), so new fields appear in the map without a binding change.

### Recovery and checksum

When a writer opens a file, records that a crashed writer appended after its last commit are adopted if they are complete and in timestamp order, and torn or misordered trailing bytes are truncated (`recovered_tail_records` / `dropped_tail_bytes` in `Metrics`). The CRC32C checksum covers the committed data of linear files: `Verify` compares it on demand (`false` = MISMATCH) and `VerifyOnOpen` refuses a mismatching file with `ChecksumMismatch` at open, while a corrupted file opened without `VerifyOnOpen` stays readable and every detected mismatch, at open or by `Verify`, is counted in `crc_failures`.

### API

```go
func New(ticker, path string, schema []Field, options Options) (*DB, error)
func OpenReader(ticker, path string, schema []Field) (*DB, error)
func HeaderSize() int

func (db *DB) Sync() error
func (db *DB) Refresh() error
func (db *DB) Verify() (bool, error)
func (db *DB) Compact(minTs int64) error
func (db *DB) RetainLast(n uint64) error
func (db *DB) Rollover() (string, error)
func (db *DB) Metrics() (map[string]int64, error)
func (db *DB) MetricsReset()
func (db *DB) FormatVersion() int
func (db *DB) IsReadOnly() bool

const FsyncNone, FsyncOnClose, FsyncOnFlush, FsyncInterval = "none", "on_close", "on_flush", "interval"
var ErrReadOnly, ErrChecksumUnavailable error
```

---

## Indicators & analytics

HOCDB computes 83 indicator kinds inside the engine in a single pass over the records and returns them as plain Go slices: the classic technical set (moving averages, RSI, MACD, Bollinger bands, ATR, ADX, Ichimoku, OBV, VWAP, returns, drawdown, Sharpe, ...), tick microstructure (spread, order flow, tick pressure, trade intensity, Amihud, realized volatility), pairs (ratio, ratio z-score, relative strength, correlation, beta), look-ahead labels (forward return, triple barrier) and session-anchored kinds (session VWAP / range, opening range, pivots). Records can be aggregated into OHLCV bars on the fly (tick -> bar), two databases can be computed together (`PairIndicators`), and one-call `Snapshot` / `SnapshotMulti`, `Health` and `Evaluate` give a dashboard or an LLM agent the whole picture. See [INDICATORS.md](../../INDICATORS.md) for the full kind table, default periods, parameters, output names and conventions.

### Batch of named indicators over a time range

```go
specs := []hocdb.IndicatorSpec{
    {Kind: "sma", Period: 20},
    {Kind: "ema", Period: 50, Label: "trend"},
    {Kind: "rsi", Period: 14},
    {Kind: "macd"},                      // defaults 12/26/9 -> macd, macd_signal, macd_hist
    {Kind: "bbands", Period: 20, Param: 2.0},
    {Kind: "sma", Period: 10, Field: "volume"}, // run on another field
}

res, err := db.Indicators(specs, startTs, endTs, nil) // nil = auto-detect OHLCV columns, auto lookback
if err != nil {
    panic(err)
}
fmt.Println(res.Names) // [sma_20 trend rsi_14 macd macd_signal macd_hist bbands_20_upper ... sma_10]
for i := 0; i < res.NRows; i++ {
    fmt.Printf("%d close-sma=%.2f rsi=%.1f macd_hist=%.4f\n",
        res.Timestamps[i], res.Columns["sma_20"][i], res.Columns["rsi_14"][i], res.Columns["macd_hist"][i])
}

// Last 100 records only
tail, err := db.IndicatorsTail(100, specs, nil)
```

### Snapshot for an LLM agent

```go
snap, err := db.Snapshot(&hocdb.SnapshotOptions{PeriodsPerYear: 365})
if err != nil {
    panic(err)
}
fmt.Printf("as of %d (%d bars): close=%.2f rsi_14=%.1f ema_200=%.2f adx_14=%.1f supertrend_dir=%.0f\n",
    snap.Timestamp, snap.Bars, snap.Fields["close"], snap.Fields["rsi_14"],
    snap.Fields["ema_200"], snap.Fields["adx_14"], snap.Fields["supertrend_dir"])

// Feed the whole picture to a model as JSON. Fields that are not defined yet
// are NaN (kept as NaN by the binding); JSON has no NaN, so map them to null.
fields := make(map[string]interface{}, len(snap.Fields))
for name, v := range snap.Fields {
    if math.IsNaN(v) {
        fields[name] = nil
    } else {
        fields[name] = v
    }
}
payload, _ := json.Marshal(map[string]interface{}{
    "timestamp": snap.Timestamp,
    "bars":      snap.Bars,
    "fields":    fields, // ~100 named values
})

// Several bar sizes from one read: one *Snapshot per bucket, in Buckets order
snaps, err := db.SnapshotMulti(&hocdb.SnapshotMultiOptions{
    Buckets:        []int64{60_000_000, 300_000_000, 3_600_000_000}, // 1 min, 5 min, 1 h on microsecond timestamps
    PeriodsPerYear: []float64{525600, 105120, 8760},                // same length as Buckets (nil = none)
    Bars:           500,                                            // per snapshot; 0 = 2500
})
fmt.Println(snaps[0].Fields["rsi_14"], snaps[2].Fields["rsi_14"])
```

### Tick data -> bars: `Bucket`, `OHLCV` and `OHLCVSide`

```go
// Indicators on 5-minute bars built from raw ticks (schema: timestamp, price, volume)
res, err := db.IndicatorsTail(200, []hocdb.IndicatorSpec{
    {Kind: "vwap"},
    {Kind: "atr", Period: 14},
    {Kind: "supertrend"},
}, &hocdb.IndicatorOptions{
    Columns: &hocdb.IndicatorColumns{Close: "price", Volume: "volume"}, // open/high/low come from the bars
    Bucket:  300,                                                        // timestamp units per bar
})

// Or just the bars themselves
bars, err := db.OHLCV(startTs, endTs, 300, "price", "volume") // volume field may be ""
for i := range bars.Timestamps {
    fmt.Printf("%d O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f (%.0f ticks)\n", bars.Timestamps[i],
        bars.Open[i], bars.High[i], bars.Low[i], bars.Close[i], bars.Volume[i], bars.Count[i])
}

// Bars with the buy-side volume per bar from a side field (true / 1 = buy).
// Bars.BuyVolume is only set by OHLCVSide; OHLCV leaves it nil.
bars, err = db.OHLCVSide(startTs, endTs, 60_000_000, "price", "size", "side")
fmt.Printf("buy %.0f of %.0f\n", bars.BuyVolume[0], bars.Volume[0])

// Scalar performance / risk summary of a field (29 metrics)
sum, err := db.Summary(startTs, endTs, "price", 365)
fmt.Printf("return=%.2f%% sharpe=%.2f max_drawdown=%.2f%% win_rate=%.2f\n",
    100*sum["total_return"], sum["sharpe"], 100*sum["max_drawdown"], sum["win_rate"])
```

### Tick databases: `bid` / `ask` / `side` roles and microstructure

`IndicatorColumns` has three tick-level roles besides open/high/low/close/volume: `Bid`, `Ask` and `Side` (a bool or 0/1 field, 1 = buy). With `Columns == nil` they are auto-detected from fields named `bid`, `ask` and `side`, `price` stands in for `close`, and `size` or `qty` for `volume`.

```go
// schema: timestamp, price, size, bid, ask, side (bool) -- everything auto-detected
res, err := db.IndicatorsTail(100, []hocdb.IndicatorSpec{
    {Kind: "spread"},                                  // spread_abs, spread_bps (needs bid + ask)
    {Kind: "order_flow", Period: 10},                  // order_flow_10_net, order_flow_10_imbalance (needs volume + side)
    {Kind: "trade_intensity", Period: 10, Param: 1e6}, // Param = timestamp units per second (default 1e6)
    {Kind: "tick_pressure", Period: 20},
    {Kind: "amihud", Period: 20},
    {Kind: "realized_vol", Period: 60, Param: 31_536_000}, // Param = periods per year
    {Kind: "session_vwap", Param: 3_600e6},            // Param = session length in timestamp units (mandatory)
    {Kind: "forward_return", Period: 5},               // label: look-ahead, see below
}, nil)

// Explicit roles instead of auto-detection
opts := &hocdb.IndicatorOptions{Columns: &hocdb.IndicatorColumns{
    Close: "price", Volume: "size", Bid: "bid", Ask: "ask", Side: "side",
}}
```

With `Bucket > 0` the microstructure kinds run on the bars: `order_flow` then uses the per-bar buy volume derived from the `Side` role.

### Pairs: indicators over two databases

`PairIndicators` / `PairIndicatorsTail` compute over this database (A) aligned with another open `*hocdb.DB` (B). B's close column is the second series of `series2`, `ratio`, `ratio_zscore`, `rel_strength`, `correl` and `beta`; single-series kinds run on A. On ticks B is as-of joined onto A's rows (the latest B row at or before each A row); with `Bucket > 0` both databases are resampled and inner-joined on bar timestamps.

```go
res, err := btc.PairIndicatorsTail(eth, 50, []hocdb.IndicatorSpec{
    {Kind: "series"},                      // A's close (passthrough)
    {Kind: "series2"},                     // B's close, aligned to A
    {Kind: "ratio"},                       // series / series2
    {Kind: "ratio_zscore", Period: 100},
    {Kind: "correl", Period: 30},
    {Kind: "rel_strength", Period: 10},
}, nil) // nil = auto-detect columns on both sides, auto lookback, ticks

// On 1-minute bars over a time range, with explicit roles for each side
res, err = btc.PairIndicators(eth, specs, startTs, endTs, &hocdb.PairOptions{
    IndicatorOptions: hocdb.IndicatorOptions{Bucket: 60_000_000, Columns: &hocdb.IndicatorColumns{Close: "price"}},
    OtherColumns:     &hocdb.IndicatorColumns{Close: "px"}, // roles of the other database; nil = auto-detect
})
```

In single-database calls the second series is `IndicatorSpec.Field2` instead.

### Data quality and decision evaluation

```go
// Health: count, first_ts, last_ts, span, mean_gap, median_gap, max_gap, max_gap_at, n_gaps (gaps above the
// threshold), n_nonpositive_price, n_nan_price, n_outlier_returns (|log return| above the threshold),
// first_outlier_at, max_abs_return, n_zero_volume, n_negative_volume (volume field may be "")
h, err := db.Health(startTs, endTs, "price", "size", 5_000_000, 0.05)
fmt.Println(h["count"], h["n_gaps"], h["median_gap"], h["n_outlier_returns"], h["last_ts"])

// Evaluate decisions against the price history: entry at the first price at or after Timestamp, exit at the
// first price at or after Timestamp + Horizon (0 = default horizon), costBps per side. Size 0 counts as 1.
ev, err := db.Evaluate([]hocdb.Decision{
    {Timestamp: t0, Direction: 1, Size: 1000, Horizon: 60_000_000}, // long for a minute
    {Timestamp: t1, Direction: -1, Size: 500},                       // short, default horizon
}, "price", 120_000_000, 5)
fmt.Println(ev.Fields["n_evaluated"], ev.Fields["hit_rate"], ev.Fields["total_pnl"], ev.Fields["sharpe"])
fmt.Println(ev.Entry, ev.Exit, ev.NetReturn) // per decision, NaN where not evaluated (flat, or no price after the exit time)
```

Both return the names reported by the C library's introspection (16 health fields, 20 evaluation fields). Counts and timestamps are `float64` values in the map; they are exact up to 2^53, so `int64(h["last_ts"])` is safe.

**Look-ahead warning.** `forward_return` (outputs `ret`, `max`, `min`; `Period` = horizon) and `triple_barrier` (`label`, `ret`, `bars`; `Param` = up fraction, default 0.02, `Param2` = down fraction = up) are labels: they read the rows AFTER each row, so the last `Period` rows of every result are NaN. Use them as training targets or to score decisions, never as features of the same row. `hocdb.IndicatorIsLookahead(kind)` reports the flag.

**Session kinds need `Param`.** `session_vwap`, `session_range` (`open`, `high`, `low`, `ret`), `opening_range` (`high`, `low`, `breakout`; `Period` = rows) and `pivots` (`pp`, `r1`, `s1`, `r2`, `s2`) anchor on sessions of `Param` timestamp units (e.g. `86_400e6` for daily sessions on microsecond timestamps) starting at offset `Param2`. `Param` is mandatory: without it the call returns the usual validation error.

### API

```go
func (db *DB) Indicators(specs []IndicatorSpec, startTs, endTs int64, opts *IndicatorOptions) (*IndicatorResult, error)
func (db *DB) IndicatorsTail(n int, specs []IndicatorSpec, opts *IndicatorOptions) (*IndicatorResult, error)
func (db *DB) PairIndicators(other *DB, specs []IndicatorSpec, startTs, endTs int64, opts *PairOptions) (*IndicatorResult, error)
func (db *DB) PairIndicatorsTail(other *DB, n int, specs []IndicatorSpec, opts *PairOptions) (*IndicatorResult, error)
func (db *DB) OHLCV(startTs, endTs int64, bucket int64, priceField, volumeField string) (*Bars, error)
func (db *DB) OHLCVSide(startTs, endTs int64, bucket int64, priceField, volumeField, sideField string) (*Bars, error)
func (db *DB) Summary(startTs, endTs int64, field string, periodsPerYear float64) (map[string]float64, error)
func (db *DB) Health(startTs, endTs int64, priceField, volumeField string, gapThreshold int64, outlierThreshold float64) (map[string]float64, error)
func (db *DB) Evaluate(decisions []Decision, priceField string, defaultHorizon int64, costBps float64) (*Evaluation, error)
func (db *DB) Snapshot(opts *SnapshotOptions) (*Snapshot, error)
func (db *DB) SnapshotMulti(opts *SnapshotMultiOptions) ([]*Snapshot, error)

func IndicatorKinds() []string                 // all 83 kind names ("sma", "ema", ..., "pivots")
func IndicatorOutputs(kind string) []string    // e.g. "macd" -> [macd signal hist]; nil if unknown
func IndicatorIsLookahead(kind string) bool    // true for forward_return / triple_barrier
func IndicatorWarmup(spec IndicatorSpec) int   // recommended warm-up rows for a spec
func Lookback(n int) *int                      // helper for IndicatorOptions.Lookback
```

`IndicatorSpec` fields: `Kind` (name, case-insensitive), `Period`, `Period2`, `Period3`, `Period4`, `Param`, `Param2`, `Field`, `Field2`, `Label`. Zero periods/params select the documented defaults. `Param` is the BBANDS k, KELTNER/SUPERTREND multiplier, PSAR acceleration, periods-per-year for HIST_VOL/SHARPE/SORTINO/REALIZED_VOL, timestamp units per second for TRADE_INTENSITY, the up-barrier fraction for TRIPLE_BARRIER, or the session length for the session kinds; `Param2` is the PSAR max acceleration, the TRIPLE_BARRIER down-barrier fraction, or the session offset. `Field`/`Field2` name schema fields (`""` = the close column); `Field2` is the second series for SERIES2/RATIO/RATIO_ZSCORE/REL_STRENGTH/CORREL/BETA in single-database calls.

`IndicatorResult`: `Timestamps []int64`, `NRows int`, `Names []string` (output order) and `Columns map[string][]float64`. `Bars`: `Timestamps`, `Open`, `High`, `Low`, `Close`, `Volume`, `Count` and `BuyVolume` (nil unless `OHLCVSide`). `Decision`: `Timestamp int64`, `Direction float64` (+1/-1/0), `Size float64` (0 = 1), `Horizon int64` (0 = default). `Evaluation`: `Fields map[string]float64`, `Entry`, `Exit`, `NetReturn []float64`.

**Options** (`IndicatorOptions`; `PairOptions` embeds it and adds `OtherColumns`; `SnapshotOptions` / `SnapshotMultiOptions` where marked):

| Option | Default | Meaning |
|--------|---------|---------|
| `Columns *IndicatorColumns` | `nil` = auto-detect | Field names for the open/high/low/close/volume/bid/ask/side roles. Auto-detection uses fields literally named `open`/`high`/`low`/`close`/`volume`/`bid`/`ask`/`side`; `price` is used as close when there is no `close`, `size` or `qty` as volume when there is no `volume`. `Close` is required; leave the others `""` if absent (indicators that need them return an error). |
| `OtherColumns *IndicatorColumns` (pairs) | `nil` = auto-detect | The same mapping for the other database of `PairIndicators`. |
| `Lookback *int` | `nil` = auto | Extra records (bars when `Bucket > 0`) read before the window so the first in-window values are converged. `nil` or `hocdb.LookbackAuto` (-1) = recommended per-spec warm-up; `hocdb.Lookback(0)` = none (warm-up rows inside the window are NaN); `hocdb.Lookback(n)` = exactly n. Warm-up rows are never returned. |
| `Bucket int64` | `0` | `0` = one row per record; `> 0` = records are first aggregated into OHLCV bars of that many timestamp units. Per-spec `Field` overrides are rejected in that mode. (Also in `SnapshotOptions`; `SnapshotMultiOptions.Buckets` is the list variant.) |
| `Bars int` (snapshot) | `0` = 2500 | Records (or bars) the snapshot is computed from; 2500 is enough for every field to converge. |
| `PeriodsPerYear float64` (snapshot) | `0` = none | Annualisation for volatility / Sharpe / Sortino. (`SnapshotMultiOptions.PeriodsPerYear []float64`, one per bucket.) |

**Naming rule.** The label of a spec is `Label` if given, otherwise the kind name plus `_<Period>` when `Period > 0` (`sma_20`, `rsi_14`, `macd`, `bbands`, `spread`, `order_flow_10`). Single-output kinds use the label as the column name; multi-output kinds use `<label>_<output>` (`macd_signal`, `bbands_20_upper`, `stoch_14_k`, `spread_bps`, `order_flow_10_imbalance`, `forward_return_5_ret`), except that the output named like the kind itself (`macd`, `ppo`, `adx`, `tsi`) is just the label — so MACD yields `macd`, `macd_signal`, `macd_hist`. `IndicatorOutputs(kind)` lists a kind's output names (single-output kinds report `value`). Two specs must not produce the same column name; set `Label` to disambiguate.

**Warm-up / NaN.** A value that is not yet defined (the indicator has not seen enough data) is `NaN`, never zero. With the default automatic lookback the engine reads enough history before the window for the first returned rows to be converged when that history exists; at the very start of the data set, or with `Lookback(0)`, the leading rows of a column are NaN. In `Snapshot`, fields that need more bars than available are NaN (e.g. `sma_200` with 50 bars). Label kinds are NaN at the END of the window instead (look-ahead).

**Memory.** Results are copied into Go slices and the C buffers are freed before the call returns; nothing needs to be released by the caller.

---

## Complete Example

```go
package main

import (
    "fmt"
    "hocdb"
)

func main() {
    // Define schema
    schema := []hocdb.Field{
        {Name: "timestamp", Type: hocdb.TypeI64},
        {Name: "price", Type: hocdb.TypeF64},
        {Name: "volume", Type: hocdb.TypeF64},
        {Name: "is_buy", Type: hocdb.TypeBool},
    }

    // Initialize with ring buffer
    db, err := hocdb.New("ETH_USD", "./market_data", schema, hocdb.Options{
        MaxFileSize:   100 * 1024 * 1024, // 100MB
        OverwriteFull: true,
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Append some trades
    trades := []struct {
        ts     int64
        price  float64
        volume float64
        isBuy  bool
    }{
        {1620000000, 2500.0, 10.0, true},
        {1620000001, 2501.5, 5.0, false},
        {1620000002, 2502.0, 15.0, true},
    }

    for _, t := range trades {
        record, _ := hocdb.CreateRecordBytes(schema, t.ts, t.price, t.volume, t.isBuy)
        if err := db.Append(record); err != nil {
            fmt.Printf("Append failed: %v\n", err)
        }
    }
    db.Flush()

    // Query buy orders only
    filters := map[string]interface{}{
        "is_buy": true,
    }
    data, _ := db.Query(1620000000, 1620001000, filters)
    fmt.Printf("Buy orders: %d bytes\n", len(data))

    // Get price statistics with percentiles
    stats, _ := db.GetStatsByName(1620000000, 1620001000, "price", true)
    fmt.Printf("Price: min=%.2f, max=%.2f, mean=%.2f, p99=%.2f\n",
        stats.Min, stats.Max, stats.Mean, stats.P99)

    // Get latest price
    latest, _ := db.GetLatestByName("price")
    fmt.Printf("Latest: %.2f at %d\n", latest.Value, latest.Timestamp)

    // Load all data
    allData, _ := db.Load()
    fmt.Printf("Total data: %d bytes\n", len(allData))
}
```

## Architecture

The Go bindings use CGO to interface with the underlying C library:

- **CGO CFLAGS**: `-I../../bindings/c` (to find hocdb.h)
- **CGO LDFLAGS**: `-L../../zig-out/lib -lhocdb_c` (to link with the C library)

## Testing

```bash
cd bindings/go
go mod tidy
go test -v
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

```go
db, err := hocdb.New("AAPL", "./data", schema, &hocdb.Options{CalendarName: "nyse", TimestampUnitNs: 1000})
db.Calendar()                  // 3
db.PeriodsPerYear(60_000_000)  // 98280

// session kinds with param 0 follow the exchange sessions
db.Indicators([]hocdb.Spec{{Kind: "session_vwap", Param: 0}, {Kind: "pivots", Param: 0}}, opts)

params := hocdb.DefaultBacktestParams()
params.InitialEquity, params.CostBps, params.StopLoss, params.PositionMode = 100000, 5, 0.02, 1
rep, err := db.Backtest(target, startTs, endTs, 60_000_000, &params,
    &hocdb.BacktestOptions{Equity: true, MaxTrades: 1000})
rep.Result.Sharpe; rep.Trades[0].ExitReason

splits := hocdb.WalkForwardSplits(len(close), 5, 0.6, true)
perWindow, err := hocdb.BacktestSplits(ts, open, high, low, close, target, splits, &params)

u, err := hocdb.Universe([]*hocdb.DB{dbA, dbB, dbC}, cols, 500, 60_000_000, nil)
u.Rows[0].RankMomMid; u.Summary.BreadthUp; u.Corr[0][1]
```

Package-level calendar helpers: `CalendarID`, `CalendarName`, `CalendarSession`,
`CalendarSessionForDay`, `CalendarIsOpen`, `CalendarOpenSeconds`,
`CalendarSessionsBetween`, `CalendarPeriodsPerYear`, `CalendarToLocal`,
`DaysFromCivil`, `CivilFromDays`, `CalendarDefine`; per handle `SetCalendar`,
`SetCalendarName`, `Calendar`, `SetTimestampUnit`, `TimestampUnit`,
`PeriodsPerYear`, `Backtest`, `BacktestTail`.
