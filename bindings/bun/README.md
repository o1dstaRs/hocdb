# HOCDB Bun Bindings

Bun bindings for HOCDB - The World's Most Performant Time-Series Database, using `bun:ffi` for high-performance native library interaction.

## Prerequisites

Before using the Bun bindings, build the C library:

```bash
# From the main HOCDB directory
zig build c-bindings
```

## Installation

```bash
cd bindings/bun
bun install
```

## Quick Start

```typescript
import { HOCDBAsync } from "./index.ts";

// Define schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "volume", type: "f64" },
    { name: "active", type: "bool" }
];

// Create async database instance (recommended)
const db = new HOCDBAsync("BTC_USD", "./data", schema);

// Append records
await db.append({ timestamp: 1620000000n, price: 50000.0, volume: 1.5, active: 1 });
await db.append({ timestamp: 1620000001n, price: 50100.0, volume: 2.0, active: 1 });

// Flush to disk
await db.flush();

// Query data
const results = await db.query(1620000000n, 1620000100n);
console.log(`Found ${results.length} records`);

// Get statistics
const stats = await db.getStats(1620000000n, 1620000100n, 1); // field index
console.log(`Min: ${stats.min}, Max: ${stats.max}`);

// Get latest value
const latest = await db.getLatest(1);
console.log(`Latest price: ${latest.value}`);

// Close when done
await db.close();
```

## API Reference

### Field Types

| Type | Description | Size |
|------|-------------|------|
| `"i64"` | Signed 64-bit integer | 8 bytes |
| `"f64"` | 64-bit floating point | 8 bytes |
| `"u64"` | Unsigned 64-bit integer | 8 bytes |
| `"bool"` | Boolean | 1 byte |

### Schema Definition

```typescript
import { FieldDef } from "./index.ts";

const schema: FieldDef[] = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "volume", type: "f64" },
    { name: "is_buy", type: "bool" }
];
```

### Configuration Options

```typescript
import { DBConfig } from "./index.ts";

const config: DBConfig = {
    max_file_size: 1024 * 1024 * 100,  // 100MB (0 or omit for default); ring capacity = (max_file_size - 64) / record_size
    overwrite_on_full: true,            // Ring buffer mode
    flush_on_write: false,              // Flush on every write
    auto_increment: false,              // Auto-increment timestamps
    fsync: "on_flush",                  // durability: "none" | "on_close" (default) | "on_flush" | "interval"
    timestampUnitNs: 1e6,               // timestamps are milliseconds (enables ingest-lag metrics)
};
```

The durability, retention, rollover and reader options (`fsync`, `fsync_interval_ms`, `verify_on_open`, `retention_span`, `rollover_size`, `auto_migrate`, `timestamp_unit_ns`, `index_stride`, `read_only`; camelCase aliases are accepted for every option) are described in [Durability, readers and operations](#durability-readers-and-operations).

---

## Synchronous API (`HOCDB`)

For use cases where blocking is acceptable.

### `new HOCDB(ticker, path, schema, config?)`

Create a synchronous database instance.

```typescript
import { HOCDB } from "./index.ts";

const db = new HOCDB("BTC_USD", "./data", schema, config);
```

A writer holds an exclusive lock on its data file: opening a second writer on the same ticker and path throws immediately (`... DatabaseLocked`). The error message of every failed open ends with the engine's error name (`DatabaseLocked`, `SchemaMismatch`, `ChecksumMismatch`, `LegacyFormatNeedsMigration`, `EmptyDatabase`, `FileNotFound`, ...).

### `HOCDB.openReader(ticker, path, schema)`

Attach to a database that another process writes, without any lock. The reader supports every read method (`query`, `load`, `getStats`, `getLatest`, indicators, `summary`, `snapshot`, `health`, `evaluate`, ...) plus `refresh()` and `isReadOnly()`; `append` and the maintenance methods throw a read-only error. See [Durability, readers and operations](#durability-readers-and-operations).

```typescript
const reader = HOCDB.openReader("BTC_USD", "./data", schema);
reader.isReadOnly(); // true
```

### `db.append(data)`

Append a record to the database.

```typescript
db.append({
    timestamp: 1620000000n,  // Use BigInt for i64
    price: 50000.0,
    volume: 1.5,
    active: 1  // Use 1/0 for booleans
});
```

### `db.flush()`

Force buffered data to be written to disk.

```typescript
db.flush();
```

### `db.load()`

Load all records from the database.

```typescript
const records = db.load();
for (const record of records) {
    console.log(`Price at ${record.timestamp}: ${record.price}`);
}
```

### `db.query(startTs, endTs, filters?)`

Query records within a timestamp range with optional filters.

```typescript
// Query all records in time range
const results = db.query(1620000000n, 1620000100n);

// With filter (using field name)
const results = db.query(1620000000n, 1620000100n, { price: 50000.0 });

// With filter (using field index)
const results = db.query(1620000000n, 1620000100n, [
    { field_index: 1, value: 50000.0 }
]);
```

### `db.queryRaw(startTs, endTs, filters?)`

Query records and return raw bytes (for advanced use cases).

```typescript
const buffer: ArrayBuffer = db.queryRaw(1620000000n, 1620000100n);
```

### `db.queryInto(startTs, endTs, filters, buffer)`

Query records into a pre-allocated buffer (zero-copy optimization).

```typescript
const buffer = new Uint8Array(1024 * 1024); // 1MB buffer
const bytesWritten = db.queryInto(1620000000n, 1620000100n, {}, buffer);
console.log(`Wrote ${bytesWritten} bytes`);
```

### `db.getStats(startTs, endTs, field, options?)`

Get statistics for a specific field within a time range.

**Parameters:**
- `startTs` (bigint): Start timestamp
- `endTs` (bigint): End timestamp
- `field` (string or number): Field name or index
- `options` (optional): `{ percentiles: true }` to compute percentiles

**Returns:** Object with `min`, `max`, `sum`, `count`, `mean`, and optionally `p50`, `p90`, `p95`, `p99`

```typescript
// Basic stats
const stats = db.getStats(1620000000n, 1620000100n, "price");
console.log(`Min: ${stats.min}, Max: ${stats.max}`);

// With percentiles
const stats = db.getStats(1620000000n, 1620000100n, "price", { percentiles: true });
console.log(`P99: ${stats.p99}`);

// Using field index
const stats = db.getStats(1620000000n, 1620000100n, 1);
```

### `db.getLatest(field)`

Get the most recent value and timestamp for a specific field.

```typescript
const latest = db.getLatest("price");
console.log(`Latest: ${latest.value} at ${latest.timestamp}`);

// Using field index
const latest = db.getLatest(1);
```

### `db.close()`

Close the database handle.

```typescript
db.close();
```

### `db.drop()`

Close the database and delete all data files.

```typescript
// WARNING: This permanently deletes all data!
db.drop();
```

---

## Asynchronous API (`HOCDBAsync`) - Recommended

Uses worker threads to avoid blocking the main thread.

### `new HOCDBAsync(ticker, path, schema, config?)`

Create an async database instance.

```typescript
import { HOCDBAsync } from "./index.ts";

const db = new HOCDBAsync("BTC_USD", "./data", schema, config);
```

### `db.append(data)`

Append a record asynchronously.

```typescript
await db.append({
    timestamp: 1620000000n,
    price: 50000.0,
    volume: 1.5,
    active: 1
});
```

### `db.appendBatch(records)`

Append multiple records in a single operation.

```typescript
await db.appendBatch([
    { timestamp: 1620000000n, price: 50000.0, volume: 1.5, active: 1 },
    { timestamp: 1620000001n, price: 50100.0, volume: 2.0, active: 1 },
    { timestamp: 1620000002n, price: 50200.0, volume: 1.0, active: 0 }
]);
```

### `db.flush()`

Flush to disk asynchronously.

```typescript
await db.flush();
```

### `db.load()`

Load all records asynchronously.

```typescript
const records = await db.load();
```

### `db.query(startTs, endTs, filters?)`

Query records asynchronously.

```typescript
const results = await db.query(1620000000n, 1620000100n, { active: 1 });
```

### `db.getStats(startTs, endTs, fieldIndex)`

Get statistics asynchronously.

```typescript
const stats = await db.getStats(1620000000n, 1620000100n, 1);
```

### `db.getLatest(fieldIndex)`

Get latest value asynchronously.

```typescript
const latest = await db.getLatest(1);
```

### `db.close()`

Close the database asynchronously.

```typescript
await db.close();
```

### `db.drop()`

Close and delete data files asynchronously.

```typescript
await db.drop();
```

---

## Complete Example

```typescript
import { HOCDBAsync, FieldDef, DBConfig } from "./index.ts";

const schema: FieldDef[] = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "volume", type: "f64" },
    { name: "is_buy", type: "bool" }
];

const config: DBConfig = {
    max_file_size: 1024 * 1024 * 100,  // 100MB
    overwrite_on_full: true
};

async function main() {
    const db = new HOCDBAsync("ETH_USD", "./market_data", schema, config);

    try {
        // Batch insert trades
        await db.appendBatch([
            { timestamp: 1620000000n, price: 2500.0, volume: 10.0, is_buy: 1 },
            { timestamp: 1620000001n, price: 2501.5, volume: 5.0, is_buy: 0 },
            { timestamp: 1620000002n, price: 2502.0, volume: 15.0, is_buy: 1 }
        ]);

        await db.flush();

        // Query buy orders only (using field index for is_buy)
        const buyOrders = await db.query(1620000000n, 1620000100n, { is_buy: 1 });
        console.log(`Buy orders: ${buyOrders.length}`);

        // Get price statistics
        const stats = await db.getStats(1620000000n, 1620000100n, 1);
        console.log(`Price: min=${stats.min}, max=${stats.max}, mean=${stats.mean}`);

        // Get latest price
        const latest = await db.getLatest(1);
        console.log(`Latest price: ${latest.value} at ${latest.timestamp}`);

        // Load all records
        const allRecords = await db.load();
        console.log(`Total records: ${allRecords.length}`);

    } finally {
        await db.close();
    }
}

main();
```

## TypeScript Types

The bindings include full TypeScript definitions:

```typescript
export interface DBConfig {
    max_file_size?: number | bigint;
    overwrite_on_full?: boolean;
    flush_on_write?: boolean;
    auto_increment?: boolean;
    fsync?: "none" | "on_close" | "on_flush" | "interval" | 0 | 1 | 2 | 3;
    fsync_interval_ms?: number;
    verify_on_open?: boolean;
    retention_span?: number | bigint;
    rollover_size?: number | bigint;
    auto_migrate?: boolean;
    timestamp_unit_ns?: number | bigint;
    index_stride?: number | bigint;
    read_only?: boolean;
    // ... plus the camelCase aliases (maxFileSize, fsyncIntervalMs, verifyOnOpen, retentionSpan, rolloverSize, autoMigrate, timestampUnitNs, indexStride, readOnly)
}

export interface FieldDef {
    name: string;
    type: 'i64' | 'f64' | 'u64' | 'bool';
}

export interface Filter {
    field_index: number;
    value: number | bigint | string;
}
```

The indicator API exports `IndicatorSpec`, `IndicatorOptions`, `PairIndicatorOptions`, `IndicatorColumns`, `IndicatorResult`, `OhlcvOptions`, `OhlcvResult`, `SummaryResult`, `SnapshotOptions`, `SnapshotMultiOptions`, `SnapshotResult`, `HealthResult`, `Decision`, `EvaluateOptions` and `EvaluationResult` (see [Indicators & analytics](#indicators--analytics)); the storage API exports `FsyncPolicy` and `MetricsResult` (see [Durability, readers and operations](#durability-readers-and-operations)).

## Important Notes

- **BigInt for integers**: Use `BigInt` (e.g., `1620000000n`) for `i64` and `u64` fields
- **Booleans as numbers**: Use `1` and `0` instead of `true`/`false` for boolean fields
- **Async preferred**: Use `HOCDBAsync` for production to avoid blocking
- **Buffer optimization**: Use `queryInto()` for zero-copy queries when performance is critical
- **One writer, many readers**: a writer locks its file; other processes attach with `HOCDB.openReader()` and see the data the writer has flushed

## Indicators & analytics

The synchronous `HOCDB` class can compute 83 technical indicators (moving averages, momentum, trend, volatility, volume, statistics / risk, price transforms, tick microstructure, pairs, session-anchored levels and training labels) and quantitative analytics (summary, snapshot, health, decision evaluation) directly inside the database, in a single pass over the stored records. Results are returned as `Float64Array`s (one per output column) plus a `BigInt64Array` of timestamps, so they can be handed straight to charting or ML code without any parsing. Indicator kinds and output names are resolved at runtime from the native registry, so the binding never needs to be updated when the core gains a new indicator.

The full kind table (ids, parameters, defaults, required columns, outputs, warm-up lengths) and the numerical conventions live in [../../INDICATORS.md](../../INDICATORS.md).

### Batch of indicators over a time range

```typescript
import { HOCDB } from "./index.ts";

const db = new HOCDB("BTC_USD", "./data", [
    { name: "timestamp", type: "i64" },
    { name: "open", type: "f64" },
    { name: "high", type: "f64" },
    { name: "low", type: "f64" },
    { name: "close", type: "f64" },
    { name: "volume", type: "f64" },
]);

// Columns are auto-detected from the field names open/high/low/close/volume.
const res = db.indicators([
    { kind: "sma", period: 20 },
    { kind: "ema", period: 50, label: "trend" },       // custom column name
    { kind: "rsi", period: 14 },
    { kind: "macd" },                                   // defaults: 12/26/9
    { kind: "bbands", period: 20, param: 2 },           // param = k
    { kind: "atr", period: 14 },
    { kind: "sma", period: 10, field: "volume" },       // run on another field
], { start: 1620000000n, end: 1620086400n });

console.log(res.n_rows, res.names);
// [ "sma_20", "trend", "rsi_14", "macd", "macd_signal", "macd_hist",
//   "bbands_20_upper", "bbands_20_middle", "bbands_20_lower",
//   "bbands_20_percent_b", "bbands_20_bandwidth", "atr_14", "sma_10" ]
const rsi: Float64Array = res.columns["rsi_14"];
const ts: BigInt64Array = res.timestamps;
for (let i = 0; i < res.n_rows; i++) {
    if (rsi[i] < 30) console.log(`oversold at ${ts[i]}: rsi=${rsi[i].toFixed(1)}`);
}

// Last 200 rows instead of a time range:
const tail = db.indicatorsTail(200, [{ kind: "supertrend" }, { kind: "adx", period: 14 }]);
console.log(tail.columns["supertrend_dir"][199], tail.columns["adx_14"][199]);
```

### Snapshot for an LLM / trading agent

`snapshot()` computes ~100 indicator values for the latest bar in one call and returns a flat object (`timestamp` is a `bigint`, `bars` a `number`, everything else a `number`) that can be serialised into a prompt or a feature vector.

```typescript
const snap = db.snapshot({ periodsPerYear: 365 * 24 * 12 }); // 5-minute bars

console.log(snap.timestamp, snap.bars, snap.close);
console.log(snap.rsi_14, snap.macd_hist, snap.bb_percent_b, snap.adx_14, snap.supertrend_dir);

// Feed everything to an agent (bigint -> string for JSON)
const features = JSON.stringify(snap, (_, v) => typeof v === "bigint" ? v.toString() : v);

// Snapshot of the last 500 five-minute bars built from raw ticks:
const snap5m = db.snapshot({ bars: 500, bucket: 300 });
```

Scalar performance / risk statistics of any field over a window (29 fields: `count`, `first`, `last`, `min`, `max`, `mean`, `std`, `total_return`, `ann_return`, `ann_vol`, `sharpe`, `sortino`, `max_drawdown`, `calmar`, `skew`, `kurtosis`, `var_95`, `cvar_95`, `win_rate`, `profit_factor`, `hurst`, `half_life`, ...):

```typescript
const sum = db.summary(1620000000n, 1620086400n, "close", 252); // periodsPerYear = 252
console.log(sum.count, sum.sharpe, sum.max_drawdown, sum.win_rate);
```

### Tick -> bar: `bucket` and `ohlcv()`

Databases that store raw ticks (a `price` field, optionally `volume`) can be aggregated into OHLCV bars on the fly. With `bucket > 0` every indicator runs on bars of `bucket` timestamp units (floor-aligned to the bucket); `ohlcv()` returns the bars themselves.

```typescript
const ticks = new HOCDB("BTC_TICKS", "./ticks", [
    { name: "timestamp", type: "i64" },   // milliseconds
    { name: "price", type: "f64" },       // used as close when there is no "close" field
    { name: "size", type: "f64" },
]);

// 1-minute bars from ticks, then indicators on the bars
const bars = ticks.indicatorsTail(500, [
    { kind: "ema", period: 21 },
    { kind: "vwap" },
    { kind: "atr", period: 14 },
], { bucket: 60_000, columns: { volume: "size" } });

// The bars themselves
const b = ticks.ohlcv(1620000000000n, 1620086400000n, 60_000, { price: "price", volume: "size" });
console.log(b.n_bars, b.timestamps[0], b.open[0], b.high[0], b.low[0], b.close[0], b.volume[0], b.count[0]);

// With a trade-side field (1 = buy, 0 = sell) every bar also carries `buy_volume`
// (absent from the result unless `side` is given):
const bs = ticks.ohlcv(1620000000000n, 1620086400000n, 60_000, { price: "price", volume: "size", side: "side" });
console.log(bs.buy_volume![0] / bs.volume[0]); // buy share of the first bar
```

### Ticks with quotes: microstructure, session and label indicators

Tick databases with `bid` / `ask` / `side` fields (`side`: `1` = buy, `0` = sell, typically a `bool` field) unlock the microstructure kinds. The column roles are auto-detected by name (`bid`, `ask`, `side`; `price` is used as close when there is no `close`, and `size` or `qty` as volume when there is no `volume`) or given explicitly in `columns`.

```typescript
const ticks = new HOCDB("BTC_TICKS", "./ticks", [
    { name: "timestamp", type: "i64" },   // microseconds
    { name: "price", type: "f64" },
    { name: "size", type: "f64" },
    { name: "bid", type: "f64" },
    { name: "ask", type: "f64" },
    { name: "side", type: "bool" },
]);

const res = ticks.indicatorsTail(1000, [
    { kind: "spread" },                                    // spread_abs, spread_bps               (bid + ask)
    { kind: "order_flow", period: 50 },                    // order_flow_50_net, order_flow_50_imbalance (volume + side)
    { kind: "trade_intensity", period: 100, param: 1e6 },  // trades_per_sec, volume_per_sec; param = timestamp units per second (default 1e6)
    { kind: "tick_pressure", period: 50 },
    { kind: "amihud", period: 100 },
    { kind: "realized_vol", period: 300, param: 365 * 86400 },  // param = periods per year
    { kind: "session_vwap", param: 86_400e6 },             // param = session length (MANDATORY), param2 = session offset, timestamp units
    { kind: "pivots", param: 86_400e6 },                   // pp, r1, s1, r2, s2 from the previous session
    { kind: "forward_return", period: 20 },                // LABEL: looks 20 rows ahead (ret, max, min)
], { columns: { bid: "bid", ask: "ask", side: "side" } }); // explicit roles (optional here: they are auto-detected)
```

The session-anchored kinds (`session_vwap`, `session_range`, `opening_range`, `pivots`) require `param` = session length in timestamp units (`param2` = session offset); omitting it throws the usual validation error (`invalid indicator spec`). `order_flow` runs on ticks with `volume` + `side`, or on bars (`bucket > 0`) when the database has a `side` role.

**Look-ahead warning.** The label kinds `forward_return` and `triple_barrier` (`period` = horizon; `param` = up-barrier fraction, default 0.02, `param2` = down-barrier fraction) use *future* rows by design: they are training targets, `NaN` at the end of every window, and must never be fed to a live strategy as features. `HOCDB.indicatorIsLookahead(kind)` tells the two apart at runtime.

### Pairs: two databases

`pairIndicators(other, specs, options)` / `pairIndicatorsTail(other, n, specs, options)` run the specs over this database aligned with another open `HOCDB` instance, whose close column becomes the second series of `series2`, `ratio`, `ratio_zscore`, `rel_strength`, `correl` and `beta` (`series` is the primary input; single-series kinds run on this database). With `bucket > 0` both sides are resampled and inner-joined on bar timestamps; on ticks the other database is as-of joined onto this database's rows (its latest row at or before each of ours). Options are those of `indicators`, plus `columns2` (alias `otherColumns`) for the other database's column roles.

```typescript
const eth = new HOCDB("ETH_TICKS", "./ticks", tickSchema);
const pair = ticks.pairIndicatorsTail(eth, 500, [
    { kind: "series" },                    // btc price
    { kind: "series2" },                   // eth price
    { kind: "ratio" },                     // btc / eth
    { kind: "ratio_zscore", period: 100 },
    { kind: "correl", period: 60 },
    { kind: "beta", period: 60 },
], { bucket: 60_000_000 });                // on 1-minute bars, inner-joined
console.log(pair.names, pair.columns["ratio_zscore_100"][499]);
```

In single-database calls the same kinds take their second series from the spec's `field2` (e.g. `{ kind: "ratio", field2: "bid" }`).

### Health, decision evaluation and multi-timeframe snapshots

```typescript
// Data quality over [start, end): gaps above 5 s and |log returns| above 5 % are counted
// -> { count, first_ts, last_ts, span, mean_gap, median_gap, max_gap, max_gap_at, n_gaps, n_nonpositive_price,
//      n_nan_price, n_outlier_returns, first_outlier_at, max_abs_return, n_zero_volume, n_negative_volume }
const hl = ticks.health(start, end, "price", "size", 5_000_000, 0.05); // health(start, end, price?, volume = null, gapThreshold = 0, outlierThreshold = 0)

// Evaluate trading decisions against the stored prices: entry at the first price at or after `timestamp`,
// exit `horizon` later (`defaultHorizon` when omitted / 0), `costBps` per side
const ev = ticks.evaluate([
    { timestamp: 1620000000000000n, direction: 1, size: 1000, horizon: 60_000_000 },
    { timestamp: 1620000120000000n, direction: -1, size: 500 },   // size defaults to 1, horizon to defaultHorizon
], { priceField: "price", defaultHorizon: 120_000_000, costBps: 5 });
console.log(ev.n_evaluated, ev.hit_rate, ev.avg_net_return, ev.total_pnl, ev.net_return[0]); // + per-decision entry / exit / net_return (NaN when not evaluated)

// One snapshot per bar size from a single read of the data: an array in `buckets` order
const [m1, m5, h1] = ticks.snapshotMulti({ buckets: [60e6, 300e6, 3600e6], periodsPerYear: [525600, 105120, 8760], bars: 500 });
console.log(m1.rsi_14, m5.rsi_14, h1.rsi_14, h1.bars);
```

### Options

`indicators(specs, options)` / `indicatorsTail(n, specs, options)`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `start`, `end` | `number \| bigint` | everything | Window `[start, end)` in timestamp units |
| `tail` | `number` | - | Last `n` rows (or bars when `bucket > 0`) instead of `start`/`end` (`indicatorsTail` sets it) |
| `columns` | `{ open?, high?, low?, close?, volume?, bid?, ask?, side? }` | auto-detect | Field names or indices for the OHLCV and quote roles. Missing roles are auto-detected by name (`close` falls back to `price`, `volume` to `size` / `qty`); `null` or `-1` marks a role as absent. `close` is required; `bid` / `ask` / `side` are needed only by the microstructure kinds |
| `columns2` / `otherColumns` | `IndicatorColumns` | auto-detect | `pairIndicators` only: the roles of the other database |
| `lookback` | `"auto" \| number` | `"auto"` | Extra rows read before the window so that the first in-window values are converged. `0` = raw warm-up NaNs inside the window |
| `bucket` | `number \| bigint` | `0` | `0` = one row per record; `> 0` = aggregate records into OHLCV bars of that many timestamp units first (per-spec `field` overrides are rejected in that mode) |

A spec is `{ kind, period?, period2?, period3?, period4?, param?, param2?, field?, field2?, label? }`. `kind` is a name (`"rsi"`, case-insensitive) or the stable id; omitted or zero periods / params select the documented defaults (except the mandatory session length `param` of the session kinds); `field` / `field2` are field names or indices (`field2` is the second series for `correl` / `beta` / `series2` / `ratio` / `ratio_zscore` / `rel_strength` in single-database calls).

`ohlcv(start, end, bucket, { price?, volume?, side? })` returns `{ timestamps, n_bars, open, high, low, close, volume, count }` plus `buy_volume` when `side` is given; without a volume field `volume` equals the record count.

`summary(start, end, field, periodsPerYear = 0)`, `snapshot({ columns?, bars = 0, bucket = 0, periodsPerYear = 0 })`, `snapshotMulti({ buckets, periodsPerYear?, bars = 0, columns? })`, `health(start, end, price?, volume = null, gapThreshold = 0, outlierThreshold = 0)` and `evaluate(decisions, { priceField?, defaultHorizon = 0, costBps = 0 })` decode the native structs generically through the library's field introspection, so every field the core reports is present (`bars = 0` means the recommended 2500 rows; `int64` fields such as `health().first_ts` are `bigint`s, counts and doubles are `number`s). The struct sizes are verified against the library when the binding is loaded.

Registry helpers: `HOCDB.indicatorKinds()` (all 83 kind names), `HOCDB.indicatorOutputs(kind)` (output names, e.g. `["macd", "signal", "hist"]`), `HOCDB.indicatorWarmup(spec)` (recommended warm-up rows), `HOCDB.indicatorIsLookahead(kind)` (`true` for the label kinds that use future rows).

### Column naming

`label` if given, else the kind name plus `_period` when a period is given (`sma_20`, `rsi_14`, `macd`). Single-output kinds use the label as the column name; multi-output kinds append the output name: `macd_signal`, `macd_hist`, `bbands_20_upper`, `adx_14_plus_di` (the output named like the kind itself, e.g. `macd`, `ppo`, `adx`, `tsi`, keeps the bare label). Two specs that would produce the same column name are rejected; use `label` to disambiguate. `res.names` lists the columns in output order and `res.values` is the raw planar buffer (`res.columns[name]` is a view of it).

### Warm-up and NaN

A value that is not yet defined (the indicator has not seen enough history) is `NaN`, never `null`. With the default `lookback: "auto"` the extra history is read before the window, so in-window values are converged and the first row is already defined when enough records exist before `start`; with `lookback: 0`, or at the very beginning of the database, the first rows of a column are `NaN`. Check with `Number.isNaN(v)`.

### Memory

Native results are copied into JavaScript-owned typed arrays and freed before the call returns, so the returned arrays can be kept, transferred or modified freely. The indicator API is available on the synchronous `HOCDB` class.

## Durability, readers and operations

Data files start with a 64-byte header (`HOC2`) that holds the atomically committed write cursor and a CRC32C of the committed data; legacy `HOC1` files are migrated in place the first time a writer opens them (`auto_migrate`, default on). A writer takes an exclusive lock: a second writer on the same ticker and path fails immediately, and the error message carries the engine's error name (`DatabaseLocked`, `SchemaMismatch`, `ChecksumMismatch`, `LegacyFormatNeedsMigration`, ...). Any number of lock-free readers can attach to the file, from any process, and see the writer's committed data.

### Options

`new HOCDB(ticker, path, schema, config)` and `new HOCDBAsync(...)` accept these options in addition to the storage options above (camelCase aliases such as `fsyncIntervalMs`, `verifyOnOpen`, `retentionSpan`, `rolloverSize`, `autoMigrate`, `timestampUnitNs`, `indexStride`, `readOnly` are accepted too):

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `fsync` | `"none" \| "on_close" \| "on_flush" \| "interval"` (or `0`-`3`) | `"on_close"` | When the file is fsync'ed: never (the OS decides), once on close, after every flush, or at most every `fsync_interval_ms` and on close |
| `fsync_interval_ms` | `number` | `1000` | Interval for `fsync: "interval"` |
| `verify_on_open` | `boolean` | `false` | Recompute the checksum when opening; a mismatch fails the open with `ChecksumMismatch` instead of opening the file |
| `retention_span` | `number \| bigint` | `0` (off) | Automatically drop records older than `last_timestamp - span` (timestamp units); the compaction runs once the excess exceeds 25 % |
| `rollover_size` | `number \| bigint` | `0` (off) | Archive the file (as `rollover()` does) once it grows above this many bytes |
| `auto_migrate` | `boolean` | `true` | Rewrite legacy `HOC1` files to the current format when a writer opens them (readers cannot open legacy files) |
| `timestamp_unit_ns` | `number \| bigint` | `0` (unknown) | Nanoseconds per timestamp unit (`1e9` for seconds, `1e6` for milliseconds); enables `ingest_lag_record_ns` in `metrics()` |
| `index_stride` | `number \| bigint` | `1024` | Records per sparse-index entry |
| `read_only` | `boolean` | `false` | Attach as a lock-free reader (what `HOCDB.openReader` does; also works through `HOCDBAsync`) |

Data written by the writer becomes visible to readers only after the writer's `flush()` (the commit); durability follows the fsync policy, and `sync()` forces a flush + fsync at any time.

### Reader pattern: ingestion process + agent process

```typescript
// ingest.ts — the single writer of BTC_USD (one per database)
const w = new HOCDB("BTC_USD", "./data", schema, { fsync: "interval", fsyncIntervalMs: 500, timestampUnitNs: 1e6 });
for await (const tick of feed) {
    w.append({ timestamp: tick.ts, price: tick.price, volume: tick.size });
    if (tick.endOfBatch) w.flush();      // commit: from here on readers can see the records
}

// agent.ts — any number of readers, in any process, no lock
const r = HOCDB.openReader("BTC_USD", "./data", schema);
r.isReadOnly();                          // true
const snap = r.snapshot({ bucket: 60_000 });   // every read picks up the latest commit ...
r.refresh();                             // ... or do it explicitly
const latest = r.getLatest("price");
r.metrics().ingest_lag_wall_ns;          // ns since the writer's last commit
r.append({ timestamp: 1n, price: 0, volume: 0 }); // throws: "read-only: this handle is a reader ..."
```

Readers need a committed current-format file: opening one before the writer's first `flush()` throws with `EmptyDatabase` (retry later), and a legacy `HOC1` file has to be opened by a writer once to be migrated. Readers follow the files that the writer compacts or rolls over automatically.

### Maintenance and metrics

```typescript
import { basename } from "path";

w.sync();                        // flush + fsync now, whatever the fsync policy
w.verify();                      // true = CRC32C matches, false = MISMATCH; throws "checksum unavailable" for ring buffers / legacy files
w.compact(1_700_000_000_000n);   // keep timestamp >= minTs (atomic rewrite; readers follow)
w.retainLast(1_000_000);         // keep the last n records
const archive = w.rollover();    // "./data/BTC_USD.<first_ts>-<last_ts>.bin"; the live file continues empty
const old = new HOCDB(basename(archive, ".bin"), "./data", schema); // archives are ordinary databases
w.formatVersion();               // 2 (1 = legacy HOC1)
HOCDB.headerSize();              // 64

const m = w.metrics();
console.log(m.appends, m.commits, m.fsyncs, m.fsync_ns_max, m.committed_records, m.file_size);
console.log(m.last_record_ts, m.ingest_lag_wall_ns);   // bigint
console.log(m.recovered_tail_records, m.dropped_tail_bytes, m.crc_failures, m.compactions, m.rollovers);
w.metricsReset();                // counters back to 0; state fields (last_record_ts, committed_records, file_size, ...) are kept
```

`metrics()` returns the 30 `HOCDBMetrics` fields decoded by name through the library's introspection, with the same convention as `summary()`: the `uint64` counters (`appends`, `bytes_written`, `flushes`, `commits`, `fsyncs`, `fsync_ns_total`, `fsync_ns_max`, `reads`, `read_ns_total`, `read_ns_max`, `read_ns_last`, `read_ns_p50`, `read_ns_p99`, `records_read`, `refreshes`, `recovered_tail_records`, `dropped_tail_bytes`, `crc_failures`, `compactions`, `rollovers`, `migrations`, `committed_records`, `file_size`, `format_version`, `read_only`) are `number`s, the `int64` timestamps and lags (`last_append_wall_ns`, `last_commit_wall_ns`, `last_record_ts`, `ingest_lag_wall_ns`, `ingest_lag_record_ns`) are `bigint`s. `sync`, `compact`, `retainLast`, `rollover` and `drop` on a reader throw an error saying the handle is a reader; `refresh()` is a no-op on writers.

### Recovery and checksum

Records written after the last commit (for example by a writer that crashed before flushing) are adopted on the next writer open when they are complete and in timestamp order; torn or misordered trailing bytes are truncated, and `metrics()` reports both in `recovered_tail_records` and `dropped_tail_bytes`. The CRC32C covers the committed data of linear files: `verify()` recomputes it (`true` / `false`, or a "checksum unavailable" error for ring buffers, legacy files and a recovered tail that has not been flushed yet), and `verify_on_open: true` makes the open fail with `ChecksumMismatch` instead.

### Ring-buffer capacity

With `overwrite_on_full` the file is a ring buffer over the data area that follows the 64-byte header, so `max_file_size = HOCDB.headerSize() + N * record_size` holds exactly `N` records: `{ max_file_size: 64 + 50 * 16, overwrite_on_full: true }` keeps the last 50 records of a `[{ i64 }, { f64 }]` schema. A `max_file_size` below the header plus one record fails the open with `MaxFileSizeTooSmall`.

## Testing

```bash
bun test

# Or run specific tests
bun run test/test_async_drop.ts
bun run test/test_agg.ts
bun run test/test_query.ts
bun run test/test_indicators.ts
bun run test/test_storage.ts
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

```ts
import { HOCDB } from "hocdb";

const db = new HOCDB("AAPL", "./data", schema, { calendar: "nyse", timestamp_unit_ns: 1000 });
db.getCalendar();              // 3
db.periodsPerYear(60_000_000); // 98280

// session kinds with param 0 follow the exchange sessions
db.indicators([{ kind: "session_vwap", param: 0 }, { kind: "pivots", param: 0 }],
              { tail: 390, columns: cols, bucket: 60_000_000 });

const run = db.backtest(target, start, end, {
    bucket: 60_000_000, columns: cols,
    params: { initial_equity: 100_000, cost_bps: 5, stop_loss: 0.02, position_mode: 1 },
    outputs: ["equity", "drawdown"], maxTrades: 1000,
});
run.result.sharpe; run.trades![0]!.exit_reason;

const splits = HOCDB.walkForwardSplits(close.length, 5, 0.6, true);
const perWindow = HOCDB.backtestSplits(ts, open, high, low, close, target, splits, params);

const u = HOCDB.universe([dbA, dbB, dbC], { columns: cols, bars: 500, bucket: 60_000_000 });
u.rows[0]!.rank_mom_mid; u.summary.breadth_up; u.corr![0]![1];
```

Static calendar helpers: `HOCDB.calendarId`, `calendarName`, `calendarSession`,
`calendarSessionForDay`, `calendarIsOpen`, `calendarOpenSeconds`,
`calendarSessionsBetween`, `calendarPeriodsPerYear`, `calendarToLocal`,
`daysFromCivil`, `civilFromDays`, `calendarDefine`; per instance `setCalendar`,
`getCalendar`, `setTimestampUnit`, `getTimestampUnit`, `periodsPerYear`,
`backtest`, `backtestTail`. Also `HOCDB.backtestArrays`, `HOCDB.backtestDefaults`,
`HOCDB.universeArrays`, `HOCDB.universeDefaults`.
