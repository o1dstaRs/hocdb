# HOCDB Node.js Bindings

Node.js bindings for HOCDB - The World's Most Performant Time-Series Database.

## Prerequisites

Before using the Node.js bindings, build the native library:

```bash
# From the main HOCDB directory
zig build node-bindings
```

## Installation

```bash
cd bindings/node
npm install
```

## Quick Start

```javascript
const hocdb = require('./index.js');

// Define schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "volume", type: "f64" },
    { name: "active", type: "bool" }
];

// Async API (recommended)
async function main() {
    const db = await hocdb.dbInitAsync("BTC_USD", "./data", schema);

    // Append records
    await db.append({ timestamp: 1620000000n, price: 50000.0, volume: 1.5, active: true });
    await db.append({ timestamp: 1620000001n, price: 50100.0, volume: 2.0, active: true });

    // Flush to disk
    await db.flush();

    // Query data
    const results = await db.query(1620000000n, 1620000100n);
    console.log(`Found ${results.length} records`);

    // Get statistics
    const stats = await db.getStats(1620000000n, 1620000100n, "price");
    console.log(`Min: ${stats.min}, Max: ${stats.max}`);

    // Get latest value
    const latest = await db.getLatest("price");
    console.log(`Latest price: ${latest.value}`);

    // Close when done
    await db.close();
}

main();
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

```javascript
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "volume", type: "f64" },
    { name: "is_buy", type: "bool" }
];
```

### Configuration Options

```javascript
const config = {
    max_file_size: 1024 * 1024 * 100,  // 100MB (0 for default)
    overwrite_on_full: true,            // Ring buffer mode
    flush_on_write: false,              // Flush on every write
    auto_increment: false,              // Auto-increment timestamps
    fsync: "on_flush",                  // Durability policy (see "Durability, readers and operations")
    timestampUnitNs: 1e6                // camelCase works as well
};
```

The full list of options (fsync policies, retention, rollover, checksum
verification, timestamp unit) is in [Durability, readers and operations](#durability-readers-and-operations).

---

## Synchronous API

### `dbInit(ticker, path, schema, config?)`

Initialize a database with synchronous operations.

```javascript
const db = hocdb.dbInit("BTC_USD", "./data", schema, config);
```

### `db.append(data)`

Append a record to the database.

```javascript
db.append({
    timestamp: 1620000000n,  // Use BigInt for i64
    price: 50000.0,
    volume: 1.5,
    active: true
});
```

### `db.flush()`

Force buffered data to be written to disk.

```javascript
db.flush();
```

### `db.load()`

Load all records from the database.

```javascript
const records = db.load();
for (const record of records) {
    console.log(`Price at ${record.timestamp}: ${record.price}`);
}
```

### `db.query(startTs, endTs, filters?)`

Query records within a timestamp range with optional filters.

**Parameters:**
- `startTs` (BigInt): Start timestamp (inclusive)
- `endTs` (BigInt): End timestamp (inclusive)
- `filters` (object, optional): Filter conditions

**Filter Syntax:**
```javascript
// Simple equality filter
const results = db.query(1620000000n, 1620000100n, { price: 50000.0 });

// Multiple filters
const results = db.query(1620000000n, 1620000100n, {
    active: true,
    price: 50000.0
});
```

### `db.getStats(startTs, endTs, field)`

Get statistics for a specific field within a time range.

**Parameters:**
- `startTs` (BigInt): Start timestamp
- `endTs` (BigInt): End timestamp
- `field` (string or number): Field name or index

**Returns:** Object with `min`, `max`, `sum`, `count`, `mean`

```javascript
const stats = db.getStats(1620000000n, 1620000100n, "price");
console.log(`Min: ${stats.min}, Max: ${stats.max}, Mean: ${stats.mean}`);

// Using field index
const stats = db.getStats(1620000000n, 1620000100n, 1);
```

### `db.getLatest(field)`

Get the most recent value and timestamp for a specific field.

**Parameters:**
- `field` (string or number): Field name or index

**Returns:** Object with `value` and `timestamp`

```javascript
const latest = db.getLatest("price");
console.log(`Latest: ${latest.value} at ${latest.timestamp}`);
```

### `db.close()`

Close the database handle.

```javascript
db.close();
```

### `db.drop()`

Close the database and delete all data files.

```javascript
// WARNING: This permanently deletes all data!
db.drop();
```

---

## Asynchronous API (Recommended)

The async API uses worker threads to avoid blocking the main event loop.

### `dbInitAsync(ticker, path, schema, config?)`

Initialize a database with asynchronous operations. Returns a Promise.

```javascript
const db = await hocdb.dbInitAsync("BTC_USD", "./data", schema, config);
```

### `db.append(data)`

Append a record asynchronously.

```javascript
await db.append({
    timestamp: 1620000000n,
    price: 50000.0,
    volume: 1.5,
    active: true
});
```

### `db.appendBatch(records)`

Append multiple records in a single operation.

```javascript
await db.appendBatch([
    { timestamp: 1620000000n, price: 50000.0, volume: 1.5, active: true },
    { timestamp: 1620000001n, price: 50100.0, volume: 2.0, active: true },
    { timestamp: 1620000002n, price: 50200.0, volume: 1.0, active: false }
]);
```

### `db.flush()`

Flush to disk asynchronously.

```javascript
await db.flush();
```

### `db.load()`

Load all records asynchronously.

```javascript
const records = await db.load();
```

### `db.query(startTs, endTs, filters?)`

Query records asynchronously.

```javascript
const results = await db.query(1620000000n, 1620000100n, { active: true });
```

### `db.getStats(startTs, endTs, field)`

Get statistics asynchronously.

```javascript
const stats = await db.getStats(1620000000n, 1620000100n, "price");
```

### `db.getLatest(field)`

Get latest value asynchronously.

```javascript
const latest = await db.getLatest("price");
```

### `db.close()`

Close the database asynchronously.

```javascript
await db.close();
```

### `db.drop()`

Close and delete data files asynchronously.

```javascript
await db.drop();
```

---

## Complete Example

```javascript
const hocdb = require('./index.js');

const schema = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "volume", type: "f64" },
    { name: "is_buy", type: "bool" }
];

async function main() {
    // Initialize with ring buffer configuration
    const db = await hocdb.dbInitAsync("ETH_USD", "./market_data", schema, {
        max_file_size: 1024 * 1024 * 100,  // 100MB
        overwrite_on_full: true
    });

    try {
        // Batch insert trades
        await db.appendBatch([
            { timestamp: 1620000000n, price: 2500.0, volume: 10.0, is_buy: true },
            { timestamp: 1620000001n, price: 2501.5, volume: 5.0, is_buy: false },
            { timestamp: 1620000002n, price: 2502.0, volume: 15.0, is_buy: true }
        ]);

        await db.flush();

        // Query buy orders only
        const buyOrders = await db.query(1620000000n, 1620000100n, { is_buy: true });
        console.log(`Buy orders: ${buyOrders.length}`);

        // Get price statistics
        const stats = await db.getStats(1620000000n, 1620000100n, "price");
        console.log(`Price: min=${stats.min}, max=${stats.max}, mean=${stats.mean}`);

        // Get latest price
        const latest = await db.getLatest("price");
        console.log(`Latest price: ${latest.value} at ${latest.timestamp}`);

        // Load all records
        const allRecords = await db.load();
        console.log(`Total records: ${allRecords.length}`);

    } finally {
        await db.close();
    }
}

main().catch(console.error);
```

## Indicators & analytics

HOCDB computes technical indicators and quantitative analytics directly on the
stored data in one pass, so you never have to load records into JavaScript first.
The engine ships 83 indicator kinds (moving averages, momentum, trend, volatility,
volume, statistics / risk, price transforms, tick microstructure, pairs, labels and
session-anchored kinds), tick-to-bar resampling, pair alignment across two databases,
a scalar performance summary, data-quality health statistics, decision evaluation and
one-shot ~100-field snapshots for the latest bar (single or multi-timeframe).
Results come back as typed arrays (`Float64Array`, `BigInt64Array`) that are owned
by JavaScript; the native memory is freed before the call returns.

All methods exist on both the sync (`dbInit`) and the async (`dbInitAsync`) instance;
the async ones return Promises with the same shapes.

### Batch indicators by name

```javascript
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "open", type: "f64" }, { name: "high", type: "f64" },
    { name: "low", type: "f64" }, { name: "close", type: "f64" },
    { name: "volume", type: "f64" }
];
const db = hocdb.dbInit("BTC_USD", "./data", schema);

// Columns are auto-detected from the field names (open/high/low/close/volume).
const res = db.indicators([
    { kind: "sma", period: 20 },
    { kind: "rsi", period: 14 },
    { kind: "macd" },                                    // defaults 12/26/9
    { kind: "bbands", period: 20, param: 2.0 },          // param = k
    { kind: "sma", period: 10, field: "volume", label: "vol_sma" }
], { start: 1620000000n, end: 1620086400n });            // window [start, end)

res.n_rows;               // number of rows
res.timestamps;           // BigInt64Array
res.sma_20;               // Float64Array
res.rsi_14;               // Float64Array, 0..100
res.macd; res.macd_signal; res.macd_hist;
res.bbands_20_upper; res.bbands_20_middle; res.bbands_20_lower;
res.vol_sma;              // SMA(10) of the volume field
res.names;                // ["sma_20", "rsi_14", "macd", ...] in output order

// Last 5 rows only, warm-up handled for you:
const last = db.indicatorsTail(5, ["rsi", "atr"]);       // string shorthand = defaults
```

### Snapshot for an LLM agent

`snapshot()` returns ~100 named values (SMAs, EMAs, RSI, MACD, ADX, Bollinger,
Keltner, ATR, OBV, VWAP, returns, drawdown, Sharpe, ...) for the latest bar in one call:

```javascript
const snap = db.snapshot({ periodsPerYear: 365 });      // 2500 most recent records
// snap.timestamp (BigInt), snap.bars (BigInt), snap.close, snap.rsi_14, snap.ema_200,
// snap.macd_hist, snap.bb_percent_b, snap.adx_14, snap.supertrend_dir, snap.drawdown, ...

// Hand it to a model (BigInt fields need a replacer for JSON):
const json = JSON.stringify(snap, (k, v) => typeof v === "bigint" ? v.toString() : v);
const prompt = `Latest market state for BTC_USD:\n${json}\nShould we reduce exposure?`;

// Daily bars built from ticks, using only the last 300 days:
const daily = db.snapshot({ bars: 300, bucket: 86_400n, periodsPerYear: 365 });
```

### Tick -> bar with `bucket`, and `ohlcv()`

With `bucket > 0`, records are first aggregated into OHLCV bars of that many
timestamp units (e.g. `300` for 5-minute bars when timestamps are seconds):

```javascript
// Tick data: (timestamp, price, size)
const tickSchema = [
    { name: "timestamp", type: "i64" }, { name: "price", type: "f64" }, { name: "size", type: "f64" }
];
const ticks = hocdb.dbInit("ETH_USD", "./ticks", tickSchema);

// "price" stands in for a missing close; tell it which field is the volume.
const bars = ticks.indicatorsTail(100, [{ kind: "ema", period: 20 }, "vwap"], {
    bucket: 300n,
    columns: { close: "price", volume: "size" }
});
bars.timestamps;          // 300 apart and aligned to multiples of 300

// Plain OHLCV resampling without indicators:
const ohlcv = ticks.ohlcv(null, null, 300n, { price: "price", volume: "size" });
ohlcv.n_bars; ohlcv.timestamps; ohlcv.open; ohlcv.high; ohlcv.low; ohlcv.close;
ohlcv.volume;             // summed volume (record count when no volume field is given)
ohlcv.count;              // records per bar

// Scalar summary of a series over a window (29 fields):
const s = ticks.summary(null, null, "price", 365 * 24 * 12);
// s.count (BigInt), s.total_return, s.ann_vol, s.sharpe, s.max_drawdown, s.win_rate, ...
```

### Tick microstructure: the `bid`, `ask` and `side` roles

Besides open/high/low/close/volume, a schema can provide `bid`, `ask` and `side`
(1 / true = buy). They are auto-detected by name like the other roles (and `size` /
`qty` stand in for a missing `volume`), or given explicitly in `columns`:

```javascript
const quotes = hocdb.dbInit("BTC_USD", "./ticks", [
    { name: "timestamp", type: "i64" }, { name: "price", type: "f64" }, { name: "size", type: "f64" },
    { name: "bid", type: "f64" }, { name: "ask", type: "f64" }, { name: "side", type: "bool" }
]);

const m = quotes.indicatorsTail(100, [
    "spread",                                              // needs bid + ask -> spread_abs, spread_bps
    { kind: "order_flow", period: 10 },                    // needs volume + side -> _net, _imbalance
    { kind: "trade_intensity", period: 10, param: 1e6 },   // param = timestamp units per second
    { kind: "tick_pressure", period: 20 },
    "amihud", { kind: "realized_vol", param: 525600 },      // param = periods per year
    { kind: "session_vwap", param: 600e6 },                // param = session length (mandatory)
    { kind: "forward_return", period: 5 }                  // label: looks 5 rows ahead
], { columns: { close: "price", volume: "size", bid: "bid", ask: "ask", side: "side" } });  // same as auto-detect here
m.spread_bps; m.order_flow_10_imbalance; m.trade_intensity_10_trades_per_sec; m.forward_return_5_ret;

// OHLCV bars with per-bar buy volume: pass the side field to ohlcv().
const bars = quotes.ohlcv(null, null, 60_000_000n, { side: "side" });
bars.buy_volume;          // Float64Array, 0 <= buy_volume <= volume; absent when no side is given
```

With `bucket > 0`, `order_flow` runs on bars as long as the database has a `side` role.

**Labels look ahead.** `forward_return` (`period` = horizon) and `triple_barrier`
(`period` = horizon, `param` = up fraction, `param2` = down fraction) are labels
for training / back-testing: they use *future* rows by design and are `NaN` at the
end of every window. Never feed them to a live model as features;
`hocdb.indicatorIsLookahead(kind)` (and the `lookahead` flag in the registry) tells
them apart from ordinary indicators.

**Session kinds need `param`.** `session_vwap`, `session_range`, `opening_range`
(`period` = rows of the opening range) and `pivots` are anchored to sessions of
`param` timestamp units starting at offset `param2` (e.g. `86_400_000_000` and a
UTC-offset in microseconds). `param` is mandatory: omitting it throws the usual
validation error.

### Pairs: two databases, one result

`pairIndicators(other, specs, options)` runs the specs over this database (series A)
aligned with another one (series B, whose close is the second input of `series2`,
`ratio`, `ratio_zscore`, `rel_strength`, `correl` and `beta`; single-series kinds run
on A). On ticks, B is as-of joined onto A's rows (latest B row at or before each A
row); with `bucket > 0` both are resampled and inner-joined on bar timestamps.
Options are those of `indicators()` (`columns` for this database, `columns2` /
`otherColumns` for the other one, both auto-detected by default):

```javascript
const btc = hocdb.dbInit("BTC_USD", "./ticks", tickSchema);
const eth = hocdb.dbInit("ETH_USD", "./ticks", tickSchema);

const pair = btc.pairIndicators(eth, ["series", "series2", "ratio", { kind: "correl", period: 30 },
                                      { kind: "rel_strength", period: 10 }], { tail: 50 });
pair.ratio[0] === pair.series[0] / pair.series2[0];      // BTC / ETH

// Hourly bars, inner-joined:
const hourly = btc.pairIndicators(eth, [{ kind: "beta", period: 24 }], {
    start: 1_700_000_000_000_000n, end: 1_700_086_400_000_000n, bucket: 3_600_000_000n
});
```

In the async API both legs must live on the same worker thread, so open the second
one *from* the first: `const eth = await btc.openAsync("ETH_USD", "./ticks", tickSchema)`;
then `await btc.pairIndicators(eth, specs, options)`. The worker stays alive until
every database opened on it has been closed.

### Health, decision evaluation, multi-timeframe snapshots

```javascript
// Data quality over a window: gaps above 5 s, |log returns| above 5 % (16 fields; counters are BigInt)
const h = db.health(null, null, "price", "size", 5_000_000n, 0.05);   // or db.health(null, null, { gapThreshold, outlierThreshold })
// h.count, h.first_ts, h.last_ts, h.mean_gap, h.median_gap, h.max_gap, h.max_gap_at, h.n_gaps,
// h.n_nonpositive_price, h.n_nan_price, h.n_outlier_returns, h.first_outlier_at, h.max_abs_return, h.n_zero_volume, ...

// Evaluate decisions: entry at the first price at or after `timestamp`, exit after `horizon` (0 = defaultHorizon), 5 bps per side
const ev = db.evaluate([{ timestamp: 1_700_000_000_000_000n, direction: 1, size: 1000, horizon: 60_000_000n },
                        { timestamp: 1_700_000_100_000_000n, direction: -1, size: 500 }],
                       { priceField: "price", defaultHorizon: 120_000_000n, costBps: 5 });
// ev.n_decisions, ev.n_evaluated (BigInt), ev.hit_rate, ev.avg_net_return, ev.total_pnl, ev.sharpe, ev.max_drawdown, ...
// plus per-decision Float64Arrays ev.entry, ev.exit, ev.net_return (NaN where the decision could not be evaluated)

// Snapshots for several bar sizes from one read -> array in `buckets` order
const [m1, m5] = db.snapshotMulti({ buckets: [60_000_000n, 300_000_000n], periodsPerYear: [525600, 105120], bars: 50 });
m1.rsi_14; m5.bars;       // m1 equals db.snapshot({ bars: 50, bucket: 60_000_000n, periodsPerYear: 525600 })
```

### API

| Method | Description |
|--------|-------------|
| `db.indicators(specs, options?)` | Batch of indicators over `[start, end)` or the last `tail` rows |
| `db.indicatorsTail(n, specs, options?)` | Same as `indicators(specs, { ...options, tail: n })` |
| `db.pairIndicators(other, specs, options?)` | Indicators over this database aligned with `other` (`columns2` / `otherColumns` select its roles) |
| `db.ohlcv(start, end, bucket, { price?, volume?, side? }?)` | OHLCV bars of `bucket` timestamp units (`null` start/end = open-ended); `side` adds `buy_volume` |
| `db.summary(start, end, field, periodsPerYear = 0)` | Performance / risk summary of a field |
| `db.health(start, end, price?, volume?, gapThreshold = 0, outlierThreshold = 0)` | Data-quality statistics (also `health(start, end, { price, volume, gapThreshold, outlierThreshold })`) |
| `db.evaluate(decisions, { priceField?, defaultHorizon = 0, costBps = 0 }?)` | Evaluate `[{ timestamp, direction, size = 1, horizon = 0 }]` decisions; returns the summary plus `entry` / `exit` / `net_return` arrays |
| `db.snapshot({ columns?, bars = 0, bucket = 0, periodsPerYear = 0 }?)` | ~100 indicator values for the latest bar |
| `db.snapshotMulti({ buckets, periodsPerYear?, bars = 0, columns? })` | One snapshot per bucket, as an array in `buckets` order |
| `adb.openAsync(ticker, path, schema, config?)` | (async only) open another database on the same worker, e.g. for `pairIndicators` |
| `hocdb.indicatorKinds()` | All kind names |
| `hocdb.indicatorKindId(kind)` / `hocdb.INDICATOR_KINDS` | Kind name -> id (case-insensitive) |
| `hocdb.indicatorOutputs(kind)` | Output names of a kind, e.g. `["macd", "signal", "hist"]` |
| `hocdb.indicatorIsLookahead(kind)` | `true` for label kinds that use future rows (`forward_return`, `triple_barrier`) |
| `hocdb.indicatorWarmup(spec)` | Recommended warm-up rows for a spec |

**Spec** (`specs` is an array; a bare kind name such as `"rsi"` means "all defaults"):

| Key | Description |
|-----|-------------|
| `kind` | Kind name (case-insensitive) or numeric id |
| `period`, `period2`, `period3`, `period4` | Periods; `0`/omitted = documented default (RSI 14, MACD 12/26/9, BBANDS 20, ATR 14, ...). `period` is the horizon of `forward_return` / `triple_barrier` and the row count of `opening_range` |
| `param` | BBANDS k (2.0), KELTNER / SUPERTREND multiplier, PSAR acceleration, periods-per-year for HIST_VOL / SHARPE / SORTINO / REALIZED_VOL, timestamp units per second for TRADE_INTENSITY (1e6), up-barrier fraction for TRIPLE_BARRIER (0.02), session length for the session kinds (**mandatory** there) |
| `param2` | PSAR max acceleration, TRIPLE_BARRIER down-barrier fraction (= up), session offset for the session kinds |
| `field` | Field name or index to run a single-series indicator on (default: the close column) |
| `field2` | Second series for `correl` / `beta` / `series2` / `ratio` / `ratio_zscore` / `rel_strength` in single-database calls (in `pairIndicators` the other database is the second series) |
| `label` | Column name to use instead of the default |

**Options** of `indicators()`:

| Option | Default | Description |
|--------|---------|-------------|
| `start`, `end` | whole DB | Window `[start, end)` (BigInt or number) |
| `tail` | - | Last `n` rows (or bars) instead of `start`/`end` |
| `columns` | auto-detect | `{ open, high, low, close, volume, bid, ask, side }` as field names or indices. Without it, fields literally named like the roles are used, a field named `price` stands in for a missing `close` and `size` / `qty` for a missing `volume`; an error is thrown when no close can be found. When given, only the roles you list are used (`close` is required). |
| `columns2` / `otherColumns` | auto-detect | (`pairIndicators` only) the same for the other database |
| `lookback` | `"auto"` | Extra rows (bars when `bucket > 0`) read before the window so the first in-window values are converged. `"auto"` = recommended per-spec warm-up; `0` = none |
| `bucket` | `0` | `0` = one row per record; `> 0` = aggregate records into OHLCV bars of that many timestamp units first. Per-spec `field` overrides are rejected with `bucket > 0` |

**Naming rule.** The column name is `spec.label` if given, otherwise the kind name
plus `_<period>` when a period is given (`"sma_20"`, `"rsi"`, `"macd"`).
Single-output kinds use that label directly; multi-output kinds append the output
name, except that the output named like the kind keeps the bare label:
`macd` -> `macd`, `macd_signal`, `macd_hist`; `{ kind: "bbands", period: 20 }`
-> `bbands_20_upper`, `bbands_20_middle`, `bbands_20_lower`, `bbands_20_percent_b`,
`bbands_20_bandwidth`. Names must be unique (use `label` to disambiguate two SMAs of
the same period on different fields). `res.values` is the raw planar buffer
(output `k` occupies `values[k * n_rows, (k + 1) * n_rows)`) and each named column
is a zero-copy view into it.

**Warm-up.** `NaN` marks values that are not yet defined (the warm-up region of a
window, e.g. the first 19 rows of an SMA(20) with `lookback: 0`). NaN is kept as
NaN, never converted to `null`. With the default `lookback: "auto"`, enough earlier
rows are read that the first in-window values are already converged.

**Types.** Timestamps and integer counters (`timestamps`, `snapshot.timestamp`,
`snapshot.bars`, `summary.count`, the `count` / `n_*` / `*_ts` / `*_at` / `max_gap` /
`span` fields of `health()` and the `n_*` fields of `evaluate()`) are BigInt, like
everywhere else in this binding; all indicator values, gaps and returns are doubles.

See [../../INDICATORS.md](../../INDICATORS.md) for the full table of kinds (including
the microstructure, pairs, label and session kinds), their defaults, outputs, required
columns, the snapshot / health / evaluation fields and the alignment conventions.

## Durability, readers and operations

Every database file starts with a 64-byte header (`hocdb.headerSize()`) that
records the schema hash, the *committed* write cursor, a CRC32C of the committed
data and the last timestamp. A writer (`dbInit` / `dbInitAsync`) holds an
exclusive lock on its file; any number of **readers** (`openReader` /
`openReaderAsync`) can follow it without any lock, in the same or in another
process, and see exactly the data the writer has committed with `flush()`.

### Options

Every option is accepted in `snake_case` or `camelCase` (`fsync_interval_ms` /
`fsyncIntervalMs`); readers take no options.

| Option | Default | Description |
|--------|---------|-------------|
| `max_file_size` | 2 GiB | File size cap in bytes (`0` = default). A ring buffer of N records needs `hocdb.headerSize() + N * recordSize` |
| `overwrite_on_full` | `true` | Ring buffer: overwrite the oldest records when the file is full |
| `flush_on_write` | `false` | Flush (commit) after every append |
| `auto_increment` | `false` | Timestamps are assigned by the database |
| `fsync` | `"on_close"` | `"none"` (the OS decides when data reaches disk), `"on_close"` (once, when closing), `"on_flush"` (after every flush, safest) or `"interval"` (at most every `fsync_interval_ms` during flushes, and on close). The numbers 0-3 / `hocdb.FSYNC.on_flush` are accepted too |
| `fsync_interval_ms` | `1000` | Interval for `fsync: "interval"` |
| `verify_on_open` | `false` | Recompute the checksum when opening; the open fails with `ChecksumMismatch` when it differs |
| `retention_span` | `0` (off) | Keep only records with `timestamp >= last - retention_span` (timestamp units); the file is compacted automatically once the excess is more than 25 % |
| `rollover_size` | `0` (off) | Archive the file as `<ticker>.<first_ts>-<last_ts>.bin` and continue with an empty one once it exceeds this many bytes |
| `auto_migrate` | `true` | Rewrite legacy `HOC1` files into the current format the first time a writer opens them (readers cannot open legacy files: `LegacyFormatNeedsMigration`) |
| `timestamp_unit_ns` | `0` (unknown) | Nanoseconds per timestamp unit (`1e9` seconds, `1e6` milliseconds, `1e3` microseconds); enables `metrics().ingest_lag_record_ns` |
| `index_stride` | `1024` | Records between sparse-index entries |

A failed open throws an `Error` whose message starts with the engine's error
name, which is also available as `err.code`: `DatabaseLocked` (another writer
holds the file; writers fail immediately instead of blocking), `SchemaMismatch`,
`ChecksumMismatch`, `LegacyFormatNeedsMigration`, `MaxFileSizeTooSmall`,
`FileNotFound` (a reader opened before the writer created the file), ...

```javascript
try {
    db = hocdb.dbInit("BTC_USD", "./data", schema, { fsync: "on_flush", timestampUnitNs: 1e6 });
} catch (e) {
    if (e.code === "DatabaseLocked") console.log("another ingestion process is running");
    else throw e;
}
```

### The reader pattern: one ingestion process, many agents

```javascript
// ingestion.js -- the only writer
const db = hocdb.dbInit("BTC_USD", "./data", schema, {
    fsync: "interval", fsyncIntervalMs: 500,    // bounded data loss on power failure
    retentionSpan: 7n * 86_400_000_000n,        // keep a week (timestamps in microseconds)
    timestampUnitNs: 1000
});
for await (const trade of feed) {
    db.append(trade);
    if (trade.isBatchEnd) db.flush();           // commit: from here on readers can see the batch
}
```

```javascript
// agent.js -- any number of these, in other processes
const r = hocdb.openReader("BTC_USD", "./data", schema);   // no lock, no config
setInterval(() => {
    // every read re-reads the writer's committed cursor; refresh() does it explicitly
    const snap = r.snapshot({ bars: 500, bucket: 60_000_000n, periodsPerYear: 525_600 });
    const lag = r.metrics().ingest_lag_record_ns;           // BigInt nanoseconds since the last record
    console.log(snap.close, snap.rsi_14, `lag ${lag / 1_000_000n} ms`);
}, 1000);
```

Readers support every read method (`query`, `load`, `getStats`, `getLatest`,
`indicators`, `pairIndicators`, `ohlcv`, `summary`, `snapshot`, `snapshotMulti`,
`health`, `evaluate`) plus `refresh()` and `isReadOnly()`. Data the writer has
appended but not yet flushed is invisible to them. `append`, `drop`, `sync`,
`compact`, `retainLast` and `rollover` on a reader throw an `Error` with
`code === "ReadOnly"` whose message says the handle is a reader. Readers follow
a writer's compaction and rollover automatically.

### Operations

```javascript
db.sync();                       // flush + fsync now, whatever the fsync policy
db.verify();                     // true: CRC32C of the committed data matches; false: MISMATCH (see crc_failures)
                                 // throws code "ChecksumUnavailable" for ring buffers / legacy files
db.compact(1_700_000_000_000_000n);   // keep records with timestamp >= minTs (atomic rewrite)
db.retainLast(1_000_000);        // keep the last n records
const archive = db.rollover();   // "./data/BTC_USD.1699999000000000-1700000000000000.bin"; db continues empty
const old = hocdb.dbInit("BTC_USD.1699999000000000-1700000000000000", "./data", schema); // archives are databases
db.refresh();                    // no-op for writers
db.formatVersion();              // 2 (1 for a legacy file that has not been migrated)
hocdb.headerSize();              // 64

const m = db.metrics();          // 30 BigInt fields
m.appends; m.bytes_written; m.flushes; m.commits; m.fsyncs; m.fsync_ns_total; m.fsync_ns_max;
m.reads; m.read_ns_total; m.read_ns_max; m.read_ns_last; m.read_ns_p50; m.read_ns_p99; m.records_read;
m.refreshes; m.recovered_tail_records; m.dropped_tail_bytes; m.crc_failures;
m.compactions; m.rollovers; m.migrations;
m.last_append_wall_ns; m.last_commit_wall_ns; m.last_record_ts;
m.ingest_lag_wall_ns;            // now - last commit (readers) / last append (writers)
m.ingest_lag_record_ns;          // now - last record time (needs timestamp_unit_ns)
m.committed_records; m.file_size; m.format_version; m.read_only;
db.metricsReset();               // zero the counters (last_record_ts / last_commit_wall_ns are kept)
```

All of these exist on the async instances as Promises (`await db.verify()`,
`await db.metrics()`, ...), and `hocdb.openReaderAsync(ticker, path, schema)` /
`adb.openReaderAsync(...)` open readers on a worker thread. Metric fields are
BigInt like every other integer counter in this binding (`summary().count`,
`health().n_gaps`, ...).

### Recovery and checksums

Records that were written after the last commit (a crash before the header
update) are adopted on the next writer open when they are complete and in
timestamp order; torn or misordered bytes are truncated (`metrics()` reports
`recovered_tail_records` / `dropped_tail_bytes`). Linear files carry a CRC32C of
the committed data, checked by `verify()` or, with `verify_on_open`, at open time
(a mismatch then fails the open with `ChecksumMismatch`; without it the data
stays readable, `verify()` returns `false` and `crc_failures` counts it).

### Ring-buffer capacity

With `overwrite_on_full` the file is a ring buffer of exactly
`(max_file_size - 64) / recordSize` records, so size it as

```javascript
const recordSize = 8 + 8 + 8;                                   // timestamp, price, volume
const db = hocdb.dbInit("BTC_USD", "./data", schema, {
    max_file_size: hocdb.headerSize() + 50_000 * recordSize,    // holds exactly 50 000 records
    overwrite_on_full: true
});
```

Ring buffers have no checksum (`verify()` throws `ChecksumUnavailable`) and are
not compacted, rolled over or subject to `retention_span`.

## Important Notes

- **BigInt for timestamps**: Use `BigInt` (e.g., `1620000000n`) for `i64` and `u64` fields
- **Async preferred**: Use `dbInitAsync` for production to avoid blocking the event loop
- **One writer, many readers**: a database has a single writer (exclusive lock); other processes attach with `openReader`
- **Memory management**: The native library handles memory automatically

## Testing

```bash
npm test

# Or run specific tests
node test/test_async_drop.js
node test/test_agg.js
node test/test_query.js
node test/test_indicators.js
node test/test_storage.js     # durability, readers, maintenance, metrics
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

```js
const hocdb = require('hocdb');

const db = hocdb.dbInit('AAPL', './data', schema, { calendar: 'nyse', timestampUnitNs: 1000 });
db.getCalendar();              // 3
db.periodsPerYear(60_000_000); // 98280

db.indicators([{ kind: 'session_vwap', param: 0 }, { kind: 'pivots', param: 0 }],
              { tail: 390, columns: cols, bucket: 60_000_000 });

const run = db.backtest(target, start, end, {
    bucket: 60_000_000, columns: cols,
    params: { initial_equity: 100000, cost_bps: 5, stop_loss: 0.02, position_mode: 1 },
    equity: true, drawdown: true, maxTrades: 1000,
});
run.result.sharpe; run.trades[0].exit_reason;

const splits = hocdb.walkForwardSplits(close.length, 5, 0.6, true);
const perWindow = hocdb.backtestSplits(ts, open, high, low, close, target, splits, params);

const u = hocdb.universe([dbA, dbB, dbC], { columns: cols, bars: 500, bucket: 60_000_000 });
u.rows[0].rank_mom_mid; u.summary.breadth_up; u.corr[0][1];
```

Module-level calendar helpers: `calendarId`, `calendarName`, `calendarSession`,
`calendarSessionForDay`, `calendarIsOpen`, `calendarOpenSeconds`,
`calendarSessionsBetween`, `calendarPeriodsPerYear`, `calendarToLocal`,
`daysFromCivil`, `civilFromDays`, `calendarDefine`, plus the `CALENDAR` id
constants; per handle `setCalendar`, `getCalendar`, `setTimestampUnit`,
`getTimestampUnit`, `periodsPerYear`, `backtest`, `backtestTail`. Counters and
timestamps come back as BigInt, like the other struct decoders.
