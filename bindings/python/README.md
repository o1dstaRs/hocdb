# HOCDB Python Bindings

Python bindings for HOCDB - The World's Most Performant Time-Series Database.

## Prerequisites

Before using the Python bindings, build the C library:

```bash
# From the main HOCDB directory
zig build c-bindings
```

This creates the required shared library in `zig-out/lib/`.

## Requirements

- Python 3.8+
- HOCDB C library (built with `zig build c-bindings`)

## Quick Start

```python
from hocdb_python import HOCDB, HOCDBField, FieldTypes

# Define schema
schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("volume", FieldTypes.F64),
    HOCDBField("active", FieldTypes.BOOL)
]

# Create database instance
db = HOCDB("BTC_USD", "data", schema)

# Append records
db.append({"timestamp": 1620000000, "price": 50000.0, "volume": 1.5, "active": True})
db.append({"timestamp": 1620000001, "price": 50100.0, "volume": 2.0, "active": True})

# Flush to disk
db.flush()

# Query data
results = db.query(1620000000, 1620000100)
print(f"Found {len(results)} records")

# Get statistics
stats = db.get_stats(1620000000, 1620000100, "price")
print(f"Min: {stats['min']}, Max: {stats['max']}, Mean: {stats['mean']}")

# Get latest value
latest = db.get_latest("price")
print(f"Latest price: {latest['value']} at {latest['timestamp']}")

# Close when done
db.close()
```

## API Reference

### Field Types

| Constant | Type | Size |
|----------|------|------|
| `FieldTypes.I64` | Signed 64-bit integer | 8 bytes |
| `FieldTypes.F64` | 64-bit floating point | 8 bytes |
| `FieldTypes.U64` | Unsigned 64-bit integer | 8 bytes |
| `FieldTypes.BOOL` | Boolean | 1 byte |

### `HOCDB(ticker, path, schema, **options)`

Initialize the database with a dynamic schema.

Opens (or creates) the database as a **writer**: the constructor calls `hocdb_init_ex`, takes an exclusive lock on the file and fails immediately when another writer holds it. To attach to a database that another process writes, use [`HOCDB.open_reader()`](#durability-readers-and-operations).

**Parameters:**
- `ticker` (str): Ticker symbol / database name
- `path` (str): Directory path for data files
- `schema` (list): List of `HOCDBField` objects defining the schema
- `max_file_size` (int, optional): Maximum file size in bytes (0 for default). A ring buffer for N records is `HOCDB.header_size() + N * record_size` bytes
- `overwrite_on_full` (bool, optional): Enable ring buffer mode - overwrite oldest records when full (default: False)
- `flush_on_write` (bool, optional): Flush to disk on every write (default: False)
- `auto_increment` (bool, optional): Auto-increment timestamp field (default: False)
- `fsync`, `fsync_interval_ms`, `verify_on_open`, `retention_span`, `rollover_size`, `auto_migrate`, `timestamp_unit_ns`, `index_stride`: durability and maintenance options, see [Durability, readers and operations](#durability-readers-and-operations)

**Raises:** `RuntimeError("Failed to initialize HOCDB: <error name>")` where the error name comes from `hocdb_last_error()`: `DatabaseLocked` (another writer holds the file), `SchemaMismatch`, `ChecksumMismatch` (with `verify_on_open=True`), `LegacyFormatNeedsMigration`, ...

**Example:**
```python
from hocdb_python import HOCDB, HOCDBField, FieldTypes

schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("volume", FieldTypes.F64)
]

# Basic initialization
db = HOCDB("BTC_USD", "data", schema)

# With ring buffer (circular buffer that overwrites old data)
db = HOCDB("BTC_USD", "data", schema,
           max_file_size=1024*1024*100,  # 100MB
           overwrite_on_full=True)

# With auto-incrementing timestamps
db = HOCDB("BTC_USD", "data", schema, auto_increment=True)
```

### `append(*args)` / `append(dict)` / `append(tuple)`

Append a record to the database.

**Parameters:**
- Values can be passed as separate arguments, a dictionary, a tuple, or a list

**Returns:** `bool` - True if successful

**Example:**
```python
# Using dictionary (recommended)
db.append({"timestamp": 1620000000, "price": 50000.0, "volume": 1.5})

# Using positional arguments
db.append(1620000000, 50000.0, 1.5)

# Using tuple
db.append((1620000000, 50000.0, 1.5))
```

### `flush()`

Commit the buffered records: they become visible to readers and durable according to the `fsync` policy (see [Durability, readers and operations](#durability-readers-and-operations)). On a reader handle `flush()` is the same as `refresh()`.

**Returns:** `bool` - True if successful

**Example:**
```python
db.append({"timestamp": 1620000000, "price": 50000.0, "volume": 1.5})
db.flush()  # Ensure data is persisted
```

### `load()`

Load all records from the database.

**Returns:** `list[dict]` - List of records as dictionaries

**Example:**
```python
records = db.load()
for record in records:
    print(f"Price at {record['timestamp']}: {record['price']}")
```

### `query(start_ts, end_ts, filters=None)`

Query records within a timestamp range with optional filters.

**Parameters:**
- `start_ts` (int): Start timestamp (inclusive)
- `end_ts` (int): End timestamp (inclusive)
- `filters` (list or dict, optional): Filter conditions

**Returns:** `list[dict]` - List of matching records

**Filter Syntax:**
```python
# Simple equality filter using dict syntax
filters = {"price": 50000.0}

# Multiple filters
filters = [
    {"price": 50000.0},
    {"active": True}
]

# Legacy syntax with field_index
filters = [{"field_index": 1, "value": 50000.0}]
```

**Example:**
```python
# Query all records in time range
results = db.query(1620000000, 1620000100)

# Query with filter
results = db.query(1620000000, 1620000100, {"active": True})
print(f"Found {len(results)} active records")
```

### `get_stats(start_ts, end_ts, field, compute_percentiles=False)`

Compute statistics for a specific field within a time range.

**Parameters:**
- `start_ts` (int): Start timestamp
- `end_ts` (int): End timestamp
- `field` (int or str): Field index or field name
- `compute_percentiles` (bool, optional): Whether to compute percentiles (slower)

**Returns:** `dict` with keys:
- `min`: Minimum value
- `max`: Maximum value
- `sum`: Sum of all values
- `count`: Number of records
- `mean`: Average value
- `p50`, `p90`, `p95`, `p99`: Percentiles (only if `compute_percentiles=True`)

**Example:**
```python
# Basic stats
stats = db.get_stats(1620000000, 1620000100, "price")
print(f"Price range: {stats['min']} - {stats['max']}")
print(f"Average: {stats['mean']}")

# With percentiles
stats = db.get_stats(1620000000, 1620000100, "price", compute_percentiles=True)
print(f"P99: {stats['p99']}")

# Using field index
stats = db.get_stats(1620000000, 1620000100, 1)  # Index of 'price' field
```

### `get_latest(field)`

Get the most recent value and timestamp for a specific field.

**Parameters:**
- `field` (int or str): Field index or field name

**Returns:** `dict` with keys:
- `value`: The latest value
- `timestamp`: The timestamp of the latest record

**Example:**
```python
# Using field name
latest = db.get_latest("price")
print(f"Latest price: {latest['value']} at {latest['timestamp']}")

# Using field index
latest = db.get_latest(1)
```

### `close()`

Close the database handle and release resources.

**Example:**
```python
db.close()
```

### `drop()`

Close the database and delete all data files from disk (writers only; a reader raises `RuntimeError`).

**Example:**
```python
# WARNING: This permanently deletes all data!
db.drop()
```

### Storage operations

`HOCDB.open_reader(ticker, path, schema)`, `refresh()`, `is_read_only()`, `sync()`, `verify()`, `compact(min_ts)`, `retain_last(n)`, `rollover()`, `metrics()`, `metrics_reset()`, `format_version()` and `HOCDB.header_size()` are documented in [Durability, readers and operations](#durability-readers-and-operations).

## Helper Functions

### `create_record_bytes(schema, *values)`

Create raw bytes for a record based on the schema. Useful for advanced use cases.

**Parameters:**
- `schema` (list): List of `HOCDBField` objects
- `*values`: Values for each field in order

**Returns:** `bytes` - Raw bytes representation of the record

**Example:**
```python
from hocdb_python import create_record_bytes, HOCDBField, FieldTypes

schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64)
]

record_bytes = create_record_bytes(schema, 1620000000, 50000.0)
```

## Complete Example

```python
from hocdb_python import HOCDB, HOCDBField, FieldTypes

# Define schema
schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("volume", FieldTypes.F64),
    HOCDBField("is_buy", FieldTypes.BOOL)
]

# Initialize database with ring buffer
db = HOCDB(
    ticker="ETH_USD",
    path="market_data",
    schema=schema,
    max_file_size=1024*1024*100,  # 100MB
    overwrite_on_full=True
)

try:
    # Append market data
    trades = [
        {"timestamp": 1620000000, "price": 2500.0, "volume": 10.0, "is_buy": True},
        {"timestamp": 1620000001, "price": 2501.5, "volume": 5.0, "is_buy": False},
        {"timestamp": 1620000002, "price": 2502.0, "volume": 15.0, "is_buy": True},
    ]

    for trade in trades:
        db.append(trade)

    db.flush()

    # Query buy orders only
    buy_orders = db.query(1620000000, 1620000100, {"is_buy": True})
    print(f"Buy orders: {len(buy_orders)}")

    # Get price statistics
    stats = db.get_stats(1620000000, 1620000100, "price", compute_percentiles=True)
    print(f"Price stats: min={stats['min']}, max={stats['max']}, p50={stats['p50']}")

    # Get latest price
    latest = db.get_latest("price")
    print(f"Latest price: {latest['value']}")

finally:
    db.close()
```

## Indicators & analytics

HOCDB computes technical indicators and quantitative analytics inside the engine, in one pass over the data: batches of indicators over a time window or the last *n* rows, tick-to-bar aggregation (`bucket`), OHLCV bars (with buy volume from a trade side), microstructure kinds on ticks with bid/ask/side, pairs analytics across two databases (`pair_indicators()`), forward-looking labels, session-anchored kinds, a scalar risk/return `summary()` of a series, data-quality `health()`, an `evaluate()` of trading decisions and one-shot `snapshot()` / `snapshot_multi()` of ~100 indicators for the latest bar of one or several timeframes (a ready-made input for an LLM or trading agent). Results are plain Python lists, or numpy arrays with `as_numpy=True` when numpy is installed; nothing beyond the standard library is required. See [../../INDICATORS.md](../../INDICATORS.md) for the full table of the 83 kinds, parameters, defaults and conventions.

### Batch of indicators by name

```python
from hocdb_python import HOCDB, HOCDBField, FieldTypes

schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("open", FieldTypes.F64),
    HOCDBField("high", FieldTypes.F64),
    HOCDBField("low", FieldTypes.F64),
    HOCDBField("close", FieldTypes.F64),
    HOCDBField("volume", FieldTypes.F64),
]
db = HOCDB("BTC_USD", "data", schema)
# ... append bars ...

res = db.indicators(
    [
        {"kind": "sma", "period": 20},
        {"kind": "rsi", "period": 14},
        {"kind": "macd"},                                  # defaults: 12/26/9
        {"kind": "bbands", "period": 20, "param": 2.0},    # param = k
        {"kind": "sma", "period": 10, "field": "volume", "label": "vol_sma"},
    ],
    start_ts=1620000000, end_ts=1620086400,                # window [start, end)
)
print(res["n_rows"], res["timestamps"][:3])
print(list(res["columns"]))
# ['sma_20', 'rsi_14', 'macd', 'macd_signal', 'macd_hist',
#  'bbands_20_upper', 'bbands_20_middle', 'bbands_20_lower',
#  'bbands_20_percent_b', 'bbands_20_bandwidth', 'vol_sma']
last_rsi = res["columns"]["rsi_14"][-1]

# the last 100 rows instead of a window
tail = db.indicators([{"kind": "ema", "period": 50}], tail=100)

# numpy arrays instead of lists (numpy is optional)
arr = db.indicators([{"kind": "ema", "period": 50}], tail=100, as_numpy=True)
```

### Snapshot for an LLM agent

```python
import json

snap = db.snapshot(periods_per_year=365)   # ~100 indicators for the latest bar
print(snap["timestamp"], snap["close"], snap["rsi_14"], snap["macd_hist"], snap["adx_14"])

# hand the whole market state to a model
prompt = "Latest market state:\n" + json.dumps(snap, indent=2) + "\nDescribe the trend and the risk."

# risk / return statistics of a series over a window
s = db.summary(1620000000, 1620086400, "close", periods_per_year=365)
print(s["sharpe"], s["max_drawdown"], s["win_rate"], s["hurst"])
```

### Tick -> bar with `bucket` + `ohlcv()`

```python
# a tick database: (timestamp, price, qty)
tick_schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("qty", FieldTypes.F64),
]
ticks = HOCDB("BTC_USD_TICKS", "ticks", tick_schema)
# ... append trades ...

# 5-minute OHLCV bars (bucket = 300 timestamp units)
bars = ticks.ohlcv(0, 2**62, 300, price="price", volume="qty")
print(bars["n_bars"], bars["close"][-1], bars["volume"][-1], bars["count"][-1])

# indicators computed on 5-minute bars built from the ticks, for the last 200 bars
res = ticks.indicators(
    [{"kind": "ema", "period": 21}, {"kind": "atr", "period": 14}, {"kind": "vwap"}],
    tail=200, bucket=300,
    columns={"close": "price", "volume": "qty"},   # optional here: price / qty are auto-detected
)
```

### Ticks with quotes: microstructure, sessions, labels

With `bid`, `ask` and `side` (1 / `True` = buy) fields the microstructure kinds run directly on ticks. Column roles are auto-detected by field name (`open`/`high`/`low`/`close`/`volume`/`bid`/`ask`/`side`; `price` serves as `close` and `size` or `qty` as `volume` when the literal names are absent) or given explicitly with `columns={"close": "price", "bid": "bid", "ask": "ask", "side": "side"}`.

```python
# (timestamp us, price, size, bid, ask, side) ticks
res = ticks.indicators(
    [
        {"kind": "spread"},                                       # spread_abs, spread_bps
        {"kind": "order_flow", "period": 10},                     # order_flow_10_net / _imbalance (volume + side)
        {"kind": "trade_intensity", "period": 10, "param": 1e6},  # param = timestamp units per second
        {"kind": "session_vwap", "param": 3_600_000_000},         # param = session length (mandatory), param2 = offset
        {"kind": "forward_return", "period": 5},                  # LABEL: ret / max / min over the NEXT 5 rows
    ],
    tail=1000,
)
```

**Look-ahead warning:** `forward_return` and `triple_barrier` are *labels*: they use future rows by design (and are NaN at the end of every window). Never feed them to a model as features for the same row. `indicator_is_lookahead(kind)` (module-level and `db.indicator_is_lookahead(kind)`) returns `True` for them.

**Session kinds** (`session_vwap`, `session_range`, `opening_range`, `pivots`) require `param` = the session length in timestamp units (`param2` = session offset); a spec without it raises `ValueError`.

### Pairs: two databases

```python
btc = HOCDB("BTC_USD", "ticks", tick_schema)
eth = HOCDB("ETH_USD", "ticks", tick_schema)

# the close of `eth` is the second series: series2 / ratio / ratio_zscore / rel_strength / correl / beta use both
pair = btc.pair_indicators(
    eth,
    [{"kind": "series"}, {"kind": "series2"}, {"kind": "ratio"},
     {"kind": "ratio_zscore", "period": 100}, {"kind": "correl", "period": 30}],
    tail=500, bucket=60_000_000,                   # 1-minute bars, inner-joined on bar timestamps
    columns=None, other_columns=None,              # roles of btc / eth (auto-detected here)
)
print(pair["columns"]["ratio"][-1], pair["columns"]["correl_30"][-1])
# bucket=0: each btc tick is paired with the latest eth tick at or before it (as-of join)
```

### OHLCV with buy volume, health, evaluation, multi-timeframe snapshot

```python
bars = ticks.ohlcv(0, 2**62, 60_000_000, price="price", volume="size", side="side")   # + bars["buy_volume"]
h = ticks.health(0, 2**62, price="price", volume="size", gap_threshold=5_000_000, outlier_threshold=0.05)  # count, n_gaps, median_gap, n_outlier_returns, ...
ev = ticks.evaluate([{"timestamp": t0, "direction": 1, "size": 1000, "horizon": 3_600_000_000}], price="price", default_horizon=600_000_000, cost_bps=5)  # hit_rate, sharpe, ... + entry / exit / net_return lists
snaps = ticks.snapshot_multi([60_000_000, 300_000_000], periods_per_year=[525600, 105120], bars=500)   # [1-minute snapshot, 5-minute snapshot]
```

### `indicators(specs, start_ts=None, end_ts=None, tail=None, columns=None, lookback="auto", bucket=0, as_numpy=False)`

Compute a batch of indicators in one pass. Pass either `start_ts`/`end_ts` or `tail`.

| Option | Default | Meaning |
|--------|---------|---------|
| `specs` | required | List of spec dicts (see below); a single dict is accepted too |
| `start_ts`, `end_ts` | - | Window `[start_ts, end_ts)`; mutually exclusive with `tail` |
| `tail` | - | Return the last *n* rows (bars when `bucket > 0`) |
| `columns` | auto | `{"open", "high", "low", "close", "volume", "bid", "ask", "side"}` -> field name or index. By default fields literally named like the roles are used; a field named `price` serves as `close` when there is no `close`, and `size` or `qty` as `volume` when there is no `volume`. `close` is required; an error is raised when it cannot be found. `bid`/`ask` feed `spread`, `volume`+`side` feed `order_flow` (and per-bar buy volume when `bucket > 0`) |
| `lookback` | `"auto"` | Extra records (bars when `bucket > 0`) read *before* the window so that the first in-window values are converged. `"auto"` = the recommended per-spec warm-up, an int for a fixed amount, `0` for none |
| `bucket` | `0` | `0` = one row per record; `> 0` = records are first aggregated into OHLCV bars of that many timestamp units (tick -> bar). Per-spec `field` overrides are rejected when `bucket > 0` |
| `as_numpy` | `False` | Return numpy arrays instead of lists (only when numpy is importable; otherwise lists) |

**Spec keys:** `kind` (name such as `"rsi"` or an `IndicatorKinds` id), `period`, `period2`, `period3`, `period4`, `param`, `param2`, `field`, `field2`, `label`. Zero or missing periods/params select the documented defaults (RSI 14, MACD 12/26/9, BBANDS 20 x 2.0, ATR 14, ...). `param` is the BBANDS k, the KELTNER/SUPERTREND multiplier, the PSAR acceleration, the periods-per-year for HIST_VOL/SHARPE/SORTINO/REALIZED_VOL, the timestamp units per second for TRADE_INTENSITY (default 1e6), the up-barrier fraction for TRIPLE_BARRIER (0.02) and the session length for the session kinds (mandatory); `param2` is the PSAR max acceleration, the TRIPLE_BARRIER down-barrier fraction and the session offset. `field` (name or index) runs a single-series indicator on that field instead of the close column; `field2` is the second series for `series2`/`ratio`/`ratio_zscore`/`rel_strength`/`correl`/`beta` (in `pair_indicators()` the other database's close takes that role instead).

**Returns:** `{"timestamps": [...], "n_rows": int, "columns": {name: [...]}}`, one array per output.

**Column naming rule:** the label is `spec["label"]` if given, otherwise the kind name plus `_<period>` when a period is given (`"sma_20"`, `"rsi"`, `"macd"`). Single-output kinds use the label as the column name; multi-output kinds use `<label>_<output>`, except that the output named like the kind keeps the bare label (`"macd"`, `"macd_signal"`, `"macd_hist"`, `"bbands_20_upper"`, ...). `indicator_outputs(kind)` lists the outputs of a kind.

**Warm-up / NaN rule:** `NaN` marks the warm-up region where a value is not defined yet (for example the first 19 rows of `sma_20` with `lookback=0`). NaN is kept as `float("nan")`, never converted to `None`. With `lookback="auto"` the warm-up is read before the window, so in-window values are converged whenever enough history exists.

**Errors:** unknown kind names/ids, unknown field names, invalid field indices, a missing `close` column, an indicator that needs `open`/`high`/`low`/`volume` data that was not provided, and `field` overrides with `bucket > 0` raise `ValueError` with a readable message.

### `pair_indicators(other, specs, start_ts=None, end_ts=None, tail=None, columns=None, other_columns=None, lookback="auto", bucket=0, as_numpy=False)`

Same options and result shape as `indicators()`, computed over this database aligned with `other` (another open `HOCDB` instance): the close column of `other` becomes the second input series of `series2`, `ratio`, `ratio_zscore`, `rel_strength`, `correl` and `beta`; every other kind runs on this database. `columns` are the roles of this database, `other_columns` the roles of `other` (both auto-detected by default). With `bucket > 0` both databases are resampled into bars and inner-joined on bar timestamps; with `bucket = 0` every row of this database is paired with the latest row of `other` at or before it (as-of join).

### `ohlcv(start_ts, end_ts, bucket, price="close", volume=None, side=None, as_numpy=False)`

Aggregate the records in `[start_ts, end_ts)` into OHLCV bars of `bucket` timestamp units using the `price` field (name or index) and, optionally, the `volume` field. Returns a dict with `timestamps`, `open`, `high`, `low`, `close`, `volume` (the record count per bar when no volume field is given), `count` and `n_bars`. With a `side` field (name or index; 1 / `True` = buy) the dict also contains `buy_volume`, the per-bar volume of the buy-side records; without `side` the key is absent.

### `health(start_ts, end_ts, price="close", volume=None, gap_threshold=0, outlier_threshold=0.0)`

Data-quality statistics of the records in `[start_ts, end_ts)`: a dict with 16 fields (`count`, `first_ts`, `last_ts`, `span`, `mean_gap`, `median_gap`, `max_gap`, `max_gap_at`, `n_gaps`, `n_nonpositive_price`, `n_nan_price`, `n_outlier_returns`, `first_outlier_at`, `max_abs_return`, `n_zero_volume`, `n_negative_volume`). Gaps between consecutive timestamps above `gap_threshold` count in `n_gaps`; |log return| above `outlier_threshold` counts in `n_outlier_returns`; the volume counters need `volume`.

### `evaluate(decisions, price="close", default_horizon=0, cost_bps=0.0)`

Evaluate trading decisions against the recorded prices. `decisions` is a list of dicts `{"timestamp": int, "direction": +1 long / -1 short / 0 flat (ignored), "size": float = 1, "horizon": int = 0}`: entry at the first price at or after `timestamp`, exit at the first price at or after `timestamp + horizon` (`horizon` 0 = `default_horizon`), `cost_bps` charged per side. Returns a dict with the 20 evaluation fields (`n_decisions`, `n_evaluated`, `n_long`, `n_short`, `hit_rate`, `avg_return`, `avg_net_return`, `total_pnl`, `total_cost`, `sharpe`, `profit_factor`, `max_drawdown`, `avg_win`, `avg_loss`, `best`, `worst`, `long_hit_rate`, `short_hit_rate`, `long_avg_return`, `short_avg_return`) plus the per-decision lists `entry`, `exit` and `net_return` (NaN where a decision could not be evaluated, e.g. its exit lies past the data).

### `summary(start_ts, end_ts, field, periods_per_year=0.0)`

Scalar performance / risk summary of a field over `[start_ts, end_ts)`. Returns a dict with 29 fields: `count`, `first`, `last`, `min`, `max`, `mean`, `std`, `total_return`, `log_return`, `ann_return`, `ann_vol`, `sharpe`, `sortino`, `max_drawdown`, `max_drawdown_bars`, `calmar`, `skew`, `kurtosis`, `var_95`, `cvar_95`, `win_rate`, `avg_gain`, `avg_loss`, `profit_factor`, `best`, `worst`, `autocorr_1`, `hurst`, `half_life`. `periods_per_year` annualises `ann_return`/`ann_vol`/`sharpe`/`sortino` (`0` = no annualisation).

### `snapshot(columns=None, bars=0, bucket=0, periods_per_year=0.0)`

One-shot snapshot of ~100 indicators for the latest bar. `bars` is the number of records (bars when `bucket > 0`) to use, `0` = the recommended 2500 so that every field converges. Returns a dict: `timestamp` (int), `bars` (int) and one float per indicator (`sma_20`, `ema_200`, `rsi_14`, `macd_hist`, `adx_14`, `bb_upper`, `supertrend`, `vwap`, `return_20`, `sharpe_20`, ...). NaN means there was not enough data for that field.

### `snapshot_multi(buckets, periods_per_year=None, bars=0, columns=None)`

The same snapshot for several bar sizes from a single read of the data: `buckets` is a list of bar sizes (timestamp units, each `> 0`), `periods_per_year` an optional list of the same length (default: no annualisation). Returns a **list of snapshot dicts in `buckets` order**; `bars` of each entry reports how many bars actually existed for that bucket.

### Registry helpers

```python
from hocdb_python import indicator_kinds, indicator_outputs, indicator_warmup, indicator_is_lookahead, IndicatorKinds

indicator_kinds()                           # ['sma', 'ema', ..., 'pivots'] (83 kinds)
indicator_outputs("macd")                   # ['macd', 'signal', 'hist']
indicator_outputs(IndicatorKinds.BBANDS)    # ['upper', 'middle', 'lower', 'percent_b', 'bandwidth']
indicator_warmup({"kind": "ema", "period": 200})   # recommended warm-up rows for the spec
indicator_is_lookahead("forward_return")    # True: a label that uses future rows; "sma" -> False
```

The same helpers exist as methods on a database instance (`db.indicator_kinds()`, `db.indicator_outputs(kind)`, `db.indicator_warmup(spec)`, `db.indicator_is_lookahead(kind)`); the module-level functions load the C library on first use.

## Durability, readers and operations

Every database file starts with a 64-byte header (`"HOC2"`) that holds an atomically committed write cursor and a CRC32C checksum of the committed data. Legacy `"HOC1"` files are migrated in place the first time a **writer** opens them (`auto_migrate=True`, the default). A writer (`HOCDB(...)`) holds an exclusive lock on the file and a second writer on the same ticker/path fails immediately with `RuntimeError("Failed to initialize HOCDB: DatabaseLocked")`. Readers (`HOCDB.open_reader(...)`) take no lock at all.

### Options

All options are keyword arguments of `HOCDB(ticker, path, schema, ...)` and map 1:1 onto the C `HOCDBConfig` struct (`hocdb_python.HOCDBConfig`, 72 bytes, also available as `db.config`).

| Option | Default | Meaning |
|--------|---------|---------|
| `fsync` | `"on_close"` | Durability policy: `"none"` (never, the OS decides), `"on_close"` (once on close), `"on_flush"` (after every `flush()` / commit), `"interval"` (at most every `fsync_interval_ms`, and on close). The ints 0-3 and `FsyncPolicy.NONE / ON_CLOSE / ON_FLUSH / INTERVAL` are accepted too |
| `fsync_interval_ms` | `0` (= 1000) | Interval of the `"interval"` policy in milliseconds |
| `verify_on_open` | `False` | Recompute the checksum when opening: a corrupted file fails with `ChecksumMismatch` instead of opening |
| `retention_span` | `0` (off) | Drop records older than `last timestamp - retention_span` (timestamp units). Compaction runs automatically once the excess exceeds 25% of the file |
| `rollover_size` | `0` (off) | Archive the file as `<ticker>.<first_ts>-<last_ts>.bin` and continue with an empty file once it exceeds this many bytes |
| `auto_migrate` | `True` | Rewrite legacy `HOC1` files to the current format when a writer opens them (pass `False` to refuse legacy files with `LegacyFormatNeedsMigration`) |
| `timestamp_unit_ns` | `0` (unknown) | Nanoseconds per timestamp unit (`1_000_000_000` for seconds, `1_000_000` for milliseconds, ...); enables the `ingest_lag_record_ns` metric |
| `index_stride` | `0` (= 1024) | Sparse index stride in records |
| `max_file_size`, `overwrite_on_full`, `flush_on_write`, `auto_increment` | | As before (see the constructor) |

```python
from hocdb_python import HOCDB, HOCDBField, FieldTypes, FsyncPolicy

schema = [HOCDBField("timestamp", FieldTypes.I64), HOCDBField("price", FieldTypes.F64)]

db = HOCDB("BTC_USD", "data", schema,
           fsync="on_flush",                  # or FsyncPolicy.ON_FLUSH / 2
           timestamp_unit_ns=1_000_000_000,   # timestamps are seconds
           retention_span=7 * 24 * 3600,      # keep one week of data
           rollover_size=512 * 1024 * 1024,   # archive the file above 512 MiB
           verify_on_open=True)
```

Data written by a writer is visible to readers only after the writer's `flush()` (the commit); durability follows the `fsync` policy. `sync()` flushes and fsyncs right now, whatever the policy.

### The reader pattern: ingestion process + agent process

A reader is a lock-free handle to a database that another process writes. It supports every read method (`query`, `load`, `get_stats`, `get_latest`, `indicators`, `pair_indicators`, `ohlcv`, `summary`, `snapshot`, `snapshot_multi`, `health`, `evaluate`) plus `refresh()` and `is_read_only()`; every read entry point re-reads the writer's committed cursor first (only committed data is visible), `refresh()` does it explicitly. Readers follow files the writer compacts or rolls over. Writes (`append`, `sync`, `compact`, `retain_last`, `rollover`, `drop`) raise `RuntimeError("... the handle is a read-only reader ...")`. Readers require the current file format (legacy files are migrated the first time a writer opens them).

```python
# ingestion process: the only writer
from hocdb_python import HOCDB, HOCDBField, FieldTypes

schema = [HOCDBField("timestamp", FieldTypes.I64), HOCDBField("price", FieldTypes.F64)]
writer = HOCDB("BTC_USD", "data", schema, fsync="interval", fsync_interval_ms=200,
               timestamp_unit_ns=1_000_000)
for ts, price in feed():
    writer.append(ts, price)
    writer.flush()          # commit: from now on readers can see the record
```

```python
# agent process: any number of readers, no lock, no coordination
from hocdb_python import HOCDB, HOCDBField, FieldTypes

schema = [HOCDBField("timestamp", FieldTypes.I64), HOCDBField("price", FieldTypes.F64)]
reader = HOCDB.open_reader("BTC_USD", "data", schema)
assert reader.is_read_only()

latest = reader.get_latest("price")                    # auto-refresh: the latest committed record
snap = reader.snapshot(columns={"close": "price"}, bucket=60_000_000, periods_per_year=525600)
reader.refresh()                                       # explicit refresh (cheap: one 8-byte read)
lag_ms = reader.metrics()["ingest_lag_wall_ns"] / 1e6  # time since the writer's last commit
```

### sync / verify / compact / retain_last / rollover / metrics

```python
db = HOCDB("BTC_USD", "data", schema, fsync="on_flush", timestamp_unit_ns=1_000_000_000)

db.sync()                       # flush + fsync now, whatever the fsync policy (writers only)

ok = db.verify()                # recompute the CRC32C of the committed data: True = ok, False = MISMATCH
                                # raises RuntimeError("verify failed: checksum unavailable ...") for a ring buffer,
                                # a legacy HOC1 file or a file whose tail was adopted by crash recovery

db.compact(min_ts=1_700_000_000)   # keep only records with timestamp >= min_ts (atomic rewrite; readers follow)
db.retain_last(100_000)            # keep only the last 100 000 records

archive = db.rollover()         # archive the current file, continue with an empty one; timestamps stay monotonic
print(archive)                  # data/BTC_USD.1700000000-1700086399.bin
archive_ticker = os.path.basename(archive)[:-len(".bin")]
old = HOCDB(archive_ticker, "data", schema)   # an archive is a normal database: "BTC_USD.1700000000-1700086399"

m = db.metrics()                # dict of the 30 counters (Python ints), decoded from the C struct by name
print(m["appends"], m["flushes"], m["fsyncs"], m["committed_records"], m["file_size"])
print(m["read_ns_p50"], m["read_ns_p99"], m["ingest_lag_wall_ns"], m["ingest_lag_record_ns"])
db.metrics_reset()              # counters back to 0; state fields (last_record_ts, committed_records, ...) are kept

db.format_version()             # 2 (current, 64-byte header) or 1 (legacy HOC1)
HOCDB.header_size()             # 64
```

`metrics()` keys: `appends`, `bytes_written`, `flushes`, `commits`, `fsyncs`, `fsync_ns_total`, `fsync_ns_max`, `reads`, `read_ns_total`, `read_ns_max`, `read_ns_last`, `read_ns_p50`, `read_ns_p99`, `records_read`, `refreshes`, `recovered_tail_records`, `dropped_tail_bytes`, `crc_failures`, `compactions`, `rollovers`, `migrations`, `last_append_wall_ns`, `last_commit_wall_ns`, `last_record_ts`, `ingest_lag_wall_ns` (now - last commit for readers / last append for writers), `ingest_lag_record_ns` (now - last record time, needs `timestamp_unit_ns`), `committed_records`, `file_size`, `format_version`, `read_only`. All values are Python `int`s (the C struct is decoded generically via `hocdb_metrics_field_*`, so new fields appear automatically).

**Recovery and checksum.** Records written after the last commit are adopted on the next writer open when they are complete and in timestamp order; torn or misordered trailing bytes are truncated (`metrics()["recovered_tail_records"]` / `["dropped_tail_bytes"]`). The header stores a CRC32C of the committed data of linear files: `verify()` recomputes and compares it (`crc_failures` counts mismatches), `verify_on_open=True` refuses a corrupted file with `ChecksumMismatch`, and ring buffers, legacy files and a file whose tail was just recovered report "checksum unavailable" until a writer's next `flush()`, `compact()` or `rollover()`.

**Ring-buffer capacity.** With `overwrite_on_full=True` the file is a ring buffer whose capacity in records is `(max_file_size - HOCDB.header_size()) // record_size`; to hold exactly N records pass `max_file_size=HOCDB.header_size() + N * record_size`, i.e. `64 + N * record_size` (a `(timestamp i64, value f64)` record is 16 bytes: `64 + 50 * 16 = 864` bytes hold exactly 50 records).

## Performance

The Python bindings maintain HOCDB's high-performance characteristics through direct C API calls using ctypes, ensuring minimal overhead compared to the native Zig implementation.

## Testing

Run the test scripts from the repository root (after `zig build c-bindings`):

```bash
export PYTHONPATH=$(pwd)/bindings/python
python3 bindings/python/test/test_query.py
python3 bindings/python/test/test_agg.py
python3 bindings/python/test/test_indicators.py
python3 bindings/python/test/test_storage.py     # durability, readers, maintenance, metrics
```

or, from the bindings directory:

```bash
cd bindings/python
python -m pytest test/
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

```python
from hocdb_python import (HOCDB, calendar_id, calendar_session_for_day, days_from_civil,
                          backtest_arrays, walk_forward_splits, universe)

# a database that knows the NYSE calendar; timestamps are microseconds
db = HOCDB("AAPL", "./data", schema, calendar="nyse", timestamp_unit_ns=1000)
db.get_calendar()               # 3
db.periods_per_year(60_000_000) # 98280 = 252 sessions x 390 one-minute bars

# session kinds with param 0 use the exchange sessions (pivots: previous trading day)
res = db.indicators([{"kind": "session_vwap", "param": 0}, {"kind": "pivots", "param": 0}],
                    tail=390, columns=cols, bucket=60_000_000)

# gaps are measured in trading time: nights, weekends and holidays are not gaps
h = db.health(0, 2**63 - 1, price="close", gap_threshold=5 * 60_000_000)
h["n_session_breaks"], h["n_missing_sessions"], h["closed_span"]

# backtest a signal over the same rows an indicator window would return
run = db.backtest(target, start_ts, end_ts, bucket=60_000_000,
                  params={"initial_equity": 100_000, "cost_bps": 5, "slippage_bps": 2,
                          "stop_loss": 0.02, "position_mode": "fraction"},
                  columns=cols, outputs=["equity", "drawdown"], max_trades=1000)
run["result"]["sharpe"], run["result"]["max_drawdown"], run["trades"][0]["exit_reason"]

# walk-forward evaluation on caller-provided bars
splits = walk_forward_splits(len(close), 5, 0.6, anchored=True)
per_window = backtest_splits(ts, open_, high, low, close, target, splits, params=params)

# cross-sectional context over a watch-list (inner-joined on timestamps)
u = universe([db_a, db_b, db_c], cols, n_bars=500, bucket=60_000_000)
u["rows"][0]["rank_mom_mid"], u["summary"]["breadth_up"], u["corr"][0][1]
```

Calendar helpers (all times are UTC seconds): `calendar_id`, `calendar_name`,
`calendar_session`, `calendar_session_for_day`, `calendar_is_open`,
`calendar_open_seconds`, `calendar_sessions_between`, `calendar_periods_per_year`,
`calendar_to_local`, `days_from_civil`, `civil_from_days`, `calendar_define`;
per handle `set_calendar`, `get_calendar`, `set_timestamp_unit`,
`get_timestamp_unit`, `periods_per_year`. Errors: `CalendarRequired` (param 0
without a calendar), `UnknownCalendar`.
