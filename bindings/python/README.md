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

**Parameters:**
- `ticker` (str): Ticker symbol / database name
- `path` (str): Directory path for data files
- `schema` (list): List of `HOCDBField` objects defining the schema
- `max_file_size` (int, optional): Maximum file size in bytes (0 for default)
- `overwrite_on_full` (bool, optional): Enable ring buffer mode - overwrite oldest records when full (default: False)
- `flush_on_write` (bool, optional): Flush to disk on every write (default: False)
- `auto_increment` (bool, optional): Auto-increment timestamp field (default: False)

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

Force buffered data to be written to disk.

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

Close the database and delete all data files from disk.

**Example:**
```python
# WARNING: This permanently deletes all data!
db.drop()
```

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

## Performance

The Python bindings maintain HOCDB's high-performance characteristics through direct C API calls using ctypes, ensuring minimal overhead compared to the native Zig implementation.

## Testing

Run the tests from the bindings directory:

```bash
cd bindings/python
python -m pytest test/
```
