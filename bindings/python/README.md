# HOCDB Python Bindings

Python bindings for HOCDB - The World's Most Performant Time-Series Database.

## Prerequisites

Before using the Python bindings, you need to build the C library:

```bash
# From the main HOCDB directory
zig build c-bindings
```

This creates the required shared library in `zig-out/lib/`.

## Installation

The Python bindings work directly with the shared library. No separate installation is required beyond ensuring the C library is built.

## Usage

```python
from hocdb_python import HOCDB, HOCDBField, FieldTypes, create_record_bytes

# Define schema
schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("volume", FieldTypes.F64)
]

# Create database instance
db = HOCDB("BTC_USD", "my_data", schema)

# Create and append records
record = create_record_bytes(schema, 1620000000, 50000.0, 1.5)
db.append(record)

# Load all data
data = db.load()
print(f"Loaded {len(data)} bytes of data")

# Get Stats (using field name)
stats = db.get_stats(0, 1000, "price")
print(stats)

# Get Latest Value (using field name)
latest = db.get_latest("price")
print(latest)

# Close when done
db.close()
```

## Requirements

- Python 3.8+
- HOCDB C library (built with `zig build c-bindings`)

## Performance

The Python bindings maintain HOCDB's high-performance characteristics through direct C API calls using ctypes, ensuring minimal overhead compared to the native Zig implementation.