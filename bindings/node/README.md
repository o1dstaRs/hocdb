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
    auto_increment: false               // Auto-increment timestamps
};
```

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

## Important Notes

- **BigInt for timestamps**: Use `BigInt` (e.g., `1620000000n`) for `i64` and `u64` fields
- **Async preferred**: Use `dbInitAsync` for production to avoid blocking the event loop
- **Memory management**: The native library handles memory automatically

## Testing

```bash
npm test

# Or run specific tests
node test/test_async_drop.js
node test/test_agg.js
node test/test_query.js
```
