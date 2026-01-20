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
    max_file_size: 1024 * 1024 * 100,  // 100MB (0 or omit for default)
    overwrite_on_full: true,            // Ring buffer mode
    flush_on_write: false,              // Flush on every write
    auto_increment: false               // Auto-increment timestamps
};
```

---

## Synchronous API (`HOCDB`)

For use cases where blocking is acceptable.

### `new HOCDB(ticker, path, schema, config?)`

Create a synchronous database instance.

```typescript
import { HOCDB } from "./index.ts";

const db = new HOCDB("BTC_USD", "./data", schema, config);
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
    max_file_size?: number;
    overwrite_on_full?: boolean;
    flush_on_write?: boolean;
    auto_increment?: boolean;
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

## Important Notes

- **BigInt for integers**: Use `BigInt` (e.g., `1620000000n`) for `i64` and `u64` fields
- **Booleans as numbers**: Use `1` and `0` instead of `true`/`false` for boolean fields
- **Async preferred**: Use `HOCDBAsync` for production to avoid blocking
- **Buffer optimization**: Use `queryInto()` for zero-copy queries when performance is critical

## Testing

```bash
bun test

# Or run specific tests
bun run test/test_async_drop.ts
bun run test/test_agg.ts
bun run test/test_query.ts
```
