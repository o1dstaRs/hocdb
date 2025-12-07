# HOCDB Bun.js Bindings

This directory contains the Bun.js bindings for HOCDB, utilizing `bun:ffi` for high-performance interaction with the native library.

## Installation

```bash
bun install
```

## Usage

```typescript
import { HOCDB } from "./index.ts";

// Initialize
const db = await HOCDB.initAsync("BTC_USD", "./data", schema);

// Append
await db.append({ timestamp: 100, price: 50000.0 });

// Query
const results = await db.query(0, 200, { price: { gt: 40000.0 } });

// Close
await db.close();
```

## Testing

Run the tests using:

```bash
bun test
```

Or run specific tests:

```bash
bun run test/test_async_drop.ts
```
