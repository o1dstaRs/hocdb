# HOCDB Node.js Bindings

This directory contains the Node.js bindings for HOCDB, a high-performance, embedded, append-only time-series database.

## Installation

```bash
npm install
```

## Usage

```javascript
const hocdb = require('./index.js');

// Initialize
const db = hocdb.dbInitAsync("BTC_USD", "./data", schema, config);

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
npm test
```

Or run specific tests:

```bash
node test/test_async_drop.js
```
