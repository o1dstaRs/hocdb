const hocdb = require('./index.js');
const fs = require('fs');
const path = require('path');

const TICKER = "TEST_FILTER_SYNTAX";
const DATA_DIR = path.join(__dirname, '..', '..', 'b_node_test_filter_syntax');

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

// Define Schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "usd", type: "f64" },
    { name: "event", type: "i64" }
];

console.log("Initializing DB...");
const db = hocdb.dbInit(TICKER, DATA_DIR, schema);

console.log("Appending data...");
// Append 3 records
// 1. event = 0
db.append({ timestamp: 100n, usd: 1.0, event: 0n });
// 2. event = 1
db.append({ timestamp: 200n, usd: 2.0, event: 1n });
// 3. event = 2
db.append({ timestamp: 300n, usd: 3.0, event: 2n });

// Query with new syntax: { event: 1n }
console.log("Querying with filter { event: 1n }...");
try {
    const results = db.query(0n, 1000n, { event: 1n });
    console.log(`Results count: ${results.length}`);

    if (results.length !== 1) {
        throw new Error(`Expected 1 result, got ${results.length}`);
    }

    const rec = results[0];
    console.log(`Result: TS=${rec.timestamp}, Event=${rec.event}`);

    if (rec.event !== 1n) {
        throw new Error(`Expected event 1, got ${rec.event}`);
    }

    console.log("✅ Filter Syntax Test Passed!");
} catch (e) {
    console.error("Test Failed:", e);
    process.exit(1);
} finally {
    db.close();
    if (fs.existsSync(DATA_DIR)) {
        fs.rmSync(DATA_DIR, { recursive: true, force: true });
    }
}
