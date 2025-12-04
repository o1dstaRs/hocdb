const hocdb = require('./index.js');
const path = require('path');
const fs = require('fs');

const TICKER = "TEST_QUERY_NODE";
const DATA_DIR = path.join(__dirname, "test_query_node_data");

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

// Define Schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "value", type: "f64" }
];

console.log("Initializing DB...");
const db = hocdb.dbInit(TICKER, DATA_DIR, schema, {
    max_file_size: 1024 * 1024,
    overwrite_on_full: true
});

console.log("Appending data...");
// Append 100, 200, 300, 400, 500
db.append({ timestamp: 100n, value: 1.0 });
db.append({ timestamp: 200n, value: 2.0 });
db.append({ timestamp: 300n, value: 3.0 });
db.append({ timestamp: 400n, value: 4.0 });
db.append({ timestamp: 500n, value: 5.0 });

console.log("Querying range 200 to 450...");
const res = db.query(200n, 450n);
console.log(`Query result count: ${res.length}`);

if (res.length !== 3) {
    throw new Error(`Expected 3 records, got ${res.length}`);
}

if (Number(res[0].timestamp) !== 200) throw new Error(`Expected 200, got ${res[0].timestamp}`);
if (Number(res[1].timestamp) !== 300) throw new Error(`Expected 300, got ${res[1].timestamp}`);
if (Number(res[2].timestamp) !== 400) throw new Error(`Expected 400, got ${res[2].timestamp}`);

console.log("✅ Node.js Query Test Passed!");

db.close();
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}
