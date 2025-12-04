const hocdb = require('./index.js');
const fs = require('fs');

const path = require('path');
const TICKER = "TEST_NODE";
const DATA_DIR = path.join(__dirname, '..', '..', 'b_node_test_data');

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

console.log("Initializing DB...");
const db = hocdb.dbInit(TICKER, DATA_DIR);

console.log("Appending 1,000,000 records...");
const start = performance.now();
for (let i = 0; i < 1_000_000; i++) {
    hocdb.dbAppend(db, i, i * 1.5, i * 2.5);
}
const end = performance.now();
console.log(`Write Time: ${(end - start).toFixed(2)} ms`);
console.log(`Write Throughput: ${(1_000_000 / ((end - start) / 1000)).toFixed(0)} ops / sec`);

console.log("Loading data (Zero-Copy)...");
const loadStart = performance.now();
const buffer = hocdb.dbLoad(db);
const loadEnd = performance.now();

console.log(`Load Time: ${(loadEnd - loadStart).toFixed(4)} ms`);
console.log(`Buffer ByteLength: ${buffer.byteLength} `);

// Verify Data
const float64View = new Float64Array(buffer);
// Struct layout: timestamp(i64), usd(f64), volume(f64) -> 24 bytes
// JS TypedArrays are aligned.
// We need a DataView to read mixed types properly, or assume alignment.
// Zig struct:
// timestamp: i64 (0)
// usd: f64 (8)
// volume: f64 (16)
// Total 24 bytes.

const view = new DataView(buffer);
const recordCount = buffer.byteLength / 24;
console.log(`Records Loaded: ${recordCount} `);

if (recordCount !== 1_000_000) {
    console.error("Record count mismatch!");
    process.exit(1);
}

// Check first and last record
const firstTimestamp = view.getBigInt64(0, true); // Little endian
const firstUsd = view.getFloat64(8, true);
const firstVolume = view.getFloat64(16, true);

console.log(`First Record: TS = ${firstTimestamp}, USD = ${firstUsd}, VOL = ${firstVolume} `);

const lastOffset = (recordCount - 1) * 24;
const lastTimestamp = view.getBigInt64(lastOffset, true);
const lastUsd = view.getFloat64(lastOffset + 8, true);
const lastVolume = view.getFloat64(lastOffset + 16, true);

console.log(`Last Record: TS = ${lastTimestamp}, USD = ${lastUsd}, VOL = ${lastVolume} `);

if (Number(firstTimestamp) !== 0 || firstUsd !== 0 || firstVolume !== 0) {
    console.error("First record mismatch!");
    process.exit(1);
}

if (Number(lastTimestamp) !== 999999) {
    console.error("Last record timestamp mismatch!");
    process.exit(1);
}

console.log("Closing DB...");
hocdb.dbClose(db);

console.log("✅ Test Passed!");
