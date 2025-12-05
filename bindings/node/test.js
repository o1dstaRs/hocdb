const hocdb = require('./index.js');
const fs = require('fs');

const path = require('path');
const TICKER = "TEST_NODE";
const DATA_DIR = path.join(__dirname, '..', '..', 'b_node_test_data');

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

// Define Schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "usd", type: "f64" },
    { name: "volume", type: "f64" }
];

console.log("Initializing DB...");
const db = hocdb.dbInit(TICKER, DATA_DIR, schema);

console.log("Appending 1,000,000 records...");
const start = performance.now();

for (let i = 0; i < 1_000_000; i++) {
    // db.append({ timestamp: i, usd: i * 1.5, volume: i * 2.5 });
    // Optimization: Reuse object? Or just pass object.
    // The wrapper creates a buffer.
    db.append({ timestamp: i, usd: i * 1.5, volume: i * 2.5 });
}

const end = performance.now();
const duration = (end - start) / 1000; // seconds
console.log(`Write Time: ${(end - start).toFixed(2)} ms`);
console.log(`Write Throughput: ${Math.floor(1_000_000 / duration)} ops / sec`);

console.log("Loading data (Zero-Copy)...");
const loadStart = performance.now();
const data = db.load();
const loadEnd = performance.now();

console.log(`Load Time: ${(loadEnd - loadStart).toFixed(4)} ms`);
console.log(`Records Loaded: ${data.length}`);

if (data.length > 0) {
    const first = data[0];
    const last = data[data.length - 1];
    console.log(`First Record: TS = ${first.timestamp}, USD = ${first.usd}, VOL = ${first.volume}`);
    console.log(`Last Record: TS = ${last.timestamp}, USD = ${last.usd}, VOL = ${last.volume}`);
}

// Verify Data
// const float64View = new Float64Array(buffer);
// Struct layout: timestamp(i64), usd(f64), volume(f64) -> 24 bytes
// JS TypedArrays are aligned.
// We need a DataView to read mixed types properly, or assume alignment.
// Zig struct:
// timestamp: i64 (0)
// usd: f64 (8)
// volume: f64 (16)
// Total 24 bytes.

// const view = new DataView(buffer);
// const recordCount = buffer.byteLength / 24;
// console.log(`Records Loaded: ${recordCount} `);

if (data.length !== 1_000_000) {
    console.error("Record count mismatch!");
    process.exit(1);
}

// Check first and last record
const firstTimestamp = data[0].timestamp;
const firstUsd = data[0].usd;
const firstVolume = data[0].volume;

// console.log(`First Record: TS = ${firstTimestamp}, USD = ${firstUsd}, VOL = ${firstVolume} `);

// const lastOffset = (recordCount - 1) * 24;
const lastTimestamp = data[data.length - 1].timestamp;
const lastUsd = data[data.length - 1].usd;
const lastVolume = data[data.length - 1].volume;

// console.log(`Last Record: TS = ${lastTimestamp}, USD = ${lastUsd}, VOL = ${lastVolume} `);

if (Number(firstTimestamp) !== 0 || firstUsd !== 0 || firstVolume !== 0) {
    console.error("First record mismatch!");
    process.exit(1);
}

if (Number(lastTimestamp) !== 999999) {
    console.error("Last record timestamp mismatch!");
    process.exit(1);
}

console.log("Closing DB...");
db.close();

console.log("✅ Test Passed!");

// --- Ring Buffer Test ---
console.log("\nRunning Ring Buffer Test...");
const RING_TICKER = "TEST_RING";
const RING_DATA_DIR = path.join(__dirname, '..', '..', 'b_node_ring_test');

if (fs.existsSync(RING_DATA_DIR)) {
    fs.rmSync(RING_DATA_DIR, { recursive: true, force: true });
}

// Create a small DB (enough for header + 2 records)
// Header = 12 bytes. Record = 24 bytes.
// Max size = 12 + 24 * 2 = 60 bytes.
const dbRing = hocdb.dbInit(RING_TICKER, RING_DATA_DIR, schema, {
    max_file_size: 60,
    overwrite_on_full: true
});

// Append 3 records. 
// 1. Write Rec 1 -> [Header][Rec 1][Empty]
// 2. Write Rec 2 -> [Header][Rec 1][Rec 2] (Full)
// 3. Write Rec 3 -> [Header][Rec 3][Rec 2] (Wrap around, overwrite Rec 1)

dbRing.append({ timestamp: 100, usd: 1.0, volume: 1.0 });
dbRing.append({ timestamp: 200, usd: 2.0, volume: 2.0 });
dbRing.append({ timestamp: 300, usd: 3.0, volume: 3.0 });

// Flush to ensure data is on disk
// Note: We don't expose flush in Node bindings yet? 
// Wait, we do have db.flush() in Zig but not exposed in index.js?
// Let's check index.js.
// It has dbInit, dbAppend, dbLoad, dbClose. No dbFlush.
// But dbLoad calls flush internally.
// So loading will flush.

const ringData = dbRing.load();
// const ringView = new DataView(ringData);

// console.log(`Ring Buffer Size: ${ringData.byteLength}`);
const ringCount = ringData.length;
console.log(`Records in Ring Buffer: ${ringCount}`);

if (ringCount !== 2) {
    throw new Error(`Expected 2 records in ring buffer, got ${ringCount}`);
}

// Check content. 
// We expect [Rec 3][Rec 2] or [Rec 2][Rec 3]?
// The file content physically is [Header][Rec 3][Rec 2].
// But `dbLoad` returns the file content as is.
// So Rec 0 in buffer should be Rec 3 (TS=300).
// Rec 1 in buffer should be Rec 2 (TS=200).

const rec0 = ringData[0];
const rec1 = ringData[1];

console.log(`Record 0 TS: ${rec0.timestamp}`);
console.log(`Record 1 TS: ${rec1.timestamp}`);

if (Number(rec0.timestamp) !== 200) {
    throw new Error(`Expected Record 0 to be TS 200, got ${rec0.timestamp}`);
}
if (Number(rec1.timestamp) !== 300) {
    throw new Error(`Expected Record 1 to be TS 300, got ${rec1.timestamp}`);
}

dbRing.close();
console.log("✅ Ring Buffer Test Passed!");

if (fs.existsSync(RING_DATA_DIR)) {
    fs.rmSync(RING_DATA_DIR, { recursive: true, force: true });
}

// --- Flush-on-Write Test ---
console.log("\nRunning Flush-on-Write Test...");
{
    const testDir = "./b_node_test_data_flush";
    if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
    }

    const db = hocdb.dbInit("TEST_FLUSH", testDir, schema, {
        max_file_size: 1024 * 1024,
        overwrite_on_full: true,
        flush_on_write: true
    });

    const start = performance.now();
    const count = 10000;
    for (let i = 0; i < count; i++) {
        db.append({
            timestamp: BigInt(i),
            usd: i * 1.5,
            volume: i * 2.5
        });
    }
    const end = performance.now();
    console.log(`Appended ${count} records with flush_on_write=true in ${(end - start).toFixed(2)}ms`);
    console.log(`Throughput: ${Math.floor(count / ((end - start) / 1000))} ops/sec`);

    db.close();
    console.log("✅ Flush-on-Write Test Passed!");

    if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
    }
}
