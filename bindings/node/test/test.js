const hocdb = require('../index.js');
const fs = require('fs');

const path = require('path');
const TICKER = "TEST_NODE";
const DATA_DIR = path.join(__dirname, '..', '..', '..', 'b_node_test_data');

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

function runBasicTest() {
    // Cleanup
    if (fs.existsSync(DATA_DIR)) {
        fs.rmSync(DATA_DIR, { recursive: true, force: true });
    }

    let db;
    try {
        console.log("Initializing DB...");
        db = hocdb.dbInit(TICKER, DATA_DIR, schema);

        console.log("Appending 1,000,000 records...");
        const start = performance.now();

        for (let i = 0; i < 1_000_000; i++) {
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

        if (data.length !== 1_000_000) {
            throw new Error("Record count mismatch!");
        }

        // Check first and last record
        const firstTimestamp = data[0].timestamp;
        const firstUsd = data[0].usd;
        const firstVolume = data[0].volume;

        const lastTimestamp = data[data.length - 1].timestamp;

        if (Number(firstTimestamp) !== 0 || firstUsd !== 0 || firstVolume !== 0) {
            throw new Error("First record mismatch!");
        }

        if (Number(lastTimestamp) !== 999999) {
            throw new Error("Last record timestamp mismatch!");
        }

        console.log("Closing DB...");
        db.close();
        db = null; // Prevent double close in finally if it was closed here
        console.log("✅ Test Passed!");

    } finally {
        if (db) {
            try { db.close(); } catch (e) { /* ignore */ }
        }
        if (fs.existsSync(DATA_DIR)) {
            fs.rmSync(DATA_DIR, { recursive: true, force: true });
        }
    }
}

function runRingBufferTest() {
    console.log("\nRunning Ring Buffer Test...");
    const RING_TICKER = "TEST_RING";
    const RING_DATA_DIR = path.join(__dirname, '..', '..', '..', 'b_node_ring_test');

    if (fs.existsSync(RING_DATA_DIR)) {
        fs.rmSync(RING_DATA_DIR, { recursive: true, force: true });
    }

    let dbRing;
    try {
        dbRing = hocdb.dbInit(RING_TICKER, RING_DATA_DIR, schema, {
            max_file_size: 60,
            overwrite_on_full: true
        });

        dbRing.append({ timestamp: 100, usd: 1.0, volume: 1.0 });
        dbRing.append({ timestamp: 200, usd: 2.0, volume: 2.0 });
        dbRing.append({ timestamp: 300, usd: 3.0, volume: 3.0 });

        const ringData = dbRing.load();
        const ringCount = ringData.length;
        console.log(`Records in Ring Buffer: ${ringCount}`);

        if (ringCount !== 2) {
            throw new Error(`Expected 2 records in ring buffer, got ${ringCount}`);
        }

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
        dbRing = null;
        console.log("✅ Ring Buffer Test Passed!");

    } finally {
        if (dbRing) {
            try { dbRing.close(); } catch (e) { /* ignore */ }
        }
        if (fs.existsSync(RING_DATA_DIR)) {
            fs.rmSync(RING_DATA_DIR, { recursive: true, force: true });
        }
    }
}

function runFlushTest() {
    console.log("\nRunning Flush-on-Write Test...");
    const testDir = path.join(__dirname, '..', '..', '..', 'b_node_test_data_flush');
    if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
    }

    let db;
    try {
        db = hocdb.dbInit("TEST_FLUSH", testDir, schema, {
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
        db = null;
        console.log("✅ Flush-on-Write Test Passed!");

    } finally {
        if (db) {
            try { db.close(); } catch (e) { /* ignore */ }
        }
        if (fs.existsSync(testDir)) {
            fs.rmSync(testDir, { recursive: true, force: true });
        }
    }
}

function main() {
    try {
        runBasicTest();
        runRingBufferTest();
        runFlushTest();
    } catch (e) {
        console.error("Test failed:", e);
        process.exit(1);
    }
}

main();
