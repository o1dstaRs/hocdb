import { HOCDB, FieldDef } from "../index.ts";
import { join } from "path";
import { rmSync, existsSync } from "fs";

const TICKER = "TEST_BUN";
const DATA_DIR = join(import.meta.dir, "..", "..", "..", "b_bun_test_data");

// Cleanup
if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
}

// Define Schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "usd", type: "f64" },
    { name: "volume", type: "f64" }
] as const;

console.log("Initializing DB...");
const db = new HOCDB(TICKER, DATA_DIR, schema as any);

console.log("Appending 1,000,000 records...");
const start = performance.now();

for (let i = 0; i < 1_000_000; i++) {
    db.append({ timestamp: BigInt(i), usd: i * 1.5, volume: i * 2.5 });
}

const end = performance.now();
const duration = (end - start) / 1000; // seconds
console.log(`Write Time: ${(end - start).toFixed(2)} ms`);
console.log(`Write Throughput: ${Math.floor(1_000_000 / duration)} ops / sec`);

console.log("Flushing...");
db.flush();

console.log("Loading data (Zero-Copy)...");
const loadStart = performance.now();
const data = db.load();
const loadEnd = performance.now();

console.log(`Load Time: ${(loadEnd - loadStart).toFixed(4)} ms`);
console.log(`Records Loaded: ${data.length} `);

if (data.length > 0) {
    const first = data[0];
    const last = data[data.length - 1];
    console.log(`First Record: TS = ${first.timestamp}, USD = ${first.usd}, VOL = ${first.volume} `);
    console.log(`Last Record: TS = ${last.timestamp}, USD = ${last.usd}, VOL = ${last.volume} `);

    if (Number(first.timestamp) !== 0 || first.usd !== 0 || first.volume !== 0) {
        throw new Error("First record mismatch!");
    }

    if (Number(last.timestamp) !== 999999) {
        throw new Error("Last record timestamp mismatch!");
    }
}

console.log("Closing DB...");
db.close();

console.log("✅ Bun Test Passed!");

// --- Ring Buffer Test ---
console.log("\nRunning Ring Buffer Test...");
const RING_TICKER = "TEST_RING";
const RING_DATA_DIR = join(import.meta.dir, "..", "..", "b_bun_ring_test");

if (existsSync(RING_DATA_DIR)) {
    rmSync(RING_DATA_DIR, { recursive: true, force: true });
}

const dbRing = new HOCDB(RING_TICKER, RING_DATA_DIR, schema as any, {
    max_file_size: 60,
    overwrite_on_full: true
});

dbRing.append({ timestamp: 100n, usd: 1.0, volume: 1.0 });
dbRing.append({ timestamp: 200n, usd: 2.0, volume: 2.0 });
dbRing.append({ timestamp: 300n, usd: 3.0, volume: 3.0 });

const ringData = dbRing.load();
console.log(`Records in Ring Buffer: ${ringData.length} `);

if (ringData.length !== 2) {
    throw new Error(`Expected 2 records in ring buffer, got ${ringData.length} `);
}

const rec0 = ringData[0];
const rec1 = ringData[1];

console.log(`Record 0 TS: ${rec0.timestamp} `);
console.log(`Record 1 TS: ${rec1.timestamp} `);

if (Number(rec0.timestamp) !== 200) {
    throw new Error(`Expected Record 0 to be TS 200, got ${rec0.timestamp} `);
}
if (Number(rec1.timestamp) !== 300) {
    throw new Error(`Expected Record 1 to be TS 300, got ${rec1.timestamp} `);
}

dbRing.close();
console.log("✅ Bun Ring Buffer Test Passed!");

if (existsSync(RING_DATA_DIR)) {
    rmSync(RING_DATA_DIR, { recursive: true, force: true });
}

// --- Flush-on-Write Test ---
console.log("\nRunning Flush-on-Write Test...");
{
    const testDir = join(import.meta.dir, "..", "..", "b_bun_test_data_flush");
    if (existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
    }

    const db = new HOCDB("TEST_FLUSH", testDir, schema, {
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
    console.log(`Appended ${count} records with flush_on_write = true in ${(end - start).toFixed(2)} ms`);
    console.log(`Throughput: ${Math.floor(count / ((end - start) / 1000))} ops / sec`);

    db.close();
    console.log("✅ Flush-on-Write Test Passed!");

    if (existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
    }
}

// --- Filtering Test ---
console.log("\nRunning Filtering Test...");
{
    const testDir = join(import.meta.dir, "..", "..", "b_bun_test_data_filter");
    if (existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
    }

    const db = new HOCDB("FILTER_TEST", testDir, [
        { name: "timestamp", type: "i64" },
        { name: "price", type: "f64" },
        { name: "event", type: "i64" }
    ], { overwrite_on_full: true });

    // Append records
    // 1. Deposit (event=1)
    db.append({ timestamp: 100n, price: 100.0, event: 1n });

    // 2. Withdraw (event=2)
    db.append({ timestamp: 200n, price: 50.0, event: 2n });

    // 3. Deposit (event=1)
    db.append({ timestamp: 300n, price: 200.0, event: 1n });

    db.flush();

    // Filter by event = 1
    const results = db.query(0n, 1000n, { event: 1n });

    console.log(`Filtered results count: ${results.length} `);

    if (results.length !== 2) {
        throw new Error(`Expected 2 records, got ${results.length} `);
    }
    if (results[0].timestamp !== 100n) {
        throw new Error(`Expected first record ts 100, got ${results[0].timestamp} `);
    }
    if (results[1].timestamp !== 300n) {
        throw new Error(`Expected second record ts 300, got ${results[1].timestamp} `);
    }

    db.close();
    console.log("✅ Filtering Test Passed!");

    if (existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
    }
}
