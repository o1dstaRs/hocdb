import { HOCDB } from "./index.ts";
import { join } from "path";
import { rmSync, existsSync } from "fs";

const TICKER = "TEST_BUN";
const DATA_DIR = join(import.meta.dir, "..", "..", "b_bun_test_data");

// Cleanup
if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
}

console.log("Initializing DB...");
const db = new HOCDB(TICKER, DATA_DIR);

console.log("Appending 1,000,000 records...");
const start = performance.now();
for (let i = 0; i < 1_000_000; i++) {
    db.append(i, i * 1.5, i * 2.5);
}
const end = performance.now();
console.log(`Write Time: ${(end - start).toFixed(2)}ms`);
console.log(`Write Throughput: ${(1_000_000 / ((end - start) / 1000)).toFixed(0)} ops/sec`);

console.log("Flushing...");
db.flush();

console.log("Loading data (Zero-Copy)...");
const loadStart = performance.now();
const data = db.load();
const loadEnd = performance.now();

console.log(`Load Time: ${(loadEnd - loadStart).toFixed(4)}ms`);
console.log(`Buffer ByteLength: ${data.byteLength}`);

const recordCount = data.byteLength / 24;
console.log(`Records Loaded: ${recordCount}`);

if (recordCount !== 1_000_000) {
    throw new Error(`Record count mismatch! Expected 1000000, got ${recordCount}`);
}

// Verify Data
// Float64Array view:
// Record 0: [TS (as f64?), USD, VOL]
// Wait, TS is i64. Float64Array will interpret i64 bits as f64.
// This is garbage for TS.
// We need a DataView to read properly.
const view = new DataView(data.buffer);

const firstTimestamp = view.getBigInt64(0, true);
const firstUsd = view.getFloat64(8, true);
const firstVolume = view.getFloat64(16, true);

console.log(`First Record: TS=${firstTimestamp}, USD=${firstUsd}, VOL=${firstVolume}`);

const lastOffset = (recordCount - 1) * 24;
const lastTimestamp = view.getBigInt64(lastOffset, true);
const lastUsd = view.getFloat64(lastOffset + 8, true);
const lastVolume = view.getFloat64(lastOffset + 16, true);

console.log(`Last Record: TS=${lastTimestamp}, USD=${lastUsd}, VOL=${lastVolume}`);

if (Number(firstTimestamp) !== 0 || firstUsd !== 0 || firstVolume !== 0) {
    throw new Error("First record mismatch!");
}

if (Number(lastTimestamp) !== 999999) {
    throw new Error("Last record timestamp mismatch!");
}

console.log("Closing DB...");
db.close();

console.log("✅ Bun Test Passed!");
