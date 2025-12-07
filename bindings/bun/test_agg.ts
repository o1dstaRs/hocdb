import { HOCDB } from "./index.ts";
import { rmSync, existsSync } from "fs";

import { join } from "path";

const TEST_DIR = join(import.meta.dir, "..", "..", "b_bun_test_data");
const TICKER = "TEST_BUN_AGG";

if (existsSync(TEST_DIR)) {
    rmSync(TEST_DIR, { recursive: true, force: true });
}

const schema = [
    { name: "timestamp", type: "i64" as const },
    { name: "value", type: "f64" as const }
];

const db = new HOCDB(TICKER, TEST_DIR, schema, {
    max_file_size: 1024 * 1024,
    flush_on_write: true
});

console.log("Appending data...");
db.append({ timestamp: 100n, value: 10.0 });
db.append({ timestamp: 200n, value: 20.0 });
db.append({ timestamp: 300n, value: 30.0 });

console.log("Testing getLatest...");
const latest = db.getLatest(1); // value index = 1
console.log("Latest:", latest);

if (latest.value !== 30.0 || latest.timestamp !== 300n) {
    throw new Error(`getLatest failed: expected {value: 30.0, timestamp: 300}, got ${JSON.stringify(latest, (key, value) => typeof value === 'bigint' ? value.toString() : value)}`);
}

console.log("Testing getStats...");
const stats = db.getStats(0n, 400n, 1);
console.log("Stats:", stats);

if (stats.count !== 3n) throw new Error(`Count mismatch: expected 3, got ${stats.count}`);
if (stats.min !== 10.0) throw new Error(`Min mismatch: expected 10.0, got ${stats.min}`);
if (stats.max !== 30.0) throw new Error(`Max mismatch: expected 30.0, got ${stats.max}`);
if (stats.sum !== 60.0) throw new Error(`Sum mismatch: expected 60.0, got ${stats.sum}`);
if (stats.mean !== 20.0) throw new Error(`Mean mismatch: expected 20.0, got ${stats.mean}`);

db.close();
console.log("Bun Aggregation Test Passed!");
