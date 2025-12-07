import { HOCDB } from "../index.ts";
import { join } from "path";
import { rmSync, existsSync } from "fs";

const TICKER = "TEST_QUERY_BUN";
const DATA_DIR = join(import.meta.dir, "..", "..", "..", "b_bun_test_query");

// Cleanup
if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
}

// Define Schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "value", type: "f64" }
] as const;

console.log("Initializing DB...");
const db = new HOCDB(TICKER, DATA_DIR, schema as any, {
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

console.log("✅ Bun Query Test Passed!");

db.close();
if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
}
