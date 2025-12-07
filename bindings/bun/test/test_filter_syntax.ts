import { HOCDB } from "../index";
import { existsSync, rmSync } from "fs";
import { join } from "path";

const TICKER = "TEST_BUN_FILTER";
const DATA_DIR = join(import.meta.dir, "..", "..", "..", "b_bun_test_filter_syntax");

if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
}

const schema: FieldDef[] = [
    { name: "timestamp", type: "i64" },
    { name: "price", type: "f64" },
    { name: "event", type: "i64" }
];

console.log("Initializing DB...");
const db = new HOCDB(TICKER, DATA_DIR, schema);

console.log("Appending data...");
// 1. event = 0
db.append({ timestamp: 100n, price: 1.0, event: 0n });
// 2. event = 1
db.append({ timestamp: 200n, price: 2.0, event: 1n });
// 3. event = 2
db.append({ timestamp: 300n, price: 3.0, event: 2n });

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

    // Test array syntax too
    console.log("Querying with filter array [{ field_index: 2, value: 2n }]...");
    const results2 = db.query(0n, 1000n, [{ field_index: 2, value: 2n }]);
    if (results2.length !== 1 || results2[0].event !== 2n) {
        throw new Error(`Expected event 2, got ${results2[0]?.event}`);
    }

    console.log("✅ Bun Filter Syntax Test Passed!");
} catch (e) {
    console.error("Test Failed:", e);
    process.exit(1);
} finally {
    db.close();
    if (existsSync(DATA_DIR)) {
        rmSync(DATA_DIR, { recursive: true, force: true });
    }
}
