import { HOCDBAsync, FieldDef } from "../index";
import { join } from "path";
import { rmSync, existsSync } from "node:fs";

const TICKER = "TEST_BUN_ASYNC";
const DATA_DIR = join(import.meta.dir, "..", "..", "..", "b_bun_test_data");

// Cleanup
if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
}

const schema: FieldDef[] = [
    { name: "timestamp", type: "i64" },
    { name: "value", type: "f64" }
];

async function runTest() {
    let db;
    try {
        console.log("Initializing Async DB...");
        // @ts-ignore
        db = new HOCDBAsync(TICKER, DATA_DIR, schema, {
            max_file_size: 1024 * 1024,
            overwrite_on_full: true
        });

        console.log("Appending data asynchronously...");
        const count = 1000;
        const start = performance.now();
        const promises = [];
        for (let i = 0; i < count; i++) {
            promises.push(db.append({ timestamp: BigInt(i), value: i * 1.1 }));
        }
        await Promise.all(promises);
        const end = performance.now();
        console.log(`Appended ${count} records in ${(end - start).toFixed(2)}ms`);

        console.log("Querying data asynchronously...");
        const results = await db.query(0, count, []) as Record<string, number | bigint>[];
        console.log(`Query returned ${results.length} records`);
        if (results.length !== count) {
            throw new Error(`Expected ${count} records, got ${results.length}`);
        }

        console.log("Getting stats asynchronously (using string field 'value')...");
        const stats = await db.getStats(0, count, "value") as { min: number, max: number, sum: number, count: bigint, mean: number };
        console.log("Stats:", stats);
        if (stats.count !== BigInt(count)) {
            throw new Error(`Expected stats count ${count}, got ${stats.count}`);
        }

        console.log("Dropping DB...");
        await db.drop();
        db = null; // Prevent double close

        // Verify file is gone
        const filePath = join(DATA_DIR, `${TICKER}.bin`);
        if (existsSync(filePath)) {
            throw new Error(`Database file ${filePath} still exists after drop!`);
        }
        console.log("Database file successfully deleted.");

        console.log("✅ Bun Async Test Passed!");
    } finally {
        if (db) {
            try { await db.close(); } catch (e) { /* ignore */ }
        }
        if (existsSync(DATA_DIR)) {
            rmSync(DATA_DIR, { recursive: true, force: true });
        }
    }
}

runTest().catch(err => {
    console.error("Test Failed:", err);
    process.exit(1);
});
