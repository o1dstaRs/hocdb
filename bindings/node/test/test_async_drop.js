const hocdb = require('../index.js');
const fs = require('fs');
const path = require('path');

const TICKER = "TEST_ASYNC";
const DATA_DIR = path.join(__dirname, '..', '..', '..', 'b_node_test_data');

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

const schema = [
    { name: "timestamp", type: "i64" },
    { name: "value", type: "f64" }
];

async function runTest() {
    let db;
    try {
        console.log("Initializing Async DB...");
        db = await hocdb.dbInitAsync(TICKER, DATA_DIR, schema, {
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
        const results = await db.query(0, count, []);
        console.log(`Query returned ${results.length} records`);
        if (results.length !== count) {
            throw new Error(`Expected ${count} records, got ${results.length}`);
        }

        console.log("Getting stats asynchronously...");
        const stats = await db.getStats(0, count, 1); // Field index 1 is 'value'
        console.log("Stats:", stats);
        if (stats.count !== BigInt(count)) {
            throw new Error(`Expected stats count ${count}, got ${stats.count}`);
        }

        console.log("Dropping DB...");
        await db.drop();
        db = null; // Prevent double close

        // Verify file is gone
        const filePath = path.join(DATA_DIR, `${TICKER}.bin`);
        if (fs.existsSync(filePath)) {
            throw new Error(`Database file ${filePath} still exists after drop!`);
        }
        console.log("Database file successfully deleted.");

        console.log("✅ Async Test Passed!");
    } finally {
        if (db) {
            try { await db.close(); } catch (e) { /* ignore */ }
        }
        if (fs.existsSync(DATA_DIR)) {
            fs.rmSync(DATA_DIR, { recursive: true, force: true });
        }
    }
}

runTest().catch(err => {
    console.error("Test Failed:", err);
    process.exit(1);
});
