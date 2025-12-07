const { dbInitAsync } = require("../index");
const fs = require("fs");
const { performance } = require("perf_hooks");

const DATA_DIR = "bench_data_node";
const ITERATIONS = 1_000_000;
const BATCH_SIZE = 1000;

async function runBench() {
    console.log(`\n=== Running Benchmark: WORKER ASYNC ===`);

    if (fs.existsSync(DATA_DIR)) {
        fs.rmSync(DATA_DIR, { recursive: true, force: true });
    }
    fs.mkdirSync(DATA_DIR);

    const schema = [
        { name: "timestamp", type: "i64" },
        { name: "value", type: "f64" },
    ];

    console.log(`Starting Benchmark: ${ITERATIONS} writes...`);

    // Initialize Async DB
    const db = await dbInitAsync("bench_metric", DATA_DIR, schema);

    const start = performance.now();

    const chunk = [];
    for (let i = 0; i < ITERATIONS; i++) {
        // Node.js implementation might not have appendBatch yet, so we check
        if (db.appendBatch) {
            chunk.push({
                timestamp: Date.now() + i,
                value: Math.random() * 100,
            });

            if (chunk.length >= BATCH_SIZE) {
                await db.appendBatch(chunk);
                chunk.length = 0;
            }
        } else {
            // Fallback to single append if batch not implemented
            await db.append({
                timestamp: Date.now() + i,
                value: Math.random() * 100,
            });
        }
    }

    if (chunk.length > 0 && db.appendBatch) {
        await db.appendBatch(chunk);
    }

    // Wait for all operations to complete (flush)
    // Check if flush is async (returns promise) or sync
    const flushResult = db.flush();
    if (flushResult instanceof Promise) {
        await flushResult;
    }

    const end = performance.now();
    const duration = (end - start) / 1000;
    const opsPerSec = ITERATIONS / duration;

    console.log(`\n[WRITE] ${ITERATIONS} records in ${duration.toFixed(2)}s`);
    console.log(`Throughput: ${opsPerSec.toFixed(2)} ops/sec`);

    await db.close();

    // Cleanup
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

async function main() {
    await runBench();
}

main().catch(console.error);
