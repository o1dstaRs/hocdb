import { HOCDB } from "../index";
import { unlinkSync, existsSync, mkdirSync } from "fs";

const DATA_DIR = "bench_data";
const ITERATIONS = 1_000_000;
const BATCH_SIZE = 1000;

async function runBench(fakeAsync = false) {
    const mode = fakeAsync ? "FAKE ASYNC" : "WORKER ASYNC";
    console.log(`\n=== Running Benchmark: ${mode} ===`);

    if (existsSync(DATA_DIR)) {
        // Simple recursive delete for cleanup
        const fs = require('fs');
        fs.rmSync(DATA_DIR, { recursive: true, force: true });
    }
    mkdirSync(DATA_DIR);

    const schema = [
        { name: "timestamp", type: "i64" },
        { name: "value", type: "f64" },
    ];

    console.log(`Starting Benchmark: ${ITERATIONS} writes...`);

    // Initialize Async DB
    const db = await HOCDB.initAsync("bench_metric", DATA_DIR, schema, { fakeAsync });

    const start = performance.now();

    const chunk = [];
    for (let i = 0; i < ITERATIONS; i++) {
        chunk.push({
            timestamp: Date.now() + i,
            value: Math.random() * 100,
        });

        if (chunk.length >= BATCH_SIZE) {
            await db.appendBatch(chunk);
            chunk.length = 0;
        }
    }

    if (chunk.length > 0) {
        await db.appendBatch(chunk);
    }

    // Wait for all operations to complete (flush)
    await db.flush();

    const end = performance.now();
    const duration = (end - start) / 1000;
    const opsPerSec = ITERATIONS / duration;

    console.log(`\n[WRITE] ${ITERATIONS} records in ${duration.toFixed(2)}s`);
    console.log(`Throughput: ${opsPerSec.toFixed(2)} ops/sec`);

    await db.close();

    // Cleanup
    const fs = require('fs');
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

async function main() {
    await runBench(false); // Worker
    await runBench(true);  // Fake Async
}

main().catch(console.error);
