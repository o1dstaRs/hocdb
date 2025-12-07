const hocdb = require('../index.js');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const TICKER = "TEST_AUTO_INC_NODE";
const DATA_DIR = path.join(__dirname, '..', '..', '..', 'b_node_test_auto_inc');

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

// Define Schema
const schema = [
    { name: "timestamp", type: "i64" },
    { name: "value", type: "f64" }
];

console.log("Running Node.js Auto-Increment Test...");

// 1. Initialize with auto_increment = true
{
    const db = hocdb.dbInit(TICKER, DATA_DIR, schema, {
        auto_increment: true
    });

    for (let i = 0; i < 10; i++) {
        // Pass 0 as timestamp, should be overwritten
        db.append({ timestamp: 0, value: i * 1.0 });
    }

    const data = db.load();
    assert.strictEqual(data.length, 10);

    for (let i = 0; i < 10; i++) {
        assert.strictEqual(Number(data[i].timestamp), i + 1);
        assert.strictEqual(data[i].value, i * 1.0);
    }

    db.close();
}

// 2. Reopen and append more
{
    const db = hocdb.dbInit(TICKER, DATA_DIR, schema, {
        auto_increment: true
    });

    for (let i = 10; i < 15; i++) {
        db.append({ timestamp: 999, value: i * 1.0 });
    }

    const data = db.load();
    assert.strictEqual(data.length, 15);

    for (let i = 0; i < 15; i++) {
        assert.strictEqual(Number(data[i].timestamp), i + 1);
        assert.strictEqual(data[i].value, i * 1.0);
    }

    db.close();
}

// Cleanup
if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
}

console.log("✅ Node.js Auto-Increment Test Passed!");
