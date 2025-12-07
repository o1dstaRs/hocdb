```javascript
const hocdb = require('../index.js');
const fs = require('fs');
const path = require('path');

const TEST_DIR = path.join(__dirname, '..', '..', '..', 'b_node_test_ring_overwrite');
if (fs.existsSync(TEST_DIR)) fs.rmSync(TEST_DIR, { recursive: true });
fs.mkdirSync(TEST_DIR, { recursive: true });

const schema = [
    { name: 'timestamp', type: 'i64' },
    { name: 'value', type: 'i64' }
];

// Record size: 8 + 8 = 16 bytes
// Header size: 12 bytes
// Capacity 5 records: 16 * 5 = 80 bytes
// Max file size: 80 + 12 = 92 bytes

const config = {
    max_file_size: 92,
    overwrite_on_full: true,
    flush_on_write: true // Ensure writes are flushed immediately for predictable testing
};

console.log('Initializing DB with capacity for 5 records...');
const db = dbInit('RING_TEST', TEST_DIR, schema, config);

// Helper to append
function append(ts, val) {
    db.append({ timestamp: BigInt(ts), value: BigInt(val) });
}

// 1. Write 1, 2, 3, 4, 5
console.log('Writing 1, 2, 3, 4, 5...');
append(1, 10);
append(2, 20);
append(3, 30);
append(4, 40);
append(5, 50);

// Verify initial state
let results = db.query(0n, 100n);
console.log('Initial state (1-5):', results.map(r => Number(r.timestamp)));
if (results.length !== 5) throw new Error(`Expected 5 records, got ${ results.length } `);
if (results[0].timestamp !== 1n) throw new Error('Expected first record to be 1');

// 2. Write 6 (should overwrite 1)
console.log('Writing 6 (overwriting 1)...');
append(6, 60);

// 3. Write 7 (should overwrite 2)
console.log('Writing 7 (overwriting 2)...');
append(7, 70);

// 4. Request ranges 1-5 (should return only 3-5)
// Note: Range is [start, end). So 1-5 means timestamps >= 1 and < 5.
// Records present: 3, 4, 5, 6, 7.
// Timestamps in range [1, 5): 3, 4.
// Wait, user said "request ranges 1-5 (should return only 3-5)".
// If user means inclusive 1-5, then 3, 4, 5.
// HOCDB query is [start, end).
// If user wants 1,2,3,4,5, they usually query 1 to 6.
// Let's assume user means "records with timestamps 1 to 5".
// But records 1 and 2 are overwritten.
// So records remaining are 3, 4, 5.
// If query is [1, 6), it should return 3, 4, 5.
// If query is [1, 5), it should return 3, 4.
// Let's check what user said: "request ranges 1-5 (should return only 3-5)".
// This implies inclusive.
// But HOCDB is exclusive end.
// I will query [1, 6) to cover 1-5.
console.log('Querying range 1-6 (expecting 3, 4, 5)...');
results = db.query(1n, 6n);
console.log('Results 1-6:', results.map(r => Number(r.timestamp)));
// Expect: 3, 4, 5
if (results.length !== 3) throw new Error(`Expected 3 records, got ${ results.length } `);
if (results[0].timestamp !== 3n) throw new Error('Expected 3');
if (results[1].timestamp !== 4n) throw new Error('Expected 4');
if (results[2].timestamp !== 5n) throw new Error('Expected 5');

// 5. Request ranges 2-6 (should return 3-6)
// Query [2, 7)
console.log('Querying range 2-7 (expecting 3, 4, 5, 6)...');
results = db.query(2n, 7n);
console.log('Results 2-7:', results.map(r => Number(r.timestamp)));
// Expect: 3, 4, 5, 6
if (results.length !== 4) throw new Error(`Expected 4 records, got ${ results.length } `);
if (results[0].timestamp !== 3n) throw new Error('Expected 3');
if (results[3].timestamp !== 6n) throw new Error('Expected 6');

// 6. Request range 3-7 (should return all 3 4 5 6 7)
// Query [3, 8)
console.log('Querying range 3-8 (expecting 3, 4, 5, 6, 7)...');
results = db.query(3n, 8n);
console.log('Results 3-8:', results.map(r => Number(r.timestamp)));
// Expect: 3, 4, 5, 6, 7
if (results.length !== 5) throw new Error(`Expected 5 records, got ${ results.length } `);
if (results[0].timestamp !== 3n) throw new Error('Expected 3');
if (results[4].timestamp !== 7n) throw new Error('Expected 7');

console.log('✅ Ring buffer overwrite test passed!');
db.close();
