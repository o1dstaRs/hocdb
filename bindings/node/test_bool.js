const { dbInit } = require('./index');
const fs = require('fs');
const path = require('path');

const TEST_DIR = path.join(__dirname, 'b_node_test_bool');
if (fs.existsSync(TEST_DIR)) fs.rmSync(TEST_DIR, { recursive: true });
fs.mkdirSync(TEST_DIR);

const schema = [
    { name: 'timestamp', type: 'i64' },
    { name: 'val', type: 'bool' }
];

console.log('Initializing DB...');
const db = dbInit('BOOL_TEST', TEST_DIR, schema);

// Append
console.log('Appending data...');
db.append({ timestamp: 100n, val: true });
db.append({ timestamp: 200n, val: false });
db.append({ timestamp: 300n, val: true });

// Query (implicitly flushes)
console.log('Querying all...');
const results = db.query(0n, 1000n);
console.log('Results:', results);

if (results.length !== 3) throw new Error('Expected 3 records');
if (results[0].val !== true) throw new Error('Record 0 val should be true');
if (results[1].val !== false) throw new Error('Record 1 val should be false');
if (results[2].val !== true) throw new Error('Record 2 val should be true');

// Filter True
console.log('Filtering (true)...');
const filtered = db.query(0n, 1000n, [{ field_index: 1, type: 'bool', value: true }]);
console.log('Filtered (true):', filtered);
if (filtered.length !== 2) throw new Error('Expected 2 filtered records');
if (filtered[0].timestamp !== 100n) throw new Error('Filtered 0 timestamp mismatch');
if (filtered[1].timestamp !== 300n) throw new Error('Filtered 1 timestamp mismatch');

// Filter False
console.log('Filtering (false)...');
const filteredFalse = db.query(0n, 1000n, [{ field_index: 1, type: 'bool', value: false }]);
console.log('Filtered (false):', filteredFalse);
if (filteredFalse.length !== 1) throw new Error('Expected 1 filtered record');
if (filteredFalse[0].timestamp !== 200n) throw new Error('FilteredFalse 0 timestamp mismatch');

console.log('✅ Boolean test passed!');
db.close();
