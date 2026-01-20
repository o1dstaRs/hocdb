/**
 * Test for explicit flush() method in Node.js API
 */
const hocdb = require('../index.js');
const fs = require('fs');
const path = require('path');

const testDir = './test_flush_data';

function rmrf(dir) {
    if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

async function testFlushSync() {
    console.log('Testing sync flush()...');

    rmrf(testDir);

    const schema = [
        { name: 'timestamp', type: 'i64' },
        { name: 'price', type: 'f64' },
        { name: 'volume', type: 'f64' }
    ];

    const db = hocdb.dbInit('FLUSH_TEST', testDir, schema);

    // Append records without flushing
    db.append({ timestamp: 1620000000n, price: 50000.0, volume: 1.5 });
    db.append({ timestamp: 1620000001n, price: 50001.0, volume: 1.6 });

    // Before flush - data may not be fully persisted
    // After flush - data should definitely be persisted
    db.flush();

    // Verify data is persisted by loading
    const records = db.load();
    if (records.length !== 2) {
        console.error(`FAIL: Expected 2 records after flush, got ${records.length}`);
        db.close();
        rmrf(testDir);
        process.exit(1);
    }

    console.log(`  Loaded ${records.length} records after flush (OK)`);

    // Verify the data directory has files
    const files = fs.readdirSync(testDir);
    if (files.length === 0) {
        console.error('FAIL: No data files after flush');
        db.close();
        rmrf(testDir);
        process.exit(1);
    }
    console.log(`  Data files after flush: ${files.length} (OK)`);

    db.close();
    rmrf(testDir);
    console.log('PASS: sync flush() test\n');
}

async function testFlushAsync() {
    console.log('Testing async flush()...');

    rmrf(testDir);

    const schema = [
        { name: 'timestamp', type: 'i64' },
        { name: 'price', type: 'f64' },
        { name: 'volume', type: 'f64' }
    ];

    const db = await hocdb.dbInitAsync('FLUSH_TEST_ASYNC', testDir, schema);

    // Append records without flushing
    await db.append({ timestamp: 1620000000n, price: 50000.0, volume: 1.5 });
    await db.append({ timestamp: 1620000001n, price: 50001.0, volume: 1.6 });

    // Explicitly flush
    await db.flush();

    // Verify data is persisted by loading
    const records = await db.load();
    if (records.length !== 2) {
        console.error(`FAIL: Expected 2 records after async flush, got ${records.length}`);
        await db.close();
        rmrf(testDir);
        process.exit(1);
    }

    console.log(`  Loaded ${records.length} records after async flush (OK)`);

    // Verify the data directory has files
    const files = fs.readdirSync(testDir);
    if (files.length === 0) {
        console.error('FAIL: No data files after async flush');
        await db.close();
        rmrf(testDir);
        process.exit(1);
    }
    console.log(`  Data files after async flush: ${files.length} (OK)`);

    await db.close();
    rmrf(testDir);
    console.log('PASS: async flush() test\n');
}

async function testFlushPersistence() {
    console.log('Testing flush persistence (close and reopen)...');

    rmrf(testDir);

    const schema = [
        { name: 'timestamp', type: 'i64' },
        { name: 'price', type: 'f64' }
    ];

    // Create, append, flush, and close
    let db = hocdb.dbInit('FLUSH_PERSIST', testDir, schema);
    db.append({ timestamp: 1620000000n, price: 100.0 });
    db.append({ timestamp: 1620000001n, price: 200.0 });
    db.append({ timestamp: 1620000002n, price: 300.0 });
    db.flush();
    db.close();

    // Reopen and verify data persisted
    db = hocdb.dbInit('FLUSH_PERSIST', testDir, schema);
    const records = db.load();

    if (records.length !== 3) {
        console.error(`FAIL: Expected 3 records after reopen, got ${records.length}`);
        db.close();
        rmrf(testDir);
        process.exit(1);
    }

    // Verify record values
    if (Number(records[0].price) !== 100.0 ||
        Number(records[1].price) !== 200.0 ||
        Number(records[2].price) !== 300.0) {
        console.error('FAIL: Record values do not match after reopen');
        db.close();
        rmrf(testDir);
        process.exit(1);
    }

    console.log(`  Verified ${records.length} records after reopen (OK)`);

    db.close();
    rmrf(testDir);
    console.log('PASS: flush persistence test\n');
}

async function main() {
    console.log('=== Node.js Flush Tests ===\n');

    await testFlushSync();
    await testFlushAsync();
    await testFlushPersistence();

    console.log('All flush tests PASSED!');
}

main().catch(err => {
    console.error('Test error:', err);
    rmrf(testDir);
    process.exit(1);
});
