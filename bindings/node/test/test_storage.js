// Durability, lock-free readers, maintenance and metrics for the Node.js binding.
// Data dir: b_node_test_storage (removed on exit).
const hocdb = require('../index.js');
const fs = require('fs');
const path = require('path');

const DIR = path.join(__dirname, '..', '..', '..', 'b_node_test_storage');
const TICKER = 'T';
const schema = [{ name: 'timestamp', type: 'i64' }, { name: 'value', type: 'f64' }];
const REC = 16; // record size in bytes
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

function rmrf() {
    if (fs.existsSync(DIR)) fs.rmSync(DIR, { recursive: true, force: true });
}

function assert(cond, msg) {
    if (!cond) throw new Error(`FAIL: ${msg}`);
    console.log(`  ok: ${msg}`);
}

function count(db) {
    return Number(db.getStats(INT64_MIN, INT64_MAX, 'timestamp').count);
}

function appendN(db, from, n) {
    for (let i = 0; i < n; i++) db.append({ timestamp: BigInt(from + i), value: from + i });
}

function catchError(fn) {
    try { fn(); } catch (e) { return e; }
    return null;
}

async function catchAsync(p) {
    try { await p; } catch (e) { return e; }
    return null;
}

function testWriterReaderMaintenance() {
    console.log('\n[1] writer, checksum, metrics');
    const w = hocdb.dbInit(TICKER, DIR, schema, { fsync: 'on_flush', timestampUnitNs: 1e9 });
    assert(w.formatVersion() === 2 && w.isReadOnly() === false, 'writer: formatVersion() 2, isReadOnly() false');
    appendN(w, 1, 1000);
    w.flush();
    assert(w.verify() === true, 'verify() true after flush');
    let m = w.metrics();
    assert(m.appends === 1000n, `metrics.appends 1000 (${m.appends})`);
    assert(m.flushes >= 1n && m.fsyncs >= 1n && m.commits >= 1n, `flushes ${m.flushes}, fsyncs ${m.fsyncs}, commits ${m.commits}`);
    assert(m.committed_records === 1000n && m.read_only === 0n && m.format_version === 2n, 'committed_records 1000, read_only 0, format_version 2');
    assert(m.last_record_ts === 1000n && m.bytes_written === BigInt(1000 * REC), 'last_record_ts 1000, bytes_written 16000');
    assert(m.ingest_lag_wall_ns >= 0n && m.ingest_lag_record_ns !== 0n, 'lag fields (timestampUnitNs set)');
    assert(Object.keys(m).length === 30 && Object.values(m).every(v => typeof v === 'bigint'), '30 BigInt metric fields');

    console.log('\n[2] second writer is refused');
    const locked = catchError(() => hocdb.dbInit(TICKER, DIR, schema));
    assert(locked && locked.message.includes('DatabaseLocked') && locked.code === 'DatabaseLocked', `second writer -> ${locked && locked.message}`);

    console.log('\n[3] lock-free reader');
    const r = hocdb.openReader(TICKER, DIR, schema);
    assert(r.isReadOnly() === true && r.formatVersion() === 2, 'reader: isReadOnly() true');
    assert(count(r) === 1000, 'reader sees 1000 committed records');
    appendN(w, 1001, 500);
    r.refresh();
    assert(count(r) === 1000, 'uncommitted appends are invisible to the reader');
    w.flush();
    r.refresh();
    assert(count(r) === 1500, 'after the writer flush the reader sees 1500');
    assert(r.getLatest('value').timestamp === 1500n, 'reader getLatest auto-refreshes (ts 1500)');
    const q = r.query(1498n, 1500n);
    assert(q.length >= 2 && q[0].timestamp === 1498n && q[1].timestamp === 1499n, 'reader query works');
    assert(r.load().length === 1500, 'reader load works');
    const ro = catchError(() => r.append({ timestamp: 9999n, value: 0 }));
    assert(ro && ro.code === 'ReadOnly' && /reader/.test(ro.message), `reader append -> ${ro && ro.message}`);
    for (const [name, fn] of [['sync', () => r.sync()], ['compact', () => r.compact(0n)], ['retainLast', () => r.retainLast(1)], ['rollover', () => r.rollover()], ['drop', () => r.drop()]]) {
        const e = catchError(fn);
        assert(e && e.code === 'ReadOnly', `reader ${name}() -> ReadOnly`);
    }
    const rm = r.metrics();
    assert(rm.read_only === 1n && rm.refreshes >= 1n && rm.reads >= 3n, `reader metrics: read_only 1, refreshes ${rm.refreshes}, reads ${rm.reads}`);
    w.metricsReset();
    m = w.metrics();
    assert(m.appends === 0n && m.last_record_ts === 1500n, 'metricsReset() zeroes counters, keeps last_record_ts');

    console.log('\n[4] compaction and retention');
    w.compact(1001n);
    assert(count(w) === 500 && w.getStats(INT64_MIN, INT64_MAX, 'timestamp').min === 1001, 'compact(1001): 500 records, min timestamp 1001');
    assert(w.verify() === true, 'checksum valid after compaction');
    r.refresh();
    assert(count(r) === 500, 'reader follows compaction');
    w.retainLast(100);
    assert(count(w) === 100, 'retainLast(100): 100 records');
    assert(count(r) === 100 && r.getStats(INT64_MIN, INT64_MAX, 'timestamp').min === 1401, 'reader follows retainLast without an explicit refresh');

    console.log('\n[5] rollover');
    const archive = w.rollover();
    assert(typeof archive === 'string' && archive.endsWith('.bin') && archive.includes(`${TICKER}.`), `rollover() -> ${archive}`);
    assert(path.basename(archive) === `${TICKER}.1401-1500.bin`, 'archive is named <ticker>.<first_ts>-<last_ts>.bin');
    assert(count(w) === 0, 'writer is empty after rollover');
    appendN(w, 1501, 10);
    w.flush();
    assert(count(w) === 10, 'appending after rollover works (timestamps stay monotonic)');
    assert(count(r) === 10, 'reader follows rollover');
    m = w.metrics();
    assert(m.rollovers === 1n && m.compactions === 2n, 'metrics: rollovers 1, compactions 2');
    const a = hocdb.dbInit(path.basename(archive, '.bin'), DIR, schema);
    assert(count(a) === 100 && a.getStats(INT64_MIN, INT64_MAX, 'timestamp').max === 1500, 'archive opens as a normal database with 100 records');
    a.close();
    const e = hocdb.dbInit('EMPTY', DIR, schema);
    const empty = catchError(() => e.rollover());
    assert(empty && empty.code === 'EmptyDatabase', 'rollover() of an empty database -> EmptyDatabase');
    e.close();
    r.close();
    w.close();
}

function testCrashRecovery() {
    console.log('\n[6] crash recovery');
    const file = path.join(DIR, `${TICKER}.bin`);
    const tail = Buffer.alloc(2 * REC + 5);
    tail.writeBigInt64LE(1511n, 0); tail.writeDoubleLE(1, 8);
    tail.writeBigInt64LE(1512n, 16); tail.writeDoubleLE(2, 24);
    tail.set([1, 2, 3, 4, 5], 32); // torn record
    fs.appendFileSync(file, tail);
    const w = hocdb.dbInit(TICKER, DIR, schema, { fsync: 'on_flush' });
    assert(count(w) === 12, 'uncommitted complete records are adopted (10 + 2)');
    const m = w.metrics();
    assert(m.recovered_tail_records === 2n && m.dropped_tail_bytes === 5n, 'metrics: recovered_tail_records 2, dropped_tail_bytes 5');
    w.flush();
    assert(w.verify() === true, 'verify() true after a flush');
    assert(fs.statSync(file).size === 64 + 12 * REC, 'torn bytes were truncated');
    w.close();

    console.log('\n[6b] verify_on_open');
    const fd = fs.openSync(file, 'r+');
    fs.writeSync(fd, Buffer.from([0xff]), 0, 1, 64 + 3 * REC + 8);
    fs.closeSync(fd);
    const bad = catchError(() => hocdb.dbInit(TICKER, DIR, schema, { verifyOnOpen: true }));
    assert(bad && bad.code === 'ChecksumMismatch' && bad.message.includes('ChecksumMismatch'), `verify_on_open on a corrupted file -> ${bad && bad.message}`);
    const w2 = hocdb.dbInit(TICKER, DIR, schema);
    assert(count(w2) === 12, 'without verify_on_open the data stays readable');
    assert(w2.verify() === false, 'verify() returns false for the corrupted data');
    assert(w2.metrics().crc_failures === 2n, 'crc_failures counted at open and by verify()');
    w2.close();
}

function testHeaderAndRing() {
    console.log('\n[7] header size and ring-buffer capacity');
    assert(hocdb.headerSize() === 64, 'headerSize() == 64');
    const ring = hocdb.dbInit('RING', DIR, schema, { max_file_size: hocdb.headerSize() + 50 * REC, overwrite_on_full: true });
    appendN(ring, 1, 80);
    ring.flush();
    assert(count(ring) === 50, 'ring buffer sized 64 + 50 * 16 holds exactly 50 records after 80 appends');
    const rows = ring.load();
    assert(rows[0].timestamp === 31n && rows[49].timestamp === 80n, 'the oldest records were overwritten');
    const e = catchError(() => ring.verify());
    assert(e && e.code === 'ChecksumUnavailable', 'ring buffer: verify() throws ChecksumUnavailable');
    ring.close();
    const tiny = catchError(() => hocdb.dbInit('TINY', DIR, schema, { max_file_size: 64 }));
    assert(tiny && tiny.code === 'MaxFileSizeTooSmall', 'max_file_size below header + one record -> MaxFileSizeTooSmall');
}

function testConfigAndPolicies() {
    console.log('\n[8] config parsing and automatic policies');
    let w = hocdb.dbInit('CFG', DIR, schema, { fsync: hocdb.FSYNC.on_flush, fsync_interval_ms: 50, autoMigrate: true, indexStride: 16, verify_on_open: false });
    appendN(w, 1, 5);
    w.flush();
    assert(w.metrics().fsyncs >= 1n, 'fsync: 2 (number) == on_flush');
    w.close();
    for (const cfg of [{ fsync: 'sometimes' }, { fsync: 7 }, { retention_span: -1 }, { rollover_size: 'big' }]) {
        const e = catchError(() => hocdb.dbInit('CFG', DIR, schema, cfg));
        assert(e && /Invalid config\./.test(e.message), `${JSON.stringify(cfg)} -> ${e && e.message}`);
    }
    w = hocdb.dbInit('CFG', DIR, schema, { fsync: 'interval', fsyncIntervalMs: 1 });
    appendN(w, 6, 5);
    w.flush();
    assert(count(w) === 10, 'reopen with fsync: interval');
    w.close();

    // rollover_size: archives automatically once the file exceeds the size
    w = hocdb.dbInit('AUTO', DIR, schema, { rolloverSize: 64 + 20 * REC });
    appendN(w, 1, 100);
    w.flush();
    assert(w.metrics().rollovers >= 1n, `rollover_size: rollovers ${w.metrics().rollovers}`);
    assert(fs.readdirSync(DIR).some(f => /^AUTO\.\d+-\d+\.bin$/.test(f)), 'archives named AUTO.<first>-<last>.bin exist');
    w.close();

    // retention_span: compacts away records older than last - span once the excess is > 25 %
    w = hocdb.dbInit('RET', DIR, schema, { retentionSpan: 100 });
    appendN(w, 1, 1000);
    w.flush();
    const n = count(w);
    const min = w.getStats(INT64_MIN, INT64_MAX, 'timestamp').min;
    assert(n < 1000 && min > 1 && w.metrics().compactions >= 1n, `retention_span: ${n} records left, min timestamp ${min}`);
    w.close();
}

async function testAsync() {
    console.log('\n[9] async API (worker thread)');
    const w = await hocdb.dbInitAsync('A', DIR, schema, { fsync: 'on_flush', timestampUnitNs: 1e9 });
    await w.appendBatch(Array.from({ length: 10 }, (_, i) => ({ timestamp: BigInt(i + 1), value: i + 1 })));
    await w.flush();
    assert((await w.verify()) === true, 'async verify() true');
    const m = await w.metrics();
    assert(m.appends === 10n && m.format_version === 2n && m.read_only === 0n, 'async metrics (BigInt fields)');
    assert((await w.formatVersion()) === 2 && (await w.isReadOnly()) === false, 'async formatVersion() / isReadOnly()');
    const locked = await catchAsync(hocdb.dbInitAsync('A', DIR, schema));
    assert(locked && locked.code === 'DatabaseLocked' && locked.message.includes('DatabaseLocked'), `async second writer -> ${locked && locked.message}`);
    const r = await hocdb.openReaderAsync('A', DIR, schema);
    assert((await r.isReadOnly()) === true, 'openReaderAsync: isReadOnly() true');
    assert(Number((await r.getStats(INT64_MIN, INT64_MAX, 'value')).count) === 10, 'async reader sees 10');
    const ro = await catchAsync(r.append({ timestamp: 11n, value: 11 }));
    assert(ro && ro.code === 'ReadOnly' && /reader/.test(ro.message), `async reader append -> ${ro && ro.message}`);
    assert((await r.metrics()).read_only === 1n, 'async reader metrics.read_only 1');
    await w.compact(6n);
    await r.refresh();
    assert(Number((await r.getStats(INT64_MIN, INT64_MAX, 'value')).count) === 5, 'async compact + reader refresh -> 5');
    await w.retainLast(2);
    assert((await r.getLatest('value')).timestamp === 10n && Number((await r.getStats(INT64_MIN, INT64_MAX, 'value')).count) === 2, 'async retainLast -> 2');
    const archive = await w.rollover();
    assert(archive.endsWith('.bin') && path.basename(archive) === 'A.9-10.bin', `async rollover -> ${archive}`);
    await w.sync();
    await w.metricsReset();
    assert((await w.metrics()).appends === 0n, 'async sync() + metricsReset()');
    const r2 = await w.openReaderAsync('A', DIR, schema); // reader on the writer's worker
    assert((await r2.isReadOnly()) === true && (await r2.formatVersion()) === 2, 'adb.openReaderAsync on the same worker');
    await r2.close();
    await r.close();
    await w.close();
}

async function main() {
    rmrf();
    try {
        testWriterReaderMaintenance();
        testCrashRecovery();
        testHeaderAndRing();
        testConfigAndPolicies();
        await testAsync();
        console.log('\nAll Node.js storage tests PASSED');
    } catch (e) {
        console.error('\nTest failed:', e);
        process.exitCode = 1;
    } finally {
        rmrf();
    }
}

main();
