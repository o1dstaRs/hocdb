// Test for durability, lock-free readers, maintenance and metrics (round 3) of the Bun binding.
// Mirrors bindings/c/test/test_storage.c.
import { HOCDB, FieldDef, MetricsResult } from "../index";
import { join, basename } from "path";
import { rmSync, existsSync, appendFileSync, openSync, writeSync, closeSync, statSync } from "node:fs";

const TICKER = "TEST_BUN_STORAGE";
const DATA_DIR = join(import.meta.dir, "..", "..", "..", "b_bun_test_storage");

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const RECORD_SIZE = 16; // i64 timestamp + f64 value

function check(cond: boolean, msg: string) {
    if (!cond) throw new Error(`FAIL: ${msg}`);
}

function expectThrows(fn: () => unknown, what: string): string {
    try {
        fn();
    } catch (e: any) {
        console.log(`  ${what} -> threw: ${e.message}`);
        return String(e.message);
    }
    throw new Error(`FAIL: ${what} did not throw`);
}

function cleanup() {
    if (existsSync(DATA_DIR)) {
        rmSync(DATA_DIR, { recursive: true, force: true });
    }
}

const schema: FieldDef[] = [
    { name: "timestamp", type: "i64" },
    { name: "value", type: "f64" },
];

function appendN(db: HOCDB, from: number, n: number) {
    for (let i = 0; i < n; i++) db.append({ timestamp: BigInt(from + i), value: from + i });
}

// count / min of the value field (== timestamp) over everything
function count(db: HOCDB): number {
    return Number(db.getStats(INT64_MIN, INT64_MAX, "value").count);
}
function minValue(db: HOCDB): number {
    return db.getStats(INT64_MIN, INT64_MAX, "value").min;
}

const METRIC_NAMES = [
    "appends", "bytes_written", "flushes", "commits", "fsyncs", "fsync_ns_total", "fsync_ns_max",
    "reads", "read_ns_total", "read_ns_max", "read_ns_last", "read_ns_p50", "read_ns_p99", "records_read",
    "refreshes", "recovered_tail_records", "dropped_tail_bytes", "crc_failures", "compactions", "rollovers", "migrations",
    "last_append_wall_ns", "last_commit_wall_ns", "last_record_ts", "ingest_lag_wall_ns", "ingest_lag_record_ns",
    "committed_records", "file_size", "format_version", "read_only",
];

function checkMetricsShape(m: MetricsResult) {
    check(Object.keys(m).length === 30, `30 metrics fields (got ${Object.keys(m).length})`);
    for (const name of METRIC_NAMES) check(name in m, `metrics has '${name}'`);
    // uint64 counters are numbers (like summary().count), int64 timestamps / lags are bigints
    for (const name of ["appends", "committed_records", "file_size", "format_version", "read_only", "fsync_ns_max"]) {
        check(typeof m[name] === "number", `metrics.${name} is a number`);
    }
    for (const name of ["last_append_wall_ns", "last_commit_wall_ns", "last_record_ts", "ingest_lag_wall_ns", "ingest_lag_record_ns"]) {
        check(typeof m[name] === "bigint", `metrics.${name} is a bigint`);
    }
}

cleanup();
const open = new Set<HOCDB>();
function track(db: HOCDB): HOCDB { open.add(db); return db; }
function closeDb(db: HOCDB) { db.close(); open.delete(db); }

try {
    // ---- 7a. header size ------------------------------------------------
    console.log("Checking headerSize...");
    check(HOCDB.headerSize() === 64, `headerSize() == 64 (got ${HOCDB.headerSize()})`);

    // ---- 1. writer: append, flush, verify, metrics ----------------------
    console.log("Opening writer (fsync on_flush, timestamp unit = 1 s)...");
    const w = track(new HOCDB(TICKER, DATA_DIR, schema, { fsync: "on_flush", timestampUnitNs: 1e9 }));
    check(w.isReadOnly() === false && w.readOnly === false, "writer is not read-only");
    check(w.formatVersion() === 2, `writer format version 2 (got ${w.formatVersion()})`);
    appendN(w, 1, 1000);
    w.flush();
    check(w.verify() === true, "verify() true after flush");
    check(count(w) === 1000, "writer count 1000");
    w.refresh(); // no-op for writers

    let m = w.metrics();
    checkMetricsShape(m);
    console.log(`  writer metrics: appends=${m.appends} flushes=${m.flushes} commits=${m.commits} fsyncs=${m.fsyncs} committed=${m.committed_records} last_ts=${m.last_record_ts} file_size=${m.file_size}`);
    check(m.appends === 1000, `metrics.appends == 1000 (got ${m.appends})`);
    check(m.bytes_written === 1000 * RECORD_SIZE, `metrics.bytes_written == ${1000 * RECORD_SIZE} (got ${m.bytes_written})`);
    check(m.flushes >= 1, "metrics.flushes >= 1");
    check(m.commits >= 1, "metrics.commits >= 1");
    check(m.fsyncs >= 1, "metrics.fsyncs >= 1 (fsync on_flush)");
    check(m.committed_records === 1000, `metrics.committed_records == 1000 (got ${m.committed_records})`);
    check(m.read_only === 0, "metrics.read_only == 0");
    check(m.format_version === 2, "metrics.format_version == 2");
    check(m.last_record_ts === 1000n, `metrics.last_record_ts == 1000n (got ${m.last_record_ts})`);
    check(m.ingest_lag_wall_ns >= 0n, "metrics.ingest_lag_wall_ns >= 0");
    check(m.ingest_lag_record_ns !== 0n, "metrics.ingest_lag_record_ns set (timestamp unit known)");
    check(m.file_size === 64 + 1000 * RECORD_SIZE, `metrics.file_size == header + data (got ${m.file_size})`);
    check(statSync(join(DATA_DIR, `${TICKER}.bin`)).size === 64 + 1000 * RECORD_SIZE, "file on disk = 64-byte header + 1000 records");

    // ---- 2. second writer refused ---------------------------------------
    console.log("Opening a second writer on the same database...");
    const lockMsg = expectThrows(() => new HOCDB(TICKER, DATA_DIR, schema, { fsync: "on_flush" }), "second writer");
    check(lockMsg.includes("DatabaseLocked"), `second writer error mentions DatabaseLocked: ${lockMsg}`);

    // ---- 3. lock-free reader --------------------------------------------
    console.log("Opening reader...");
    const r = track(HOCDB.openReader(TICKER, DATA_DIR, schema));
    check(r.isReadOnly() === true && r.readOnly === true, "reader.isReadOnly()");
    check(r.formatVersion() === 2, "reader format version 2");
    check(count(r) === 1000, `reader sees 1000 (got ${count(r)})`);

    appendN(w, 1001, 500); // not flushed: invisible to the reader
    r.refresh();
    check(count(r) === 1000, `uncommitted records invisible after refresh (got ${count(r)})`);
    w.flush();
    r.refresh();
    check(count(r) === 1500, `reader sees 1500 after writer flush + refresh (got ${count(r)})`);
    check(r.getLatest("value").timestamp === 1500n, "reader getLatest timestamp == 1500 (auto refresh)");
    check(r.load().length === 1500, "reader load() returns 1500 records");
    check(r.query(1n, 11n).length === 10, "reader query works");
    check(r.getStats(1n, 1501n, "value").max === 1500, "reader getStats works");
    check(r.summary(1n, 1501n, "value").count === 1500, "reader summary works");
    r.flush(); // readers: same as refresh(), never an error

    const roAppend = expectThrows(() => r.append({ timestamp: 9999n, value: 0 }), "reader append");
    check(roAppend.includes("reader") && roAppend.includes("read-only"), "reader append error says the handle is a reader");
    for (const [what, fn] of [
        ["sync", () => r.sync()],
        ["compact", () => r.compact(0)],
        ["retainLast", () => r.retainLast(1)],
        ["rollover", () => r.rollover()],
        ["drop", () => r.drop()],
    ] as [string, () => unknown][]) {
        const msg = expectThrows(fn, `reader ${what}`);
        check(msg.includes("reader"), `reader ${what} error says the handle is a reader`);
    }
    check(r.db !== null, "reader still open after refused operations");

    const rm = r.metrics();
    checkMetricsShape(rm);
    console.log(`  reader metrics: reads=${rm.reads} refreshes=${rm.refreshes} read_only=${rm.read_only} read_ns_p50=${rm.read_ns_p50}`);
    check(rm.read_only === 1, "reader metrics.read_only == 1");
    check(rm.refreshes >= 1, "reader metrics.refreshes >= 1");
    check(rm.reads >= 3, "reader metrics.reads >= 3");
    check(rm.committed_records === 1500, "reader metrics.committed_records == 1500");

    m = w.metrics();
    check(m.appends === 1500 && m.flushes >= 2 && m.fsyncs >= 2, "writer counters after second flush");
    w.metricsReset();
    m = w.metrics();
    check(m.appends === 0 && m.flushes === 0, "metricsReset() clears counters");
    check(m.last_record_ts === 1500n && m.committed_records === 1500, "metricsReset() keeps state fields");

    // ---- 4. compaction / retention: readers follow ----------------------
    console.log("Compacting (min_ts = 1001)...");
    w.compact(1001);
    check(count(w) === 500, `writer count 500 after compact (got ${count(w)})`);
    check(minValue(w) === 1001, `min timestamp 1001 after compact (got ${minValue(w)})`);
    check(w.verify() === true, "verify() true after compaction");
    r.refresh();
    check(count(r) === 500, `reader follows compaction (got ${count(r)})`);
    console.log("retainLast(100)...");
    w.retainLast(100);
    check(count(w) === 100 && minValue(w) === 1401, "writer holds the last 100 (1401..1500)");
    check(count(r) === 100, `reader follows retainLast without explicit refresh (got ${count(r)})`);

    // ---- 5. rollover ----------------------------------------------------
    console.log("Rolling over...");
    const archive = w.rollover();
    console.log(`  archive: ${archive}`);
    check(archive.endsWith(".bin") && archive.includes(`${TICKER}.`), "archive path looks like <ticker>.<first>-<last>.bin");
    check(basename(archive) === `${TICKER}.1401-1500.bin`, `archive name (got ${basename(archive)})`);
    check(existsSync(archive), "archive file exists");
    check(count(w) === 0, "writer empty after rollover");
    appendN(w, 1501, 10);
    w.flush();
    check(count(w) === 10 && minValue(w) === 1501, "appends continue after rollover");
    check(count(r) === 10, `reader follows rollover (got ${count(r)})`);
    m = w.metrics();
    check(m.rollovers === 1 && m.compactions === 2, `maintenance counters: rollovers=${m.rollovers} compactions=${m.compactions}`);

    const a = track(new HOCDB(basename(archive, ".bin"), DATA_DIR, schema));
    check(count(a) === 100 && minValue(a) === 1401, `archive opens as a normal database with 100 records (got ${count(a)})`);
    check(a.getLatest("value").timestamp === 1500n, "archive latest timestamp 1500");
    closeDb(a);

    closeDb(r);
    closeDb(w);

    // ---- 6. crash simulation --------------------------------------------
    console.log("Simulating a crash: 2 valid uncommitted records + 5 torn bytes...");
    const file = join(DATA_DIR, `${TICKER}.bin`);
    const tail = new Uint8Array(2 * RECORD_SIZE + 5);
    const tv = new DataView(tail.buffer);
    tv.setBigInt64(0, 1511n, true); tv.setFloat64(8, 1.0, true);
    tv.setBigInt64(16, 1512n, true); tv.setFloat64(24, 2.0, true);
    tail.set([1, 2, 3, 4, 5], 2 * RECORD_SIZE);
    appendFileSync(file, tail);

    const w3 = track(new HOCDB(TICKER, DATA_DIR, schema, { fsync: "on_flush", timestampUnitNs: 1e9 }));
    check(count(w3) === 12, `tail adopted: 12 records (got ${count(w3)})`);
    check(w3.getLatest("value").timestamp === 1512n, "recovered tail latest timestamp 1512");
    m = w3.metrics();
    check(m.recovered_tail_records === 2, `metrics.recovered_tail_records == 2 (got ${m.recovered_tail_records})`);
    check(m.dropped_tail_bytes === 5, `metrics.dropped_tail_bytes == 5 (got ${m.dropped_tail_bytes})`);
    w3.flush();
    check(w3.verify() === true, "verify() true after the recovered tail is committed");
    closeDb(w3);

    // ---- 6b. verify_on_open refuses a corrupted file (as in the C test) --
    console.log("Corrupting one byte, opening with verifyOnOpen...");
    {
        const fd = openSync(file, "r+");
        writeSync(fd, new Uint8Array([0xFF]), 0, 1, 64 + 3 * RECORD_SIZE + 8);
        closeSync(fd);
    }
    const crcMsg = expectThrows(() => new HOCDB(TICKER, DATA_DIR, schema, { verifyOnOpen: true }), "verifyOnOpen on a corrupted file");
    check(crcMsg.includes("ChecksumMismatch"), `open error mentions ChecksumMismatch: ${crcMsg}`);
    const w5 = track(new HOCDB(TICKER, DATA_DIR, schema));
    let verdict: boolean | string;
    try { verdict = w5.verify(); } catch (e: any) { verdict = String(e.message); }
    console.log(`  verify() on the corrupted file -> ${verdict}`);
    check(verdict !== true, "corrupted data is not reported as verified");
    if (verdict === false) check(w5.metrics().crc_failures >= 1, "crc_failures counted when verify() reports a mismatch");
    check(count(w5) === 12, "corrupted file is still readable");
    closeDb(w5);

    // ---- 7. ring buffer capacity with the 64-byte header -----------------
    console.log("Ring buffer: 64 + 50 * record_size, 80 appends...");
    const ring = track(new HOCDB("RING", DATA_DIR, schema, { max_file_size: HOCDB.headerSize() + 50 * RECORD_SIZE, overwrite_on_full: true }));
    appendN(ring, 1, 80);
    ring.flush();
    check(count(ring) === 50, `ring buffer holds exactly 50 records (got ${count(ring)})`);
    const rows = ring.load();
    check(rows.length === 50, `ring load() returns 50 rows (got ${rows.length})`);
    const ts = rows.map((x) => Number(x.timestamp)).sort((x, y) => x - y);
    check(ts[0] === 31 && ts[49] === 80, `ring keeps the last 50 records 31..80 (got ${ts[0]}..${ts[49]})`);
    expectThrows(() => ring.verify(), "verify() on a ring buffer (checksum unavailable)");
    closeDb(ring);

    // ---- misc: option handling and error names --------------------------
    console.log("Option handling...");
    const missing = expectThrows(() => HOCDB.openReader("NOPE", DATA_DIR, schema), "reader on a missing database");
    check(/Failed to open HOCDB 'NOPE'.*as reader: \w+/.test(missing), "missing-database error carries the error name");
    expectThrows(() => new HOCDB("OPTS", DATA_DIR, schema, { fsync: "sometimes" as any }), "invalid fsync policy");
    expectThrows(() => new HOCDB("OPTS", DATA_DIR, schema, { fsync_interval_ms: -1 }), "negative fsync_interval_ms");
    const x = track(new HOCDB("OPTS", DATA_DIR, schema, { fsync: 3, fsyncIntervalMs: 50, auto_migrate: true, retention_span: 0, rolloverSize: 0, indexStride: 256 }));
    appendN(x, 1, 3);
    x.sync();
    check(x.metrics().fsyncs >= 1 && x.metrics().committed_records === 3, "sync() flushes and fsyncs");
    closeDb(x);
    const y = track(new HOCDB("OPTS", DATA_DIR, schema, { read_only: true }));
    check(y.isReadOnly() && count(y) === 3, "readOnly: true config opens a reader");
    closeDb(y);

    console.log("\nBun storage API test passed");
} catch (e: any) {
    console.error(e.message);
    for (const db of open) { try { db.close(); } catch { /* ignore */ } }
    cleanup();
    process.exit(1);
}
for (const db of open) { try { db.close(); } catch { /* ignore */ } }
cleanup();
