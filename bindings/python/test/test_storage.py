"""
Python binding test for durability, lock-free readers, maintenance and metrics
(mirrors bindings/c/test/test_storage.c).
Run from the repo root:
    PYTHONPATH=$(pwd)/bindings/python python3 bindings/python/test/test_storage.py
"""
import ctypes
import os
import shutil
import struct

from hocdb_python import (HOCDB, HOCDBField, FieldTypes, FsyncPolicy, HOCDBConfig, HOCDBMetrics,
                          header_size, last_error)

TICKER = "T"
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "b_python_test_storage"))
INT64_MIN, INT64_MAX = -(1 << 63), (1 << 63) - 1
RECORD_SIZE = 16  # i64 timestamp + f64 value

schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("value", FieldTypes.F64),
]


def check(cond, msg):
    if not cond:
        raise RuntimeError(f"FAIL: {msg}")


def expect_error(fn, msg, needle, exc=RuntimeError):
    try:
        fn()
    except exc as e:
        check(needle.lower() in str(e).lower(), f"{msg}: message {str(e)!r} does not mention {needle!r}")
        print(f"  ok: {msg} -> {type(e).__name__}: {e}")
        return
    raise RuntimeError(f"FAIL: {msg} (no {exc.__name__} raised)")


def append_n(db, start, n):
    for ts in range(start, start + n):
        check(db.append(ts, float(ts)), f"append {ts}")


def count_of(db):
    return db.get_stats(INT64_MIN, INT64_MAX, "value")["count"]


def min_ts_of(db):
    return db.get_stats(INT64_MIN, INT64_MAX, "timestamp")["min"]


def run():
    print("Struct layouts and header size...")
    check(ctypes.sizeof(HOCDBConfig) == 80, "HOCDBConfig is 80 bytes")
    check(HOCDBConfig.auto_migrate.offset == 48 and HOCDBConfig.timestamp_unit_ns.offset == 56, "HOCDBConfig offsets")
    check(ctypes.sizeof(HOCDBMetrics) == 240, "HOCDBMetrics is 240 bytes")
    check(HOCDB.header_size() == 64, "HOCDB.header_size() == 64")
    check(header_size() == 64, "header_size() == 64")

    # 1. writer with fsync on_flush and a timestamp unit of one second
    print("Writer: append 1000, flush, verify, metrics...")
    w = HOCDB(TICKER, DATA_DIR, schema, fsync="on_flush", timestamp_unit_ns=1_000_000_000)
    check(w.config.fsync_policy == FsyncPolicy.ON_FLUSH and w.config.auto_migrate == 1, "config fields")
    check(w.format_version() == 2, "writer format version 2")
    check(w.is_read_only() is False and w.read_only is False, "writer is not read-only")
    append_n(w, 1, 1000)
    check(w.flush(), "flush")
    check(w.verify() is True, "verify() True after flush")
    m = w.metrics()
    check(len(m) == 30, f"metrics has 30 fields, got {len(m)}")
    check(m["appends"] == 1000, f"metrics.appends == 1000, got {m['appends']}")
    check(m["flushes"] >= 1 and m["fsyncs"] >= 1 and m["commits"] >= 1, "flushes / fsyncs / commits >= 1")
    check(m["bytes_written"] == 1000 * RECORD_SIZE, "metrics.bytes_written")
    check(m["committed_records"] == 1000, "metrics.committed_records == 1000")
    check(m["read_only"] == 0 and m["format_version"] == 2, "metrics.read_only / format_version")
    check(m["last_record_ts"] == 1000, "metrics.last_record_ts == 1000")
    check(m["ingest_lag_wall_ns"] >= 0 and m["ingest_lag_record_ns"] != 0, "lag fields")
    check(all(isinstance(v, int) for v in m.values()), "every metrics value is an int")
    check(w.refresh() is True, "refresh() is a no-op for writers")

    # 2. a second writer is refused
    print("Second writer...")
    expect_error(lambda: HOCDB(TICKER, DATA_DIR, schema, fsync=FsyncPolicy.ON_FLUSH),
                 "second writer refused", "DatabaseLocked")
    check(last_error() == "DatabaseLocked", f"last_error() == DatabaseLocked, got {last_error()!r}")

    # 3. reader in the same process
    print("Reader...")
    r = HOCDB.open_reader(TICKER, DATA_DIR, schema)
    check(r.read_only is True and r.is_read_only() is True, "reader is read-only")
    check(r.format_version() == 2, "reader format version")
    check(count_of(r) == 1000, "reader sees 1000")
    check(len(r.load()) == 1000 and len(r.query(1, 1001)) == 1000, "reader load / query")
    append_n(w, 1001, 500)
    r.refresh()
    check(count_of(r) == 1000, "uncommitted appends are invisible to the reader")
    check(w.flush(), "flush 2")
    check(r.refresh() is True, "refresh")
    check(count_of(r) == 1500, "reader sees 1500 after the writer's flush")
    check(r.get_latest("value")["timestamp"] == 1500, "reader get_latest auto-refreshes")
    expect_error(lambda: r.append(9999, 0.0), "reader append", "read-only")
    expect_error(lambda: r.sync(), "reader sync", "read-only")
    expect_error(lambda: r.compact(0), "reader compact", "read-only")
    expect_error(lambda: r.retain_last(1), "reader retain_last", "read-only")
    expect_error(lambda: r.rollover(), "reader rollover", "read-only")
    expect_error(lambda: r.drop(), "reader drop", "read-only")
    check(r.handle, "reader handle survives the refused drop")
    check(r.flush() is True, "flush() on a reader is a refresh")
    rm = r.metrics()
    check(rm["read_only"] == 1, "reader metrics.read_only == 1")
    check(rm["refreshes"] >= 1, "reader metrics.refreshes >= 1")
    check(rm["reads"] >= 3 and rm["committed_records"] == 1500, "reader metrics.reads / committed_records")
    check(r.verify() is True, "reader verify()")
    ind = r.indicators([{"kind": "sma", "period": 10, "field": "value"}], tail=5, columns={"close": "value"})
    check(ind["n_rows"] == 5 and abs(ind["columns"]["sma_10"][-1] - 1495.5) < 1e-9, "reader indicators")
    check(r.summary(1, 1501, "value")["count"] == 1500, "reader summary")

    print("metrics_reset keeps the state fields...")
    w.metrics_reset()
    m = w.metrics()
    check(m["appends"] == 0 and m["last_record_ts"] == 1500 and m["committed_records"] == 1500, "metrics after reset")

    # 4. compaction and retention
    print("compact / retain_last...")
    check(w.compact(1001) is True, "compact")
    check(count_of(w) == 500 and min_ts_of(w) == 1001.0, "writer compacted to 500 from 1001")
    check(w.verify() is True, "checksum after compaction")
    r.refresh()
    check(count_of(r) == 500, "reader follows compaction")
    check(w.retain_last(100) is True, "retain_last")
    check(count_of(w) == 100 and min_ts_of(w) == 1401.0, "writer keeps the last 100")
    check(count_of(r) == 100, "reader follows retain_last")
    m = w.metrics()
    check(m["compactions"] == 2, f"metrics.compactions == 2, got {m['compactions']}")

    # 5. rollover
    print("rollover...")
    archive = w.rollover()
    print(f"  archive: {archive}")
    check(isinstance(archive, str) and archive.endswith(".bin") and f"{TICKER}." in os.path.basename(archive), "archive path")
    check(os.path.basename(archive) == f"{TICKER}.1401-1500.bin", f"archive name, got {os.path.basename(archive)}")
    check(os.path.isfile(archive), "archive file exists")
    check(os.path.dirname(os.path.abspath(archive)) == DATA_DIR, "archive is in the data directory")
    check(count_of(w) == 0, "writer is empty after rollover")
    append_n(w, 1501, 10)
    check(w.flush(), "flush after rollover")
    check(count_of(w) == 10, "10 records after rollover")
    check(count_of(r) == 10, "reader follows rollover")
    check(w.metrics()["rollovers"] == 1, "metrics.rollovers == 1")
    archive_ticker = os.path.basename(archive)[:-len(".bin")]
    a = HOCDB(archive_ticker, DATA_DIR, schema)
    check(count_of(a) == 100 and min_ts_of(a) == 1401.0, "archive opens as a normal database with 100 records")
    a.close()

    w.sync()
    check(w.metrics()["fsyncs"] >= 1, "sync() fsyncs")
    r.close()
    w.close()
    check(w.handle is None and r.handle is None, "closed")

    # 6. crash simulation: an uncommitted valid tail is adopted, torn bytes are dropped
    print("Crash recovery...")
    raw = os.path.join(DATA_DIR, f"{TICKER}.bin")
    with open(raw, "r+b") as f:
        f.seek(0, os.SEEK_END)
        f.write(struct.pack("<qd", 1511, 1.0))
        f.write(struct.pack("<qd", 1512, 2.0))
        f.write(bytes([1, 2, 3, 4, 5]))
    w3 = HOCDB(TICKER, DATA_DIR, schema, fsync="on_flush", timestamp_unit_ns=1_000_000_000)
    check(count_of(w3) == 12, f"tail adopted: 12 records, got {count_of(w3)}")
    m = w3.metrics()
    check(m["recovered_tail_records"] == 2, f"metrics.recovered_tail_records == 2, got {m['recovered_tail_records']}")
    check(m["dropped_tail_bytes"] == 5, f"metrics.dropped_tail_bytes == 5, got {m['dropped_tail_bytes']}")
    check(w3.get_latest("value")["timestamp"] == 1512, "latest record after recovery")
    append_n(w3, 1513, 1)
    check(w3.flush(), "flush after recovery")
    check(w3.verify() is True, "verify() True after a flush")
    w3.close()

    # verify_on_open refuses a corrupted file; without it verify() reports the mismatch and the data stays readable
    print("verify_on_open...")
    with open(raw, "r+b") as f:
        f.seek(64 + 3 * RECORD_SIZE + 8)
        f.write(b"\xff")
    expect_error(lambda: HOCDB(TICKER, DATA_DIR, schema, verify_on_open=True),
                 "verify_on_open on a corrupted file", "ChecksumMismatch")
    w5 = HOCDB(TICKER, DATA_DIR, schema)
    check(count_of(w5) == 13, "corrupted file still readable")
    check(w5.verify() is False, "verify() False on a file with a corrupted byte")
    m = w5.metrics()
    check(m["crc_failures"] == 2 and m["committed_records"] > 0, f"crc_failures counted at open and by verify, got {m['crc_failures']}")
    w5.close()

    # 7. ring buffer capacity: header + 50 records holds exactly 50 records after 80 appends
    print("Ring buffer capacity...")
    ring = HOCDB("RING", DATA_DIR, schema, max_file_size=HOCDB.header_size() + 50 * RECORD_SIZE,
                 overwrite_on_full=True, fsync="none")
    append_n(ring, 1, 80)
    ring.flush()
    check(count_of(ring) == 50, f"ring buffer holds exactly 50 records, got {count_of(ring)}")
    check(ring.get_latest("value")["timestamp"] == 80, "ring buffer keeps the newest records")
    expect_error(lambda: ring.verify(), "verify() on a ring buffer", "checksum unavailable")
    ring.drop()
    check(not os.path.exists(os.path.join(DATA_DIR, "RING.bin")), "drop deletes the ring buffer file")

    # option validation and the other policies
    print("Options...")
    expect_error(lambda: HOCDB("OPT", DATA_DIR, schema, fsync="sometimes"), "unknown fsync name", "fsync", ValueError)
    expect_error(lambda: HOCDB("OPT", DATA_DIR, schema, fsync=7), "fsync out of range", "fsync", ValueError)
    expect_error(lambda: HOCDB("OPT", DATA_DIR, schema, retention_span=-1), "negative retention_span", "retention_span", ValueError)
    opt = HOCDB("OPT", DATA_DIR, schema, fsync="interval", fsync_interval_ms=50, retention_span=1000,
                rollover_size=1 << 20, auto_migrate=False, index_stride=256)
    c = opt.config
    check(c.fsync_policy == 3 and c.fsync_interval_ms == 50 and c.retention_span == 1000 and c.rollover_size == 1 << 20
          and c.auto_migrate == 0 and c.index_stride == 256, "config fields are passed through")
    append_n(opt, 1, 5)
    opt.flush()
    check(opt.verify() is True, "interval policy writer verifies")
    opt.close()
    check(HOCDB("OPT", DATA_DIR, schema, fsync=FsyncPolicy.NONE).format_version() == 2, "fsync as int")
    # legacy constructor arguments still work
    legacy = HOCDB("LEGACY_ARGS", DATA_DIR, schema, 1024 * 1024, True, False, False)
    check(legacy.config.overwrite_on_full == 1 and legacy.config.max_file_size == 1024 * 1024, "positional legacy args")
    legacy.close()

    print("Python Storage Test Passed!")


if __name__ == "__main__":
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    try:
        run()
    finally:
        shutil.rmtree(DATA_DIR, ignore_errors=True)
