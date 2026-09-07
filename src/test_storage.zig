//! Storage-layer tests: versioned header, crash recovery, lock-free readers,
//! checksums, fsync policies, compaction / retention / rollover, legacy
//! migration and metrics.
const std = @import("std");
const hocdb = @import("root.zig");
const DB = hocdb.DynamicTimeSeriesDB;

const Rec = extern struct { timestamp: i64, value: f64 };
const schema = hocdb.Schema{ .fields = &[_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "value", .type = .f64 },
} };
const RS: u64 = 16;

fn tmpDir(buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "test_storage_{x}", .{std.crypto.random.int(u64)});
}

// The writer keeps pointers into its own struct, so databases live on the heap.
fn open(dir: []const u8, config: DB.Config) !*DB {
    const db = try std.testing.allocator.create(DB);
    errdefer std.testing.allocator.destroy(db);
    db.* = try DB.init("T", dir, std.testing.allocator, schema, config);
    try db.initWriter();
    return db;
}

fn openReader(dir: []const u8) !*DB {
    const db = try std.testing.allocator.create(DB);
    errdefer std.testing.allocator.destroy(db);
    db.* = try DB.openReader("T", dir, std.testing.allocator, schema);
    try db.initWriter();
    return db;
}

fn close(db: *DB) void {
    db.deinit();
    std.testing.allocator.destroy(db);
}

fn appendN(db: *DB, from: i64, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const r = Rec{ .timestamp = from + @as(i64, @intCast(i)), .value = @floatFromInt(from + @as(i64, @intCast(i))) };
        try db.append(std.mem.asBytes(&r));
    }
}

fn filePath(buf: []u8, dir: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/T.bin", .{dir});
}

test "v2 header round trip and committed cursor" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    {
        const db = try open(dir, .{});
        defer close(db);
        try appendN(db, 100, 10);
        // nothing committed before flush
        var info = try db.headerInfo();
        try std.testing.expectEqual(@as(u16, 2), info.version);
        try std.testing.expectEqual(DB.HEADER_SIZE, info.committed_cursor);
        try db.flush();
        info = try db.headerInfo();
        try std.testing.expectEqual(DB.HEADER_SIZE + 10 * RS, info.committed_cursor);
        try std.testing.expectEqual(@as(i64, 109), info.last_timestamp);
        try std.testing.expect(info.crc_valid and info.crc != 0);
        try std.testing.expect(info.last_commit_wall_ns > 0);
    }
    const db = try open(dir, .{});
    defer close(db);
    try std.testing.expectEqual(@as(u64, 10), db.count());
    try std.testing.expectEqual(@as(i64, 109), db.last_timestamp.?);
    try std.testing.expectEqual(@as(u16, 2), db.format_version);
    try std.testing.expect(try db.verify());
}

test "crash recovery: uncommitted tail is adopted when valid, torn or misordered bytes are dropped" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var path_buf: [128]u8 = undefined;
    const path = try filePath(&path_buf, dir);
    {
        const db = try open(dir, .{});
        defer close(db);
        try appendN(db, 1, 5);
        try db.flush(); // committed cursor = 5 records
    }
    // simulate a crash after the data write but before the header commit:
    // append 3 valid records, then a torn half record
    {
        const f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();
        const st = try f.stat();
        var pos = st.size;
        var i: i64 = 6;
        while (i <= 8) : (i += 1) {
            const r = Rec{ .timestamp = i, .value = 0 };
            try f.pwriteAll(std.mem.asBytes(&r), pos);
            pos += RS;
        }
        try f.pwriteAll(&[_]u8{ 1, 2, 3, 4, 5, 6, 7 }, pos);
    }
    {
        const db = try open(dir, .{});
        defer close(db);
        try std.testing.expectEqual(@as(u64, 8), db.count());
        try std.testing.expectEqual(@as(i64, 8), db.last_timestamp.?);
        const m = db.getMetrics();
        try std.testing.expectEqual(@as(u64, 3), m.recovered_tail_records);
        try std.testing.expectEqual(@as(u64, 7), m.dropped_tail_bytes);
        const st = try db.file.stat();
        try std.testing.expectEqual(DB.HEADER_SIZE + 8 * RS, st.size);
        try db.flush(); // re-commits with the adopted records and a rebuilt checksum
        try std.testing.expectEqual(DB.HEADER_SIZE + 8 * RS, (try db.headerInfo()).committed_cursor);
        try std.testing.expect(try db.verify());
        try appendN(db, 9, 2);
    }
    // misordered record beyond the commit is dropped
    {
        const f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();
        const st = try f.stat();
        const bad = Rec{ .timestamp = 3, .value = 0 };
        try f.pwriteAll(std.mem.asBytes(&bad), st.size);
    }
    const db = try open(dir, .{});
    defer close(db);
    try std.testing.expectEqual(@as(u64, 10), db.count());
    try std.testing.expectEqual(@as(u64, 16), db.getMetrics().dropped_tail_bytes);
    try std.testing.expectEqual(@as(u64, 0), db.getMetrics().recovered_tail_records);
}

test "lock-free readers follow the writer; second writer is refused" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    const w = try open(dir, .{});
    defer close(w);
    try appendN(w, 1, 100);
    try w.flush();
    // a second writer cannot open the same file
    try std.testing.expectError(error.DatabaseLocked, DB.init("T", dir, std.testing.allocator, schema, .{}));
    const r = try openReader(dir);
    defer close(r);
    try std.testing.expect(r.read_only);
    try std.testing.expectEqual(@as(u64, 100), r.count());
    const q1 = try r.query(50, 60, &[_]hocdb.Filter{}, std.testing.allocator);
    defer std.testing.allocator.free(q1);
    try std.testing.expectEqual(@as(usize, 10 * RS), q1.len);
    // the writer appends; uncommitted data is invisible, committed data appears on refresh
    try appendN(w, 101, 50);
    try r.refresh();
    try std.testing.expectEqual(@as(u64, 100), r.count());
    try w.flush();
    try std.testing.expectEqual(@as(u64, 100), r.count()); // not refreshed yet
    try r.refresh();
    try std.testing.expectEqual(@as(u64, 150), r.count());
    try std.testing.expectEqual(@as(i64, 150), r.last_timestamp.?);
    // every read entry point refreshes on its own
    try appendN(w, 151, 10);
    try w.flush();
    const latest = try r.getLatest(1);
    try std.testing.expectEqual(@as(i64, 160), latest.timestamp);
    const stats = try r.getStats(std.math.minInt(i64), std.math.maxInt(i64), 1, false);
    try std.testing.expectEqual(@as(u64, 160), stats.count);
    // the sparse index keeps up (records beyond the index stride)
    try appendN(w, 161, 3000);
    try w.flush();
    const q2 = try r.query(2000, 2010, &[_]hocdb.Filter{}, std.testing.allocator);
    defer std.testing.allocator.free(q2);
    try std.testing.expectEqual(@as(usize, 10 * RS), q2.len);
    try std.testing.expect(r.sparse_index.items.len >= 3);
    // indicators work on the reader
    const specs = [_]hocdb.indicators.Spec{.{ .kind = @intFromEnum(hocdb.indicators.Kind.sma), .period = 5 }};
    const res = try r.indicatorsTail(3, .{ .close = 1 }, &specs, DB.lookback_auto, 0, std.testing.allocator);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 3), res.n_rows);
    try std.testing.expectApproxEqRel(@as(f64, 3158), res.output(0)[2], 1e-12);
    // readers cannot write or maintain
    const rec = Rec{ .timestamp = 9_999, .value = 0 };
    try std.testing.expectError(error.ReadOnly, r.append(std.mem.asBytes(&rec)));
    try std.testing.expectError(error.ReadOnly, r.compact(0));
    try std.testing.expectError(error.ReadOnly, r.sync());
    try std.testing.expect(r.getMetrics().read_only == 1);
    try std.testing.expect(r.getMetrics().refreshes > 0);
}

test "readers follow a ring buffer across the wrap" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    const w = try open(dir, .{ .max_file_size = DB.HEADER_SIZE + 50 * RS, .overwrite_on_full = true });
    defer close(w);
    try appendN(w, 1, 30);
    try w.flush();
    const r = try openReader(dir);
    defer close(r);
    try std.testing.expectEqual(@as(u64, 30), r.count());
    try appendN(w, 31, 45); // wraps
    try w.flush();
    try r.refresh();
    try std.testing.expect(r.is_wrapped);
    try std.testing.expectEqual(@as(u64, 50), r.count());
    try std.testing.expectEqual(@as(i64, 26), try r.readTimestampAt(0));
    try std.testing.expectEqual(@as(i64, 75), try r.readTimestampAt(49));
    const data = try r.load(std.testing.allocator);
    defer std.testing.allocator.free(data);
    try std.testing.expectEqual(@as(usize, 50 * RS), data.len);
    try std.testing.expectError(error.ChecksumUnavailable, w.verify());
}

test "fsync policies and metrics" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    {
        const db = try open(dir, .{ .fsync = .on_flush });
        defer close(db);
        try appendN(db, 1, 10);
        try db.flush();
        try appendN(db, 11, 10);
        try db.flush();
        try db.flush(); // nothing pending: no extra fsync
        const m = db.getMetrics();
        try std.testing.expectEqual(@as(u64, 2), m.fsyncs);
        try std.testing.expectEqual(@as(u64, 2), m.flushes);
        try std.testing.expectEqual(@as(u64, 2), m.commits);
        try std.testing.expectEqual(@as(u64, 20), m.appends);
        try std.testing.expectEqual(@as(u64, 20 * RS), m.bytes_written);
        try std.testing.expect(m.fsync_ns_total > 0 and m.fsync_ns_max > 0);
        try std.testing.expect(m.last_append_wall_ns > 0 and m.ingest_lag_wall_ns >= 0);
        try std.testing.expectEqual(@as(i64, 20), m.last_record_ts);
        try std.testing.expectEqual(@as(u64, 20), m.committed_records);
        try std.testing.expectEqual(DB.HEADER_SIZE + 20 * RS, m.file_size);
    }
    {
        const db = try open(dir, .{ .fsync = .none });
        defer close(db);
        try appendN(db, 21, 10);
        try db.flush();
        try std.testing.expectEqual(@as(u64, 0), db.getMetrics().fsyncs);
        try db.sync(); // explicit
        try std.testing.expectEqual(@as(u64, 1), db.getMetrics().fsyncs);
    }
    {
        const db = try open(dir, .{ .fsync = .interval, .fsync_interval_ms = 0, .timestamp_unit_ns = 1_000_000_000 });
        defer close(db);
        try appendN(db, 31, 10);
        try db.flush();
        try std.testing.expectEqual(@as(u64, 1), db.getMetrics().fsyncs);
        // read metrics
        _ = try db.getStats(0, 100, 1, false);
        _ = try db.getStats(0, 100, 1, false);
        const m = db.getMetrics();
        try std.testing.expectEqual(@as(u64, 2), m.reads);
        try std.testing.expect(m.read_ns_total > 0 and m.read_ns_p50 > 0 and m.read_ns_p99 >= m.read_ns_p50);
        try std.testing.expect(m.records_read >= 40);
        try std.testing.expect(m.ingest_lag_record_ns != 0); // unit known -> record lag reported
        db.resetMetrics();
        try std.testing.expectEqual(@as(u64, 0), db.getMetrics().reads);
    }
}

test "checksum detects corruption; verify_on_open refuses a corrupted file" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var path_buf: [128]u8 = undefined;
    const path = try filePath(&path_buf, dir);
    {
        const db = try open(dir, .{});
        defer close(db);
        try appendN(db, 1, 1000);
        try db.flush();
        try std.testing.expect(try db.verify());
    }
    {
        const db = try open(dir, .{ .verify_on_open = true });
        defer close(db);
        try std.testing.expect(try db.verify());
        try appendN(db, 1001, 5); // appending keeps the running checksum consistent
        try db.flush();
        try std.testing.expect(try db.verify());
    }
    // flip one byte in the middle of the data
    {
        const f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();
        var b: [1]u8 = undefined;
        _ = try f.preadAll(&b, DB.HEADER_SIZE + 500 * RS + 9);
        b[0] ^= 0x55;
        try f.pwriteAll(&b, DB.HEADER_SIZE + 500 * RS + 9);
    }
    try std.testing.expectError(error.ChecksumMismatch, DB.init("T", dir, std.testing.allocator, schema, .{ .verify_on_open = true }));
    const db = try open(dir, .{});
    defer close(db);
    // without verify_on_open the mismatch is counted, verify() reports it, data stays readable
    try std.testing.expect(db.crc_valid);
    try std.testing.expectEqual(@as(u64, 1), db.getMetrics().crc_failures);
    try std.testing.expect(!(try db.verify()));
    try std.testing.expectEqual(@as(u64, 2), db.getMetrics().crc_failures);
    try std.testing.expectEqual(@as(u64, 1005), db.count());
    // the next commit republishes the checksum of the data as it is now
    try appendN(db, 1006, 1);
    try db.flush();
    try std.testing.expect(try db.verify());
}

test "compaction, retention and readers following a rewritten file" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var w = try open(dir, .{});
    defer close(w);
    try appendN(w, 1, 5000);
    try w.flush();
    const r = try openReader(dir);
    defer close(r);
    try std.testing.expectEqual(@as(u64, 5000), r.count());
    try w.compact(3001); // keep timestamps >= 3001
    try std.testing.expectEqual(@as(u64, 2000), w.count());
    try std.testing.expectEqual(@as(i64, 3001), try w.readTimestampAt(0));
    try std.testing.expectEqual(@as(i64, 5000), w.last_timestamp.?);
    try std.testing.expect(try w.verify());
    try std.testing.expectEqual(@as(u64, 1), w.getMetrics().compactions);
    const q = try w.query(3001, 3011, &[_]hocdb.Filter{}, std.testing.allocator);
    defer std.testing.allocator.free(q);
    try std.testing.expectEqual(@as(usize, 10 * RS), q.len);
    // the reader notices the replaced file
    try r.refresh();
    try std.testing.expectEqual(@as(u64, 2000), r.count());
    try std.testing.expectEqual(@as(i64, 3001), try r.readTimestampAt(0));
    // appends continue monotonically after compaction, reader follows
    try appendN(w, 5001, 10);
    try w.flush();
    try r.refresh();
    try std.testing.expectEqual(@as(u64, 2010), r.count());
    try w.retainLast(100);
    try std.testing.expectEqual(@as(u64, 100), w.count());
    try std.testing.expectEqual(@as(i64, 4911), try w.readTimestampAt(0));
    try r.refresh();
    try std.testing.expectEqual(@as(u64, 100), r.count());
    // automatic retention: span 1000 -> compaction once the excess is > 25%
    close(w);
    w = try open(dir, .{ .retention_span = 1000 });
    try appendN(w, 5011, 1500);
    try w.flush();
    try std.testing.expect(w.getMetrics().compactions >= 1);
    try std.testing.expect(@as(i64, 6510) - (try w.readTimestampAt(0)) <= 1250);
    try std.testing.expectEqual(@as(i64, 6510), w.last_timestamp.?);
    try r.refresh();
    try std.testing.expectEqual(w.count(), r.count());
}

test "rollover archives the file and continues; auto rollover by size" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var w = try open(dir, .{});
    defer close(w);
    try appendN(w, 1, 100);
    try w.flush();
    const r = try openReader(dir);
    defer close(r);
    const archive = try w.rollover(std.testing.allocator);
    defer std.testing.allocator.free(archive);
    try std.testing.expect(std.mem.endsWith(u8, archive, "T.1-100.bin"));
    try std.testing.expectEqual(@as(u64, 0), w.count());
    try std.testing.expectEqual(@as(u64, 1), w.getMetrics().rollovers);
    // the stream stays monotonic: an older timestamp is still rejected
    const old = Rec{ .timestamp = 50, .value = 0 };
    try std.testing.expectError(error.TimestampNotMonotonic, w.append(std.mem.asBytes(&old)));
    try appendN(w, 101, 10);
    try w.flush();
    try r.refresh();
    try std.testing.expectEqual(@as(u64, 10), r.count());
    // the archive is a normal database
    const base = std.fs.path.basename(archive);
    const ticker = base[0 .. base.len - 4];
    var adb = try DB.init(ticker, dir, std.testing.allocator, schema, .{});
    try adb.initWriter();
    defer adb.deinit();
    try std.testing.expectEqual(@as(u64, 100), adb.count());
    try std.testing.expect(try adb.verify());
    // automatic rollover
    close(w);
    w = try open(dir, .{ .rollover_size = DB.HEADER_SIZE + 500 * RS });
    try appendN(w, 111, 1200);
    try w.flush();
    try std.testing.expect(w.getMetrics().rollovers >= 2);
    try std.testing.expect(w.count() < 600);
    try std.testing.expectEqual(@as(i64, 1310), w.last_timestamp.?);
    // rollover of an empty database is refused
    try std.testing.expect(w.count() > 0);
    const last_archive = try w.rollover(std.testing.allocator);
    std.testing.allocator.free(last_archive);
    try std.testing.expectEqual(@as(u64, 0), w.count());
    try std.testing.expectError(error.EmptyDatabase, w.rollover(std.testing.allocator));
}

test "legacy HOC1 files are migrated on the first writer open (linear and wrapped)" {
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    try std.fs.cwd().makePath(dir);
    var path_buf: [128]u8 = undefined;
    const path = try filePath(&path_buf, dir);
    // hand-made legacy file: "HOC1" + schema hash + 20 records
    {
        const f = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer f.close();
        try f.writeAll("HOC1");
        const hash = schema.computeHash();
        try f.writeAll(std.mem.asBytes(&hash));
        var i: i64 = 1;
        while (i <= 20) : (i += 1) {
            const rec = Rec{ .timestamp = i, .value = @floatFromInt(i) };
            try f.writeAll(std.mem.asBytes(&rec));
        }
    }
    try std.testing.expectError(error.LegacyFormatNeedsMigration, DB.openReader("T", dir, std.testing.allocator, schema));
    {
        const db = try open(dir, .{});
        defer close(db);
        try std.testing.expectEqual(@as(u16, 2), db.format_version);
        try std.testing.expectEqual(@as(u64, 20), db.count());
        try std.testing.expectEqual(@as(i64, 20), db.last_timestamp.?);
        try std.testing.expectEqual(@as(u64, 1), db.getMetrics().migrations);
        try std.testing.expect(try db.verify());
        try appendN(db, 21, 5);
        try db.flush();
    }
    const r = try openReader(dir);
    defer close(r);
    try std.testing.expectEqual(@as(u64, 25), r.count());
    // wrapped legacy ring buffer (capacity 5, records 3..7 written after 1..5 wrapped): physical order 6,7,3,4,5
    var dir2_buf: [64]u8 = undefined;
    const dir2 = try tmpDir(&dir2_buf);
    defer std.fs.cwd().deleteTree(dir2) catch {};
    try std.fs.cwd().makePath(dir2);
    var path2_buf: [128]u8 = undefined;
    const path2 = try filePath(&path2_buf, dir2);
    {
        const f = try std.fs.cwd().createFile(path2, .{ .truncate = true });
        defer f.close();
        try f.writeAll("HOC1");
        const hash = schema.computeHash();
        try f.writeAll(std.mem.asBytes(&hash));
        for ([_]i64{ 6, 7, 3, 4, 5 }) |t| {
            const rec = Rec{ .timestamp = t, .value = @floatFromInt(t) };
            try f.writeAll(std.mem.asBytes(&rec));
        }
    }
    var db2 = try DB.init("T", dir2, std.testing.allocator, schema, .{ .max_file_size = 12 + 5 * RS, .overwrite_on_full = true });
    try db2.initWriter();
    defer db2.deinit();
    try std.testing.expectEqual(@as(u64, 5), db2.count());
    try std.testing.expect(!db2.is_wrapped); // migrated into logical (linear) order
    try std.testing.expectEqual(@as(i64, 3), try db2.readTimestampAt(0));
    try std.testing.expectEqual(@as(i64, 7), try db2.readTimestampAt(4));
    try std.testing.expectEqual(@as(i64, 7), db2.last_timestamp.?);
}
