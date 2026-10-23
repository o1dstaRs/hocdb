//! Regression coverage for Zig 0.17 I/O and the optimized storage paths.
const std = @import("std");
const hocdb = @import("root.zig");
const runtime = hocdb.runtime;
const DB = hocdb.DynamicTimeSeriesDB;
const Rec = extern struct { timestamp: i64, value: f64 };
const schema = hocdb.Schema{ .fields = &.{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "value", .type = .f64 },
} };

test "lower bounds match a scalar model across strides, sparse gaps and ring wraps" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_search_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    for ([_]u64{ 1, 2, 31, 1024, 8192 }) |stride| {
        for ([_]bool{ false, true }) |ring| {
            const db = try a.create(DB);
            defer a.destroy(db);
            db.* = try DB.init("SEARCH", dir, a, schema, .{
                .index_stride = stride,
                .max_file_size = DB.HEADER_SIZE + (if (ring) @as(u64, 67) else 5000) * 16,
            });
            try db.initWriter();
            defer db.drop() catch unreachable;
            const n: i64 = 4500;
            for (0..@intCast(n)) |i| {
                const rec = Rec{ .timestamp = -10000 + @as(i64, @intCast(i)) * 7, .value = @floatFromInt(i) };
                try db.append(std.mem.asBytes(&rec));
            }
            try db.flush();
            const first: i64 = if (ring) n - 67 else 0;
            const retained: i64 = n - first;
            var prng = std.Random.DefaultPrng.init(stride);
            for (0..600) |_| {
                const target = prng.random().intRangeAtMost(i64, -11000, 23000);
                const logical = std.math.clamp(@divFloor(target + 10000 + 6, 7) - first, 0, retained);
                try std.testing.expectEqual(@as(u64, @intCast(logical)), try db.binarySearch(target));
            }
            const loaded = try db.load(a);
            defer a.free(loaded);
            try std.testing.expectEqual(@as(usize, @intCast(retained * 16)), loaded.len);
            const buffer = try a.alloc(u8, loaded.len);
            defer a.free(buffer);
            const written = try db.queryInto(std.math.minInt(i64), std.math.maxInt(i64), &.{}, buffer);
            try std.testing.expectEqualSlices(u8, loaded, buffer[0..written]);
            try std.testing.expectError(error.BufferTooSmall, db.queryInto(std.math.minInt(i64), std.math.maxInt(i64), &.{}, buffer[0..1]));
        }
    }
}

test "typed records round-trip native padding and reordered fields with caller-owned I/O" {
    const a = std.testing.allocator;
    var backend: std.Io.Threaded = .init_single_threaded;
    const io = backend.io();
    const Mixed = struct { flag: bool, timestamp: i64, code: u8, value: f64 };
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_layout_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(io, dir) catch {};
    for ([_]bool{ false, true }) |ring| {
        var db = try hocdb.TimeSeriesDB(Mixed).initWithIo(io, "MIXED", dir, a, .{
            .max_file_size = DB.HEADER_SIZE + (if (ring) @as(u64, 3) else 4) * 18,
            .overwrite_on_full = ring,
        });
        try std.testing.expectEqual(io.userdata, db.dynamic_db.io.userdata);
        for (0..4) |i| try db.append(.{
            .flag = i % 2 == 0,
            .timestamp = std.math.maxInt(i64) - 3 + @as(i64, @intCast(i)),
            .code = @intCast(i),
            .value = @as(f64, @floatFromInt(i)) + 0.5,
        });
        const loaded = try db.load(a);
        defer a.free(loaded);
        try std.testing.expectEqual(@as(usize, if (ring) 3 else 4), loaded.len);
        for (loaded, 0..) |rec, j| {
            const i = j + @as(usize, if (ring) 1 else 0);
            try std.testing.expectEqual(i % 2 == 0, rec.flag);
            try std.testing.expectEqual(@as(u8, @intCast(i)), rec.code);
            try std.testing.expectEqual(@as(f64, @floatFromInt(i)) + 0.5, rec.value);
        }
        const queried = try db.query(std.math.minInt(i64), std.math.maxInt(i64), a);
        defer a.free(queried);
        try std.testing.expectEqual(loaded.len - 1, queried.len);
        db.deinit();
        // Reopen exercises schema order and the persisted CRC.
        db = try hocdb.TimeSeriesDB(Mixed).initWithIo(io, "MIXED", dir, a, .{});
        const reopened = try db.load(a);
        defer a.free(reopened);
        try std.testing.expectEqual(std.math.maxInt(i64), reopened[reopened.len - 1].timestamp);
        try db.dynamic_db.drop();
        a.destroy(db.dynamic_db);
    }
}

test "bulk load includes the maximum timestamp for native records and wrapped rings" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_max_ts_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    var db = try hocdb.TimeSeriesDB(Rec).init("MAX", dir, a, .{ .max_file_size = DB.HEADER_SIZE + 2 * 16 });
    defer db.deinit();
    for (0..3) |i| try db.append(.{ .timestamp = std.math.maxInt(i64) - 2 + @as(i64, @intCast(i)), .value = @floatFromInt(i) });
    const loaded = try db.load(a);
    defer a.free(loaded);
    try std.testing.expectEqual(@as(usize, 2), loaded.len);
    try std.testing.expectEqual(std.math.maxInt(i64), loaded[1].timestamp);
}

test "failed append leaves timestamps, checksums and counters intact" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_full_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    var db = try hocdb.TimeSeriesDB(Rec).init("FULL", dir, a, .{
        .max_file_size = DB.HEADER_SIZE + 16,
        .overwrite_on_full = false,
    });
    defer db.deinit();
    try db.append(.{ .timestamp = 1, .value = 42 });
    try std.testing.expectError(error.DiskFull, db.append(.{ .timestamp = 2, .value = 43 }));
    try db.flush();
    try std.testing.expectEqual(@as(i64, 1), db.dynamic_db.last_timestamp.?);
    try std.testing.expectEqual(@as(u64, 1), db.dynamic_db.getMetrics().appends);
    try std.testing.expect(try db.dynamic_db.verify());
    try std.testing.expectError(error.InvalidIndexStride, DB.init("BAD", dir, a, schema, .{ .index_stride = 0 }));
}

test "readers load only published records even when an uncommitted tail is on disk" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_tail_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    var writer = try hocdb.TimeSeriesDB(Rec).init("TAIL", dir, a, .{});
    defer writer.deinit();
    try writer.append(.{ .timestamp = 1, .value = 1 });
    try writer.flush();
    var reader = try DB.openReader("TAIL", dir, a, schema);
    defer reader.deinit();
    const extra = Rec{ .timestamp = 2, .value = 2 };
    try writer.dynamic_db.file.writePositionalAll(runtime.io, std.mem.asBytes(&extra), DB.HEADER_SIZE + 16);
    const loaded = try reader.load(a);
    defer a.free(loaded);
    try std.testing.expectEqual(@as(usize, 16), loaded.len);
}

test "initialization and index allocation failures release all owned resources" {
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_oom_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    const Exercise = struct {
        fn run(allocator: std.mem.Allocator, directory: []const u8) !void {
            var db = try hocdb.TimeSeriesDB(Rec).init("OOM", directory, allocator, .{});
            defer db.deinit();
            try db.append(.{ .timestamp = (db.dynamic_db.last_timestamp orelse 0) + 1, .value = 1 });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{dir});
}

test "index allocation failure does not accept or checksum a rejected append" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_reject_oom_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    var db = try hocdb.TimeSeriesDB(Rec).init("REJECT", dir, a, .{});
    defer db.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    db.dynamic_db.allocator = failing.allocator();
    const result = db.append(.{ .timestamp = 1, .value = 42 });
    db.dynamic_db.allocator = a;
    try std.testing.expectError(error.OutOfMemory, result);
    try std.testing.expectEqual(@as(?i64, null), db.dynamic_db.last_timestamp);
    try std.testing.expectEqual(@as(u64, 0), db.dynamic_db.getMetrics().appends);
    try std.testing.expect(try db.dynamic_db.verify());
    try db.append(.{ .timestamp = 1, .value = 42 });
    try db.flush();
    try std.testing.expect(try db.dynamic_db.verify());
}

test "wrapping during the final flush invalidates sparse index bounds" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_flush_wrap_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    var db = try hocdb.TimeSeriesDB(Rec).init("WRAP", dir, a, .{
        .max_file_size = DB.HEADER_SIZE + 2050 * 16,
        .index_stride = 64,
    });
    defer db.deinit();
    for (0..2052) |i| try db.append(.{ .timestamp = @intCast(i), .value = @floatFromInt(i) });
    try db.flush();
    try std.testing.expect(db.dynamic_db.is_wrapped);
    for (0..2054) |i| {
        const expected = std.math.clamp(@as(i64, @intCast(i)) - 2, 0, 2050);
        try std.testing.expectEqual(@as(u64, @intCast(expected)), try db.dynamic_db.binarySearch(@intCast(i)));
    }
}

test "writers reject an I/O implementation that accepts conflicting exclusive locks" {
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_unreliable_locks_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    const Broken = struct {
        fn tryLock(_: ?*anyopaque, _: std.Io.File, _: std.Io.File.Lock) std.Io.File.LockError!bool {
            return true;
        }
    };
    var vtable = runtime.io.vtable.*;
    vtable.fileTryLock = Broken.tryLock;
    const io = std.Io{ .userdata = runtime.io.userdata, .vtable = &vtable };
    try std.testing.expectError(error.FileLocksUnsupported, DB.initWithIo(io, "LOCK", dir, std.testing.allocator, schema, .{}));
}

test "large auto-increment records preserve input and reject allocation failures atomically" {
    const a = std.testing.allocator;
    var path: [100]u8 = undefined;
    const dir = try std.fmt.bufPrint(&path, "test_storage_large_auto_{x}", .{runtime.randomInt(u64)});
    defer std.Io.Dir.cwd().deleteTree(runtime.io, dir) catch {};
    const large_schema = hocdb.Schema{ .fields = &.{
        .{ .name = "payload", .type = .string },
        .{ .name = "payload2", .type = .string },
        .{ .name = "timestamp", .type = .i64 },
        .{ .name = "value", .type = .f64 },
    } };
    const db = try a.create(DB);
    defer a.destroy(db);
    db.* = try DB.init("LARGE", dir, a, large_schema, .{ .auto_increment = true });
    defer db.deinit();
    try db.initWriter();
    var record: [272]u8 = @splat(0);
    std.mem.writeInt(i64, record[256..264], 999, .little);
    for (0..3) |_| try db.append(&record);
    try std.testing.expectEqual(@as(i64, 999), std.mem.readInt(i64, record[256..264], .little));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    db.allocator = failing.allocator();
    const result = db.append(&record);
    db.allocator = a;
    try std.testing.expectError(error.OutOfMemory, result);
    try std.testing.expectEqual(@as(i64, 3), db.last_timestamp.?);
    try std.testing.expectEqual(@as(u64, 3), db.getMetrics().appends);
    try std.testing.expect(try db.verify());
    const loaded = try db.load(a);
    defer a.free(loaded);
    for (0..3) |i| {
        const offset = i * record.len + 256;
        try std.testing.expectEqual(@as(i64, @intCast(i + 1)), std.mem.readInt(i64, loaded[offset..][0..8], .little));
    }
}
