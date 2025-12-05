const std = @import("std");
const root = @import("root.zig");
const TimeSeriesDB = root.TimeSeriesDB;

test "Integrity: Load with Ring Buffer" {
    const TestStruct = struct {
        timestamp: i64,
        value: f64,
        id: u64,
    };

    const ticker = "TEST_INTEGRITY_RING";
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "test_integrity_ring_{x}", .{std.crypto.random.int(u64)});

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch |err| if (err != error.FileNotFound) return err;
    defer std.fs.cwd().deleteTree(dir) catch {};

    // Calculate size for exactly 5 records
    // Header (4+8=12) + 5 * (8+8+8=24) = 12 + 120 = 132 bytes
    const record_size = 24;
    const capacity = 5;
    const max_size = 12 + (capacity * record_size);

    const DB = TimeSeriesDB(TestStruct);
    var db = try DB.init(ticker, dir, std.testing.allocator, .{ .max_file_size = max_size, .overwrite_on_full = true });
    defer db.deinit();

    // Write 15 records (wraps 3 times)
    var i: i64 = 0;
    while (i < 15) : (i += 1) {
        try db.append(.{ .timestamp = i * 100, .value = @floatFromInt(i), .id = @intCast(i) });
    }

    // Expected: 10, 11, 12, 13, 14

    // Test load()
    const data = try db.load(std.testing.allocator);
    defer std.testing.allocator.free(data);

    // Verify size
    try std.testing.expectEqual(@as(usize, capacity), data.len);

    // Verify content and order
    var j: usize = 0;
    while (j < capacity) : (j += 1) {
        const rec = data[j];
        const expected_idx = 10 + j;

        try std.testing.expectEqual(@as(i64, @intCast(expected_idx)) * 100, rec.timestamp);
        try std.testing.expectEqual(@as(f64, @floatFromInt(expected_idx)), rec.value);
        try std.testing.expectEqual(@as(u64, @intCast(expected_idx)), rec.id);
    }
}

test "Integrity: Random Data" {
    const TestStruct = struct {
        timestamp: i64,
        v1: u64,
        v2: u64,
    };

    const ticker = "TEST_INTEGRITY_RANDOM";
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "test_integrity_random_{x}", .{std.crypto.random.int(u64)});

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch |err| if (err != error.FileNotFound) return err;
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(TestStruct);
    var db = try DB.init(ticker, dir, std.testing.allocator, .{});
    defer db.deinit();

    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    const count = 1000;
    var expected_sums: u64 = 0;

    var i: i64 = 0;
    while (i < count) : (i += 1) {
        const v1 = random.int(u64);
        const v2 = random.int(u64);
        expected_sums +%= (v1 +% v2);

        try db.append(.{ .timestamp = i, .v1 = v1, .v2 = v2 });
    }

    // Load and verify sum
    const data = try db.load(std.testing.allocator);
    defer std.testing.allocator.free(data);

    try std.testing.expectEqual(@as(usize, count), data.len);

    var actual_sums: u64 = 0;
    var k: usize = 0;
    while (k < count) : (k += 1) {
        const rec = data[k];
        try std.testing.expectEqual(@as(i64, @intCast(k)), rec.timestamp);
        actual_sums +%= (rec.v1 +% rec.v2);
    }

    try std.testing.expectEqual(expected_sums, actual_sums);
}
