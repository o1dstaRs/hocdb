const std = @import("std");
const root = @import("root.zig");
const TimeSeriesDB = root.TimeSeriesDB;

test "Auto-Increment: Basic Functionality & Persistence" {
    const TestStruct = struct {
        timestamp: i64,
        value: f64,
    };

    const ticker = "TEST_AUTO_INC";
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "test_auto_inc_{x}", .{std.crypto.random.int(u64)});

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch |err| if (err != error.FileNotFound) return err;
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(TestStruct);

    // 1. Initialize with auto_increment = true
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{ .auto_increment = true });
        defer db.deinit();

        // Append 10 records with dummy timestamp
        var i: i64 = 0;
        while (i < 10) : (i += 1) {
            try db.append(.{ .timestamp = 0, .value = @floatFromInt(i) });
        }

        // Verify timestamps 1..10
        const data = try db.load(std.testing.allocator);
        defer std.testing.allocator.free(data);

        try std.testing.expectEqual(@as(usize, 10), data.len);
        for (data, 0..) |rec, idx| {
            try std.testing.expectEqual(@as(i64, @intCast(idx + 1)), rec.timestamp);
            try std.testing.expectEqual(@as(f64, @floatFromInt(idx)), rec.value);
        }
    }

    // 2. Reopen and append more
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{ .auto_increment = true });
        defer db.deinit();

        // Append 5 more records
        var i: i64 = 10;
        while (i < 15) : (i += 1) {
            try db.append(.{ .timestamp = 999, .value = @floatFromInt(i) });
        }

        // Verify timestamps 1..15
        const data = try db.load(std.testing.allocator);
        defer std.testing.allocator.free(data);

        try std.testing.expectEqual(@as(usize, 15), data.len);
        for (data, 0..) |rec, idx| {
            try std.testing.expectEqual(@as(i64, @intCast(idx + 1)), rec.timestamp);
            try std.testing.expectEqual(@as(f64, @floatFromInt(idx)), rec.value);
        }
    }
}

test "AutoIncrement: Missing Timestamp Field" {
    const NoTimestampStruct = struct {
        value: f64,
        id: u64,
    };
    const ticker = "TEST_NO_TS";
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "test_no_ts_{x}", .{std.crypto.random.int(u64)});

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch |err| if (err != error.FileNotFound) return err;
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(NoTimestampStruct);

    // Should fail with MissingTimestampField regardless of auto_increment
    if (DB.init(ticker, dir, std.testing.allocator, .{ .auto_increment = true })) |db_val| {
        var db = db_val;
        db.deinit();
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(error.MissingTimestampField, err);
    }

    if (DB.init(ticker, dir, std.testing.allocator, .{ .auto_increment = false })) |db_val| {
        var db = db_val;
        db.deinit();
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(error.MissingTimestampField, err);
    }
}
