const std = @import("std");
const hocdb = @import("root.zig");
const TimeSeriesDB = hocdb.TimeSeriesDB;
const DynamicTimeSeriesDB = hocdb.DynamicTimeSeriesDB;

const TestRecord = extern struct {
    timestamp: i64,
    value: f64,
};

test "Aggregation API (getStats, getLatest)" {
    const ticker = "TEST_STATS";
    const dir = "test_stats_data";

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch {};
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(TestRecord);
    var db = try DB.init(ticker, dir, std.testing.allocator, .{
        .max_file_size = 1024 * 1024, // Large enough to avoid wrap for basic test
    });
    defer db.deinit();

    // 1. Empty DB
    {
        const latest = db.dynamic_db.getLatest(1); // value field index = 1
        try std.testing.expectError(error.EmptyDB, latest);

        const stats = try db.dynamic_db.getStats(0, 1000, 1);
        try std.testing.expectEqual(@as(u64, 0), stats.count);
    }

    // 2. Append Data
    // 100: 10.0
    // 200: 20.0
    // 300: 30.0
    // 400: 40.0
    // 500: 50.0
    try db.append(.{ .timestamp = 100, .value = 10.0 });
    try db.append(.{ .timestamp = 200, .value = 20.0 });
    try db.append(.{ .timestamp = 300, .value = 30.0 });
    try db.append(.{ .timestamp = 400, .value = 40.0 });
    try db.append(.{ .timestamp = 500, .value = 50.0 });

    // 3. getLatest
    {
        const latest = try db.dynamic_db.getLatest(1);
        try std.testing.expectEqual(50.0, latest.value);
        try std.testing.expectEqual(500, latest.timestamp);
    }

    // 4. getStats (Full Range)
    {
        const stats = try db.dynamic_db.getStats(0, 600, 1);
        try std.testing.expectEqual(@as(u64, 5), stats.count);
        try std.testing.expectEqual(10.0, stats.min);
        try std.testing.expectEqual(50.0, stats.max);
        try std.testing.expectEqual(150.0, stats.sum);
        try std.testing.expectEqual(30.0, stats.mean);
    }

    // 5. getStats (Partial Range)
    {
        // 200, 300, 400
        const stats = try db.dynamic_db.getStats(200, 450, 1);
        try std.testing.expectEqual(@as(u64, 3), stats.count);
        try std.testing.expectEqual(20.0, stats.min);
        try std.testing.expectEqual(40.0, stats.max);
        try std.testing.expectEqual(90.0, stats.sum);
        try std.testing.expectEqual(30.0, stats.mean);
    }
}

test "Aggregation API (Wrapped Buffer)" {
    const ticker = "TEST_STATS_WRAPPED";
    const dir = "test_stats_wrapped_data";

    std.fs.cwd().deleteTree(dir) catch {};
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(TestRecord);
    const record_size = @sizeOf(TestRecord); // 16
    const header_size = 12; // 4 magic + 8 hash
    const capacity = 5;
    const max_size = header_size + capacity * record_size; // 12 + 80 = 92

    var db = try DB.init(ticker, dir, std.testing.allocator, .{
        .max_file_size = max_size,
        .overwrite_on_full = true,
    });
    defer db.deinit();

    // Fill buffer: 100, 200, 300, 400, 500
    try db.append(.{ .timestamp = 100, .value = 10.0 });
    try db.append(.{ .timestamp = 200, .value = 20.0 });
    try db.append(.{ .timestamp = 300, .value = 30.0 });
    try db.append(.{ .timestamp = 400, .value = 40.0 });
    try db.append(.{ .timestamp = 500, .value = 50.0 });

    // Wrap: 600 (overwrites 100), 700 (overwrites 200)
    // Buffer: [600, 700, 300, 400, 500] (physically)
    // Logical: 300, 400, 500, 600, 700
    try db.append(.{ .timestamp = 600, .value = 60.0 });
    try db.append(.{ .timestamp = 700, .value = 70.0 });

    // 1. getLatest
    {
        const latest = try db.dynamic_db.getLatest(1);
        try std.testing.expectEqual(70.0, latest.value);
        try std.testing.expectEqual(700, latest.timestamp);
    }

    // 2. getStats (Full Range)
    {
        const stats = try db.dynamic_db.getStats(0, 1000, 1);
        try std.testing.expectEqual(@as(u64, 5), stats.count);
        try std.testing.expectEqual(30.0, stats.min);
        try std.testing.expectEqual(70.0, stats.max);
        try std.testing.expectEqual(250.0, stats.sum);
        try std.testing.expectEqual(50.0, stats.mean);
    }

    // 3. getStats (Cross-Wrap Range)
    // Query 400 to 650 -> Should get 400, 500, 600
    {
        const stats = try db.dynamic_db.getStats(400, 650, 1);
        try std.testing.expectEqual(@as(u64, 3), stats.count);
        try std.testing.expectEqual(40.0, stats.min);
        try std.testing.expectEqual(60.0, stats.max);
        try std.testing.expectEqual(150.0, stats.sum);
        try std.testing.expectEqual(50.0, stats.mean);
    }
}
