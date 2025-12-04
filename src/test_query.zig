const std = @import("std");
const root = @import("root.zig");
const TimeSeriesDB = root.TimeSeriesDB;

test "TimeSeriesDB query usage" {
    const TestStruct = struct {
        timestamp: i64,
        value: f64,
    };

    const ticker = "TEST_QUERY";
    const dir = "test_query_data";

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch {};
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(TestStruct);

    // 1. Linear Test
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{});
        defer db.deinit();

        try db.append(.{ .timestamp = 100, .value = 1.0 });
        try db.append(.{ .timestamp = 200, .value = 2.0 });
        try db.append(.{ .timestamp = 300, .value = 3.0 });
        try db.append(.{ .timestamp = 400, .value = 4.0 });
        try db.append(.{ .timestamp = 500, .value = 5.0 });

        // Query subset
        const res = try db.query(200, 450, std.testing.allocator); // Should get 200, 300, 400
        defer std.testing.allocator.free(res);

        try std.testing.expectEqual(3, res.len);
        try std.testing.expectEqual(200, res[0].timestamp);
        try std.testing.expectEqual(400, res[2].timestamp);
    }

    // Cleanup for next test
    std.fs.cwd().deleteTree(dir) catch {};

    // 2. Ring Buffer Test
    {
        // Small file size to force wrap
        // Header (4+8=12) + 3 records (16*3=48) = 60 bytes
        // We want to write 5 records.
        // 1, 2, 3 -> Full.
        // 4 -> Overwrites 1.
        // 5 -> Overwrites 2.
        // Content: 4, 5, 3 (physically: 4 at 0, 5 at 1, 3 at 2)
        // Logical: 3, 4, 5 (Wait, 3 is oldest? No)
        // Write 1, 2, 3. Cursor at end.
        // Write 4. Cursor at 0+rec_size. Overwrites 1.
        // Oldest is 2 (at 1). Newest is 4 (at 0).
        // Wait, if we overwrite 1, 1 is gone.
        // Records: 4, 2, 3.
        // Order: 2, 3, 4.
        // Write 5. Overwrites 2.
        // Records: 4, 5, 3.
        // Order: 3, 4, 5.

        var db = try DB.init(ticker, dir, std.testing.allocator, .{ .max_file_size = 60, .overwrite_on_full = true });
        defer db.deinit();

        try db.append(.{ .timestamp = 100, .value = 1.0 });
        try db.append(.{ .timestamp = 200, .value = 2.0 });
        try db.append(.{ .timestamp = 300, .value = 3.0 });

        // Full now.
        try db.append(.{ .timestamp = 400, .value = 4.0 }); // Overwrites 100
        try db.append(.{ .timestamp = 500, .value = 5.0 }); // Overwrites 200

        // Current state: 300, 400, 500.
        // Physical: [400, 500, 300] (assuming 0-based index of records)

        // Query all
        const res = try db.query(0, 600, std.testing.allocator);
        defer std.testing.allocator.free(res);

        try std.testing.expectEqual(3, res.len);
        try std.testing.expectEqual(300, res[0].timestamp);
        try std.testing.expectEqual(400, res[1].timestamp);
        try std.testing.expectEqual(500, res[2].timestamp);

        // Query partial crossing wrap
        // 300 is at index 2 (physical). 400 is at index 0. 500 is at index 1.
        // Logical: 0->300, 1->400, 2->500.
        // Query 350 to 550 -> Should get 400, 500.
        const res2 = try db.query(350, 550, std.testing.allocator);
        defer std.testing.allocator.free(res2);

        try std.testing.expectEqual(2, res2.len);
        try std.testing.expectEqual(400, res2[0].timestamp);
        try std.testing.expectEqual(500, res2[1].timestamp);
    }
}

test "TimeSeriesDB multiple wrap" {
    const TestStruct = struct {
        timestamp: i64,
        value: f64,
    };
    const ticker = "TEST_QUERY_WRAP";
    const dir = "test_query_wrap_data";
    const DB = TimeSeriesDB(TestStruct);

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch {};
    defer std.fs.cwd().deleteTree(dir) catch {};

    // 3. Multiple Wrap Test
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{ .max_file_size = 60, .overwrite_on_full = true });
        defer db.deinit();

        // Write 10 records. Capacity is 3.
        // Should end up with 8, 9, 10.
        var i: i64 = 1;
        while (i <= 10) : (i += 1) {
            try db.append(.{ .timestamp = i * 100, .value = @floatFromInt(i) });
        }

        // Expected state: 800, 900, 1000.

        // Query All
        const res = try db.query(0, 2000, std.testing.allocator);
        defer std.testing.allocator.free(res);

        try std.testing.expectEqual(3, res.len);
        try std.testing.expectEqual(800, res[0].timestamp);
        try std.testing.expectEqual(900, res[1].timestamp);
        try std.testing.expectEqual(1000, res[2].timestamp);

        // Query Middle (900)
        const res2 = try db.query(850, 950, std.testing.allocator);
        defer std.testing.allocator.free(res2);

        try std.testing.expectEqual(1, res2.len);
        try std.testing.expectEqual(900, res2[0].timestamp);

        // Query Wrap (900, 1000)
        const res3 = try db.query(850, 1050, std.testing.allocator);
        defer std.testing.allocator.free(res3);

        try std.testing.expectEqual(2, res3.len);
        try std.testing.expectEqual(900, res3[0].timestamp);
        try std.testing.expectEqual(1000, res3[1].timestamp);
    }
}
