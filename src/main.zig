const std = @import("std");
const hocdb = @import("hocdb");

const TradeData = struct {
    timestamp: i64,
    price: f64,
    volume: f64,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const ticker = "GROWING";
    const data_dir = "data";

    // Clean up previous run
    std.fs.cwd().deleteTree(data_dir) catch {};

    const DB = hocdb.TimeSeriesDB(TradeData);

    // Write data
    {
        var db = try DB.init(ticker, data_dir);
        defer db.deinit();

        const now = std.time.timestamp();

        std.debug.print("Writing data points for {s}...\n", .{ticker});
        for (0..5) |i| {
            const point = TradeData{
                .timestamp = now + @as(i64, @intCast(i)) * 60,
                .price = 100.0 + @as(f64, @floatFromInt(i)) * 1.5,
                .volume = 1000.0 + @as(f64, @floatFromInt(i)) * 10.0,
            };
            try db.append(point);
            std.debug.print("Wrote: ts={d}, price={d:.2}, vol={d:.2}\n", .{ point.timestamp, point.price, point.volume });
        }
    }

    // Load data
    {
        var db = try DB.init(ticker, data_dir);
        defer db.deinit();

        std.debug.print("\nLoading data points back into memory...\n", .{});
        const points = try db.load(allocator);
        defer allocator.free(points);

        for (points) |p| {
            std.debug.print("Read: ts={d}, price={d:.2}, vol={d:.2}\n", .{ p.timestamp, p.price, p.volume });
        }
        std.debug.print("Loaded {d} records.\n", .{points.len});
    }
}
