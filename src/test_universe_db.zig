//! DB-level tests for universe(): the k-way inner join over several
//! databases and agreement with the pure kernel on hand-joined arrays.
const std = @import("std");
const hocdb = @import("root.zig");
const ind = hocdb.indicators;
const uni = hocdb.universe_mod;
const DB = hocdb.DynamicTimeSeriesDB;

const Bar = extern struct { timestamp: i64, open: f64, high: f64, low: f64, close: f64, volume: f64 };

const schema = hocdb.Schema{ .fields = &[_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "open", .type = .f64 },
    .{ .name = "high", .type = .f64 },
    .{ .name = "low", .type = .f64 },
    .{ .name = "close", .type = .f64 },
    .{ .name = "volume", .type = .f64 },
} };

const cols = DB.IndicatorColumns{ .open = 1, .high = 2, .low = 3, .close = 4, .volume = 5 };

fn tmpDir(buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "test_universe_db_{x}", .{std.crypto.random.int(u64)});
}

/// Bars every 60 units from t0; `skip` drops every skip-th bar (0 = none).
fn fill(db: *DB, n: usize, seed: u64, skip: usize) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();
    var p: f64 = 100.0;
    for (0..n) |i| {
        const o = p;
        p *= @exp(r.floatNorm(f64) * 0.01);
        if (skip > 0 and i % skip == skip - 1) continue;
        const bar = Bar{ .timestamp = @intCast(1_000 + i * 60), .open = o, .high = @max(o, p) * 1.003, .low = @min(o, p) * 0.997, .close = p, .volume = 1000.0 + @as(f64, @floatFromInt(i % 50)) };
        try db.append(std.mem.asBytes(&bar));
    }
    try db.flush();
}

test "universe joins several databases and matches the kernel on hand-joined arrays" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var d1 = try DB.init("U1", dir, a, schema, .{});
    try d1.initWriter();
    defer d1.deinit();
    var d2 = try DB.init("U2", dir, a, schema, .{});
    try d2.initWriter();
    defer d2.deinit();
    var d3 = try DB.init("U3", dir, a, schema, .{});
    try d3.initWriter();
    defer d3.deinit();
    try fill(&d1, 400, 1, 0);
    try fill(&d2, 400, 2, 0);
    try fill(&d3, 400, 3, 7); // every 7th bar missing
    const dbs = [_]*DB{ &d1, &d2, &d3 };
    var rows: [3]uni.Row = undefined;
    var corr: [9]f64 = undefined;
    const params = uni.Params{ .mom_long = 30, .corr_period = 30, .beta_period = 30, .sma_period = 25 };
    const sum = try DB.universe(&dbs, cols, 120, 0, params, &rows, &corr, a);
    try std.testing.expectEqual(@as(u64, 3), sum.n_tickers);
    // 120 records of each: the join keeps the timestamps present in all three
    try std.testing.expect(sum.n_bars < 120 and sum.n_bars > 100);
    try std.testing.expect(!ind.isNan(rows[0].mom_long) and !ind.isNan(rows[2].beta));
    try std.testing.expectApproxEqAbs(@as(f64, 1), corr[0], 1e-12);
    try std.testing.expectApproxEqAbs(corr[1], corr[3], 1e-12);
    // hand-join the last 120 records and run the kernel directly
    const raw1 = try d1.query(std.math.minInt(i64), std.math.maxInt(i64), &.{}, a);
    defer a.free(raw1);
    const raw2 = try d2.query(std.math.minInt(i64), std.math.maxInt(i64), &.{}, a);
    defer a.free(raw2);
    const raw3 = try d3.query(std.math.minInt(i64), std.math.maxInt(i64), &.{}, a);
    defer a.free(raw3);
    const b1 = @as([*]const Bar, @ptrCast(@alignCast(raw1.ptr)))[0 .. raw1.len / @sizeOf(Bar)];
    const b2 = @as([*]const Bar, @ptrCast(@alignCast(raw2.ptr)))[0 .. raw2.len / @sizeOf(Bar)];
    const b3 = @as([*]const Bar, @ptrCast(@alignCast(raw3.ptr)))[0 .. raw3.len / @sizeOf(Bar)];
    const t1 = b1[b1.len - 120 ..];
    const t2 = b2[b2.len - 120 ..];
    const t3 = b3[b3.len - 120 ..];
    var jts = std.ArrayList(i64){};
    defer jts.deinit(a);
    var c1 = std.ArrayList(f64){};
    defer c1.deinit(a);
    var c2 = std.ArrayList(f64){};
    defer c2.deinit(a);
    var c3 = std.ArrayList(f64){};
    defer c3.deinit(a);
    var v1 = std.ArrayList(f64){};
    defer v1.deinit(a);
    var v2 = std.ArrayList(f64){};
    defer v2.deinit(a);
    var v3 = std.ArrayList(f64){};
    defer v3.deinit(a);
    for (t3) |x| {
        var k1: ?usize = null;
        var k2: ?usize = null;
        for (t1, 0..) |y, k| if (y.timestamp == x.timestamp) {
            k1 = k;
        };
        for (t2, 0..) |y, k| if (y.timestamp == x.timestamp) {
            k2 = k;
        };
        if (k1 != null and k2 != null) {
            try jts.append(a, x.timestamp);
            try c1.append(a, t1[k1.?].close);
            try c2.append(a, t2[k2.?].close);
            try c3.append(a, x.close);
            try v1.append(a, t1[k1.?].volume);
            try v2.append(a, t2[k2.?].volume);
            try v3.append(a, x.volume);
        }
    }
    try std.testing.expectEqual(jts.items.len, sum.n_bars);
    const closes = [_][]const f64{ c1.items, c2.items, c3.items };
    const vols = [_][]const f64{ v1.items, v2.items, v3.items };
    var rows2: [3]uni.Row = undefined;
    var corr2: [9]f64 = undefined;
    const sum2 = try uni.compute(&closes, &vols, jts.items, params, &rows2, &corr2, a);
    try std.testing.expectEqual(sum.n_bars, sum2.n_bars);
    try std.testing.expectEqual(sum.first_ts, sum2.first_ts);
    try std.testing.expectEqual(sum.last_ts, sum2.last_ts);
    inline for (std.meta.fields(uni.Row)) |f| {
        for (0..3) |i| {
            const x = @field(rows[i], f.name);
            const y = @field(rows2[i], f.name);
            if (f.type == f64) {
                try std.testing.expect((ind.isNan(x) and ind.isNan(y)) or x == y);
            } else {
                try std.testing.expectEqual(y, x);
            }
        }
    }
    for (corr, corr2) |x, y| try std.testing.expect((ind.isNan(x) and ind.isNan(y)) or x == y);
    // bucket mode with n_bars = 0 picks enough bars for the longest period
    const sum3 = try DB.universe(&dbs, cols, 0, 120, params, &rows, null, a);
    try std.testing.expect(sum3.n_bars >= 31);
    try std.testing.expect(!ind.isNan(rows[1].mom_long));
    // errors
    var short: [2]uni.Row = undefined;
    try std.testing.expectError(error.LengthMismatch, DB.universe(&dbs, cols, 0, 0, params, &short, null, a));
    const empty = try DB.universe(&.{}, cols, 0, 0, params, &.{}, null, a);
    try std.testing.expectEqual(@as(u64, 0), empty.n_tickers);
}
