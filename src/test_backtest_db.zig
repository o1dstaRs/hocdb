//! DB-level tests for backtest() / backtestTail(): window semantics equal to
//! ohlcv() and results equal to the kernel on the same bars.
const std = @import("std");
const hocdb = @import("root.zig");
const ind = hocdb.indicators;
const bt = hocdb.backtest_mod;
const DB = hocdb.DynamicTimeSeriesDB;

const Tick = extern struct { timestamp: i64, price: f64, size: f64 };

const schema = hocdb.Schema{ .fields = &[_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "price", .type = .f64 },
    .{ .name = "size", .type = .f64 },
} };

const cols = DB.IndicatorColumns{ .close = 1, .volume = 2 };

fn tmpDir(buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "test_backtest_db_{x}", .{std.crypto.random.int(u64)});
}

test "backtest on bucketed bars equals the kernel on ohlcv() bars; tail and errors" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("BT", dir, a, schema, .{ .calendar = @intFromEnum(hocdb.calendar.Id.crypto), .timestamp_unit_ns = 1_000_000_000 });
    try db.initWriter();
    defer db.deinit();
    var prng = std.Random.DefaultPrng.init(5);
    const r = prng.random();
    var p: f64 = 100;
    var t: i64 = 1_700_000_000;
    for (0..20_000) |_| {
        t += r.intRangeAtMost(i64, 1, 20);
        p *= @exp(r.floatNorm(f64) * 0.001);
        const tick = Tick{ .timestamp = t, .price = p, .size = 1 + @as(f64, @floatFromInt(r.intRangeAtMost(u8, 0, 9))) };
        try db.append(std.mem.asBytes(&tick));
    }
    try db.flush();
    // bucket-aligned bounds: the backtest window (bars whose start lies in [start, end), the rows
    // of indicatorsRange) is then identical to ohlcv(start, end, bucket)
    const start: i64 = 1_700_000_100;
    const end: i64 = start + 24 * 3600;
    const bars = try db.ohlcv(start, end, 1, 2, 300, a);
    defer bars.deinit(a);
    const n = bars.len();
    try std.testing.expect(n > 100);
    const target = try a.alloc(f64, n);
    defer a.free(target);
    // simple momentum signal: long when close > close 5 bars ago, short otherwise
    for (0..n) |i| target[i] = if (i >= 5 and bars.close[i] > bars.close[i - 5]) 1.0 else if (i >= 5) -1.0 else 0.0;
    const params = bt.Params{ .initial_equity = 10_000, .cost_bps = 5, .slippage_bps = 1, .stop_loss = 0.01, .position_mode = 1 };
    var trades: [4096]bt.Trade = undefined;
    const res = try db.backtest(cols, start, end, 300, target, params, .{}, &trades, a);
    var kernel_params = params;
    kernel_params.periods_per_year = 365.0 * 288.0; // crypto calendar, 5-minute bars, filled automatically by the DB
    const ref = try bt.run(bars.ts, bars.open, bars.high, bars.low, bars.close, target, kernel_params, .{}, null, a);
    try std.testing.expectEqual(ref.n_bars, res.n_bars);
    try std.testing.expectEqual(ref.n_trades, res.n_trades);
    try std.testing.expectEqual(ref.final_equity, res.final_equity);
    try std.testing.expectEqual(ref.sharpe, res.sharpe);
    try std.testing.expectEqual(ref.ann_vol, res.ann_vol);
    try std.testing.expect(res.ann_vol > 0);
    // tail: the last 50 bars
    const tail_target = target[n - 50 ..];
    const res_tail = try db.backtestTail(cols, 50, 300, tail_target, params, .{}, null, a);
    try std.testing.expectEqual(@as(u64, 50), res_tail.n_bars);
    const tail_bars = try db.ohlcv(std.math.minInt(i64), std.math.maxInt(i64), 1, 2, 300, a);
    defer tail_bars.deinit(a);
    const m = tail_bars.len();
    kernel_params.periods_per_year = 365.0 * 288.0;
    const ref_tail = try bt.run(tail_bars.ts[m - 50 ..], tail_bars.open[m - 50 ..], tail_bars.high[m - 50 ..], tail_bars.low[m - 50 ..], tail_bars.close[m - 50 ..], tail_target, kernel_params, .{}, null, a);
    try std.testing.expectEqual(ref_tail.final_equity, res_tail.final_equity);
    // raw records (bucket 0): one row per tick
    const raw_target = try a.alloc(f64, 100);
    defer a.free(raw_target);
    @memset(raw_target, 1.0);
    const res_raw = try db.backtestTail(cols, 100, 0, raw_target, .{ .initial_equity = 1 }, .{}, null, a);
    try std.testing.expectEqual(@as(u64, 100), res_raw.n_bars);
    try std.testing.expectEqual(@as(u64, 1), res_raw.n_trades);
    // length mismatch and missing close column
    try std.testing.expectError(error.LengthMismatch, db.backtest(cols, start, end, 300, target[0 .. n - 1], params, .{}, null, a));
    try std.testing.expectError(error.MissingCloseColumn, db.backtest(.{}, start, end, 300, target, params, .{}, null, a));
}
