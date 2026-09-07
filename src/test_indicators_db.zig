//! DB-level tests for the indicator API: column extraction, range/tail
//! batches, warm-up trimming, tick->bar resampling, summary and snapshot,
//! including the ring-buffer wrap path.
const std = @import("std");
const hocdb = @import("root.zig");
const ind = hocdb.indicators;
const DB = hocdb.DynamicTimeSeriesDB;

const Bar = extern struct {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
};

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
    return std.fmt.bufPrint(buf, "test_ind_db_{x}", .{std.crypto.random.int(u64)});
}

fn fill(db: *DB, n: usize, seed: u64) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();
    var p: f64 = 100.0;
    for (0..n) |i| {
        const o = p;
        p *= @exp(r.floatNorm(f64) * 0.01);
        const bar = Bar{
            .timestamp = @intCast(1_000 + i * 60),
            .open = o,
            .high = @max(o, p) * 1.003,
            .low = @min(o, p) * 0.997,
            .close = p,
            .volume = 1000.0 + @as(f64, @floatFromInt(i % 50)),
        };
        try db.append(std.mem.asBytes(&bar));
    }
    try db.flush();
}

test "indicatorsRange trims warm-up rows and matches direct kernels" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("IND", dir, a, schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    const n: usize = 1200;
    try fill(&db, n, 11);

    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.sma), .period = 20 },
        .{ .kind = @intFromEnum(ind.Kind.macd) },
        .{ .kind = @intFromEnum(ind.Kind.atr), .period = 14 },
        .{ .kind = @intFromEnum(ind.Kind.obv) },
    };
    // window = records 500..800 (timestamps 1000+500*60 .. 1000+800*60)
    const start_ts: i64 = 1_000 + 500 * 60;
    const end_ts: i64 = 1_000 + 800 * 60;
    const res = try db.indicatorsRange(start_ts, end_ts, cols, &specs, DB.lookback_auto, 0, a);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 300), res.n_rows);
    try std.testing.expectEqual(@as(usize, 1 + 3 + 1 + 1), res.n_outputs);
    try std.testing.expectEqual(start_ts, res.timestamps[0]);
    try std.testing.expectEqual(end_ts - 60, res.timestamps[299]);
    // no NaN inside the window thanks to auto warm-up
    for (res.output(0)) |v| try std.testing.expect(!ind.isNan(v));
    for (res.output(1)) |v| try std.testing.expect(!ind.isNan(v));
    for (res.output(4)) |v| try std.testing.expect(!ind.isNan(v));

    // Compare against kernels on the full column history.
    const set = try db.readColumns(0, n, &[_]usize{ 2, 3, 4, 5 }, a);
    defer set.deinit();
    const out = try a.alloc(f64, n);
    defer a.free(out);
    try ind.sma(set.cols[2], 20, out);
    for (0..300) |i| try std.testing.expectApproxEqRel(out[500 + i], res.output(0)[i], 1e-9);
    try ind.atr(set.cols[0], set.cols[1], set.cols[2], 14, out);
    // ATR warm-up is 14 + 21*14 = 308 rows; from row 500 the truncated and
    // full-history recurrences agree to ~1e-9
    for (0..300) |i| try std.testing.expectApproxEqRel(out[500 + i], res.output(4)[i], 1e-7);
    // OBV is cumulative from the window start (auto lookback 0 for obv, but
    // the batch's shared lookback applies: verify relative differences match)
    const obv_win = res.output(5);
    try ind.obv(set.cols[2][500..], set.cols[3][500..], out[0..700]);
    for (1..300) |i| {
        try std.testing.expectApproxEqAbs(out[i] - out[0], obv_win[i] - obv_win[0], 1e-6);
    }

    // explicit lookback 0: first 19 SMA rows are NaN
    const res0 = try db.indicatorsRange(start_ts, end_ts, cols, specs[0..1], 0, 0, a);
    defer res0.deinit();
    for (0..19) |i| try std.testing.expect(ind.isNan(res0.output(0)[i]));
    try std.testing.expect(!ind.isNan(res0.output(0)[19]));
}

test "indicatorsTail returns the last n rows with converged values" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("TAIL", dir, a, schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fill(&db, 800, 12);
    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.rsi), .period = 14 },
        .{ .kind = @intFromEnum(ind.Kind.bbands) },
    };
    const res = try db.indicatorsTail(5, cols, &specs, DB.lookback_auto, 0, a);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 5), res.n_rows);
    try std.testing.expectEqual(@as(usize, 6), res.n_outputs);
    try std.testing.expectEqual(@as(i64, 1_000 + 799 * 60), res.timestamps[4]);
    const set = try db.readColumns(0, 800, &[_]usize{4}, a);
    defer set.deinit();
    const out = try a.alloc(f64, 800);
    defer a.free(out);
    try ind.rsi(set.cols[0], 14, out);
    for (0..5) |i| try std.testing.expectApproxEqRel(out[795 + i], res.output(0)[i], 1e-9);
    // more rows than available: everything is returned, no error
    const all = try db.indicatorsTail(10_000, cols, specs[0..1], 0, 0, a);
    defer all.deinit();
    try std.testing.expectEqual(@as(usize, 800), all.n_rows);
}

test "field overrides run indicators on arbitrary fields and two-series kinds" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("OVR", dir, a, schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fill(&db, 300, 13);
    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.sma), .period = 10, .field_index = 5 }, // SMA of volume
        .{ .kind = @intFromEnum(ind.Kind.correl), .period = 20, .field_index = 4, .field_index2 = 2 }, // corr(close, high)
    };
    const res = try db.indicatorsTail(50, cols, &specs, DB.lookback_auto, 0, a);
    defer res.deinit();
    const set = try db.readColumns(0, 300, &[_]usize{ 2, 4, 5 }, a);
    defer set.deinit();
    const out = try a.alloc(f64, 300);
    defer a.free(out);
    try ind.sma(set.cols[2], 10, out);
    for (0..50) |i| try std.testing.expectApproxEqRel(out[250 + i], res.output(0)[i], 1e-9);
    try ind.correl(set.cols[1], set.cols[0], 20, out);
    for (0..50) |i| try std.testing.expectApproxEqRel(out[250 + i], res.output(1)[i], 1e-9);
}

test "bucket > 0 resamples ticks into bars before computing" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("BUCKET", dir, a, schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fill(&db, 1000, 14); // 60s spacing -> bucket 300 = 5 records per bar = 200 bars
    const specs = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.sma), .period = 3 }};
    // last 10 bars
    const res = try db.indicatorsTail(10, cols, &specs, DB.lookback_auto, 300, a);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 10), res.n_rows);
    // bar timestamps are bucket-aligned and 300 apart
    for (1..10) |i| try std.testing.expectEqual(@as(i64, 300), res.timestamps[i] - res.timestamps[i - 1]);
    try std.testing.expectEqual(@as(i64, 0), @rem(res.timestamps[0], 300));
    // compare with ohlcv() + kernel
    const bars = try db.ohlcv(std.math.minInt(i64), std.math.maxInt(i64), 4, 5, 300, a);
    defer bars.deinit(a);
    try std.testing.expectEqual(@as(usize, 201), bars.len()); // ts 1000 is not bucket-aligned
    const out = try a.alloc(f64, bars.len());
    defer a.free(out);
    try ind.sma(bars.close, 3, out);
    for (0..10) |i| try std.testing.expectApproxEqRel(out[191 + i], res.output(0)[i], 1e-12);
    // range mode with auto lookback: warm-up bars are trimmed to the window
    const start_ts: i64 = bars.ts[100];
    const end_ts: i64 = bars.ts[150];
    const rr = try db.indicatorsRange(start_ts, end_ts, cols, &specs, DB.lookback_auto, 300, a);
    defer rr.deinit();
    try std.testing.expectEqual(@as(usize, 50), rr.n_rows);
    try std.testing.expectEqual(start_ts, rr.timestamps[0]);
    for (0..50) |i| try std.testing.expectApproxEqRel(out[100 + i], rr.output(0)[i], 1e-12);
    // field overrides are rejected in bucket mode
    const bad = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.sma), .field_index = 5 }};
    try std.testing.expectError(error.FieldOverrideNotSupportedWithBucket, db.indicatorsTail(10, cols, &bad, 0, 300, a));
}

test "summary and snapshot over the DB" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("SNAP", dir, a, schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fill(&db, 3000, 15);
    const s = try db.summary(std.math.minInt(i64), std.math.maxInt(i64), 4, 252, a);
    try std.testing.expectEqual(@as(u64, 3000), s.count);
    try std.testing.expect(s.max_drawdown <= 0);
    try std.testing.expect(!ind.isNan(s.sharpe));
    const snap = try db.snapshot(cols, 0, 0, 252, a);
    try std.testing.expectEqual(@as(u64, 2500), snap.bars);
    try std.testing.expectEqual(@as(i64, 1_000 + 2999 * 60), snap.timestamp);
    try std.testing.expect(!ind.isNan(snap.ema_200));
    try std.testing.expect(!ind.isNan(snap.adx_14));
    try std.testing.expect(!ind.isNan(snap.mfi_14));
    try std.testing.expect(snap.rsi_14 >= 0 and snap.rsi_14 <= 100);
    // snapshot with bucket
    const snap_b = try db.snapshot(cols, 100, 300, 252, a);
    try std.testing.expectEqual(@as(u64, 100), snap_b.bars);
    try std.testing.expect(!ind.isNan(snap_b.sma_50));
    try std.testing.expect(ind.isNan(snap_b.sma_200)); // only 100 bars
    // missing close column is an error
    try std.testing.expectError(error.MissingCloseColumn, db.snapshot(.{}, 0, 0, 252, a));
}

test "readColumns follows logical order across a ring-buffer wrap" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    // capacity: header + 100 records of 48 bytes
    var db = try DB.init("WRAP", dir, a, schema, .{ .max_file_size = DB.HEADER_SIZE + 100 * 48, .overwrite_on_full = true });
    try db.initWriter();
    defer db.deinit();
    try fill(&db, 250, 16);
    try std.testing.expect(db.is_wrapped);
    try std.testing.expectEqual(@as(u64, 100), db.count());
    const set = try db.readColumns(0, 100, &[_]usize{4}, a);
    defer set.deinit();
    for (1..100) |i| try std.testing.expect(set.ts[i] > set.ts[i - 1]);
    try std.testing.expectEqual(@as(i64, 1_000 + 249 * 60), set.ts[99]);
    const specs = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.ema), .period = 5 }};
    const res = try db.indicatorsTail(3, cols, &specs, DB.lookback_auto, 0, a);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 3), res.n_rows);
    const out = try a.alloc(f64, 100);
    defer a.free(out);
    try ind.ema(set.cols[0], 5, out);
    for (0..3) |i| try std.testing.expectApproxEqRel(out[97 + i], res.output(0)[i], 1e-12);
}

test "bucket mode with session gaps: tails count existing bars, windows select bars by timestamp" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("GAPS", dir, a, schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    // 3 "sessions" of 100 minutes (1 record / 30 s) separated by 1-day gaps,
    // starting on a minute boundary so that each session is exactly 100 bars
    var ts: i64 = 1_020;
    var p: f64 = 100.0;
    var i: usize = 0;
    while (i < 3 * 200) : (i += 1) {
        if (i > 0 and i % 200 == 0) ts += 86_400;
        p += if (i % 3 == 0) 0.5 else -0.2;
        const bar = Bar{ .timestamp = ts, .open = p, .high = p + 0.1, .low = p - 0.1, .close = p, .volume = 1 };
        try db.append(std.mem.asBytes(&bar));
        ts += 30;
    }
    try db.flush();
    const specs = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.sma), .period = 3 }};
    // 300 one-minute bars exist in total (100 per session)
    const bars = try db.ohlcv(std.math.minInt(i64), std.math.maxInt(i64), 4, 5, 60, a);
    defer bars.deinit(a);
    try std.testing.expectEqual(@as(usize, 300), bars.len());
    // tail(150) must return 150 existing bars although they span two sessions (> 1 day)
    const tail = try db.indicatorsTail(150, cols, &specs, DB.lookback_auto, 60, a);
    defer tail.deinit();
    try std.testing.expectEqual(@as(usize, 150), tail.n_rows);
    try std.testing.expectEqual(bars.ts[150], tail.timestamps[0]);
    try std.testing.expectEqual(bars.ts[299], tail.timestamps[149]);
    for (tail.output(0)) |v| try std.testing.expect(!ind.isNan(v));
    // more bars than exist: everything
    const all = try db.indicatorsTail(1000, cols, &specs, 0, 60, a);
    defer all.deinit();
    try std.testing.expectEqual(@as(usize, 300), all.n_rows);
    // a window ending 1 timestamp unit after the last bar's start includes the whole last bar
    const rr = try db.indicatorsRange(bars.ts[250], bars.ts[299] + 1, cols, &specs, DB.lookback_auto, 60, a);
    defer rr.deinit();
    try std.testing.expectEqual(@as(usize, 50), rr.n_rows);
    const out = try a.alloc(f64, bars.len());
    defer a.free(out);
    try ind.sma(bars.close, 3, out);
    for (0..50) |k| try std.testing.expectApproxEqRel(out[250 + k], rr.output(0)[k], 1e-12);
    // a window starting in the middle of a bar excludes that bar
    const mid = try db.indicatorsRange(bars.ts[10] + 1, bars.ts[20] + 1, cols, &specs, 0, 60, a);
    defer mid.deinit();
    try std.testing.expectEqual(@as(usize, 10), mid.n_rows);
    try std.testing.expectEqual(bars.ts[11], mid.timestamps[0]);
    // the window that starts right after a gap still warms up from the previous session
    const after_gap = try db.indicatorsRange(bars.ts[200], bars.ts[205], cols, &specs, DB.lookback_auto, 60, a);
    defer after_gap.deinit();
    try std.testing.expectEqual(@as(usize, 5), after_gap.n_rows);
    for (0..5) |k| try std.testing.expectApproxEqRel(out[200 + k], after_gap.output(0)[k], 1e-12);
    // snapshot over the last 150 existing bars
    const snap = try db.snapshot(cols, 150, 60, 0, a);
    try std.testing.expectEqual(@as(u64, 150), snap.bars);
    try std.testing.expectEqual(bars.ts[299], snap.timestamp);
}

/// A 41-byte tick record (i64 + 4 x f64 + bool) laid out like the schema.
fn tickBytes(timestamp: i64, price: f64, size: f64, bid: f64, ask: f64, side: bool) [41]u8 {
    var b: [41]u8 = undefined;
    @memcpy(b[0..8], std.mem.asBytes(&timestamp));
    @memcpy(b[8..16], std.mem.asBytes(&price));
    @memcpy(b[16..24], std.mem.asBytes(&size));
    @memcpy(b[24..32], std.mem.asBytes(&bid));
    @memcpy(b[32..40], std.mem.asBytes(&ask));
    b[40] = if (side) 1 else 0;
    return b;
}

const tick_schema = hocdb.Schema{ .fields = &[_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "price", .type = .f64 },
    .{ .name = "size", .type = .f64 },
    .{ .name = "bid", .type = .f64 },
    .{ .name = "ask", .type = .f64 },
    .{ .name = "side", .type = .bool },
} };

const tick_cols = DB.IndicatorColumns{ .close = 1, .volume = 2, .bid = 3, .ask = 4, .side = 5 };

fn fillTicks(db: *DB, n: usize, t0: i64, step: i64, seed: u64) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();
    var p: f64 = 100.0;
    for (0..n) |i| {
        p *= @exp(r.floatNorm(f64) * 0.002);
        const rec = tickBytes(t0 + @as(i64, @intCast(i)) * step, p, 1.0 + @as(f64, @floatFromInt(i % 5)), p * 0.999, p * 1.001, (i % 3 != 0));
        try db.append(&rec);
    }
    try db.flush();
}

test "microstructure kinds on ticks and bars, ohlcv with buy volume" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("MICRO", dir, a, tick_schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fillTicks(&db, 600, 1_000_000, 1_000_000, 21); // 1 tick / second, microseconds
    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.spread) },
        .{ .kind = @intFromEnum(ind.Kind.order_flow), .period = 3 },
        .{ .kind = @intFromEnum(ind.Kind.trade_intensity), .period = 10 },
        .{ .kind = @intFromEnum(ind.Kind.tick_pressure), .period = 5 },
    };
    const res = try db.indicatorsTail(20, tick_cols, &specs, DB.lookback_auto, 0, a);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 7), res.n_outputs);
    for (res.output(1)) |v| try std.testing.expectApproxEqRel(20.0, v, 1e-9); // 0.2% spread = 20 bps
    // order flow: sides 1,1,0 repeating with sizes 1..5 -> compare with direct computation
    const set = try db.readColumns(0, 600, &[_]usize{ 2, 5 }, a);
    defer set.deinit();
    const net = try a.alloc(f64, 600);
    defer a.free(net);
    const imb = try a.alloc(f64, 600);
    defer a.free(imb);
    try ind.orderFlow(set.cols[0], set.cols[1], null, 3, net, imb, a);
    for (0..20) |i| try std.testing.expectApproxEqRel(net[580 + i], res.output(2)[i], 1e-12);
    for (res.output(4)) |v| try std.testing.expectApproxEqRel(1.0, v, 1e-9); // 1 trade per second
    // bars of 10 seconds carry buy volume; order_flow works on bars
    const bar_specs = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.order_flow), .period = 1 }};
    const bres = try db.indicatorsTail(5, tick_cols, &bar_specs, 0, 10_000_000, a);
    defer bres.deinit();
    try std.testing.expectEqual(@as(usize, 5), bres.n_rows);
    const bars = try db.ohlcvSide(std.math.minInt(i64), std.math.maxInt(i64), 1, 2, 5, 10_000_000, a);
    defer bars.deinit(a);
    try std.testing.expectEqual(@as(usize, 61), bars.len()); // ticks at 1..600 s span 61 ten-second buckets
    for (0..5) |i| {
        const b = bars.len() - 5 + i;
        try std.testing.expectApproxEqRel(2.0 * bars.buy_volume[b] - bars.volume[b], bres.output(0)[i], 1e-12);
    }
    // without a side column bars have NaN buy volume and order_flow is rejected
    const nb = try db.ohlcv(std.math.minInt(i64), std.math.maxInt(i64), 1, 2, 10_000_000, a);
    defer nb.deinit(a);
    try std.testing.expect(ind.isNan(nb.buy_volume[0]));
    const no_side = DB.IndicatorColumns{ .close = 1, .volume = 2 };
    try std.testing.expectError(error.MissingColumn, db.indicatorsTail(5, no_side, &bar_specs, 0, 10_000_000, a));
}

test "session-anchored kinds see the session start even when the window starts later" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("SESS", dir, a, tick_schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fillTicks(&db, 1000, 0, 1_000, 22); // ms ticks; sessions of 100 s = 100_000 units -> 100 ticks each
    // tick mode: session VWAP and range
    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.session_vwap), .param = 100_000 },
        .{ .kind = @intFromEnum(ind.Kind.session_range), .param = 100_000 },
    };
    // window in the middle of session 5 (ticks 500..599): rows 550..559
    const win = try db.indicatorsRange(550_000, 560_000, tick_cols, &specs, 0, 0, a);
    defer win.deinit();
    try std.testing.expectEqual(@as(usize, 10), win.n_rows);
    const full = try db.indicatorsRange(std.math.minInt(i64), std.math.maxInt(i64), tick_cols, &specs, 0, 0, a);
    defer full.deinit();
    for (0..10) |i| {
        try std.testing.expectApproxEqRel(full.output(0)[550 + i], win.output(0)[i], 1e-12);
        try std.testing.expectApproxEqRel(full.output(1)[550 + i], win.output(1)[i], 1e-12); // session open
        try std.testing.expectApproxEqRel(full.output(4)[550 + i], win.output(4)[i], 1e-12); // session return
    }
    const tail = try db.indicatorsTail(10, tick_cols, &specs, 0, 0, a);
    defer tail.deinit();
    for (0..10) |i| try std.testing.expectApproxEqRel(full.output(0)[990 + i], tail.output(0)[i], 1e-12);
    // bar mode (10 s bars, 10 per session): pivots need the previous session, opening range the session start
    const bar_specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.pivots), .param = 100_000 },
        .{ .kind = @intFromEnum(ind.Kind.opening_range), .period = 5, .param = 100_000 },
    };
    const bfull = try db.indicatorsRange(std.math.minInt(i64), std.math.maxInt(i64), tick_cols, &bar_specs, 0, 10_000, a);
    defer bfull.deinit();
    try std.testing.expectEqual(@as(usize, 100), bfull.n_rows);
    const bwin = try db.indicatorsRange(550_000, 600_000, tick_cols, &bar_specs, 0, 10_000, a);
    defer bwin.deinit();
    try std.testing.expectEqual(@as(usize, 5), bwin.n_rows);
    for (0..5) |i| {
        try std.testing.expectApproxEqRel(bfull.output(0)[55 + i], bwin.output(0)[i], 1e-12); // pp
        try std.testing.expectApproxEqRel(bfull.output(5)[55 + i], bwin.output(5)[i], 1e-12); // opening-range high
        try std.testing.expectEqual(bfull.output(7)[55 + i], bwin.output(7)[i]); // breakout flag
    }
    try std.testing.expect(ind.isNan(bfull.output(0)[5])); // first session has no previous session
    try std.testing.expect(!ind.isNan(bfull.output(0)[15]));
    // missing session length is rejected
    const bad = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.session_vwap) }};
    try std.testing.expectError(error.CalendarRequired, db.indicatorsTail(10, tick_cols, &bad, 0, 0, a)); // param 0 = calendar sessions, none configured
}

test "pair indicators: as-of join on ticks and inner join on bars" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var dba = try DB.init("PAIR_A", dir, a, tick_schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try dba.initWriter();
    defer dba.deinit();
    var dbb = try DB.init("PAIR_B", dir, a, tick_schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try dbb.initWriter();
    defer dbb.deinit();
    try fillTicks(&dba, 500, 0, 1_000, 31); // A every 1 s (ms units)
    try fillTicks(&dbb, 200, 300, 2_500, 32); // B every 2.5 s starting at 0.3 s
    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.series) },
        .{ .kind = @intFromEnum(ind.Kind.series2) },
        .{ .kind = @intFromEnum(ind.Kind.ratio) },
        .{ .kind = @intFromEnum(ind.Kind.correl), .period = 20 },
    };
    // tick mode: every A row, B as-of
    const r = try dba.pairRange(&dbb, tick_cols, tick_cols, 100_000, 110_000, &specs, DB.lookback_auto, 0, a);
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 10), r.n_rows);
    const ca = try dba.readColumns(0, 500, &[_]usize{1}, a);
    defer ca.deinit();
    const cb = try dbb.readColumns(0, 200, &[_]usize{1}, a);
    defer cb.deinit();
    for (0..10) |i| {
        const ta = r.timestamps[i];
        try std.testing.expectEqual(ca.cols[0][100 + i], r.output(0)[i]);
        // latest B at or before ta
        var j: usize = 0;
        while (j < 200 and cb.ts[j] <= ta) j += 1;
        try std.testing.expectEqual(cb.cols[0][j - 1], r.output(1)[i]);
        try std.testing.expectApproxEqRel(r.output(0)[i] / r.output(1)[i], r.output(2)[i], 1e-12);
        try std.testing.expect(!ind.isNan(r.output(3)[i]));
    }
    // bar mode (10 s bars): inner join keeps bars both have; B only exists for the first 500 s
    const rb = try dba.pairTail(&dbb, tick_cols, tick_cols, 50, &specs, 0, 10_000, a);
    defer rb.deinit();
    try std.testing.expectEqual(@as(usize, 50), rb.n_rows);
    for (0..50) |i| try std.testing.expectEqual(@as(i64, 0), @rem(rb.timestamps[i], 10_000));
    const bars_a = try dba.ohlcv(std.math.minInt(i64), std.math.maxInt(i64), 1, 2, 10_000, a);
    defer bars_a.deinit(a);
    const bars_b = try dbb.ohlcv(std.math.minInt(i64), std.math.maxInt(i64), 1, 2, 10_000, a);
    defer bars_b.deinit(a);
    try std.testing.expectEqual(bars_a.ts[bars_a.len() - 1], rb.timestamps[49]);
    try std.testing.expectEqual(bars_a.close[bars_a.len() - 1], rb.output(0)[49]);
    try std.testing.expectEqual(bars_b.close[bars_b.len() - 1], rb.output(1)[49]);
}

test "snapshotMulti equals individual snapshots; health; evaluate" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("MULTI", dir, a, tick_schema, .{ .max_file_size = 16 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    try fillTicks(&db, 20_000, 0, 1_000_000, 41); // 1 tick / s for ~5.5 h
    const buckets = [_]i64{ 60_000_000, 300_000_000 };
    const ppy = [_]f64{ 525_600, 105_120 };
    var outs: [2]ind.Snapshot = undefined;
    try db.snapshotMulti(tick_cols, 50, &buckets, &ppy, &outs, a);
    const s1 = try db.snapshot(tick_cols, 50, 60_000_000, 525_600, a);
    const s5 = try db.snapshot(tick_cols, 50, 300_000_000, 105_120, a);
    try std.testing.expectEqual(s1.timestamp, outs[0].timestamp);
    try std.testing.expectEqual(s5.timestamp, outs[1].timestamp);
    try std.testing.expectEqual(s1.rsi_14, outs[0].rsi_14);
    try std.testing.expectEqual(s5.ema_21, outs[1].ema_21);
    try std.testing.expectEqual(s5.obv, outs[1].obv);
    try std.testing.expectEqual(@as(u64, 50), outs[1].bars);
    const h = try db.health(std.math.minInt(i64), std.math.maxInt(i64), 1, 2, 5_000_000, 0.05, a);
    try std.testing.expectEqual(@as(u64, 20_000), h.count);
    try std.testing.expectEqual(@as(u64, 0), h.n_gaps);
    try std.testing.expectEqual(@as(f64, 1_000_000), h.median_gap);
    try std.testing.expectEqual(@as(u64, 0), h.n_outlier_returns);
    const d = [_]ind.Decision{
        .{ .timestamp = 1_000_000_000, .direction = 1, .size = 100, .horizon = 60_000_000 },
        .{ .timestamp = 2_000_000_000, .direction = -1, .size = 100, .horizon = 0 },
        .{ .timestamp = 19_990_000_000, .direction = 1, .size = 100, .horizon = 60_000_000 }, // exit past the data
    };
    var entry: [3]f64 = undefined;
    var exit: [3]f64 = undefined;
    var net: [3]f64 = undefined;
    const e = try db.evaluate(1, &d, 120_000_000, 5, &entry, &exit, &net, a);
    try std.testing.expectEqual(@as(u64, 2), e.n_evaluated);
    const set = try db.readColumns(0, 20_000, &[_]usize{1}, a);
    defer set.deinit();
    try std.testing.expectEqual(set.cols[0][1000], entry[0]);
    try std.testing.expectEqual(set.cols[0][1060], exit[0]);
    try std.testing.expectEqual(set.cols[0][2000], entry[1]);
    try std.testing.expectEqual(set.cols[0][2120], exit[1]);
    try std.testing.expect(ind.isNan(net[2]));
    try std.testing.expectApproxEqRel(set.cols[0][1060] / set.cols[0][1000] - 1.0 - 0.001, net[0], 1e-12);
}

test "pivots in a window use the previous session that has data (weekend gaps)" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    var db = try DB.init("WEEKEND", dir, a, tick_schema, .{ .max_file_size = 8 * 1024 * 1024 });
    try db.initWriter();
    defer db.deinit();
    // sessions of 1000 units; data only in sessions 0, 1, 4, 5 (sessions 2 and 3 are a "weekend")
    var prng = std.Random.DefaultPrng.init(7);
    const r = prng.random();
    var p: f64 = 100.0;
    for ([_]i64{ 0, 1, 4, 5 }) |sess| {
        var k: i64 = 0;
        while (k < 100) : (k += 1) {
            p *= @exp(r.floatNorm(f64) * 0.003);
            const rec = tickBytes(sess * 1000 + 100 + k * 5, p, 1, p * 0.999, p * 1.001, true);
            try db.append(&rec);
        }
    }
    try db.flush();
    const specs = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.pivots), .param = 1000 }};
    const full = try db.indicatorsRange(std.math.minInt(i64), std.math.maxInt(i64), tick_cols, &specs, 0, 10, a);
    defer full.deinit();
    // bars: 50 per session -> session 4 occupies rows 100..149; its pivots come from session 1
    try std.testing.expectEqual(@as(usize, 200), full.n_rows);
    try std.testing.expect(!ind.isNan(full.output(0)[120]));
    // a window inside session 4 (with a lookback that lands in the middle of session 1)
    const win = try db.indicatorsRange(4_300, 4_400, tick_cols, &specs, 20, 10, a);
    defer win.deinit();
    try std.testing.expectEqual(@as(usize, 10), win.n_rows);
    for (0..10) |i| try std.testing.expectEqual(full.output(0)[120 + i], win.output(0)[i]);
    // and a tail inside session 5 with a short lookback
    const tail = try db.indicatorsTail(10, tick_cols, &specs, 3, 10, a);
    defer tail.deinit();
    for (0..10) |i| try std.testing.expectEqual(full.output(0)[190 + i], tail.output(0)[i]);
}
