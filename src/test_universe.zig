//! Tests for the cross-sectional universe kernel (src/universe.zig).
//! Run standalone: `zig test src/test_universe.zig`.
//!
//! The "tied example" reproduces, bit for bit, the deterministic data set of
//! scripts/stress/references_universe.py and asserts the numbers that the
//! pandas reference asserts in its own self-test.
const std = @import("std");
const math = std.math;
const uni = @import("universe.zig");
const ind = @import("indicators.zig");

const nan = uni.nan;
const Row = uni.Row;
const Params = uni.Params;

fn expectNear(want: f64, got: f64, tol: f64) !void {
    if (math.isNan(want)) {
        if (!math.isNan(got)) {
            std.debug.print("expected NaN, got {d}\n", .{got});
            return error.TestExpectedNan;
        }
        return;
    }
    if (math.isNan(got)) {
        std.debug.print("expected {d}, got NaN\n", .{want});
        return error.TestUnexpectedNan;
    }
    try std.testing.expectApproxEqAbs(want, got, tol);
}

fn expectNanRow(row: Row) !void {
    inline for (@typeInfo(Row).@"struct".fields) |f| {
        if (f.type == f64 and !std.mem.eql(u8, f.name, "last_close")) {
            if (!math.isNan(@field(row, f.name))) {
                std.debug.print("field {s} = {d}, expected NaN\n", .{ f.name, @field(row, f.name) });
                return error.TestExpectedNan;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tied example: identical formula to references_universe.py::tied_example
// ---------------------------------------------------------------------------

const tied_n: usize = 80;
const tied_m: usize = 4;

const TiedData = struct {
    closes: [tied_m][tied_n]f64,
    volumes: [tied_m][tied_n]f64,

    fn closeSlices(self: *const TiedData) [tied_m][]const f64 {
        var out: [tied_m][]const f64 = undefined;
        for (0..tied_m) |i| out[i] = &self.closes[i];
        return out;
    }

    fn volumeSlices(self: *const TiedData) [tied_m][]const f64 {
        var out: [tied_m][]const f64 = undefined;
        for (0..tied_m) |i| out[i] = &self.volumes[i];
        return out;
    }
};

fn tiedExample(nan_ticker: ?usize, nan_bars: usize) TiedData {
    var d: TiedData = undefined;
    for (0..tied_m) |i| {
        const fi1: f64 = @floatFromInt(i + 1);
        var c: f64 = 100.0 + 10.0 * @as(f64, @floatFromInt(i));
        for (0..tied_n) |t| {
            if (t > 0) {
                const k1: i64 = @as(i64, @intCast((7 * t + 3 * i) % 11)) - 5;
                const k2: i64 = @as(i64, @intCast((5 * t + i) % 7)) - 3;
                const k1f: f64 = @floatFromInt(k1);
                const k2f: f64 = @floatFromInt(k2);
                const dr = 0.0005 * fi1 + 0.004 * k1f + 0.002 * fi1 * k2f;
                c = c * (1.0 + dr);
            }
            d.closes[i][t] = c;
            d.volumes[i][t] = 1000.0 * fi1 + 100.0 * @as(f64, @floatFromInt((11 * t + 5 * i) % 13));
        }
    }
    if (nan_ticker) |i| {
        for (0..nan_bars) |t| {
            d.closes[i][t] = nan;
            d.volumes[i][t] = nan;
        }
    }
    return d;
}

const tied_params = Params{
    .mom_short = 5,
    .mom_mid = 20,
    .mom_long = 60,
    .vol_period = 20,
    .corr_period = 60,
    .sma_period = 50,
    .beta_period = 60,
    .periods_per_year = 252,
    .weights_mode = 0,
};

test "tied example A matches the pandas reference (4 tickers x 80 bars)" {
    const a = std.testing.allocator;
    const d = tied_example_a;
    const closes = d.closeSlices();
    const volumes = d.volumeSlices();
    var rows: [tied_m]Row = undefined;
    var corr: [tied_m * tied_m]f64 = undefined;
    const s = try uni.compute(&closes, &volumes, null, tied_params, &rows, &corr, a);
    const tol = 1e-9;
    try std.testing.expectEqual(@as(u64, 4), s.n_tickers);
    try std.testing.expectEqual(@as(u64, 80), s.n_bars);
    try expectNear(-0.011983426780339701, rows[0].mom_short, tol);
    try expectNear(0.21879646344264161, rows[0].vol, tol);
    try expectNear(0.24304610360079246, rows[0].beta, tol);
    try expectNear(0.1158486037889159, rows[0].corr_market, tol);
    try expectNear(-1.4987602243609328, rows[0].z_mom_mid, tol);
    try expectNear(0.21249457123319324, rows[0].idio_vol, tol);
    try expectNear(1.316614420062696, rows[0].volume_ratio, tol);
    try expectNear(0.038977353544358884, rows[1].sma_distance, tol);
    try expectNear(-0.0012511260506085087, rows[1].rel_strength, tol);
    try expectNear(0.10044578231065526, rows[2].mom_long, tol);
    try expectNear(0.6666666666666666, rows[2].rank_vol, tol);
    try expectNear(-0.12292024264300071, rows[2].avg_corr, tol);
    try expectNear(1.9468547041468531, rows[3].beta, tol);
    try expectNear(0.05397517554593363, rows[3].max_corr, tol);
    try std.testing.expectEqual(@as(u64, 2), rows[3].max_corr_index);
    try expectNear(0.019707995246545984, s.market_mom_mid, tol);
    try expectNear(0.10920164503958005, s.market_vol, tol);
    try expectNear(0.013107726728918344, s.dispersion, tol);
    try expectNear(-0.14392888331800396, s.avg_pair_corr, tol);
    try expectNear(-0.395160397997002, s.min_pair_corr, tol);
    try expectNear(-0.395160397997002, corr[0 * tied_m + 2], tol);
    try expectNear(-0.27085962531368346, corr[1 * tied_m + 3], tol);
    // a few more structural checks on the same run
    try expectNear(0.75, s.breadth_sma, 0);
    try expectNear(0.75, s.breadth_up, 0);
    try expectNear(0.0, rows[0].rank_mom_short, 0);
    try expectNear(1.0, rows[2].rank_mom_mid, 0);
    for (0..tied_m) |i| try expectNear(1.0, corr[i * tied_m + i], 0);
    try std.testing.expectEqual(@as(i64, 0), s.first_ts);
    try std.testing.expectEqual(@as(i64, 0), s.last_ts);
}

test "tied example B (late listing + volume weights) matches the pandas reference" {
    const a = std.testing.allocator;
    const d = tied_example_b;
    const closes = d.closeSlices();
    const volumes = d.volumeSlices();
    var rows: [tied_m]Row = undefined;
    var corr: [tied_m * tied_m]f64 = undefined;
    var p = tied_params;
    p.weights_mode = 1;
    const s = try uni.compute(&closes, &volumes, null, p, &rows, &corr, a);
    const tol = 1e-9;
    try expectNear(-0.30791567284597654, rows[0].beta, tol);
    try expectNear(0.5, rows[1].rank_mom_long, tol);
    try expectNear(0.5239365729764366, rows[2].corr_market, tol);
    try expectNear(nan, rows[3].mom_long, tol);
    try expectNear(nan, rows[3].beta, tol);
    try expectNear(nan, rows[3].idio_vol, tol);
    try expectNear(nan, rows[3].rank_mom_long, tol);
    try expectNear(0.7900585420715146, rows[3].corr_market, tol);
    try expectNear(0.04738841868195653, rows[3].sma_distance, tol);
    try expectNear(-0.13675903096642864, rows[3].avg_corr, tol);
    try expectNear(0.013203055652688502, s.market_ret_1, tol);
    try expectNear(0.08603224010778687, s.market_mom_long, tol);
    try expectNear(0.13885650431334112, s.market_vol, tol);
    try expectNear(-0.15506255454506374, corr[0 * tied_m + 3], tol);
    try expectNear(-0.15506255454506374, corr[3 * tied_m + 0], tol);
    // ticker 3 has 49 finite returns: pairwise correlation is still defined
    try std.testing.expect(math.isFinite(corr[1 * tied_m + 3]));
}

const tied_example_a = tiedExample(null, 0);
const tied_example_b = tiedExample(3, 30);

// ---------------------------------------------------------------------------
// rank()
// ---------------------------------------------------------------------------

fn naiveRank(values: []const f64, out: []f64) void {
    var m: usize = 0;
    for (values) |v| {
        if (!math.isNan(v)) m += 1;
    }
    for (values, out) |v, *o| {
        if (math.isNan(v)) {
            o.* = nan;
            continue;
        }
        if (m == 1) {
            o.* = 0.5;
            continue;
        }
        var less: f64 = 0;
        var equal: f64 = 0;
        for (values) |u| {
            if (u < v) less += 1;
            if (u == v) equal += 1;
        }
        const avg = less + (equal + 1) / 2;
        o.* = (avg - 1) / @as(f64, @floatFromInt(m - 1));
    }
}

test "rank: ties, NaNs, single element, all equal, empty" {
    var out: [8]f64 = undefined;
    // ties + NaN (same case as the python self-test)
    uni.rank(&[_]f64{ 3, 1, 2, nan, 1 }, out[0..5]);
    try expectNear(1.0, out[0], 1e-15);
    try expectNear(1.0 / 6.0, out[1], 1e-15);
    try expectNear(2.0 / 3.0, out[2], 1e-15);
    try expectNear(nan, out[3], 0);
    try expectNear(1.0 / 6.0, out[4], 1e-15);
    // strictly increasing: 0, .25, .5, .75, 1
    uni.rank(&[_]f64{ -2, -1, 0, 1, 2 }, out[0..5]);
    for (0..5) |i| try expectNear(@as(f64, @floatFromInt(i)) * 0.25, out[i], 0);
    // single element
    uni.rank(&[_]f64{7}, out[0..1]);
    try expectNear(0.5, out[0], 0);
    // single finite element among NaNs
    uni.rank(&[_]f64{ nan, 7, nan }, out[0..3]);
    try expectNear(nan, out[0], 0);
    try expectNear(0.5, out[1], 0);
    try expectNear(nan, out[2], 0);
    // all equal
    uni.rank(&[_]f64{ 2, 2, 2, 2 }, out[0..4]);
    for (0..4) |i| try expectNear(0.5, out[i], 0);
    // all NaN
    uni.rank(&[_]f64{ nan, nan }, out[0..2]);
    try expectNear(nan, out[0], 0);
    try expectNear(nan, out[1], 0);
    // two elements: 0 and 1
    uni.rank(&[_]f64{ 5, 4 }, out[0..2]);
    try expectNear(1, out[0], 0);
    try expectNear(0, out[1], 0);
    // +-inf are ordinary (comparable) values
    uni.rank(&[_]f64{ math.inf(f64), 0, -math.inf(f64) }, out[0..3]);
    try expectNear(1, out[0], 0);
    try expectNear(0.5, out[1], 0);
    try expectNear(0, out[2], 0);
    // empty
    uni.rank(&[_]f64{}, out[0..0]);
}

test "rank: SIMD path equals the naive O(n^2) reference on longer inputs" {
    var prng = std.Random.DefaultPrng.init(11);
    const r = prng.random();
    const n = 3 * uni.lanes + 5;
    var values: [64]f64 = undefined;
    var got: [64]f64 = undefined;
    var want: [64]f64 = undefined;
    for (values[0..n]) |*v| {
        const k = r.intRangeAtMost(u8, 0, 9);
        v.* = if (k == 0) nan else @as(f64, @floatFromInt(k)); // many ties
    }
    uni.rank(values[0..n], got[0..n]);
    naiveRank(values[0..n], want[0..n]);
    for (0..n) |i| try expectNear(want[i], got[i], 1e-15);
    // pure percentiles: 0 and 1 occur, everything within [0, 1]
    for (0..n) |i| {
        if (!math.isNan(got[i])) try std.testing.expect(got[i] >= 0 and got[i] <= 1);
    }
}

// ---------------------------------------------------------------------------
// Synthetic helpers
// ---------------------------------------------------------------------------

fn randomWalk(allocator: std.mem.Allocator, n: usize, seed: u64, sigma: f64) ![]f64 {
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();
    const x = try allocator.alloc(f64, n);
    var p: f64 = 100.0;
    for (x) |*v| {
        p *= 1 + r.floatNorm(f64) * sigma + 0.0002;
        v.* = p;
    }
    return x;
}

test "two identical tickers: corr 1, beta 1, rel_strength 0, tie ranks" {
    const a = std.testing.allocator;
    const x = try randomWalk(a, 100, 5, 0.01);
    defer a.free(x);
    const closes = [_][]const f64{ x, x };
    var rows: [2]Row = undefined;
    var corr: [4]f64 = undefined;
    const p = Params{ .periods_per_year = 252 };
    const s = try uni.compute(&closes, null, null, p, &rows, &corr, a);
    try expectNear(1, corr[1], 1e-12);
    try expectNear(1, corr[2], 1e-12);
    try expectNear(1, corr[0], 0);
    try expectNear(1, corr[3], 0);
    for (rows, 0..) |row, i| {
        try expectNear(1, row.beta, 1e-12);
        try expectNear(1, row.corr_market, 1e-12);
        try expectNear(0, row.rel_strength, 1e-12);
        try expectNear(0, row.idio_vol, 1e-12);
        try expectNear(1, row.max_corr, 1e-12);
        try expectNear(1, row.avg_corr, 1e-12);
        try std.testing.expectEqual(@as(u64, 1 - i), row.max_corr_index);
        try expectNear(0.5, row.rank_mom_short, 0);
        try expectNear(0.5, row.rank_mom_mid, 0);
        try expectNear(0.5, row.rank_mom_long, 0);
        try expectNear(0.5, row.rank_vol, 0);
        try expectNear(0.5, row.rank_rel_strength, 0);
        try expectNear(nan, row.z_mom_mid, 0); // zero cross-sectional std
        try expectNear(nan, row.volume_ratio, 0); // no volumes
        try expectNear(x[99] / x[98] - 1, row.ret_1, 1e-15);
        try expectNear(x[99] / x[79] - 1, row.mom_mid, 1e-15);
        try expectNear(x[99] / x[39] - 1, row.mom_long, 1e-15);
        try expectNear(x[99], row.last_close, 0);
    }
    // the market IS the ticker
    try expectNear(rows[0].ret_1, s.market_ret_1, 1e-15);
    try expectNear(rows[0].mom_mid, s.market_mom_mid, 1e-12);
    try expectNear(rows[0].vol, s.market_vol, 1e-12);
    try expectNear(0, s.dispersion, 0);
    try expectNear(0, s.dispersion_mid, 0);
    try expectNear(1, s.avg_pair_corr, 1e-12);
    try expectNear(1, s.max_pair_corr, 1e-12);
    try expectNear(1, s.min_pair_corr, 1e-12);
}

test "a ticker mirroring the market has beta 1 and idio_vol 0" {
    const a = std.testing.allocator;
    const n: usize = 120;
    const x = try randomWalk(a, n, 21, 0.02);
    defer a.free(x);
    const y = try randomWalk(a, n, 22, 0.015);
    defer a.free(y);
    // z's return is the mean of x's and y's -> the equal-weight market of
    // {x, y, z} equals z's return at every bar
    const z = try a.alloc(f64, n);
    defer a.free(z);
    z[0] = 50;
    for (1..n) |t| {
        const rx = x[t] / x[t - 1] - 1;
        const ry = y[t] / y[t - 1] - 1;
        z[t] = z[t - 1] * (1 + (rx + ry) / 2);
    }
    const closes = [_][]const f64{ x, y, z };
    var rows: [3]Row = undefined;
    const s = try uni.compute(&closes, null, null, .{ .periods_per_year = 252 }, &rows, null, a);
    try expectNear(1, rows[2].beta, 1e-10);
    try expectNear(0, rows[2].idio_vol, 1e-10);
    try expectNear(1, rows[2].corr_market, 1e-10);
    try expectNear(0, rows[2].rel_strength, 1e-10);
    try expectNear(rows[2].ret_1, s.market_ret_1, 1e-14);
    try expectNear(rows[2].mom_mid, s.market_mom_mid, 1e-12);
    try expectNear(rows[2].mom_long, s.market_mom_long, 1e-12);
    try expectNear(rows[2].vol, s.market_vol, 1e-10);
    // betas average to 1 under equal weights
    try expectNear(3, rows[0].beta + rows[1].beta + rows[2].beta, 1e-10);
    // idiosyncratic vol never exceeds total vol
    for (rows) |row| try std.testing.expect(row.idio_vol <= row.vol + 1e-12);
    // rows without a matrix buffer carry the same correlation aggregates
    var rows2: [3]Row = undefined;
    var corr: [9]f64 = undefined;
    _ = try uni.compute(&closes, null, null, .{ .periods_per_year = 252 }, &rows2, &corr, a);
    for (rows, rows2) |r1, r2| {
        inline for (@typeInfo(Row).@"struct".fields) |f| {
            if (f.type == f64) {
                try expectNear(@field(r1, f.name), @field(r2, f.name), 0);
            } else {
                try std.testing.expectEqual(@field(r1, f.name), @field(r2, f.name));
            }
        }
    }
}

test "fewer bars than the periods: NaN features, no crash, exclusion" {
    const a = std.testing.allocator;
    const x = try randomWalk(a, 10, 31, 0.01);
    defer a.free(x);
    const y = try randomWalk(a, 10, 32, 0.01);
    defer a.free(y);
    const z = try randomWalk(a, 10, 33, 0.01);
    defer a.free(z);
    const closes = [_][]const f64{ x, y, z };
    var rows: [3]Row = undefined;
    var corr: [9]f64 = undefined;
    const s = try uni.compute(&closes, null, null, .{}, &rows, &corr, a); // defaults: 5/20/60, vol 20, corr 60, sma 50, beta 60
    try std.testing.expectEqual(@as(u64, 10), s.n_bars);
    for (rows, 0..) |row, i| {
        try std.testing.expect(math.isFinite(row.ret_1));
        try std.testing.expect(math.isFinite(row.mom_short));
        try std.testing.expect(math.isFinite(row.rank_mom_short));
        try expectNear(nan, row.mom_mid, 0);
        try expectNear(nan, row.mom_long, 0);
        try expectNear(nan, row.vol, 0);
        try expectNear(nan, row.sma_distance, 0);
        try expectNear(nan, row.beta, 0);
        try expectNear(nan, row.idio_vol, 0);
        try expectNear(nan, row.rel_strength, 0);
        try expectNear(nan, row.rank_mom_mid, 0);
        try expectNear(nan, row.rank_mom_long, 0);
        try expectNear(nan, row.rank_vol, 0);
        try expectNear(nan, row.rank_rel_strength, 0);
        try expectNear(nan, row.z_mom_mid, 0);
        // correlations are pairwise over the 9 available returns
        try std.testing.expect(math.isFinite(row.corr_market));
        try std.testing.expect(math.isFinite(row.avg_corr));
        try std.testing.expect(math.isFinite(row.max_corr));
        try std.testing.expect(row.max_corr_index != i);
    }
    try std.testing.expect(math.isFinite(s.market_ret_1));
    try std.testing.expect(math.isFinite(s.market_mom_short));
    try expectNear(nan, s.market_mom_mid, 0);
    try expectNear(nan, s.market_mom_long, 0);
    try expectNear(nan, s.market_vol, 0);
    try expectNear(nan, s.dispersion_mid, 0);
    try expectNear(nan, s.breadth_sma, 0);
    try std.testing.expect(math.isFinite(s.dispersion));
    try std.testing.expect(math.isFinite(s.breadth_up));
    try std.testing.expect(math.isFinite(s.avg_pair_corr));

    // a ticker with too few finite bars is excluded from the market, the
    // ranks and the cross-sectional moments
    const w = try randomWalk(a, 100, 34, 0.01);
    defer a.free(w);
    const v = try randomWalk(a, 100, 35, 0.01);
    defer a.free(v);
    const u = try a.alloc(f64, 100);
    defer a.free(u);
    ind.fillNan(u);
    u[99] = 12.5; // a single finite close: not even ret_1
    const closes2 = [_][]const f64{ w, v, u };
    var rows2: [3]Row = undefined;
    var rows_ref: [2]Row = undefined;
    const s2 = try uni.compute(&closes2, null, null, .{ .periods_per_year = 252 }, &rows2, null, a);
    const s_ref = try uni.compute(closes2[0..2], null, null, .{ .periods_per_year = 252 }, &rows_ref, null, a);
    try expectNanRow(rows2[2]);
    try expectNear(12.5, rows2[2].last_close, 0);
    try std.testing.expectEqual(@as(u64, 2), rows2[2].max_corr_index);
    for (rows2[0..2], rows_ref) |r1, r2| {
        try expectNear(r2.beta, r1.beta, 1e-15);
        try expectNear(r2.vol, r1.vol, 0);
        try expectNear(r2.corr_market, r1.corr_market, 1e-15);
        try expectNear(r2.rel_strength, r1.rel_strength, 1e-15);
        try expectNear(r2.rank_mom_mid, r1.rank_mom_mid, 0);
        try expectNear(r2.z_mom_mid, r1.z_mom_mid, 1e-15);
        try expectNear(r2.avg_corr, r1.avg_corr, 0);
    }
    try expectNear(s_ref.market_ret_1, s2.market_ret_1, 0);
    try expectNear(s_ref.market_mom_long, s2.market_mom_long, 0);
    try expectNear(s_ref.market_vol, s2.market_vol, 0);
    try expectNear(s_ref.dispersion, s2.dispersion, 0);
    try expectNear(s_ref.breadth_up, s2.breadth_up, 0);
    try expectNear(s_ref.breadth_sma, s2.breadth_sma, 0);
    try expectNear(s_ref.avg_pair_corr, s2.avg_pair_corr, 0);
}

test "n_tickers = 1, n_bars = 0 / 1, n_tickers = 0" {
    const a = std.testing.allocator;
    // one ticker
    {
        const x = try randomWalk(a, 100, 41, 0.01);
        defer a.free(x);
        const closes = [_][]const f64{x};
        var rows: [1]Row = undefined;
        var corr: [1]f64 = undefined;
        const s = try uni.compute(&closes, null, null, .{}, &rows, &corr, a);
        try expectNear(1, corr[0], 0);
        try expectNear(0.5, rows[0].rank_mom_short, 0);
        try expectNear(0.5, rows[0].rank_vol, 0);
        try expectNear(nan, rows[0].z_mom_mid, 0);
        try expectNear(nan, rows[0].avg_corr, 0);
        try expectNear(nan, rows[0].max_corr, 0);
        try std.testing.expectEqual(@as(u64, 0), rows[0].max_corr_index);
        try expectNear(1, rows[0].beta, 1e-12);
        try expectNear(1, rows[0].corr_market, 1e-12);
        try expectNear(0, rows[0].rel_strength, 1e-12);
        try expectNear(0, s.dispersion, 0);
        try expectNear(nan, s.avg_pair_corr, 0);
        try expectNear(nan, s.max_pair_corr, 0);
        try expectNear(nan, s.min_pair_corr, 0);
        try expectNear(if (rows[0].ret_1 > 0) 1 else 0, s.breadth_up, 0);
    }
    // zero bars
    {
        const empty = [_]f64{};
        const closes = [_][]const f64{ &empty, &empty };
        var rows: [2]Row = undefined;
        var corr: [4]f64 = undefined;
        const s = try uni.compute(&closes, null, null, .{}, &rows, &corr, a);
        try std.testing.expectEqual(@as(u64, 0), s.n_bars);
        try std.testing.expectEqual(@as(u64, 2), s.n_tickers);
        for (rows, 0..) |row, i| {
            try expectNanRow(row);
            try expectNear(nan, row.last_close, 0);
            try std.testing.expectEqual(@as(u64, i), row.max_corr_index);
        }
        try expectNear(1, corr[0], 0);
        try expectNear(nan, corr[1], 0);
        try expectNear(nan, corr[2], 0);
        try expectNear(1, corr[3], 0);
        try expectNear(nan, s.market_ret_1, 0);
        try expectNear(nan, s.dispersion, 0);
        try expectNear(nan, s.breadth_up, 0);
        try expectNear(nan, s.avg_pair_corr, 0);
    }
    // one bar
    {
        const x = [_]f64{101};
        const y = [_]f64{202};
        const closes = [_][]const f64{ &x, &y };
        const vols = [_][]const f64{ &x, &y };
        const ts = [_]i64{1_700_000_000};
        var rows: [2]Row = undefined;
        var corr: [4]f64 = undefined;
        const s = try uni.compute(&closes, &vols, &ts, .{ .sma_period = 1, .weights_mode = 1 }, &rows, &corr, a);
        try std.testing.expectEqual(@as(u64, 1), s.n_bars);
        try std.testing.expectEqual(@as(i64, 1_700_000_000), s.first_ts);
        try std.testing.expectEqual(@as(i64, 1_700_000_000), s.last_ts);
        try expectNear(101, rows[0].last_close, 0);
        try expectNear(202, rows[1].last_close, 0);
        for (rows) |row| {
            try expectNear(nan, row.ret_1, 0);
            try expectNear(nan, row.mom_short, 0);
            try expectNear(nan, row.vol, 0);
            try expectNear(nan, row.beta, 0);
            try expectNear(nan, row.corr_market, 0);
            try expectNear(nan, row.volume_ratio, 0);
            try expectNear(0, row.sma_distance, 0); // sma_period 1: sma == close
        }
        try expectNear(0, s.breadth_sma, 0);
        try expectNear(nan, s.breadth_up, 0);
        try expectNear(nan, corr[1], 0);
    }
    // zero tickers
    {
        const closes = [_][]const f64{};
        var rows: [0]Row = undefined;
        var corr: [0]f64 = undefined;
        const s = try uni.compute(&closes, null, null, .{}, &rows, &corr, a);
        try std.testing.expectEqual(@as(u64, 0), s.n_tickers);
        try std.testing.expectEqual(@as(u64, 0), s.n_bars);
    }
}

test "argument validation" {
    const a = std.testing.allocator;
    const x = try randomWalk(a, 50, 51, 0.01);
    defer a.free(x);
    const y = try randomWalk(a, 49, 52, 0.01);
    defer a.free(y);
    var rows: [2]Row = undefined;
    var corr: [4]f64 = undefined;
    const ok = [_][]const f64{ x, x };
    const bad = [_][]const f64{ x, y };
    try std.testing.expectError(error.LengthMismatch, uni.compute(&bad, null, null, .{}, &rows, &corr, a));
    try std.testing.expectError(error.LengthMismatch, uni.compute(&ok, null, null, .{}, rows[0..1], &corr, a));
    try std.testing.expectError(error.LengthMismatch, uni.compute(&ok, null, null, .{}, &rows, corr[0..3], a));
    try std.testing.expectError(error.LengthMismatch, uni.compute(&ok, &bad, null, .{}, &rows, &corr, a));
    try std.testing.expectError(error.LengthMismatch, uni.compute(&ok, ok[0..1], null, .{}, &rows, &corr, a));
    const ts = [_]i64{ 1, 2, 3 };
    try std.testing.expectError(error.LengthMismatch, uni.compute(&ok, null, &ts, .{}, &rows, &corr, a));
    try std.testing.expectError(error.InvalidPeriod, uni.compute(&ok, null, null, .{ .mom_short = 0 }, &rows, &corr, a));
    try std.testing.expectError(error.InvalidPeriod, uni.compute(&ok, null, null, .{ .vol_period = 1 }, &rows, &corr, a));
    try std.testing.expectError(error.InvalidPeriod, uni.compute(&ok, null, null, .{ .corr_period = 2 }, &rows, &corr, a));
    try std.testing.expectError(error.InvalidPeriod, uni.compute(&ok, null, null, .{ .beta_period = 1 }, &rows, &corr, a));
    try std.testing.expectError(error.InvalidPeriod, uni.compute(&ok, null, null, .{ .sma_period = 0 }, &rows, &corr, a));
    try std.testing.expectError(error.InvalidPeriod, uni.compute(&ok, null, null, .{ .mom_long = uni.max_period + 1 }, &rows, &corr, a));
    try std.testing.expectError(error.InvalidParameter, uni.compute(&ok, null, null, .{ .weights_mode = 2 }, &rows, &corr, a));
    // a valid call with timestamps fills first/last
    const ts50 = try a.alloc(i64, 50);
    defer a.free(ts50);
    for (ts50, 0..) |*t, i| t.* = 1000 + @as(i64, @intCast(i)) * 60;
    const s = try uni.compute(&ok, null, ts50, .{}, &rows, null, a);
    try std.testing.expectEqual(@as(i64, 1000), s.first_ts);
    try std.testing.expectEqual(@as(i64, 1000 + 49 * 60), s.last_ts);
}

test "weights_mode 1 changes the market when volumes differ" {
    const a = std.testing.allocator;
    const n: usize = 90;
    const x = try randomWalk(a, n, 61, 0.02);
    defer a.free(x);
    const y = try randomWalk(a, n, 62, 0.02);
    defer a.free(y);
    const z = try randomWalk(a, n, 63, 0.02);
    defer a.free(z);
    const vx = try a.alloc(f64, n);
    defer a.free(vx);
    const vy = try a.alloc(f64, n);
    defer a.free(vy);
    const vz = try a.alloc(f64, n);
    defer a.free(vz);
    for (0..n) |t| {
        vx[t] = 1000 + @as(f64, @floatFromInt(t % 7)) * 10;
        vy[t] = 5000 + @as(f64, @floatFromInt(t % 5)) * 100;
        vz[t] = 250 + @as(f64, @floatFromInt(t % 3));
    }
    const closes = [_][]const f64{ x, y, z };
    const vols = [_][]const f64{ vx, vy, vz };
    const cp: usize = 30;
    var rows0: [3]Row = undefined;
    var rows1: [3]Row = undefined;
    const p0 = Params{ .corr_period = cp, .beta_period = 30, .mom_long = 30, .sma_period = 30, .weights_mode = 0 };
    var p1 = p0;
    p1.weights_mode = 1;
    const s0 = try uni.compute(&closes, &vols, null, p0, &rows0, null, a);
    const s1 = try uni.compute(&closes, &vols, null, p1, &rows1, null, a);
    // explicit weighted last return
    var w: [3]f64 = undefined;
    for (vols, 0..) |v, i| {
        var sum: f64 = 0;
        for (v[n - cp ..]) |q| sum += q;
        w[i] = sum / @as(f64, @floatFromInt(cp));
    }
    var num: f64 = 0;
    var den: f64 = 0;
    for (closes, 0..) |c, i| {
        num += w[i] * (c[n - 1] / c[n - 2] - 1);
        den += w[i];
    }
    try expectNear(num / den, s1.market_ret_1, 1e-14);
    var eq: f64 = 0;
    for (closes) |c| eq += (c[n - 1] / c[n - 2] - 1) / 3;
    try expectNear(eq, s0.market_ret_1, 1e-14);
    try std.testing.expect(@abs(s0.market_ret_1 - s1.market_ret_1) > 1e-6);
    try std.testing.expect(@abs(s0.market_mom_mid - s1.market_mom_mid) > 1e-6);
    try std.testing.expect(@abs(rows0[0].beta - rows1[0].beta) > 1e-6);
    // the heavy ticker dominates: its beta and corr_market move towards 1
    try std.testing.expect(@abs(rows1[1].beta - 1) < @abs(rows0[1].beta - 1));
    try std.testing.expect(rows1[1].corr_market > rows0[1].corr_market);
    // ticker-only features are unaffected by the weighting
    for (rows0, rows1) |r0, r1| {
        try expectNear(r0.mom_mid, r1.mom_mid, 0);
        try expectNear(r0.vol, r1.vol, 0);
        try expectNear(r0.sma_distance, r1.sma_distance, 0);
        try expectNear(r0.volume_ratio, r1.volume_ratio, 0);
        try expectNear(r0.avg_corr, r1.avg_corr, 0);
    }
    // without volumes mode 1 falls back to the equal-weight market
    var rows2: [3]Row = undefined;
    const s2 = try uni.compute(&closes, null, null, p1, &rows2, null, a);
    try expectNear(s0.market_ret_1, s2.market_ret_1, 0);
    try expectNear(rows0[2].beta, rows2[2].beta, 0);
    // a ticker with zero volume is left out of the market
    @memset(vz, 0);
    var rows3: [3]Row = undefined;
    var rows_xy: [2]Row = undefined;
    const s3 = try uni.compute(&closes, &vols, null, p1, &rows3, null, a);
    const s_xy = try uni.compute(closes[0..2], vols[0..2], null, p1, &rows_xy, null, a);
    try expectNear(s_xy.market_ret_1, s3.market_ret_1, 0);
    try expectNear(s_xy.market_vol, s3.market_vol, 0);
    try expectNear(rows_xy[0].beta, rows3[0].beta, 0);
    try expectNear(nan, rows3[2].volume_ratio, 0); // 0 / 0
}

// ---------------------------------------------------------------------------
// Random data vs a slow scalar reference
// ---------------------------------------------------------------------------

fn allFinite(x: []const f64) bool {
    for (x) |v| if (!math.isFinite(v)) return false;
    return true;
}

fn slowMean(x: []const f64) f64 {
    var s: f64 = 0;
    for (x) |v| s += v;
    return s / @as(f64, @floatFromInt(x.len));
}

fn slowPopStd(x: []const f64) f64 {
    if (x.len == 0 or !allFinite(x)) return nan;
    const mu = slowMean(x);
    var s: f64 = 0;
    for (x) |v| s += (v - mu) * (v - mu);
    return @sqrt(s / @as(f64, @floatFromInt(x.len)));
}

/// Pairwise-complete Pearson correlation, >= 3 pairs, clamped.
fn slowCorr(x: []const f64, y: []const f64) f64 {
    var cnt: usize = 0;
    var mx: f64 = 0;
    var my: f64 = 0;
    for (x, y) |u, v| {
        if (math.isFinite(u) and math.isFinite(v)) {
            cnt += 1;
            mx += u;
            my += v;
        }
    }
    if (cnt < 3) return nan;
    mx /= @floatFromInt(cnt);
    my /= @floatFromInt(cnt);
    var sxx: f64 = 0;
    var syy: f64 = 0;
    var sxy: f64 = 0;
    for (x, y) |u, v| {
        if (math.isFinite(u) and math.isFinite(v)) {
            sxx += (u - mx) * (u - mx);
            syy += (v - my) * (v - my);
            sxy += (u - mx) * (v - my);
        }
    }
    if (!(sxx * syy > 0)) return nan;
    return @min(1.0, @max(-1.0, sxy / @sqrt(sxx * syy)));
}

/// Strict-window population beta of y on x (NaN with any non-finite value).
fn slowBeta(x: []const f64, y: []const f64) f64 {
    if (!allFinite(x) or !allFinite(y)) return nan;
    const mx = slowMean(x);
    const my = slowMean(y);
    var sxx: f64 = 0;
    var sxy: f64 = 0;
    for (x, y) |u, v| {
        sxx += (u - mx) * (u - mx);
        sxy += (u - mx) * (v - my);
    }
    return if (sxx > 0) sxy / sxx else nan;
}

test "random universe vs slow scalar reference (NaN gaps, ranks, matrix symmetry)" {
    const a = std.testing.allocator;
    const m: usize = 6;
    const n: usize = 150;
    const ks: usize = 5;
    const km: usize = 20;
    const kl: usize = 60;
    const vp: usize = 20;
    const cp: usize = 60;
    const sp: usize = 50;
    const bp: usize = 60;
    const ppy: f64 = 252;
    const scale = @sqrt(ppy);

    var series: [m][]f64 = undefined;
    for (0..m) |i| series[i] = try randomWalk(a, n, 100 + i, 0.005 + 0.004 * @as(f64, @floatFromInt(i)));
    defer for (series) |s| a.free(s);
    // ticker 4 lists late (NaN for the first 100 bars -> 49 returns):
    // mom_long / beta NaN, sma (50 bars) and vol defined, correlations
    // pairwise over 49 returns; ticker 1 misses one print inside the beta /
    // corr windows but outside the vol / sma windows.
    for (0..100) |t| series[4][t] = nan;
    series[1][100] = nan;
    var closes: [m][]const f64 = undefined;
    for (0..m) |i| closes[i] = series[i];

    var rows: [m]Row = undefined;
    var corr: [m * m]f64 = undefined;
    const p = Params{ .mom_short = ks, .mom_mid = km, .mom_long = kl, .vol_period = vp, .corr_period = cp, .sma_period = sp, .beta_period = bp, .periods_per_year = ppy };
    const s = try uni.compute(&closes, null, null, p, &rows, &corr, a);

    // --- slow reference
    var ret: [m][]f64 = undefined;
    for (0..m) |i| {
        ret[i] = try a.alloc(f64, n);
        ret[i][0] = nan;
        for (1..n) |t| ret[i][t] = closes[i][t] / closes[i][t - 1] - 1;
    }
    defer for (ret) |r| a.free(r);
    const mkt = try a.alloc(f64, n);
    defer a.free(mkt);
    mkt[0] = nan;
    for (1..n) |t| {
        var sum: f64 = 0;
        var cnt: f64 = 0;
        for (0..m) |i| {
            if (math.isFinite(ret[i][t])) {
                sum += ret[i][t];
                cnt += 1;
            }
        }
        mkt[t] = if (cnt > 0) sum / cnt else nan;
    }
    const tol = 1e-10;
    // market summary
    try expectNear(mkt[n - 1], s.market_ret_1, tol);
    var acc: f64 = 1;
    for (mkt[n - km ..]) |r| acc *= 1 + r;
    const market_mom_mid = acc - 1;
    try expectNear(market_mom_mid, s.market_mom_mid, tol);
    acc = 1;
    for (mkt[n - kl ..]) |r| acc *= 1 + r;
    try expectNear(acc - 1, s.market_mom_long, tol);
    const lm = try a.alloc(f64, vp);
    defer a.free(lm);
    for (0..vp) |k| lm[k] = @log(1 + mkt[n - vp + k]);
    try expectNear(slowPopStd(lm) * scale, s.market_vol, tol);

    const lr = try a.alloc(f64, vp);
    defer a.free(lr);
    const e = try a.alloc(f64, vp);
    defer a.free(e);
    var want_mom: [5][m]f64 = undefined; // mom_short, mom_mid, mom_long, vol, rel_strength
    var want_rank: [5][m]f64 = undefined;
    for (0..m) |i| {
        const c = closes[i];
        const r = ret[i];
        const row = rows[i];
        try expectNear(c[n - 1], row.last_close, 0);
        try expectNear(r[n - 1], row.ret_1, 0);
        const mom_short = c[n - 1] / c[n - 1 - ks] - 1;
        const mom_mid = c[n - 1] / c[n - 1 - km] - 1;
        const mom_long = c[n - 1] / c[n - 1 - kl] - 1;
        try expectNear(mom_short, row.mom_short, tol);
        try expectNear(mom_mid, row.mom_mid, tol);
        try expectNear(mom_long, row.mom_long, tol);
        for (0..vp) |k| lr[k] = @log(c[n - vp + k] / c[n - vp + k - 1]);
        const vol = slowPopStd(lr) * scale;
        try expectNear(vol, row.vol, tol);
        const sma = if (allFinite(c[n - sp ..])) slowMean(c[n - sp ..]) else nan;
        try expectNear((c[n - 1] - sma) / sma, row.sma_distance, tol);
        const beta = slowBeta(mkt[n - bp ..], r[n - bp ..]);
        try expectNear(beta, row.beta, tol);
        try expectNear(slowCorr(r[n - cp ..], mkt[n - cp ..]), row.corr_market, tol);
        const rel = mom_mid - market_mom_mid;
        try expectNear(rel, row.rel_strength, tol);
        for (0..vp) |k| e[k] = r[n - vp + k] - beta * mkt[n - vp + k];
        try expectNear(if (math.isNan(beta)) nan else slowPopStd(e) * scale, row.idio_vol, tol);
        try expectNear(nan, row.volume_ratio, 0);
        want_mom[0][i] = mom_short;
        want_mom[1][i] = mom_mid;
        want_mom[2][i] = mom_long;
        want_mom[3][i] = vol;
        want_mom[4][i] = rel;
        // matrix row aggregates
        var sum: f64 = 0;
        var cnt: f64 = 0;
        var best: f64 = -math.inf(f64);
        var best_j: usize = i;
        for (0..m) |j| {
            if (j == i) continue;
            const v = slowCorr(r[n - cp ..], ret[j][n - cp ..]);
            try expectNear(v, corr[i * m + j], tol);
            if (math.isNan(v)) continue;
            sum += v;
            cnt += 1;
            if (v > best) {
                best = v;
                best_j = j;
            }
        }
        try expectNear(if (cnt > 0) sum / cnt else nan, row.avg_corr, tol);
        try expectNear(if (cnt > 0) best else nan, row.max_corr, tol);
        try std.testing.expectEqual(@as(u64, best_j), row.max_corr_index);
    }
    // expected NaN pattern from the gaps
    try expectNear(nan, rows[4].mom_long, 0);
    try expectNear(nan, rows[4].beta, 0);
    try expectNear(nan, rows[4].idio_vol, 0);
    try std.testing.expect(math.isFinite(rows[4].mom_mid));
    try std.testing.expect(math.isFinite(rows[4].vol));
    try std.testing.expect(math.isFinite(rows[4].sma_distance));
    try std.testing.expect(math.isFinite(rows[4].corr_market));
    try std.testing.expect(math.isFinite(corr[4 * m + 0]));
    try expectNear(nan, rows[4].rank_mom_long, 0);
    try std.testing.expect(math.isFinite(rows[4].rank_mom_mid));
    try expectNear(nan, rows[1].beta, 0);
    try expectNear(nan, rows[1].idio_vol, 0);
    try std.testing.expect(math.isFinite(rows[1].vol));
    try std.testing.expect(math.isFinite(rows[1].corr_market));
    try std.testing.expect(math.isFinite(rows[1].mom_long));
    // symmetry + diagonal
    for (0..m) |i| {
        try expectNear(1, corr[i * m + i], 0);
        for (0..m) |j| {
            try expectNear(corr[j * m + i], corr[i * m + j], 0);
            if (!math.isNan(corr[i * m + j])) try std.testing.expect(@abs(corr[i * m + j]) <= 1);
        }
    }
    // ranks
    for (0..5) |f| naiveRank(&want_mom[f], &want_rank[f]);
    for (0..m) |i| {
        try expectNear(want_rank[0][i], rows[i].rank_mom_short, 1e-15);
        try expectNear(want_rank[1][i], rows[i].rank_mom_mid, 1e-15);
        try expectNear(want_rank[2][i], rows[i].rank_mom_long, 1e-15);
        try expectNear(want_rank[3][i], rows[i].rank_vol, 1e-15);
        try expectNear(want_rank[4][i], rows[i].rank_rel_strength, 1e-15);
    }
    // cross-sectional moments (finite entries only)
    var fin_mid: [m]f64 = undefined;
    var n_mid: usize = 0;
    var fin_r1: [m]f64 = undefined;
    var n_r1: usize = 0;
    var up: f64 = 0;
    var above: f64 = 0;
    var n_sma: f64 = 0;
    for (0..m) |i| {
        if (math.isFinite(want_mom[1][i])) {
            fin_mid[n_mid] = want_mom[1][i];
            n_mid += 1;
        }
        if (math.isFinite(ret[i][n - 1])) {
            fin_r1[n_r1] = ret[i][n - 1];
            n_r1 += 1;
            if (ret[i][n - 1] > 0) up += 1;
        }
        if (math.isFinite(rows[i].sma_distance)) {
            n_sma += 1;
            if (rows[i].sma_distance > 0) above += 1;
        }
    }
    const mid_mean = slowMean(fin_mid[0..n_mid]);
    const mid_std = slowPopStd(fin_mid[0..n_mid]);
    try expectNear(mid_std, s.dispersion_mid, tol);
    try expectNear(slowPopStd(fin_r1[0..n_r1]), s.dispersion, tol);
    try expectNear(up / @as(f64, @floatFromInt(n_r1)), s.breadth_up, 0);
    try expectNear(above / n_sma, s.breadth_sma, 0);
    for (0..m) |i| try expectNear((want_mom[1][i] - mid_mean) / mid_std, rows[i].z_mom_mid, tol);
    // pair summary over the upper triangle
    var psum: f64 = 0;
    var pcnt: f64 = 0;
    var pmax: f64 = -math.inf(f64);
    var pmin: f64 = math.inf(f64);
    for (0..m) |i| {
        for (i + 1..m) |j| {
            const v = corr[i * m + j];
            if (math.isNan(v)) continue;
            psum += v;
            pcnt += 1;
            pmax = @max(pmax, v);
            pmin = @min(pmin, v);
        }
    }
    try expectNear(psum / pcnt, s.avg_pair_corr, tol);
    try expectNear(pmax, s.max_pair_corr, 0);
    try expectNear(pmin, s.min_pair_corr, 0);
    try std.testing.expectEqual(@as(u64, m), s.n_tickers);
    try std.testing.expectEqual(@as(u64, n), s.n_bars);
}

test "long history: only the tail is used, so results equal a truncated input" {
    const a = std.testing.allocator;
    const n: usize = 5000;
    const x = try randomWalk(a, n, 71, 0.01);
    defer a.free(x);
    const y = try randomWalk(a, n, 72, 0.01);
    defer a.free(y);
    const closes = [_][]const f64{ x, y };
    const p = Params{ .periods_per_year = 365 };
    var rows_full: [2]Row = undefined;
    var rows_tail: [2]Row = undefined;
    var corr_full: [4]f64 = undefined;
    var corr_tail: [4]f64 = undefined;
    const s_full = try uni.compute(&closes, null, null, p, &rows_full, &corr_full, a);
    const tail = [_][]const f64{ x[n - 61 ..], y[n - 61 ..] };
    const s_tail = try uni.compute(&tail, null, null, p, &rows_tail, &corr_tail, a);
    for (rows_full, rows_tail) |f, t| {
        inline for (@typeInfo(Row).@"struct".fields) |fld| {
            if (fld.type == f64) {
                try expectNear(@field(t, fld.name), @field(f, fld.name), 0);
            } else {
                try std.testing.expectEqual(@field(t, fld.name), @field(f, fld.name));
            }
        }
    }
    for (corr_full, corr_tail) |f, t| try expectNear(t, f, 0);
    try expectNear(s_tail.market_vol, s_full.market_vol, 0);
    try expectNear(s_tail.market_mom_long, s_full.market_mom_long, 0);
    try std.testing.expectEqual(@as(u64, n), s_full.n_bars);
}
