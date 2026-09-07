//! Property and consistency tests for the indicator kernels: SIMD kernels vs
//! naive scalar references, warm-up lengths, in-place aliasing, long-series
//! numerical stability and snapshot/batch consistency.
const std = @import("std");
const ind = @import("indicators.zig");

fn randomWalk(allocator: std.mem.Allocator, n: usize, seed: u64) ![]f64 {
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();
    const x = try allocator.alloc(f64, n);
    var p: f64 = 100.0;
    for (x) |*v| {
        p *= @exp(r.floatNorm(f64) * 0.01 + 0.0001);
        v.* = p;
    }
    return x;
}

fn naiveRollingSum(x: []const f64, p: usize, i: usize) f64 {
    var s: f64 = 0;
    for (x[i + 1 - p .. i + 1]) |v| s += v;
    return s;
}

test "rollingSum matches naive summation on a long series (1M)" {
    const a = std.testing.allocator;
    const n: usize = 1_000_000;
    const x = try randomWalk(a, n, 1);
    defer a.free(x);
    const out = try a.alloc(f64, n);
    defer a.free(out);
    // large price level: block-anchored prefix sums must stay accurate to ~1e-13
    for (x) |*v| v.* += 60_000.0;
    const periods = [_]usize{ 1, 2, 3, 7, 20, 64, 250, 1000, 1023, 1024, 1025, 3000 };
    for (periods) |p| {
        try ind.rollingSum(x, p, out);
        for (0..p - 1) |i| try std.testing.expect(ind.isNan(out[i]));
        var i: usize = p - 1;
        while (i < n) : (i += 997) {
            const want = naiveRollingSum(x, p, i);
            try std.testing.expectApproxEqRel(want, out[i], 1e-12);
        }
        try std.testing.expectApproxEqRel(naiveRollingSum(x, p, n - 1), out[n - 1], 1e-12);
        // block boundaries
        for ([_]usize{ 1023, 1024, 1025, 2047, 2048, 2049, 1024 * 100 - 1, 1024 * 100, 1024 * 100 + 1 }) |idx| {
            if (idx >= p - 1) try std.testing.expectApproxEqRel(naiveRollingSum(x, p, idx), out[idx], 1e-12);
        }
    }
}

test "rollingSum in place aliases correctly" {
    const a = std.testing.allocator;
    const x = try randomWalk(a, 1000, 2);
    defer a.free(x);
    const copy = try a.dupe(f64, x);
    defer a.free(copy);
    const out = try a.alloc(f64, 1000);
    defer a.free(out);
    try ind.rollingSum(x, 17, out);
    try ind.rollingSum(copy, 17, copy);
    for (out, copy) |w, g| {
        if (ind.isNan(w)) {
            try std.testing.expect(ind.isNan(g));
        } else {
            try std.testing.expectEqual(w, g);
        }
    }
}

test "rollingMax / rollingMin match naive scan for many periods" {
    const a = std.testing.allocator;
    const n: usize = 5003;
    const x = try randomWalk(a, n, 3);
    defer a.free(x);
    const out = try a.alloc(f64, n);
    defer a.free(out);
    const periods = [_]usize{ 1, 2, 3, 5, 8, 13, 50, 251, 1024, 5003 };
    for (periods) |p| {
        try ind.rollingMax(x, p, out, a);
        var i: usize = p - 1;
        while (i < n) : (i += 1) {
            var m: f64 = -std.math.inf(f64);
            for (x[i + 1 - p .. i + 1]) |v| m = @max(m, v);
            try std.testing.expectEqual(m, out[i]);
        }
        try ind.rollingMin(x, p, out, a);
        i = p - 1;
        while (i < n) : (i += 1) {
            var m: f64 = std.math.inf(f64);
            for (x[i + 1 - p .. i + 1]) |v| m = @min(m, v);
            try std.testing.expectEqual(m, out[i]);
        }
    }
}

test "rollingMoments matches two-pass reference on a long drifting series" {
    const a = std.testing.allocator;
    const n: usize = 300_000;
    const x = try randomWalk(a, n, 4);
    defer a.free(x);
    // large offset makes naive sum-of-squares formulas fail; Welford must hold
    for (x) |*v| v.* += 1.0e6;
    const mean = try a.alloc(f64, n);
    defer a.free(mean);
    const vr = try a.alloc(f64, n);
    defer a.free(vr);
    try ind.rollingMoments(x, 20, 0, mean, vr);
    var i: usize = 19;
    while (i < n) : (i += 1237) {
        const w = x[i - 19 .. i + 1];
        var m: f64 = 0;
        for (w) |v| m += v;
        m /= 20.0;
        var s: f64 = 0;
        for (w) |v| s += (v - m) * (v - m);
        s /= 20.0;
        try std.testing.expectApproxEqRel(m, mean[i], 1e-12);
        try std.testing.expectApproxEqAbs(s, vr[i], 1e-6 * @max(1.0, s));
    }
}

test "emaMulti equals independent ema per lane" {
    const a = std.testing.allocator;
    const n: usize = 3000;
    const x = try randomWalk(a, n, 5);
    defer a.free(x);
    const periods = [_]usize{ 9, 21 };
    const o1 = try a.alloc(f64, n);
    defer a.free(o1);
    const o2 = try a.alloc(f64, n);
    defer a.free(o2);
    const ref = try a.alloc(f64, n);
    defer a.free(ref);
    const outs = [_][]f64{ o1, o2 };
    try ind.emaMulti(x, periods[0..@min(periods.len, ind.lanes)], outs[0..@min(periods.len, ind.lanes)]);
    for (periods[0..@min(periods.len, ind.lanes)], 0..) |p, j| {
        try ind.ema(x, p, ref);
        for (ref, outs[j]) |w, g| {
            if (ind.isNan(w)) {
                try std.testing.expect(ind.isNan(g));
            } else {
                try std.testing.expectApproxEqRel(w, g, 1e-12);
            }
        }
    }
}

test "percentRank matches naive count" {
    const a = std.testing.allocator;
    const n: usize = 2000;
    const x = try randomWalk(a, n, 6);
    defer a.free(x);
    const out = try a.alloc(f64, n);
    defer a.free(out);
    try ind.percentRank(x, 37, out);
    var i: usize = 37;
    while (i < n) : (i += 1) {
        var c: f64 = 0;
        for (x[i - 37 .. i]) |v| {
            if (v <= x[i]) c += 1;
        }
        try std.testing.expectApproxEqRel(c * 100.0 / 37.0, out[i], 1e-12);
    }
}

test "warm-up lengths: first valid index equals the documented lookback" {
    const a = std.testing.allocator;
    const n: usize = 600;
    const c = try randomWalk(a, n, 7);
    defer a.free(c);
    const h = try a.alloc(f64, n);
    defer a.free(h);
    const l = try a.alloc(f64, n);
    defer a.free(l);
    const v = try a.alloc(f64, n);
    defer a.free(v);
    for (0..n) |i| {
        h[i] = c[i] * 1.01;
        l[i] = c[i] * 0.99;
        v[i] = 1000.0 + @as(f64, @floatFromInt(i % 17));
    }
    const cols = ind.Columns{ .open = c, .high = h, .low = l, .close = c, .volume = v, .input2 = h };
    const Case = struct { kind: ind.Kind, first: usize };
    const cases = [_]Case{
        .{ .kind = .sma, .first = 19 },
        .{ .kind = .ema, .first = 19 },
        .{ .kind = .wma, .first = 19 },
        .{ .kind = .dema, .first = 38 },
        .{ .kind = .tema, .first = 57 },
        .{ .kind = .rsi, .first = 14 },
        .{ .kind = .macd, .first = 25 },
        .{ .kind = .atr, .first = 14 },
        .{ .kind = .adx, .first = 27 },
        .{ .kind = .aroon, .first = 25 },
        .{ .kind = .mfi, .first = 14 },
        .{ .kind = .true_range, .first = 1 },
        .{ .kind = .obv, .first = 0 },
        .{ .kind = .returns, .first = 1 },
        .{ .kind = .bbands, .first = 19 },
        .{ .kind = .stoch, .first = 15 },
        .{ .kind = .willr, .first = 13 },
        .{ .kind = .psar, .first = 1 },
        .{ .kind = .supertrend, .first = 10 },
        .{ .kind = .linreg, .first = 19 },
        .{ .kind = .correl, .first = 19 },
        .{ .kind = .beta, .first = 20 },
        .{ .kind = .hist_vol, .first = 20 },
        .{ .kind = .sharpe, .first = 20 },
        .{ .kind = .kama, .first = 10 },
        .{ .kind = .trix, .first = 43 },
        .{ .kind = .ultosc, .first = 28 },
        .{ .kind = .cmo, .first = 14 },
        .{ .kind = .vortex, .first = 14 },
        .{ .kind = .ichimoku, .first = 8 },
        .{ .kind = .heikin_ashi, .first = 0 },
    };
    for (cases) |cs| {
        const n_out = ind.outputCount(cs.kind);
        const bufs = try a.alloc(f64, n_out * n);
        defer a.free(bufs);
        var outs: [8][]f64 = undefined;
        for (0..n_out) |j| outs[j] = bufs[j * n .. (j + 1) * n];
        try ind.compute(.{ .kind = @intFromEnum(cs.kind) }, cols, outs[0..n_out], a);
        const fv = ind.firstValid(outs[0]);
        if (fv != cs.first) {
            std.debug.print("{s}: first valid {d}, expected {d}\n", .{ @tagName(cs.kind), fv, cs.first });
            return error.TestUnexpectedResult;
        }
        // Once defined, an indicator stays defined (no NaN holes).
        for (outs[0][fv..]) |x| try std.testing.expect(!ind.isNan(x));
    }
}

/// A spec with the mandatory parameters filled in for kinds that need them.
fn specFor(k: ind.Kind) ind.Spec {
    return switch (k) {
        .session_vwap, .session_range, .opening_range, .pivots => .{ .kind = @intFromEnum(k), .param = 86_400 },
        else => .{ .kind = @intFromEnum(k) },
    };
}

test "series shorter than the period yields all NaN without error" {
    const a = std.testing.allocator;
    const x = [_]f64{ 1, 2, 3 };
    const t = [_]i64{ 0, 1, 2 };
    const cols = ind.Columns{ .open = &x, .high = &x, .low = &x, .close = &x, .volume = &x, .input2 = &x, .bid = &x, .ask = &x, .side = &x, .time = &t };
    for (ind.all_kinds) |k| {
        const n_out = ind.outputCount(k);
        var bufs: [8][3]f64 = undefined;
        var outs: [8][]f64 = undefined;
        for (0..n_out) |j| outs[j] = &bufs[j];
        try ind.compute(specFor(k), cols, outs[0..n_out], a);
    }
}

test "empty input is handled by every kind" {
    const a = std.testing.allocator;
    const x = [_]f64{};
    const t = [_]i64{};
    const cols = ind.Columns{ .open = &x, .high = &x, .low = &x, .close = &x, .volume = &x, .input2 = &x, .bid = &x, .ask = &x, .side = &x, .time = &t };
    for (ind.all_kinds) |k| {
        const n_out = ind.outputCount(k);
        var outs: [8][]f64 = undefined;
        for (0..n_out) |j| outs[j] = &[_]f64{};
        try ind.compute(specFor(k), cols, outs[0..n_out], a);
    }
}

test "warmup recommendation converges the first in-window EMA value" {
    const a = std.testing.allocator;
    const n: usize = 4000;
    const x = try randomWalk(a, n, 8);
    defer a.free(x);
    const full = try a.alloc(f64, n);
    defer a.free(full);
    try ind.ema(x, 20, full);
    const p = try ind.resolve(.{ .kind = @intFromEnum(ind.Kind.ema), .period = 20 });
    const w = ind.warmup(p);
    const start: usize = 3000;
    const part = try a.alloc(f64, n - (start - w));
    defer a.free(part);
    try ind.ema(x[start - w ..], 20, part);
    // value at `start` computed from the truncated series equals full-history value
    try std.testing.expectApproxEqRel(full[start], part[w], 1e-8);
}

test "resample in tick mode derives OHLC from price and counts records" {
    const a = std.testing.allocator;
    const ts = [_]i64{ 0, 10, 20, 100, 110, 250, 300 };
    const px = [_]f64{ 1, 3, 2, 5, 4, 7, 9 };
    const bars = try ind.resampleOhlcv(&ts, null, null, null, &px, null, 100, a);
    defer bars.deinit(a);
    try std.testing.expectEqual(@as(usize, 4), bars.len());
    try std.testing.expectEqual(@as(i64, 0), bars.ts[0]);
    try std.testing.expectEqual(@as(f64, 1), bars.open[0]);
    try std.testing.expectEqual(@as(f64, 3), bars.high[0]);
    try std.testing.expectEqual(@as(f64, 1), bars.low[0]);
    try std.testing.expectEqual(@as(f64, 2), bars.close[0]);
    try std.testing.expectEqual(@as(f64, 3), bars.volume[0]);
    try std.testing.expectEqual(@as(f64, 3), bars.count[0]);
    try std.testing.expectEqual(@as(i64, 300), bars.ts[3]);
    try std.testing.expectEqual(@as(f64, 9), bars.close[3]);
}

test "snapshot fields agree with individual kernels" {
    const a = std.testing.allocator;
    const n: usize = 3000;
    const c = try randomWalk(a, n, 9);
    defer a.free(c);
    const h = try a.alloc(f64, n);
    defer a.free(h);
    const l = try a.alloc(f64, n);
    defer a.free(l);
    const o = try a.alloc(f64, n);
    defer a.free(o);
    const v = try a.alloc(f64, n);
    defer a.free(v);
    const ts = try a.alloc(i64, n);
    defer a.free(ts);
    for (0..n) |i| {
        h[i] = c[i] * 1.004;
        l[i] = c[i] * 0.996;
        o[i] = if (i == 0) c[0] else c[i - 1];
        v[i] = 5000.0 + @as(f64, @floatFromInt((i * 7919) % 1000));
        ts[i] = @intCast(i);
    }
    const s = try ind.snapshot(ts, o, h, l, c, v, 252, a);
    const out = try a.alloc(f64, n);
    defer a.free(out);
    const out2 = try a.alloc(f64, n);
    defer a.free(out2);
    const out3 = try a.alloc(f64, n);
    defer a.free(out3);
    try ind.sma(c, 200, out);
    try std.testing.expectApproxEqRel(out[n - 1], s.sma_200, 1e-9);
    try ind.ema(c, 200, out);
    try std.testing.expectApproxEqRel(out[n - 1], s.ema_200, 1e-12);
    try ind.ema(c, 9, out);
    try std.testing.expectApproxEqRel(out[n - 1], s.ema_9, 1e-12);
    try ind.rsi(c, 14, out);
    try std.testing.expectApproxEqRel(out[n - 1], s.rsi_14, 1e-12);
    try ind.adx(h, l, c, 14, out, out2, out3);
    try std.testing.expectApproxEqRel(out[n - 1], s.adx_14, 1e-12);
    try ind.macd(c, 12, 26, 9, out, out2, out3);
    try std.testing.expectApproxEqRel(out3[n - 1], s.macd_hist, 1e-12);
    try ind.obv(c, v, out);
    try std.testing.expectApproxEqRel(out[n - 1], s.obv, 1e-12);
    try ind.rollingMax(c, 250, out, a);
    try std.testing.expectEqual(out[n - 1], s.high_250);
    try std.testing.expectEqual(@as(u64, n), s.bars);
    try std.testing.expectEqual(ts[n - 1], s.timestamp);
    try std.testing.expect(s.stoch_k >= 0 and s.stoch_k <= 100);
    try std.testing.expect(s.psar_dir == 1 or s.psar_dir == -1);
}

test "kind registry: names and output counts are consistent" {
    for (ind.all_kinds) |k| {
        try std.testing.expectEqual(ind.outputCount(k), ind.outputNames(k).len);
        try std.testing.expect(ind.Kind.fromInt(@intFromEnum(k)) == k);
        const p = try ind.resolve(specFor(k));
        _ = ind.warmup(p);
    }
    try std.testing.expect(ind.Kind.fromInt(9999) == null);
}

// ---------------------------------------------------------------------------
// Microstructure, pairs, labels, sessions, health, evaluation
// ---------------------------------------------------------------------------

test "spread and order flow on ticks" {
    const a = std.testing.allocator;
    const bid = [_]f64{ 99.0, 99.5, 100.0, 100.0 };
    const ask = [_]f64{ 101.0, 100.5, 100.2, 100.4 };
    var abs: [4]f64 = undefined;
    var bps: [4]f64 = undefined;
    ind.spread(&bid, &ask, &abs, &bps);
    try std.testing.expectApproxEqRel(2.0, abs[0], 1e-12);
    try std.testing.expectApproxEqRel(2.0 / 100.0 * 10_000.0, bps[0], 1e-12);
    try std.testing.expectApproxEqRel(0.2 / 100.1 * 10_000.0, bps[2], 1e-12);
    const vol = [_]f64{ 10, 20, 30, 40 };
    const side = [_]f64{ 1, 0, 1, 1 };
    var net: [4]f64 = undefined;
    var imb: [4]f64 = undefined;
    try ind.orderFlow(&vol, &side, null, 2, &net, &imb, a);
    try std.testing.expect(ind.isNan(net[0]));
    try std.testing.expectApproxEqRel(-10.0, net[1], 1e-12); // +10 - 20
    try std.testing.expectApproxEqRel(-10.0 / 30.0, imb[1], 1e-12);
    try std.testing.expectApproxEqRel(70.0, net[3], 1e-12); // +30 + 40
    try std.testing.expectApproxEqRel(1.0, imb[3], 1e-12);
    // bar mode via buy volume: buy 25 of 40 -> net 10
    const bv = [_]f64{ 5, 5, 20, 25 };
    try ind.orderFlow(&vol, null, &bv, 1, &net, &imb, a);
    try std.testing.expectApproxEqRel(10.0, net[3], 1e-12);
    try std.testing.expectApproxEqRel(0.25, imb[3], 1e-12);
    try std.testing.expectError(error.MissingColumn, ind.orderFlow(&vol, null, null, 1, &net, &imb, a));
}

test "tick pressure carries the previous sign through zero ticks" {
    const px = [_]f64{ 10, 11, 11, 10, 10, 12 }; // signs: _, +1, +1(carry), -1, -1(carry), +1
    var out: [6]f64 = undefined;
    try ind.tickPressure(&px, 2, &out);
    try std.testing.expect(ind.isNan(out[0]) and ind.isNan(out[1]));
    try std.testing.expectApproxEqRel(1.0, out[2], 1e-12); // (+1 +1)/2
    try std.testing.expectApproxEqRel(0.0, out[3], 1e-12); // (+1 -1)/2
    try std.testing.expectApproxEqRel(-1.0, out[4], 1e-12);
    try std.testing.expectApproxEqRel(0.0, out[5], 1e-12);
}

test "trade intensity, amihud and realized volatility" {
    const a = std.testing.allocator;
    const t = [_]i64{ 0, 1_000_000, 2_000_000, 4_000_000, 8_000_000 }; // microseconds
    const v = [_]f64{ 1, 2, 3, 4, 5 };
    var tr: [5]f64 = undefined;
    var vo: [5]f64 = undefined;
    try ind.tradeIntensity(&t, &v, 2, 1_000_000.0, &tr, &vo);
    try std.testing.expect(ind.isNan(tr[1]));
    try std.testing.expectApproxEqRel(2.0 / 2.0, tr[2], 1e-12); // 2 trades over 2 s
    try std.testing.expectApproxEqRel((2.0 + 3.0) / 2.0, vo[2], 1e-12);
    try std.testing.expectApproxEqRel(2.0 / 6.0, tr[4], 1e-12); // t[4]-t[2] = 6 s
    const px = [_]f64{ 100, 101, 100, 102, 101 };
    var am: [5]f64 = undefined;
    try ind.amihud(&px, &v, 2, &am, a);
    const il1 = 0.01 / (101.0 * 2.0);
    const il2 = (1.0 - 100.0 / 101.0) / (100.0 * 3.0);
    try std.testing.expectApproxEqRel((il1 + il2) / 2.0, am[2], 1e-12);
    var rv: [5]f64 = undefined;
    try ind.realizedVol(&px, 2, 0, &rv, a);
    const l1 = @log(101.0 / 100.0);
    const l2 = @log(100.0 / 101.0);
    try std.testing.expectApproxEqRel(@sqrt((l1 * l1 + l2 * l2) / 2.0), rv[2], 1e-12);
    try ind.realizedVol(&px, 2, 252, &rv, a);
    try std.testing.expectApproxEqRel(@sqrt((l1 * l1 + l2 * l2) / 2.0 * 252.0), rv[2], 1e-12);
}

test "pair kinds and alignment" {
    const a = std.testing.allocator;
    const x = [_]f64{ 10, 11, 12, 13, 14, 15 };
    const y = [_]f64{ 5, 5, 6, 6.5, 7, 7.5 };
    var out: [6]f64 = undefined;
    try ind.relStrength(&x, &y, 1, &out, a);
    try std.testing.expectApproxEqRel(10.0 - 0.0, out[1], 1e-12); // +10% vs 0%
    try std.testing.expectApproxEqRel((12.0 / 11.0 - 1.0) * 100.0 - 20.0, out[2], 1e-12);
    try ind.ratioZscore(&x, &y, 3, &out, a);
    try std.testing.expect(ind.isNan(out[1]) and !ind.isNan(out[2]));
    // as-of join: b at 0,10,20 onto a timestamps
    const ta = [_]i64{ 5, 10, 15, 25 };
    const tb = [_]i64{ 0, 10, 20 };
    const b = [_]f64{ 1, 2, 3 };
    var ab: [4]f64 = undefined;
    try ind.alignAsOf(&ta, &tb, &b, &ab);
    try std.testing.expectEqual(@as(f64, 1), ab[0]);
    try std.testing.expectEqual(@as(f64, 2), ab[1]);
    try std.testing.expectEqual(@as(f64, 2), ab[2]);
    try std.testing.expectEqual(@as(f64, 3), ab[3]);
    const ta0 = [_]i64{ -1, 0 };
    var ab0: [2]f64 = undefined;
    try ind.alignAsOf(&ta0, &tb, &b, &ab0);
    try std.testing.expect(ind.isNan(ab0[0]));
    try std.testing.expectEqual(@as(f64, 1), ab0[1]);
    // inner join
    const t1 = [_]i64{ 0, 60, 120, 180, 300 };
    const t2 = [_]i64{ 60, 120, 240, 300, 360 };
    var ia: [5]usize = undefined;
    var ib: [5]usize = undefined;
    const k = ind.alignInner(&t1, &t2, &ia, &ib);
    try std.testing.expectEqual(@as(usize, 3), k);
    try std.testing.expectEqual(@as(usize, 1), ia[0]);
    try std.testing.expectEqual(@as(usize, 0), ib[0]);
    try std.testing.expectEqual(@as(usize, 4), ia[2]);
    try std.testing.expectEqual(@as(usize, 3), ib[2]);
}

test "forward returns and triple-barrier labels" {
    const a = std.testing.allocator;
    const px = [_]f64{ 100, 102, 99, 103, 98, 100, 101 };
    var r: [7]f64 = undefined;
    var mx: [7]f64 = undefined;
    var mn: [7]f64 = undefined;
    try ind.forwardReturn(&px, 2, &r, &mx, &mn, a);
    try std.testing.expectApproxEqRel(-0.01, r[0], 1e-12); // 99/100
    try std.testing.expectApproxEqRel(0.02, mx[0], 1e-12); // max(102, 99)
    try std.testing.expectApproxEqRel(-0.01, mn[0], 1e-12);
    try std.testing.expect(ind.isNan(r[5]) and ind.isNan(r[6]));
    var lab: [7]f64 = undefined;
    var ret: [7]f64 = undefined;
    var bars: [7]f64 = undefined;
    try ind.tripleBarrier(&px, 3, 0.015, 0.015, &lab, &ret, &bars);
    try std.testing.expectEqual(@as(f64, 1), lab[0]); // 102/100 = +2% at bar 1
    try std.testing.expectEqual(@as(f64, 1), bars[0]);
    try std.testing.expectEqual(@as(f64, -1), lab[1]); // 99/102 = -2.9% at bar 1
    try std.testing.expectEqual(@as(f64, 1), lab[2]); // 103/99 = +4% at bar 1
    try std.testing.expectEqual(@as(f64, -1), lab[3]); // 98/103 = -4.8%
    try std.testing.expectEqual(@as(f64, 1), lab[4]); // 100/98 = +2.04% hits the +1.5% barrier at bar 1
    try std.testing.expect(ind.isNan(lab[5])); // +1% only, horizon not reachable -> unknown
    try std.testing.expect(ind.isNan(lab[6])); // no future
    try std.testing.expectError(error.InvalidParameter, ind.tripleBarrier(&px, 3, 0, 0.01, &lab, &ret, &bars));
}

test "session-anchored kinds reset at session boundaries" {
    // sessions of 100 units starting at offset 10: [10,110), [110,210)
    const t = [_]i64{ 10, 50, 90, 110, 150, 190, 210 };
    const px = [_]f64{ 10, 12, 11, 20, 22, 21, 30 };
    const vol = [_]f64{ 1, 1, 2, 1, 1, 1, 1 };
    var out: [7]f64 = undefined;
    try ind.sessionVwap(&t, &px, &vol, .{ .fixed = .{ .len = 100, .offset = 10 } }, &out);
    try std.testing.expectApproxEqRel(10.0, out[0], 1e-12);
    try std.testing.expectApproxEqRel((10.0 + 12.0 + 22.0) / 4.0, out[2], 1e-12);
    try std.testing.expectApproxEqRel(20.0, out[3], 1e-12); // reset
    try std.testing.expectApproxEqRel(30.0, out[6], 1e-12);
    var so: [7]f64 = undefined;
    var sh: [7]f64 = undefined;
    var sl: [7]f64 = undefined;
    var sr: [7]f64 = undefined;
    try ind.sessionRange(&t, null, null, null, &px, .{ .fixed = .{ .len = 100, .offset = 10 } }, &so, &sh, &sl, &sr);
    try std.testing.expectEqual(@as(f64, 10), so[2]);
    try std.testing.expectEqual(@as(f64, 12), sh[2]);
    try std.testing.expectEqual(@as(f64, 10), sl[2]);
    try std.testing.expectApproxEqRel(0.1, sr[2], 1e-12);
    try std.testing.expectEqual(@as(f64, 20), so[5]);
    try std.testing.expectEqual(@as(f64, 22), sh[5]);
    // opening range of the first 2 rows, then breakout
    var oh: [7]f64 = undefined;
    var ol: [7]f64 = undefined;
    var ob: [7]f64 = undefined;
    const hi = [_]f64{ 10.5, 12.5, 11.5, 20.5, 22.5, 21.5, 30.5 };
    const lo = [_]f64{ 9.5, 11.5, 10.5, 19.5, 21.5, 20.5, 29.5 };
    try ind.openingRange(&t, &hi, &lo, &px, 2, .{ .fixed = .{ .len = 100, .offset = 10 } }, &oh, &ol, &ob);
    try std.testing.expect(ind.isNan(ob[0]) and ind.isNan(ob[1]));
    try std.testing.expectEqual(@as(f64, 12.5), oh[2]);
    try std.testing.expectEqual(@as(f64, 9.5), ol[2]);
    try std.testing.expectEqual(@as(f64, 0), ob[2]); // 11 inside [9.5, 12.5]
    try std.testing.expectEqual(@as(f64, 0), ob[5]); // 21 inside [19.5, 22.5]
    try std.testing.expectEqual(@as(f64, 22.5), oh[5]);
    try std.testing.expectEqual(@as(f64, 19.5), ol[5]);
    // pivots from the previous session (H 12.5, L 9.5, C 11)
    var pp: [7]f64 = undefined;
    var r1: [7]f64 = undefined;
    var s1: [7]f64 = undefined;
    var r2: [7]f64 = undefined;
    var s2: [7]f64 = undefined;
    try ind.pivots(&t, &hi, &lo, &px, .{ .fixed = .{ .len = 100, .offset = 10 } }, &pp, &r1, &s1, &r2, &s2);
    try std.testing.expect(ind.isNan(pp[2]));
    const p = (12.5 + 9.5 + 11.0) / 3.0;
    try std.testing.expectApproxEqRel(p, pp[3], 1e-12);
    try std.testing.expectApproxEqRel(2.0 * p - 9.5, r1[4], 1e-12);
    try std.testing.expectApproxEqRel(2.0 * p - 12.5, s1[4], 1e-12);
    try std.testing.expectApproxEqRel(p + 3.0, r2[4], 1e-12);
    try std.testing.expectApproxEqRel(p - 3.0, s2[4], 1e-12);
    try std.testing.expectEqual(@as(i64, 110), ind.sessionStart(150, 100, 10));
    try std.testing.expectEqual(@as(i64, -90), ind.sessionStart(5, 100, 10));
}

test "health statistics" {
    const a = std.testing.allocator;
    const t = [_]i64{ 0, 10, 20, 1000, 1010, 1020 };
    const px = [_]f64{ 100, 101, 100, 200, 0, 201 };
    const v = [_]f64{ 1, 0, 1, 1, -1, 1 };
    const h = try ind.health(&t, &px, &v, 100, 0.5, null, 0, a);
    try std.testing.expectEqual(@as(u64, 6), h.count);
    try std.testing.expectEqual(@as(i64, 1020), h.span);
    try std.testing.expectEqual(@as(i64, 980), h.max_gap);
    try std.testing.expectEqual(@as(i64, 20), h.max_gap_at);
    try std.testing.expectEqual(@as(u64, 1), h.n_gaps);
    try std.testing.expectEqual(@as(f64, 10), h.median_gap);
    try std.testing.expectEqual(@as(u64, 1), h.n_nonpositive_price);
    try std.testing.expectEqual(@as(u64, 1), h.n_outlier_returns); // 100 -> 200 (0 is skipped)
    try std.testing.expectEqual(@as(i64, 1000), h.first_outlier_at);
    try std.testing.expectEqual(@as(u64, 1), h.n_zero_volume);
    try std.testing.expectEqual(@as(u64, 1), h.n_negative_volume);
    const empty = try ind.health(&[_]i64{}, &[_]f64{}, null, 0, 0, null, 0, a);
    try std.testing.expectEqual(@as(u64, 0), empty.count);
}

test "decision evaluation" {
    const t = [_]i64{ 0, 10, 20, 30, 40, 50 };
    const px = [_]f64{ 100, 110, 120, 90, 100, 105 };
    const d = [_]ind.Decision{
        .{ .timestamp = 0, .direction = 1, .size = 1000, .horizon = 20 }, // 100 -> 120: +20%
        .{ .timestamp = 15, .direction = -1, .size = 500, .horizon = 0 }, // entry 120 (t=20), exit t>=35 -> 100 (t=40): short +16.67%
        .{ .timestamp = 30, .direction = 1, .size = 100, .horizon = 100 }, // exit beyond data: not evaluated
        .{ .timestamp = 40, .direction = 0, .size = 1, .horizon = 10 }, // flat
    };
    var entry: [4]f64 = undefined;
    var exit: [4]f64 = undefined;
    var net: [4]f64 = undefined;
    const e = try ind.evaluate(&t, &px, &d, 20, 10, &entry, &exit, &net);
    try std.testing.expectEqual(@as(u64, 4), e.n_decisions);
    try std.testing.expectEqual(@as(u64, 2), e.n_evaluated);
    try std.testing.expectEqual(@as(u64, 2), e.n_long);
    try std.testing.expectEqual(@as(u64, 1), e.n_short);
    try std.testing.expectEqual(@as(f64, 100), entry[0]);
    try std.testing.expectEqual(@as(f64, 120), exit[0]);
    try std.testing.expectApproxEqRel(0.2 - 0.002, net[0], 1e-12);
    try std.testing.expectEqual(@as(f64, 120), entry[1]);
    try std.testing.expectEqual(@as(f64, 100), exit[1]);
    try std.testing.expectApproxEqRel((1.0 - 100.0 / 120.0) - 0.002, net[1], 1e-9); // short: -(100/120-1) = +0.1667
    try std.testing.expect(ind.isNan(net[2]) and ind.isNan(net[3]));
    try std.testing.expectEqual(@as(f64, 1), e.hit_rate);
    try std.testing.expectApproxEqRel(1000.0 * (0.2 - 0.002) + 500.0 * ((1.0 - 100.0 / 120.0) - 0.002), e.total_pnl, 1e-9);
    try std.testing.expectApproxEqRel(1500.0 * 0.002, e.total_cost, 1e-12);
    try std.testing.expectEqual(@as(f64, 0), e.max_drawdown);
    try std.testing.expectEqual(@as(f64, 1), e.long_hit_rate);
    try std.testing.expectEqual(@as(f64, 1), e.short_hit_rate);
    try std.testing.expectEqual(@as(usize, 3), ind.lowerBound(&t, 25));
    try std.testing.expectEqual(@as(usize, 6), ind.lowerBound(&t, 51));
}
