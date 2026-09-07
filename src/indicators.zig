//! HOCDB technical-indicator and quantitative-analytics kernels.
//!
//! Every kernel operates on plain `[]const f64` slices and writes into caller
//! supplied `[]f64` output slices of the same length. Positions where the
//! indicator is not yet defined (the warm-up / lookback region) are set to NaN,
//! matching the pandas / TradingView convention. Inputs containing NaN
//! propagate NaN.
//!
//! Conventions (chosen to match TA-Lib where TA-Lib defines the indicator):
//!   * EMA-family seeds with the SMA of the first `period` values.
//!   * Wilder smoothing (RSI, ATR, ADX, ...) seeds with a simple average of
//!     the first `period` observations.
//!   * True Range needs a previous close, so TR/ATR/ADX start at index 1.
//!   * STDDEV / VAR / BBANDS / ZSCORE use the population estimator (ddof=0),
//!     finance-style risk metrics (historical volatility, Sharpe, Sortino,
//!     summary std) use the sample estimator (ddof=1).
//!
//! SIMD: elementwise transforms, prefix-sum based rolling windows, rolling
//! min/max (van Herk–Gil–Werman) and window scans use `@Vector` with the lane
//! count suggested by the target CPU (`lanes`). Recurrences that are sequential
//! by definition (EMA, Wilder smoothing, KAMA, SAR, Supertrend) run as O(1)
//! per element scalar loops; `emaMulti` vectorises *across* several EMAs.
const std = @import("std");
pub const calendar = @import("calendar.zig");
const math = std.math;
const Allocator = std.mem.Allocator;

pub const nan: f64 = math.nan(f64);

/// Number of f64 lanes in one SIMD register on the compilation target.
pub const lanes: usize = std.simd.suggestVectorLength(f64) orelse 2;
const V = @Vector(lanes, f64);
const VB = @Vector(lanes, bool);

pub const Error = error{
    InvalidPeriod,
    InvalidParameter,
    LengthMismatch,
    MissingColumn,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Small SIMD helpers
// ---------------------------------------------------------------------------

inline fn splat(x: f64) V {
    return @splat(x);
}

inline fn load(s: []const f64, i: usize) V {
    return s[i..][0..lanes].*;
}

inline fn store(s: []f64, i: usize, v: V) void {
    s[i..][0..lanes].* = v;
}

pub inline fn isNan(x: f64) bool {
    return x != x;
}

pub fn fillNan(out: []f64) void {
    @memset(out, nan);
}

fn fillNanRange(out: []f64, from: usize, to: usize) void {
    if (from < to) @memset(out[from..to], nan);
}

/// out[i] = a[i] - b[i]
pub fn vecSub(a: []const f64, b: []const f64, out: []f64) void {
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) store(out, i, load(a, i) - load(b, i));
    while (i < a.len) : (i += 1) out[i] = a[i] - b[i];
}

/// out[i] = a[i] + b[i]
pub fn vecAdd(a: []const f64, b: []const f64, out: []f64) void {
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) store(out, i, load(a, i) + load(b, i));
    while (i < a.len) : (i += 1) out[i] = a[i] + b[i];
}

/// out[i] = a[i] * b[i]
pub fn vecMul(a: []const f64, b: []const f64, out: []f64) void {
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) store(out, i, load(a, i) * load(b, i));
    while (i < a.len) : (i += 1) out[i] = a[i] * b[i];
}

/// out[i] = a[i] / b[i]  (b == 0 -> NaN, matching IEEE)
pub fn vecDiv(a: []const f64, b: []const f64, out: []f64) void {
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) store(out, i, load(a, i) / load(b, i));
    while (i < a.len) : (i += 1) out[i] = a[i] / b[i];
}

/// out[i] = a[i] * k
pub fn vecScale(a: []const f64, k: f64, out: []f64) void {
    const kv = splat(k);
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) store(out, i, load(a, i) * kv);
    while (i < a.len) : (i += 1) out[i] = a[i] * k;
}

/// Compensated (Neumaier) running prefix sum: out[i] = sum(input[0..i]).
/// The scan is sequential by nature; compensation keeps the absolute error of
/// each stored prefix at O(eps * |prefix|) instead of O(n * eps * |prefix|).
pub fn prefixSum(input: []const f64, out: []f64) void {
    var sum: f64 = 0;
    var c: f64 = 0;
    for (input, 0..) |x, i| {
        const t = sum + x;
        if (@abs(sum) >= @abs(x)) {
            c += (sum - t) + x;
        } else {
            c += (x - t) + sum;
        }
        sum = t;
        out[i] = sum + c;
    }
}

/// Block size for block-anchored prefix sums (bounds rounding error to
/// O(eps * block_sum) instead of O(eps * total_sum) on long series).
const sum_block: usize = 1024;

/// Rolling sum over `period` samples. out[i] = sum(input[i-period+1..=i]).
/// Works in place (`out` may alias `input`). Prefix sums are restarted every
/// `sum_block` rows and windows are evaluated with vectorised differences,
/// so the error stays O(eps * 1024 * |x|) however long the series is.
pub fn rollingSum(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n == 0) return;
    if (period > n) {
        fillNan(out);
        return;
    }
    const p = period;
    if (p > sum_block) return rollingSumSliding(input, p, out);
    // Pass 1: compensated prefix sums restarted at every block boundary.
    var bs: usize = 0;
    while (bs < n) : (bs += sum_block) {
        const be = @min(bs + sum_block, n);
        var sum: f64 = 0;
        var c: f64 = 0;
        for (input[bs..be], bs..) |x, i| {
            const t = sum + x;
            if (@abs(sum) >= @abs(x)) c += (sum - t) + x else c += (x - t) + sum;
            sum = t;
            out[i] = sum + c;
        }
    }
    // Pass 2 (descending, in place): window sum = P[i] - P[i-p] inside a block,
    // or P[i] + T(prev block) - P[i-p] when the window starts in the previous
    // block (T = that block's last prefix, still intact when we get there).
    var b_end: usize = n;
    while (b_end > 0) {
        const b_start = (b_end - 1) / sum_block * sum_block;
        const prev_total: f64 = if (b_start == 0) 0.0 else out[b_start - 1];
        // region A: i in [b_start + p, b_end): same-block difference
        var i: usize = b_end;
        const a_lo = b_start + p;
        if (a_lo < b_end) {
            while (i >= a_lo + lanes) {
                i -= lanes;
                store(out, i, load(out, i) - load(out, i - p));
            }
            while (i > a_lo) {
                i -= 1;
                out[i] = out[i] - out[i - p];
            }
        }
        // region B: i in [max(b_start, p-1), min(b_start + p, b_end)): crosses into the previous block
        const b_lo = @max(b_start, p - 1);
        const b_hi = @min(a_lo, b_end);
        if (b_lo < b_hi) {
            const base = splat(prev_total);
            i = b_hi;
            if (b_start > 0) {
                while (i >= b_lo + lanes) {
                    i -= lanes;
                    store(out, i, load(out, i) + base - load(out, i - p));
                }
                while (i > b_lo) {
                    i -= 1;
                    out[i] = out[i] + prev_total - out[i - p];
                }
            } else {
                // first block: only i == p-1 lies here and its window starts at 0
                while (i > b_lo) {
                    i -= 1;
                    if (i >= p) out[i] = out[i] - out[i - p];
                }
            }
        }
        b_end = b_start;
    }
    fillNanRange(out, 0, p - 1);
}

/// Long windows (> sum_block): compensated sliding sum re-seeded exactly every
/// `sum_block` rows so rounding cannot accumulate.
fn rollingSumSliding(input: []const f64, p: usize, out: []f64) Error!void {
    const n = input.len;
    var sum: f64 = 0;
    var c: f64 = 0;
    var since: usize = 0;
    var i: usize = p - 1;
    while (i < n) : (i += 1) {
        if (i == p - 1 or since >= sum_block) {
            sum = 0;
            c = 0;
            for (input[i + 1 - p .. i + 1]) |x| {
                const t = sum + x;
                if (@abs(sum) >= @abs(x)) c += (sum - t) + x else c += (x - t) + sum;
                sum = t;
            }
            since = 0;
        } else {
            const d = input[i] - input[i - p];
            const t = sum + d;
            if (@abs(sum) >= @abs(d)) c += (sum - t) + d else c += (d - t) + sum;
            sum = t;
            since += 1;
        }
        out[i] = sum + c;
    }
    fillNanRange(out, 0, p - 1);
}

/// Simple moving average.
pub fn sma(input: []const f64, period: usize, out: []f64) Error!void {
    try rollingSum(input, period, out);
    const inv = 1.0 / @as(f64, @floatFromInt(period));
    vecScale(out, inv, out);
}

/// Sliding-window mean and population/sample variance. Windows up to
/// `exact_moment_period` are evaluated with an exact two-pass SIMD loop; longer
/// windows use Welford sliding updates re-seeded exactly every `sum_block`
/// rows so that the mean's rounding offset cannot telescope with price drift.
/// `out_var` / `out_mean` may be null.
pub const exact_moment_period: usize = 64;

pub fn rollingMoments(input: []const f64, period: usize, ddof: usize, out_mean: ?[]f64, out_var: ?[]f64) Error!void {
    const n = input.len;
    if (period == 0 or ddof >= period) return Error.InvalidPeriod;
    if (out_mean) |m| if (m.len != n) return Error.LengthMismatch;
    if (out_var) |v| if (v.len != n) return Error.LengthMismatch;
    const p = period;
    const pf: f64 = @floatFromInt(p);
    const denom: f64 = @floatFromInt(p - ddof);
    if (p > n) {
        if (out_mean) |m| fillNan(m);
        if (out_var) |v| fillNan(v);
        return;
    }
    if (out_mean) |m| fillNanRange(m, 0, p - 1);
    if (out_var) |v| fillNanRange(v, 0, p - 1);

    if (p <= exact_moment_period) {
        var i: usize = p - 1;
        while (i < n) : (i += 1) {
            const win = input[i + 1 - p .. i + 1];
            var acc: V = splat(0);
            var k: usize = 0;
            while (k + lanes <= p) : (k += lanes) acc += load(win, k);
            var mean = @reduce(.Add, acc);
            while (k < p) : (k += 1) mean += win[k];
            mean /= pf;
            if (out_mean) |m| m[i] = mean;
            if (out_var) |v| {
                const mv = splat(mean);
                var acc2: V = splat(0);
                k = 0;
                while (k + lanes <= p) : (k += lanes) {
                    const d = load(win, k) - mv;
                    acc2 += d * d;
                }
                var m2 = @reduce(.Add, acc2);
                while (k < p) : (k += 1) {
                    const d = win[k] - mean;
                    m2 += d * d;
                }
                v[i] = m2 / denom;
            }
        }
        return;
    }

    var mean: f64 = 0;
    var m2: f64 = 0;
    var since: usize = 0;
    var i: usize = p - 1;
    while (i < n) : (i += 1) {
        if (i == p - 1 or since >= sum_block) {
            // exact re-seed
            const win = input[i + 1 - p .. i + 1];
            mean = 0;
            for (win) |x| mean += x;
            mean /= pf;
            m2 = 0;
            for (win) |x| {
                const d = x - mean;
                m2 += d * d;
            }
            since = 0;
        } else {
            const xo = input[i - p];
            const xn = input[i];
            const old_mean = mean;
            mean += (xn - xo) / pf;
            m2 += (xn - xo) * (xn - mean + xo - old_mean);
            if (m2 < 0) m2 = 0; // rounding guard
            since += 1;
        }
        if (out_mean) |m| m[i] = mean;
        if (out_var) |v| v[i] = m2 / denom;
    }
}

/// Rolling standard deviation (population by default, ddof selectable).
pub fn stddev(input: []const f64, period: usize, ddof: usize, out: []f64) Error!void {
    try rollingMoments(input, period, ddof, null, out);
    var i: usize = 0;
    while (i + lanes <= out.len) : (i += lanes) store(out, i, @sqrt(load(out, i)));
    while (i < out.len) : (i += 1) out[i] = @sqrt(out[i]);
}

/// Rolling variance.
pub fn variance(input: []const f64, period: usize, ddof: usize, out: []f64) Error!void {
    try rollingMoments(input, period, ddof, null, out);
}

/// Rolling max / min over `period` samples using the van Herk–Gil–Werman
/// algorithm: two scalar scans per block and a vectorised combine, O(n)
/// independent of the period. `scratch` must hold 2*n f64.
fn rollingExtreme(comptime is_max: bool, input: []const f64, period: usize, out: []f64, scratch: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n or scratch.len < 2 * n) return Error.LengthMismatch;
    if (period > n) {
        fillNan(out);
        return;
    }
    const p = period;
    if (p == 1) {
        @memcpy(out, input);
        return;
    }
    const pre = scratch[0..n]; // running extreme from block start
    const suf = scratch[n .. 2 * n]; // running extreme to block end
    var bs: usize = 0;
    while (bs < n) : (bs += p) {
        const be = @min(bs + p, n);
        var acc = input[bs];
        pre[bs] = acc;
        var i = bs + 1;
        while (i < be) : (i += 1) {
            acc = if (is_max) @max(acc, input[i]) else @min(acc, input[i]);
            pre[i] = acc;
        }
        acc = input[be - 1];
        suf[be - 1] = acc;
        i = be - 1;
        while (i > bs) {
            i -= 1;
            acc = if (is_max) @max(acc, input[i]) else @min(acc, input[i]);
            suf[i] = acc;
        }
    }
    fillNanRange(out, 0, p - 1);
    // out[i] = combine(suf[i-p+1], pre[i])
    var i: usize = p - 1;
    while (i + lanes <= n) : (i += lanes) {
        const a = load(suf, i + 1 - p);
        const b = load(pre, i);
        store(out, i, if (is_max) @max(a, b) else @min(a, b));
    }
    while (i < n) : (i += 1) {
        out[i] = if (is_max) @max(suf[i + 1 - p], pre[i]) else @min(suf[i + 1 - p], pre[i]);
    }
}

pub fn rollingMax(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const scratch = try allocator.alloc(f64, 2 * input.len);
    defer allocator.free(scratch);
    try rollingExtreme(true, input, period, out, scratch);
}

pub fn rollingMin(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const scratch = try allocator.alloc(f64, 2 * input.len);
    defer allocator.free(scratch);
    try rollingExtreme(false, input, period, out, scratch);
}

/// Index of the first non-NaN element, or input.len if none.
pub fn firstValid(input: []const f64) usize {
    for (input, 0..) |x, i| if (!isNan(x)) return i;
    return input.len;
}

// ---------------------------------------------------------------------------
// Moving averages
// ---------------------------------------------------------------------------

/// Exponential moving average with an arbitrary smoothing constant `k`,
/// seeded with the simple average of the first `period` valid samples
/// (starting at the first non-NaN input). Works in place.
pub fn emaK(input: []const f64, period: usize, k: f64, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const start = firstValid(input);
    if (start + period > n) {
        fillNan(out);
        return;
    }
    var seed: f64 = 0;
    for (input[start .. start + period]) |x| seed += x;
    seed /= @floatFromInt(period);
    fillNanRange(out, 0, start + period - 1);
    var s = seed;
    out[start + period - 1] = s;
    var i = start + period;
    while (i < n) : (i += 1) {
        s += k * (input[i] - s);
        out[i] = s;
    }
}

/// Exponential moving average, alpha = 2 / (period + 1).
pub fn ema(input: []const f64, period: usize, out: []f64) Error!void {
    if (period == 0) return Error.InvalidPeriod;
    return emaK(input, period, 2.0 / (@as(f64, @floatFromInt(period)) + 1.0), out);
}

/// Wilder's smoothing (RMA), alpha = 1 / period.
pub fn rma(input: []const f64, period: usize, out: []f64) Error!void {
    if (period == 0) return Error.InvalidPeriod;
    return emaK(input, period, 1.0 / @as(f64, @floatFromInt(period)), out);
}

/// Up to `lanes` EMAs with different periods advanced in one SIMD recurrence.
/// `periods.len` must be <= lanes; `outs[j]` receives EMA(periods[j]).
pub fn emaMulti(input: []const f64, periods: []const usize, outs: []const []f64) Error!void {
    const n = input.len;
    const m = periods.len;
    if (m == 0 or m > lanes or outs.len != m) return Error.LengthMismatch;
    var kv: V = splat(0);
    var state: V = splat(0);
    var max_period: usize = 0;
    var kv_arr: [lanes]f64 = [_]f64{0} ** lanes;
    for (periods, 0..) |p, j| {
        if (p == 0) return Error.InvalidPeriod;
        if (outs[j].len != n) return Error.LengthMismatch;
        kv_arr[j] = 2.0 / (@as(f64, @floatFromInt(p)) + 1.0);
        max_period = @max(max_period, p);
    }
    kv = kv_arr;
    // Seed each lane independently (SMA of first period values), then run the
    // shared recurrence from max_period onward; lanes with shorter periods
    // are advanced scalar-wise until all lanes are live.
    var seeded: [lanes]bool = [_]bool{false} ** lanes;
    var st_arr: [lanes]f64 = [_]f64{0} ** lanes;
    for (periods, 0..) |p, j| {
        fillNan(outs[j]);
        if (p > n) continue;
        var s: f64 = 0;
        for (input[0..p]) |x| s += x;
        s /= @floatFromInt(p);
        outs[j][p - 1] = s;
        var i = p;
        while (i < @min(max_period, n)) : (i += 1) {
            s += kv_arr[j] * (input[i] - s);
            outs[j][i] = s;
        }
        st_arr[j] = s;
        seeded[j] = true;
    }
    state = st_arr;
    if (max_period > n) return;
    var i = max_period;
    while (i < n) : (i += 1) {
        const x = splat(input[i]);
        state += kv * (x - state);
        const arr: [lanes]f64 = state;
        for (0..m) |j| outs[j][i] = arr[j];
    }
}

/// Weighted moving average, linear weights 1..period.
pub fn wma(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const p = period;
    if (p > n) {
        fillNan(out);
        return;
    }
    const pf: f64 = @floatFromInt(p);
    const divisor = pf * (pf + 1.0) / 2.0;
    // period_sum: plain sum of window, period_sub: weighted sum.
    var period_sum: f64 = 0;
    var period_sub: f64 = 0;
    for (0..p) |k| {
        const x = input[k];
        period_sum += x;
        period_sub += x * @as(f64, @floatFromInt(k + 1));
    }
    fillNanRange(out, 0, p - 1);
    out[p - 1] = period_sub / divisor;
    var i: usize = p;
    while (i < n) : (i += 1) {
        const x = input[i];
        period_sub -= period_sum;
        period_sum -= input[i - p];
        period_sum += x;
        period_sub += x * pf;
        out[i] = period_sub / divisor;
    }
}

/// Double EMA: 2*EMA - EMA(EMA).
pub fn dema(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out.len != n) return Error.LengthMismatch;
    const e1 = try allocator.alloc(f64, n);
    defer allocator.free(e1);
    try ema(input, period, e1);
    try ema(e1, period, out); // out = ema(ema)
    var i: usize = 0;
    const two = splat(2.0);
    while (i + lanes <= n) : (i += lanes) store(out, i, two * load(e1, i) - load(out, i));
    while (i < n) : (i += 1) out[i] = 2.0 * e1[i] - out[i];
}

/// Triple EMA: 3*E1 - 3*E2 + E3.
pub fn tema(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out.len != n) return Error.LengthMismatch;
    const e1 = try allocator.alloc(f64, n);
    defer allocator.free(e1);
    const e2 = try allocator.alloc(f64, n);
    defer allocator.free(e2);
    try ema(input, period, e1);
    try ema(e1, period, e2);
    try ema(e2, period, out); // out = e3
    var i: usize = 0;
    const three = splat(3.0);
    while (i + lanes <= n) : (i += lanes) store(out, i, three * load(e1, i) - three * load(e2, i) + load(out, i));
    while (i < n) : (i += 1) out[i] = 3.0 * e1[i] - 3.0 * e2[i] + out[i];
}

/// Triangular moving average (TA-Lib definition: SMA of SMA).
pub fn trima(input: []const f64, period: usize, out: []f64) Error!void {
    if (period == 0) return Error.InvalidPeriod;
    if (period == 1) {
        if (out.len != input.len) return Error.LengthMismatch;
        @memcpy(out, input);
        return;
    }
    const first: usize = if (period % 2 == 1) (period + 1) / 2 else period / 2;
    const second: usize = if (period % 2 == 1) (period + 1) / 2 else period / 2 + 1;
    try sma(input, first, out);
    // sma of a NaN-prefixed series: shift the window start.
    try smaFrom(out, first - 1, second, out);
}

/// SMA that starts at `start` (values before it are NaN/ignored). In place OK.
fn smaFrom(input: []const f64, start: usize, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (start >= n) {
        fillNan(out);
        return;
    }
    // Compute on the tail slice; `out` may alias input so we handle prefix after.
    try sma(input[start..], period, out[start..]);
    fillNanRange(out, 0, start);
}

/// Kaufman adaptive moving average (TA-Lib algorithm, fast=2, slow=30).
pub fn kama(input: []const f64, period: usize, fast: usize, slow: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0 or fast == 0 or slow == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const p = period;
    if (p + 1 > n) {
        fillNan(out);
        return;
    }
    const const_max = 2.0 / (@as(f64, @floatFromInt(slow)) + 1.0);
    const const_diff = 2.0 / (@as(f64, @floatFromInt(fast)) + 1.0) - const_max;
    fillNanRange(out, 0, p);
    var sum_roc1: f64 = 0;
    for (0..p) |k| sum_roc1 += @abs(input[k + 1] - input[k]);
    var prev = input[p - 1];
    var trailing: usize = 0;
    var today: usize = p;
    while (today < n) : (today += 1) {
        const x = input[today];
        const trailing_val = input[trailing];
        const period_roc = x - trailing_val;
        // slide the volatility sum: drop |x[t+1]-x[t]| at the trailing edge,
        // add the newest change.
        if (today > p) {
            sum_roc1 -= @abs(trailing_val - input[trailing - 1]);
            sum_roc1 += @abs(x - input[today - 1]);
        }
        var er: f64 = undefined;
        if (sum_roc1 <= 0.0 or sum_roc1 <= period_roc) {
            er = 1.0;
        } else {
            er = @abs(period_roc / sum_roc1);
            if (er > 1.0) er = 1.0;
        }
        var sc = er * const_diff + const_max;
        sc *= sc;
        prev += (x - prev) * sc;
        out[today] = prev;
        trailing += 1;
    }
}

/// Hull moving average: WMA(2*WMA(n/2) - WMA(n), floor(sqrt(n))).
pub fn hma(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (period < 2) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const half = period / 2;
    const sq: usize = @intFromFloat(@floor(@sqrt(@as(f64, @floatFromInt(period)))));
    const w_half = try allocator.alloc(f64, n);
    defer allocator.free(w_half);
    const w_full = try allocator.alloc(f64, n);
    defer allocator.free(w_full);
    try wma(input, half, w_half);
    try wma(input, period, w_full);
    var i: usize = 0;
    const two = splat(2.0);
    while (i + lanes <= n) : (i += lanes) store(w_half, i, two * load(w_half, i) - load(w_full, i));
    while (i < n) : (i += 1) w_half[i] = 2.0 * w_half[i] - w_full[i];
    // WMA over a NaN-prefixed series.
    const start = period - 1;
    if (start >= n) {
        fillNan(out);
        return;
    }
    try wma(w_half[start..], sq, out[start..]);
    fillNanRange(out, 0, start);
}

/// Zero-lag EMA: EMA of (2x[i] - x[i-lag]), lag = (period-1)/2.
pub fn zlema(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const lag = (period - 1) / 2;
    if (lag >= n) {
        fillNan(out);
        return;
    }
    // de-lagged series into out (vectorised), then EMA in place.
    fillNanRange(out, 0, lag);
    var i: usize = lag;
    const two = splat(2.0);
    while (i + lanes <= n) : (i += lanes) store(out, i, two * load(input, i) - load(input, i - lag));
    while (i < n) : (i += 1) out[i] = 2.0 * input[i] - input[i - lag];
    try ema(out, period, out);
}

/// Volume weighted moving average: sum(p*v)/sum(v) over period.
pub fn vwma(price: []const f64, volume: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = price.len;
    if (volume.len != n or out.len != n) return Error.LengthMismatch;
    const pv = try allocator.alloc(f64, n);
    defer allocator.free(pv);
    vecMul(price, volume, pv);
    try rollingSum(pv, period, pv);
    try rollingSum(volume, period, out);
    vecDiv(pv, out, out);
}

// ---------------------------------------------------------------------------
// Elementwise price transforms (SIMD)
// ---------------------------------------------------------------------------

/// (high + low + close) / 3
pub fn typicalPrice(high: []const f64, low: []const f64, close: []const f64, out: []f64) void {
    // Division (not multiplication by 1/3) so that equal typical prices compare
    // equal exactly as in TA-Lib; MFI's up/down decision depends on it.
    const three = splat(3.0);
    var i: usize = 0;
    while (i + lanes <= out.len) : (i += lanes) store(out, i, (load(high, i) + load(low, i) + load(close, i)) / three);
    while (i < out.len) : (i += 1) out[i] = (high[i] + low[i] + close[i]) / 3.0;
}

/// (high + low) / 2
pub fn medianPrice(high: []const f64, low: []const f64, out: []f64) void {
    const half = splat(0.5);
    var i: usize = 0;
    while (i + lanes <= out.len) : (i += lanes) store(out, i, (load(high, i) + load(low, i)) * half);
    while (i < out.len) : (i += 1) out[i] = (high[i] + low[i]) * 0.5;
}

/// True range: max(h-l, |h-pc|, |l-pc|). Index 0 is NaN (no previous close).
pub fn trueRange(high: []const f64, low: []const f64, close: []const f64, out: []f64) void {
    const n = out.len;
    if (n == 0) return;
    out[0] = nan;
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const h = load(high, i);
        const l = load(low, i);
        const pc = load(close, i - 1);
        store(out, i, @max(h - l, @max(@abs(h - pc), @abs(l - pc))));
    }
    while (i < n) : (i += 1) {
        const pc = close[i - 1];
        out[i] = @max(high[i] - low[i], @max(@abs(high[i] - pc), @abs(low[i] - pc)));
    }
}

/// Close location value: ((c-l) - (h-c)) / (h-l), 0 when h == l.
pub fn clv(high: []const f64, low: []const f64, close: []const f64, out: []f64) void {
    const n = out.len;
    const zero = splat(0);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const h = load(high, i);
        const l = load(low, i);
        const c = load(close, i);
        const range = h - l;
        const v = ((c - l) - (h - c)) / range;
        store(out, i, @select(f64, range == zero, zero, v));
    }
    while (i < n) : (i += 1) {
        const range = high[i] - low[i];
        out[i] = if (range == 0) 0 else ((close[i] - low[i]) - (high[i] - close[i])) / range;
    }
}

/// Simple returns over `period`: x[i]/x[i-period] - 1.
pub fn returns(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (period >= n) {
        fillNan(out);
        return;
    }
    fillNanRange(out, 0, period);
    const one = splat(1.0);
    var i: usize = period;
    while (i + lanes <= n) : (i += lanes) store(out, i, load(input, i) / load(input, i - period) - one);
    while (i < n) : (i += 1) out[i] = input[i] / input[i - period] - 1.0;
}

/// Log returns over `period`: ln(x[i]/x[i-period]).
pub fn logReturns(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (period >= n) {
        fillNan(out);
        return;
    }
    fillNanRange(out, 0, period);
    var i: usize = period;
    while (i + lanes <= n) : (i += lanes) store(out, i, @log(load(input, i) / load(input, i - period)));
    while (i < n) : (i += 1) out[i] = @log(input[i] / input[i - period]);
}

/// Momentum: x[i] - x[i-period].
pub fn mom(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (period >= n) {
        fillNan(out);
        return;
    }
    fillNanRange(out, 0, period);
    var i: usize = period;
    while (i + lanes <= n) : (i += lanes) store(out, i, load(input, i) - load(input, i - period));
    while (i < n) : (i += 1) out[i] = input[i] - input[i - period];
}

/// Rate of change in percent: (x[i]/x[i-period] - 1) * 100.
pub fn roc(input: []const f64, period: usize, out: []f64) Error!void {
    try returns(input, period, out);
    vecScale(out, 100.0, out);
}

// ---------------------------------------------------------------------------
// Momentum oscillators
// ---------------------------------------------------------------------------

/// Relative strength index (Wilder smoothing, TA-Lib seeding). First value at
/// index `period`. A window with zero gain and zero loss yields 0 (TA-Lib).
pub fn rsi(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const p = period;
    if (p + 1 > n) {
        fillNan(out);
        return;
    }
    const pf: f64 = @floatFromInt(p);
    var gain: f64 = 0;
    var loss: f64 = 0;
    for (1..p + 1) |k| {
        const d = input[k] - input[k - 1];
        if (d > 0) gain += d else loss -= d;
    }
    gain /= pf;
    loss /= pf;
    fillNanRange(out, 0, p);
    var total = gain + loss;
    out[p] = if (total != 0) 100.0 * (gain / total) else 0.0;
    var i: usize = p + 1;
    while (i < n) : (i += 1) {
        const d = input[i] - input[i - 1];
        gain = (gain * (pf - 1.0) + @max(d, 0.0)) / pf;
        loss = (loss * (pf - 1.0) + @max(-d, 0.0)) / pf;
        total = gain + loss;
        out[i] = if (total != 0) 100.0 * (gain / total) else 0.0;
    }
}

/// MACD line, signal line and histogram. Both EMAs are seeded at index
/// slow-1 (TA-Lib alignment); the MACD line is defined from slow-1, the
/// signal from slow+signal-2.
pub fn macd(input: []const f64, fast: usize, slow: usize, signal: usize, out_macd: []f64, out_signal: []f64, out_hist: []f64) Error!void {
    const n = input.len;
    if (fast == 0 or slow == 0 or signal == 0) return Error.InvalidPeriod;
    if (out_macd.len != n or out_signal.len != n or out_hist.len != n) return Error.LengthMismatch;
    var f = fast;
    var s = slow;
    if (f > s) {
        const t = f;
        f = s;
        s = t;
    }
    if (s > n) {
        fillNan(out_macd);
        fillNan(out_signal);
        fillNan(out_hist);
        return;
    }
    const kf = 2.0 / (@as(f64, @floatFromInt(f)) + 1.0);
    const ks = 2.0 / (@as(f64, @floatFromInt(s)) + 1.0);
    var ema_s: f64 = 0;
    for (input[0..s]) |x| ema_s += x;
    ema_s /= @floatFromInt(s);
    var ema_f: f64 = 0;
    for (input[s - f .. s]) |x| ema_f += x;
    ema_f /= @floatFromInt(f);
    fillNanRange(out_macd, 0, s - 1);
    out_macd[s - 1] = ema_f - ema_s;
    var i: usize = s;
    while (i < n) : (i += 1) {
        const x = input[i];
        ema_f += kf * (x - ema_f);
        ema_s += ks * (x - ema_s);
        out_macd[i] = ema_f - ema_s;
    }
    try ema(out_macd, signal, out_signal);
    vecSub(out_macd, out_signal, out_hist);
}

/// Percentage price oscillator with EMAs: (EMA_fast - EMA_slow)/EMA_slow*100,
/// plus signal EMA and histogram.
pub fn ppo(input: []const f64, fast: usize, slow: usize, signal: usize, out_ppo: []f64, out_signal: []f64, out_hist: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out_ppo.len != n or out_signal.len != n or out_hist.len != n) return Error.LengthMismatch;
    var f = fast;
    var s = slow;
    if (f > s) {
        const t = f;
        f = s;
        s = t;
    }
    const ef = try allocator.alloc(f64, n);
    defer allocator.free(ef);
    try ema(input, f, ef);
    try ema(input, s, out_ppo); // slow in out_ppo
    var i: usize = 0;
    const hundred = splat(100.0);
    while (i + lanes <= n) : (i += lanes) {
        const sl = load(out_ppo, i);
        store(out_ppo, i, (load(ef, i) - sl) / sl * hundred);
    }
    while (i < n) : (i += 1) out_ppo[i] = (ef[i] - out_ppo[i]) / out_ppo[i] * 100.0;
    try ema(out_ppo, signal, out_signal);
    vecSub(out_ppo, out_signal, out_hist);
}

/// Stochastic oscillator: %K = SMA(fastK, k_smooth), %D = SMA(%K, d_period),
/// fastK = 100*(close-LL)/(HH-LL) over `k_period` (0 when HH == LL).
pub fn stoch(high: []const f64, low: []const f64, close: []const f64, k_period: usize, k_smooth: usize, d_period: usize, out_k: []f64, out_d: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (high.len != n or low.len != n or out_k.len != n or out_d.len != n) return Error.LengthMismatch;
    if (k_period == 0 or k_smooth == 0 or d_period == 0) return Error.InvalidPeriod;
    const hh = try allocator.alloc(f64, n);
    defer allocator.free(hh);
    const ll = try allocator.alloc(f64, n);
    defer allocator.free(ll);
    const scratch = try allocator.alloc(f64, 2 * n);
    defer allocator.free(scratch);
    try rollingExtreme(true, high, k_period, hh, scratch);
    try rollingExtreme(false, low, k_period, ll, scratch);
    // fastK into out_k
    const zero = splat(0);
    const hundred = splat(100.0);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const range = load(hh, i) - load(ll, i);
        const v = (load(close, i) - load(ll, i)) / range * hundred;
        store(out_k, i, @select(f64, range == zero, zero, v));
    }
    while (i < n) : (i += 1) {
        const range = hh[i] - ll[i];
        out_k[i] = if (range == 0) 0 else (close[i] - ll[i]) / range * 100.0;
    }
    const start = k_period - 1;
    if (start >= n) {
        fillNan(out_k);
        fillNan(out_d);
        return;
    }
    try smaFrom(out_k, start, k_smooth, out_k);
    const start2 = start + k_smooth - 1;
    try smaFrom(out_k, start2, d_period, out_d);
}

/// Stochastic RSI: stochastic of RSI(rsi_period) over `stoch_period`,
/// smoothed by `k_smooth` (%K) and `d_period` (%D).
pub fn stochRsi(input: []const f64, rsi_period: usize, stoch_period: usize, k_smooth: usize, d_period: usize, out_k: []f64, out_d: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out_k.len != n or out_d.len != n) return Error.LengthMismatch;
    const r = try allocator.alloc(f64, n);
    defer allocator.free(r);
    try rsi(input, rsi_period, r);
    const start = rsi_period;
    if (start >= n) {
        fillNan(out_k);
        fillNan(out_d);
        return;
    }
    try stoch(r[start..], r[start..], r[start..], stoch_period, k_smooth, d_period, out_k[start..], out_d[start..], allocator);
    fillNanRange(out_k, 0, start);
    fillNanRange(out_d, 0, start);
}

/// Commodity channel index: (TP - SMA(TP)) / (0.015 * mean |TP - SMA(TP)|).
pub fn cci(high: []const f64, low: []const f64, close: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (period == 0) return Error.InvalidPeriod;
    if (high.len != n or low.len != n or out.len != n) return Error.LengthMismatch;
    const tp = try allocator.alloc(f64, n);
    defer allocator.free(tp);
    typicalPrice(high, low, close, tp);
    try sma(tp, period, out);
    if (period > n) return;
    const pf: f64 = @floatFromInt(period);
    var i: usize = period - 1;
    while (i < n) : (i += 1) {
        const m = out[i];
        const mv = splat(m);
        const win = tp[i + 1 - period .. i + 1];
        var acc: V = splat(0);
        var k: usize = 0;
        while (k + lanes <= win.len) : (k += lanes) acc += @abs(load(win, k) - mv);
        var mad = @reduce(.Add, acc);
        while (k < win.len) : (k += 1) mad += @abs(win[k] - m);
        mad /= pf;
        out[i] = if (mad == 0) 0 else (tp[i] - m) / (0.015 * mad);
    }
}

/// Williams %R: -100 * (HH - close) / (HH - LL); 0 when HH == LL.
pub fn willr(high: []const f64, low: []const f64, close: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (high.len != n or low.len != n or out.len != n) return Error.LengthMismatch;
    const ll = try allocator.alloc(f64, n);
    defer allocator.free(ll);
    const scratch = try allocator.alloc(f64, 2 * n);
    defer allocator.free(scratch);
    try rollingExtreme(true, high, period, out, scratch); // hh in out
    try rollingExtreme(false, low, period, ll, scratch);
    const zero = splat(0);
    const neg100 = splat(-100.0);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const hh = load(out, i);
        const range = hh - load(ll, i);
        const v = (hh - load(close, i)) / range * neg100;
        store(out, i, @select(f64, range == zero, zero, v));
    }
    while (i < n) : (i += 1) {
        const range = out[i] - ll[i];
        out[i] = if (range == 0) 0 else (out[i] - close[i]) / range * -100.0;
    }
}

/// Chande momentum oscillator (original definition with plain sums):
/// 100 * (sum(up) - sum(down)) / (sum(up) + sum(down)) over `period` changes.
pub fn cmo(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const up = try allocator.alloc(f64, n);
    defer allocator.free(up);
    const dn = try allocator.alloc(f64, n);
    defer allocator.free(dn);
    up[0] = 0;
    dn[0] = 0;
    const zero = splat(0);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const d = load(input, i) - load(input, i - 1);
        store(up, i, @max(d, zero));
        store(dn, i, @max(-d, zero));
    }
    while (i < n) : (i += 1) {
        const d = input[i] - input[i - 1];
        up[i] = @max(d, 0.0);
        dn[i] = @max(-d, 0.0);
    }
    try rollingSum(up[1..], period, up[1..]);
    try rollingSum(dn[1..], period, dn[1..]);
    out[0] = nan;
    const hundred = splat(100.0);
    i = 1;
    while (i + lanes <= n) : (i += lanes) {
        const su = load(up, i);
        const sd = load(dn, i);
        const tot = su + sd;
        store(out, i, @select(f64, tot == zero, zero, (su - sd) / tot * hundred));
    }
    while (i < n) : (i += 1) {
        const tot = up[i] + dn[i];
        out[i] = if (tot == 0) 0 else (up[i] - dn[i]) / tot * 100.0;
    }
}

/// TRIX: 1-period percent rate of change of a triple-smoothed EMA.
pub fn trix(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out.len != n) return Error.LengthMismatch;
    const e = try allocator.alloc(f64, n);
    defer allocator.free(e);
    try ema(input, period, e);
    try ema(e, period, e);
    try ema(e, period, e);
    if (n == 0) return;
    out[0] = nan;
    const one = splat(1.0);
    const hundred = splat(100.0);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) store(out, i, (load(e, i) / load(e, i - 1) - one) * hundred);
    while (i < n) : (i += 1) out[i] = (e[i] / e[i - 1] - 1.0) * 100.0;
}

/// Ultimate oscillator (Williams), default periods 7/14/28.
pub fn ultosc(high: []const f64, low: []const f64, close: []const f64, p1: usize, p2: usize, p3: usize, out: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (p1 == 0 or p2 == 0 or p3 == 0) return Error.InvalidPeriod;
    if (high.len != n or low.len != n or out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const bp = try allocator.alloc(f64, n);
    defer allocator.free(bp);
    const tr = try allocator.alloc(f64, n);
    defer allocator.free(tr);
    bp[0] = 0;
    tr[0] = 0;
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const h = load(high, i);
        const l = load(low, i);
        const c = load(close, i);
        const pc = load(close, i - 1);
        const lo = @min(l, pc);
        store(bp, i, c - lo);
        store(tr, i, @max(h, pc) - lo);
    }
    while (i < n) : (i += 1) {
        const pc = close[i - 1];
        const lo = @min(low[i], pc);
        bp[i] = close[i] - lo;
        tr[i] = @max(high[i], pc) - lo;
    }
    // Three rolling-sum pairs. Reuse buffers: we need bp1,tr1,bp2,tr2,bp3,tr3.
    const buf = try allocator.alloc(f64, 6 * n);
    defer allocator.free(buf);
    const periods = [_]usize{ p1, p2, p3 };
    for (periods, 0..) |p, k| {
        const b = buf[(2 * k) * n .. (2 * k + 1) * n];
        const t = buf[(2 * k + 1) * n .. (2 * k + 2) * n];
        try rollingSum(bp[1..], p, b[1..]);
        try rollingSum(tr[1..], p, t[1..]);
        b[0] = nan;
        t[0] = nan;
    }
    const b1 = buf[0..n];
    const t1 = buf[n .. 2 * n];
    const b2 = buf[2 * n .. 3 * n];
    const t2 = buf[3 * n .. 4 * n];
    const b3 = buf[4 * n .. 5 * n];
    const t3 = buf[5 * n .. 6 * n];
    const lookback = @max(p1, @max(p2, p3));
    fillNanRange(out, 0, @min(n, lookback));
    i = lookback;
    while (i < n) : (i += 1) {
        var o: f64 = 0;
        if (t1[i] != 0) o += 4.0 * (b1[i] / t1[i]);
        if (t2[i] != 0) o += 2.0 * (b2[i] / t2[i]);
        if (t3[i] != 0) o += b3[i] / t3[i];
        out[i] = 100.0 * (o / 7.0);
    }
}

/// Awesome oscillator: SMA(hl2, fast) - SMA(hl2, slow); defaults 5/34.
pub fn ao(high: []const f64, low: []const f64, fast: usize, slow: usize, out: []f64, allocator: Allocator) Error!void {
    const n = high.len;
    if (low.len != n or out.len != n) return Error.LengthMismatch;
    const mid = try allocator.alloc(f64, n);
    defer allocator.free(mid);
    medianPrice(high, low, mid);
    const f = try allocator.alloc(f64, n);
    defer allocator.free(f);
    try sma(mid, fast, f);
    try sma(mid, slow, out);
    vecSub(f, out, out);
}

/// True strength index: 100 * EMA(EMA(mom, long), short) / EMA(EMA(|mom|, long), short)
/// plus a signal EMA of the TSI.
pub fn tsi(input: []const f64, long: usize, short: usize, signal: usize, out_tsi: []f64, out_signal: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out_tsi.len != n or out_signal.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out_tsi);
        fillNan(out_signal);
        return;
    }
    const m = try allocator.alloc(f64, n);
    defer allocator.free(m);
    const a = try allocator.alloc(f64, n);
    defer allocator.free(a);
    m[0] = nan;
    a[0] = nan;
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const d = load(input, i) - load(input, i - 1);
        store(m, i, d);
        store(a, i, @abs(d));
    }
    while (i < n) : (i += 1) {
        m[i] = input[i] - input[i - 1];
        a[i] = @abs(m[i]);
    }
    try ema(m, long, m);
    try ema(m, short, m);
    try ema(a, long, a);
    try ema(a, short, a);
    const hundred = splat(100.0);
    i = 0;
    while (i + lanes <= n) : (i += lanes) store(out_tsi, i, hundred * load(m, i) / load(a, i));
    while (i < n) : (i += 1) out_tsi[i] = 100.0 * m[i] / a[i];
    try ema(out_tsi, signal, out_signal);
}

/// Balance of power: (close - open) / (high - low), 0 when high == low.
pub fn bop(open: []const f64, high: []const f64, low: []const f64, close: []const f64, out: []f64) void {
    const n = out.len;
    const zero = splat(0);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const range = load(high, i) - load(low, i);
        const v = (load(close, i) - load(open, i)) / range;
        store(out, i, @select(f64, range == zero, zero, v));
    }
    while (i < n) : (i += 1) {
        const range = high[i] - low[i];
        out[i] = if (range == 0) 0 else (close[i] - open[i]) / range;
    }
}

/// Detrended price oscillator: x[i - (period/2 + 1)] - SMA(period)[i].
pub fn dpo(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    try sma(input, period, out);
    const shift = period / 2 + 1;
    const start = @max(period - 1, shift);
    if (start >= n) {
        fillNan(out);
        return;
    }
    fillNanRange(out, 0, start);
    var i: usize = start;
    while (i + lanes <= n) : (i += lanes) store(out, i, load(input, i - shift) - load(out, i));
    while (i < n) : (i += 1) out[i] = input[i - shift] - out[i];
}

// ---------------------------------------------------------------------------
// Volatility
// ---------------------------------------------------------------------------

/// Average true range (Wilder). TR needs a previous close, so the first ATR is
/// at index `period` (TA-Lib lookback).
pub fn atr(high: []const f64, low: []const f64, close: []const f64, period: usize, out: []f64) Error!void {
    const n = close.len;
    if (period == 0) return Error.InvalidPeriod;
    if (high.len != n or low.len != n or out.len != n) return Error.LengthMismatch;
    trueRange(high, low, close, out);
    // out[0] is NaN, so RMA's firstValid() seeds from index 1 and the first
    // output lands at index period.
    try rma(out, period, out);
}

/// Normalised ATR: 100 * ATR / close.
pub fn natr(high: []const f64, low: []const f64, close: []const f64, period: usize, out: []f64) Error!void {
    try atr(high, low, close, period, out);
    const hundred = splat(100.0);
    var i: usize = 0;
    while (i + lanes <= out.len) : (i += lanes) store(out, i, hundred * load(out, i) / load(close, i));
    while (i < out.len) : (i += 1) out[i] = 100.0 * out[i] / close[i];
}

/// Bollinger bands: middle = SMA, upper/lower = middle +/- k * population
/// stddev, %B = (close - lower)/(upper - lower), bandwidth = (upper-lower)/middle.
pub fn bbands(input: []const f64, period: usize, k: f64, out_upper: []f64, out_middle: []f64, out_lower: []f64, out_pct_b: []f64, out_width: []f64) Error!void {
    const n = input.len;
    if (out_upper.len != n or out_middle.len != n or out_lower.len != n or out_pct_b.len != n or out_width.len != n) return Error.LengthMismatch;
    try rollingMoments(input, period, 0, out_middle, out_upper); // var in upper
    const kv = splat(k);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const m = load(out_middle, i);
        const dev = @sqrt(load(out_upper, i)) * kv;
        const up = m + dev;
        const lo = m - dev;
        store(out_upper, i, up);
        store(out_lower, i, lo);
        store(out_pct_b, i, (load(input, i) - lo) / (up - lo));
        store(out_width, i, (up - lo) / m);
    }
    while (i < n) : (i += 1) {
        const m = out_middle[i];
        const dev = @sqrt(out_upper[i]) * k;
        out_upper[i] = m + dev;
        out_lower[i] = m - dev;
        out_pct_b[i] = (input[i] - out_lower[i]) / (out_upper[i] - out_lower[i]);
        out_width[i] = (out_upper[i] - out_lower[i]) / m;
    }
}

/// Keltner channels: middle = EMA(close, period), bands = middle +/- mult * ATR(atr_period).
pub fn keltner(high: []const f64, low: []const f64, close: []const f64, period: usize, atr_period: usize, mult: f64, out_upper: []f64, out_middle: []f64, out_lower: []f64) Error!void {
    const n = close.len;
    if (out_upper.len != n or out_middle.len != n or out_lower.len != n) return Error.LengthMismatch;
    try ema(close, period, out_middle);
    try atr(high, low, close, atr_period, out_upper); // atr in upper
    const mv = splat(mult);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const m = load(out_middle, i);
        const a = load(out_upper, i) * mv;
        store(out_upper, i, m + a);
        store(out_lower, i, m - a);
    }
    while (i < n) : (i += 1) {
        const a = out_upper[i] * mult;
        out_upper[i] = out_middle[i] + a;
        out_lower[i] = out_middle[i] - a;
    }
}

/// Donchian channels: upper = max(high), lower = min(low), middle = average.
pub fn donchian(high: []const f64, low: []const f64, period: usize, out_upper: []f64, out_middle: []f64, out_lower: []f64, allocator: Allocator) Error!void {
    const n = high.len;
    if (low.len != n or out_upper.len != n or out_middle.len != n or out_lower.len != n) return Error.LengthMismatch;
    const scratch = try allocator.alloc(f64, 2 * n);
    defer allocator.free(scratch);
    try rollingExtreme(true, high, period, out_upper, scratch);
    try rollingExtreme(false, low, period, out_lower, scratch);
    const half = splat(0.5);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) store(out_middle, i, (load(out_upper, i) + load(out_lower, i)) * half);
    while (i < n) : (i += 1) out_middle[i] = (out_upper[i] + out_lower[i]) * 0.5;
}

/// Historical (realised) volatility: sample stddev of 1-period log returns
/// over `period`, scaled by sqrt(periods_per_year) (<= 0 -> no scaling).
pub fn histVol(input: []const f64, period: usize, periods_per_year: f64, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (period < 2) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const lr = try allocator.alloc(f64, n);
    defer allocator.free(lr);
    try logReturns(input, 1, lr);
    try stddev(lr[1..], period, 1, out[1..]);
    out[0] = nan;
    const scale: f64 = if (periods_per_year > 0) @sqrt(periods_per_year) else 1.0;
    vecScale(out, scale, out);
}

// ---------------------------------------------------------------------------
// Trend
// ---------------------------------------------------------------------------

/// Directional movement system (TA-Lib algorithm): ADX, +DI, -DI.
/// +DI/-DI are defined from index `period`, ADX from 2*period-1.
pub fn adx(high: []const f64, low: []const f64, close: []const f64, period: usize, out_adx: []f64, out_plus_di: []f64, out_minus_di: []f64) Error!void {
    const n = close.len;
    if (period < 2) return Error.InvalidPeriod;
    if (high.len != n or low.len != n or out_adx.len != n or out_plus_di.len != n or out_minus_di.len != n) return Error.LengthMismatch;
    fillNan(out_adx);
    fillNan(out_plus_di);
    fillNan(out_minus_di);
    const p = period;
    if (n < p + 1) return;
    const pf: f64 = @floatFromInt(p);
    var prev_high = high[0];
    var prev_low = low[0];
    var prev_close = close[0];
    var sum_plus: f64 = 0;
    var sum_minus: f64 = 0;
    var sum_tr: f64 = 0;
    // Initial accumulation over period-1 bars (raw sums).
    var today: usize = 1;
    while (today < p) : (today += 1) {
        const h = high[today];
        const l = low[today];
        const diff_p = h - prev_high;
        const diff_m = prev_low - l;
        prev_high = h;
        prev_low = l;
        if (diff_m > 0 and diff_p < diff_m) {
            sum_minus += diff_m;
        } else if (diff_p > 0 and diff_p > diff_m) {
            sum_plus += diff_p;
        }
        const tr = @max(h - l, @max(@abs(h - prev_close), @abs(l - prev_close)));
        sum_tr += tr;
        prev_close = close[today];
    }
    var sum_dx: f64 = 0;
    var dx_count: usize = 0;
    var prev_adx: f64 = nan;
    while (today < n) : (today += 1) {
        const h = high[today];
        const l = low[today];
        const diff_p = h - prev_high;
        const diff_m = prev_low - l;
        prev_high = h;
        prev_low = l;
        sum_minus -= sum_minus / pf;
        sum_plus -= sum_plus / pf;
        if (diff_m > 0 and diff_p < diff_m) {
            sum_minus += diff_m;
        } else if (diff_p > 0 and diff_p > diff_m) {
            sum_plus += diff_p;
        }
        const tr = @max(h - l, @max(@abs(h - prev_close), @abs(l - prev_close)));
        sum_tr = sum_tr - sum_tr / pf + tr;
        prev_close = close[today];
        var dx: f64 = 0;
        var minus_di: f64 = 0;
        var plus_di: f64 = 0;
        if (sum_tr > 0) {
            minus_di = 100.0 * (sum_minus / sum_tr);
            plus_di = 100.0 * (sum_plus / sum_tr);
            const s = minus_di + plus_di;
            if (s != 0) dx = 100.0 * (@abs(minus_di - plus_di) / s);
        }
        out_plus_di[today] = plus_di;
        out_minus_di[today] = minus_di;
        if (dx_count < p) {
            sum_dx += dx;
            dx_count += 1;
            if (dx_count == p) {
                prev_adx = sum_dx / pf;
                out_adx[today] = prev_adx;
            }
        } else {
            prev_adx = (prev_adx * (pf - 1.0) + dx) / pf;
            out_adx[today] = prev_adx;
        }
    }
}

/// Aroon up/down/oscillator over a window of period+1 bars (TA-Lib).
pub fn aroon(high: []const f64, low: []const f64, period: usize, out_up: []f64, out_down: []f64, out_osc: []f64) Error!void {
    const n = high.len;
    if (period == 0) return Error.InvalidPeriod;
    if (low.len != n or out_up.len != n or out_down.len != n or out_osc.len != n) return Error.LengthMismatch;
    fillNan(out_up);
    fillNan(out_down);
    fillNan(out_osc);
    if (n < period + 1) return;
    const p = period;
    const factor = 100.0 / @as(f64, @floatFromInt(p));
    var lowest_idx: usize = 0;
    var highest_idx: usize = 0;
    var lowest: f64 = 0;
    var highest: f64 = 0;
    var have: bool = false;
    var today: usize = p;
    while (today < n) : (today += 1) {
        const trailing = today - p;
        if (!have or lowest_idx < trailing) {
            lowest_idx = trailing;
            lowest = low[trailing];
            var i = trailing + 1;
            while (i <= today) : (i += 1) {
                if (low[i] <= lowest) {
                    lowest_idx = i;
                    lowest = low[i];
                }
            }
        } else if (low[today] <= lowest) {
            lowest_idx = today;
            lowest = low[today];
        }
        if (!have or highest_idx < trailing) {
            highest_idx = trailing;
            highest = high[trailing];
            var i = trailing + 1;
            while (i <= today) : (i += 1) {
                if (high[i] >= highest) {
                    highest_idx = i;
                    highest = high[i];
                }
            }
        } else if (high[today] >= highest) {
            highest_idx = today;
            highest = high[today];
        }
        have = true;
        const up = factor * @as(f64, @floatFromInt(p - (today - highest_idx)));
        const down = factor * @as(f64, @floatFromInt(p - (today - lowest_idx)));
        out_up[today] = up;
        out_down[today] = down;
        out_osc[today] = up - down;
    }
}

/// Parabolic SAR (TA-Lib algorithm). `out_dir` is +1 while long (SAR below
/// price) and -1 while short. Defined from index 1.
pub fn psar(high: []const f64, low: []const f64, accel: f64, max_accel: f64, out_sar: []f64, out_dir: []f64) Error!void {
    const n = high.len;
    if (low.len != n or out_sar.len != n or out_dir.len != n) return Error.LengthMismatch;
    if (accel < 0 or max_accel < 0) return Error.InvalidParameter;
    fillNan(out_sar);
    fillNan(out_dir);
    if (n < 2) return;
    var af = accel;
    if (af > max_accel) af = max_accel;
    const accel_base = af;
    // Initial direction from the first bar's -DM (TA-Lib).
    const diff_p = high[1] - high[0];
    const diff_m = low[0] - low[1];
    var is_long = !(diff_m > 0 and diff_p < diff_m);
    var ep: f64 = undefined;
    var sar: f64 = undefined;
    if (is_long) {
        ep = high[1];
        sar = low[0];
    } else {
        ep = low[1];
        sar = high[0];
    }
    var new_low = low[1];
    var new_high = high[1];
    var today: usize = 1;
    while (today < n) : (today += 1) {
        const prev_low = new_low;
        const prev_high = new_high;
        new_low = low[today];
        new_high = high[today];
        if (is_long) {
            if (new_low <= sar) {
                // switch to short
                is_long = false;
                sar = ep;
                if (sar < prev_high) sar = prev_high;
                if (sar < new_high) sar = new_high;
                out_sar[today] = sar;
                out_dir[today] = -1;
                af = accel_base;
                ep = new_low;
                sar = sar + af * (ep - sar);
                if (sar < prev_high) sar = prev_high;
                if (sar < new_high) sar = new_high;
            } else {
                out_sar[today] = sar;
                out_dir[today] = 1;
                if (new_high > ep) {
                    ep = new_high;
                    af += accel;
                    if (af > max_accel) af = max_accel;
                }
                sar = sar + af * (ep - sar);
                if (sar > prev_low) sar = prev_low;
                if (sar > new_low) sar = new_low;
            }
        } else {
            if (new_high >= sar) {
                // switch to long
                is_long = true;
                sar = ep;
                if (sar > prev_low) sar = prev_low;
                if (sar > new_low) sar = new_low;
                out_sar[today] = sar;
                out_dir[today] = 1;
                af = accel_base;
                ep = new_high;
                sar = sar + af * (ep - sar);
                if (sar > prev_low) sar = prev_low;
                if (sar > new_low) sar = new_low;
            } else {
                out_sar[today] = sar;
                out_dir[today] = -1;
                if (new_low < ep) {
                    ep = new_low;
                    af += accel;
                    if (af > max_accel) af = max_accel;
                }
                sar = sar + af * (ep - sar);
                if (sar < prev_high) sar = prev_high;
                if (sar < new_high) sar = new_high;
            }
        }
    }
}

/// Supertrend (ATR bands with carry-forward). `out_dir` is +1 bullish
/// (line below price) / -1 bearish. Defined from index `period`.
pub fn supertrend(high: []const f64, low: []const f64, close: []const f64, period: usize, mult: f64, out_line: []f64, out_dir: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (high.len != n or low.len != n or out_line.len != n or out_dir.len != n) return Error.LengthMismatch;
    fillNan(out_line);
    fillNan(out_dir);
    const a = try allocator.alloc(f64, n);
    defer allocator.free(a);
    try atr(high, low, close, period, a);
    if (n <= period) return;
    var final_upper: f64 = undefined;
    var final_lower: f64 = undefined;
    var dir: f64 = 1;
    var i: usize = period;
    while (i < n) : (i += 1) {
        const hl2 = (high[i] + low[i]) * 0.5;
        const basic_upper = hl2 + mult * a[i];
        const basic_lower = hl2 - mult * a[i];
        if (i == period) {
            final_upper = basic_upper;
            final_lower = basic_lower;
            dir = if (close[i] > basic_upper) 1 else if (close[i] < basic_lower) -1 else 1;
        } else {
            const prev_close = close[i - 1];
            const upper = if (basic_upper < final_upper or prev_close > final_upper) basic_upper else final_upper;
            const lower = if (basic_lower > final_lower or prev_close < final_lower) basic_lower else final_lower;
            if (close[i] > final_upper) {
                dir = 1;
            } else if (close[i] < final_lower) {
                dir = -1;
            }
            final_upper = upper;
            final_lower = lower;
        }
        out_dir[i] = dir;
        out_line[i] = if (dir > 0) final_lower else final_upper;
    }
}

/// Vortex indicator: VI+ = sum|H - L[-1]| / sum(TR), VI- = sum|L - H[-1]| / sum(TR).
pub fn vortex(high: []const f64, low: []const f64, close: []const f64, period: usize, out_plus: []f64, out_minus: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (period == 0) return Error.InvalidPeriod;
    if (high.len != n or low.len != n or out_plus.len != n or out_minus.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out_plus);
        fillNan(out_minus);
        return;
    }
    const tr = try allocator.alloc(f64, n);
    defer allocator.free(tr);
    trueRange(high, low, close, tr);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        store(out_plus, i, @abs(load(high, i) - load(low, i - 1)));
        store(out_minus, i, @abs(load(low, i) - load(high, i - 1)));
    }
    while (i < n) : (i += 1) {
        out_plus[i] = @abs(high[i] - low[i - 1]);
        out_minus[i] = @abs(low[i] - high[i - 1]);
    }
    try rollingSum(out_plus[1..], period, out_plus[1..]);
    try rollingSum(out_minus[1..], period, out_minus[1..]);
    try rollingSum(tr[1..], period, tr[1..]);
    out_plus[0] = nan;
    out_minus[0] = nan;
    vecDiv(out_plus, tr, out_plus);
    vecDiv(out_minus, tr, out_minus);
}

/// Ichimoku cloud. Senkou A/B are shifted forward by `displacement` (value at
/// bar i is the cloud drawn at bar i), chikou is close shifted back.
pub fn ichimoku(high: []const f64, low: []const f64, close: []const f64, tenkan_p: usize, kijun_p: usize, senkou_p: usize, displacement: usize, out_tenkan: []f64, out_kijun: []f64, out_senkou_a: []f64, out_senkou_b: []f64, out_chikou: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (high.len != n or low.len != n) return Error.LengthMismatch;
    if (out_tenkan.len != n or out_kijun.len != n or out_senkou_a.len != n or out_senkou_b.len != n or out_chikou.len != n) return Error.LengthMismatch;
    const scratch = try allocator.alloc(f64, 3 * n);
    defer allocator.free(scratch);
    const tmp = scratch[2 * n .. 3 * n];
    const half = splat(0.5);
    const Mid = struct {
        fn run(h: []const f64, l: []const f64, p: usize, out: []f64, t: []f64, sc: []f64) Error!void {
            try rollingExtreme(true, h, p, out, sc);
            try rollingExtreme(false, l, p, t, sc);
            var i: usize = 0;
            while (i + lanes <= out.len) : (i += lanes) store(out, i, (load(out, i) + load(t, i)) * half);
            while (i < out.len) : (i += 1) out[i] = (out[i] + t[i]) * 0.5;
        }
    };
    try Mid.run(high, low, tenkan_p, out_tenkan, tmp, scratch[0 .. 2 * n]);
    try Mid.run(high, low, kijun_p, out_kijun, tmp, scratch[0 .. 2 * n]);
    try Mid.run(high, low, senkou_p, tmp, out_senkou_b, scratch[0 .. 2 * n]); // raw senkou B in tmp
    // senkou A raw = (tenkan + kijun)/2, shifted forward.
    fillNan(out_senkou_a);
    fillNan(out_senkou_b);
    if (displacement < n) {
        var i: usize = displacement;
        while (i < n) : (i += 1) {
            out_senkou_a[i] = (out_tenkan[i - displacement] + out_kijun[i - displacement]) * 0.5;
            out_senkou_b[i] = tmp[i - displacement];
        }
    }
    fillNan(out_chikou);
    if (displacement < n) {
        @memcpy(out_chikou[0 .. n - displacement], close[displacement..]);
    }
}

/// Linear regression over the trailing window (x = 0..period-1, last bar at
/// x = period-1): fitted endpoint value, slope, intercept and r^2. Running
/// sums are re-seeded every few windows to bound rounding drift.
pub fn linreg(input: []const f64, period: usize, out_value: []f64, out_slope: []f64, out_intercept: []f64, out_r2: []f64) Error!void {
    const n = input.len;
    if (period < 2) return Error.InvalidPeriod;
    if (out_value.len != n or out_slope.len != n or out_intercept.len != n or out_r2.len != n) return Error.LengthMismatch;
    fillNan(out_value);
    fillNan(out_slope);
    fillNan(out_intercept);
    if (period > n) {
        fillNan(out_r2);
        return;
    }
    // variance of y from the stable Welford kernel (for r^2 denominator)
    try rollingMoments(input, period, 0, null, out_r2);
    const p = period;
    const pf: f64 = @floatFromInt(p);
    const sum_x = pf * (pf - 1.0) / 2.0;
    const sum_xx = (pf - 1.0) * pf * (2.0 * pf - 1.0) / 6.0;
    const denom_x = pf * sum_xx - sum_x * sum_x;
    const reseed_every: usize = @max(4 * p, 1024);
    var sum_y: f64 = 0;
    var sum_ky: f64 = 0;
    var anchor: f64 = 0;
    var since_seed: usize = 0;
    var i: usize = p - 1;
    while (i < n) : (i += 1) {
        if (i == p - 1 or since_seed >= reseed_every) {
            anchor = input[i + 1 - p];
            sum_y = 0;
            sum_ky = 0;
            for (0..p) |k| {
                const d = input[i + 1 - p + k] - anchor;
                sum_y += d;
                sum_ky += @as(f64, @floatFromInt(k)) * d;
            }
            since_seed = 0;
        } else {
            const y_old = input[i - p] - anchor;
            const y_new = input[i] - anchor;
            sum_ky = sum_ky - sum_y + y_old + (pf - 1.0) * y_new;
            sum_y = sum_y - y_old + y_new;
            since_seed += 1;
        }
        const cov = pf * sum_ky - sum_x * sum_y;
        const slope = cov / denom_x;
        const intercept = (sum_y - slope * sum_x) / pf + anchor;
        out_slope[i] = slope;
        out_intercept[i] = intercept;
        out_value[i] = intercept + slope * (pf - 1.0);
        const var_y = out_r2[i] * pf; // sum of squared deviations
        const denom = denom_x * var_y;
        out_r2[i] = if (denom > 0) (cov * cov) / (denom * pf) else 0;
    }
}

// ---------------------------------------------------------------------------
// Volume
// ---------------------------------------------------------------------------

/// On-balance volume, cumulative from the start of the window.
pub fn obv(close: []const f64, volume: []const f64, out: []f64) Error!void {
    const n = close.len;
    if (volume.len != n or out.len != n) return Error.LengthMismatch;
    if (n == 0) return;
    // signed volume (vectorised), then a compensated prefix sum.
    out[0] = volume[0];
    const zero = splat(0);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const d = load(close, i) - load(close, i - 1);
        const v = load(volume, i);
        const pos = @select(f64, d > zero, v, zero);
        const neg = @select(f64, d < zero, -v, zero);
        store(out, i, pos + neg);
    }
    while (i < n) : (i += 1) {
        const d = close[i] - close[i - 1];
        out[i] = if (d > 0) volume[i] else if (d < 0) -volume[i] else 0;
    }
    prefixSum(out, out);
}

/// VWAP of `price` (typical price if H/L provided by the caller). period == 0
/// gives the cumulative VWAP from the window start, otherwise rolling.
pub fn vwap(price: []const f64, volume: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = price.len;
    if (volume.len != n or out.len != n) return Error.LengthMismatch;
    const pv = try allocator.alloc(f64, n);
    defer allocator.free(pv);
    vecMul(price, volume, pv);
    if (period == 0) {
        prefixSum(pv, pv);
        prefixSum(volume, out);
    } else {
        try rollingSum(pv, period, pv);
        try rollingSum(volume, period, out);
    }
    vecDiv(pv, out, out);
}

/// Money flow index over `period` (TA-Lib). Defined from index `period`.
pub fn mfi(high: []const f64, low: []const f64, close: []const f64, volume: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (period == 0) return Error.InvalidPeriod;
    if (high.len != n or low.len != n or volume.len != n or out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const tp = try allocator.alloc(f64, n);
    defer allocator.free(tp);
    typicalPrice(high, low, close, tp);
    const pos = try allocator.alloc(f64, n);
    defer allocator.free(pos);
    const neg = try allocator.alloc(f64, n);
    defer allocator.free(neg);
    pos[0] = 0;
    neg[0] = 0;
    const zero = splat(0);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const cur = load(tp, i);
        const prev = load(tp, i - 1);
        const mf = cur * load(volume, i);
        store(pos, i, @select(f64, cur > prev, mf, zero));
        store(neg, i, @select(f64, cur < prev, mf, zero));
    }
    while (i < n) : (i += 1) {
        const mf = tp[i] * volume[i];
        pos[i] = if (tp[i] > tp[i - 1]) mf else 0;
        neg[i] = if (tp[i] < tp[i - 1]) mf else 0;
    }
    try rollingSum(pos[1..], period, pos[1..]);
    try rollingSum(neg[1..], period, neg[1..]);
    out[0] = nan;
    const hundred = splat(100.0);
    i = 1;
    while (i + lanes <= n) : (i += lanes) {
        const ps = load(pos, i);
        const tot = ps + load(neg, i);
        store(out, i, @select(f64, tot > zero, hundred * ps / tot, zero));
    }
    while (i < n) : (i += 1) {
        const tot = pos[i] + neg[i];
        out[i] = if (tot > 0) 100.0 * pos[i] / tot else 0;
    }
    fillNanRange(out, 0, @min(n, period));
}

/// Chaikin money flow: sum(CLV * V) / sum(V) over `period`.
pub fn cmf(high: []const f64, low: []const f64, close: []const f64, volume: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (high.len != n or low.len != n or volume.len != n or out.len != n) return Error.LengthMismatch;
    const mfv = try allocator.alloc(f64, n);
    defer allocator.free(mfv);
    clv(high, low, close, mfv);
    vecMul(mfv, volume, mfv);
    try rollingSum(mfv, period, mfv);
    try rollingSum(volume, period, out);
    vecDiv(mfv, out, out);
}

/// Chaikin accumulation/distribution line (cumulative from window start).
pub fn ad(high: []const f64, low: []const f64, close: []const f64, volume: []const f64, out: []f64) Error!void {
    const n = close.len;
    if (high.len != n or low.len != n or volume.len != n or out.len != n) return Error.LengthMismatch;
    clv(high, low, close, out);
    vecMul(out, volume, out);
    prefixSum(out, out);
}

/// Chaikin A/D oscillator: EMA(fast) - EMA(slow) of the A/D line, both EMAs
/// seeded with the first A/D value (TA-Lib). Defined from index slow-1.
pub fn adosc(high: []const f64, low: []const f64, close: []const f64, volume: []const f64, fast: usize, slow: usize, out: []f64, allocator: Allocator) Error!void {
    const n = close.len;
    if (fast == 0 or slow == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    const line = try allocator.alloc(f64, n);
    defer allocator.free(line);
    try ad(high, low, close, volume, line);
    fillNan(out);
    if (n == 0) return;
    var f = fast;
    var s = slow;
    if (f > s) {
        const t = f;
        f = s;
        s = t;
    }
    const kf = 2.0 / (@as(f64, @floatFromInt(f)) + 1.0);
    const ks = 2.0 / (@as(f64, @floatFromInt(s)) + 1.0);
    var ef = line[0];
    var es = line[0];
    var i: usize = 1;
    while (i < n) : (i += 1) {
        ef = (1.0 - kf) * ef + kf * line[i];
        es = (1.0 - ks) * es + ks * line[i];
        if (i >= s - 1) out[i] = ef - es;
    }
    if (s == 1) out[0] = 0;
}

/// Elder force index: EMA(period) of (close - close[-1]) * volume.
pub fn efi(close: []const f64, volume: []const f64, period: usize, out: []f64) Error!void {
    const n = close.len;
    if (volume.len != n or out.len != n) return Error.LengthMismatch;
    if (n == 0) return;
    out[0] = nan;
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) store(out, i, (load(close, i) - load(close, i - 1)) * load(volume, i));
    while (i < n) : (i += 1) out[i] = (close[i] - close[i - 1]) * volume[i];
    try ema(out, period, out);
}

// ---------------------------------------------------------------------------
// Statistics / risk
// ---------------------------------------------------------------------------

/// Rolling z-score: (x - SMA) / population stddev.
pub fn zscore(input: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (out.len != n) return Error.LengthMismatch;
    const mean = try allocator.alloc(f64, n);
    defer allocator.free(mean);
    try rollingMoments(input, period, 0, mean, out);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) store(out, i, (load(input, i) - load(mean, i)) / @sqrt(load(out, i)));
    while (i < n) : (i += 1) out[i] = (input[i] - mean[i]) / @sqrt(out[i]);
}

/// Percent rank: share (0..100) of the previous `period` values that are
/// <= the current value. Inner comparison is vectorised.
pub fn percentRank(input: []const f64, period: usize, out: []f64) Error!void {
    const n = input.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    fillNanRange(out, 0, @min(n, period));
    const one = splat(1.0);
    const zero = splat(0);
    const scale = 100.0 / @as(f64, @floatFromInt(period));
    var i: usize = period;
    while (i < n) : (i += 1) {
        const cur = splat(input[i]);
        const win = input[i - period .. i];
        var acc: V = zero;
        var k: usize = 0;
        while (k + lanes <= win.len) : (k += lanes) acc += @select(f64, load(win, k) <= cur, one, zero);
        var cnt = @reduce(.Add, acc);
        while (k < win.len) : (k += 1) {
            if (win[k] <= input[i]) cnt += 1;
        }
        out[i] = cnt * scale;
    }
}

/// Drawdown from the running maximum since the start of the window (<= 0).
pub fn drawdown(input: []const f64, out: []f64) Error!void {
    const n = input.len;
    if (out.len != n) return Error.LengthMismatch;
    var peak: f64 = -math.inf(f64);
    for (input, 0..) |x, i| {
        if (x > peak) peak = x;
        out[i] = x / peak - 1.0;
    }
}

/// Rolling Sharpe ratio of 1-period simple returns (risk-free 0, sample std),
/// scaled by sqrt(periods_per_year) when > 0. Defined from index `period`.
pub fn sharpe(input: []const f64, period: usize, periods_per_year: f64, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (period < 2) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const r = try allocator.alloc(f64, n);
    defer allocator.free(r);
    try returns(input, 1, r);
    const mean = try allocator.alloc(f64, n);
    defer allocator.free(mean);
    try rollingMoments(r[1..], period, 1, mean[1..], out[1..]);
    out[0] = nan;
    const scale: f64 = if (periods_per_year > 0) @sqrt(periods_per_year) else 1.0;
    var i: usize = 1;
    while (i < n) : (i += 1) out[i] = mean[i] / @sqrt(out[i]) * scale;
}

/// Rolling Sortino ratio: mean(r) / sqrt(mean(min(r,0)^2)), scaled like Sharpe.
pub fn sortino(input: []const f64, period: usize, periods_per_year: f64, out: []f64, allocator: Allocator) Error!void {
    const n = input.len;
    if (period < 2) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const r = try allocator.alloc(f64, n);
    defer allocator.free(r);
    try returns(input, 1, r);
    const dn = try allocator.alloc(f64, n);
    defer allocator.free(dn);
    dn[0] = 0;
    const zero = splat(0);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const m = @min(load(r, i), zero);
        store(dn, i, m * m);
    }
    while (i < n) : (i += 1) {
        const m = @min(r[i], 0.0);
        dn[i] = m * m;
    }
    try sma(r[1..], period, out[1..]); // mean returns
    try sma(dn[1..], period, dn[1..]); // mean downside squares
    out[0] = nan;
    const scale: f64 = if (periods_per_year > 0) @sqrt(periods_per_year) else 1.0;
    i = 1;
    while (i < n) : (i += 1) out[i] = out[i] / @sqrt(dn[i]) * scale;
}

/// Rolling Pearson correlation between two series over `period`.
pub fn correl(a: []const f64, b: []const f64, period: usize, out: []f64) Error!void {
    const n = a.len;
    if (period < 2) return Error.InvalidPeriod;
    if (b.len != n or out.len != n) return Error.LengthMismatch;
    fillNan(out);
    if (period > n) return;
    const p = period;
    const pf: f64 = @floatFromInt(p);
    const reseed_every: usize = @max(4 * p, 1024);
    var sx: f64 = 0;
    var sy: f64 = 0;
    var sxx: f64 = 0;
    var syy: f64 = 0;
    var sxy: f64 = 0;
    var ax: f64 = 0;
    var ay: f64 = 0;
    var since: usize = 0;
    var i: usize = p - 1;
    while (i < n) : (i += 1) {
        if (i == p - 1 or since >= reseed_every) {
            ax = a[i + 1 - p];
            ay = b[i + 1 - p];
            sx = 0;
            sy = 0;
            sxx = 0;
            syy = 0;
            sxy = 0;
            for (0..p) |k| {
                const x = a[i + 1 - p + k] - ax;
                const y = b[i + 1 - p + k] - ay;
                sx += x;
                sy += y;
                sxx += x * x;
                syy += y * y;
                sxy += x * y;
            }
            since = 0;
        } else {
            const xo = a[i - p] - ax;
            const yo = b[i - p] - ay;
            const xn = a[i] - ax;
            const yn = b[i] - ay;
            sx += xn - xo;
            sy += yn - yo;
            sxx += xn * xn - xo * xo;
            syy += yn * yn - yo * yo;
            sxy += xn * yn - xo * yo;
            since += 1;
        }
        const cov = pf * sxy - sx * sy;
        const vx = pf * sxx - sx * sx;
        const vy = pf * syy - sy * sy;
        const denom = vx * vy;
        out[i] = if (denom > 0) cov / @sqrt(denom) else 0;
    }
}

/// Rolling beta of `asset` relative to `benchmark`, computed on 1-period
/// simple returns: cov(r_a, r_b) / var(r_b). Defined from index `period`.
/// Sums are anchored on the window's first returns and re-seeded
/// periodically, so tiny non-centred returns do not cancel catastrophically.
pub fn beta(asset: []const f64, benchmark: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = asset.len;
    if (period < 2) return Error.InvalidPeriod;
    if (benchmark.len != n or out.len != n) return Error.LengthMismatch;
    fillNan(out);
    if (n < 2) return;
    const ra = try allocator.alloc(f64, n);
    defer allocator.free(ra);
    const rb = try allocator.alloc(f64, n);
    defer allocator.free(rb);
    try returns(asset, 1, ra);
    try returns(benchmark, 1, rb);
    const m = n - 1;
    if (period > m) return;
    const p = period;
    const pf: f64 = @floatFromInt(p);
    const reseed_every: usize = @max(4 * p, 1024);
    const x_s = rb[1..];
    const y_s = ra[1..];
    var sx: f64 = 0;
    var sy: f64 = 0;
    var sxx: f64 = 0;
    var sxy: f64 = 0;
    var ax: f64 = 0;
    var ay: f64 = 0;
    var since: usize = 0;
    var i: usize = p - 1;
    while (i < m) : (i += 1) {
        if (i == p - 1 or since >= reseed_every) {
            ax = x_s[i + 1 - p];
            ay = y_s[i + 1 - p];
            sx = 0;
            sy = 0;
            sxx = 0;
            sxy = 0;
            for (0..p) |k| {
                const x = x_s[i + 1 - p + k] - ax;
                const y = y_s[i + 1 - p + k] - ay;
                sx += x;
                sy += y;
                sxx += x * x;
                sxy += x * y;
            }
            since = 0;
        } else {
            const xo = x_s[i - p] - ax;
            const yo = y_s[i - p] - ay;
            const xn = x_s[i] - ax;
            const yn = y_s[i] - ay;
            sx += xn - xo;
            sy += yn - yo;
            sxx += xn * xn - xo * xo;
            sxy += xn * yn - xo * yo;
            since += 1;
        }
        const vx = pf * sxx - sx * sx;
        out[i + 1] = if (vx > 0) (pf * sxy - sx * sy) / vx else 0;
    }
}

/// Rolling skewness (population, g1) and excess kurtosis (population, g2)
/// of the input over `period`. Either output may be null. Every window is
/// evaluated with an exact two-pass mean so results do not depend on history.
pub fn skewKurt(input: []const f64, period: usize, out_skew: ?[]f64, out_kurt: ?[]f64, allocator: Allocator) Error!void {
    _ = allocator;
    const n = input.len;
    if (period < 3) return Error.InvalidPeriod;
    if (out_skew) |s| if (s.len != n) return Error.LengthMismatch;
    if (out_kurt) |k| if (k.len != n) return Error.LengthMismatch;
    if (out_skew) |s| fillNan(s);
    if (out_kurt) |k| fillNan(k);
    if (period > n) return;
    const pf: f64 = @floatFromInt(period);
    var i: usize = period - 1;
    while (i < n) : (i += 1) {
        const win = input[i + 1 - period .. i + 1];
        var acc1: V = splat(0);
        var k: usize = 0;
        while (k + lanes <= win.len) : (k += lanes) acc1 += load(win, k);
        var m = @reduce(.Add, acc1);
        while (k < win.len) : (k += 1) m += win[k];
        m /= pf;
        const mv = splat(m);
        var acc2: V = splat(0);
        var acc3: V = splat(0);
        var acc4: V = splat(0);
        k = 0;
        while (k + lanes <= win.len) : (k += lanes) {
            const d = load(win, k) - mv;
            const d2 = d * d;
            acc2 += d2;
            acc3 += d2 * d;
            acc4 += d2 * d2;
        }
        var m2 = @reduce(.Add, acc2);
        var m3 = @reduce(.Add, acc3);
        var m4 = @reduce(.Add, acc4);
        while (k < win.len) : (k += 1) {
            const d = win[k] - m;
            const d2 = d * d;
            m2 += d2;
            m3 += d2 * d;
            m4 += d2 * d2;
        }
        m2 /= pf;
        m3 /= pf;
        m4 /= pf;
        if (out_skew) |s| s[i] = if (m2 > 0) m3 / (m2 * @sqrt(m2)) else 0;
        if (out_kurt) |kk| kk[i] = if (m2 > 0) m4 / (m2 * m2) - 3.0 else 0;
    }
}

// ---------------------------------------------------------------------------
// Bars: resampling and Heikin-Ashi
// ---------------------------------------------------------------------------

pub const Bars = struct {
    ts: []i64,
    open: []f64,
    high: []f64,
    low: []f64,
    close: []f64,
    volume: []f64,
    count: []f64,
    /// Volume traded on the buy side (NaN when no side column was given).
    buy_volume: []f64,

    pub fn len(self: Bars) usize {
        return self.ts.len;
    }

    pub fn deinit(self: Bars, allocator: Allocator) void {
        allocator.free(self.ts);
        allocator.free(self.open);
        allocator.free(self.high);
        allocator.free(self.low);
        allocator.free(self.close);
        allocator.free(self.volume);
        allocator.free(self.count);
        allocator.free(self.buy_volume);
    }
};

fn floorDiv(a: i64, b: i64) i64 {
    const q = @divTrunc(a, b);
    return if ((@rem(a, b) != 0) and ((a < 0) != (b < 0))) q - 1 else q;
}

/// Aggregate records into OHLCV bars of `bucket` timestamp units. Timestamps
/// must be sorted ascending. When `open/high/low` are null the records are
/// treated as ticks (all derived from `close`). Buckets without records are
/// omitted. `volume` may be null (bar volume becomes the record count).
pub fn resampleOhlcv(ts: []const i64, open: ?[]const f64, high: ?[]const f64, low: ?[]const f64, close: []const f64, volume: ?[]const f64, bucket: i64, allocator: Allocator) Error!Bars {
    return resampleOhlcvSide(ts, open, high, low, close, volume, null, bucket, allocator);
}

/// Like `resampleOhlcv`; `side` (1 = buy, 0 = sell per record) additionally
/// yields the buy volume of every bar.
pub fn resampleOhlcvSide(ts: []const i64, open: ?[]const f64, high: ?[]const f64, low: ?[]const f64, close: []const f64, volume: ?[]const f64, side: ?[]const f64, bucket: i64, allocator: Allocator) Error!Bars {
    const n = ts.len;
    if (bucket <= 0) return Error.InvalidParameter;
    if (close.len != n) return Error.LengthMismatch;
    // Count buckets first (single pass) so we allocate exactly.
    var nb: usize = 0;
    var last_key: i64 = 0;
    for (ts, 0..) |t, i| {
        const key = floorDiv(t, bucket);
        if (i == 0 or key != last_key) nb += 1;
        last_key = key;
    }
    const out_ts = try allocator.alloc(i64, nb);
    errdefer allocator.free(out_ts);
    const o = try allocator.alloc(f64, nb);
    errdefer allocator.free(o);
    const h = try allocator.alloc(f64, nb);
    errdefer allocator.free(h);
    const l = try allocator.alloc(f64, nb);
    errdefer allocator.free(l);
    const c = try allocator.alloc(f64, nb);
    errdefer allocator.free(c);
    const v = try allocator.alloc(f64, nb);
    errdefer allocator.free(v);
    const cnt = try allocator.alloc(f64, nb);
    errdefer allocator.free(cnt);
    const bvol = try allocator.alloc(f64, nb);
    errdefer allocator.free(bvol);
    var b: usize = 0;
    var i: usize = 0;
    while (i < n) {
        const key = floorDiv(ts[i], bucket);
        out_ts[b] = key * bucket;
        const bo = if (open) |op| op[i] else close[i];
        var bh = if (high) |hp| hp[i] else close[i];
        var bl = if (low) |lp| lp[i] else close[i];
        var bc = close[i];
        const v0: f64 = if (volume) |vp| vp[i] else 1.0;
        var bv: f64 = v0;
        var bb: f64 = if (side) |sp| (if (sp[i] != 0) v0 else 0.0) else nan;
        var k: f64 = 1;
        i += 1;
        while (i < n and floorDiv(ts[i], bucket) == key) : (i += 1) {
            const hi = if (high) |hp| hp[i] else close[i];
            const lo = if (low) |lp| lp[i] else close[i];
            if (hi > bh) bh = hi;
            if (lo < bl) bl = lo;
            bc = close[i];
            const vi: f64 = if (volume) |vp| vp[i] else 1.0;
            bv += vi;
            if (side) |sp| {
                if (sp[i] != 0) bb += vi;
            }
            k += 1;
        }
        o[b] = bo;
        h[b] = bh;
        l[b] = bl;
        c[b] = bc;
        v[b] = bv;
        cnt[b] = k;
        bvol[b] = bb;
        b += 1;
    }
    return Bars{ .ts = out_ts, .open = o, .high = h, .low = l, .close = c, .volume = v, .count = cnt, .buy_volume = bvol };
}

/// Heikin-Ashi candles.
pub fn heikinAshi(open: []const f64, high: []const f64, low: []const f64, close: []const f64, out_open: []f64, out_high: []f64, out_low: []f64, out_close: []f64) Error!void {
    const n = close.len;
    if (open.len != n or high.len != n or low.len != n) return Error.LengthMismatch;
    if (out_open.len != n or out_high.len != n or out_low.len != n or out_close.len != n) return Error.LengthMismatch;
    if (n == 0) return;
    // ha_close is elementwise (SIMD); ha_open is a recurrence.
    const quarter = splat(0.25);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) store(out_close, i, (load(open, i) + load(high, i) + load(low, i) + load(close, i)) * quarter);
    while (i < n) : (i += 1) out_close[i] = (open[i] + high[i] + low[i] + close[i]) * 0.25;
    out_open[0] = (open[0] + close[0]) * 0.5;
    i = 1;
    while (i < n) : (i += 1) out_open[i] = (out_open[i - 1] + out_close[i - 1]) * 0.5;
    i = 0;
    while (i + lanes <= n) : (i += lanes) {
        const ho = load(out_open, i);
        const hc = load(out_close, i);
        store(out_high, i, @max(load(high, i), @max(ho, hc)));
        store(out_low, i, @min(load(low, i), @min(ho, hc)));
    }
    while (i < n) : (i += 1) {
        out_high[i] = @max(high[i], @max(out_open[i], out_close[i]));
        out_low[i] = @min(low[i], @min(out_open[i], out_close[i]));
    }
}

// ---------------------------------------------------------------------------
// Scalar summary / risk analytics over a window
// ---------------------------------------------------------------------------

pub const Summary = extern struct {
    count: u64,
    first: f64,
    last: f64,
    min: f64,
    max: f64,
    mean: f64,
    std: f64, // sample std of the series
    total_return: f64, // last/first - 1
    log_return: f64, // ln(last/first)
    ann_return: f64, // geometric, needs periods_per_year (else = total_return)
    ann_vol: f64, // sample std of 1-period log returns * sqrt(ppy)
    sharpe: f64, // mean(r)/std(r) * sqrt(ppy), simple returns, rf = 0
    sortino: f64,
    max_drawdown: f64, // <= 0
    max_drawdown_bars: f64, // longest stretch below a prior peak
    calmar: f64, // ann_return / |max_drawdown|
    skew: f64, // of simple returns (population)
    kurtosis: f64, // excess, population
    var_95: f64, // 5th percentile of simple returns (a negative number)
    cvar_95: f64, // mean of returns <= var_95
    win_rate: f64, // share of positive returns
    avg_gain: f64,
    avg_loss: f64, // negative number
    profit_factor: f64, // sum(gains)/|sum(losses)|
    best: f64,
    worst: f64,
    autocorr_1: f64, // lag-1 autocorrelation of returns
    hurst: f64, // Hurst exponent of log prices (variance-of-lags estimator)
    half_life: f64, // OU mean-reversion half-life in bars (NaN if none)
};

fn nanSummary() Summary {
    var s: Summary = undefined;
    inline for (@typeInfo(Summary).@"struct".fields) |f| {
        if (f.type == f64) @field(s, f.name) = nan;
    }
    s.count = 0;
    return s;
}

fn percentileSorted(sorted: []const f64, q: f64) f64 {
    if (sorted.len == 0) return nan;
    if (sorted.len == 1) return sorted[0];
    const pos = q * @as(f64, @floatFromInt(sorted.len - 1));
    const lo: usize = @intFromFloat(@floor(pos));
    const hi = @min(lo + 1, sorted.len - 1);
    const frac = pos - @as(f64, @floatFromInt(lo));
    return sorted[lo] + (sorted[hi] - sorted[lo]) * frac;
}

fn pearson(a: []const f64, b: []const f64) f64 {
    const n = a.len;
    if (n < 2) return nan;
    var ma: f64 = 0;
    var mb: f64 = 0;
    for (a, b) |x, y| {
        ma += x;
        mb += y;
    }
    ma /= @floatFromInt(n);
    mb /= @floatFromInt(n);
    var sxy: f64 = 0;
    var sxx: f64 = 0;
    var syy: f64 = 0;
    for (a, b) |x, y| {
        const dx = x - ma;
        const dy = y - mb;
        sxy += dx * dy;
        sxx += dx * dx;
        syy += dy * dy;
    }
    const d = sxx * syy;
    return if (d > 0) sxy / @sqrt(d) else 0;
}

/// Compute the full summary for a price series.
pub fn summary(input: []const f64, periods_per_year: f64, allocator: Allocator) Error!Summary {
    const n = input.len;
    var s = nanSummary();
    s.count = n;
    if (n == 0) return s;
    s.first = input[0];
    s.last = input[n - 1];
    // min/max/mean via SIMD reductions
    {
        var vmin: V = splat(math.inf(f64));
        var vmax: V = splat(-math.inf(f64));
        var vsum: V = splat(0);
        var i: usize = 0;
        while (i + lanes <= n) : (i += lanes) {
            const x = load(input, i);
            vmin = @min(vmin, x);
            vmax = @max(vmax, x);
            vsum += x;
        }
        var mn = @reduce(.Min, vmin);
        var mx = @reduce(.Max, vmax);
        var sum = @reduce(.Add, vsum);
        while (i < n) : (i += 1) {
            mn = @min(mn, input[i]);
            mx = @max(mx, input[i]);
            sum += input[i];
        }
        s.min = mn;
        s.max = mx;
        s.mean = sum / @as(f64, @floatFromInt(n));
    }
    if (n >= 2) {
        var m2: f64 = 0;
        for (input) |x| {
            const d = x - s.mean;
            m2 += d * d;
        }
        s.std = @sqrt(m2 / @as(f64, @floatFromInt(n - 1)));
    }
    s.total_return = s.last / s.first - 1.0;
    s.log_return = @log(s.last / s.first);
    if (periods_per_year > 0 and n >= 2) {
        const years = @as(f64, @floatFromInt(n - 1)) / periods_per_year;
        s.ann_return = math.pow(f64, s.last / s.first, 1.0 / years) - 1.0;
    } else {
        s.ann_return = s.total_return;
    }
    // drawdown
    {
        var peak = input[0];
        var mdd: f64 = 0;
        var under: usize = 0;
        var longest: usize = 0;
        for (input) |x| {
            if (x >= peak) {
                peak = x;
                under = 0;
            } else {
                under += 1;
                if (under > longest) longest = under;
            }
            const dd = x / peak - 1.0;
            if (dd < mdd) mdd = dd;
        }
        s.max_drawdown = mdd;
        s.max_drawdown_bars = @floatFromInt(longest);
        s.calmar = if (mdd < 0) s.ann_return / -mdd else nan;
    }
    if (n < 2) return s;
    const m = n - 1;
    const r = try allocator.alloc(f64, m);
    defer allocator.free(r);
    const lr = try allocator.alloc(f64, m);
    defer allocator.free(lr);
    {
        const one = splat(1.0);
        var i: usize = 0;
        while (i + lanes <= m) : (i += lanes) {
            const ratio = load(input, i + 1) / load(input, i);
            store(r, i, ratio - one);
            store(lr, i, @log(ratio));
        }
        while (i < m) : (i += 1) {
            const ratio = input[i + 1] / input[i];
            r[i] = ratio - 1.0;
            lr[i] = @log(ratio);
        }
    }
    const mf: f64 = @floatFromInt(m);
    var mean_r: f64 = 0;
    var mean_lr: f64 = 0;
    var gains: f64 = 0;
    var losses: f64 = 0;
    var n_gain: usize = 0;
    var n_loss: usize = 0;
    var best: f64 = -math.inf(f64);
    var worst: f64 = math.inf(f64);
    for (r, lr) |x, y| {
        mean_r += x;
        mean_lr += y;
        if (x > 0) {
            gains += x;
            n_gain += 1;
        } else if (x < 0) {
            losses += x;
            n_loss += 1;
        }
        if (x > best) best = x;
        if (x < worst) worst = x;
    }
    mean_r /= mf;
    mean_lr /= mf;
    s.best = best;
    s.worst = worst;
    s.win_rate = @as(f64, @floatFromInt(n_gain)) / mf;
    s.avg_gain = if (n_gain > 0) gains / @as(f64, @floatFromInt(n_gain)) else 0;
    s.avg_loss = if (n_loss > 0) losses / @as(f64, @floatFromInt(n_loss)) else 0;
    s.profit_factor = if (losses < 0) gains / -losses else if (gains > 0) math.inf(f64) else nan;
    var m2r: f64 = 0;
    var m3r: f64 = 0;
    var m4r: f64 = 0;
    var m2lr: f64 = 0;
    var down2: f64 = 0;
    for (r, lr) |x, y| {
        const d = x - mean_r;
        const d2 = d * d;
        m2r += d2;
        m3r += d2 * d;
        m4r += d2 * d2;
        const dl = y - mean_lr;
        m2lr += dl * dl;
        const dn = @min(x, 0.0);
        down2 += dn * dn;
    }
    const scale: f64 = if (periods_per_year > 0) @sqrt(periods_per_year) else 1.0;
    if (m >= 2) {
        const std_r = @sqrt(m2r / (mf - 1.0));
        s.sharpe = if (std_r > 0) mean_r / std_r * scale else nan;
        s.ann_vol = @sqrt(m2lr / (mf - 1.0)) * scale;
    }
    const dd_dev = @sqrt(down2 / mf);
    s.sortino = if (dd_dev > 0) mean_r / dd_dev * scale else nan;
    const pv = m2r / mf;
    s.skew = if (pv > 0) (m3r / mf) / (pv * @sqrt(pv)) else 0;
    s.kurtosis = if (pv > 0) (m4r / mf) / (pv * pv) - 3.0 else 0;
    // VaR / CVaR (historical)
    {
        const sorted = try allocator.alloc(f64, m);
        defer allocator.free(sorted);
        @memcpy(sorted, r);
        std.sort.pdq(f64, sorted, {}, std.sort.asc(f64));
        const v = percentileSorted(sorted, 0.05);
        s.var_95 = v;
        var acc: f64 = 0;
        var cnt: usize = 0;
        for (sorted) |x| {
            if (x <= v) {
                acc += x;
                cnt += 1;
            } else break;
        }
        s.cvar_95 = if (cnt > 0) acc / @as(f64, @floatFromInt(cnt)) else v;
    }
    if (m >= 3) s.autocorr_1 = pearson(r[1..], r[0 .. m - 1]);
    // Hurst exponent on log prices, lags 2..min(20, n/4)
    if (n >= 40) {
        const lp = try allocator.alloc(f64, n);
        defer allocator.free(lp);
        {
            var i: usize = 0;
            while (i + lanes <= n) : (i += lanes) store(lp, i, @log(load(input, i)));
            while (i < n) : (i += 1) lp[i] = @log(input[i]);
        }
        const max_lag: usize = @min(20, n / 4);
        var sx: f64 = 0;
        var sy: f64 = 0;
        var sxx: f64 = 0;
        var sxy: f64 = 0;
        var k: f64 = 0;
        var lag: usize = 2;
        while (lag <= max_lag) : (lag += 1) {
            const cnt = n - lag;
            // mean and variance of the lagged differences (SIMD reductions)
            var acc: V = splat(0);
            var i: usize = 0;
            while (i + lanes <= cnt) : (i += lanes) acc += load(lp, i + lag) - load(lp, i);
            var mean_d = @reduce(.Add, acc);
            while (i < cnt) : (i += 1) mean_d += lp[i + lag] - lp[i];
            mean_d /= @floatFromInt(cnt);
            const mv = splat(mean_d);
            var acc2: V = splat(0);
            i = 0;
            while (i + lanes <= cnt) : (i += lanes) {
                const d = load(lp, i + lag) - load(lp, i) - mv;
                acc2 += d * d;
            }
            var var_d = @reduce(.Add, acc2);
            while (i < cnt) : (i += 1) {
                const d = (lp[i + lag] - lp[i]) - mean_d;
                var_d += d * d;
            }
            var_d /= @floatFromInt(cnt);
            if (var_d <= 0) continue;
            const x = @log(@as(f64, @floatFromInt(lag)));
            const y = 0.5 * @log(var_d);
            sx += x;
            sy += y;
            sxx += x * x;
            sxy += x * y;
            k += 1;
        }
        if (k >= 2) {
            const denom = k * sxx - sx * sx;
            s.hurst = if (denom != 0) (k * sxy - sx * sy) / denom else nan;
        }
    }
    // OU half-life: regress dx on x_{t-1}
    if (n >= 3) {
        var mx: f64 = 0;
        var md: f64 = 0;
        for (0..m) |i| {
            mx += input[i];
            md += input[i + 1] - input[i];
        }
        mx /= mf;
        md /= mf;
        var sxx: f64 = 0;
        var sxy: f64 = 0;
        for (0..m) |i| {
            const dx = input[i] - mx;
            sxx += dx * dx;
            sxy += dx * ((input[i + 1] - input[i]) - md);
        }
        if (sxx > 0) {
            const lambda = sxy / sxx;
            s.half_life = if (lambda < 0) -@log(2.0) / lambda else nan;
        }
    }
    return s;
}

// ---------------------------------------------------------------------------
// Microstructure (tick-level bid / ask / side)
// ---------------------------------------------------------------------------

/// Quoted spread: absolute and in basis points of the mid.
pub fn spread(bid: []const f64, ask: []const f64, out_abs: []f64, out_bps: []f64) void {
    const n = out_abs.len;
    const half = splat(0.5);
    const bps = splat(10_000.0);
    var i: usize = 0;
    while (i + lanes <= n) : (i += lanes) {
        const b = load(bid, i);
        const a = load(ask, i);
        const d = a - b;
        store(out_abs, i, d);
        store(out_bps, i, d / ((a + b) * half) * bps);
    }
    while (i < n) : (i += 1) {
        const d = ask[i] - bid[i];
        out_abs[i] = d;
        out_bps[i] = d / ((ask[i] + bid[i]) * 0.5) * 10_000.0;
    }
}

/// Order-flow over `period` rows: net signed volume (buy - sell) and the
/// imbalance (net / total volume, in [-1, 1]). Uses the per-row `side`
/// (1 = buy) on ticks or the bar-level `buy_volume`.
pub fn orderFlow(volume: []const f64, side: ?[]const f64, buy_volume: ?[]const f64, period: usize, out_net: []f64, out_imb: []f64, allocator: Allocator) Error!void {
    const n = volume.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out_net.len != n or out_imb.len != n) return Error.LengthMismatch;
    if (side == null and buy_volume == null) return Error.MissingColumn;
    const signed = try allocator.alloc(f64, n);
    defer allocator.free(signed);
    const zero = splat(0);
    const two = splat(2.0);
    var i: usize = 0;
    if (side) |sd| {
        // buy: +v, sell: -v
        while (i + lanes <= n) : (i += lanes) {
            const v = load(volume, i);
            store(signed, i, @select(f64, load(sd, i) != zero, v, -v));
        }
        while (i < n) : (i += 1) signed[i] = if (sd[i] != 0) volume[i] else -volume[i];
    } else {
        const bv = buy_volume.?;
        while (i + lanes <= n) : (i += lanes) store(signed, i, two * load(bv, i) - load(volume, i));
        while (i < n) : (i += 1) signed[i] = 2.0 * bv[i] - volume[i];
    }
    try rollingSum(signed, period, out_net);
    try rollingSum(volume, period, out_imb); // total in out_imb for now
    i = 0;
    while (i + lanes <= n) : (i += lanes) {
        const tot = load(out_imb, i);
        store(out_imb, i, @select(f64, tot == zero, zero, load(out_net, i) / tot));
    }
    while (i < n) : (i += 1) out_imb[i] = if (out_imb[i] == 0) 0 else out_net[i] / out_imb[i];
}

/// Tick-rule pressure: rolling mean of the trade tick sign (+1 up-tick,
/// -1 down-tick, zero-ticks carry the previous sign), in [-1, 1].
pub fn tickPressure(price: []const f64, period: usize, out: []f64) Error!void {
    const n = price.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n == 0) return;
    out[0] = 0;
    var sgn: f64 = 0;
    var i: usize = 1;
    while (i < n) : (i += 1) {
        const d = price[i] - price[i - 1];
        if (d > 0) sgn = 1 else if (d < 0) sgn = -1;
        out[i] = sgn;
    }
    if (n < 2) {
        fillNan(out);
        return;
    }
    try sma(out[1..], period, out[1..]);
    out[0] = nan;
}

/// Trades and volume per second over the last `period` rows.
/// `units_per_second` converts timestamp units (1e6 for microseconds).
pub fn tradeIntensity(time: []const i64, volume: []const f64, period: usize, units_per_second: f64, out_trades: []f64, out_volume: []f64) Error!void {
    const n = time.len;
    if (period == 0) return Error.InvalidPeriod;
    if (units_per_second <= 0) return Error.InvalidParameter;
    if (volume.len != n or out_trades.len != n or out_volume.len != n) return Error.LengthMismatch;
    try rollingSum(volume, period, out_volume);
    fillNanRange(out_trades, 0, @min(n, period));
    fillNanRange(out_volume, 0, @min(n, period));
    const pf: f64 = @floatFromInt(period);
    var i: usize = period;
    while (i < n) : (i += 1) {
        const dt = @as(f64, @floatFromInt(time[i] - time[i - period])) / units_per_second;
        if (dt > 0) {
            out_trades[i] = pf / dt;
            out_volume[i] = out_volume[i] / dt;
        } else {
            out_trades[i] = nan;
            out_volume[i] = nan;
        }
    }
}

/// Amihud illiquidity: rolling mean of |return| / (price * volume).
pub fn amihud(price: []const f64, volume: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = price.len;
    if (period == 0) return Error.InvalidPeriod;
    if (volume.len != n or out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const il = try allocator.alloc(f64, n);
    defer allocator.free(il);
    il[0] = nan;
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const c = load(price, i);
        const r = @abs(c / load(price, i - 1) - splat(1.0));
        store(il, i, r / (c * load(volume, i)));
    }
    while (i < n) : (i += 1) il[i] = @abs(price[i] / price[i - 1] - 1.0) / (price[i] * volume[i]);
    try sma(il[1..], period, out[1..]);
    out[0] = nan;
}

/// Realised volatility: root mean square of 1-row log returns over `period`,
/// scaled by sqrt(periods_per_year) when > 0.
pub fn realizedVol(price: []const f64, period: usize, periods_per_year: f64, out: []f64, allocator: Allocator) Error!void {
    const n = price.len;
    if (period == 0) return Error.InvalidPeriod;
    if (out.len != n) return Error.LengthMismatch;
    if (n < 2) {
        fillNan(out);
        return;
    }
    const sq = try allocator.alloc(f64, n);
    defer allocator.free(sq);
    sq[0] = nan;
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const lr = @log(load(price, i) / load(price, i - 1));
        store(sq, i, lr * lr);
    }
    while (i < n) : (i += 1) {
        const lr = @log(price[i] / price[i - 1]);
        sq[i] = lr * lr;
    }
    try sma(sq[1..], period, out[1..]);
    out[0] = nan;
    const scale: f64 = if (periods_per_year > 0) periods_per_year else 1.0;
    i = 0;
    while (i < n) : (i += 1) out[i] = @sqrt(out[i] * scale);
}

// ---------------------------------------------------------------------------
// Pairs
// ---------------------------------------------------------------------------

/// Rolling z-score of the ratio a/b (pairs trading spread).
pub fn ratioZscore(a: []const f64, b: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = a.len;
    if (b.len != n or out.len != n) return Error.LengthMismatch;
    const r = try allocator.alloc(f64, n);
    defer allocator.free(r);
    vecDiv(a, b, r);
    try zscore(r, period, out, allocator);
}

/// Relative strength: ROC(a, period) - ROC(b, period) in percent points.
pub fn relStrength(a: []const f64, b: []const f64, period: usize, out: []f64, allocator: Allocator) Error!void {
    const n = a.len;
    if (b.len != n or out.len != n) return Error.LengthMismatch;
    const rb = try allocator.alloc(f64, n);
    defer allocator.free(rb);
    try roc(a, period, out);
    try roc(b, period, rb);
    vecSub(out, rb, out);
}

/// Align series b onto the timestamps of a with an as-of join (latest b at or
/// before each a timestamp). Both timestamp arrays must be ascending.
/// out_b[i] = NaN when no b row exists at or before ts_a[i].
pub fn alignAsOf(ts_a: []const i64, ts_b: []const i64, b: []const f64, out_b: []f64) Error!void {
    if (ts_b.len != b.len or out_b.len != ts_a.len) return Error.LengthMismatch;
    var j: usize = 0;
    for (ts_a, 0..) |t, i| {
        while (j < ts_b.len and ts_b[j] <= t) j += 1;
        out_b[i] = if (j == 0) nan else b[j - 1];
    }
}

/// Inner join on equal timestamps. Fills `idx_a` / `idx_b` (capacity >= min
/// length) with the matching row indices and returns the number of matches.
pub fn alignInner(ts_a: []const i64, ts_b: []const i64, idx_a: []usize, idx_b: []usize) usize {
    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;
    while (i < ts_a.len and j < ts_b.len) {
        if (ts_a[i] == ts_b[j]) {
            if (k >= idx_a.len or k >= idx_b.len) break;
            idx_a[k] = i;
            idx_b[k] = j;
            k += 1;
            i += 1;
            j += 1;
        } else if (ts_a[i] < ts_b[j]) {
            i += 1;
        } else {
            j += 1;
        }
    }
    return k;
}

// ---------------------------------------------------------------------------
// Labels (look-ahead by design)
// ---------------------------------------------------------------------------

/// Forward return over `horizon` rows plus the maximum favourable / adverse
/// excursion within the horizon. NaN for the last `horizon` rows.
pub fn forwardReturn(price: []const f64, horizon: usize, out_ret: []f64, out_max: []f64, out_min: []f64, allocator: Allocator) Error!void {
    const n = price.len;
    if (horizon == 0) return Error.InvalidPeriod;
    if (out_ret.len != n or out_max.len != n or out_min.len != n) return Error.LengthMismatch;
    fillNan(out_ret);
    fillNan(out_max);
    fillNan(out_min);
    if (n <= horizon) return;
    const hi = try allocator.alloc(f64, n);
    defer allocator.free(hi);
    const lo = try allocator.alloc(f64, n);
    defer allocator.free(lo);
    try rollingMax(price, horizon, hi, allocator);
    try rollingMin(price, horizon, lo, allocator);
    const m = n - horizon;
    const one = splat(1.0);
    var i: usize = 0;
    while (i + lanes <= m) : (i += lanes) {
        const p0 = load(price, i);
        store(out_ret, i, load(price, i + horizon) / p0 - one);
        store(out_max, i, load(hi, i + horizon) / p0 - one);
        store(out_min, i, load(lo, i + horizon) / p0 - one);
    }
    while (i < m) : (i += 1) {
        out_ret[i] = price[i + horizon] / price[i] - 1.0;
        out_max[i] = hi[i + horizon] / price[i] - 1.0;
        out_min[i] = lo[i + horizon] / price[i] - 1.0;
    }
}

/// Triple-barrier labels: +1 if the price rises by `up` (fraction) before
/// falling by `down` within `horizon` rows, -1 the other way round, 0 when
/// neither barrier is hit (label = 0, ret = return at the horizon). Outputs
/// the label, the return at exit and the rows until exit. NaN when the
/// horizon is not available yet and no barrier was hit.
pub fn tripleBarrier(price: []const f64, horizon: usize, up: f64, down: f64, out_label: []f64, out_ret: []f64, out_bars: []f64) Error!void {
    const n = price.len;
    if (horizon == 0) return Error.InvalidPeriod;
    if (up <= 0 or down <= 0) return Error.InvalidParameter;
    if (out_label.len != n or out_ret.len != n or out_bars.len != n) return Error.LengthMismatch;
    fillNan(out_label);
    fillNan(out_ret);
    fillNan(out_bars);
    for (0..n) |i| {
        const p0 = price[i];
        var j: usize = i + 1;
        const end = @min(n, i + horizon + 1);
        var hit = false;
        while (j < end) : (j += 1) {
            const r = price[j] / p0 - 1.0;
            if (r >= up) {
                out_label[i] = 1;
                out_ret[i] = r;
                out_bars[i] = @floatFromInt(j - i);
                hit = true;
                break;
            }
            if (r <= -down) {
                out_label[i] = -1;
                out_ret[i] = r;
                out_bars[i] = @floatFromInt(j - i);
                hit = true;
                break;
            }
        }
        if (!hit and i + horizon < n) {
            out_label[i] = 0;
            out_ret[i] = price[i + horizon] / p0 - 1.0;
            out_bars[i] = @floatFromInt(horizon);
        }
    }
}

// ---------------------------------------------------------------------------
// Session-anchored kinds
// ---------------------------------------------------------------------------

/// Start timestamp of the session (of `len` timestamp units, starting at
/// `offset` within the day cycle) containing `t`.
pub fn sessionStart(t: i64, len: f64, offset: f64) i64 {
    const l: i128 = @intFromFloat(len);
    const o: i128 = @intFromFloat(offset);
    const start: i128 = @divFloor(@as(i128, t) - o, l) * l + o;
    if (start < std.math.minInt(i64)) return std.math.minInt(i64);
    if (start > std.math.maxInt(i64)) return std.math.maxInt(i64);
    return @intCast(start);
}

/// How rows are assigned to sessions: fixed-length cycles (`len` timestamp
/// units starting at `offset`) or explicit per-row session starts, e.g. from
/// a trading calendar (see calendar.zig and the storage layer).
pub const Sessions = union(enum) {
    fixed: struct { len: f64, offset: f64 },
    starts: []const i64,

    pub inline fn startOf(self: Sessions, time: []const i64, i: usize) i64 {
        return switch (self) {
            .fixed => |f| sessionStart(time[i], f.len, f.offset),
            .starts => |s| s[i],
        };
    }

    fn validate(self: Sessions, n: usize) Error!void {
        switch (self) {
            .fixed => |f| if (f.len <= 0) return Error.InvalidParameter,
            .starts => |s| if (s.len != n) return Error.LengthMismatch,
        }
    }
};

/// VWAP anchored at every session start.
pub fn sessionVwap(time: []const i64, price: []const f64, volume: []const f64, sess: Sessions, out: []f64) Error!void {
    const n = time.len;
    try sess.validate(n);
    if (price.len != n or volume.len != n or out.len != n) return Error.LengthMismatch;
    var cur: i64 = 0;
    var pv: f64 = 0;
    var vv: f64 = 0;
    for (0..n) |i| {
        const st = sess.startOf(time, i);
        if (i == 0 or st != cur) {
            cur = st;
            pv = 0;
            vv = 0;
        }
        pv += price[i] * volume[i];
        vv += volume[i];
        out[i] = pv / vv;
    }
}

/// Running session open / high / low and return since the session open.
pub fn sessionRange(time: []const i64, open: ?[]const f64, high: ?[]const f64, low: ?[]const f64, close: []const f64, sess: Sessions, out_open: []f64, out_high: []f64, out_low: []f64, out_ret: []f64) Error!void {
    const n = time.len;
    try sess.validate(n);
    if (close.len != n or out_open.len != n or out_high.len != n or out_low.len != n or out_ret.len != n) return Error.LengthMismatch;
    var cur: i64 = 0;
    var so: f64 = 0;
    var sh: f64 = 0;
    var sl: f64 = 0;
    for (0..n) |i| {
        const st = sess.startOf(time, i);
        const hi = if (high) |h| h[i] else close[i];
        const lo = if (low) |l| l[i] else close[i];
        if (i == 0 or st != cur) {
            cur = st;
            so = if (open) |o| o[i] else close[i];
            sh = hi;
            sl = lo;
        } else {
            if (hi > sh) sh = hi;
            if (lo < sl) sl = lo;
        }
        out_open[i] = so;
        out_high[i] = sh;
        out_low[i] = sl;
        out_ret[i] = close[i] / so - 1.0;
    }
}

/// Opening range: high / low of the first `rows` rows of each session, then
/// a breakout flag (+1 close above the range, -1 below, 0 inside; NaN while
/// the range is still forming).
pub fn openingRange(time: []const i64, high: []const f64, low: []const f64, close: []const f64, rows: usize, sess: Sessions, out_high: []f64, out_low: []f64, out_break: []f64) Error!void {
    const n = time.len;
    if (rows == 0) return Error.InvalidPeriod;
    try sess.validate(n);
    if (high.len != n or low.len != n or close.len != n or out_high.len != n or out_low.len != n or out_break.len != n) return Error.LengthMismatch;
    var cur: i64 = 0;
    var k: usize = 0;
    var rh: f64 = 0;
    var rl: f64 = 0;
    for (0..n) |i| {
        const st = sess.startOf(time, i);
        if (i == 0 or st != cur) {
            cur = st;
            k = 0;
            rh = high[i];
            rl = low[i];
        }
        if (k < rows) {
            if (high[i] > rh) rh = high[i];
            if (low[i] < rl) rl = low[i];
            out_high[i] = rh;
            out_low[i] = rl;
            out_break[i] = nan;
        } else {
            out_high[i] = rh;
            out_low[i] = rl;
            out_break[i] = if (close[i] > rh) 1 else if (close[i] < rl) -1 else 0;
        }
        k += 1;
    }
}

/// Classic floor pivots from the previous session's high / low / close.
pub fn pivots(time: []const i64, high: []const f64, low: []const f64, close: []const f64, sess: Sessions, out_pp: []f64, out_r1: []f64, out_s1: []f64, out_r2: []f64, out_s2: []f64) Error!void {
    const n = time.len;
    try sess.validate(n);
    if (high.len != n or low.len != n or close.len != n) return Error.LengthMismatch;
    if (out_pp.len != n or out_r1.len != n or out_s1.len != n or out_r2.len != n or out_s2.len != n) return Error.LengthMismatch;
    var cur: i64 = 0;
    var have_prev = false;
    var ph: f64 = 0;
    var pl: f64 = 0;
    var pc: f64 = 0;
    var sh: f64 = 0;
    var sl: f64 = 0;
    var sc: f64 = 0;
    var pp: f64 = nan;
    var r1: f64 = nan;
    var s1: f64 = nan;
    var r2: f64 = nan;
    var s2: f64 = nan;
    for (0..n) |i| {
        const st = sess.startOf(time, i);
        if (i == 0 or st != cur) {
            if (i > 0) {
                ph = sh;
                pl = sl;
                pc = sc;
                have_prev = true;
                pp = (ph + pl + pc) / 3.0;
                r1 = 2.0 * pp - pl;
                s1 = 2.0 * pp - ph;
                r2 = pp + (ph - pl);
                s2 = pp - (ph - pl);
            }
            cur = st;
            sh = high[i];
            sl = low[i];
        } else {
            if (high[i] > sh) sh = high[i];
            if (low[i] < sl) sl = low[i];
        }
        sc = close[i];
        out_pp[i] = if (have_prev) pp else nan;
        out_r1[i] = if (have_prev) r1 else nan;
        out_s1[i] = if (have_prev) s1 else nan;
        out_r2[i] = if (have_prev) r2 else nan;
        out_s2[i] = if (have_prev) s2 else nan;
    }
}

// ---------------------------------------------------------------------------
// Data health
// ---------------------------------------------------------------------------

/// Timestamp units -> UTC seconds (floor) for a database timestamp unit.
pub fn toSec(ts: i64, unit_ns: u64) i64 {
    const v: i128 = @as(i128, ts) * @as(i128, unit_ns);
    return @intCast(@divFloor(v, 1_000_000_000));
}

/// UTC seconds -> timestamp units (floor).
pub fn fromSec(sec: i64, unit_ns: u64) i64 {
    const v: i128 = @as(i128, sec) * 1_000_000_000;
    return @intCast(@divFloor(v, @as(i128, unit_ns)));
}

pub const Health = extern struct {
    count: u64,
    first_ts: i64,
    last_ts: i64,
    span: i64, // last - first
    mean_gap: f64,
    median_gap: f64,
    max_gap: i64,
    max_gap_at: i64, // timestamp of the row before the largest gap
    n_gaps: u64, // gaps larger than the threshold (session breaks / outages)
    n_nonpositive_price: u64,
    n_nan_price: u64,
    n_outlier_returns: u64, // |log return| above the threshold
    first_outlier_at: i64, // timestamp, or 0
    max_abs_return: f64,
    n_zero_volume: u64,
    n_negative_volume: u64,
    /// With a trading calendar: closed (non-session) time inside [first, last]
    /// in timestamp units; gaps are measured in trading time.
    closed_span: i64,
    /// Gaps between consecutive rows that span a session boundary.
    n_session_breaks: u64,
    /// Calendar sessions between the first and last row with no rows at all.
    n_missing_sessions: u64,
};

/// Basic data-quality statistics for a price (and optional volume) series.
/// With a trading calendar (`cal` and `unit_ns` > 0) gaps are measured in
/// trading time: closed periods (nights, weekends, holidays) are subtracted
/// before the threshold, mean, median and maximum are computed.
pub fn health(time: []const i64, price: []const f64, volume: ?[]const f64, gap_threshold: i64, outlier_threshold: f64, cal: ?*const calendar.Calendar, unit_ns: u64, allocator: Allocator) Error!Health {
    const n = time.len;
    var h = std.mem.zeroes(Health);
    h.count = n;
    if (n == 0) return h;
    h.first_ts = time[0];
    h.last_ts = time[n - 1];
    h.span = time[n - 1] - time[0];
    if (n >= 2) {
        const gaps = try allocator.alloc(i64, n - 1);
        defer allocator.free(gaps);
        var sum: i128 = 0;
        var max_gap: i64 = 0;
        var max_at: i64 = time[0];
        const use_cal = cal != null and unit_ns > 0;
        var cur: ?calendar.Session = null; // session containing the previous row (calendar mode)
        var closed_total: i128 = 0;
        if (use_cal) {
            const s0 = toSec(time[0], unit_ns);
            cur = cal.?.sessionAt(s0) orelse cal.?.prevSession(s0);
        }
        for (0..n - 1) |i| {
            var g = time[i + 1] - time[i];
            if (use_cal) {
                const a = toSec(time[i], unit_ns);
                const b = toSec(time[i + 1], unit_ns);
                var same = false;
                if (cur) |s| same = a >= s.open and b < s.close and a >= s.open;
                if (!same) {
                    const open_sec = cal.?.openSecondsBetween(a, b);
                    const closed_sec = (b - a) - open_sec;
                    if (closed_sec > 0) {
                        const closed_units = fromSec(closed_sec, unit_ns);
                        closed_total += closed_units;
                        g -= @intCast(@min(@as(i128, g), closed_units));
                        h.n_session_breaks += 1;
                        // sessions skipped entirely between the two rows
                        if (cur) |s| {
                            const next = cal.?.sessionAt(b) orelse cal.?.prevSession(b);
                            if (next) |nx| {
                                if (nx.open > s.open) {
                                    const between = cal.?.sessionsBetween(s.close, nx.open);
                                    h.n_missing_sessions += between;
                                }
                            }
                        }
                    }
                    cur = cal.?.sessionAt(b) orelse cal.?.prevSession(b);
                }
            }
            gaps[i] = g;
            sum += g;
            if (g > max_gap) {
                max_gap = g;
                max_at = time[i];
            }
            if (gap_threshold > 0 and g > gap_threshold) h.n_gaps += 1;
        }
        h.mean_gap = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(n - 1));
        h.max_gap = max_gap;
        h.max_gap_at = max_at;
        h.closed_span = @intCast(@min(closed_total, @as(i128, std.math.maxInt(i64))));
        std.sort.pdq(i64, gaps, {}, std.sort.asc(i64));
        const m = gaps.len;
        h.median_gap = if (m % 2 == 1) @floatFromInt(gaps[m / 2]) else (@as(f64, @floatFromInt(gaps[m / 2 - 1])) + @as(f64, @floatFromInt(gaps[m / 2]))) / 2.0;
    }
    var prev: f64 = nan;
    for (price, 0..) |x, i| {
        if (isNan(x)) {
            h.n_nan_price += 1;
            continue;
        }
        if (x <= 0) h.n_nonpositive_price += 1;
        if (!isNan(prev) and prev > 0 and x > 0) {
            const lr = @abs(@log(x / prev));
            if (lr > h.max_abs_return) h.max_abs_return = lr;
            if (outlier_threshold > 0 and lr > outlier_threshold) {
                if (h.n_outlier_returns == 0) h.first_outlier_at = time[i];
                h.n_outlier_returns += 1;
            }
        }
        prev = x;
    }
    if (volume) |v| {
        for (v) |x| {
            if (x == 0) h.n_zero_volume += 1;
            if (x < 0) h.n_negative_volume += 1;
        }
    }
    return h;
}

// ---------------------------------------------------------------------------
// Decision evaluation
// ---------------------------------------------------------------------------

pub const Decision = extern struct {
    timestamp: i64,
    direction: f64, // +1 long, -1 short, 0 flat
    size: f64, // position size in units of the evaluation currency
    horizon: i64, // exit after this many timestamp units (0 = default)
};

pub const Evaluation = extern struct {
    n_decisions: u64,
    n_evaluated: u64, // decisions with both entry and exit prices available
    n_long: u64,
    n_short: u64,
    hit_rate: f64, // share of evaluated decisions with positive net return
    avg_return: f64, // mean gross directional return
    avg_net_return: f64, // after costs
    total_pnl: f64, // sum(size * net return)
    total_cost: f64,
    sharpe: f64, // mean / std of net returns (per decision, not annualised)
    profit_factor: f64,
    max_drawdown: f64, // of cumulative pnl, in currency units
    avg_win: f64,
    avg_loss: f64,
    best: f64,
    worst: f64,
    long_hit_rate: f64,
    short_hit_rate: f64,
    long_avg_return: f64,
    short_avg_return: f64,
};

/// Evaluate directional decisions against a price series: entry at the first
/// price at or after the decision time, exit at the first price at or after
/// time + horizon, `cost_bps` charged per side. Optional per-decision outputs
/// (entry price, exit price, net return) must have `decisions.len` entries.
pub fn evaluate(time: []const i64, price: []const f64, decisions: []const Decision, default_horizon: i64, cost_bps: f64, out_entry: ?[]f64, out_exit: ?[]f64, out_net: ?[]f64) Error!Evaluation {
    var e = std.mem.zeroes(Evaluation);
    e.n_decisions = decisions.len;
    e.best = -math.inf(f64);
    e.worst = math.inf(f64);
    if (out_entry) |o| if (o.len != decisions.len) return Error.LengthMismatch;
    if (out_exit) |o| if (o.len != decisions.len) return Error.LengthMismatch;
    if (out_net) |o| if (o.len != decisions.len) return Error.LengthMismatch;
    const n = time.len;
    var sum_gross: f64 = 0;
    var sum_net: f64 = 0;
    var sum_net2: f64 = 0;
    var gains: f64 = 0;
    var losses: f64 = 0;
    var n_win: usize = 0;
    var n_loss: usize = 0;
    var long_hits: usize = 0;
    var short_hits: usize = 0;
    var long_sum: f64 = 0;
    var short_sum: f64 = 0;
    var n_long_eval: usize = 0;
    var n_short_eval: usize = 0;
    var cum: f64 = 0;
    var peak: f64 = 0;
    var mdd: f64 = 0;
    for (decisions, 0..) |d, k| {
        if (out_entry) |o| o[k] = nan;
        if (out_exit) |o| o[k] = nan;
        if (out_net) |o| o[k] = nan;
        if (d.direction == 0) continue;
        if (d.direction > 0) e.n_long += 1 else e.n_short += 1;
        const horizon = if (d.horizon > 0) d.horizon else default_horizon;
        if (horizon <= 0) continue;
        const ei = lowerBound(time, d.timestamp);
        if (ei >= n) continue;
        const xi = lowerBound(time, d.timestamp + horizon);
        if (xi >= n) continue;
        const entry = price[ei];
        const exit = price[xi];
        const dir: f64 = if (d.direction > 0) 1 else -1;
        const gross = dir * (exit / entry - 1.0);
        const cost = 2.0 * cost_bps / 10_000.0;
        const net = gross - cost;
        const pnl = d.size * net;
        if (out_entry) |o| o[k] = entry;
        if (out_exit) |o| o[k] = exit;
        if (out_net) |o| o[k] = net;
        e.n_evaluated += 1;
        e.total_cost += d.size * cost;
        e.total_pnl += pnl;
        sum_gross += gross;
        sum_net += net;
        sum_net2 += net * net;
        if (net > 0) {
            n_win += 1;
            gains += pnl;
        } else if (net < 0) {
            n_loss += 1;
            losses += pnl;
        }
        if (net > e.best) e.best = net;
        if (net < e.worst) e.worst = net;
        if (dir > 0) {
            n_long_eval += 1;
            long_sum += net;
            if (net > 0) long_hits += 1;
        } else {
            n_short_eval += 1;
            short_sum += net;
            if (net > 0) short_hits += 1;
        }
        cum += pnl;
        if (cum > peak) peak = cum;
        if (peak - cum > mdd) mdd = peak - cum;
    }
    const m: f64 = @floatFromInt(e.n_evaluated);
    if (e.n_evaluated == 0) {
        e.best = nan;
        e.worst = nan;
        e.hit_rate = nan;
        e.avg_return = nan;
        e.avg_net_return = nan;
        e.sharpe = nan;
        e.profit_factor = nan;
        e.avg_win = nan;
        e.avg_loss = nan;
        e.long_hit_rate = nan;
        e.short_hit_rate = nan;
        e.long_avg_return = nan;
        e.short_avg_return = nan;
        return e;
    }
    e.hit_rate = @as(f64, @floatFromInt(n_win)) / m;
    e.avg_return = sum_gross / m;
    e.avg_net_return = sum_net / m;
    const var_net = if (e.n_evaluated > 1) (sum_net2 - sum_net * sum_net / m) / (m - 1.0) else 0.0;
    e.sharpe = if (var_net > 0) e.avg_net_return / @sqrt(var_net) else nan;
    e.profit_factor = if (losses < 0) gains / -losses else if (gains > 0) math.inf(f64) else nan;
    e.max_drawdown = mdd;
    e.avg_win = if (n_win > 0) gains / @as(f64, @floatFromInt(n_win)) else 0;
    e.avg_loss = if (n_loss > 0) losses / @as(f64, @floatFromInt(n_loss)) else 0;
    e.long_hit_rate = if (n_long_eval > 0) @as(f64, @floatFromInt(long_hits)) / @as(f64, @floatFromInt(n_long_eval)) else nan;
    e.short_hit_rate = if (n_short_eval > 0) @as(f64, @floatFromInt(short_hits)) / @as(f64, @floatFromInt(n_short_eval)) else nan;
    e.long_avg_return = if (n_long_eval > 0) long_sum / @as(f64, @floatFromInt(n_long_eval)) else nan;
    e.short_avg_return = if (n_short_eval > 0) short_sum / @as(f64, @floatFromInt(n_short_eval)) else nan;
    return e;
}

/// First index with time[i] >= t (time ascending).
pub fn lowerBound(time: []const i64, t: i64) usize {
    var lo: usize = 0;
    var hi: usize = time.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (time[mid] < t) lo = mid + 1 else hi = mid;
    }
    return lo;
}

// ---------------------------------------------------------------------------
// Registry: kinds, specs, defaults, warm-up and dispatch
// ---------------------------------------------------------------------------

pub const Kind = enum(u32) {
    // moving averages
    sma = 1,
    ema = 2,
    wma = 3,
    dema = 4,
    tema = 5,
    trima = 6,
    kama = 7,
    hma = 8,
    zlema = 9,
    vwma = 10,
    rma = 11,
    // momentum
    rsi = 20,
    macd = 21,
    ppo = 22,
    stoch = 23,
    stoch_rsi = 24,
    cci = 25,
    willr = 26,
    mom = 27,
    roc = 28,
    cmo = 29,
    trix = 30,
    ultosc = 31,
    ao = 32,
    tsi = 33,
    bop = 34,
    dpo = 35,
    // trend
    adx = 40,
    aroon = 41,
    psar = 42,
    supertrend = 43,
    vortex = 44,
    ichimoku = 45,
    linreg = 46,
    // volatility
    atr = 60,
    natr = 61,
    true_range = 62,
    bbands = 63,
    keltner = 64,
    donchian = 65,
    stddev = 66,
    variance = 67,
    hist_vol = 68,
    // volume
    obv = 80,
    vwap = 81,
    mfi = 82,
    cmf = 83,
    ad = 84,
    adosc = 85,
    efi = 86,
    // statistics
    returns = 100,
    log_returns = 101,
    zscore = 102,
    percent_rank = 103,
    rolling_min = 104,
    rolling_max = 105,
    drawdown = 106,
    sharpe = 107,
    sortino = 108,
    correl = 109,
    beta = 110,
    skew = 111,
    kurtosis = 112,
    // price transforms
    typical_price = 120,
    median_price = 121,
    heikin_ashi = 122,
    // microstructure (ticks with bid / ask / side)
    spread = 130,
    order_flow = 131,
    tick_pressure = 132,
    trade_intensity = 133,
    amihud = 134,
    realized_vol = 135,
    // pairs / passthrough
    series = 140,
    series2 = 141,
    ratio = 142,
    ratio_zscore = 143,
    rel_strength = 144,
    // labels (look-ahead by design)
    forward_return = 150,
    triple_barrier = 151,
    // session-anchored
    session_vwap = 160,
    session_range = 161,
    opening_range = 162,
    pivots = 163,
    _,

    pub fn fromInt(v: u32) ?Kind {
        const k: Kind = @enumFromInt(v);
        return switch (k) {
            .sma, .ema, .wma, .dema, .tema, .trima, .kama, .hma, .zlema, .vwma, .rma, .rsi, .macd, .ppo, .stoch, .stoch_rsi, .cci, .willr, .mom, .roc, .cmo, .trix, .ultosc, .ao, .tsi, .bop, .dpo, .adx, .aroon, .psar, .supertrend, .vortex, .ichimoku, .linreg, .atr, .natr, .true_range, .bbands, .keltner, .donchian, .stddev, .variance, .hist_vol, .obv, .vwap, .mfi, .cmf, .ad, .adosc, .efi, .returns, .log_returns, .zscore, .percent_rank, .rolling_min, .rolling_max, .drawdown, .sharpe, .sortino, .correl, .beta, .skew, .kurtosis, .typical_price, .median_price, .heikin_ashi, .spread, .order_flow, .tick_pressure, .trade_intensity, .amihud, .realized_vol, .series, .series2, .ratio, .ratio_zscore, .rel_strength, .forward_return, .triple_barrier, .session_vwap, .session_range, .opening_range, .pivots => k,
            _ => null,
        };
    }
};

/// All valid kinds (for enumeration from bindings).
pub const all_kinds = [_]Kind{ .sma, .ema, .wma, .dema, .tema, .trima, .kama, .hma, .zlema, .vwma, .rma, .rsi, .macd, .ppo, .stoch, .stoch_rsi, .cci, .willr, .mom, .roc, .cmo, .trix, .ultosc, .ao, .tsi, .bop, .dpo, .adx, .aroon, .psar, .supertrend, .vortex, .ichimoku, .linreg, .atr, .natr, .true_range, .bbands, .keltner, .donchian, .stddev, .variance, .hist_vol, .obv, .vwap, .mfi, .cmf, .ad, .adosc, .efi, .returns, .log_returns, .zscore, .percent_rank, .rolling_min, .rolling_max, .drawdown, .sharpe, .sortino, .correl, .beta, .skew, .kurtosis, .typical_price, .median_price, .heikin_ashi, .spread, .order_flow, .tick_pressure, .trade_intensity, .amihud, .realized_vol, .series, .series2, .ratio, .ratio_zscore, .rel_strength, .forward_return, .triple_barrier, .session_vwap, .session_range, .opening_range, .pivots };

/// C-ABI compatible indicator request. Zero periods/params select the
/// documented defaults for the kind; field_index -1 selects the close column.
pub const Spec = extern struct {
    kind: u32,
    period: u32 = 0,
    period2: u32 = 0,
    period3: u32 = 0,
    period4: u32 = 0,
    param: f64 = 0,
    param2: f64 = 0,
    field_index: i64 = -1,
    field_index2: i64 = -1,
};

/// Fully resolved parameters (defaults applied).
pub const Params = struct {
    kind: Kind,
    p1: usize,
    p2: usize,
    p3: usize,
    p4: usize,
    a: f64,
    b: f64,
};

fn pick(v: u32, d: usize) usize {
    return if (v == 0) d else @intCast(v);
}

fn pickF(v: f64, d: f64) f64 {
    return if (v == 0) d else v;
}

/// Apply per-kind defaults to a spec.
pub fn resolve(spec: Spec) Error!Params {
    const kind = Kind.fromInt(spec.kind) orelse return Error.InvalidParameter;
    var p = Params{ .kind = kind, .p1 = 0, .p2 = 0, .p3 = 0, .p4 = 0, .a = spec.param, .b = spec.param2 };
    switch (kind) {
        .sma, .ema, .wma, .dema, .tema, .trima, .hma, .zlema, .vwma, .cci, .dpo, .donchian, .stddev, .variance, .hist_vol, .cmf, .zscore, .percent_rank, .rolling_min, .rolling_max, .sharpe, .sortino, .correl, .beta, .skew, .kurtosis, .linreg => p.p1 = pick(spec.period, 20),
        .kama => {
            p.p1 = pick(spec.period, 10);
            p.p2 = pick(spec.period2, 2);
            p.p3 = pick(spec.period3, 30);
        },
        .rma, .rsi, .willr, .cmo, .atr, .natr, .adx, .vortex, .mfi => p.p1 = pick(spec.period, 14),
        .macd, .ppo => {
            p.p1 = pick(spec.period, 12);
            p.p2 = pick(spec.period2, 26);
            p.p3 = pick(spec.period3, 9);
        },
        .stoch => {
            p.p1 = pick(spec.period, 14);
            p.p2 = pick(spec.period2, 3);
            p.p3 = pick(spec.period3, 3);
        },
        .stoch_rsi => {
            p.p1 = pick(spec.period, 14);
            p.p2 = pick(spec.period2, 14);
            p.p3 = pick(spec.period3, 3);
            p.p4 = pick(spec.period4, 3);
        },
        .mom, .roc => p.p1 = pick(spec.period, 10),
        .trix => p.p1 = pick(spec.period, 15),
        .ultosc => {
            p.p1 = pick(spec.period, 7);
            p.p2 = pick(spec.period2, 14);
            p.p3 = pick(spec.period3, 28);
        },
        .ao => {
            p.p1 = pick(spec.period, 5);
            p.p2 = pick(spec.period2, 34);
        },
        .tsi => {
            p.p1 = pick(spec.period, 25);
            p.p2 = pick(spec.period2, 13);
            p.p3 = pick(spec.period3, 13);
        },
        .aroon => p.p1 = pick(spec.period, 25),
        .psar => {
            p.a = pickF(spec.param, 0.02);
            p.b = pickF(spec.param2, 0.2);
        },
        .supertrend => {
            p.p1 = pick(spec.period, 10);
            p.a = pickF(spec.param, 3.0);
        },
        .ichimoku => {
            p.p1 = pick(spec.period, 9);
            p.p2 = pick(spec.period2, 26);
            p.p3 = pick(spec.period3, 52);
            p.p4 = pick(spec.period4, 26);
        },
        .bbands => {
            p.p1 = pick(spec.period, 20);
            p.a = pickF(spec.param, 2.0);
        },
        .keltner => {
            p.p1 = pick(spec.period, 20);
            p.p2 = pick(spec.period2, 10);
            p.a = pickF(spec.param, 2.0);
        },
        .adosc => {
            p.p1 = pick(spec.period, 3);
            p.p2 = pick(spec.period2, 10);
        },
        .efi => p.p1 = pick(spec.period, 13),
        .returns, .log_returns => p.p1 = pick(spec.period, 1),
        .vwap => p.p1 = @intCast(spec.period), // 0 = cumulative
        .bop, .true_range, .obv, .ad, .drawdown, .typical_price, .median_price, .heikin_ashi, .spread, .series, .series2, .ratio => {},
        .order_flow, .amihud, .ratio_zscore, .realized_vol => p.p1 = pick(spec.period, 20),
        .tick_pressure => p.p1 = pick(spec.period, 50),
        .trade_intensity => {
            p.p1 = pick(spec.period, 100);
            p.a = pickF(spec.param, 1_000_000.0); // timestamp units per second
        },
        .rel_strength => p.p1 = pick(spec.period, 10),
        .forward_return => p.p1 = pick(spec.period, 1),
        .triple_barrier => {
            p.p1 = pick(spec.period, 20);
            p.a = pickF(spec.param, 0.02);
            p.b = pickF(spec.param2, p.a);
        },
        .session_vwap, .session_range, .pivots => {
            // session length in timestamp units; 0 = sessions of the database's trading calendar
            if (spec.param < 0) return Error.InvalidParameter;
        },
        .opening_range => {
            p.p1 = pick(spec.period, 5);
            if (spec.param < 0) return Error.InvalidParameter;
        },
        _ => return Error.InvalidParameter,
    }
    return p;
}

/// Number of output series produced by a kind.
pub fn outputCount(kind: Kind) usize {
    return switch (kind) {
        .macd, .ppo, .adx, .aroon, .keltner, .donchian, .forward_return, .triple_barrier, .opening_range => 3,
        .stoch, .stoch_rsi, .psar, .supertrend, .vortex, .tsi, .spread, .order_flow, .trade_intensity => 2,
        .ichimoku, .bbands, .pivots => 5,
        .linreg, .heikin_ashi, .session_range => 4,
        else => 1,
    };
}

/// Names of the outputs of a kind (for bindings).
pub fn outputNames(kind: Kind) []const []const u8 {
    return switch (kind) {
        .macd => &.{ "macd", "signal", "hist" },
        .ppo => &.{ "ppo", "signal", "hist" },
        .adx => &.{ "adx", "plus_di", "minus_di" },
        .aroon => &.{ "up", "down", "osc" },
        .keltner, .donchian => &.{ "upper", "middle", "lower" },
        .stoch, .stoch_rsi => &.{ "k", "d" },
        .psar => &.{ "sar", "dir" },
        .supertrend => &.{ "line", "dir" },
        .vortex => &.{ "plus", "minus" },
        .tsi => &.{ "tsi", "signal" },
        .ichimoku => &.{ "tenkan", "kijun", "senkou_a", "senkou_b", "chikou" },
        .bbands => &.{ "upper", "middle", "lower", "percent_b", "bandwidth" },
        .linreg => &.{ "value", "slope", "intercept", "r2" },
        .heikin_ashi => &.{ "open", "high", "low", "close" },
        .spread => &.{ "abs", "bps" },
        .order_flow => &.{ "net", "imbalance" },
        .trade_intensity => &.{ "trades_per_sec", "volume_per_sec" },
        .forward_return => &.{ "ret", "max", "min" },
        .triple_barrier => &.{ "label", "ret", "bars" },
        .session_range => &.{ "open", "high", "low", "ret" },
        .opening_range => &.{ "high", "low", "breakout" },
        .pivots => &.{ "pp", "r1", "s1", "r2", "s2" },
        else => &.{"value"},
    };
}

pub fn kindName(kind: Kind) []const u8 {
    return @tagName(kind);
}

/// Which price columns a kind requires beyond its primary input.
pub const Needs = struct { open: bool = false, high: bool = false, low: bool = false, close: bool = false, volume: bool = false, second: bool = false, bid: bool = false, ask: bool = false, flow: bool = false, time: bool = false };

pub fn needs(kind: Kind) Needs {
    return switch (kind) {
        .cci, .willr, .stoch, .ultosc, .adx, .supertrend, .vortex, .ichimoku, .atr, .natr, .true_range, .keltner, .typical_price => .{ .high = true, .low = true, .close = true },
        .aroon, .donchian, .ao, .median_price => .{ .high = true, .low = true },
        .psar => .{ .high = true, .low = true },
        .bop, .heikin_ashi => .{ .open = true, .high = true, .low = true, .close = true },
        .mfi, .cmf, .ad, .adosc => .{ .high = true, .low = true, .close = true, .volume = true },
        .vwma, .obv, .efi, .vwap, .amihud => .{ .volume = true },
        .correl, .beta, .series2, .ratio, .ratio_zscore, .rel_strength => .{ .second = true },
        .spread => .{ .bid = true, .ask = true },
        .order_flow => .{ .volume = true, .flow = true },
        .trade_intensity => .{ .time = true, .volume = true },
        .session_vwap => .{ .time = true, .volume = true },
        .session_range => .{ .time = true },
        .opening_range, .pivots => .{ .time = true, .high = true, .low = true, .close = true },
        else => .{},
    };
}

/// Kinds whose outputs depend on future rows (labels, chikou): NaN at the end
/// of every window by construction. Never use them as live features.
pub fn isLookahead(kind: Kind) bool {
    return switch (kind) {
        .forward_return, .triple_barrier => true,
        else => false,
    };
}

/// Session-anchored kinds need rows back to the start of the session that
/// contains the first window row. Returns that timestamp, or null.
pub fn sessionLookbackTs(p: Params, first_ts: i64) ?i64 {
    return switch (p.kind) {
        .session_vwap, .session_range, .opening_range, .pivots => if (p.a > 0) sessionStart(first_ts, p.a, p.b) else null,
        else => null,
    };
}

pub fn isSessionKind(kind: Kind) bool {
    return switch (kind) {
        .session_vwap, .session_range, .opening_range, .pivots => true,
        else => false,
    };
}

/// Session kinds with `param` = 0 take their sessions from the database's
/// trading calendar (`Columns.session_starts`, filled by the storage layer).
pub fn usesCalendarSessions(p: Params) bool {
    return isSessionKind(p.kind) and p.a <= 0;
}

/// Session assignment for a resolved session kind.
pub fn sessionsOf(p: Params, cols: Columns) Sessions {
    if (p.a <= 0) {
        if (cols.session_starts) |st| return .{ .starts = st };
    }
    return .{ .fixed = .{ .len = p.a, .offset = p.b } };
}

/// Kinds that also need the *previous existing* session (pivots use its
/// high / low / close). The storage layer resolves which session that is.
pub fn needsPreviousSession(kind: Kind) bool {
    return kind == .pivots;
}

/// Recommended number of extra bars *before* a window so that the first
/// output inside the window is fully converged (exact lookback for finite
/// windows, plus a decay tail for exponential / Wilder recurrences).
pub fn warmup(p: Params) usize {
    const ema_tail = struct {
        fn f(period: usize) usize {
            return 10 * (period + 1);
        }
    }.f;
    const wilder_tail = struct {
        fn f(period: usize) usize {
            return 21 * period;
        }
    }.f;
    return switch (p.kind) {
        .sma, .wma, .trima, .cci, .donchian, .stddev, .variance, .zscore, .rolling_min, .rolling_max, .skew, .kurtosis, .correl, .linreg, .cmf => p.p1 - 1,
        .hma => p.p1 - 1 + @as(usize, @intFromFloat(@floor(@sqrt(@as(f64, @floatFromInt(p.p1)))))),
        .vwma => p.p1 - 1,
        .ema, .zlema => p.p1 - 1 + ema_tail(p.p1),
        .dema => 2 * (p.p1 - 1) + 2 * ema_tail(p.p1),
        .tema, .trix => 3 * (p.p1 - 1) + 3 * ema_tail(p.p1) + 1,
        .kama => p.p1 + ema_tail(p.p3),
        .rma => p.p1 - 1 + wilder_tail(p.p1),
        .rsi, .cmo => p.p1 + wilder_tail(p.p1),
        .willr, .mom, .roc, .returns, .log_returns, .percent_rank, .dpo, .aroon, .hist_vol, .sharpe, .sortino, .beta, .vortex, .mfi => p.p1 + 1,
        .macd, .ppo => @max(p.p1, p.p2) + p.p3 + ema_tail(@max(p.p1, p.p2)) + ema_tail(p.p3),
        .stoch => p.p1 + p.p2 + p.p3,
        .stoch_rsi => p.p1 + wilder_tail(p.p1) + p.p2 + p.p3 + p.p4,
        .ultosc => @max(p.p1, @max(p.p2, p.p3)) + 1,
        .ao => @max(p.p1, p.p2),
        .tsi => p.p1 + p.p2 + p.p3 + ema_tail(p.p1) + ema_tail(p.p2) + ema_tail(p.p3),
        .atr, .natr => p.p1 + wilder_tail(p.p1),
        .adx => 2 * p.p1 + wilder_tail(p.p1) * 2,
        .supertrend => p.p1 + wilder_tail(p.p1) + 64,
        .keltner => @max(p.p1 + ema_tail(p.p1), p.p2 + wilder_tail(p.p2)),
        .bbands => p.p1 - 1,
        .ichimoku => @max(p.p1, @max(p.p2, p.p3)) - 1 + p.p4,
        .psar => 256,
        .adosc => p.p2 + ema_tail(p.p2),
        .efi => p.p1 + ema_tail(p.p1),
        .vwap => if (p.p1 == 0) 0 else p.p1 - 1,
        .bop, .true_range, .obv, .ad, .drawdown, .typical_price, .median_price, .heikin_ashi => 0,
        .spread, .series, .series2, .ratio, .forward_return, .triple_barrier => 0,
        .order_flow, .ratio_zscore => p.p1 - 1,
        .tick_pressure, .trade_intensity, .amihud, .realized_vol, .rel_strength => p.p1,
        .session_vwap, .session_range, .opening_range, .pivots => 0, // handled by time (sessionLookbackTs)
        _ => 0,
    };
}

/// Kinds that accumulate from the first row they see (OBV, A/D line, cumulative
/// VWAP, drawdown). The storage layer anchors them at the window start so that
/// their values never depend on warm-up rows read for other indicators.
pub fn anchoredAtWindowStart(p: Params) bool {
    return switch (p.kind) {
        .obv, .ad, .drawdown => true,
        .vwap => p.p1 == 0,
        else => false,
    };
}

/// Input columns for `compute`. Any missing column is null; `input` overrides
/// the primary series (defaults to close), `input2` is the second series.
pub const Columns = struct {
    open: ?[]const f64 = null,
    high: ?[]const f64 = null,
    low: ?[]const f64 = null,
    close: ?[]const f64 = null,
    volume: ?[]const f64 = null,
    /// Tick-level quotes and aggressor side (1 = buy, 0 = sell).
    bid: ?[]const f64 = null,
    ask: ?[]const f64 = null,
    side: ?[]const f64 = null,
    /// Bar-level buy volume (from resampling with a side column).
    buy_volume: ?[]const f64 = null,
    /// Timestamps of the rows (needed by time-aware kinds).
    time: ?[]const i64 = null,
    /// Per-row session start (from a trading calendar) for session kinds
    /// with `param` = 0; when null the fixed-length rule (`param`, `param2`)
    /// is used.
    session_starts: ?[]const i64 = null,
    input: ?[]const f64 = null,
    input2: ?[]const f64 = null,
};

/// Compute one indicator. `outs` must contain outputCount(kind) slices, each
/// of the column length.
pub fn compute(spec: Spec, cols: Columns, outs: []const []f64, allocator: Allocator) Error!void {
    const p = try resolve(spec);
    const kind = p.kind;
    if (outs.len != outputCount(kind)) return Error.LengthMismatch;
    const x: []const f64 = cols.input orelse cols.close orelse return Error.MissingColumn;
    const nd = needs(kind);
    const o: []const f64 = if (nd.open) (cols.open orelse return Error.MissingColumn) else x;
    const h: []const f64 = if (nd.high) (cols.high orelse return Error.MissingColumn) else x;
    const l: []const f64 = if (nd.low) (cols.low orelse return Error.MissingColumn) else x;
    const c: []const f64 = if (nd.close) (cols.close orelse return Error.MissingColumn) else x;
    const v: []const f64 = if (nd.volume) (cols.volume orelse return Error.MissingColumn) else x;
    const x2: []const f64 = if (nd.second) (cols.input2 orelse return Error.MissingColumn) else x;
    const bid: []const f64 = if (nd.bid) (cols.bid orelse return Error.MissingColumn) else x;
    const ask: []const f64 = if (nd.ask) (cols.ask orelse return Error.MissingColumn) else x;
    const tm: []const i64 = if (nd.time) (cols.time orelse return Error.MissingColumn) else &[_]i64{};
    if (nd.flow and cols.side == null and cols.buy_volume == null) return Error.MissingColumn;
    const n = x.len;
    for (outs) |out| if (out.len != n) return Error.LengthMismatch;
    if (o.len != n or h.len != n or l.len != n or c.len != n or v.len != n or x2.len != n or bid.len != n or ask.len != n) return Error.LengthMismatch;
    if (nd.time and tm.len != n) return Error.LengthMismatch;
    switch (kind) {
        .sma => try sma(x, p.p1, outs[0]),
        .ema => try ema(x, p.p1, outs[0]),
        .wma => try wma(x, p.p1, outs[0]),
        .dema => try dema(x, p.p1, outs[0], allocator),
        .tema => try tema(x, p.p1, outs[0], allocator),
        .trima => try trima(x, p.p1, outs[0]),
        .kama => try kama(x, p.p1, p.p2, p.p3, outs[0]),
        .hma => try hma(x, p.p1, outs[0], allocator),
        .zlema => try zlema(x, p.p1, outs[0]),
        .vwma => try vwma(x, v, p.p1, outs[0], allocator),
        .rma => try rma(x, p.p1, outs[0]),
        .rsi => try rsi(x, p.p1, outs[0]),
        .macd => try macd(x, p.p1, p.p2, p.p3, outs[0], outs[1], outs[2]),
        .ppo => try ppo(x, p.p1, p.p2, p.p3, outs[0], outs[1], outs[2], allocator),
        .stoch => try stoch(h, l, c, p.p1, p.p2, p.p3, outs[0], outs[1], allocator),
        .stoch_rsi => try stochRsi(x, p.p1, p.p2, p.p3, p.p4, outs[0], outs[1], allocator),
        .cci => try cci(h, l, c, p.p1, outs[0], allocator),
        .willr => try willr(h, l, c, p.p1, outs[0], allocator),
        .mom => try mom(x, p.p1, outs[0]),
        .roc => try roc(x, p.p1, outs[0]),
        .cmo => try cmo(x, p.p1, outs[0], allocator),
        .trix => try trix(x, p.p1, outs[0], allocator),
        .ultosc => try ultosc(h, l, c, p.p1, p.p2, p.p3, outs[0], allocator),
        .ao => try ao(h, l, p.p1, p.p2, outs[0], allocator),
        .tsi => try tsi(x, p.p1, p.p2, p.p3, outs[0], outs[1], allocator),
        .bop => bop(o, h, l, c, outs[0]),
        .dpo => try dpo(x, p.p1, outs[0]),
        .adx => try adx(h, l, c, p.p1, outs[0], outs[1], outs[2]),
        .aroon => try aroon(h, l, p.p1, outs[0], outs[1], outs[2]),
        .psar => try psar(h, l, p.a, p.b, outs[0], outs[1]),
        .supertrend => try supertrend(h, l, c, p.p1, p.a, outs[0], outs[1], allocator),
        .vortex => try vortex(h, l, c, p.p1, outs[0], outs[1], allocator),
        .ichimoku => try ichimoku(h, l, c, p.p1, p.p2, p.p3, p.p4, outs[0], outs[1], outs[2], outs[3], outs[4], allocator),
        .linreg => try linreg(x, p.p1, outs[0], outs[1], outs[2], outs[3]),
        .atr => try atr(h, l, c, p.p1, outs[0]),
        .natr => try natr(h, l, c, p.p1, outs[0]),
        .true_range => trueRange(h, l, c, outs[0]),
        .bbands => try bbands(x, p.p1, p.a, outs[0], outs[1], outs[2], outs[3], outs[4]),
        .keltner => try keltner(h, l, c, p.p1, p.p2, p.a, outs[0], outs[1], outs[2]),
        .donchian => try donchian(h, l, p.p1, outs[0], outs[1], outs[2], allocator),
        .stddev => try stddev(x, p.p1, 0, outs[0]),
        .variance => try variance(x, p.p1, 0, outs[0]),
        .hist_vol => try histVol(x, p.p1, p.a, outs[0], allocator),
        .obv => try obv(x, v, outs[0]),
        .vwap => {
            // typical price when H/L are available, else the primary series
            if (cols.high != null and cols.low != null and cols.close != null and cols.input == null) {
                const tp = try allocator.alloc(f64, n);
                defer allocator.free(tp);
                typicalPrice(cols.high.?, cols.low.?, cols.close.?, tp);
                try vwap(tp, v, p.p1, outs[0], allocator);
            } else {
                try vwap(x, v, p.p1, outs[0], allocator);
            }
        },
        .mfi => try mfi(h, l, c, v, p.p1, outs[0], allocator),
        .cmf => try cmf(h, l, c, v, p.p1, outs[0], allocator),
        .ad => try ad(h, l, c, v, outs[0]),
        .adosc => try adosc(h, l, c, v, p.p1, p.p2, outs[0], allocator),
        .efi => try efi(x, v, p.p1, outs[0]),
        .returns => try returns(x, p.p1, outs[0]),
        .log_returns => try logReturns(x, p.p1, outs[0]),
        .zscore => try zscore(x, p.p1, outs[0], allocator),
        .percent_rank => try percentRank(x, p.p1, outs[0]),
        .rolling_min => try rollingMin(x, p.p1, outs[0], allocator),
        .rolling_max => try rollingMax(x, p.p1, outs[0], allocator),
        .drawdown => try drawdown(x, outs[0]),
        .sharpe => try sharpe(x, p.p1, p.a, outs[0], allocator),
        .sortino => try sortino(x, p.p1, p.a, outs[0], allocator),
        .correl => try correl(x, x2, p.p1, outs[0]),
        .beta => try beta(x, x2, p.p1, outs[0], allocator),
        .skew => try skewKurt(x, p.p1, outs[0], null, allocator),
        .kurtosis => try skewKurt(x, p.p1, null, outs[0], allocator),
        .typical_price => typicalPrice(h, l, c, outs[0]),
        .median_price => medianPrice(h, l, outs[0]),
        .heikin_ashi => try heikinAshi(o, h, l, c, outs[0], outs[1], outs[2], outs[3]),
        .spread => spread(bid, ask, outs[0], outs[1]),
        .order_flow => try orderFlow(v, cols.side, cols.buy_volume, p.p1, outs[0], outs[1], allocator),
        .tick_pressure => try tickPressure(x, p.p1, outs[0]),
        .trade_intensity => try tradeIntensity(tm, v, p.p1, p.a, outs[0], outs[1]),
        .amihud => try amihud(x, v, p.p1, outs[0], allocator),
        .realized_vol => try realizedVol(x, p.p1, p.a, outs[0], allocator),
        .series => @memcpy(outs[0], x),
        .series2 => @memcpy(outs[0], x2),
        .ratio => vecDiv(x, x2, outs[0]),
        .ratio_zscore => try ratioZscore(x, x2, p.p1, outs[0], allocator),
        .rel_strength => try relStrength(x, x2, p.p1, outs[0], allocator),
        .forward_return => try forwardReturn(x, p.p1, outs[0], outs[1], outs[2], allocator),
        .triple_barrier => try tripleBarrier(x, p.p1, p.a, p.b, outs[0], outs[1], outs[2]),
        .session_vwap => {
            if (cols.high != null and cols.low != null and cols.close != null and cols.input == null) {
                const tp = try allocator.alloc(f64, n);
                defer allocator.free(tp);
                typicalPrice(cols.high.?, cols.low.?, cols.close.?, tp);
                try sessionVwap(tm, tp, v, sessionsOf(p, cols), outs[0]);
            } else {
                try sessionVwap(tm, x, v, sessionsOf(p, cols), outs[0]);
            }
        },
        .session_range => try sessionRange(tm, cols.open, cols.high, cols.low, x, sessionsOf(p, cols), outs[0], outs[1], outs[2], outs[3]),
        .opening_range => try openingRange(tm, h, l, c, p.p1, sessionsOf(p, cols), outs[0], outs[1], outs[2]),
        .pivots => try pivots(tm, h, l, c, sessionsOf(p, cols), outs[0], outs[1], outs[2], outs[3], outs[4]),
        _ => return Error.InvalidParameter,
    }
}

// ---------------------------------------------------------------------------
// One-shot snapshot: a fixed struct of the most common indicators for the
// latest bar, all computed from one column read.
// ---------------------------------------------------------------------------

pub const Snapshot = extern struct {
    timestamp: i64,
    bars: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    sma_5: f64,
    sma_10: f64,
    sma_20: f64,
    sma_50: f64,
    sma_100: f64,
    sma_200: f64,
    ema_9: f64,
    ema_12: f64,
    ema_21: f64,
    ema_26: f64,
    ema_50: f64,
    ema_200: f64,
    wma_20: f64,
    hma_20: f64,
    vwma_20: f64,
    kama_10: f64,
    tema_20: f64,
    rsi_14: f64,
    stoch_k: f64,
    stoch_d: f64,
    stochrsi_k: f64,
    stochrsi_d: f64,
    macd: f64,
    macd_signal: f64,
    macd_hist: f64,
    ppo: f64,
    cci_20: f64,
    williams_r_14: f64,
    roc_10: f64,
    mom_10: f64,
    cmo_14: f64,
    trix_15: f64,
    ultosc: f64,
    ao: f64,
    tsi: f64,
    tsi_signal: f64,
    adx_14: f64,
    plus_di_14: f64,
    minus_di_14: f64,
    aroon_up_25: f64,
    aroon_down_25: f64,
    aroon_osc_25: f64,
    psar: f64,
    psar_dir: f64,
    supertrend: f64,
    supertrend_dir: f64,
    vortex_plus_14: f64,
    vortex_minus_14: f64,
    ichimoku_tenkan: f64,
    ichimoku_kijun: f64,
    ichimoku_senkou_a: f64,
    ichimoku_senkou_b: f64,
    linreg_value_20: f64,
    linreg_slope_20: f64,
    linreg_r2_20: f64,
    atr_14: f64,
    natr_14: f64,
    true_range: f64,
    bb_upper: f64,
    bb_middle: f64,
    bb_lower: f64,
    bb_percent_b: f64,
    bb_bandwidth: f64,
    keltner_upper: f64,
    keltner_middle: f64,
    keltner_lower: f64,
    donchian_upper_20: f64,
    donchian_middle_20: f64,
    donchian_lower_20: f64,
    stddev_20: f64,
    hist_vol_20: f64,
    obv: f64,
    vwap: f64,
    mfi_14: f64,
    cmf_20: f64,
    ad: f64,
    adosc: f64,
    efi_13: f64,
    return_1: f64,
    return_5: f64,
    return_10: f64,
    return_20: f64,
    log_return_1: f64,
    zscore_20: f64,
    percent_rank_20: f64,
    high_20: f64,
    low_20: f64,
    high_250: f64,
    low_250: f64,
    drawdown: f64,
    sharpe_20: f64,
    sortino_20: f64,
    skew_20: f64,
    kurtosis_20: f64,
};

/// Number of bars the snapshot wants in order to converge every field.
pub const snapshot_recommended_bars: usize = 2500;

fn last(s: []const f64) f64 {
    return if (s.len == 0) nan else s[s.len - 1];
}

/// Compute a snapshot from bar columns (open/high/low/volume optional).
/// `periods_per_year` annualises volatility / Sharpe / Sortino (0 = none).
pub fn snapshot(ts: []const i64, open: ?[]const f64, high: ?[]const f64, low: ?[]const f64, close: []const f64, volume: ?[]const f64, periods_per_year: f64, allocator: Allocator) Error!Snapshot {
    const n = close.len;
    var s: Snapshot = undefined;
    inline for (@typeInfo(Snapshot).@"struct".fields) |f| {
        if (f.type == f64) @field(s, f.name) = nan;
    }
    s.timestamp = if (n > 0) ts[n - 1] else 0;
    s.bars = n;
    if (n == 0) return s;
    s.close = close[n - 1];
    if (open) |op| s.open = op[n - 1];
    if (high) |hp| s.high = hp[n - 1];
    if (low) |lp| s.low = lp[n - 1];
    if (volume) |vp| s.volume = vp[n - 1];

    // scratch: 6 buffers of n
    const buf = try allocator.alloc(f64, 6 * n);
    defer allocator.free(buf);
    const b0 = buf[0..n];
    const b1 = buf[n .. 2 * n];
    const b2 = buf[2 * n .. 3 * n];
    const b3 = buf[3 * n .. 4 * n];
    const b4 = buf[4 * n .. 5 * n];
    const b5 = buf[5 * n .. 6 * n];

    // --- moving averages (multi-period SMA from one prefix sum) ---
    prefixSum(close, b0);
    const sma_periods = [_]usize{ 5, 10, 20, 50, 100, 200 };
    var sma_vals: [6]f64 = undefined;
    for (sma_periods, 0..) |p, k| {
        sma_vals[k] = if (n >= p) (b0[n - 1] - (if (n > p) b0[n - 1 - p] else 0.0)) / @as(f64, @floatFromInt(p)) else nan;
    }
    s.sma_5 = sma_vals[0];
    s.sma_10 = sma_vals[1];
    s.sma_20 = sma_vals[2];
    s.sma_50 = sma_vals[3];
    s.sma_100 = sma_vals[4];
    s.sma_200 = sma_vals[5];
    // EMAs: vectorised across periods in groups of `lanes`.
    {
        const ema_periods = [_]usize{ 9, 12, 21, 26, 50, 200 };
        var ema_vals: [6]f64 = undefined;
        var k: usize = 0;
        while (k < ema_periods.len) {
            const cnt = @min(lanes, ema_periods.len - k);
            var outs_arr: [lanes][]f64 = undefined;
            const bufs = [_][]f64{ b1, b2, b3, b4, b5, b0 };
            for (0..cnt) |j| outs_arr[j] = bufs[j % bufs.len];
            if (cnt <= 5) {
                try emaMulti(close, ema_periods[k .. k + cnt], outs_arr[0..cnt]);
                for (0..cnt) |j| ema_vals[k + j] = last(outs_arr[j]);
            } else {
                // more lanes than scratch buffers: fall back to scalar EMAs
                for (0..cnt) |j| {
                    try ema(close, ema_periods[k + j], b1);
                    ema_vals[k + j] = last(b1);
                }
            }
            k += cnt;
        }
        s.ema_9 = ema_vals[0];
        s.ema_12 = ema_vals[1];
        s.ema_21 = ema_vals[2];
        s.ema_26 = ema_vals[3];
        s.ema_50 = ema_vals[4];
        s.ema_200 = ema_vals[5];
    }
    prefixSum(close, b0); // restore prefix (b0 may have been reused)
    try wma(close, 20, b1);
    s.wma_20 = last(b1);
    try hma(close, 20, b1, allocator);
    s.hma_20 = last(b1);
    try kama(close, 10, 2, 30, b1);
    s.kama_10 = last(b1);
    try tema(close, 20, b1, allocator);
    s.tema_20 = last(b1);
    if (volume) |vp| {
        try vwma(close, vp, 20, b1, allocator);
        s.vwma_20 = last(b1);
    }

    // --- momentum ---
    try rsi(close, 14, b1);
    s.rsi_14 = last(b1);
    try stochRsi(close, 14, 14, 3, 3, b1, b2, allocator);
    s.stochrsi_k = last(b1);
    s.stochrsi_d = last(b2);
    try macd(close, 12, 26, 9, b1, b2, b3);
    s.macd = last(b1);
    s.macd_signal = last(b2);
    s.macd_hist = last(b3);
    try ppo(close, 12, 26, 9, b1, b2, b3, allocator);
    s.ppo = last(b1);
    try mom(close, 10, b1);
    s.mom_10 = last(b1);
    try roc(close, 10, b1);
    s.roc_10 = last(b1);
    try cmo(close, 14, b1, allocator);
    s.cmo_14 = last(b1);
    try trix(close, 15, b1, allocator);
    s.trix_15 = last(b1);
    try tsi(close, 25, 13, 13, b1, b2, allocator);
    s.tsi = last(b1);
    s.tsi_signal = last(b2);

    // --- stats on close ---
    try linreg(close, 20, b1, b2, b3, b4);
    s.linreg_value_20 = last(b1);
    s.linreg_slope_20 = last(b2);
    s.linreg_r2_20 = last(b4);
    try rollingMoments(close, 20, 0, b1, b2);
    s.stddev_20 = @sqrt(last(b2));
    s.bb_middle = last(b1);
    s.bb_upper = s.bb_middle + 2.0 * s.stddev_20;
    s.bb_lower = s.bb_middle - 2.0 * s.stddev_20;
    s.bb_percent_b = (s.close - s.bb_lower) / (s.bb_upper - s.bb_lower);
    s.bb_bandwidth = (s.bb_upper - s.bb_lower) / s.bb_middle;
    s.zscore_20 = (s.close - s.bb_middle) / s.stddev_20;
    try histVol(close, 20, periods_per_year, b1, allocator);
    s.hist_vol_20 = last(b1);
    try percentRank(close, 20, b1);
    s.percent_rank_20 = last(b1);
    try rollingMax(close, 20, b1, allocator);
    s.high_20 = last(b1);
    try rollingMin(close, 20, b1, allocator);
    s.low_20 = last(b1);
    try rollingMax(close, 250, b1, allocator);
    s.high_250 = last(b1);
    try rollingMin(close, 250, b1, allocator);
    s.low_250 = last(b1);
    try drawdown(close, b1);
    s.drawdown = last(b1);
    try sharpe(close, 20, periods_per_year, b1, allocator);
    s.sharpe_20 = last(b1);
    try sortino(close, 20, periods_per_year, b1, allocator);
    s.sortino_20 = last(b1);
    try skewKurt(close, 20, b1, b2, allocator);
    s.skew_20 = last(b1);
    s.kurtosis_20 = last(b2);
    if (n >= 2) s.return_1 = close[n - 1] / close[n - 2] - 1.0;
    if (n >= 6) s.return_5 = close[n - 1] / close[n - 6] - 1.0;
    if (n >= 11) s.return_10 = close[n - 1] / close[n - 11] - 1.0;
    if (n >= 21) s.return_20 = close[n - 1] / close[n - 21] - 1.0;
    if (n >= 2) s.log_return_1 = @log(close[n - 1] / close[n - 2]);

    // --- H/L/C based ---
    if (high != null and low != null) {
        const h = high.?;
        const l = low.?;
        try stoch(h, l, close, 14, 3, 3, b1, b2, allocator);
        s.stoch_k = last(b1);
        s.stoch_d = last(b2);
        try cci(h, l, close, 20, b1, allocator);
        s.cci_20 = last(b1);
        try willr(h, l, close, 14, b1, allocator);
        s.williams_r_14 = last(b1);
        try ultosc(h, l, close, 7, 14, 28, b1, allocator);
        s.ultosc = last(b1);
        try ao(h, l, 5, 34, b1, allocator);
        s.ao = last(b1);
        try adx(h, l, close, 14, b1, b2, b3);
        s.adx_14 = last(b1);
        s.plus_di_14 = last(b2);
        s.minus_di_14 = last(b3);
        try aroon(h, l, 25, b1, b2, b3);
        s.aroon_up_25 = last(b1);
        s.aroon_down_25 = last(b2);
        s.aroon_osc_25 = last(b3);
        try psar(h, l, 0.02, 0.2, b1, b2);
        s.psar = last(b1);
        s.psar_dir = last(b2);
        try supertrend(h, l, close, 10, 3.0, b1, b2, allocator);
        s.supertrend = last(b1);
        s.supertrend_dir = last(b2);
        try vortex(h, l, close, 14, b1, b2, allocator);
        s.vortex_plus_14 = last(b1);
        s.vortex_minus_14 = last(b2);
        try ichimoku(h, l, close, 9, 26, 52, 26, b1, b2, b3, b4, b5, allocator);
        s.ichimoku_tenkan = last(b1);
        s.ichimoku_kijun = last(b2);
        s.ichimoku_senkou_a = last(b3);
        s.ichimoku_senkou_b = last(b4);
        try atr(h, l, close, 14, b1);
        s.atr_14 = last(b1);
        s.natr_14 = 100.0 * s.atr_14 / s.close;
        trueRange(h, l, close, b1);
        s.true_range = last(b1);
        try keltner(h, l, close, 20, 10, 2.0, b1, b2, b3);
        s.keltner_upper = last(b1);
        s.keltner_middle = last(b2);
        s.keltner_lower = last(b3);
        try donchian(h, l, 20, b1, b2, b3, allocator);
        s.donchian_upper_20 = last(b1);
        s.donchian_middle_20 = last(b2);
        s.donchian_lower_20 = last(b3);
        if (volume) |vp| {
            try mfi(h, l, close, vp, 14, b1, allocator);
            s.mfi_14 = last(b1);
            try cmf(h, l, close, vp, 20, b1, allocator);
            s.cmf_20 = last(b1);
            try ad(h, l, close, vp, b1);
            s.ad = last(b1);
            try adosc(h, l, close, vp, 3, 10, b1, allocator);
            s.adosc = last(b1);
            typicalPrice(h, l, close, b2);
            try vwap(b2, vp, 0, b1, allocator);
            s.vwap = last(b1);
        }
    }
    if (volume) |vp| {
        try obv(close, vp, b1);
        s.obv = last(b1);
        try efi(close, vp, 13, b1);
        s.efi_13 = last(b1);
        if (high == null or low == null) {
            try vwap(close, vp, 0, b1, allocator);
            s.vwap = last(b1);
        }
    }
    return s;
}
