//! HOCDB cross-sectional ("universe") features kernel.
//!
//! Given a watch-list of aligned close series it produces, in one call, the
//! cross-sectional context an AI trading agent needs: momentum / volatility
//! percentile ranks, a correlation matrix, a market factor with betas and
//! idiosyncratic volatility, relative strength, dispersion and breadth.
//! Pure kernel: no I/O, allocations only through the passed `Allocator`
//! (one scratch block per call, nothing per bar or per ticker).
//!
//! ## Input
//! `closes[i]` is ticker `i`'s close series. All series share one time axis
//! (same length `n_bars`; the storage layer inner-joins them beforehand with
//! `indicators.alignInner`). `volumes[i]` is optional and aligned the same
//! way. `ts` (optional) only feeds `Summary.first_ts` / `last_ts` (0 without
//! it). Bar `t = n_bars - 1` is "now": every feature describes the last bar.
//! Only the last `max(period) + 1` bars are ever touched.
//!
//! ## Returns
//!   * simple returns   `r[t]  = c[t] / c[t-1] - 1`   (every return statistic)
//!   * log returns      `lr[t] = ln(c[t] / c[t-1])`   (volatility only)
//!
//! Bar 0 has no return, so a series of `n_bars` bars has `n_bars - 1`
//! returns. A non-finite close (NaN, +-inf, a zero divisor) makes the
//! touching returns non-finite; non-finite values are treated as *missing*
//! everywhere below (a ticker with too few finite bars simply gets NaN
//! features and is excluded from ranks, means and the market).
//!
//! ## Market factor
//! `r_m[t]` is the weighted mean of the tickers whose return at bar `t` is
//! finite: `sum(w_i * r_i[t]) / sum(w_i)` over those tickers, NaN when there
//! are none. `weights_mode = 0`: `w_i = 1` (equal weight). `weights_mode =
//! 1` (only when volumes are given, otherwise it falls back to mode 0):
//! `w_i` is the mean of ticker `i`'s finite volumes over the last
//! `corr_period` bars (all bars when the series is shorter); a ticker whose
//! mean volume is not finite or not > 0 gets weight 0, i.e. is left out of
//! the market. Weights are constant over the window. The market is thus a
//! bar-by-bar rebalanced index: its cumulative return over `k` bars is
//! `prod(1 + r_m[t]) - 1` and its log return `ln(1 + r_m[t])`.
//!
//! ## Windows
//! Rolling statistics are *strict* (pandas `rolling(period)` semantics):
//! they use exactly the last `period` observations - returns for return
//! based statistics, bars for price / volume based ones - and are NaN when
//! fewer are available or when any observation inside the window is
//! non-finite. Correlations are the exception (pandas
//! `DataFrame.corr(min_periods=3)` semantics): they use the last
//! `min(corr_period, n_bars - 1)` returns, pairwise-complete (only bars where
//! both series are finite), and are NaN with fewer than 3 such bars or when
//! either side has zero variance. Correlations are clamped to [-1, 1].
//!
//! ## Per-ticker features (`Row`, in field order)
//!   * `last_close`      close at the last bar (raw, may be NaN).
//!   * `ret_1`           `r[n-1]`.
//!   * `mom_short/mid/long`  `c[n-1] / c[n-1-k] - 1` for k = mom_short /
//!                       mom_mid / mom_long; NaN with fewer than k returns.
//!   * `vol`             population std of the last `vol_period` log
//!                       returns, times `sqrt(periods_per_year)` when > 0.
//!   * `sma_distance`    `(c[n-1] - sma) / sma`, sma over `sma_period` bars.
//!   * `beta`            population `cov(r_i, r_m) / var(r_m)` over the last
//!                       `beta_period` returns (NaN when `var(r_m) = 0`).
//!   * `corr_market`     Pearson correlation of `r_i` and `r_m` over the
//!                       correlation window (rules above).
//!   * `rel_strength`    `mom_mid - market_mom_mid` (the ticker's cumulative
//!                       return over `mom_mid` bars minus the market's).
//!   * `rank_*`          cross-sectional percentile ranks of mom_short /
//!                       mom_mid / mom_long / vol / rel_strength (see `rank`).
//!   * `z_mom_mid`       `(mom_mid - mean) / std` across the tickers with a
//!                       finite mom_mid (population std; NaN when std = 0).
//!   * `avg_corr`        mean of the finite correlations with the *other*
//!                       tickers (the row of the matrix without the diagonal).
//!   * `max_corr`        largest such correlation, `max_corr_index` the index
//!                       of that ticker (the lowest index on ties). Without a
//!                       finite correlation `max_corr` is NaN and
//!                       `max_corr_index` is the ticker's own index.
//!   * `idio_vol`        population std of the simple-return residual
//!                       `r_i[t] - beta * r_m[t]` over the last `vol_period`
//!                       returns, annualised like `vol`; NaN when beta is.
//!   * `volume_ratio`    last volume / mean volume over the last `vol_period`
//!                       bars; NaN without volumes.
//!
//! ## Universe summary (`Summary`)
//!   * `market_ret_1`    `r_m[n-1]`.
//!   * `market_mom_*`    `prod(1 + r_m[t]) - 1` over the last k market
//!                       returns (k = mom_short / mom_mid / mom_long); NaN
//!                       with fewer than k returns or a NaN inside.
//!   * `market_vol`      population std of `ln(1 + r_m[t])` over the last
//!                       `vol_period` returns, annualised like `vol`.
//!   * `dispersion`      cross-sectional population std of the finite `ret_1`
//!                       values (`dispersion_mid`: of `mom_mid`); NaN when
//!                       there are none, 0 with a single one.
//!   * `breadth_sma`     share of tickers with `close > sma` among those with
//!                       a finite `sma_distance` (`breadth_up`: `ret_1 > 0`
//!                       among finite `ret_1`); NaN when the denominator is 0.
//!   * `avg/max/min_pair_corr`  mean / max / min of the finite entries of the
//!                       matrix' upper triangle; NaN when there are none.
//!   * `first_ts`, `last_ts`  `ts[0]` / `ts[n-1]` when `ts` is given, else 0.
//!
//! ## Cross-sectional ranks (`rank`)
//! Percentile rank in [0, 1] of the non-NaN entries: `(avg_rank - 1) /
//! (m - 1)` with 1-based average ranks for ties and `m` = number of ranked
//! entries, so the lowest value scores 0, the highest 1, ties share the mean
//! of their positions and a lone entry (or all-equal entries) scores 0.5.
//! NaN entries are excluded from the ranking and ranked NaN.
//!
//! SIMD: the return pass, the market accumulation, every mean / variance /
//! covariance window reduction and the rank comparisons use `@Vector` with
//! the target's lane count (`indicators.lanes`). The correlation matrix is
//! O(n_tickers^2 * corr_period).
const std = @import("std");
const math = std.math;
const ind = @import("indicators.zig");
const Allocator = std.mem.Allocator;

pub const Error = ind.Error;
pub const nan: f64 = ind.nan;
pub const lanes: usize = ind.lanes;
const V = @Vector(lanes, f64);
const VB = @Vector(lanes, bool);

/// Periods are in bars; `periods_per_year` annualises volatilities with
/// `sqrt(periods_per_year)` (0 = no annualisation); `weights_mode` selects
/// the market weighting (0 equal, 1 mean volume over `corr_period`).
pub const Params = extern struct {
    mom_short: u64 = 5,
    mom_mid: u64 = 20,
    mom_long: u64 = 60,
    vol_period: u64 = 20,
    corr_period: u64 = 60,
    sma_period: u64 = 50,
    beta_period: u64 = 60,
    periods_per_year: f64 = 0,
    weights_mode: u64 = 0,
};

/// Per-ticker features for the last bar (see the module documentation).
pub const Row = extern struct {
    last_close: f64,
    ret_1: f64,
    mom_short: f64,
    mom_mid: f64,
    mom_long: f64,
    vol: f64,
    sma_distance: f64,
    beta: f64,
    corr_market: f64,
    rel_strength: f64,
    rank_mom_short: f64,
    rank_mom_mid: f64,
    rank_mom_long: f64,
    rank_vol: f64,
    rank_rel_strength: f64,
    z_mom_mid: f64,
    avg_corr: f64,
    max_corr: f64,
    max_corr_index: u64,
    idio_vol: f64,
    volume_ratio: f64,
};

/// Universe-level features for the last bar (see the module documentation).
pub const Summary = extern struct {
    n_tickers: u64,
    n_bars: u64,
    market_ret_1: f64,
    market_mom_short: f64,
    market_mom_mid: f64,
    market_mom_long: f64,
    market_vol: f64,
    dispersion: f64,
    dispersion_mid: f64,
    breadth_sma: f64,
    breadth_up: f64,
    avg_pair_corr: f64,
    max_pair_corr: f64,
    min_pair_corr: f64,
    first_ts: i64,
    last_ts: i64,
};

/// Largest accepted period (keeps `period + 1` and scratch sizes sane).
pub const max_period: u64 = 1 << 32;

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

/// Lane mask of finite values (false for NaN and +-inf).
inline fn finiteMask(v: V) VB {
    return @abs(v) < splat(math.inf(f64));
}

/// `x` when finite, NaN otherwise (normalises +-inf produced by a zero divisor).
inline fn fin(x: f64) f64 {
    return if (math.isFinite(x)) x else nan;
}

inline fn toF(x: usize) f64 {
    return @floatFromInt(x);
}

/// Count, mean and centred second moment (`sum (x - mean)^2`) of the finite
/// entries of `x`. Exact two-pass evaluation, SIMD. `count == 0` gives NaN
/// mean / m2.
const Moments = struct {
    count: usize,
    mean: f64,
    m2: f64,

    /// Population standard deviation (NaN when empty).
    fn popStd(self: Moments) f64 {
        return if (self.count == 0) nan else @sqrt(self.m2 / toF(self.count));
    }
};

fn moments(x: []const f64) Moments {
    const zero = splat(0);
    const one = splat(1);
    var vc = zero;
    var vs = zero;
    var i: usize = 0;
    while (i + lanes <= x.len) : (i += lanes) {
        const a = load(x, i);
        const ok = finiteMask(a);
        vc += @select(f64, ok, one, zero);
        vs += @select(f64, ok, a, zero);
    }
    var cnt = @reduce(.Add, vc);
    var sum = @reduce(.Add, vs);
    while (i < x.len) : (i += 1) {
        if (math.isFinite(x[i])) {
            cnt += 1;
            sum += x[i];
        }
    }
    if (cnt == 0) return .{ .count = 0, .mean = nan, .m2 = nan };
    const mean = sum / cnt;
    const mv = splat(mean);
    var v2 = zero;
    i = 0;
    while (i + lanes <= x.len) : (i += lanes) {
        const a = load(x, i);
        const d = @select(f64, finiteMask(a), a - mv, zero);
        v2 += d * d;
    }
    var m2 = @reduce(.Add, v2);
    while (i < x.len) : (i += 1) {
        if (math.isFinite(x[i])) {
            const d = x[i] - mean;
            m2 += d * d;
        }
    }
    return .{ .count = @intFromFloat(cnt), .mean = mean, .m2 = m2 };
}

/// Pairwise-complete second moments of two equally long series: only bars
/// where both values are finite count. `sxx`, `syy`, `sxy` are centred sums
/// (population covariance = `sxy / count`). Exact two-pass evaluation, SIMD.
const PairMoments = struct {
    count: usize,
    mean_x: f64,
    mean_y: f64,
    sxx: f64,
    syy: f64,
    sxy: f64,

    /// Pearson correlation: NaN with fewer than 3 pairs or zero variance,
    /// clamped to [-1, 1] against rounding.
    fn pearson(self: PairMoments) f64 {
        if (self.count < 3) return nan;
        const d = self.sxx * self.syy;
        if (!(d > 0)) return nan;
        return @min(@as(f64, 1), @max(@as(f64, -1), self.sxy / @sqrt(d)));
    }

    /// Slope of y on x: `cov(x, y) / var(x)`, NaN when `var(x)` is 0.
    fn slope(self: PairMoments) f64 {
        return if (self.sxx > 0) self.sxy / self.sxx else nan;
    }
};

fn pairMoments(x: []const f64, y: []const f64) PairMoments {
    std.debug.assert(x.len == y.len);
    const zero = splat(0);
    const one = splat(1);
    var vc = zero;
    var vsx = zero;
    var vsy = zero;
    var i: usize = 0;
    while (i + lanes <= x.len) : (i += lanes) {
        const a = load(x, i);
        const b = load(y, i);
        const ok = finiteMask(a) & finiteMask(b);
        vc += @select(f64, ok, one, zero);
        vsx += @select(f64, ok, a, zero);
        vsy += @select(f64, ok, b, zero);
    }
    var cnt = @reduce(.Add, vc);
    var sx = @reduce(.Add, vsx);
    var sy = @reduce(.Add, vsy);
    while (i < x.len) : (i += 1) {
        if (math.isFinite(x[i]) and math.isFinite(y[i])) {
            cnt += 1;
            sx += x[i];
            sy += y[i];
        }
    }
    if (cnt == 0) return .{ .count = 0, .mean_x = nan, .mean_y = nan, .sxx = nan, .syy = nan, .sxy = nan };
    const mx = sx / cnt;
    const my = sy / cnt;
    const mxv = splat(mx);
    const myv = splat(my);
    var vxx = zero;
    var vyy = zero;
    var vxy = zero;
    i = 0;
    while (i + lanes <= x.len) : (i += lanes) {
        const a = load(x, i);
        const b = load(y, i);
        const ok = finiteMask(a) & finiteMask(b);
        const dx = @select(f64, ok, a - mxv, zero);
        const dy = @select(f64, ok, b - myv, zero);
        vxx += dx * dx;
        vyy += dy * dy;
        vxy += dx * dy;
    }
    var sxx = @reduce(.Add, vxx);
    var syy = @reduce(.Add, vyy);
    var sxy = @reduce(.Add, vxy);
    while (i < x.len) : (i += 1) {
        if (math.isFinite(x[i]) and math.isFinite(y[i])) {
            const dx = x[i] - mx;
            const dy = y[i] - my;
            sxx += dx * dx;
            syy += dy * dy;
            sxy += dx * dy;
        }
    }
    return .{ .count = @intFromFloat(cnt), .mean_x = mx, .mean_y = my, .sxx = sxx, .syy = syy, .sxy = sxy };
}

/// Cumulative return of a return series: `prod(1 + r) - 1` (NaN propagates).
fn compounded(r: []const f64) f64 {
    var acc: f64 = 1;
    for (r) |x| acc *= 1 + x;
    return fin(acc - 1);
}

/// Population std of the last `window.len` values when all are finite,
/// scaled by `scale`; NaN otherwise (strict window).
fn strictStd(window: []const f64, scale: f64) f64 {
    const mo = moments(window);
    return if (mo.count == window.len and window.len > 0) mo.popStd() * scale else nan;
}

/// Mean of the last `window.len` values when all are finite; NaN otherwise.
fn strictMean(window: []const f64) f64 {
    const mo = moments(window);
    return if (mo.count == window.len and window.len > 0) mo.mean else nan;
}

// ---------------------------------------------------------------------------
// Percentile ranks
// ---------------------------------------------------------------------------

/// Cross-sectional percentile rank of every entry of `values` into `out`
/// (`out.len == values.len`): `(avg_rank - 1) / (m - 1)` with 1-based average
/// ranks for ties over the `m` non-NaN entries, so the lowest value scores 0
/// and the highest 1; with `m == 1` (or all entries equal) the score is 0.5.
/// NaN entries are excluded and ranked NaN. O(n^2) comparisons, no
/// allocation, SIMD compare-and-count.
pub fn rank(values: []const f64, out: []f64) void {
    std.debug.assert(out.len == values.len);
    const n = values.len;
    var m: usize = 0;
    for (values) |v| {
        if (!ind.isNan(v)) m += 1;
    }
    const zero = splat(0);
    const one = splat(1);
    for (values, out) |v, *o| {
        if (ind.isNan(v)) {
            o.* = nan;
            continue;
        }
        if (m == 1) {
            o.* = 0.5;
            continue;
        }
        const vv = splat(v);
        var vl = zero;
        var ve = zero;
        var i: usize = 0;
        while (i + lanes <= n) : (i += lanes) {
            const a = load(values, i);
            vl += @select(f64, a < vv, one, zero);
            ve += @select(f64, a == vv, one, zero);
        }
        var less = @reduce(.Add, vl);
        var equal = @reduce(.Add, ve);
        while (i < n) : (i += 1) {
            if (values[i] < v) less += 1;
            if (values[i] == v) equal += 1;
        }
        // `equal` includes the entry itself, so avg_rank >= 1.
        const avg_rank = less + (equal + 1) * 0.5;
        o.* = (avg_rank - 1) / toF(m - 1);
    }
}

// ---------------------------------------------------------------------------
// The kernel
// ---------------------------------------------------------------------------

fn nanRow(row: *Row, index: usize) void {
    inline for (@typeInfo(Row).@"struct".fields) |f| {
        if (f.type == f64) @field(row.*, f.name) = nan;
    }
    row.max_corr_index = index;
}

fn nanSummary() Summary {
    var s: Summary = undefined;
    inline for (@typeInfo(Summary).@"struct".fields) |f| {
        if (f.type == f64) @field(s, f.name) = nan;
    }
    s.n_tickers = 0;
    s.n_bars = 0;
    s.first_ts = 0;
    s.last_ts = 0;
    return s;
}

const Periods = struct {
    mom_short: usize,
    mom_mid: usize,
    mom_long: usize,
    vol: usize,
    corr: usize,
    sma: usize,
    beta: usize,

    fn longest(self: Periods) usize {
        return @max(@max(@max(self.mom_short, self.mom_mid), @max(self.mom_long, self.vol)), @max(@max(self.corr, self.sma), self.beta));
    }
};

fn checkPeriod(p: u64, min: u64) Error!usize {
    if (p < min or p > max_period) return Error.InvalidPeriod;
    return @intCast(p);
}

fn periods(params: Params) Error!Periods {
    return .{
        .mom_short = try checkPeriod(params.mom_short, 1),
        .mom_mid = try checkPeriod(params.mom_mid, 1),
        .mom_long = try checkPeriod(params.mom_long, 1),
        .vol = try checkPeriod(params.vol_period, 2),
        .corr = try checkPeriod(params.corr_period, 3),
        .sma = try checkPeriod(params.sma_period, 1),
        .beta = try checkPeriod(params.beta_period, 2),
    };
}

/// Cross-sectional features for the last bar of a universe.
///
/// `closes[i]` / `volumes.?[i]` are ticker `i`'s aligned series (all of one
/// length), `ts` the shared timestamps (optional, same length). `rows.len`
/// must equal the number of tickers and `corr`, when given, must hold
/// `n_tickers * n_tickers` f64 (row-major correlation matrix, diagonal 1).
/// Semantics are documented at the top of this file. Errors:
/// `InvalidPeriod` (mom_* / sma_period < 1, vol_period / beta_period < 2,
/// corr_period < 3, or any period > `max_period`), `InvalidParameter`
/// (weights_mode > 1), `LengthMismatch`, `OutOfMemory`.
pub fn compute(closes: []const []const f64, volumes: ?[]const []const f64, ts: ?[]const i64, params: Params, rows: []Row, corr: ?[]f64, allocator: Allocator) Error!Summary {
    const m = closes.len;
    const n: usize = if (m > 0) closes[0].len else 0;

    // --- validation
    const per = try periods(params);
    if (params.weights_mode > 1) return Error.InvalidParameter;
    if (rows.len != m) return Error.LengthMismatch;
    if (corr) |c| if (c.len != m * m) return Error.LengthMismatch;
    for (closes) |c| if (c.len != n) return Error.LengthMismatch;
    if (volumes) |vs| {
        if (vs.len != m) return Error.LengthMismatch;
        for (vs) |v| if (v.len != n) return Error.LengthMismatch;
    }
    if (ts) |t| if (t.len != n) return Error.LengthMismatch;

    // --- defaults: everything NaN, diagonal 1
    for (rows, 0..) |*row, i| nanRow(row, i);
    var s = nanSummary();
    s.n_tickers = m;
    s.n_bars = n;
    if (ts) |t| if (n > 0) {
        s.first_ts = t[0];
        s.last_ts = t[n - 1];
    };
    if (corr) |c| {
        ind.fillNan(c);
        for (0..m) |i| c[i * m + i] = 1;
    }
    if (m == 0 or n == 0) return s;
    for (rows, closes) |*row, c| row.last_close = c[n - 1];

    // Work on the tail: T bars, R = T - 1 returns cover every window.
    const T = @min(n, per.longest() + 1);
    const base = n - T;
    const R = T - 1;
    const cw = @min(per.corr, R); // correlation window (lenient)
    const scale: f64 = if (params.periods_per_year > 0) @sqrt(params.periods_per_year) else 1;
    const weighted = params.weights_mode == 1 and volumes != null;

    // --- scratch: returns (m*T), market (T), aux (T), weights + 3 vectors (m)
    const buf = try allocator.alloc(f64, m * T + 2 * T + 4 * m);
    defer allocator.free(buf);
    const ret = buf[0 .. m * T];
    const mkt = buf[m * T ..][0..T];
    const aux = buf[m * T + T ..][0..T];
    const w = buf[m * T + 2 * T ..][0..m];
    const xs = buf[m * T + 2 * T + m ..][0..m];
    const ys = buf[m * T + 2 * T + 2 * m ..][0..m];
    const cnt = buf[m * T + 2 * T + 3 * m ..][0..m];

    // --- simple returns per ticker (SIMD), NaN at the first tail bar
    for (0..m) |i| try ind.returns(closes[i][base..], 1, ret[i * T ..][0..T]);

    // --- market weights
    if (weighted) {
        const vw = @min(per.corr, n);
        for (0..m) |i| {
            const mo = moments(volumes.?[i][n - vw ..]);
            w[i] = if (mo.count > 0 and math.isFinite(mo.mean) and mo.mean > 0) mo.mean else 0;
        }
    } else {
        @memset(w, 1);
    }

    // --- market factor: weighted mean of the finite returns per bar (SIMD
    //     across bars, accumulated ticker by ticker)
    @memset(mkt, 0); // numerator
    @memset(aux, 0); // denominator
    {
        const zero = splat(0);
        for (0..m) |i| {
            const wi = w[i];
            if (!(wi > 0)) continue;
            const wv = splat(wi);
            const r = ret[i * T ..][0..T];
            var t: usize = 0;
            while (t + lanes <= T) : (t += lanes) {
                const a = load(r, t);
                const ok = finiteMask(a);
                store(mkt, t, load(mkt, t) + @select(f64, ok, a * wv, zero));
                store(aux, t, load(aux, t) + @select(f64, ok, wv, zero));
            }
            while (t < T) : (t += 1) {
                if (math.isFinite(r[t])) {
                    mkt[t] += r[t] * wi;
                    aux[t] += wi;
                }
            }
        }
    }
    ind.vecDiv(mkt, aux, mkt); // 0 / 0 -> NaN where no ticker is finite

    // --- market summary
    s.market_ret_1 = fin(mkt[T - 1]);
    if (R >= per.mom_short) s.market_mom_short = compounded(mkt[T - per.mom_short ..]);
    if (R >= per.mom_mid) s.market_mom_mid = compounded(mkt[T - per.mom_mid ..]);
    if (R >= per.mom_long) s.market_mom_long = compounded(mkt[T - per.mom_long ..]);
    if (R >= per.vol) {
        const src = mkt[T - per.vol ..];
        const lm = aux[0..per.vol];
        const one = splat(1);
        var t: usize = 0;
        while (t + lanes <= per.vol) : (t += lanes) store(lm, t, @log(one + load(src, t)));
        while (t < per.vol) : (t += 1) lm[t] = @log(1 + src[t]);
        s.market_vol = strictStd(lm, scale);
    }

    // --- per-ticker features
    for (rows, 0..) |*row, i| {
        const c = closes[i][base..];
        const r = ret[i * T ..][0..T];
        row.ret_1 = fin(r[T - 1]);
        if (R >= per.mom_short) row.mom_short = fin(c[T - 1] / c[T - 1 - per.mom_short] - 1);
        if (R >= per.mom_mid) row.mom_mid = fin(c[T - 1] / c[T - 1 - per.mom_mid] - 1);
        if (R >= per.mom_long) row.mom_long = fin(c[T - 1] / c[T - 1 - per.mom_long] - 1);
        row.rel_strength = fin(row.mom_mid - s.market_mom_mid);
        if (R >= per.vol) {
            const lr = aux[0 .. per.vol + 1];
            try ind.logReturns(c[T - 1 - per.vol ..], 1, lr);
            row.vol = strictStd(lr[1..], scale);
        }
        if (T >= per.sma) {
            const sma = strictMean(c[T - per.sma ..]);
            if (sma != 0) row.sma_distance = fin((c[T - 1] - sma) / sma);
        }
        if (R >= per.beta) {
            const pm = pairMoments(mkt[T - per.beta ..], r[T - per.beta ..]);
            if (pm.count == per.beta) row.beta = fin(pm.slope());
        }
        row.corr_market = pairMoments(r[T - cw ..], mkt[T - cw ..]).pearson();
        if (R >= per.vol and math.isFinite(row.beta)) {
            const e = aux[0..per.vol];
            ind.vecScale(mkt[T - per.vol ..], row.beta, e);
            ind.vecSub(r[T - per.vol ..], e, e);
            row.idio_vol = strictStd(e, scale);
        }
        if (volumes) |vs| if (T >= per.vol) {
            const v = vs[i][base..];
            row.volume_ratio = fin(v[T - 1] / strictMean(v[T - per.vol ..]));
        };
    }

    // --- correlation matrix (pairwise-complete over the last cw returns);
    //     row sums / maxima are accumulated in place, `cnt` counts them
    @memset(cnt, 0);
    for (rows) |*row| {
        row.avg_corr = 0;
        row.max_corr = -math.inf(f64);
    }
    var pair_n: f64 = 0;
    for (0..m) |i| {
        const xi = ret[i * T + T - cw ..][0..cw];
        for (i + 1..m) |j| {
            const v = pairMoments(xi, ret[j * T + T - cw ..][0..cw]).pearson();
            if (corr) |c| {
                c[i * m + j] = v;
                c[j * m + i] = v;
            }
            if (ind.isNan(v)) continue;
            rows[i].avg_corr += v;
            cnt[i] += 1;
            if (v > rows[i].max_corr) {
                rows[i].max_corr = v;
                rows[i].max_corr_index = j;
            }
            rows[j].avg_corr += v;
            cnt[j] += 1;
            if (v > rows[j].max_corr) {
                rows[j].max_corr = v;
                rows[j].max_corr_index = i;
            }
            // summary over the upper triangle
            if (pair_n == 0) {
                s.avg_pair_corr = v;
                s.max_pair_corr = v;
                s.min_pair_corr = v;
            } else {
                s.avg_pair_corr += v;
                if (v > s.max_pair_corr) s.max_pair_corr = v;
                if (v < s.min_pair_corr) s.min_pair_corr = v;
            }
            pair_n += 1;
        }
    }
    if (pair_n > 0) s.avg_pair_corr /= pair_n;
    for (rows, 0..) |*row, i| {
        if (cnt[i] > 0) {
            row.avg_corr /= cnt[i];
        } else {
            row.avg_corr = nan;
            row.max_corr = nan;
            row.max_corr_index = i;
        }
    }

    // --- cross-sectional ranks, z-score, dispersion, breadth
    inline for (.{
        .{ "mom_short", "rank_mom_short" },
        .{ "mom_mid", "rank_mom_mid" },
        .{ "mom_long", "rank_mom_long" },
        .{ "vol", "rank_vol" },
        .{ "rel_strength", "rank_rel_strength" },
    }) |pair| {
        for (rows, xs) |row, *x| x.* = @field(row, pair[0]);
        rank(xs, ys);
        for (rows, ys) |*row, y| @field(row.*, pair[1]) = y;
    }
    {
        for (rows, xs) |row, *x| x.* = row.mom_mid;
        const mo = moments(xs);
        if (mo.count > 0) s.dispersion_mid = mo.popStd();
        const sd = mo.popStd();
        if (mo.count > 0 and sd > 0) {
            for (rows) |*row| {
                if (math.isFinite(row.mom_mid)) row.z_mom_mid = (row.mom_mid - mo.mean) / sd;
            }
        }
    }
    {
        for (rows, xs) |row, *x| x.* = row.ret_1;
        const mo = moments(xs);
        if (mo.count > 0) s.dispersion = mo.popStd();
    }
    {
        var up: usize = 0;
        var up_n: usize = 0;
        var above: usize = 0;
        var above_n: usize = 0;
        for (rows) |row| {
            if (math.isFinite(row.ret_1)) {
                up_n += 1;
                if (row.ret_1 > 0) up += 1;
            }
            if (math.isFinite(row.sma_distance)) {
                above_n += 1;
                if (row.sma_distance > 0) above += 1;
            }
        }
        if (up_n > 0) s.breadth_up = toF(up) / toF(up_n);
        if (above_n > 0) s.breadth_sma = toF(above) / toF(above_n);
    }
    return s;
}
