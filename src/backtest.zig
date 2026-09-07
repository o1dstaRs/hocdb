//! HOCDB signal backtester kernel: turns a target-position series over bars
//! into an equity curve with costs, slippage, protective exits, a trade list
//! and a full set of performance statistics. Pure computation: no I/O, O(n),
//! no per-bar allocation; the only allocation is one scratch equity buffer
//! when `Outputs.equity` is null.
//!
//! Inputs
//!   * `ts`, `close`, `target` of length n; `open` / `high` / `low` are
//!     optional. A null column is substituted bar by bar by the close, so
//!     without open the fills happen at the close and without high/low there
//!     are no intrabar stop checks (stops trigger on the close instead).
//!   * `target[i]` is the desired position at the END of bar i, i.e. decided
//!     from information up to and including close[i]. Its unit depends on
//!     `Params.position_mode`: 0 = units, 1 = fraction of current equity
//!     (1.0 = 100% long, -0.5 = 50% short), 2 = notional in currency.
//!   * NaN target -> hold the previous signal (initially 0). `allow_short`
//!     == 0 clamps negative targets to 0.
//!
//! Fills
//!   * A fill is generated whenever the effective target value differs from
//!     the value the current position was sized from (the "applied" signal).
//!     The position is therefore not rebalanced as equity drifts in mode 1,
//!     and the same signal value never generates a second fill.
//!   * fill_mode 0 (default, no look-ahead): the change implied by target[i]
//!     is executed at open[i+1]; fill_mode 1: at close[i]. Fill prices are
//!     adverse-slipped, buys at price * (1 + slippage), sells at
//!     price * (1 - slippage), and cost_bps is charged on |traded notional|
//!     (units * slipped price). Units in modes 1/2 are computed at the
//!     unslipped fill price from the equity marked at that price
//!     (cash + position * price); fractional units are allowed and |units|
//!     is capped by `max_position` when > 0.
//!
//! Stops
//!   * Checked on every valid bar while a position is open (including the
//!     entry bar for fill_mode 0), in the priority stop_loss, trailing_stop,
//!     take_profit, against the bar's high/low. stop_loss / take_profit
//!     levels are relative to the average slipped entry price of the trade;
//!     the trailing level is relative to the best price since entry (the
//!     entry fill price, then the max high / min low of the bars completed
//!     before the bar being checked).
//!   * The exit fills at the level, or at the open when the bar opens beyond
//!     the level (gap-through), pays cost + slippage and sets the position
//!     to 0 for that bar. Stopped positions are not re-entered on the same
//!     signal: the target series re-enters only when it changes to a
//!     different non-zero value (a change to 0 just clears the signal).
//!
//! Marking and statistics
//!   * equity[i] = cash + position * close[i] after the fills of bar i;
//!     r[i] = equity[i] / equity[i-1] - 1 with equity[-1] = initial_equity.
//!   * A bar with a NaN price (any of the used open/high/low/close) is
//!     skipped: no fill, no stop check, no mark; equity, cash and position
//!     are carried forward (r = 0) and a pending fill executes on the next
//!     valid bar. The target of such a bar is still read.
//!   * Statistics use the population std of bar returns. With
//!     periods_per_year (ppy) > 0: ann_return = (final/initial)^(ppy/n) - 1,
//!     ann_vol = std(r) * sqrt(ppy), sharpe = (mean(r) - rf/ppy) / std(r) *
//!     sqrt(ppy), sortino = (mean(r) - rf/ppy) / sqrt(mean(min(r,0)^2)) *
//!     sqrt(ppy); with ppy == 0 there is no scaling, rf is ignored and
//!     ann_return = total_return. calmar = ann_return / max_drawdown.
//!     Drawdowns are positive fractions below the running equity peak (the
//!     peak starts at initial_equity); max_drawdown_bars is the longest run
//!     of consecutive bars below a prior peak; avg_drawdown is the mean of
//!     the per-bar drawdown.
//!
//! Trades
//!   * A trade opens when the position goes from 0 to non-zero or flips sign
//!     (the flip closes the old trade and opens the new one at the same
//!     fill); increases / decreases without crossing zero belong to the same
//!     trade. entry_price = units-weighted average of the slipped entry-side
//!     fills, exit_price = the same over the exit-side fills, size = total
//!     units accumulated on the entry side, pnl = net realised cash flow of
//!     the trade (costs and slippage included), ret = pnl / (size *
//!     entry_price), bars = exit bar index - entry bar index.
//!   * A position still open at the end is reported with exit_reason 4,
//!     exit_ts 0, exit_price = last valid close and its unrealised pnl (no
//!     exit cost). It counts in n_trades / n_long_trades / n_short_trades
//!     but the per-trade statistics (win_rate, profit_factor,
//!     avg_trade_return, avg_win, avg_loss, best_trade, worst_trade,
//!     avg_holding_bars) cover closed trades only. avg_win, avg_loss,
//!     best_trade and worst_trade are in currency; avg_win / avg_loss are 0
//!     when there are closed trades but no winners / losers.
//!   * net_pnl = final_equity - initial_equity, gross_pnl = net_pnl +
//!     total_cost + total_slippage, turnover = sum |traded notional| /
//!     mean(equity), exposure = share of bars with a non-zero position,
//!     long_share = share of those bars that are long.
//!
//! Prices are assumed positive; equity must stay positive for the return
//! based statistics to be meaningful.
const std = @import("std");
const math = std.math;
const Allocator = std.mem.Allocator;
const ind = @import("indicators.zig");

pub const Error = ind.Error;
pub const nan = ind.nan;
pub const lanes = ind.lanes;
const V = @Vector(lanes, f64);
const isNan = ind.isNan;

/// Unit of `target` values.
pub const PositionMode = enum(u64) { units = 0, fraction = 1, notional = 2 };

/// When the position change implied by target[i] is executed.
pub const FillMode = enum(u64) { next_open = 0, same_close = 1 };

/// `Trade.exit_reason` values.
pub const ExitReason = enum(u64) { signal = 0, stop_loss = 1, take_profit = 2, trailing = 3, end_of_data = 4 };

pub const Params = extern struct {
    /// Starting equity (<= 0 -> 1.0).
    initial_equity: f64 = 1.0,
    /// Transaction cost per side in basis points of the traded notional.
    cost_bps: f64 = 0,
    /// Adverse price move per side in basis points on every fill.
    slippage_bps: f64 = 0,
    /// Stop loss as a fraction of the entry price (0 = none).
    stop_loss: f64 = 0,
    /// Take profit as a fraction of the entry price (0 = none).
    take_profit: f64 = 0,
    /// Trailing stop as a fraction from the best price since entry (0 = none).
    trailing_stop: f64 = 0,
    /// Cap on |position| in units (0 = none).
    max_position: f64 = 0,
    /// 0 = units, 1 = fraction of current equity, 2 = notional in currency.
    position_mode: u64 = 0,
    /// 0 = next bar open (default, no look-ahead), 1 = same bar close.
    fill_mode: u64 = 0,
    /// Bars per year for annualisation (0 = none).
    periods_per_year: f64 = 0,
    /// 0 clamps negative targets to 0.
    allow_short: u64 = 1,
    /// Annual risk-free rate used by sharpe / sortino.
    risk_free_rate: f64 = 0,
};

pub const Result = extern struct {
    n_bars: u64,
    n_trades: u64,
    n_long_trades: u64,
    n_short_trades: u64,
    final_equity: f64,
    total_return: f64,
    ann_return: f64,
    ann_vol: f64,
    sharpe: f64,
    sortino: f64,
    calmar: f64,
    max_drawdown: f64,
    max_drawdown_bars: u64,
    avg_drawdown: f64,
    win_rate: f64,
    profit_factor: f64,
    avg_trade_return: f64,
    avg_win: f64,
    avg_loss: f64,
    best_trade: f64,
    worst_trade: f64,
    avg_holding_bars: f64,
    exposure: f64,
    long_share: f64,
    turnover: f64,
    total_cost: f64,
    total_slippage: f64,
    n_stop_exits: u64,
    n_take_profit_exits: u64,
    n_trailing_exits: u64,
    gross_pnl: f64,
    net_pnl: f64,
};

pub const Trade = extern struct {
    entry_ts: i64,
    /// 0 when the trade is still open at the end of the data.
    exit_ts: i64,
    /// +1 long, -1 short.
    direction: i64,
    entry_price: f64,
    exit_price: f64,
    /// Units accumulated on the entry side (positive).
    size: f64,
    /// Net pnl in currency (costs and slippage included).
    pnl: f64,
    /// pnl / (size * entry_price).
    ret: f64,
    /// Exit bar index - entry bar index.
    bars: u64,
    /// See `ExitReason`.
    exit_reason: u64,
};

/// Optional per-bar outputs, each either null or of length n.
pub const Outputs = struct {
    equity: ?[]f64 = null,
    /// Position in units after the fills of the bar.
    position: ?[]f64 = null,
    cash: ?[]f64 = null,
    /// equity[i] - equity[i-1].
    pnl: ?[]f64 = null,
    /// Positive fraction below the running equity peak.
    drawdown: ?[]f64 = null,
};

/// Contiguous index ranges, end exclusive.
pub const Split = extern struct {
    train_start: u64,
    train_end: u64,
    test_start: u64,
    test_end: u64,
};

// ---------------------------------------------------------------------------
// Engine: cash / position accounting, fills, stops and trade bookkeeping
// ---------------------------------------------------------------------------

const Engine = struct {
    ts: []const i64,
    cost: f64,
    slip: f64,
    stop_loss: f64,
    take_profit: f64,
    trailing: f64,
    max_position: f64,
    mode: u64,
    trades_out: ?[]Trade,

    cash: f64,
    pos: f64 = 0,
    /// Most recent effective target value.
    sig: f64 = 0,
    /// Target value the current position was sized from.
    applied: f64 = 0,

    // open trade
    has_trade: bool = false,
    entry_ts: i64 = 0,
    entry_bar: usize = 0,
    dir: i64 = 0,
    entry_units: f64 = 0,
    entry_notional: f64 = 0,
    exit_units: f64 = 0,
    exit_notional: f64 = 0,
    /// Net cash flow of the open trade so far.
    flow: f64 = 0,
    /// Best price since entry (for the trailing stop).
    best: f64 = 0,

    // accumulators
    total_cost: f64 = 0,
    total_slippage: f64 = 0,
    traded: f64 = 0,
    n_trades: u64 = 0,
    n_long: u64 = 0,
    n_short: u64 = 0,
    n_closed: u64 = 0,
    n_win: u64 = 0,
    n_loss: u64 = 0,
    n_sl: u64 = 0,
    n_tp: u64 = 0,
    n_tr: u64 = 0,
    gains: f64 = 0,
    losses: f64 = 0,
    sum_ret: f64 = 0,
    sum_bars: u64 = 0,
    best_trade: f64 = -math.inf(f64),
    worst_trade: f64 = math.inf(f64),

    /// Size the current signal at `price` and trade the difference.
    fn fillSignal(self: *Engine, i: usize, price: f64) void {
        var units: f64 = switch (self.mode) {
            1 => self.sig * (self.cash + self.pos * price) / price,
            2 => self.sig / price,
            else => self.sig,
        };
        if (self.max_position > 0 and @abs(units) > self.max_position) {
            units = if (units > 0) self.max_position else -self.max_position;
        }
        self.fill(i, price, units, @intFromEnum(ExitReason.signal));
        self.applied = self.sig;
    }

    /// Move the position to `new_units` at the unslipped price `price`.
    fn fill(self: *Engine, i: usize, price: f64, new_units: f64, reason: u64) void {
        const delta = new_units - self.pos;
        if (delta == 0) return;
        const buying = delta > 0;
        const f = if (buying) price * (1.0 + self.slip) else price * (1.0 - self.slip);
        const qty = @abs(delta);
        const notional = qty * f;
        const cost = notional * self.cost;
        self.cash -= delta * f + cost;
        self.total_cost += cost;
        self.total_slippage += qty * price * self.slip;
        self.traded += notional;
        const old = self.pos;
        self.pos = new_units;
        if (old == 0) {
            self.openTrade(i, new_units, f);
            return;
        }
        if ((old > 0) == buying) {
            // increase
            self.entry_units += qty;
            self.entry_notional += notional;
            self.flow -= delta * f + cost;
            return;
        }
        // decrease, close or flip: the part that offsets the old position
        const close_qty = @min(qty, @abs(old));
        const close_notional = close_qty * f;
        const close_cost = close_notional * self.cost;
        self.exit_units += close_qty;
        self.exit_notional += close_notional;
        self.flow += (if (old > 0) close_notional else -close_notional) - close_cost;
        if ((old > 0 and new_units > 0) or (old < 0 and new_units < 0)) return;
        self.finishTrade(i, self.ts[i], reason);
        if (new_units != 0) self.openTrade(i, new_units, f);
    }

    fn openTrade(self: *Engine, i: usize, units: f64, f: f64) void {
        const q = @abs(units);
        self.has_trade = true;
        self.entry_ts = self.ts[i];
        self.entry_bar = i;
        self.dir = if (units > 0) 1 else -1;
        self.entry_units = q;
        self.entry_notional = q * f;
        self.exit_units = 0;
        self.exit_notional = 0;
        self.flow = -(units * f) - q * f * self.cost;
        self.best = f;
        self.n_trades += 1;
        if (units > 0) self.n_long += 1 else self.n_short += 1;
    }

    fn finishTrade(self: *Engine, i: usize, exit_ts: i64, reason: u64) void {
        const pnl = self.flow;
        const ret = pnl / self.entry_notional;
        const bars: u64 = @intCast(i - self.entry_bar);
        const t = Trade{
            .entry_ts = self.entry_ts,
            .exit_ts = exit_ts,
            .direction = self.dir,
            .entry_price = self.entry_notional / self.entry_units,
            .exit_price = self.exit_notional / self.exit_units,
            .size = self.entry_units,
            .pnl = pnl,
            .ret = ret,
            .bars = bars,
            .exit_reason = reason,
        };
        if (self.trades_out) |out| {
            const idx = self.n_trades - 1;
            if (idx < out.len) out[idx] = t;
        }
        self.has_trade = false;
        if (reason == @intFromEnum(ExitReason.end_of_data)) return;
        self.n_closed += 1;
        if (pnl > 0) {
            self.n_win += 1;
            self.gains += pnl;
        } else if (pnl < 0) {
            self.n_loss += 1;
            self.losses += pnl;
        }
        self.sum_ret += ret;
        self.sum_bars += bars;
        if (pnl > self.best_trade) self.best_trade = pnl;
        if (pnl < self.worst_trade) self.worst_trade = pnl;
        switch (reason) {
            @intFromEnum(ExitReason.stop_loss) => self.n_sl += 1,
            @intFromEnum(ExitReason.take_profit) => self.n_tp += 1,
            @intFromEnum(ExitReason.trailing) => self.n_tr += 1,
            else => {},
        }
    }

    /// Protective exits for the open position against bar i (o, h, l).
    fn checkStops(self: *Engine, i: usize, o: f64, h: f64, l: f64) void {
        const long = self.pos > 0;
        const entry = self.entry_notional / self.entry_units;
        if (self.stop_loss > 0) {
            const lvl = if (long) entry * (1.0 - self.stop_loss) else entry * (1.0 + self.stop_loss);
            if (if (long) l <= lvl else h >= lvl) {
                self.fill(i, adverseFill(long, o, lvl), 0, @intFromEnum(ExitReason.stop_loss));
                return;
            }
        }
        if (self.trailing > 0) {
            const lvl = if (long) self.best * (1.0 - self.trailing) else self.best * (1.0 + self.trailing);
            if (if (long) l <= lvl else h >= lvl) {
                self.fill(i, adverseFill(long, o, lvl), 0, @intFromEnum(ExitReason.trailing));
                return;
            }
        }
        if (self.take_profit > 0) {
            const lvl = if (long) entry * (1.0 + self.take_profit) else entry * (1.0 - self.take_profit);
            if (if (long) h >= lvl else l <= lvl) {
                self.fill(i, favourableFill(long, o, lvl), 0, @intFromEnum(ExitReason.take_profit));
                return;
            }
        }
        self.best = if (long) @max(self.best, h) else @min(self.best, l);
    }

    /// Close the position at the end of the data (exit_reason 4).
    fn closeAtEnd(self: *Engine, i: usize, last_close: f64) void {
        if (!self.has_trade) return;
        const q = @abs(self.pos);
        self.exit_units += q;
        self.exit_notional += q * last_close;
        self.flow += self.pos * last_close;
        self.finishTrade(i, 0, @intFromEnum(ExitReason.end_of_data));
    }
};

/// Fill price of a stop-loss / trailing exit: the level, or the open when
/// the bar opens beyond it.
inline fn adverseFill(long: bool, o: f64, lvl: f64) f64 {
    return if (long) (if (o <= lvl) o else lvl) else (if (o >= lvl) o else lvl);
}

/// Fill price of a take-profit exit: the level, or the open when the bar
/// opens beyond it.
inline fn favourableFill(long: bool, o: f64, lvl: f64) f64 {
    return if (long) (if (o >= lvl) o else lvl) else (if (o <= lvl) o else lvl);
}

// ---------------------------------------------------------------------------
// Statistics over the equity curve (SIMD, compensated sums)
// ---------------------------------------------------------------------------

inline fn load(s: []const f64, i: usize) V {
    return s[i..][0..lanes].*;
}

/// Neumaier-compensated accumulator, one running sum per lane.
const VecAcc = struct {
    sum: V = @splat(0),
    c: V = @splat(0),

    inline fn add(self: *VecAcc, x: V) void {
        const t = self.sum + x;
        const big = @abs(self.sum) >= @abs(x);
        self.c += @select(f64, big, (self.sum - t) + x, (x - t) + self.sum);
        self.sum = t;
    }

    inline fn addScalar(self: *VecAcc, x: f64) void {
        var v: V = @splat(0);
        v[0] = x;
        self.add(v);
    }

    fn total(self: VecAcc) f64 {
        return @reduce(.Add, self.sum) + @reduce(.Add, self.c);
    }
};

const Moments = struct {
    mean_r: f64,
    /// sum (r - mean_r)^2
    m2: f64,
    /// sum min(r, 0)^2
    down2: f64,
    mean_eq: f64,
};

/// Mean equity and the first two moments of the bar returns
/// r[i] = eq[i] / eq[i-1] - 1 (eq[-1] = e0). Two passes, no allocation.
fn moments(eq: []const f64, e0: f64) Moments {
    const n = eq.len;
    const nf: f64 = @floatFromInt(n);
    const one: V = @splat(1.0);
    const zero: V = @splat(0.0);
    var sr = VecAcc{};
    var se = VecAcc{};
    sr.addScalar(eq[0] / e0 - 1.0);
    se.addScalar(eq[0]);
    var i: usize = 1;
    while (i + lanes <= n) : (i += lanes) {
        const cur = load(eq, i);
        sr.add(cur / load(eq, i - 1) - one);
        se.add(cur);
    }
    while (i < n) : (i += 1) {
        sr.addScalar(eq[i] / eq[i - 1] - 1.0);
        se.addScalar(eq[i]);
    }
    const mean_r = sr.total() / nf;
    const mean_eq = se.total() / nf;
    const mv: V = @splat(mean_r);
    var m2 = VecAcc{};
    var dn = VecAcc{};
    {
        const r0 = eq[0] / e0 - 1.0;
        const d0 = r0 - mean_r;
        const n0 = @min(r0, 0.0);
        m2.addScalar(d0 * d0);
        dn.addScalar(n0 * n0);
    }
    i = 1;
    while (i + lanes <= n) : (i += lanes) {
        const r = load(eq, i) / load(eq, i - 1) - one;
        const d = r - mv;
        const neg = @min(r, zero);
        m2.add(d * d);
        dn.add(neg * neg);
    }
    while (i < n) : (i += 1) {
        const r = eq[i] / eq[i - 1] - 1.0;
        const d = r - mean_r;
        const neg = @min(r, 0.0);
        m2.addScalar(d * d);
        dn.addScalar(neg * neg);
    }
    return .{ .mean_r = mean_r, .m2 = m2.total(), .down2 = dn.total(), .mean_eq = mean_eq };
}

fn nanResult() Result {
    var r = std.mem.zeroes(Result);
    inline for (@typeInfo(Result).@"struct".fields) |f| {
        if (f.type == f64) @field(r, f.name) = nan;
    }
    return r;
}

fn validParam(x: f64) bool {
    return !isNan(x) and x >= 0;
}

fn checkLen(col: ?[]const f64, n: usize) Error!void {
    if (col) |c| if (c.len != n) return Error.LengthMismatch;
}

fn checkOut(col: ?[]f64, n: usize) Error!void {
    if (col) |c| if (c.len != n) return Error.LengthMismatch;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run the backtest (semantics in the module doc). `trades` is an optional
/// capacity buffer: up to `trades.len` trades are written in order while
/// `Result.n_trades` counts all of them. Every non-null `Outputs` slice must
/// have `close.len` entries.
pub fn run(
    ts: []const i64,
    open: ?[]const f64,
    high: ?[]const f64,
    low: ?[]const f64,
    close: []const f64,
    target: []const f64,
    params: Params,
    out: Outputs,
    trades: ?[]Trade,
    allocator: Allocator,
) Error!Result {
    const n = close.len;
    if (ts.len != n or target.len != n) return Error.LengthMismatch;
    try checkLen(open, n);
    try checkLen(high, n);
    try checkLen(low, n);
    try checkOut(out.equity, n);
    try checkOut(out.position, n);
    try checkOut(out.cash, n);
    try checkOut(out.pnl, n);
    try checkOut(out.drawdown, n);
    if (!validParam(params.cost_bps) or !validParam(params.slippage_bps) or
        !validParam(params.stop_loss) or !validParam(params.take_profit) or
        !validParam(params.trailing_stop) or !validParam(params.max_position) or
        !validParam(params.periods_per_year) or isNan(params.initial_equity) or
        isNan(params.risk_free_rate) or params.position_mode > 2 or params.fill_mode > 1)
    {
        return Error.InvalidParameter;
    }
    const e0: f64 = if (params.initial_equity > 0) params.initial_equity else 1.0;
    var res = nanResult();
    res.n_bars = n;
    res.final_equity = e0;
    res.total_return = 0;
    res.ann_return = 0;
    res.max_drawdown = 0;
    res.avg_drawdown = 0;
    res.exposure = 0;
    res.total_cost = 0;
    res.total_slippage = 0;
    res.gross_pnl = 0;
    res.net_pnl = 0;
    if (n == 0) return res;

    const owned = out.equity == null;
    const eq_buf: []f64 = if (out.equity) |e| e else try allocator.alloc(f64, n);
    defer if (owned) allocator.free(eq_buf);

    var eng = Engine{
        .ts = ts,
        .cost = params.cost_bps / 10_000.0,
        .slip = params.slippage_bps / 10_000.0,
        .stop_loss = params.stop_loss,
        .take_profit = params.take_profit,
        .trailing = params.trailing_stop,
        .max_position = params.max_position,
        .mode = params.position_mode,
        .trades_out = trades,
        .cash = e0,
    };
    const next_open = params.fill_mode == @intFromEnum(FillMode.next_open);
    const no_short = params.allow_short == 0;

    var peak = e0;
    var under: u64 = 0;
    var longest: u64 = 0;
    var mdd: f64 = 0;
    var sum_dd: f64 = 0;
    var prev_eq = e0;
    var last_close: f64 = nan;
    var exposed: u64 = 0;
    var long_bars: u64 = 0;

    for (0..n) |i| {
        const c = close[i];
        const o = if (open) |x| x[i] else c;
        const h = if (high) |x| x[i] else c;
        const l = if (low) |x| x[i] else c;
        const valid = !(isNan(c) or isNan(o) or isNan(h) or isNan(l));
        if (valid) {
            // pending change decided on the previous bar
            if (next_open and eng.sig != eng.applied) eng.fillSignal(i, o);
            if (eng.pos != 0) eng.checkStops(i, o, h, l);
        }
        const t = target[i];
        if (!isNan(t)) eng.sig = if (no_short and t < 0) 0 else t;
        if (valid and !next_open and eng.sig != eng.applied) eng.fillSignal(i, c);

        var eq = prev_eq;
        if (valid) {
            eq = eng.cash + eng.pos * c;
            last_close = c;
        }
        if (eq >= peak) {
            peak = eq;
            under = 0;
        } else {
            under += 1;
            if (under > longest) longest = under;
        }
        const dd = 1.0 - eq / peak;
        if (dd > mdd) mdd = dd;
        sum_dd += dd;
        eq_buf[i] = eq;
        if (out.position) |x| x[i] = eng.pos;
        if (out.cash) |x| x[i] = eng.cash;
        if (out.pnl) |x| x[i] = eq - prev_eq;
        if (out.drawdown) |x| x[i] = dd;
        if (eng.pos != 0) {
            exposed += 1;
            if (eng.pos > 0) long_bars += 1;
        }
        prev_eq = eq;
    }
    if (eng.has_trade) eng.closeAtEnd(n - 1, last_close);

    const nf: f64 = @floatFromInt(n);
    const final = eq_buf[n - 1];
    const ppy = params.periods_per_year;
    const scale: f64 = if (ppy > 0) @sqrt(ppy) else 1.0;
    const rf_pp: f64 = if (ppy > 0) params.risk_free_rate / ppy else 0.0;
    const mom = moments(eq_buf, e0);
    const std_r = @sqrt(mom.m2 / nf);
    const dd_dev = @sqrt(mom.down2 / nf);

    res.n_trades = eng.n_trades;
    res.n_long_trades = eng.n_long;
    res.n_short_trades = eng.n_short;
    res.final_equity = final;
    res.total_return = final / e0 - 1.0;
    res.ann_return = if (ppy > 0) math.pow(f64, final / e0, ppy / nf) - 1.0 else res.total_return;
    res.ann_vol = std_r * scale;
    res.sharpe = if (std_r > 0) (mom.mean_r - rf_pp) / std_r * scale else nan;
    res.sortino = if (dd_dev > 0) (mom.mean_r - rf_pp) / dd_dev * scale else nan;
    res.max_drawdown = mdd;
    res.calmar = if (mdd > 0) res.ann_return / mdd else nan;
    res.max_drawdown_bars = longest;
    res.avg_drawdown = sum_dd / nf;
    if (eng.n_closed > 0) {
        const nc: f64 = @floatFromInt(eng.n_closed);
        res.win_rate = @as(f64, @floatFromInt(eng.n_win)) / nc;
        res.profit_factor = if (eng.losses < 0) eng.gains / -eng.losses else if (eng.gains > 0) math.inf(f64) else nan;
        res.avg_trade_return = eng.sum_ret / nc;
        res.avg_win = if (eng.n_win > 0) eng.gains / @as(f64, @floatFromInt(eng.n_win)) else 0;
        res.avg_loss = if (eng.n_loss > 0) eng.losses / @as(f64, @floatFromInt(eng.n_loss)) else 0;
        res.best_trade = eng.best_trade;
        res.worst_trade = eng.worst_trade;
        res.avg_holding_bars = @as(f64, @floatFromInt(eng.sum_bars)) / nc;
    }
    res.exposure = @as(f64, @floatFromInt(exposed)) / nf;
    res.long_share = if (exposed > 0) @as(f64, @floatFromInt(long_bars)) / @as(f64, @floatFromInt(exposed)) else nan;
    res.turnover = eng.traded / mom.mean_eq;
    res.total_cost = eng.total_cost;
    res.total_slippage = eng.total_slippage;
    res.n_stop_exits = eng.n_sl;
    res.n_take_profit_exits = eng.n_tp;
    res.n_trailing_exits = eng.n_tr;
    res.net_pnl = final - e0;
    res.gross_pnl = res.net_pnl + eng.total_cost + eng.total_slippage;
    return res;
}

/// Walk-forward index ranges over n bars. The first train window covers
/// floor(train_frac * n) bars; the test windows tile the remaining bars in
/// `n_splits` contiguous, non-overlapping pieces (the last one takes the
/// remainder). Anchored: train windows expand from index 0; rolling: each
/// train window is the floor(train_frac * n) bars before its test window.
/// Writes at most `out.len` splits and returns the number written (0 when
/// the request is not satisfiable).
pub fn walkForwardSplits(n: usize, n_splits: usize, train_frac: f64, anchored: bool, out: []Split) usize {
    if (n == 0 or n_splits == 0 or out.len == 0) return 0;
    if (!(train_frac > 0 and train_frac < 1)) return 0;
    const train_len: usize = @intFromFloat(@floor(train_frac * @as(f64, @floatFromInt(n))));
    if (train_len == 0) return 0;
    const test_len = (n - train_len) / n_splits;
    if (test_len == 0) return 0;
    const count = @min(n_splits, out.len);
    for (0..count) |k| {
        const test_start = train_len + k * test_len;
        const test_end = if (k == n_splits - 1) n else test_start + test_len;
        out[k] = .{
            .train_start = if (anchored) 0 else test_start - train_len,
            .train_end = test_start,
            .test_start = test_start,
            .test_end = test_end,
        };
    }
    return count;
}

inline fn sliceOpt(col: ?[]const f64, a: usize, b: usize) ?[]const f64 {
    return if (col) |c| c[a..b] else null;
}

/// Run the backtest independently on every test window of `splits` (fresh
/// initial equity each, positions from the same `target` slice), writing
/// `results[k]`. Returns the number of windows run (min of both lengths).
pub fn runSplits(
    ts: []const i64,
    open: ?[]const f64,
    high: ?[]const f64,
    low: ?[]const f64,
    close: []const f64,
    target: []const f64,
    params: Params,
    splits: []const Split,
    results: []Result,
    allocator: Allocator,
) Error!usize {
    const n = close.len;
    if (ts.len != n or target.len != n) return Error.LengthMismatch;
    try checkLen(open, n);
    try checkLen(high, n);
    try checkLen(low, n);
    const count = @min(splits.len, results.len);
    for (splits[0..count], results[0..count]) |sp, *r| {
        if (sp.test_start > sp.test_end or sp.test_end > n) return Error.InvalidParameter;
        const a: usize = @intCast(sp.test_start);
        const b: usize = @intCast(sp.test_end);
        r.* = try run(ts[a..b], sliceOpt(open, a, b), sliceOpt(high, a, b), sliceOpt(low, a, b), close[a..b], target[a..b], params, .{}, null, allocator);
    }
    return count;
}
