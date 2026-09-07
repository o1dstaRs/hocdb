//! Tests for the signal backtester kernel (src/backtest.zig). The "tied
//! example" numbers are produced by the independent Python reference
//! scripts/stress/references_backtest.py (same inputs, same expected values).
const std = @import("std");
const bt = @import("backtest.zig");
const ind = @import("indicators.zig");
const testing = std.testing;
const nan = ind.nan;

fn approx(expected: f64, actual: f64, tol: f64) !void {
    if (ind.isNan(expected) and ind.isNan(actual)) return;
    try testing.expectApproxEqAbs(expected, actual, tol);
}

fn ramp(n: usize, start: f64, step: f64, buf: []f64) []f64 {
    for (0..n) |i| buf[i] = start + step * @as(f64, @floatFromInt(i));
    return buf[0..n];
}

fn seq(n: usize, buf: []i64) []i64 {
    for (0..n) |i| buf[i] = @intCast(i + 1);
    return buf[0..n];
}

test "tied example matches the Python reference" {
    const ts = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const open = [_]f64{ 100.0, 101.0, 102.0, 98.0, 95.0, 97.0, 99.0, 103.0, 104.0, 102.0, 100.0, 99.0 };
    const high = [_]f64{ 101.0, 103.0, 103.0, 99.0, 97.0, 99.0, 104.0, 105.0, 105.0, 103.0, 101.0, 100.0 };
    const low = [_]f64{ 99.0, 100.0, 96.0, 93.0, 94.0, 96.0, 98.0, 102.0, 101.0, 99.0, 98.0, 97.0 };
    const close = [_]f64{ 100.5, 102.5, 97.0, 94.0, 96.5, 98.5, 103.5, 104.5, 102.0, 100.0, 98.5, 99.5 };
    const target = [_]f64{ 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, -1.0, -1.0, -1.0, -1.0 };
    const params = bt.Params{ .initial_equity = 1000.0, .cost_bps = 10.0, .slippage_bps = 5.0, .stop_loss = 0.05, .periods_per_year = 252.0 };
    var equity: [12]f64 = undefined;
    var position: [12]f64 = undefined;
    var trades: [8]bt.Trade = undefined;
    const r = try bt.run(&ts, &open, &high, &low, &close, &target, params, .{ .equity = &equity, .position = &position }, &trades, testing.allocator);
    try testing.expectEqual(@as(u64, 12), r.n_bars);
    try testing.expectEqual(@as(u64, 3), r.n_trades);
    try testing.expectEqual(@as(u64, 2), r.n_long_trades);
    try testing.expectEqual(@as(u64, 1), r.n_short_trades);
    try approx(1002.4465295364876, r.final_equity, 1e-9);
    try approx(0.002446529536487496, r.total_return, 1e-12);
    try approx(0.05265376905060748, r.ann_return, 1e-12);
    try approx(0.054923204627863896, r.ann_vol, 1e-12);
    try approx(0.961805823133203, r.sharpe, 1e-9);
    try approx(1.52393980545712, r.sortino, 1e-9);
    try approx(7.933339836813163, r.calmar, 1e-9);
    try approx(0.006637024271452185, r.max_drawdown, 1e-12);
    try testing.expectEqual(@as(u64, 4), r.max_drawdown_bars);
    try approx(0.0032256909020254945, r.avg_drawdown, 1e-12);
    try approx(0.5, r.win_rate, 1e-12);
    try approx(1.0187783062018276, r.profit_factor, 1e-9);
    try approx(-0.01259027803768568, r.avg_trade_return, 1e-12);
    try approx(5.397003000000012, r.avg_win, 1e-9);
    try approx(-5.297524463512502, r.avg_loss, 1e-9);
    try approx(5.397003000000012, r.best_trade, 1e-9);
    try approx(-5.297524463512502, r.worst_trade, 1e-9);
    try approx(2.5, r.avg_holding_bars, 1e-12);
    try approx(0.6666666666666666, r.exposure, 1e-12);
    try approx(0.625, r.long_share, 1e-12);
    try approx(0.7010422825639875, r.turnover, 1e-9);
    try approx(0.7009464760125, r.total_cost, 1e-9);
    try approx(0.35049898749999997, r.total_slippage, 1e-9);
    try testing.expectEqual(@as(u64, 1), r.n_stop_exits);
    try testing.expectEqual(@as(u64, 0), r.n_take_profit_exits);
    try approx(3.4979750000000602, r.gross_pnl, 1e-9);
    try approx(2.44652953648756, r.net_pnl, 1e-9);
    const eq_ref = [_]f64{ 1000.0, 1001.3484495, 995.8484495, 994.7024755365, 994.7024755365, 994.7024755365, 1003.4053765365, 1005.4053765365, 1000.4053765365, 1001.9465295365, 1003.4465295365, 1002.4465295365 };
    for (eq_ref, equity) |e, a| try approx(e, a, 1e-9);
    try testing.expectEqual(@as(i64, 2), trades[0].entry_ts);
    try testing.expectEqual(@as(i64, 4), trades[0].exit_ts);
    try testing.expectEqual(@as(i64, 1), trades[0].direction);
    try approx(101.0505, trades[0].entry_price, 1e-9);
    try approx(95.9499760125, trades[0].exit_price, 1e-9);
    try approx(-5.297524463512502, trades[0].pnl, 1e-9);
    try testing.expectEqual(@as(u64, 2), trades[0].bars);
    try testing.expectEqual(@intFromEnum(bt.ExitReason.stop_loss), trades[0].exit_reason);
    try testing.expectEqual(@as(i64, 7), trades[1].entry_ts);
    try approx(2.0, trades[1].size, 1e-12);
    try approx(0.02724396892462866, trades[1].ret, 1e-12);
    try testing.expectEqual(@as(i64, -1), trades[2].direction);
    try testing.expectEqual(@as(i64, 0), trades[2].exit_ts);
    try testing.expectEqual(@intFromEnum(bt.ExitReason.end_of_data), trades[2].exit_reason);
    try approx(2.3470510000000075, trades[2].pnl, 1e-9);
    // position outputs: stopped at bar 3, re-entered at bar 6 with 2 units, flipped to -1 at bar 9
    try approx(1.0, position[1], 0);
    try approx(0.0, position[3], 0);
    try approx(2.0, position[6], 0);
    try approx(-1.0, position[9], 0);
}

test "flat target: no trades, constant equity" {
    var tb: [50]i64 = undefined;
    var cb: [50]f64 = undefined;
    const ts = seq(50, &tb);
    const close = ramp(50, 100, 0.5, &cb);
    const target = [_]f64{0} ** 50;
    const r = try bt.run(ts, null, null, null, close, &target, .{ .initial_equity = 10 }, .{}, null, testing.allocator);
    try testing.expectEqual(@as(u64, 0), r.n_trades);
    try approx(10, r.final_equity, 0);
    try approx(0, r.total_return, 0);
    try approx(0, r.exposure, 0);
    try approx(0, r.max_drawdown, 0);
    try testing.expect(ind.isNan(r.win_rate));
    try testing.expect(ind.isNan(r.sharpe)); // zero variance
}

test "always long with full equity and no costs equals buy-and-hold from the first fill" {
    var tb: [40]i64 = undefined;
    var cb: [40]f64 = undefined;
    var ob: [40]f64 = undefined;
    const ts = seq(40, &tb);
    const close = ramp(40, 100, 1.5, &cb);
    const open = ramp(40, 99.5, 1.5, &ob);
    const target = [_]f64{1.0} ** 40;
    const r = try bt.run(ts, open, null, null, close, &target, .{ .initial_equity = 500, .position_mode = 1 }, .{}, null, testing.allocator);
    try testing.expectEqual(@as(u64, 1), r.n_trades);
    try approx(close[39] / open[1] - 1.0, r.total_return, 1e-12);
    try approx(1.0 - 1.0 / 40.0, r.exposure, 1e-12);
    try approx(1.0, r.long_share, 0);
    try approx(0, r.total_cost, 0);
    // fill_mode 1 fills at the same close
    const r2 = try bt.run(ts, open, null, null, close, &target, .{ .initial_equity = 500, .position_mode = 1, .fill_mode = 1 }, .{}, null, testing.allocator);
    try approx(close[39] / close[0] - 1.0, r2.total_return, 1e-12);
    try approx(1.0, r2.exposure, 0);
}

test "flip long to short: two trades with correctly signed slippage" {
    var tb: [10]i64 = undefined;
    var cb: [10]f64 = undefined;
    var ob: [10]f64 = undefined;
    const ts = seq(10, &tb);
    const close = ramp(10, 100, 1, &cb);
    const open = ramp(10, 100, 1, &ob);
    const target = [_]f64{ 1, 1, 1, 1, 1, -1, -1, -1, -1, -1 };
    var trades: [4]bt.Trade = undefined;
    const r = try bt.run(ts, open, null, null, close, &target, .{ .initial_equity = 1000, .slippage_bps = 10 }, .{}, &trades, testing.allocator);
    try testing.expectEqual(@as(u64, 2), r.n_trades);
    try approx(open[1] * 1.001, trades[0].entry_price, 1e-12);
    try approx(open[6] * 0.999, trades[0].exit_price, 1e-12);
    try testing.expectEqual(@as(i64, 7), trades[0].exit_ts); // the flip decided at bar 5 fills at bar 6's open
    try testing.expectEqual(@as(i64, 7), trades[1].entry_ts);
    try approx(open[6] * 0.999, trades[1].entry_price, 1e-12);
    try testing.expectEqual(@as(i64, -1), trades[1].direction);
    try testing.expectEqual(@intFromEnum(bt.ExitReason.end_of_data), trades[1].exit_reason);
    try approx(trades[0].pnl + trades[1].pnl, r.net_pnl, 1e-9);
    try approx(0.9, r.exposure, 1e-12); // bars 1..9 carry a position
}

test "stop loss, take profit and trailing exits (levels and gap-through)" {
    const ts = [_]i64{ 1, 2, 3, 4, 5, 6 };
    // long 1 unit filled at open[1] = 100; bar 3 gaps down through the 5% stop (open 90)
    const open = [_]f64{ 100, 100, 101, 90, 91, 92 };
    const high = [_]f64{ 101, 102, 103, 92, 93, 94 };
    const low = [_]f64{ 99, 99, 100, 89, 90, 91 };
    const close = [_]f64{ 100, 101, 102, 91, 92, 93 };
    const target = [_]f64{ 1, 1, 1, 1, 1, 1 };
    var trades: [2]bt.Trade = undefined;
    const r = try bt.run(&ts, &open, &high, &low, &close, &target, .{ .initial_equity = 1000, .stop_loss = 0.05 }, .{}, &trades, testing.allocator);
    try testing.expectEqual(@as(u64, 1), r.n_stop_exits);
    try approx(90, trades[0].exit_price, 0); // gap-through fills at the open
    try testing.expectEqual(@as(u64, 1), r.n_trades); // not re-entered on the same signal
    try approx(-10, r.net_pnl, 1e-9);
    // the same with the stop touched intrabar: fills at the level
    const low2 = [_]f64{ 99, 99, 100, 94, 90, 91 };
    const open2 = [_]f64{ 100, 100, 101, 98, 91, 92 };
    const r2 = try bt.run(&ts, &open2, &high, &low2, &close, &target, .{ .initial_equity = 1000, .stop_loss = 0.05 }, .{}, &trades, testing.allocator);
    try approx(95, trades[0].exit_price, 1e-12);
    try approx(-5, r2.net_pnl, 1e-9);
    // take profit at +3%: bar 2 high 103 -> exit at 103
    const r3 = try bt.run(&ts, &open, &high, &low, &close, &target, .{ .initial_equity = 1000, .take_profit = 0.03 }, .{}, &trades, testing.allocator);
    try testing.expectEqual(@as(u64, 1), r3.n_take_profit_exits);
    try approx(103, trades[0].exit_price, 1e-12);
    try approx(3, r3.net_pnl, 1e-9);
    // trailing 4%: best after bar 2 = 103 -> level 98.88; bar 3 opens at 90 -> exit at 90
    const r4 = try bt.run(&ts, &open, &high, &low, &close, &target, .{ .initial_equity = 1000, .trailing_stop = 0.04 }, .{}, &trades, testing.allocator);
    try testing.expectEqual(@as(u64, 1), r4.n_trailing_exits);
    try approx(90, trades[0].exit_price, 1e-12);
    // short side: target -1, price rallies through the stop
    const target_s = [_]f64{ -1, -1, -1, -1, -1, -1 };
    const open_s = [_]f64{ 100, 100, 101, 104, 105, 106 };
    const high_s = [_]f64{ 101, 102, 103, 107, 106, 107 };
    const low_s = [_]f64{ 99, 99, 100, 103, 104, 105 };
    const close_s = [_]f64{ 100, 101, 102, 106, 105, 106 };
    const r5 = try bt.run(&ts, &open_s, &high_s, &low_s, &close_s, &target_s, .{ .initial_equity = 1000, .stop_loss = 0.05 }, .{}, &trades, testing.allocator);
    try testing.expectEqual(@as(u64, 1), r5.n_stop_exits);
    try testing.expectEqual(@as(i64, -1), trades[0].direction);
    try approx(105, trades[0].exit_price, 1e-12);
    try approx(-5, r5.net_pnl, 1e-9);
}

test "position sizing modes, max_position and allow_short" {
    var tb: [6]i64 = undefined;
    const ts = seq(6, &tb);
    const close = [_]f64{ 50, 50, 50, 50, 50, 50 };
    var pos: [6]f64 = undefined;
    const t_units = [_]f64{ 3, 3, 3, 3, 3, 3 };
    _ = try bt.run(ts, null, null, null, &close, &t_units, .{ .initial_equity = 1000 }, .{ .position = &pos }, null, testing.allocator);
    try approx(3, pos[1], 0);
    const t_frac = [_]f64{ 0.5, 0.5, 0.5, 0.5, 0.5, 0.5 };
    _ = try bt.run(ts, null, null, null, &close, &t_frac, .{ .initial_equity = 1000, .position_mode = 1 }, .{ .position = &pos }, null, testing.allocator);
    try approx(10, pos[1], 1e-12); // 50% of 1000 at price 50
    const t_notional = [_]f64{ 250, 250, 250, 250, 250, 250 };
    _ = try bt.run(ts, null, null, null, &close, &t_notional, .{ .initial_equity = 1000, .position_mode = 2 }, .{ .position = &pos }, null, testing.allocator);
    try approx(5, pos[1], 1e-12);
    _ = try bt.run(ts, null, null, null, &close, &t_units, .{ .initial_equity = 1000, .max_position = 2 }, .{ .position = &pos }, null, testing.allocator);
    try approx(2, pos[1], 0);
    const t_short = [_]f64{ -1, -1, -1, -1, -1, -1 };
    const r = try bt.run(ts, null, null, null, &close, &t_short, .{ .initial_equity = 1000, .allow_short = 0 }, .{ .position = &pos }, null, testing.allocator);
    try testing.expectEqual(@as(u64, 0), r.n_trades);
    try approx(0, pos[3], 0);
}

test "drawdown statistics and annualisation" {
    const ts = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const close = [_]f64{ 100, 100, 110, 99, 105, 120, 108, 130 };
    const target = [_]f64{ 1, 1, 1, 1, 1, 1, 1, 1 };
    var dd: [8]f64 = undefined;
    const r = try bt.run(&ts, null, null, null, &close, &target, .{ .initial_equity = 1000, .fill_mode = 1, .periods_per_year = 252 }, .{ .drawdown = &dd }, null, testing.allocator);
    // equity = 1000 + (close - 100): 1000,1000,1010,999,1005,1020,1008,1030
    // drawdowns: 11/1010 (bars 3-4, below the 1010 peak) and 12/1020 (bar 6): the latter is deeper
    try approx(12.0 / 1020.0, r.max_drawdown, 1e-12);
    try testing.expectEqual(@as(u64, 2), r.max_drawdown_bars); // bars 3 and 4 below the 1010 peak
    try approx(11.0 / 1010.0, dd[3], 1e-12);
    try approx(0, dd[7], 0);
    try approx(std.math.pow(f64, 1.03, 252.0 / 8.0) - 1.0, r.ann_return, 1e-12);
    try approx(r.ann_return / r.max_drawdown, r.calmar, 1e-9);
    try testing.expect(r.ann_vol > 0 and r.sharpe > 0);
}

test "walk-forward splits and runSplits" {
    var out: [8]bt.Split = undefined;
    const n = bt.walkForwardSplits(100, 4, 0.5, true, &out);
    try testing.expectEqual(@as(usize, 4), n);
    try testing.expectEqual(bt.Split{ .train_start = 0, .train_end = 50, .test_start = 50, .test_end = 62 }, out[0]);
    try testing.expectEqual(bt.Split{ .train_start = 0, .train_end = 86, .test_start = 86, .test_end = 100 }, out[3]);
    const m = bt.walkForwardSplits(100, 4, 0.5, false, &out);
    try testing.expectEqual(@as(usize, 4), m);
    try testing.expectEqual(bt.Split{ .train_start = 36, .train_end = 86, .test_start = 86, .test_end = 100 }, out[3]);
    // test windows tile [50, 100) without overlap
    for (out[0..3], out[1..4]) |a, b| try testing.expectEqual(a.test_end, b.test_start);
    try testing.expectEqual(@as(usize, 0), bt.walkForwardSplits(10, 20, 0.5, true, &out));
    try testing.expectEqual(@as(usize, 0), bt.walkForwardSplits(100, 4, 1.5, true, &out));
    var tb: [100]i64 = undefined;
    var cb: [100]f64 = undefined;
    const ts = seq(100, &tb);
    const close = ramp(100, 100, 0.25, &cb);
    const target = [_]f64{1} ** 100;
    var results: [4]bt.Result = undefined;
    const k = try bt.runSplits(ts, null, null, null, close, &target, .{ .fill_mode = 1, .position_mode = 1 }, out[0..4], &results, testing.allocator);
    try testing.expectEqual(@as(usize, 4), k);
    for (results, out[0..4]) |r, sp| {
        try testing.expectEqual(sp.test_end - sp.test_start, r.n_bars);
        try approx(close[@intCast(sp.test_end - 1)] / close[@intCast(sp.test_start)] - 1.0, r.total_return, 1e-12);
    }
}

test "accounting identities on random targets (cash + position * close == equity, trade pnl sums)" {
    var prng = std.Random.DefaultPrng.init(99);
    const rnd = prng.random();
    const n: usize = 2000;
    const a = testing.allocator;
    const ts = try a.alloc(i64, n);
    defer a.free(ts);
    const open = try a.alloc(f64, n);
    defer a.free(open);
    const high = try a.alloc(f64, n);
    defer a.free(high);
    const low = try a.alloc(f64, n);
    defer a.free(low);
    const close = try a.alloc(f64, n);
    defer a.free(close);
    const target = try a.alloc(f64, n);
    defer a.free(target);
    var p: f64 = 100;
    for (0..n) |i| {
        ts[i] = @intCast(i * 60);
        const o = p * (1 + rnd.floatNorm(f64) * 0.002);
        p *= @exp(rnd.floatNorm(f64) * 0.01);
        open[i] = o;
        close[i] = p;
        high[i] = @max(o, p) * (1 + @abs(rnd.floatNorm(f64)) * 0.003);
        low[i] = @min(o, p) * (1 - @abs(rnd.floatNorm(f64)) * 0.003);
        const choice = rnd.intRangeAtMost(u8, 0, 4);
        target[i] = switch (choice) {
            0 => -1.0,
            1 => 0.0,
            2 => 1.0,
            3 => 2.0,
            else => nan,
        };
    }
    const equity = try a.alloc(f64, n);
    defer a.free(equity);
    const position = try a.alloc(f64, n);
    defer a.free(position);
    const cash = try a.alloc(f64, n);
    defer a.free(cash);
    const pnl = try a.alloc(f64, n);
    defer a.free(pnl);
    const trades = try a.alloc(bt.Trade, n);
    defer a.free(trades);
    const params = bt.Params{ .initial_equity = 10_000, .cost_bps = 5, .slippage_bps = 2, .stop_loss = 0.03, .take_profit = 0.05, .trailing_stop = 0.04 };
    const r = try bt.run(ts, open, high, low, close, target, params, .{ .equity = equity, .position = position, .cash = cash, .pnl = pnl }, trades, a);
    try testing.expect(r.n_trades > 50 and r.n_trades <= n);
    var pnl_sum: f64 = 0;
    for (0..n) |i| {
        try approx(cash[i] + position[i] * close[i], equity[i], 1e-9);
        pnl_sum += pnl[i];
    }
    try approx(r.net_pnl, pnl_sum, 1e-6);
    var trade_sum: f64 = 0;
    for (trades[0..@intCast(r.n_trades)]) |t| trade_sum += t.pnl;
    try approx(r.net_pnl, trade_sum, 1e-6);
    try approx(r.gross_pnl, r.net_pnl + r.total_cost + r.total_slippage, 1e-9);
    try testing.expectEqual(r.n_trades, r.n_long_trades + r.n_short_trades);
    try testing.expect(r.n_stop_exits + r.n_take_profit_exits + r.n_trailing_exits <= r.n_trades);
    try testing.expect(r.exposure > 0.3 and r.exposure <= 1.0);
    // a smaller trade buffer still counts every trade
    var small: [3]bt.Trade = undefined;
    const r2 = try bt.run(ts, open, high, low, close, target, params, .{}, &small, a);
    try testing.expectEqual(r.n_trades, r2.n_trades);
    try approx(r.final_equity, r2.final_equity, 0);
}

test "NaN prices carry equity and defer fills; NaN targets hold" {
    const ts = [_]i64{ 1, 2, 3, 4, 5, 6 };
    const close = [_]f64{ 100, nan, 102, 103, nan, 105 };
    const target = [_]f64{ 1, nan, nan, 0, 0, 0 };
    var equity: [6]f64 = undefined;
    var pos: [6]f64 = undefined;
    var trades: [2]bt.Trade = undefined;
    const r = try bt.run(&ts, null, null, null, &close, &target, .{ .initial_equity = 1000, .fill_mode = 0 }, .{ .equity = &equity, .position = &pos }, &trades, testing.allocator);
    try approx(1000, equity[1], 0); // NaN bar: carried, no fill
    try approx(0, pos[1], 0);
    try approx(1, pos[2], 0); // pending fill executes at bar 2 (close used as open)
    try approx(1000, equity[2], 1e-12);
    try approx(1001, equity[3], 1e-12);
    try approx(1, pos[4], 0); // bar 4 is NaN: the exit is deferred to bar 5
    try approx(1001, equity[4], 1e-12);
    try testing.expectEqual(@as(u64, 1), r.n_trades);
    try approx(105, trades[0].exit_price, 1e-12);
    try approx(1003, r.final_equity, 1e-12);
    try testing.expectError(bt.Error.LengthMismatch, bt.run(&ts, null, null, null, close[0..5], &target, .{}, .{}, null, testing.allocator));
    try testing.expectError(bt.Error.InvalidParameter, bt.run(&ts, null, null, null, &close, &target, .{ .position_mode = 7 }, .{}, null, testing.allocator));
    const empty = try bt.run(&[_]i64{}, null, null, null, &[_]f64{}, &[_]f64{}, .{}, .{}, null, testing.allocator);
    try testing.expectEqual(@as(u64, 0), empty.n_bars);
}
