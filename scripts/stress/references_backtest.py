#!/usr/bin/env python3
"""Independent reference implementation of HOCDB's signal backtester
(src/backtest.zig) used by the stress harness to cross-check the Zig kernel.

Semantics (identical to the Zig module documentation):
  * target[i] is the desired position at the END of bar i (units, fraction of
    current equity, or notional); NaN = hold the previous signal; allow_short
    = 0 clamps negative targets to 0.
  * A fill happens whenever the effective target differs from the value the
    current position was sized from ("applied"); fill_mode 0 executes it at
    the next bar's open, 1 at the same bar's close. Fills are slipped
    adversely (slippage_bps) and pay cost_bps on |traded notional|. Units in
    modes 1/2 are computed at the unslipped fill price from the equity marked
    at that price; |units| is capped by max_position when > 0.
  * Stops are checked on every valid bar while a position is open, in the
    order stop_loss, trailing_stop, take_profit, against high / low (close
    when missing); the exit fills at the level or at the open on a gap
    through it; a stopped position is not re-entered on the same signal.
  * equity[i] = cash + position * close[i] after the bar's fills; bars with a
    NaN price are skipped (equity carried, r = 0).
  * Trades open on 0 -> non-zero or a sign flip; entry_price / exit_price are
    units-weighted averages of the slipped fills; pnl is the net cash flow.
"""
import math
import sys

import numpy as np

DEFAULT_PARAMS = dict(initial_equity=1.0, cost_bps=0.0, slippage_bps=0.0, stop_loss=0.0, take_profit=0.0,
                      trailing_stop=0.0, max_position=0.0, position_mode=0, fill_mode=0, periods_per_year=0.0,
                      allow_short=1, risk_free_rate=0.0)

RESULT_FIELDS = ["n_bars", "n_trades", "n_long_trades", "n_short_trades", "final_equity", "total_return", "ann_return",
                 "ann_vol", "sharpe", "sortino", "calmar", "max_drawdown", "max_drawdown_bars", "avg_drawdown", "win_rate",
                 "profit_factor", "avg_trade_return", "avg_win", "avg_loss", "best_trade", "worst_trade", "avg_holding_bars",
                 "exposure", "long_share", "turnover", "total_cost", "total_slippage", "n_stop_exits", "n_take_profit_exits",
                 "n_trailing_exits", "gross_pnl", "net_pnl"]
TRADE_FIELDS = ["entry_ts", "exit_ts", "direction", "entry_price", "exit_price", "size", "pnl", "ret", "bars", "exit_reason"]


def _finite(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


class _Book:
    """Cash / position accounting and trade bookkeeping."""

    def __init__(self, ts, p):
        self.ts = ts
        self.cost = p["cost_bps"] / 10_000.0
        self.slip = p["slippage_bps"] / 10_000.0
        self.p = p
        self.cash = p["initial_equity"] if p["initial_equity"] > 0 else 1.0
        self.pos = 0.0
        self.sig = 0.0
        self.applied = 0.0
        self.trade = None
        self.trades = []
        self.total_cost = 0.0
        self.total_slippage = 0.0
        self.traded = 0.0
        self.n_sl = self.n_tp = self.n_tr = 0

    def fill_signal(self, i, price):
        mode = self.p["position_mode"]
        if mode == 1:
            units = self.sig * (self.cash + self.pos * price) / price
        elif mode == 2:
            units = self.sig / price
        else:
            units = self.sig
        cap = self.p["max_position"]
        if cap > 0 and abs(units) > cap:
            units = cap if units > 0 else -cap
        self.fill(i, price, units, 0)
        self.applied = self.sig

    def fill(self, i, price, new_units, reason):
        delta = new_units - self.pos
        if delta == 0:
            return
        buying = delta > 0
        f = price * (1.0 + self.slip) if buying else price * (1.0 - self.slip)
        qty = abs(delta)
        notional = qty * f
        cost = notional * self.cost
        self.cash -= delta * f + cost
        self.total_cost += cost
        self.total_slippage += qty * price * self.slip
        self.traded += notional
        old = self.pos
        self.pos = new_units
        if old == 0:
            self.open_trade(i, new_units, f)
            return
        if (old > 0) == buying:  # increase
            t = self.trade
            t["entry_units"] += qty
            t["entry_notional"] += notional
            t["flow"] -= delta * f + cost
            return
        close_qty = min(qty, abs(old))
        close_notional = close_qty * f
        close_cost = close_notional * self.cost
        t = self.trade
        t["exit_units"] += close_qty
        t["exit_notional"] += close_notional
        t["flow"] += (close_notional if old > 0 else -close_notional) - close_cost
        if (old > 0 and new_units > 0) or (old < 0 and new_units < 0):
            return
        self.finish_trade(i, self.ts[i], reason)
        if new_units != 0:
            self.open_trade(i, new_units, f)

    def open_trade(self, i, units, f):
        q = abs(units)
        self.trade = dict(entry_ts=self.ts[i], entry_bar=i, direction=1 if units > 0 else -1, entry_units=q,
                          entry_notional=q * f, exit_units=0.0, exit_notional=0.0,
                          flow=-(units * f) - q * f * self.cost, best=f)

    def finish_trade(self, i, exit_ts, reason):
        t = self.trade
        pnl = t["flow"]
        rec = dict(entry_ts=t["entry_ts"], exit_ts=exit_ts, direction=t["direction"],
                   entry_price=t["entry_notional"] / t["entry_units"], exit_price=t["exit_notional"] / t["exit_units"],
                   size=t["entry_units"], pnl=pnl, ret=pnl / t["entry_notional"], bars=i - t["entry_bar"], exit_reason=reason)
        self.trades.append(rec)
        self.trade = None
        if reason == 1:
            self.n_sl += 1
        elif reason == 2:
            self.n_tp += 1
        elif reason == 3:
            self.n_tr += 1

    def check_stops(self, i, o, h, l):
        long = self.pos > 0
        t = self.trade
        entry = t["entry_notional"] / t["entry_units"]
        sl, tr, tp = self.p["stop_loss"], self.p["trailing_stop"], self.p["take_profit"]
        if sl > 0:
            lvl = entry * (1 - sl) if long else entry * (1 + sl)
            if (l <= lvl) if long else (h >= lvl):
                self.fill(i, self._adverse(long, o, lvl), 0.0, 1)
                return
        if tr > 0:
            lvl = t["best"] * (1 - tr) if long else t["best"] * (1 + tr)
            if (l <= lvl) if long else (h >= lvl):
                self.fill(i, self._adverse(long, o, lvl), 0.0, 3)
                return
        if tp > 0:
            lvl = entry * (1 + tp) if long else entry * (1 - tp)
            if (h >= lvl) if long else (l <= lvl):
                self.fill(i, self._favourable(long, o, lvl), 0.0, 2)
                return
        t["best"] = max(t["best"], h) if long else min(t["best"], l)

    @staticmethod
    def _adverse(long, o, lvl):
        return (o if o <= lvl else lvl) if long else (o if o >= lvl else lvl)

    @staticmethod
    def _favourable(long, o, lvl):
        return (o if o >= lvl else lvl) if long else (o if o <= lvl else lvl)

    def close_at_end(self, i, last_close):
        if self.trade is None:
            return
        q = abs(self.pos)
        t = self.trade
        t["exit_units"] += q
        t["exit_notional"] += q * last_close
        t["flow"] += self.pos * last_close
        self.finish_trade(i, 0, 4)


def backtest_reference(ts, open_, high, low, close, target, params=None):
    p = dict(DEFAULT_PARAMS)
    if params:
        p.update(params)
    n = len(close)
    ts = [int(x) for x in ts]
    close = [float(x) for x in close]
    open_ = [float(x) for x in open_] if open_ is not None else None
    high = [float(x) for x in high] if high is not None else None
    low = [float(x) for x in low] if low is not None else None
    target = [float(x) for x in target]
    e0 = p["initial_equity"] if p["initial_equity"] > 0 else 1.0
    book = _Book(ts, p)
    next_open = p["fill_mode"] == 0
    no_short = p["allow_short"] == 0
    equity = np.empty(n)
    peak, under, longest, mdd, sum_dd = e0, 0, 0, 0.0, 0.0
    prev_eq, last_close = e0, float("nan")
    exposed = long_bars = 0
    for i in range(n):
        c = close[i]
        o = open_[i] if open_ is not None else c
        h = high[i] if high is not None else c
        l = low[i] if low is not None else c
        valid = not (math.isnan(c) or math.isnan(o) or math.isnan(h) or math.isnan(l))
        if valid:
            if next_open and book.sig != book.applied:
                book.fill_signal(i, o)
            if book.pos != 0:
                book.check_stops(i, o, h, l)
        t = target[i]
        if not math.isnan(t):
            book.sig = 0.0 if (no_short and t < 0) else t
        if valid and not next_open and book.sig != book.applied:
            book.fill_signal(i, c)
        eq = prev_eq
        if valid:
            eq = book.cash + book.pos * c
            last_close = c
        if eq >= peak:
            peak, under = eq, 0
        else:
            under += 1
            longest = max(longest, under)
        dd = 1.0 - eq / peak
        mdd = max(mdd, dd)
        sum_dd += dd
        equity[i] = eq
        if book.pos != 0:
            exposed += 1
            if book.pos > 0:
                long_bars += 1
        prev_eq = eq
    if n == 0:
        res = {k: 0 for k in RESULT_FIELDS}
        res["final_equity"] = e0
        return res, equity, []
    book.close_at_end(n - 1, last_close)
    trades = book.trades
    closed = [t for t in trades if t["exit_reason"] != 4]
    final = equity[-1]
    r = np.empty(n)
    r[0] = equity[0] / e0 - 1.0
    r[1:] = equity[1:] / equity[:-1] - 1.0
    ppy = p["periods_per_year"]
    scale = math.sqrt(ppy) if ppy > 0 else 1.0
    rf = p["risk_free_rate"] / ppy if ppy > 0 else 0.0
    mean_r = float(np.mean(r))
    std_r = float(np.sqrt(np.mean((r - mean_r) ** 2)))
    dd_dev = float(np.sqrt(np.mean(np.minimum(r, 0.0) ** 2)))
    res = dict(n_bars=n, n_trades=len(trades), n_long_trades=sum(1 for t in trades if t["direction"] > 0),
               n_short_trades=sum(1 for t in trades if t["direction"] < 0), final_equity=final, total_return=final / e0 - 1.0)
    if ppy > 0:
        # a negative final equity has no real geometric return (NaN, as in the Zig kernel)
        growth = final / e0
        res["ann_return"] = growth ** (ppy / n) - 1.0 if growth > 0 else float("nan")
    else:
        res["ann_return"] = res["total_return"]
    res["ann_vol"] = std_r * scale
    res["sharpe"] = (mean_r - rf) / std_r * scale if std_r > 0 else float("nan")
    res["sortino"] = (mean_r - rf) / dd_dev * scale if dd_dev > 0 else float("nan")
    res["calmar"] = res["ann_return"] / mdd if mdd > 0 else float("nan")
    res["max_drawdown"] = mdd
    res["max_drawdown_bars"] = longest
    res["avg_drawdown"] = sum_dd / n
    if closed:
        wins = [t["pnl"] for t in closed if t["pnl"] > 0]
        losses = [t["pnl"] for t in closed if t["pnl"] < 0]
        gains, loss_sum = sum(wins), sum(losses)
        res["win_rate"] = len(wins) / len(closed)
        res["profit_factor"] = gains / -loss_sum if loss_sum < 0 else (float("inf") if gains > 0 else float("nan"))
        res["avg_trade_return"] = sum(t["ret"] for t in closed) / len(closed)
        res["avg_win"] = gains / len(wins) if wins else 0.0
        res["avg_loss"] = loss_sum / len(losses) if losses else 0.0
        res["best_trade"] = max(t["pnl"] for t in closed)
        res["worst_trade"] = min(t["pnl"] for t in closed)
        res["avg_holding_bars"] = sum(t["bars"] for t in closed) / len(closed)
    else:
        for k in ("win_rate", "profit_factor", "avg_trade_return", "avg_win", "avg_loss", "best_trade", "worst_trade", "avg_holding_bars"):
            res[k] = float("nan")
    res["exposure"] = exposed / n
    res["long_share"] = long_bars / exposed if exposed > 0 else float("nan")
    res["turnover"] = book.traded / float(np.mean(equity))
    res["total_cost"] = book.total_cost
    res["total_slippage"] = book.total_slippage
    res["n_stop_exits"] = book.n_sl
    res["n_take_profit_exits"] = book.n_tp
    res["n_trailing_exits"] = book.n_tr
    res["net_pnl"] = final - e0
    res["gross_pnl"] = res["net_pnl"] + book.total_cost + book.total_slippage
    return res, equity, trades


def walk_forward_splits(n, n_splits, train_frac, anchored):
    if n == 0 or n_splits == 0 or not (0 < train_frac < 1):
        return []
    train_len = int(math.floor(train_frac * n))
    if train_len == 0:
        return []
    test_len = (n - train_len) // n_splits
    if test_len == 0:
        return []
    out = []
    for k in range(n_splits):
        ts_ = train_len + k * test_len
        te = n if k == n_splits - 1 else ts_ + test_len
        out.append((0 if anchored else ts_ - train_len, ts_, ts_, te))
    return out


# The tied example: 12 bars, long -> stop -> re-entry on a new signal -> flip short -> open at the end.
TIED_TS = list(range(1, 13))
TIED_OPEN = [100.0, 101.0, 102.0, 98.0, 95.0, 97.0, 99.0, 103.0, 104.0, 102.0, 100.0, 99.0]
TIED_HIGH = [101.0, 103.0, 103.0, 99.0, 97.0, 99.0, 104.0, 105.0, 105.0, 103.0, 101.0, 100.0]
TIED_LOW = [99.0, 100.0, 96.0, 93.0, 94.0, 96.0, 98.0, 102.0, 101.0, 99.0, 98.0, 97.0]
TIED_CLOSE = [100.5, 102.5, 97.0, 94.0, 96.5, 98.5, 103.5, 104.5, 102.0, 100.0, 98.5, 99.5]
TIED_TARGET = [1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, -1.0, -1.0, -1.0, -1.0]
TIED_PARAMS = dict(initial_equity=1000.0, cost_bps=10.0, slippage_bps=5.0, stop_loss=0.05, take_profit=0.0,
                   trailing_stop=0.0, position_mode=0, fill_mode=0, periods_per_year=252.0)


def _main():
    res, eq, trades = backtest_reference(TIED_TS, TIED_OPEN, TIED_HIGH, TIED_LOW, TIED_CLOSE, TIED_TARGET, TIED_PARAMS)
    print("tied example result:")
    for k in RESULT_FIELDS:
        print(f"  {k} = {res[k]!r}")
    print("tied example equity:", [round(x, 10) for x in eq])
    print("tied example trades:")
    for t in trades:
        print("  ", {k: t[k] for k in TRADE_FIELDS})
    # sanity: accounting identity on random data
    rng = np.random.default_rng(3)
    n = 500
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    o = c * (1 + rng.normal(0, 0.002, n))
    h = np.maximum(o, c) * (1 + abs(rng.normal(0, 0.003, n)))
    l = np.minimum(o, c) * (1 - abs(rng.normal(0, 0.003, n)))
    tgt = rng.choice([-1.0, 0.0, 1.0, 2.0, float("nan")], size=n)
    r2, e2, tr2 = backtest_reference(np.arange(n), o, h, l, c, tgt, dict(cost_bps=5, slippage_bps=2, stop_loss=0.03, take_profit=0.05, trailing_stop=0.04))
    pnl_sum = sum(t["pnl"] for t in tr2)
    assert abs(pnl_sum - r2["net_pnl"]) < 1e-6 * max(1.0, abs(r2["net_pnl"])), (pnl_sum, r2["net_pnl"])
    print("random accounting identity ok: trades", len(tr2), "net_pnl", r2["net_pnl"])
    print("RESULT: PASS")


if __name__ == "__main__":
    _main()
