#!/usr/bin/env python3
"""Round 4: trading calendars, signal backtester and universe features.

Run from the repository root:
    PYTHONPATH=$(pwd)/bindings/python python3 bindings/python/test/test_round4.py
"""
import math
import os
import shutil
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "..", "scripts", "stress"))

from hocdb_python import (HOCDB, HOCDBField, FieldTypes, backtest_arrays, backtest_params_default, backtest_splits,
                          calendar_define, calendar_id, calendar_is_open, calendar_name, calendar_open_seconds,
                          calendar_periods_per_year, calendar_session, calendar_session_for_day,
                          calendar_sessions_between, calendar_to_local, civil_from_days, days_from_civil, universe,
                          universe_arrays, universe_params_default, walk_forward_splits)

try:  # the independent reference used by the stress harness (needs numpy)
    import references_backtest as RB
except ImportError:  # pragma: no cover - the hard-coded tied-example numbers are checked either way
    RB = None

DATA_DIR = "b_python_test_round4"
US = 1_000_000
failures = 0


def check(cond, msg):
    global failures
    if cond:
        print(f"  ok: {msg}")
    else:
        print(f"  FAIL: {msg}")
        failures += 1


def near(a, b, tol=1e-9):
    a, b = float(a), float(b)
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return abs(a - b) <= tol + 1e-9 * abs(b)


def expect_error(fn, msg, needle=None):
    global failures
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        if needle is not None and needle not in str(e):
            print(f"  FAIL: {msg} -> wrong message: {e}")
            failures += 1
        else:
            print(f"  ok: {msg} -> {type(e).__name__}: {e}")
        return
    print(f"  FAIL: {msg} (no error raised)")
    failures += 1


def utc(y, m, d, hh=0, mm=0):
    return days_from_civil(y, m, d) * 86400 + hh * 3600 + mm * 60


def test_calendars():
    print("Trading calendars...")
    nyse, lse = calendar_id("nyse"), calendar_id("lse")
    check(nyse == 3 and lse == 5 and calendar_id("CRYPTO") == 1 and calendar_id("nope") == 0, "calendar ids by name")
    check(calendar_name(5) == "lse" and calendar_name(99) is None, "calendar names")
    fri = utc(2025, 9, 5, 15, 0)
    s = calendar_session(nyse, fri)
    check(s is not None and s["open"] == utc(2025, 9, 5, 13, 30) and s["close"] == utc(2025, 9, 5, 20, 0)
          and not s["early_close"] and s["trade_day"] == days_from_civil(2025, 9, 5), "NYSE Friday session 13:30-20:00 UTC")
    check(calendar_is_open(nyse, fri) and not calendar_is_open(nyse, utc(2025, 9, 6, 12, 0)), "is_open")
    sat = utc(2025, 9, 6, 12, 0)
    check(calendar_session(nyse, sat) is None, "Saturday has no session")
    check(calendar_session(nyse, sat, 1)["trade_day"] == days_from_civil(2025, 9, 5), "previous session is Friday")
    check(calendar_session(nyse, sat, 2)["trade_day"] == days_from_civil(2025, 9, 8), "next session is Monday")
    check(calendar_session_for_day(nyse, days_from_civil(2025, 7, 4)) is None, "Independence Day is closed")
    bf = calendar_session_for_day(nyse, days_from_civil(2025, 11, 28))
    check(bf["early_close"] and bf["close"] == utc(2025, 11, 28, 18, 0), "Black Friday early close 13:00 ET")
    check(calendar_open_seconds(nyse, utc(2025, 8, 29, 15, 0), utc(2025, 9, 2, 15, 0)) == 5 * 3600 + 5400,
          "trading seconds across the Labor Day weekend")
    check(calendar_sessions_between(nyse, utc(2025, 1, 1), utc(2026, 1, 1)) == 250, "250 NYSE sessions in 2025")
    check(near(calendar_periods_per_year(nyse, 60), 252 * 390) and near(calendar_periods_per_year(1, 86400), 365),
          "periods per year: NYSE 1-minute and crypto daily")
    check(calendar_to_local(nyse, fri) == utc(2025, 9, 5, 11, 0), "UTC -> New York local (EDT)")
    check(civil_from_days(days_from_civil(2024, 2, 29)) == (2024, 2, 29), "civil date round trip")
    fxm = calendar_session_for_day(calendar_id("fx"), days_from_civil(2025, 9, 8))
    check(fxm["open"] == utc(2025, 9, 7, 21, 0), "FX Monday opens Sunday 17:00 New York")
    expect_error(lambda: calendar_session(999, fri), "unknown calendar id", "UnknownCalendar")

    weekly = [{"open_sec": 10 * 3600, "close_sec": 15 * 3600}] * 4 + [None] * 3
    cid = calendar_define("py_custom", weekly, 9 * 3600, "none", [days_from_civil(2025, 9, 9)],
                          [{"day": days_from_civil(2025, 9, 10), "close_sec": 12 * 3600}], 200)
    check(cid >= 32 and calendar_id("py_custom") == cid, "custom calendar registered")
    check(calendar_session_for_day(cid, days_from_civil(2025, 9, 8))["open"] == utc(2025, 9, 8, 1, 0), "custom Monday 10:00 UTC+9")
    check(calendar_session_for_day(cid, days_from_civil(2025, 9, 9)) is None, "custom holiday")
    check(calendar_session_for_day(cid, days_from_civil(2025, 9, 10))["close"] == utc(2025, 9, 10, 3, 0), "custom early close")
    check(calendar_session_for_day(cid, days_from_civil(2025, 9, 12)) is None, "custom week has no Friday")


BAR_SCHEMA = [HOCDBField("timestamp", FieldTypes.I64), HOCDBField("open", FieldTypes.F64), HOCDBField("high", FieldTypes.F64),
              HOCDBField("low", FieldTypes.F64), HOCDBField("close", FieldTypes.F64), HOCDBField("volume", FieldTypes.F64)]
COLS = {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"}


def fill_sessions(db, days, seed=1.0):
    """One-minute bars for every minute of the given NYSE trade dates (µs timestamps)."""
    nyse = calendar_id("nyse")
    p = 100.0 * seed
    first_open = {}
    per_day = {}
    for day in days:
        s = calendar_session_for_day(nyse, day)
        t = s["open"]
        hi, lo, last = -1e18, 1e18, 0.0
        while t < s["close"]:
            o = p
            p *= 1.0 + 0.0007 * math.sin(t / 613.0) + 0.0003 * math.cos(t / 97.0)
            bar_hi, bar_lo = max(o, p) * 1.0005, min(o, p) * 0.9995
            db.append(t * US, o, bar_hi, bar_lo, p, 500.0 + (t % 97))
            if t == s["open"]:
                first_open[day] = o
            hi, lo, last = max(hi, bar_hi), min(lo, bar_lo), p
            t += 60
        per_day[day] = (hi, lo, last)
    db.flush()
    return first_open, per_day


def test_calendar_database():
    print("Database with a trading calendar...")
    nyse = calendar_id("nyse")
    expect_error(lambda: HOCDB("BAD", DATA_DIR, BAR_SCHEMA, calendar=999), "unknown calendar refused at open", "UnknownCalendar")
    expect_error(lambda: HOCDB("BAD", DATA_DIR, BAR_SCHEMA, calendar="not_a_calendar"), "unknown calendar name refused at open")
    db = HOCDB("CAL", DATA_DIR, BAR_SCHEMA, calendar="nyse", timestamp_unit_ns=1000)
    check(db.get_calendar() == nyse and db.get_timestamp_unit() == 1000, "handle reports the calendar and unit")
    check(near(db.periods_per_year(60 * US), 252 * 390) and near(db.periods_per_year(86400 * US), 252), "handle periods_per_year")
    thu, fri, tue = days_from_civil(2025, 9, 4), days_from_civil(2025, 9, 5), days_from_civil(2025, 9, 9)
    first_open, per_day = fill_sessions(db, [thu, fri, tue])
    check(len(db.load()) == 3 * 390, "3 sessions x 390 one-minute bars")

    s_fri = calendar_session_for_day(nyse, fri)
    res = db.indicators([{"kind": "session_range", "param": 0}, {"kind": "pivots", "param": 0},
                         {"kind": "session_vwap", "param": 0}, {"kind": "opening_range", "period": 5, "param": 0}],
                        start_ts=(s_fri["open"] + 100 * 60) * US, end_ts=s_fri["close"] * US,
                        columns=COLS, lookback=0, bucket=0)
    c = res["columns"]
    check(len(res["timestamps"]) == 290, f"window has 290 rows, got {len(res['timestamps'])}")
    check(all(near(v, first_open[fri], 1e-12) for v in c["session_range_open"]),
          "session_range open == Friday's first bar even though the window starts later")
    ph, pl, pc = per_day[thu]
    check(all(near(v, (ph + pl + pc) / 3.0) for v in c["pivots_pp"]), "pivots come from Thursday (previous trading day)")
    check(all(math.isfinite(v) and v > 0 for v in c["session_vwap"]), "session vwap defined over the window")
    check(all(math.isfinite(v) for v in c["opening_range_5_breakout"]), "opening range formed before the window")

    s_tue = calendar_session_for_day(nyse, tue)
    res2 = db.indicators([{"kind": "pivots", "param": 0}], start_ts=s_tue["open"] * US, end_ts=s_tue["close"] * US,
                         columns=COLS, lookback=0, bucket=0)
    fh, fl, fc = per_day[fri]
    check(near(res2["columns"]["pivots_pp"][0], (fh + fl + fc) / 3.0),
          "Tuesday's pivots use Friday, skipping the missing Monday")

    h = db.health(0, 2**63 - 1, price="close", volume="volume", gap_threshold=5 * 60 * US, outlier_threshold=0.2)
    check(h["n_session_breaks"] == 2 and h["n_missing_sessions"] == 1, "health: 2 session breaks, 1 missing session")
    check(h["n_gaps"] == 1 and h["max_gap"] == (60 + 390 * 60) * US, "health: the missing session is the only real gap")
    check(h["closed_span"] > 0 and len(h) == 19, "health: closed_span and 19 fields")

    auto = db.summary(0, 2**63 - 1, "close", periods_per_year=0)
    explicit = db.summary(0, 2**63 - 1, "close", periods_per_year=252 * 390)
    check(near(auto["ann_vol"], explicit["ann_vol"], 1e-12) and auto["ann_vol"] > 0, "summary annualises from the calendar")
    sa = db.snapshot(columns=COLS, bars=300, bucket=60 * US, periods_per_year=0)
    se = db.snapshot(columns=COLS, bars=300, bucket=60 * US, periods_per_year=252 * 390)
    check(near(sa["hist_vol_20"], se["hist_vol_20"], 1e-12), "snapshot annualises from the calendar")

    plain = HOCDB("PLAIN", DATA_DIR, BAR_SCHEMA)
    fill_sessions(plain, [thu], seed=1.1)
    expect_error(lambda: plain.indicators([{"kind": "session_vwap", "param": 0}], tail=10, columns=COLS, lookback=0, bucket=0),
                 "session kind with param 0 and no calendar", "CalendarRequired")
    expect_error(lambda: plain.set_calendar(999), "set_calendar with an unknown id", "UnknownCalendar")
    plain.set_calendar("crypto")
    plain.set_timestamp_unit(1000)
    check(plain.get_calendar() == 1 and near(plain.periods_per_year(60 * US), 365 * 1440), "set_calendar / set_timestamp_unit")
    plain.close()
    db.close()

    reopened = HOCDB("CAL", DATA_DIR, BAR_SCHEMA)
    check(reopened.get_calendar() == nyse and reopened.get_timestamp_unit() == 1000, "calendar and unit persisted in the header")
    reader = HOCDB.open_reader("CAL", DATA_DIR, BAR_SCHEMA)
    check(reader.get_calendar() == nyse and near(reader.periods_per_year(60 * US), 252 * 390), "a reader sees the writer's calendar")
    reader.close()
    reopened.close()
    plain2 = HOCDB("PLAIN", DATA_DIR, BAR_SCHEMA)
    check(plain2.get_calendar() == 1 and plain2.get_timestamp_unit() == 1000, "set_calendar persisted for a writer")
    plain2.close()


def test_backtest():
    print("Signal backtester...")
    d = backtest_params_default()
    check(d["initial_equity"] == 1.0 and d["allow_short"] == 1 and d["fill_mode"] == 0, "default params")
    ts = list(range(1, 13))
    o = [100.0, 101.0, 102.0, 98.0, 95.0, 97.0, 99.0, 103.0, 104.0, 102.0, 100.0, 99.0]
    hi = [101.0, 103.0, 103.0, 99.0, 97.0, 99.0, 104.0, 105.0, 105.0, 103.0, 101.0, 100.0]
    lo = [99.0, 100.0, 96.0, 93.0, 94.0, 96.0, 98.0, 102.0, 101.0, 99.0, 98.0, 97.0]
    c = [100.5, 102.5, 97.0, 94.0, 96.5, 98.5, 103.5, 104.5, 102.0, 100.0, 98.5, 99.5]
    tgt = [1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, -1.0, -1.0, -1.0, -1.0]
    p = dict(initial_equity=1000.0, cost_bps=10.0, slippage_bps=5.0, stop_loss=0.05, periods_per_year=252.0)
    got = backtest_arrays(ts, o, hi, lo, c, tgt, params=p, outputs=["equity", "position", "cash", "pnl", "drawdown"], max_trades=8)
    r = got["result"]
    eq = [1000.0, 1001.3484495, 995.8484495, 994.7024755365, 994.7024755365, 994.7024755365,
          1003.4053765365, 1005.4053765365, 1000.4053765365, 1001.9465295365, 1003.4465295365, 1002.4465295365]
    trades = None
    if RB is not None:
        want, ref_eq, trades = RB.backtest_reference(ts, o, hi, lo, c, tgt, p)
        bad = [k for k in RB.RESULT_FIELDS if not near(r[k], want[k], 1e-7)]
        check(not bad, f"tied example: all 32 result fields match the independent reference {bad[:4]}")
        check(all(near(a, b) for a, b in zip(ref_eq, eq)), "the reference reproduces the hard-coded equity curve")
    check(r["n_trades"] == 3 and r["n_long_trades"] == 2 and r["n_short_trades"] == 1 and r["n_stop_exits"] == 1, "tied example trade counts")
    check(near(r["final_equity"], 1002.4465295364876) and near(r["max_drawdown"], 0.006637024271452185)
          and r["max_drawdown_bars"] == 4 and near(r["sharpe"], 0.961805823133203), "tied example statistics")
    check(all(near(a, b) for a, b in zip(got["equity"], eq)) and near(got["equity"][1], 1001.3484495), "equity curve")
    check(got["position"][6] == 2.0 and got["position"][9] == -1.0 and len(got["drawdown"]) == 12, "per-bar outputs")
    t0 = got["trades"][0]
    check(len(got["trades"]) == 3 and t0["entry_ts"] == 2 and t0["exit_ts"] == 4 and t0["exit_reason"] == 1
          and near(t0["exit_price"], 95.9499760125), "first trade stopped out")
    check(got["trades"][2]["exit_ts"] == 0 and got["trades"][2]["exit_reason"] == 4 and got["trades"][2]["direction"] == -1,
          "last trade is still open at the end")
    if trades is not None:
        check(all(near(got["trades"][i][k], trades[i][k], 1e-7) for i in range(3) for k in RB.TRADE_FIELDS), "trades match the reference")
    expect_error(lambda: backtest_arrays(ts, o, hi, lo, c, tgt, params={"position_mode": 9}), "invalid position_mode")

    sp = walk_forward_splits(100, 4, 0.5, True)
    check(len(sp) == 4 and sp[0] == {"train_start": 0, "train_end": 50, "test_start": 50, "test_end": 62}
          and sp[3]["test_end"] == 100 and sp[3]["train_start"] == 0, "anchored walk-forward splits")
    rolling = walk_forward_splits(100, 4, 0.5, False)
    check(rolling[3]["train_start"] == 36 and rolling[3]["train_end"] == 86, "rolling walk-forward splits")
    ramp_ts = list(range(1, 101))
    ramp = [100 + 0.25 * i for i in range(100)]
    ones = [1.0] * 100
    results = backtest_splits(ramp_ts, None, None, None, ramp, ones, sp, params={"fill_mode": 1, "position_mode": 1})
    check(len(results) == 4 and all(near(r["total_return"], ramp[s["test_end"] - 1] / ramp[s["test_start"]] - 1.0)
                                    for r, s in zip(results, sp)), "backtest_splits runs every test window independently")

    # database windows: the backtest rows are the bars of the window
    db = HOCDB("BT", DATA_DIR, BAR_SCHEMA, calendar="crypto", timestamp_unit_ns=1_000_000_000)
    price = 100.0
    for i in range(6000):
        price *= 1.0 + 0.0006 * math.sin(i * 0.37) + 0.0002 * math.cos(i * 0.11)
        t = 1_700_000_000 + i * 7
        db.append(t, price, price * 1.001, price * 0.999, price, 10.0 + (i % 5))
    db.flush()
    start, end = 1_700_000_100, 1_700_000_100 + 6 * 3600
    bars = db.ohlcv(start, end, 300, price="close", volume="volume")
    n = len(bars["timestamps"])
    check(n == 72, f"6 hours of 5-minute bars, got {n}")
    target = [1.0 if i >= 5 and bars["close"][i] > bars["close"][i - 5] else (-1.0 if i >= 5 else 0.0) for i in range(n)]
    params = {"initial_equity": 10_000.0, "cost_bps": 5.0, "position_mode": 1}
    dbres = db.backtest(target, start, end, 300, params=params, columns=COLS, outputs=["equity"], max_trades=n)
    kernel = backtest_arrays(bars["timestamps"], bars["open"], bars["high"], bars["low"], bars["close"], target,
                             params=dict(params, periods_per_year=365 * 288), max_trades=n)
    check(dbres["result"]["n_bars"] == n and near(dbres["result"]["final_equity"], kernel["result"]["final_equity"], 0)
          and near(dbres["result"]["sharpe"], kernel["result"]["sharpe"], 0),
          "db backtest == kernel on ohlcv bars with the calendar's periods_per_year")
    check(len(dbres["equity"]) == n and dbres["result"]["ann_vol"] > 0, "db backtest outputs")
    tail = db.backtest_tail(target[-20:], 300, params=params, columns=COLS)
    check(tail["result"]["n_bars"] == 20, "backtest_tail window")
    expect_error(lambda: db.backtest(target[:-1], start, end, 300, params=params, columns=COLS), "target length mismatch")
    db.close()


def test_universe():
    print("Universe features...")
    d = universe_params_default()
    check(d["mom_short"] == 5 and d["mom_mid"] == 20 and d["corr_period"] == 60, "default params")
    n = 120
    c0 = [100 + 5 * math.sin(i * 0.2) + 0.1 * i for i in range(n)]
    c1 = [50 + 3 * math.cos(i * 0.15) - 0.05 * i for i in range(n)]
    ts = [1000 + i * 60 for i in range(n)]
    v0 = [1000.0 + (i % 7) * 10 for i in range(n)]
    params = {"mom_long": 30, "corr_period": 30, "beta_period": 30, "sma_period": 20}
    u = universe_arrays([c0, c1, c0], [v0, [2000.0] * n, [500.0] * n], ts=ts, params=params, corr=True)
    check(u["summary"]["n_tickers"] == 3 and u["summary"]["n_bars"] == n and u["summary"]["first_ts"] == 1000
          and u["summary"]["last_ts"] == ts[-1], "summary basics")
    corr = u["corr"]
    check(near(corr[0][2], 1.0) and near(corr[2][0], 1.0) and corr[0][0] == 1.0 and near(corr[0][1], corr[1][0]),
          "correlation matrix: identical tickers, symmetry, unit diagonal")
    check(u["rows"][0]["max_corr_index"] == 2 and u["rows"][2]["max_corr_index"] == 0, "most correlated partner")
    check(u["rows"][0]["rank_mom_mid"] == u["rows"][2]["rank_mom_mid"] and math.isfinite(u["rows"][1]["beta"]),
          "ranks tie for identical tickers; beta defined")
    check(len(u["rows"][0]) == 21 and len(u["summary"]) == 16, "21 row fields, 16 summary fields")
    no_vol = universe_arrays([c0, c1, c0], params=params, corr=False)
    check(math.isnan(no_vol["rows"][0]["volume_ratio"]) and no_vol["summary"]["first_ts"] == 0 and no_vol.get("corr") is None,
          "without volumes / timestamps / correlation matrix")

    dbs = []
    for k, series in enumerate((c0, c1, c0)):
        db = HOCDB(f"U{k}", DATA_DIR, BAR_SCHEMA)
        for i in range(n):
            if k == 2 and i % 7 == 6:
                continue  # this ticker is missing every 7th bar
            db.append(ts[i], series[i], series[i], series[i], series[i], v0[i])
        db.flush()
        dbs.append(db)
    got = universe(dbs, COLS, n_bars=n, bucket=0, params=params, corr=True)
    joined = [i for i in range(n) if i % 7 != 6]
    check(got["summary"]["n_bars"] == len(joined) and got["summary"]["last_ts"] == ts[joined[-1]],
          f"inner join over 3 databases: {got['summary']['n_bars']} bars")
    hand = universe_arrays([[c0[i] for i in joined], [c1[i] for i in joined], [c0[i] for i in joined]],
                           [[v0[i] for i in joined]] * 3, ts=[ts[i] for i in joined], params=params, corr=True)
    same = all(near(got["rows"][i][k], hand["rows"][i][k]) for i in range(3) for k in got["rows"][0])
    check(same, "db join == hand-joined arrays (all row fields)")
    check(all(near(got["summary"][k], hand["summary"][k]) for k in got["summary"]), "summary matches the hand join")
    auto = universe(dbs, COLS, n_bars=0, bucket=0, params=params, corr=False)
    check(auto["summary"]["n_bars"] >= 31, "n_bars 0 reads enough bars for the longest period")
    expect_error(lambda: universe(dbs, {}, n_bars=10), "universe without a close column")
    for db in dbs:
        db.close()


def main():
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        test_calendars()
        test_calendar_database()
        test_backtest()
        test_universe()
    finally:
        shutil.rmtree(DATA_DIR, ignore_errors=True)
    if failures:
        raise SystemExit(f"Python round-4 test FAILED ({failures} checks)")
    print("Python Round 4 Test Passed!")


if __name__ == "__main__":
    main()
