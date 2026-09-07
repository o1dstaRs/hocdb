"""
Python binding test for the indicator / analytics API (mirrors bindings/c/test/test_indicators.c).
Run from the repo root:
    PYTHONPATH=$(pwd)/bindings/python python3 bindings/python/test/test_indicators.py
"""
import ctypes
import math
import os
import shutil

from hocdb_python import (HOCDB, HOCDBField, FieldTypes, IndicatorKinds, HOCDBIndicatorSpec, HOCDBIndicatorColumns,
                          HOCDBIndicatorResult, HOCDBBars, HOCDBBarsEx, HOCDBHealth, HOCDBDecision, HOCDBEvaluation,
                          indicator_kinds, indicator_outputs, indicator_warmup, indicator_is_lookahead)

try:
    import numpy as np
except ImportError:
    np = None

TICKER = "TEST_INDICATORS_PYTHON"
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "b_python_test_indicators"))
N = 3000
STEP = 60
LAST_TS = 1000 + (N - 1) * STEP
INT64_MIN, INT64_MAX = -(1 << 63), (1 << 63) - 1


def check(cond, msg):
    if not cond:
        raise RuntimeError(f"FAIL: {msg}")


def expect_error(fn, msg, exc=ValueError):
    try:
        fn()
    except exc as e:
        print(f"  ok: {msg} -> {type(e).__name__}: {e}")
        return
    raise RuntimeError(f"FAIL: {msg} (no {exc.__name__} raised)")


def lcg(state):
    state[0] = (state[0] * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
    return (state[0] >> 11) / 9007199254740992.0


if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)

schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("open", FieldTypes.F64),
    HOCDBField("high", FieldTypes.F64),
    HOCDBField("low", FieldTypes.F64),
    HOCDBField("close", FieldTypes.F64),
    HOCDBField("volume", FieldTypes.F64),
]

print("Struct layouts...")
check(ctypes.sizeof(HOCDBIndicatorSpec) == 56, "HOCDBIndicatorSpec is 56 bytes")
check(HOCDBIndicatorSpec.param.offset == 24 and HOCDBIndicatorSpec.field_index.offset == 40, "HOCDBIndicatorSpec offsets")
check(ctypes.sizeof(HOCDBIndicatorResult) == 32, "HOCDBIndicatorResult is 32 bytes")
check(ctypes.sizeof(HOCDBBars) == 64, "HOCDBBars is 64 bytes")
check(ctypes.sizeof(HOCDBIndicatorColumns) == 64, "HOCDBIndicatorColumns has 8 int64 roles (64 bytes)")
check([f[0] for f in HOCDBIndicatorColumns._fields_] == ["open", "high", "low", "close", "volume", "bid", "ask", "side"], "column roles in ABI order")
check(ctypes.sizeof(HOCDBBarsEx) == 72 and HOCDBBarsEx.buy_volume.offset == 64, "HOCDBBarsEx is HOCDBBars + buy_volume at offset 64")
check(ctypes.sizeof(HOCDBDecision) == 32, "HOCDBDecision is 32 bytes")
check(ctypes.sizeof(HOCDBHealth) == 152 and len(HOCDBHealth._fields_) == 19, "HOCDBHealth is 19 fields / 152 bytes")
check(ctypes.sizeof(HOCDBEvaluation) == 160 and len(HOCDBEvaluation._fields_) == 20, "HOCDBEvaluation is 20 fields / 160 bytes")

print("Registry (module-level helpers, no database needed)...")
kinds = indicator_kinds()
check(len(kinds) == 83, f"83 indicator kinds, got {len(kinds)}")
check("rsi" in kinds and "heikin_ashi" in kinds, "kind names")
check(all(k in kinds for k in ("spread", "order_flow", "series2", "ratio", "forward_return", "triple_barrier", "session_vwap", "pivots")), "new kind names")
check(indicator_outputs("pivots") == ["pp", "r1", "s1", "r2", "s2"], "pivots outputs")
check(indicator_outputs(IndicatorKinds.FORWARD_RETURN) == ["ret", "max", "min"], "forward_return outputs by id")
check(indicator_outputs("order_flow") == ["net", "imbalance"] and indicator_outputs("spread") == ["abs", "bps"], "microstructure outputs")
check(indicator_is_lookahead("forward_return") and indicator_is_lookahead(IndicatorKinds.TRIPLE_BARRIER), "labels are look-ahead")
check(not indicator_is_lookahead("sma") and not indicator_is_lookahead("session_vwap"), "sma / session_vwap are not look-ahead")
check(indicator_is_lookahead("FORWARD_RETURN") is True and indicator_is_lookahead("sma") is False, "lookahead returns bools, case-insensitive")
expect_error(lambda: indicator_is_lookahead("nope"), "lookahead of an unknown kind")
check(indicator_outputs("macd") == ["macd", "signal", "hist"], "macd outputs")
check(indicator_outputs("MACD") == ["macd", "signal", "hist"], "kind names are case-insensitive")
check(indicator_outputs(IndicatorKinds.BBANDS) == ["upper", "middle", "lower", "percent_b", "bandwidth"], "bbands outputs by id")
check(indicator_outputs("sma") == ["value"], "single output kind")
check(indicator_warmup({"kind": "ema", "period": 200}) > 200, "warmup > period for EMA")
expect_error(lambda: indicator_outputs("nope"), "unknown kind name")
expect_error(lambda: indicator_outputs(9999), "unknown kind id")

print("Initializing DB...")
db = HOCDB(TICKER, DATA_DIR, schema)
try:
    print("Appending data...")
    closes = []
    state = [7]
    p = 100.0
    for i in range(N):
        o = p
        p *= math.exp((lcg(state) - 0.5) * 0.02)
        db.append(1000 + i * STEP, o, max(o, p) * 1.003, min(o, p) * 0.997, p, 1000.0 + (i % 50))
        closes.append(p)
    db.flush()
    check(db.indicator_kinds() == kinds, "instance registry matches module-level registry")

    specs = [
        {"kind": "sma", "period": 20},
        {"kind": "macd"},
        {"kind": "rsi", "period": 14},
        {"kind": "bbands", "period": 20, "param": 2.0},
        {"kind": "atr", "period": 14},
        {"kind": IndicatorKinds.OBV},
        {"kind": "sma", "period": 10, "field": "volume", "label": "vol_sma"},
    ]
    expected_columns = [
        "sma_20", "macd", "macd_signal", "macd_hist", "rsi_14",
        "bbands_20_upper", "bbands_20_middle", "bbands_20_lower", "bbands_20_percent_b", "bbands_20_bandwidth",
        "atr_14", "obv", "vol_sma",
    ]

    print("Batch over a range...")
    start_ts, end_ts = 1000 + 1000 * STEP, 1000 + 1500 * STEP
    res = db.indicators(specs, start_ts=start_ts, end_ts=end_ts)
    cols = res["columns"]
    check(res["n_rows"] == 500, f"500 rows, got {res['n_rows']}")
    check(list(cols.keys()) == expected_columns, f"column names, got {list(cols.keys())}")
    check(len(res["timestamps"]) == 500 and all(len(v) == 500 for v in cols.values()), "array lengths")
    check(res["timestamps"][0] == start_ts and res["timestamps"][-1] == end_ts - STEP, "first / last ts")
    check(not any(math.isnan(v) for v in cols["sma_20"]), "sma converged (auto lookback)")
    check(all(0.0 <= v <= 100.0 for v in cols["rsi_14"]), "rsi in [0,100]")
    check(all(u >= m >= l for u, m, l in zip(cols["bbands_20_upper"], cols["bbands_20_middle"], cols["bbands_20_lower"])), "bbands ordered")
    check(all(v > 0 for v in cols["atr_14"]), "atr positive")
    check(all(1000.0 <= v <= 1049.0 for v in cols["vol_sma"]), "sma of the volume field")

    print("SMA reference check...")
    for i in (0, 1, 250, 499):
        r = 1000 + i
        expected = sum(closes[r - 19:r + 1]) / 20.0
        got = cols["sma_20"][i]
        check(abs(got - expected) <= 1e-9 * abs(expected), f"sma_20 row {i}: got {got}, expected {expected}")

    print("Lookback 0...")
    res0 = db.indicators(specs[:1], start_ts=start_ts, end_ts=end_ts, lookback=0)
    sma0 = res0["columns"]["sma_20"]
    check(res0["n_rows"] == 500, "lookback 0 rows")
    check(all(math.isnan(v) for v in sma0[:19]) and not math.isnan(sma0[19]), "NaN warm-up with lookback 0")

    print("Tail...")
    tail = db.indicators(specs[:3], tail=5)
    check(tail["n_rows"] == 5 and len(tail["columns"]) == 5, "tail rows / outputs")
    check(tail["timestamps"][-1] == LAST_TS, "tail last ts")

    print("Tail with bucket (tick -> 5-minute bars)...")
    bt = db.indicators(specs[:1], tail=10, bucket=300, lookback=0)
    check(bt["n_rows"] == 10, "tail with bucket rows")
    check(all(b - a == 300 for a, b in zip(bt["timestamps"], bt["timestamps"][1:])), "bar spacing")
    check(bt["timestamps"][0] % 300 == 0, "bar alignment")

    print("Empty window...")
    empty = db.indicators(specs, start_ts=10 ** 9, end_ts=10 ** 9 + 1)
    check(empty["n_rows"] == 0 and empty["timestamps"] == [] and all(v == [] for v in empty["columns"].values()), "zero rows")

    print("Errors...")
    expect_error(lambda: db.indicators([{"kind": "nope"}], tail=10), "unknown kind name")
    expect_error(lambda: db.indicators([{"kind": 9999}], tail=10), "unknown kind id")
    expect_error(lambda: db.indicators(specs[:1], tail=10, columns={"open": "open", "high": "high"}), "missing close column")
    expect_error(lambda: db.indicators([{"kind": "atr"}], tail=10, columns={"close": "close"}), "atr with only close column")
    expect_error(lambda: db.indicators([{"kind": "sma", "period": 5, "field": "nope"}], tail=10), "invalid field name")
    expect_error(lambda: db.indicators([{"kind": "sma", "period": 5, "field": 42}], tail=10), "invalid field index")
    expect_error(lambda: db.indicators([{"kind": "sma", "period": 5, "field": "volume"}], tail=10, bucket=300), "field override with bucket")
    expect_error(lambda: db.indicators([{"kind": "sma", "peroid": 5}], tail=10), "unknown spec key")
    expect_error(lambda: db.indicators(specs[:1]), "neither window nor tail")
    expect_error(lambda: db.indicators(specs[:1], start_ts=start_ts, end_ts=end_ts, tail=5), "both window and tail")
    expect_error(lambda: db.indicators(specs[:1], start_ts=start_ts), "start without end")
    expect_error(lambda: db.indicators([{"kind": "rsi"}, {"kind": "rsi"}], tail=10), "duplicate column names")
    expect_error(lambda: db.indicators(specs[:1], tail=10, lookback="lots"), "bad lookback")

    print("OHLCV...")
    bars = db.ohlcv(INT64_MIN, INT64_MAX, 300, price="close", volume="volume")
    nb = bars["n_bars"]
    check(nb > 500, f"> 500 bars, got {nb}")
    check(all(len(bars[k]) == nb for k in ("timestamps", "open", "high", "low", "close", "volume", "count")), "bar array lengths")
    check(all(h >= l for h, l in zip(bars["high"], bars["low"])), "bar high >= low")
    check(all(l <= c <= h for h, l, c in zip(bars["high"], bars["low"], bars["close"])), "close within bar")
    check(all(c >= 1 for c in bars["count"]), "bar count >= 1")
    check(abs(sum(bars["count"]) - N) < 1e-9, "bar counts add up to N")
    expect_error(lambda: db.ohlcv(INT64_MIN, INT64_MAX, 0), "ohlcv bucket 0")

    print("Summary...")
    s = db.summary(INT64_MIN, INT64_MAX, "close", periods_per_year=252)
    check(len(s) == 29, f"29 summary fields, got {len(s)}")
    check(s["count"] == N, f"summary count {s['count']}")
    check(-1.0 <= s["max_drawdown"] <= 0.0, "max drawdown range")
    check(0.0 <= s["win_rate"] <= 1.0, "win rate range")
    check(math.isfinite(s["sharpe"]) and math.isfinite(s["hurst"]), "summary fields computed")
    check(abs(s["last"] - closes[-1]) < 1e-9 and abs(s["first"] - closes[0]) < 1e-9, "summary first / last")

    print("Snapshot...")
    snap = db.snapshot(periods_per_year=252)
    check(len(snap) >= 90, f">= 90 snapshot fields, got {len(snap)}")
    check(isinstance(snap["timestamp"], int) and isinstance(snap["bars"], int), "snapshot int fields")
    check(snap["bars"] == 2500, f"snapshot bars {snap['bars']}")
    check(snap["timestamp"] == LAST_TS, "snapshot ts")
    check(0.0 <= snap["rsi_14"] <= 100.0, "snapshot rsi")
    check(all(math.isfinite(snap[k]) for k in ("ema_200", "adx_14", "mfi_14", "supertrend")), "snapshot fields finite")
    check(abs(snap["close"] - closes[-1]) < 1e-9, "snapshot close is latest")
    snap2 = db.snapshot(bars=50, bucket=300, periods_per_year=252)
    check(snap2["bars"] == 50, "snapshot with bucket bars")
    check(math.isnan(snap2["sma_200"]) and not math.isnan(snap2["sma_20"]), "snapshot with bucket: sma_200 NaN, sma_20 finite")

    if np is not None:
        print("numpy output...")
        rn = db.indicators(specs, start_ts=start_ts, end_ts=end_ts, as_numpy=True)
        check(isinstance(rn["timestamps"], np.ndarray) and rn["timestamps"].dtype == np.int64, "numpy timestamps")
        check(np.array_equal(rn["timestamps"], np.array(res["timestamps"])), "numpy timestamps match lists")
        for name in expected_columns:
            check(isinstance(rn["columns"][name], np.ndarray) and rn["columns"][name].dtype == np.float64, f"numpy column {name}")
            check(np.allclose(rn["columns"][name], np.array(cols[name]), equal_nan=True), f"numpy column {name} matches list")
        bn = db.ohlcv(INT64_MIN, INT64_MAX, 300, price="close", volume="volume", as_numpy=True)
        check(isinstance(bn["close"], np.ndarray) and np.allclose(bn["close"], np.array(bars["close"])), "numpy ohlcv")
        en = db.indicators(specs[:1], start_ts=10 ** 9, end_ts=10 ** 9 + 1, as_numpy=True)
        check(en["timestamps"].shape == (0,) and en["columns"]["sma_20"].shape == (0,), "numpy zero rows")
    else:
        print("numpy not installed, skipping numpy checks")

    # ------------------------------------------------------------------
    # Round 2: ticks with quotes / sides, pairs, labels, sessions, health, evaluation, multi-snapshot
    # ------------------------------------------------------------------
    print("Tick databases (A: one tick per second, B: every 2 seconds, same data dir)...")
    tick_schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("price", FieldTypes.F64),
        HOCDBField("size", FieldTypes.F64),
        HOCDBField("bid", FieldTypes.F64),
        HOCDBField("ask", FieldTypes.F64),
        HOCDBField("side", FieldTypes.BOOL),
    ]
    SEC = 1_000_000            # microsecond timestamps, one tick per second
    NT = 6000
    ta = HOCDB("PAIR_A", DATA_DIR, tick_schema)
    tb = HOCDB("PAIR_B", DATA_DIR, tick_schema)
    try:
        check(ta.lib.hocdb_health_size() == ctypes.sizeof(HOCDBHealth), "library agrees on HOCDBHealth size")
        check(ta.lib.hocdb_evaluation_size() == ctypes.sizeof(HOCDBEvaluation), "library agrees on HOCDBEvaluation size")
        check(ta.lib.hocdb_decision_size() == ctypes.sizeof(HOCDBDecision), "library agrees on HOCDBDecision size")

        a_prices = []
        tstate = [11]
        pa, pb = 100.0, 50.0
        for i in range(NT):
            pa *= math.exp((lcg(tstate) - 0.5) * 0.004)
            pb *= math.exp((lcg(tstate) - 0.5) * 0.004)
            ta.append(SEC * i, pa, 1.0 + i % 4, pa * 0.999, pa * 1.001, i % 3 != 0)
            a_prices.append(pa)
            if i % 2 == 0:  # B trades every 2 seconds, 0.3 s after A
                tb.append(SEC * i + 300_000, pb, 2.0, pb * 0.999, pb * 1.001, i % 2 != 0)
        ta.flush()
        tb.flush()

        print("Microstructure / session / label batch, tail(100) with auto-detected price/size/bid/ask/side...")
        micro_specs = [
            {"kind": "spread"},
            {"kind": "order_flow", "period": 10},
            {"kind": "trade_intensity", "period": 10, "param": 1e6},   # param = timestamp units per second
            {"kind": "tick_pressure", "period": 20},
            {"kind": "session_vwap", "param": 600e6},                   # param = 10-minute sessions
            {"kind": IndicatorKinds.FORWARD_RETURN, "period": 5},       # label: looks 5 rows ahead
        ]
        micro = ta.indicators(micro_specs, tail=100)
        mc = micro["columns"]
        check(micro["n_rows"] == 100 and len(mc) == 11, f"100 rows x 11 outputs, got {micro['n_rows']} x {len(mc)}")
        check(list(mc.keys()) == [
            "spread_abs", "spread_bps", "order_flow_10_net", "order_flow_10_imbalance",
            "trade_intensity_10_trades_per_sec", "trade_intensity_10_volume_per_sec", "tick_pressure_20",
            "session_vwap", "forward_return_5_ret", "forward_return_5_max", "forward_return_5_min",
        ], f"microstructure column names, got {list(mc.keys())}")
        check(micro["timestamps"][-1] == SEC * (NT - 1) and len(micro["timestamps"]) == 100, "tick tail timestamps")
        check(all(abs(v - 20.0) < 1e-9 for v in mc["spread_bps"]), "spread == 20 bps")
        check(all(abs(v - 1.0) < 1e-9 for v in mc["trade_intensity_10_trades_per_sec"]), "1 trade per second")
        check(all(-1.0 <= v <= 1.0 for v in mc["order_flow_10_imbalance"]), "imbalance in [-1, 1]")
        check(all(math.isfinite(v) and v > 0 for v in mc["session_vwap"]), "session vwap positive")
        fr = mc["forward_return_5_ret"]
        check(all(math.isnan(v) for v in fr[-5:]) and all(math.isfinite(v) for v in fr[:-5]), "forward return NaN only in the last 5 rows")
        check(all(mx >= r >= mn for r, mx, mn in zip(fr[:-5], mc["forward_return_5_max"][:-5], mc["forward_return_5_min"][:-5])), "forward return within [min, max]")

        print("Session kinds need param; explicit bid/ask columns...")
        expect_error(lambda: ta.indicators([{"kind": "session_vwap"}], tail=10), "session_vwap without param")
        expect_error(lambda: ta.indicators([{"kind": "pivots"}], tail=10), "pivots without param")
        expect_error(lambda: ta.indicators([{"kind": "pivots", "param": 600e6}], tail=5), "pivots need high/low (ticks have only a price)")
        piv = db.indicators([{"kind": "pivots", "param": 86400}], tail=5)   # daily sessions on the 60-second OHLCV bars
        check(list(piv["columns"]) == ["pivots_pp", "pivots_r1", "pivots_s1", "pivots_r2", "pivots_s2"], "pivots column names")
        check(all(math.isfinite(v) for v in piv["columns"]["pivots_pp"]), "pivots computed from the previous session")
        check(all(r1 >= pp >= s1 for pp, r1, s1 in zip(piv["columns"]["pivots_pp"], piv["columns"]["pivots_r1"], piv["columns"]["pivots_s1"])), "pivot levels ordered")
        check(ta.indicator_is_lookahead("forward_return") and not ta.indicator_is_lookahead("sma"), "instance lookahead flag")
        expect_error(lambda: ta.indicators([{"kind": "spread"}], tail=10, columns={"close": "price"}), "spread without bid/ask columns")
        sp = ta.indicators([{"kind": "spread"}], tail=3, columns={"close": "price", "bid": 3, "ask": "ask"})
        check(all(abs(v - 20.0) < 1e-9 for v in sp["columns"]["spread_bps"]), "explicit bid/ask columns by index and by name")
        expect_error(lambda: ta.indicators([{"kind": "spread"}], tail=3, columns={"close": "price", "sides": "side"}), "unknown column role")

        print("Pairs: as-of join on ticks, tail(50)...")
        pair_specs = [
            {"kind": "series"}, {"kind": "series2"}, {"kind": "ratio"},
            {"kind": "correl", "period": 30}, {"kind": "rel_strength", "period": 10},
        ]
        pt = ta.pair_indicators(tb, pair_specs, tail=50)
        pc = pt["columns"]
        check(pt["n_rows"] == 50 and list(pc.keys()) == ["series", "series2", "ratio", "correl_30", "rel_strength_10"], f"pair tail columns {list(pc.keys())}")
        check(all(len(v) == 50 for v in pc.values()) and len(pt["timestamps"]) == 50, "pair tail array lengths")
        check(all(abs(a / b - r) < 1e-12 for a, b, r in zip(pc["series"], pc["series2"], pc["ratio"])), "ratio == series / series2")
        check(all(math.isfinite(v) for v in pc["correl_30"]), "correl finite")
        check(all(math.isfinite(v) for v in pc["rel_strength_10"]), "rel_strength finite")
        check(pt["timestamps"][-1] == SEC * (NT - 1), "pair tail ends at the last A tick")
        check(abs(pc["series"][-1] - a_prices[-1]) < 1e-12, "series is A's price")
        check(abs(pc["series2"][-1] - tb.get_latest("price")["value"]) < 1e-12, "series2 is the latest B price at or before the A tick")

        print("Pairs: inner join on 10-second bars over [1000 s, 2000 s)...")
        pr = ta.pair_indicators(tb, pair_specs, start_ts=1_000 * SEC, end_ts=2_000 * SEC, bucket=10 * SEC, lookback=0)
        check(pr["n_rows"] == 100, f"100 ten-second bars, got {pr['n_rows']}")
        check(pr["timestamps"][0] == 1_000 * SEC and pr["timestamps"][-1] == 1_990 * SEC, "first / last bar")
        check(all(b - a == 10 * SEC for a, b in zip(pr["timestamps"], pr["timestamps"][1:])), "bars 10 s apart")
        check(all(t % (10 * SEC) == 0 for t in pr["timestamps"]), "bars aligned to the bucket")
        check(all(abs(a / b - r) < 1e-12 for a, b, r in zip(pr["columns"]["series"], pr["columns"]["series2"], pr["columns"]["ratio"])), "ratio on bars")
        expect_error(lambda: ta.pair_indicators("not a db", pair_specs, tail=10), "other must be a HOCDB instance")
        expect_error(lambda: ta.pair_indicators(tb, pair_specs), "pair without window or tail")
        expect_error(lambda: ta.pair_indicators(tb, pair_specs, tail=10, other_columns={"open": "price"}), "other_columns without close")
        expect_error(lambda: ta.pair_indicators(tb, [{"kind": "nope"}], tail=10), "pair with unknown kind")

        print("OHLCV with side (buy volume)...")
        bx = ta.ohlcv(INT64_MIN, INT64_MAX, 60 * SEC, price="price", volume="size", side="side")
        check(bx["n_bars"] == 100, f"100 one-minute bars, got {bx['n_bars']}")
        check("buy_volume" in bx and len(bx["buy_volume"]) == 100, "buy_volume present")
        check(all(0.0 < b < v for b, v in zip(bx["buy_volume"], bx["volume"])), "0 <= buy_volume <= volume")
        check(abs(bx["volume"][0] - sum(1.0 + i % 4 for i in range(60))) < 1e-9, "volume reference (first minute)")
        check(abs(bx["buy_volume"][0] - sum(1.0 + i % 4 for i in range(60) if i % 3 != 0)) < 1e-9, "buy volume reference (first minute)")
        bn = ta.ohlcv(INT64_MIN, INT64_MAX, 60 * SEC, price="price", volume="size")
        check("buy_volume" not in bn and bn["n_bars"] == 100, "no buy_volume without side")
        check(bn["volume"] == bx["volume"] and bn["close"] == bx["close"] and bn["timestamps"] == bx["timestamps"], "same bars with and without side")
        expect_error(lambda: ta.ohlcv(INT64_MIN, INT64_MAX, 60 * SEC, price="price", side="nope"), "ohlcv unknown side field")

        print("Health...")
        h = ta.health(INT64_MIN, INT64_MAX, price="price", volume="size", gap_threshold=5 * SEC, outlier_threshold=0.05)
        check(len(h) == 19, f"19 health fields, got {len(h)}")
        check(h["count"] == NT and h["n_gaps"] == 0 and h["median_gap"] == float(SEC) and h["n_outlier_returns"] == 0, f"health values {h}")
        check(h["first_ts"] == 0 and h["last_ts"] == (NT - 1) * SEC and h["span"] == (NT - 1) * SEC, "health first / last / span")
        check(h["max_gap"] == SEC and abs(h["mean_gap"] - SEC) < 1e-6, "health gaps")
        check(h["n_nonpositive_price"] == 0 and h["n_nan_price"] == 0 and h["n_zero_volume"] == 0 and h["n_negative_volume"] == 0, "health counters")
        check(isinstance(h["count"], int) and isinstance(h["first_ts"], int) and isinstance(h["mean_gap"], float), "health field types")
        check(0.0 < h["max_abs_return"] < 0.05, "max abs return below the outlier threshold")
        h2 = ta.health(INT64_MIN, INT64_MAX, price="price", gap_threshold=500_000)
        check(h2["n_gaps"] == NT - 1, "every 1-second gap exceeds a 0.5-second threshold")
        expect_error(lambda: ta.health(0, 1, price="nope"), "health unknown price field")
        expect_error(lambda: ta.health(0, 1, price="price", gap_threshold=-1), "health negative gap threshold")

        print("Evaluate...")
        decisions = [
            {"timestamp": 100 * SEC, "direction": 1, "size": 1000, "horizon": 60 * SEC},
            {"timestamp": 200 * SEC, "direction": -1, "size": 500},                    # horizon 0 -> default
            {"timestamp": 5990 * SEC, "direction": 1, "size": 100, "horizon": 60 * SEC},   # exit past the data
            {"timestamp": 300 * SEC, "direction": 0},                                   # flat: ignored
        ]
        ev = ta.evaluate(decisions, price="price", default_horizon=120 * SEC, cost_bps=5.0)
        check(len(ev) == 20 + 3, f"20 evaluation fields + 3 per-decision arrays, got {len(ev)}")
        check(ev["n_decisions"] == 4 and ev["n_evaluated"] == 2 and ev["n_long"] == 2 and ev["n_short"] == 1, f"evaluate counts {ev}")
        ent, ex, net = ev["entry"], ev["exit"], ev["net_return"]
        check(len(ent) == 4 and len(ex) == 4 and len(net) == 4, "per-decision array lengths")
        check(math.isfinite(net[0]) and math.isfinite(net[1]) and math.isnan(net[2]) and math.isnan(net[3]), "net return NaN where not evaluated")
        check(abs(net[0] - (ex[0] / ent[0] - 1.0 - 0.001)) < 1e-12, "net = gross - 2 x 5 bps")
        check(abs(ent[0] - a_prices[100]) < 1e-12 and abs(ex[0] - a_prices[160]) < 1e-12, "entry / exit at the decision and horizon ticks")
        check(abs(ent[1] - a_prices[200]) < 1e-12 and abs(ex[1] - a_prices[320]) < 1e-12, "default horizon used when horizon is 0")
        check(0.0 <= ev["hit_rate"] <= 1.0 and math.isfinite(ev["total_pnl"]) and ev["total_cost"] > 0, "evaluation fields")
        check(isinstance(ev["n_long"], int) and isinstance(ev["hit_rate"], float), "evaluation field types")
        empty = ta.evaluate([], price="price", default_horizon=SEC)
        check(empty["n_decisions"] == 0 and empty["n_evaluated"] == 0 and math.isnan(empty["hit_rate"]), "empty evaluation")
        check(empty["entry"] == [] and empty["exit"] == [] and empty["net_return"] == [], "empty per-decision arrays")
        single_dec = ta.evaluate(decisions[0], price="price", cost_bps=5.0)
        check(single_dec["n_evaluated"] == 1 and abs(single_dec["net_return"][0] - net[0]) < 1e-12, "a single decision dict is accepted")
        expect_error(lambda: ta.evaluate([{"timestamp": 0}], price="price"), "decision without direction")
        expect_error(lambda: ta.evaluate([{"timestamp": 0, "direction": 1, "horizn": 5}], price="price"), "unknown decision key")
        expect_error(lambda: ta.evaluate([{"timestamp": "0", "direction": 1}], price="price"), "non-int timestamp")
        expect_error(lambda: ta.evaluate([(0, 1)], price="price"), "decision must be a dict")
        expect_error(lambda: ta.evaluate(decisions, price="price", default_horizon=-1), "negative default horizon")

        print("Snapshot multi (1-minute and 5-minute bars from one read)...")
        multi = ta.snapshot_multi([60 * SEC, 300 * SEC], periods_per_year=[525600, 105120], bars=50)
        check(isinstance(multi, list) and len(multi) == 2, "two snapshots")
        single = ta.snapshot(bars=50, bucket=60 * SEC, periods_per_year=525600)
        check(set(multi[0]) == set(single) and len(single) >= 90, "same fields as snapshot()")
        for k in single:
            a, b = single[k], multi[0][k]
            check(a == b or (isinstance(a, float) and math.isnan(a) and math.isnan(b)), f"multi[0][{k!r}] == snapshot: {b} vs {a}")
        check(multi[0]["bars"] == 50 and multi[1]["bars"] == 20, f"bars per bucket {multi[0]['bars']} / {multi[1]['bars']} (only 100 minutes of data)")
        check(multi[0]["timestamp"] % (60 * SEC) == 0 and multi[1]["timestamp"] % (300 * SEC) == 0, "snapshot bar timestamps aligned")
        check(abs(multi[0]["close"] - a_prices[-1]) < 1e-9 and abs(multi[1]["close"] - a_prices[-1]) < 1e-9, "latest close in every timeframe")
        check(math.isnan(multi[0]["sma_200"]) and not math.isnan(multi[0]["sma_20"]), "1-minute: 50 bars -> sma_200 NaN, sma_20 finite")
        one = ta.snapshot_multi([60 * SEC], bars=50)
        check(len(one) == 1 and one[0]["bars"] == 50 and one[0]["rsi_14"] == multi[0]["rsi_14"], "periods_per_year defaults to 0")
        expect_error(lambda: ta.snapshot_multi([], bars=50), "empty buckets")
        expect_error(lambda: ta.snapshot_multi([60 * SEC, 0], bars=50), "bucket 0 in snapshot_multi")
        expect_error(lambda: ta.snapshot_multi([60 * SEC], periods_per_year=[1, 2], bars=50), "periods_per_year length mismatch")
        expect_error(lambda: ta.snapshot_multi(60 * SEC, bars=50), "buckets must be a list")
        expect_error(lambda: ta.snapshot_multi([60 * SEC], bars=50, columns={"open": "price"}), "snapshot_multi without close")

        if np is not None:
            print("numpy output for pairs / ohlcv with side...")
            pn = ta.pair_indicators(tb, pair_specs, tail=50, as_numpy=True)
            check(isinstance(pn["columns"]["ratio"], np.ndarray) and pn["columns"]["ratio"].dtype == np.float64, "numpy pair columns")
            check(np.allclose(pn["columns"]["ratio"], np.array(pc["ratio"])) and np.array_equal(pn["timestamps"], np.array(pt["timestamps"])), "numpy pair values match lists")
            bxn = ta.ohlcv(INT64_MIN, INT64_MAX, 60 * SEC, price="price", volume="size", side="side", as_numpy=True)
            check(isinstance(bxn["buy_volume"], np.ndarray) and np.allclose(bxn["buy_volume"], np.array(bx["buy_volume"])), "numpy buy_volume")
            en = ta.ohlcv(10 ** 15, 10 ** 15 + 1, 60 * SEC, price="price", side="side", as_numpy=True)
            check(en["n_bars"] == 0 and en["buy_volume"].shape == (0,) and en["close"].shape == (0,), "numpy empty bars with side")
    finally:
        ta.close()
        tb.close()

    print("Python Indicators Test Passed!")
finally:
    db.close()
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
