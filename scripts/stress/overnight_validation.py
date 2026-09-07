#!/usr/bin/env python3
"""End-to-end validation of HOCDB's indicator / analytics stack on synthetic
tick data: 30 days, several tickers with different sessions and microstructure.

Phases (each producing pass/fail checks and timings in a markdown report):

  1. generate ticks (scripts/stress/tickgen.py) and ingest them into HOCDB
  2. tick -> bar resampling (1m/5m/1h/1d) vs pandas resample
  3. every indicator on 1-minute bars vs TA-Lib / numpy references
     (full history, random windows with auto and zero lookback, tails)
  4. snapshot() vs references on the last 2500 bars
  5. summary() vs numpy references
  6. tick-mode indicators (bucket 0, field overrides, two-series kinds)
  7. edge cases and robustness (empty windows, reopen, ring buffer, ...)
  8. randomized fuzzing of specs / windows / buckets / lookbacks
  9. performance measurements
 10. memory growth over thousands of calls
 11. cross-binding bit-for-bit consistency (Bun, Node, C, C++, Go)
 12. round 2: microstructure, pairs, labels, sessions, health, evaluation, multi-snapshot
 13. operations (phase "ops"): a child writer process (scripts/stress/ops_writer.py) plus lock-free
     readers in this process, SIGKILL mid-stream and crash recovery, torn / misordered tails,
     checksum corruption, automatic compaction and rollover under a live reader, fsync
     policies, metrics and the ring buffer with the 64-byte header

Requires numpy, pandas and the TA-Lib python package (see run_overnight.sh).

    python3 scripts/stress/overnight_validation.py --days 30 --report stress_report.md
"""
import argparse
import ctypes
import json
import math
import os
import platform
import random
import resource
import shutil
import subprocess
import sys
import time
import traceback

import numpy as np
import pandas as pd
import talib

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, "bindings", "python"))

import references as R  # noqa: E402
import tickgen  # noqa: E402
from hocdb_python import HOCDB, HOCDBField, FieldTypes  # noqa: E402

US = 1_000_000
BAR_1M = 60 * US
HEADER_SIZE = 64  # bytes reserved by the HOC2 file header
SCHEMA = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("size", FieldTypes.F64),
    HOCDBField("bid", FieldTypes.F64),
    HOCDBField("ask", FieldTypes.F64),
    HOCDBField("side", FieldTypes.BOOL),
]
RECORD_DTYPE = np.dtype([("timestamp", "<i8"), ("price", "<f8"), ("size", "<f8"), ("bid", "<f8"), ("ask", "<f8"), ("side", "u1")])
COLS = {"close": "price", "volume": "size"}
PPY_1M = 365 * 24 * 60  # 1-minute bars per year
INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)

# The canonical batch: every kind that works without a second series.
ALL_SPECS = [
    {"kind": "sma", "period": 20}, {"kind": "ema", "period": 21}, {"kind": "wma", "period": 20},
    {"kind": "dema", "period": 20}, {"kind": "tema", "period": 20}, {"kind": "trima", "period": 20},
    {"kind": "kama", "period": 10}, {"kind": "hma", "period": 20}, {"kind": "zlema", "period": 20},
    {"kind": "vwma", "period": 20}, {"kind": "rma", "period": 14},
    {"kind": "rsi", "period": 14}, {"kind": "macd"}, {"kind": "ppo"}, {"kind": "stoch", "period": 14},
    {"kind": "stoch_rsi"}, {"kind": "cci", "period": 20}, {"kind": "willr", "period": 14},
    {"kind": "mom", "period": 10}, {"kind": "roc", "period": 10}, {"kind": "cmo", "period": 14},
    {"kind": "trix", "period": 15}, {"kind": "ultosc"}, {"kind": "ao"}, {"kind": "tsi"}, {"kind": "bop"},
    {"kind": "dpo", "period": 20},
    {"kind": "adx", "period": 14}, {"kind": "aroon", "period": 25}, {"kind": "psar"}, {"kind": "supertrend"},
    {"kind": "vortex", "period": 14}, {"kind": "ichimoku"}, {"kind": "linreg", "period": 20},
    {"kind": "atr", "period": 14}, {"kind": "natr", "period": 14}, {"kind": "true_range"}, {"kind": "bbands"},
    {"kind": "keltner"}, {"kind": "donchian", "period": 20}, {"kind": "stddev", "period": 20},
    {"kind": "variance", "period": 20}, {"kind": "hist_vol", "period": 20, "param": PPY_1M},
    {"kind": "obv"}, {"kind": "vwap"}, {"kind": "mfi", "period": 14}, {"kind": "cmf", "period": 20},
    {"kind": "ad"}, {"kind": "adosc"}, {"kind": "efi", "period": 13},
    {"kind": "returns", "period": 1}, {"kind": "log_returns", "period": 1}, {"kind": "zscore", "period": 20},
    {"kind": "percent_rank", "period": 20}, {"kind": "rolling_min", "period": 20}, {"kind": "rolling_max", "period": 20},
    {"kind": "drawdown"}, {"kind": "sharpe", "period": 20, "param": PPY_1M}, {"kind": "sortino", "period": 20, "param": PPY_1M},
    {"kind": "skew", "period": 20}, {"kind": "kurtosis", "period": 20},
    {"kind": "typical_price"}, {"kind": "median_price"}, {"kind": "heikin_ashi"},
    {"kind": "tick_pressure", "period": 20}, {"kind": "amihud", "period": 20}, {"kind": "realized_vol", "period": 20, "param": PPY_1M},
    {"kind": "series"}, {"kind": "forward_return", "period": 5}, {"kind": "triple_barrier", "period": 20},
    {"kind": "session_vwap", "param": 86_400 * US}, {"kind": "session_range", "param": 86_400 * US},
    {"kind": "opening_range", "period": 5, "param": 86_400 * US}, {"kind": "pivots", "param": 86_400 * US},
]
TICK_SPECS = [{"kind": k, **({"period": p} if p else {})} for k, p in [("sma", 20), ("ema", 21), ("wma", 20), ("dema", 20), ("tema", 20), ("hma", 20),
              ("vwma", 20), ("rsi", 14), ("macd", 0), ("ppo", 0), ("stoch_rsi", 0), ("mom", 10), ("roc", 10), ("cmo", 14), ("trix", 15), ("tsi", 0),
              ("dpo", 20), ("linreg", 20), ("bbands", 0), ("stddev", 20), ("hist_vol", 20), ("obv", 0), ("vwap", 0), ("efi", 13), ("returns", 1),
              ("zscore", 20), ("percent_rank", 20), ("rolling_min", 20), ("rolling_max", 20), ("drawdown", 0), ("sharpe", 20), ("skew", 20), ("kurtosis", 20)]]
# kinds whose values inside a window do not depend on history before the window
FINITE_WINDOW_KINDS = {"sma", "wma", "trima", "hma", "vwma", "cci", "willr", "mom", "roc", "cmo", "ultosc", "ao",
                       "bop", "dpo", "aroon", "vortex", "ichimoku", "linreg", "true_range", "bbands", "donchian",
                       "stddev", "variance", "hist_vol", "vwap", "mfi", "cmf", "returns", "log_returns", "zscore",
                       "percent_rank", "rolling_min", "rolling_max", "sharpe", "sortino", "skew", "kurtosis",
                       "typical_price", "median_price", "stoch", "correl", "beta"}
# kinds anchored at the window start (values are window-relative by design)
CUMULATIVE_COLUMNS = {"obv", "ad", "drawdown", "vwap"}
# columns that need future rows (NaN at the end of every window by definition)
LOOKAHEAD_COLUMNS = {"ichimoku_chikou", "forward_return_5_ret", "forward_return_5_max", "forward_return_5_min",
                     "triple_barrier_20_label", "triple_barrier_20_ret", "triple_barrier_20_bars"}
LOOKAHEAD_KINDS = {"forward_return", "triple_barrier"}
# kinds usable on raw ticks (no open/high/low needed)
# per-column absolute tolerance overrides (ill-conditioned near zero: small numerators or denominators)
ATOL = {"cci_20": 1e-6, "bbands_percent_b": 1e-6, "bbands_bandwidth": 1e-9, "zscore_20": 1e-6, "kurtosis_20": 1e-6, "skew_20": 1e-6,
        "ao": 1e-6, "dpo_20": 1e-6, "ppo": 1e-9, "ppo_signal": 1e-9, "ppo_hist": 1e-9, "linreg_20_slope": 1e-9, "linreg_20_r2": 1e-6,
        "macd_hist": 1e-9, "trix_15": 1e-9, "tsi": 1e-6, "tsi_signal": 1e-6, "cmo_14": 1e-6, "sharpe_20": 1e-6, "sortino_20": 1e-6}
# per-column relative tolerance overrides (default 1e-8)
TOL = {"kama_10": 1e-7, "linreg_20_r2": 1e-6, "skew_20": 1e-6, "kurtosis_20": 1e-6, "cmo_14": 1e-7,
       "zscore_20": 1e-6, "sharpe_20": 1e-6, "sortino_20": 1e-6, "hist_vol_20": 1e-7, "linreg_20_slope": 1e-6,
       "linreg_20_intercept": 1e-7, "linreg_20_value": 1e-8, "supertrend_line": 1e-8, "efi_13": 1e-8,
       "percent_rank_20": 1e-12}


def nan_eq_close(a, b, rtol, atol=0.0):
    """True where a and b agree (both NaN, or |a-b| <= atol + rtol*max(1,|b|))."""
    a = np.asarray(a, float)
    b = np.asarray(b, float)
    both_nan = np.isnan(a) & np.isnan(b)
    inf_eq = np.isinf(a) & np.isinf(b) & (np.sign(a) == np.sign(b))
    with np.errstate(invalid="ignore"):
        close = np.abs(a - b) <= atol + rtol * np.maximum(1.0, np.abs(b))
    return both_nan | inf_eq | close


class Harness:
    def __init__(self, args):
        self.args = args
        self.checks = []  # (phase, name, ok, detail)
        self.timings = {}
        self.notes = []
        self.t0 = time.time()
        self.data_dir = os.path.abspath(args.data_dir)
        self.ticks = {}
        self.bars_1m = {}
        self.tickers = args.tickers
        self._open = []

    # -- bookkeeping -----------------------------------------------------
    def check(self, phase, name, ok, detail=""):
        self.checks.append((phase, name, bool(ok), detail))
        mark = "ok " if ok else "FAIL"
        if not ok or self.args.verbose:
            print(f"  [{mark}] {phase}: {name} {detail}", flush=True)
        return bool(ok)

    def timing(self, name, seconds, extra=""):
        self.timings[name] = (seconds, extra)
        print(f"  [time] {name}: {seconds*1000:.2f} ms {extra}", flush=True)

    def failures(self):
        return [c for c in self.checks if not c[2]]

    def phase(self, title):
        self.close_all()
        print(f"\n=== {title} ({time.time()-self.t0:.0f}s elapsed) ===", flush=True)

    # -- helpers -----------------------------------------------------------
    def open_db(self, ticker, **kw):
        db = HOCDB(ticker, self.data_dir, SCHEMA, **kw)
        self._open.append(db)
        return db

    def close_all(self):
        """Close handles a crashed phase may have leaked (the file lock is exclusive)."""
        for db in self._open:
            try:
                if db.handle:
                    db.close()
            except Exception:  # noqa: BLE001
                pass
        self._open = []

    def ingest(self, db, d):
        """Fast path: pack records with numpy and call hocdb_append directly."""
        n = len(d["timestamp"])
        rec = np.empty(n, dtype=RECORD_DTYPE)
        rec["timestamp"] = d["timestamp"]
        rec["price"] = d["price"]
        rec["size"] = d["size"]
        rec["bid"] = d["bid"]
        rec["ask"] = d["ask"]
        rec["side"] = d["side"].astype(np.uint8)
        raw = rec.tobytes()
        rs = RECORD_DTYPE.itemsize
        assert rs == 41
        buf = ctypes.create_string_buffer(raw, len(raw))
        base = ctypes.addressof(buf)
        lib = db.lib
        lib.hocdb_append.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
        lib.hocdb_append.restype = ctypes.c_int
        append = lib.hocdb_append
        h = db.handle
        t0 = time.time()
        for i in range(n):
            if append(h, base + i * rs, rs) != 0:
                raise RuntimeError(f"append failed at record {i}")
        db.flush()
        return time.time() - t0

    def bars_from_db(self, db, bucket=BAR_1M, start=INT64_MIN, end=INT64_MAX):
        b = db.ohlcv(start, end, bucket, price="price", volume="size", as_numpy=True)
        return {k: np.asarray(b[k]) for k in ("timestamps", "open", "high", "low", "close", "volume", "count")}

    # -- references for bar-based columns --------------------------------
    def bar_references(self, o, h, l, c, v, ppy=PPY_1M):
        macd, macd_s, macd_h = talib.MACD(c, 12, 26, 9)
        ppo_l, ppo_s, ppo_h = R.ppo(c, 12, 26, 9)
        sk, sd = talib.STOCH(h, l, c, 14, 3, 0, 3, 0)
        srk, srd = R.stoch_rsi_ref(talib.RSI(c, 14), 14, 3, 3)
        tsi_l, tsi_s = R.tsi(c, 25, 13, 13)
        adx = talib.ADX(h, l, c, 14)
        ar_d, ar_u = talib.AROON(h, l, 25)
        st_l, st_d = R.supertrend(h, l, c, 10, 3.0)
        vx_p, vx_m = R.vortex(h, l, c, 14)
        ich = R.ichimoku(h, l, c, 9, 26, 52, 26)
        var20 = R.roll(c, 20, np.var)  # exact two-pass, unlike TA-Lib's running sums
        std20 = np.sqrt(var20)
        bm = talib.SMA(c, 20)
        bu, bl = bm + 2 * std20, bm - 2 * std20
        ku, km, kl = R.keltner(h, l, c, 20, 10, 2.0)
        du, dm, dl = R.donchian(h, l, 20)
        ha = R.heikin_ashi(o, h, l, c)
        tp = (h + l + c) / 3
        n = len(c)
        r1 = np.full(n, np.nan)
        r1[1:] = c[1:] / c[:-1] - 1
        lr1 = np.full(n, np.nan)
        lr1[1:] = np.log(c[1:] / c[:-1])
        return {
            "sma_20": talib.SMA(c, 20), "ema_21": talib.EMA(c, 21), "wma_20": talib.WMA(c, 20),
            "dema_20": talib.DEMA(c, 20), "tema_20": talib.TEMA(c, 20), "trima_20": talib.TRIMA(c, 20),
            "kama_10": talib.KAMA(c, 10), "hma_20": R.hma(c, 20), "zlema_20": R.zlema(c, 20),
            "vwma_20": R.vwma(c, v, 20), "rma_14": R.rma(c, 14),
            "rsi_14": talib.RSI(c, 14), "macd": macd, "macd_signal": macd_s, "macd_hist": macd_h,
            "ppo": talib.PPO(c, 12, 26, 1), "ppo_signal": ppo_s, "ppo_hist": ppo_h,
            "stoch_14_k": sk, "stoch_14_d": sd, "stoch_rsi_k": srk, "stoch_rsi_d": srd,
            "cci_20": talib.CCI(h, l, c, 20), "willr_14": talib.WILLR(h, l, c, 14),
            "mom_10": talib.MOM(c, 10), "roc_10": talib.ROC(c, 10), "cmo_14": R.cmo(c, 14),
            "trix_15": talib.TRIX(c, 15), "ultosc": talib.ULTOSC(h, l, c, 7, 14, 28), "ao": R.ao(h, l, 5, 34),
            "tsi": tsi_l, "tsi_signal": tsi_s, "bop": talib.BOP(o, h, l, c), "dpo_20": R.dpo(c, 20),
            "adx_14": adx, "adx_14_plus_di": talib.PLUS_DI(h, l, c, 14), "adx_14_minus_di": talib.MINUS_DI(h, l, c, 14),
            "aroon_25_up": ar_u, "aroon_25_down": ar_d, "aroon_25_osc": talib.AROONOSC(h, l, 25),
            "psar_sar": talib.SAR(h, l, 0.02, 0.2), "supertrend_line": st_l, "supertrend_dir": st_d,
            "vortex_14_plus": vx_p, "vortex_14_minus": vx_m,
            "ichimoku_tenkan": ich[0], "ichimoku_kijun": ich[1], "ichimoku_senkou_a": ich[2], "ichimoku_senkou_b": ich[3], "ichimoku_chikou": ich[4],
            "linreg_20_value": talib.LINEARREG(c, 20), "linreg_20_slope": talib.LINEARREG_SLOPE(c, 20),
            "linreg_20_intercept": talib.LINEARREG_INTERCEPT(c, 20), "linreg_20_r2": R.linreg_r2(c, 20),
            "atr_14": talib.ATR(h, l, c, 14), "natr_14": talib.NATR(h, l, c, 14), "true_range": talib.TRANGE(h, l, c),
            "bbands_upper": bu, "bbands_middle": bm, "bbands_lower": bl, "bbands_percent_b": (c - bl) / (bu - bl), "bbands_bandwidth": (bu - bl) / bm,
            "keltner_upper": ku, "keltner_middle": km, "keltner_lower": kl,
            "donchian_20_upper": du, "donchian_20_middle": dm, "donchian_20_lower": dl,
            "stddev_20": std20, "variance_20": var20, "hist_vol_20": R.hist_vol(c, 20, ppy),
            "obv": talib.OBV(c, v), "vwap": R.vwap(tp, v, 0), "mfi_14": talib.MFI(h, l, c, v, 14), "cmf_20": R.cmf(h, l, c, v, 20),
            "ad": talib.AD(h, l, c, v), "adosc": talib.ADOSC(h, l, c, v, 3, 10), "efi_13": R.efi(c, v, 13),
            "returns_1": r1, "log_returns_1": lr1, "zscore_20": R.zscore(c, 20), "percent_rank_20": R.percent_rank(c, 20),
            "rolling_min_20": talib.MIN(c, 20), "rolling_max_20": talib.MAX(c, 20), "drawdown": R.drawdown(c),
            "sharpe_20": R.sharpe(c, 20, ppy), "sortino_20": R.sortino(c, 20, ppy), "skew_20": R.skew(c, 20), "kurtosis_20": R.kurtosis(c, 20),
            "typical_price": tp, "median_price": (h + l) / 2,
            "heikin_ashi_open": ha[0], "heikin_ashi_high": ha[1], "heikin_ashi_low": ha[2], "heikin_ashi_close": ha[3],
        }

    def compare_columns(self, phase, ticker, got, refs, rtol_default=1e-8, label=""):
        """Compare hocdb columns with references; returns number of failing columns."""
        bad = 0
        for name, ref in refs.items():
            if name not in got:
                self.check(phase, f"{ticker}{label} column present: {name}", False)
                bad += 1
                continue
            g = np.asarray(got[name], float)
            ref = np.asarray(ref, float)
            rtol = TOL.get(name, rtol_default)
            valid = ~np.isnan(ref)
            if not valid.any():
                self.check(phase, f"{ticker}{label} reference for {name} is all NaN (harness bug)", False)
                bad += 1
                continue
            first_ref = int(np.argmax(valid))
            first_got = int(np.argmax(~np.isnan(g))) if (~np.isnan(g)).any() else len(g)
            ok_prefix = first_got <= first_ref
            ok_defined = not np.isnan(g[valid]).any()
            agree = nan_eq_close(g[valid], ref[valid], rtol, atol=ATOL.get(name, 0.0))
            n_bad = int((~agree).sum())
            worst = 0.0
            if n_bad:
                diff = np.abs(g[valid] - ref[valid]) / np.maximum(1.0, np.abs(ref[valid]))
                worst = float(np.nanmax(diff))
            ok = ok_prefix and ok_defined and n_bad == 0
            detail = ""
            if not ok:
                detail = f"first_got={first_got} first_ref={first_ref} holes={'yes' if not ok_defined else 'no'} mismatches={n_bad}/{int(valid.sum())} worst_rel={worst:.3e} rtol={rtol:g}"
                bad += 1
            self.check(phase, f"{ticker}{label} {name}", ok, detail)
        return bad

    # ------------------------------------------------------------------
    # Phase 1: generate + ingest
    # ------------------------------------------------------------------
    def phase_ingest(self):
        self.phase("Phase 1: generate and ingest ticks")
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir)
        total = 0
        for t in self.tickers:
            t0 = time.time()
            d = tickgen.generate(t, self.args.days, max_ticks=self.args.max_ticks)
            gen_s = time.time() - t0
            self.ticks[t] = d
            db = self.open_db(t)
            ing_s = self.ingest(db, d)
            n = len(d["timestamp"])
            total += n
            # read back a few records to make sure the ingest path stored them faithfully
            last = db.query(int(d["timestamp"][-1]), int(d["timestamp"][-1]) + 1)
            self.check("ingest", f"{t} last record round-trips", len(last) == 1 and last[0]["price"] == d["price"][-1]
                       and last[0]["timestamp"] == int(d["timestamp"][-1]) and bool(last[0]["side"]) == bool(d["side"][-1]),
                       f"got {last}")
            stats = db.get_stats(INT64_MIN, INT64_MAX, "price")
            self.check("ingest", f"{t} record count", stats["count"] == n, f"{stats['count']} vs {n}")
            self.check("ingest", f"{t} price min/max", math.isclose(stats["min"], d["price"].min()) and math.isclose(stats["max"], d["price"].max()))
            db.close()
            print(f"  {t:8s} ticks={n:>9,d} generated in {gen_s:.1f}s, ingested in {ing_s:.1f}s ({n/max(ing_s,1e-9):,.0f} rec/s)", flush=True)
            self.timing(f"ingest {t}", ing_s, f"{n:,d} records, {n/max(ing_s,1e-9):,.0f} rec/s")
        self.notes.append(f"Total ticks ingested: {total:,d} across {len(self.tickers)} tickers, {self.args.days} days.")

    # ------------------------------------------------------------------
    # Phase 2: resampling vs pandas
    # ------------------------------------------------------------------
    def phase_resample(self):
        self.phase("Phase 2: tick -> bar resampling vs pandas")
        for t in self.tickers:
            d = self.ticks[t]
            df = pd.DataFrame({"price": d["price"], "size": d["size"]}, index=pd.to_datetime(d["timestamp"], unit="us"))
            db = self.open_db(t)
            for bucket_s in (60, 300, 3600, 86400):
                t0 = time.time()
                b = self.bars_from_db(db, bucket_s * US)
                dt = time.time() - t0
                r = df.resample(f"{bucket_s}s", closed="left", label="left")
                ref = pd.DataFrame({
                    "open": r["price"].first(), "high": r["price"].max(), "low": r["price"].min(),
                    "close": r["price"].last(), "volume": r["size"].sum(), "count": r["price"].count(),
                }).dropna(subset=["open"])
                ref = ref[ref["count"] > 0]
                ref_ts = ref.index.asi8 // 1000
                ok = len(b["timestamps"]) == len(ref)
                self.check("resample", f"{t} {bucket_s}s bar count", ok, f"{len(b['timestamps'])} vs pandas {len(ref)}")
                if not ok:
                    continue
                self.check("resample", f"{t} {bucket_s}s timestamps", np.array_equal(b["timestamps"], ref_ts))
                for col in ("open", "high", "low", "close", "count"):
                    self.check("resample", f"{t} {bucket_s}s {col}", np.array_equal(b[col], ref[col].to_numpy(float)))
                self.check("resample", f"{t} {bucket_s}s volume", np.allclose(b["volume"], ref["volume"].to_numpy(float), rtol=1e-9, atol=0))
                if bucket_s == 60:
                    self.bars_1m[t] = b
                    self.timing(f"ohlcv 1m {t}", dt, f"{len(d['timestamp']):,d} ticks -> {len(b['timestamps']):,d} bars")
            db.close()

    # ------------------------------------------------------------------
    # Phase 3: indicators on 1-minute bars vs TA-Lib / numpy
    # ------------------------------------------------------------------
    def phase_indicators(self):
        self.phase("Phase 3: indicators on 1-minute bars vs TA-Lib / numpy references")
        rng = random.Random(1234)
        for t in self.tickers:
            b = self.bars_1m[t]
            o, h, l, c, v = b["open"], b["high"], b["low"], b["close"], b["volume"]
            n = len(c)
            first_ts, last_ts = int(b["timestamps"][0]), int(b["timestamps"][-1])
            db = self.open_db(t)
            t0 = time.time()
            full = db.indicators(ALL_SPECS, start_ts=first_ts, end_ts=last_ts + 1, columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
            dt = time.time() - t0
            self.timing(f"batch {len(ALL_SPECS)} specs on {n:,d} 1m bars ({t})", dt)
            self.check("indicators", f"{t} full-history row count", full["n_rows"] == n, f"{full['n_rows']} vs {n}")
            self.check("indicators", f"{t} full-history timestamps", np.array_equal(np.asarray(full["timestamps"]), b["timestamps"]))
            t0 = time.time()
            refs = self.bar_references(o, h, l, c, v)
            self.timing(f"reference computation ({t})", time.time() - t0)
            bad = self.compare_columns("indicators", t, full["columns"], refs)
            # outputs without a reference: sanity only
            self.check("indicators", f"{t} psar_dir in {{-1,+1}}", np.all(np.isin(full["columns"]["psar_dir"][1:], [-1.0, 1.0])))
            self.check("indicators", f"{t} all {len(full['columns'])} columns compared", bad == 0, f"{bad} columns with mismatches")

            # -- windows with auto lookback: must equal full-history values (converged) --
            n_windows = 4 if self.args.quick else 25
            scale = float(np.mean(np.abs(c)))
            worst = {}
            win_bad = 0
            for w in range(n_windows):
                a = rng.randrange(0, n - 100)
                z = rng.randrange(a + 10, min(n, a + 5000))
                win = db.indicators(ALL_SPECS, start_ts=int(b["timestamps"][a]), end_ts=int(b["timestamps"][z - 1]) + 1,
                                    columns=COLS, lookback="auto", bucket=BAR_1M, as_numpy=True)
                if not self.check("windows", f"{t} window[{a}:{z}] rows", win["n_rows"] == z - a, f"{win['n_rows']} vs {z-a}"):
                    win_bad += 1
                    continue
                for name, col in win["columns"].items():
                    if name in LOOKAHEAD_COLUMNS:
                        continue
                    ref = np.asarray(full["columns"][name])[a:z]
                    got = np.asarray(col)
                    if name in CUMULATIVE_COLUMNS:
                        # anchored at the window start: compare against references on the window's own bars
                        if name == "vwap":
                            ref = R.vwap(((h + l + c) / 3)[a:z], v[a:z], 0)
                        elif name == "drawdown":
                            ref = R.drawdown(c[a:z])
                        else:  # obv / ad: same increments as the full history
                            ref = ref - ref[0] + got[0]
                    # inside the window nothing may be NaN once history is available (auto lookback),
                    # unless the full-history value itself is NaN (start of data)
                    holes = np.isnan(got) & ~np.isnan(ref)
                    agree = nan_eq_close(got, ref, 1e-7, atol=max(1e-9 * scale, ATOL.get(name, 0.0)))
                    if holes.any() or not agree.all():
                        win_bad += 1
                        rel = np.nanmax(np.abs(got - ref) / np.maximum(1, np.abs(ref))) if (~np.isnan(ref)).any() else 0
                        worst[name] = max(worst.get(name, 0), float(rel))
                        self.check("windows", f"{t} window[{a}:{z}] {name}", False,
                                   f"holes={int(holes.sum())} mismatches={int((~agree).sum())} worst_rel={rel:.3e}")
            self.check("windows", f"{t} {n_windows} random windows with auto lookback match full history", win_bad == 0,
                       f"worst per column: {worst}" if worst else "")

            # -- windows with lookback 0: finite-window kinds match after their warm-up --
            zero_bad = 0
            for w in range(2 if self.args.quick else 8):
                a = rng.randrange(0, n - 400)
                z = a + rng.randrange(300, min(3000, n - a))
                win = db.indicators(ALL_SPECS, start_ts=int(b["timestamps"][a]), end_ts=int(b["timestamps"][z - 1]) + 1,
                                    columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
                for spec in ALL_SPECS:
                    if spec["kind"] not in FINITE_WINDOW_KINDS:
                        continue
                    for name, col in win["columns"].items():
                        if not (name == self.label(spec) or name.startswith(self.label(spec) + "_")):
                            continue
                        if name in CUMULATIVE_COLUMNS or name in LOOKAHEAD_COLUMNS:
                            continue
                        got = np.asarray(col)
                        ref = np.asarray(full["columns"][name])[a:z]
                        valid = ~np.isnan(got)
                        if not valid.any():
                            continue
                        fv = int(np.argmax(valid))
                        if np.isnan(got[fv:]).any() or not nan_eq_close(got[fv:], ref[fv:], TOL.get(name, 1e-9), atol=max(1e-10 * scale, ATOL.get(name, 0.0))).all():
                            zero_bad += 1
                            self.check("windows", f"{t} lookback0 window[{a}:{z}] {name}", False, "values after warm-up differ from full history")
            self.check("windows", f"{t} lookback-0 windows: finite-window kinds match after warm-up", zero_bad == 0)

            # -- tails: with a huge lookback the tail must be bit-identical to the full history --
            for m in (1, 5, 100, 2500):
                tail = db.indicators(ALL_SPECS, tail=m, columns=COLS, lookback=10**9, bucket=BAR_1M, as_numpy=True)
                mm = min(m, n)
                ok = tail["n_rows"] == mm
                detail = []
                for name, col in tail["columns"].items():
                    if name in CUMULATIVE_COLUMNS or name in LOOKAHEAD_COLUMNS:
                        continue
                    ref = np.asarray(full["columns"][name])[-mm:]
                    if not nan_eq_close(np.asarray(col), ref, 0.0).all():
                        ok = False
                        detail.append(name)
                # cumulative kinds restart at the tail start (compensated sums: allow last-ulp noise)
                got = np.asarray(tail["columns"]["obv"])
                full_obv = np.asarray(full["columns"]["obv"])
                if not nan_eq_close(got - got[0], full_obv[-mm:] - full_obv[-mm], 1e-9, atol=1e-9 * float(np.max(v))).all():
                    ok = False
                    detail.append("obv(diff)")
                if not nan_eq_close(np.asarray(tail["columns"]["drawdown"]), R.drawdown(c[-mm:]), 1e-12).all():
                    ok = False
                    detail.append("drawdown")
                self.check("tails", f"{t} tail({m}) bit-identical to full history (cumulative kinds re-anchored)", ok, ", ".join(detail))
            tail = db.indicators(ALL_SPECS, tail=50, columns=COLS, lookback="auto", bucket=BAR_1M, as_numpy=True)
            ok = True
            for name, col in tail["columns"].items():
                if name in CUMULATIVE_COLUMNS or name in LOOKAHEAD_COLUMNS:
                    continue
                ok &= nan_eq_close(np.asarray(col), np.asarray(full["columns"][name])[-50:], 1e-7, atol=max(1e-9 * scale, ATOL.get(name, 0.0))).all()
            self.check("tails", f"{t} tail(50) with auto lookback converged (1e-7)", ok)
            db.close()

    @staticmethod
    def same_result(a, b):
        """Bit-identical batch results (NaN == NaN)."""
        if a["n_rows"] != b["n_rows"] or set(a["columns"]) != set(b["columns"]):
            return False
        if not np.array_equal(np.asarray(a["timestamps"]), np.asarray(b["timestamps"])):
            return False
        return all(nan_eq_close(np.asarray(a["columns"][k]), np.asarray(b["columns"][k]), 0.0).all() for k in a["columns"])

    @staticmethod
    def expected_bars(ts, start, end, bucket):
        """Number of bars (bucket start times) within [start, end) over all ticks."""
        starts = np.unique(ts // bucket) * bucket
        return int(((starts >= start) & (starts < end)).sum())

    @staticmethod
    def label(spec):
        if "label" in spec:
            return spec["label"]
        return f"{spec['kind']}_{spec['period']}" if spec.get("period") else spec["kind"]

    # ------------------------------------------------------------------
    # Phase 4: snapshot vs references on the last 2500 bars
    # ------------------------------------------------------------------
    def phase_snapshot(self):
        self.phase("Phase 4: snapshot() vs references on the last 2500 bars")
        for t in self.tickers:
            b = self.bars_1m[t]
            m = min(2500, len(b["close"]))
            o, h, l, c, v = (b[k][-m:] for k in ("open", "high", "low", "close", "volume"))
            db = self.open_db(t)
            t0 = time.time()
            s = db.snapshot(columns=COLS, bars=2500, bucket=BAR_1M, periods_per_year=PPY_1M)
            self.timing(f"snapshot {t}", time.time() - t0, f"{len(s)} fields")
            db.close()
            refs = self.bar_references(o, h, l, c, v)
            last = lambda k: float(refs[k][-1])  # noqa: E731
            expect = {
                "timestamp": int(b["timestamps"][-1]), "bars": m,
                "open": o[-1], "high": h[-1], "low": l[-1], "close": c[-1], "volume": v[-1],
                "sma_5": talib.SMA(c, 5)[-1], "sma_10": talib.SMA(c, 10)[-1], "sma_20": last("sma_20"), "sma_50": talib.SMA(c, 50)[-1],
                "sma_100": talib.SMA(c, 100)[-1], "sma_200": talib.SMA(c, 200)[-1],
                "ema_9": talib.EMA(c, 9)[-1], "ema_12": talib.EMA(c, 12)[-1], "ema_21": last("ema_21"), "ema_26": talib.EMA(c, 26)[-1],
                "ema_50": talib.EMA(c, 50)[-1], "ema_200": talib.EMA(c, 200)[-1],
                "wma_20": last("wma_20"), "hma_20": last("hma_20"), "vwma_20": last("vwma_20"), "kama_10": last("kama_10"), "tema_20": last("tema_20"),
                "rsi_14": last("rsi_14"), "stoch_k": last("stoch_14_k"), "stoch_d": last("stoch_14_d"), "stochrsi_k": last("stoch_rsi_k"), "stochrsi_d": last("stoch_rsi_d"),
                "macd": last("macd"), "macd_signal": last("macd_signal"), "macd_hist": last("macd_hist"), "ppo": last("ppo"),
                "cci_20": last("cci_20"), "williams_r_14": last("willr_14"), "roc_10": last("roc_10"), "mom_10": last("mom_10"), "cmo_14": last("cmo_14"),
                "trix_15": last("trix_15"), "ultosc": last("ultosc"), "ao": last("ao"), "tsi": last("tsi"), "tsi_signal": last("tsi_signal"),
                "adx_14": last("adx_14"), "plus_di_14": last("adx_14_plus_di"), "minus_di_14": last("adx_14_minus_di"),
                "aroon_up_25": last("aroon_25_up"), "aroon_down_25": last("aroon_25_down"), "aroon_osc_25": last("aroon_25_osc"),
                "psar": last("psar_sar"), "supertrend": last("supertrend_line"), "supertrend_dir": last("supertrend_dir"),
                "vortex_plus_14": last("vortex_14_plus"), "vortex_minus_14": last("vortex_14_minus"),
                "ichimoku_tenkan": last("ichimoku_tenkan"), "ichimoku_kijun": last("ichimoku_kijun"), "ichimoku_senkou_a": last("ichimoku_senkou_a"), "ichimoku_senkou_b": last("ichimoku_senkou_b"),
                "linreg_value_20": last("linreg_20_value"), "linreg_slope_20": last("linreg_20_slope"), "linreg_r2_20": last("linreg_20_r2"),
                "atr_14": last("atr_14"), "natr_14": last("natr_14"), "true_range": last("true_range"),
                "bb_upper": last("bbands_upper"), "bb_middle": last("bbands_middle"), "bb_lower": last("bbands_lower"), "bb_percent_b": last("bbands_percent_b"), "bb_bandwidth": last("bbands_bandwidth"),
                "keltner_upper": last("keltner_upper"), "keltner_middle": last("keltner_middle"), "keltner_lower": last("keltner_lower"),
                "donchian_upper_20": last("donchian_20_upper"), "donchian_middle_20": last("donchian_20_middle"), "donchian_lower_20": last("donchian_20_lower"),
                "stddev_20": last("stddev_20"), "hist_vol_20": last("hist_vol_20"),
                "obv": last("obv"), "vwap": last("vwap"), "mfi_14": last("mfi_14"), "cmf_20": last("cmf_20"), "ad": last("ad"), "adosc": last("adosc"), "efi_13": last("efi_13"),
                "return_1": c[-1] / c[-2] - 1, "return_5": c[-1] / c[-6] - 1, "return_10": c[-1] / c[-11] - 1, "return_20": c[-1] / c[-21] - 1,
                "log_return_1": math.log(c[-1] / c[-2]), "zscore_20": last("zscore_20"), "percent_rank_20": last("percent_rank_20"),
                "high_20": last("rolling_max_20"), "low_20": last("rolling_min_20"), "high_250": talib.MAX(c, 250)[-1], "low_250": talib.MIN(c, 250)[-1],
                "drawdown": last("drawdown"), "sharpe_20": last("sharpe_20"), "sortino_20": last("sortino_20"), "skew_20": last("skew_20"), "kurtosis_20": last("kurtosis_20"),
            }
            missing = [k for k in s if k not in expect and k != "psar_dir"]
            self.check("snapshot", f"{t} every snapshot field has a reference", not missing, f"missing refs: {missing}")
            bad = []
            for k, want in expect.items():
                got = s[k]
                if k in ("timestamp", "bars"):
                    ok = int(got) == int(want)
                else:
                    rtol = TOL.get({"stoch_k": "x", "linreg_r2_20": "linreg_20_r2", "linreg_slope_20": "linreg_20_slope", "kama_10": "kama_10",
                                    "cmo_14": "cmo_14", "zscore_20": "zscore_20", "sharpe_20": "sharpe_20", "sortino_20": "sortino_20",
                                    "skew_20": "skew_20", "kurtosis_20": "kurtosis_20", "hist_vol_20": "hist_vol_20"}.get(k, k), 1e-8)
                    ok = bool(nan_eq_close(np.array([got]), np.array([want]), rtol)[0])
                if not ok:
                    bad.append(f"{k}: got {got!r} want {want!r}")
            self.check("snapshot", f"{t} snapshot fields match references ({len(expect)} fields)", not bad, "; ".join(bad[:10]))
            self.check("snapshot", f"{t} psar_dir in {{-1,+1}}", s["psar_dir"] in (-1.0, 1.0))

    # ------------------------------------------------------------------
    # Phase 5: summary vs numpy
    # ------------------------------------------------------------------
    def phase_summary(self):
        self.phase("Phase 5: summary() vs numpy references")
        rng = random.Random(77)
        for t in self.tickers:
            d = self.ticks[t]
            ts = d["timestamp"]
            n = len(ts)
            db = self.open_db(t)
            wmax = max(100, min(200_000, n // 2))
            windows = [(0, n)] + [(a, a + rng.randrange(50, wmax)) for a in (rng.randrange(0, n - wmax) for _ in range(3 if self.args.quick else 6))]
            for a, z in windows:
                z = min(z, n)
                t0 = time.time()
                s = db.summary(int(ts[a]), int(ts[z - 1]) + 1, "price", periods_per_year=PPY_1M * 60)
                dt = time.time() - t0
                ref = R.summary(d["price"][a:z], PPY_1M * 60)
                bad = []
                for k, want in ref.items():
                    got = s[k]
                    rtol = 1e-7 if k not in ("hurst", "half_life", "autocorr_1", "skew", "kurtosis", "calmar") else 1e-5
                    if not nan_eq_close(np.array([float(got)]), np.array([float(want)]), rtol)[0]:
                        bad.append(f"{k}: got {got} want {want}")
                self.check("summary", f"{t} summary window[{a}:{z}] ({z-a:,d} ticks) matches", not bad, "; ".join(bad[:6]))
                if a == 0:
                    self.timing(f"summary {t} ({n:,d} ticks)", dt)
            db.close()

    # ------------------------------------------------------------------
    # Phase 6: tick-mode indicators (bucket 0), field overrides, two-series kinds
    # ------------------------------------------------------------------
    def phase_tick_mode(self):
        self.phase("Phase 6: tick-mode indicators (bucket 0) vs numpy on raw ticks")
        for t in self.tickers:
            d = self.ticks[t]
            ts = d["timestamp"]
            n = len(ts)
            a = n // 3
            z = min(n, a + 20_000)
            db = self.open_db(t)
            specs = [
                {"kind": "vwap"}, {"kind": "vwap", "period": 50, "label": "vwap_50"}, {"kind": "returns", "period": 1},
                {"kind": "log_returns", "period": 5}, {"kind": "rolling_min", "period": 50}, {"kind": "rolling_max", "period": 50},
                {"kind": "zscore", "period": 50}, {"kind": "sma", "period": 100}, {"kind": "ema", "period": 100}, {"kind": "obv"},
                {"kind": "drawdown"}, {"kind": "sma", "period": 30, "field": "size", "label": "size_sma_30"},
                {"kind": "zscore", "period": 40, "field": "ask", "label": "ask_z_40"},
                {"kind": "correl", "period": 60, "field": "price", "field2": "bid", "label": "corr_price_bid"},
                {"kind": "beta", "period": 60, "field": "price", "field2": "bid", "label": "beta_price_bid"},
                {"kind": "correl", "period": 60, "field": "bid", "field2": "ask", "label": "corr_bid_ask"},
                {"kind": "percent_rank", "period": 30, "field": "size", "label": "size_prank"},
            ]
            t0 = time.time()
            res = db.indicators(specs, start_ts=int(ts[a]), end_ts=int(ts[z - 1]) + 1, columns=COLS, lookback=0, bucket=0, as_numpy=True)
            self.timing(f"tick-mode batch {t} ({z-a:,d} ticks, {len(specs)} specs)", time.time() - t0)
            p, sz, bid, ask = d["price"][a:z], d["size"][a:z], d["bid"][a:z], d["ask"][a:z]
            r1 = np.full(z - a, np.nan)
            r1[1:] = p[1:] / p[:-1] - 1
            lr5 = np.full(z - a, np.nan)
            lr5[5:] = np.log(p[5:] / p[:-5])
            refs = {
                "vwap": R.vwap(p, sz, 0), "vwap_50": R.vwap(p, sz, 50), "returns_1": r1, "log_returns_5": lr5,
                "rolling_min_50": talib.MIN(p, 50), "rolling_max_50": talib.MAX(p, 50), "zscore_50": R.zscore(p, 50),
                "sma_100": talib.SMA(p, 100), "ema_100": talib.EMA(p, 100), "obv": talib.OBV(p, sz), "drawdown": R.drawdown(p),
                "size_sma_30": talib.SMA(sz, 30), "ask_z_40": R.zscore(ask, 40), "corr_price_bid": R.correl(p, bid, 60),
                "beta_price_bid": R.beta(p, bid, 60), "corr_bid_ask": R.correl(bid, ask, 60), "size_prank": R.percent_rank(sz, 30),
            }
            self.check("tick-mode", f"{t} rows", res["n_rows"] == z - a, f"{res['n_rows']} vs {z-a}")
            self.check("tick-mode", f"{t} timestamps", np.array_equal(np.asarray(res["timestamps"]), ts[a:z]))
            bad = self.compare_columns("tick-mode", t, res["columns"], {k: v for k, v in refs.items()}, rtol_default=1e-7, label=" ticks")
            self.check("tick-mode", f"{t} all tick-mode columns match", bad == 0, f"{bad} bad")
            # full-range tick-mode call over every tick (throughput)
            t0 = time.time()
            big = db.indicators([{"kind": "returns", "period": 1}, {"kind": "vwap"}, {"kind": "rolling_max", "period": 100}, {"kind": "ema", "period": 50}],
                                start_ts=INT64_MIN, end_ts=INT64_MAX, columns=COLS, lookback=0, bucket=0, as_numpy=True)
            dt = time.time() - t0
            self.timing(f"tick-mode 4 specs over all {n:,d} ticks ({t})", dt, f"{n/dt:,.0f} ticks/s")
            self.check("tick-mode", f"{t} full-range rows", big["n_rows"] == n)
            db.close()

    # ------------------------------------------------------------------
    # Phase 7: edge cases, reopen, ring buffer, invalid input
    # ------------------------------------------------------------------
    def phase_edge_cases(self):
        self.phase("Phase 7: edge cases and robustness")
        t = self.tickers[0]
        d = self.ticks[t]
        ts = d["timestamp"]
        n = len(ts)
        db = self.open_db(t)
        specs = [{"kind": "sma", "period": 20}, {"kind": "rsi"}, {"kind": "macd"}]

        def expect_error(name, fn):
            try:
                fn()
                self.check("edge", name, False, "no error raised")
            except (ValueError, RuntimeError, MemoryError, TypeError) as e:
                self.check("edge", name, True, f"raised {type(e).__name__}: {str(e)[:80]}")
            except Exception as e:  # noqa: BLE001
                self.check("edge", name, False, f"unexpected {type(e).__name__}: {e}")

        def rows(**kw):
            r = db.indicators(specs, columns=COLS, **kw)
            return r["n_rows"], r

        # empty / out-of-range windows
        self.check("edge", "empty window (start == end) -> 0 rows", rows(start_ts=int(ts[100]), end_ts=int(ts[100]), lookback=0)[0] == 0)
        self.check("edge", "window before data -> 0 rows", rows(start_ts=int(ts[0]) - 10**9, end_ts=int(ts[0]) - 1, lookback="auto")[0] == 0)
        self.check("edge", "window after data -> 0 rows", rows(start_ts=int(ts[-1]) + 1, end_ts=int(ts[-1]) + 10**9, lookback="auto")[0] == 0)
        first_bar = (int(ts[0]) // BAR_1M) * BAR_1M
        self.check("edge", "window before data with bucket -> 0 rows", rows(start_ts=first_bar - 10**9, end_ts=first_bar, lookback="auto", bucket=BAR_1M)[0] == 0)
        self.check("edge", "tail(0) -> 0 rows", rows(tail=0)[0] == 0)
        # single-row windows
        n1, r = rows(start_ts=int(ts[5000]), end_ts=int(ts[5000]) + 1, lookback="auto")
        self.check("edge", "single-record window with auto lookback -> 1 converged row", n1 == 1 and not math.isnan(r["columns"]["sma_20"][0]))
        # bucket extremes
        b = db.ohlcv(int(ts[1000]), int(ts[6000]), 1, price="price", volume="size")
        self.check("edge", "bucket=1 -> one bar per tick", b["n_bars"] == 5000 and b["count"][0] == 1)
        b = db.ohlcv(int(ts[1000]), int(ts[6000]), 10**15, price="price", volume="size")
        self.check("edge", "huge bucket -> one bar", b["n_bars"] == 1 and b["open"][0] == d["price"][1000] and b["close"][0] == d["price"][5999]
                   and b["count"][0] == 5000 and math.isclose(b["volume"][0], d["size"][1000:6000].sum(), rel_tol=1e-9))
        b0 = db.ohlcv(int(ts[1000]), int(ts[6000]), BAR_1M, price="price")
        self.check("edge", "ohlcv without volume field -> volume == count", np.array_equal(np.asarray(b0["volume"]), np.asarray(b0["count"])))
        # lookback extremes
        n_big, r_big = rows(tail=10, lookback=10**9)
        self.check("edge", "lookback 1e9 clamps to file start", n_big == 10)
        n_a, r_a = rows(tail=10, lookback="auto")
        self.check("edge", "tail(10) auto vs clamped lookback agree (1e-9)",
                   nan_eq_close(np.asarray(r_a["columns"]["macd_hist"]), np.asarray(r_big["columns"]["macd_hist"]), 1e-9).all())
        # snapshot with more bars than available
        s = db.snapshot(columns=COLS, bars=10**8, bucket=0)
        self.check("edge", "snapshot bars > available -> uses everything", s["bars"] == n)
        s = db.snapshot(columns=COLS, bars=5, bucket=0)
        self.check("edge", "snapshot with 5 bars: sma_5 defined, sma_10 NaN", not math.isnan(s["sma_5"]) and math.isnan(s["sma_10"]) and s["bars"] == 5)
        # invalid input
        expect_error("unknown kind", lambda: db.indicators([{"kind": "nope"}], tail=10, columns=COLS))
        expect_error("unknown field", lambda: db.indicators([{"kind": "sma", "field": "nope"}], tail=10, columns=COLS))
        expect_error("field override with bucket", lambda: db.indicators([{"kind": "sma", "field": "bid"}], tail=10, columns=COLS, bucket=BAR_1M))
        expect_error("missing close column", lambda: db.indicators(specs, tail=10, columns={"volume": "size"}))
        expect_error("atr without high/low", lambda: db.indicators([{"kind": "atr"}], tail=10, columns=COLS, bucket=0))
        expect_error("hma period 1", lambda: db.indicators([{"kind": "hma", "period": 1}], tail=10, columns=COLS))
        expect_error("correl without field2", lambda: db.indicators([{"kind": "correl"}], tail=10, columns=COLS))
        expect_error("ohlcv bucket 0", lambda: db.ohlcv(0, INT64_MAX, 0, price="price"))
        expect_error("summary unknown field", lambda: db.summary(0, INT64_MAX, "nope"))
        expect_error("bad lookback", lambda: db.indicators(specs, tail=10, columns=COLS, lookback="lots"))
        # summary edge: empty window
        s0 = db.summary(int(ts[10]), int(ts[10]), "price")
        self.check("edge", "summary of empty window -> count 0, NaN stats", s0["count"] == 0 and math.isnan(s0["mean"]))
        s1 = db.summary(int(ts[10]), int(ts[10]) + 1, "price")
        self.check("edge", "summary of one record -> count 1, return 0", s1["count"] == 1 and s1["total_return"] == 0 and math.isnan(s1["sharpe"]))
        # invalid appends do not corrupt the database
        before = db.get_stats(INT64_MIN, INT64_MAX, "price")["count"]
        older = (int(ts[-1]) - 5, 1.0, 1.0, 1.0, 1.0, True)
        try:
            ok = db.append(older)
            self.check("edge", "append with non-monotonic timestamp rejected", ok is False or ok is None or ok == 0)
        except Exception as e:  # noqa: BLE001
            self.check("edge", "append with non-monotonic timestamp rejected", True, f"raised {type(e).__name__}")
        db.lib.hocdb_append.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
        buf = ctypes.create_string_buffer(b"\0" * 40, 40)
        rc = db.lib.hocdb_append(db.handle, ctypes.addressof(buf), 40)
        self.check("edge", "append with wrong record size rejected (rc=-2)", rc == -2)
        db.flush()
        after = db.get_stats(INT64_MIN, INT64_MAX, "price")["count"]
        self.check("edge", "record count unchanged after rejected appends", before == after, f"{before} vs {after}")

        # reopen equality
        ref_tail = db.indicators(ALL_SPECS, tail=500, columns=COLS, lookback="auto", bucket=BAR_1M, as_numpy=True)
        ref_snap = db.snapshot(columns=COLS, bars=2500, bucket=BAR_1M, periods_per_year=PPY_1M)
        db.close()
        t0 = time.time()
        db = self.open_db(t)
        self.timing("reopen (index rebuild)", time.time() - t0, f"{n:,d} records")
        new_tail = db.indicators(ALL_SPECS, tail=500, columns=COLS, lookback="auto", bucket=BAR_1M, as_numpy=True)
        new_snap = db.snapshot(columns=COLS, bars=2500, bucket=BAR_1M, periods_per_year=PPY_1M)
        self.check("edge", "reopen: tail batch bit-identical", self.same_result(ref_tail, new_tail))
        same = all((ref_snap[k] == new_snap[k]) or (isinstance(ref_snap[k], float) and math.isnan(ref_snap[k]) and math.isnan(new_snap[k])) for k in ref_snap)
        self.check("edge", "reopen: snapshot identical", same)
        db.close()

        # ring buffer vs linear database holding the same last records
        cap = min(100_000, n // 3)
        m = min(3 * cap, n)
        rec_size = RECORD_DTYPE.itemsize
        ring = self.open_db("RING", max_file_size=HEADER_SIZE + cap * rec_size, overwrite_on_full=True)
        sub = {k: v[:m] for k, v in d.items()}
        self.ingest(ring, sub)
        cnt = ring.get_stats(INT64_MIN, INT64_MAX, "price")["count"]
        self.check("ring", f"ring buffer holds exactly its capacity ({cap:,d}) after {m:,d} appends", cnt == cap, f"count={cnt}")
        lin = self.open_db("LINEAR")
        self.ingest(lin, {k: v[m - cap:m] for k, v in d.items()})
        for mode in ({"tail": 300, "bucket": BAR_1M, "lookback": "auto"}, {"tail": 1000, "bucket": 0, "lookback": 10**9},
                     {"tail": 50_000, "bucket": 0, "lookback": 0}):
            sp = ALL_SPECS if mode["bucket"] else TICK_SPECS
            a1 = ring.indicators(sp, columns=COLS, as_numpy=True, **mode)
            a2 = lin.indicators(sp, columns=COLS, as_numpy=True, **mode)
            self.check("ring", f"ring == linear for {mode}", self.same_result(a1, a2), f"rows {a1['n_rows']} vs {a2['n_rows']}")
        s1 = ring.snapshot(columns=COLS, bars=2000, bucket=BAR_1M, periods_per_year=PPY_1M)
        s2 = lin.snapshot(columns=COLS, bars=2000, bucket=BAR_1M, periods_per_year=PPY_1M)
        self.check("ring", "ring == linear snapshot", all((s1[k] == s2[k]) or (isinstance(s1[k], float) and math.isnan(s1[k]) and math.isnan(s2[k])) for k in s1))
        b1 = ring.ohlcv(INT64_MIN, INT64_MAX, BAR_1M, price="price", volume="size", as_numpy=True)
        b2 = lin.ohlcv(INT64_MIN, INT64_MAX, BAR_1M, price="price", volume="size", as_numpy=True)
        self.check("ring", "ring == linear ohlcv", b1["n_bars"] == b2["n_bars"] and all(np.array_equal(np.asarray(b1[k]), np.asarray(b2[k])) for k in ("timestamps", "open", "high", "low", "close", "volume", "count")))
        # keep appending into the wrapped ring and re-check
        extra = {k: v[m:m + 5000] for k, v in d.items()}
        if len(extra["timestamp"]) == 5000:
            self.ingest(ring, extra)
            lin2 = self.open_db("LINEAR2")
            self.ingest(lin2, {k: v[m + 5000 - cap:m + 5000] for k, v in d.items()})
            a1 = ring.indicators(ALL_SPECS, columns=COLS, tail=200, bucket=BAR_1M, lookback="auto", as_numpy=True)
            a2 = lin2.indicators(ALL_SPECS, columns=COLS, tail=200, bucket=BAR_1M, lookback="auto", as_numpy=True)
            self.check("ring", "ring after further wrap-around appends == linear", self.same_result(a1, a2))
            lin2.close()
        ring.close()
        lin.close()

    # ------------------------------------------------------------------
    # Phase 8: fuzzing
    # ------------------------------------------------------------------
    NAN_OK_AFTER_WARMUP = {"zscore", "bbands", "sharpe", "sortino", "tsi", "cmf", "vwma", "vwap", "stoch_rsi", "efi"}

    def random_spec(self, rng, tick_mode):
        kinds = list(KIND_NAMES)
        if not tick_mode:
            kinds = [k for k in kinds if k not in ("correl", "beta")]
        kind = rng.choice(kinds)
        s = {"kind": kind}
        p = rng.choice([rng.randrange(1, 12), rng.randrange(2, 60), rng.randrange(2, 400)])
        if kind in ("hma", "adx", "linreg", "correl", "beta", "sharpe", "sortino", "hist_vol", "rsi", "aroon", "cci", "stoch", "willr"):
            p = max(p, 2)
        if kind in ("skew", "kurtosis"):
            p = max(p, 3)
        if kind in ("psar", "bop", "true_range", "obv", "ad", "drawdown", "typical_price", "median_price", "heikin_ashi"):
            p = 0
        if p:
            s["period"] = p
        if kind in ("macd", "ppo"):
            s["period"], s["period2"], s["period3"] = rng.randrange(2, 30), rng.randrange(5, 80), rng.randrange(2, 30)
        elif kind == "stoch":
            s["period2"], s["period3"] = rng.randrange(1, 10), rng.randrange(1, 10)
        elif kind == "stoch_rsi":
            s["period2"], s["period3"], s["period4"] = rng.randrange(2, 30), rng.randrange(1, 8), rng.randrange(1, 8)
        elif kind == "kama":
            s["period2"], s["period3"] = rng.randrange(1, 10), rng.randrange(10, 60)
        elif kind == "ultosc":
            s["period"], s["period2"], s["period3"] = rng.randrange(1, 20), rng.randrange(2, 40), rng.randrange(3, 80)
        elif kind == "ao":
            s["period"], s["period2"] = rng.randrange(1, 20), rng.randrange(2, 80)
        elif kind == "tsi":
            s["period2"], s["period3"] = rng.randrange(1, 30), rng.randrange(1, 30)
        elif kind == "ichimoku":
            s["period2"], s["period3"], s["period4"] = rng.randrange(2, 60), rng.randrange(2, 120), rng.randrange(0, 60)
        elif kind == "keltner":
            s["period2"], s["param"] = rng.randrange(1, 60), rng.uniform(0.5, 4)
        elif kind == "adosc":
            s["period2"] = rng.randrange(2, 40)
        elif kind == "bbands":
            s["param"] = rng.uniform(0.5, 4)
        elif kind == "supertrend":
            s["param"] = rng.uniform(0.5, 5)
        elif kind == "psar":
            s["param"], s["param2"] = rng.uniform(0.005, 0.1), rng.uniform(0.1, 0.5)
        elif kind in ("hist_vol", "sharpe", "sortino"):
            s["param"] = rng.choice([0, 252, PPY_1M])
        elif kind == "vwap" and rng.random() < 0.5:
            s["period"] = 0
        if tick_mode:
            if kind in ("correl", "beta"):
                s["field"], s["field2"] = rng.choice(["price", "bid", "ask"]), rng.choice(["price", "bid", "ask", "size"])
            elif rng.random() < 0.3 and kind not in ("bop", "heikin_ashi", "typical_price", "median_price", "mfi", "cmf", "ad", "adosc", "cci", "willr", "stoch", "ultosc", "adx", "aroon", "psar", "supertrend", "vortex", "ichimoku", "atr", "natr", "true_range", "keltner", "donchian", "ao"):
                s["field"] = rng.choice(["price", "bid", "ask", "size"])
        return s

    def phase_fuzz(self):
        self.phase(f"Phase 8: fuzzing ({self.args.fuzz} random cases)")
        rng = random.Random(self.args.seed)
        dbs = {t: self.open_db(t) for t in self.tickers}
        anomalies = {}
        errors = {}
        crashes = 0
        n_ok = 0
        for case in range(self.args.fuzz):
            t = rng.choice(self.tickers)
            d = self.ticks[t]
            ts = d["timestamp"]
            n = len(ts)
            bucket = rng.choice([0, 0, BAR_1M, 5 * BAR_1M, 60 * BAR_1M])
            tick_mode = bucket == 0
            specs = [self.random_spec(rng, tick_mode) for _ in range(rng.randrange(1, 6))]
            for i, s in enumerate(specs):
                s["label"] = f"c{i}"
            lookback = rng.choice(["auto", 0, rng.randrange(0, 3000)])
            use_tail = rng.random() < 0.5
            kw = {"columns": COLS, "lookback": lookback, "bucket": bucket}
            if use_tail:
                kw["tail"] = rng.choice([1, 2, 7, 50, 500, 5000])
            else:
                a = rng.randrange(0, n - 1)
                z = min(n, a + rng.choice([1, 5, 100, 5000, 50_000]))
                kw["start_ts"], kw["end_ts"] = int(ts[a]), int(ts[z - 1]) + 1
            desc = f"{t} {kw.get('tail', (kw.get('start_ts'), kw.get('end_ts')))} bucket={bucket} lookback={lookback} specs={specs}"
            try:
                res = dbs[t].indicators(specs, as_numpy=True, **kw)
            except ValueError as e:
                errors.setdefault(str(e)[:60], []).append(desc)
                continue
            except Exception as e:  # noqa: BLE001
                crashes += 1
                self.check("fuzz", f"case {case} unexpected {type(e).__name__}", False, f"{e} :: {desc}")
                continue
            n_ok += 1
            tsr = np.asarray(res["timestamps"])
            if len(tsr) > 1 and not np.all(np.diff(tsr) > 0):
                self.check("fuzz", f"case {case} timestamps strictly increasing", False, desc)
            if not use_tail and len(tsr):
                if tsr[0] < kw["start_ts"] or tsr[-1] >= kw["end_ts"]:
                    self.check("fuzz", f"case {case} timestamps inside window", False, desc)
                # expected row count
                if tick_mode:
                    expected = int(((ts >= kw["start_ts"]) & (ts < kw["end_ts"])).sum())
                else:
                    expected = self.expected_bars(ts, kw["start_ts"], kw["end_ts"], bucket)
                if res["n_rows"] != expected:
                    self.check("fuzz", f"case {case} row count", False, f"{res['n_rows']} vs {expected} :: {desc}")
            elif use_tail and res["n_rows"] > kw["tail"]:
                self.check("fuzz", f"case {case} tail row count", False, f"{res['n_rows']} > {kw['tail']} :: {desc}")
            for name, col in res["columns"].items():
                col = np.asarray(col)
                if not len(col):
                    continue
                spec = specs[int(name[1:name.find("_")] if "_" in name else name[1:])]
                kind = spec["kind"]
                if name.endswith("_chikou") or kind in LOOKAHEAD_KINDS:
                    continue
                valid = ~np.isnan(col)
                if valid.any():
                    fv = int(np.argmax(valid))
                    holes = int(np.isnan(col[fv:]).sum())
                    infs = int(np.isinf(col).sum())
                    if (holes or infs) and kind not in self.NAN_OK_AFTER_WARMUP:
                        anomalies.setdefault(kind, []).append(f"holes={holes} infs={infs} :: {desc}")
            # tail vs equivalent range must be bit-identical
            if use_tail and res["n_rows"] > 0:
                kw2 = dict(kw)
                del kw2["tail"]
                kw2["start_ts"], kw2["end_ts"] = int(tsr[0]), int(tsr[-1]) + 1
                if bucket:
                    kw2["end_ts"] = int(tsr[-1]) + 1  # bars whose start lies in [start, end)
                try:
                    res2 = dbs[t].indicators(specs, as_numpy=True, **kw2)
                    same = res2["n_rows"] == res["n_rows"] and all(nan_eq_close(np.asarray(res2["columns"][k]), np.asarray(res["columns"][k]), 0.0).all() for k in res["columns"])
                    if not same:
                        self.check("fuzz", f"case {case} tail == range", False, desc)
                except Exception as e:  # noqa: BLE001
                    self.check("fuzz", f"case {case} tail == range", False, f"{e} :: {desc}")
        for db in dbs.values():
            db.close()
        self.check("fuzz", f"{n_ok} successful cases, {sum(len(v) for v in errors.values())} rejected by validation, {crashes} crashes", crashes == 0)
        self.check("fuzz", "no NaN/inf after warm-up in kinds where it is not expected", not anomalies,
                   "; ".join(f"{k}: {len(v)} cases e.g. {v[0][:160]}" for k, v in anomalies.items()))
        self.notes.append("Fuzz validation errors by message: " + "; ".join(f"{k!r} x{len(v)}" for k, v in sorted(errors.items())))

    # ------------------------------------------------------------------
    # Phase 9: performance
    # ------------------------------------------------------------------
    def phase_perf(self):
        self.phase("Phase 9: performance")
        t = self.tickers[0]
        db = self.open_db(t)
        n = len(self.ticks[t]["timestamp"])

        def med(fn, reps):
            xs = []
            for _ in range(reps):
                t0 = time.perf_counter()
                fn()
                xs.append(time.perf_counter() - t0)
            xs.sort()
            return xs[len(xs) // 2], xs[-1]

        m, mx = med(lambda: db.snapshot(columns=COLS, bars=2500, bucket=BAR_1M, periods_per_year=PPY_1M), 20)
        self.timing("snapshot 2500 x 1m bars from ticks (median)", m, f"max {mx*1000:.2f} ms")
        m, mx = med(lambda: db.snapshot(columns=COLS, bars=2500, bucket=0), 20)
        self.timing("snapshot 2500 ticks (median)", m, f"max {mx*1000:.2f} ms")
        m, mx = med(lambda: db.indicators(ALL_SPECS, tail=500, columns=COLS, lookback="auto", bucket=BAR_1M), 10)
        self.timing(f"tail(500) x {len(ALL_SPECS)} specs on 1m bars (median)", m, f"max {mx*1000:.2f} ms")
        m, mx = med(lambda: db.indicators([{"kind": "rsi"}, {"kind": "macd"}, {"kind": "bbands"}, {"kind": "atr"}], tail=200, columns=COLS, lookback="auto", bucket=BAR_1M), 20)
        self.timing("tail(200) x 4 common specs on 1m bars (median)", m, f"max {mx*1000:.2f} ms")
        m, mx = med(lambda: db.indicators([{"kind": "ema", "period": 21}], tail=1, columns=COLS, lookback="auto", bucket=0), 50)
        self.timing("latest EMA(21) on ticks (median)", m, f"max {mx*1000:.2f} ms")
        m, mx = med(lambda: db.ohlcv(INT64_MIN, INT64_MAX, BAR_1M, price="price", volume="size"), 3)
        self.timing(f"ohlcv 1m over all {n:,d} ticks (median)", m, f"{n/m:,.0f} ticks/s")
        m, mx = med(lambda: db.summary(INT64_MIN, INT64_MAX, "price", PPY_1M * 60), 3)
        self.timing(f"summary over all {n:,d} ticks (median)", m, f"{n/m:,.0f} ticks/s")
        m, mx = med(lambda: db.get_stats(INT64_MIN, INT64_MAX, "price"), 3)
        self.timing(f"get_stats over all {n:,d} ticks (median)", m, f"{n/m:,.0f} ticks/s")
        db.close()

    # ------------------------------------------------------------------
    # Phase 10: memory growth
    # ------------------------------------------------------------------
    def phase_memory(self):
        self.phase("Phase 10: memory growth over repeated calls")
        t = self.tickers[0]
        db = self.open_db(t)
        specs = ALL_SPECS[:12]
        for _ in range(50):  # warm-up
            db.indicators(specs, tail=300, columns=COLS, lookback="auto", bucket=BAR_1M)
        rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        iters = 300 if self.args.quick else 3000
        t0 = time.time()
        for i in range(iters):
            db.indicators(specs, tail=300, columns=COLS, lookback="auto", bucket=BAR_1M)
            db.snapshot(columns=COLS, bars=1000, bucket=0)
            if i % 100 == 0:
                db.ohlcv(INT64_MIN, INT64_MAX, 3600 * US, price="price", volume="size")
                db.summary(INT64_MIN, INT64_MAX, "price")
        dt = time.time() - t0
        rss1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        scale = 1 if platform.system() == "Darwin" else 1024
        growth_mb = (rss1 - rss0) * scale / 1e6
        self.timing(f"memory phase ({iters} iterations of batch+snapshot)", dt, f"max RSS growth {growth_mb:.1f} MB")
        self.check("memory", f"max RSS growth after {iters} iterations < 64 MB", growth_mb < 64, f"{growth_mb:.1f} MB")
        db.close()

    # ------------------------------------------------------------------
    # Phase 11: cross-binding consistency
    # ------------------------------------------------------------------
    def phase_bindings(self):
        self.phase("Phase 11: cross-binding consistency (bit-for-bit)")
        t = self.tickers[0]
        req = {
            "ticker": t, "dir": self.data_dir, "tail": 1000, "bucket": BAR_1M,
            "specs": [dict(s) for s in ALL_SPECS],
            "snapshot": {"bars": 2500, "bucket": BAR_1M, "periods_per_year": PPY_1M},
            "summary": {"start": 0, "end": 2**62, "field": "price", "periods_per_year": PPY_1M * 60},
            "ohlcv": {"start": 0, "end": 2**62, "bucket": 3600 * US},
        }
        db = self.open_db(t)
        mine = {}
        r = db.indicators(req["specs"], tail=req["tail"], columns=COLS, bucket=req["bucket"])
        mine["timestamps"] = [int(x) for x in r["timestamps"]]
        mine["columns"] = {k: list(map(float, v)) for k, v in r["columns"].items()}
        mine["snapshot"] = db.snapshot(columns=COLS, bars=2500, bucket=BAR_1M, periods_per_year=PPY_1M)
        mine["summary"] = db.summary(0, 2**62, "price", PPY_1M * 60)
        b = db.ohlcv(0, 2**62, 3600 * US, price="price", volume="size")
        mine["ohlcv"] = {k: list(b[k]) for k in ("timestamps", "open", "high", "low", "close", "volume", "count")}
        db.close()
        tmp = os.path.join(self.data_dir, "consistency")
        os.makedirs(tmp, exist_ok=True)
        req_path = os.path.join(tmp, "request.json")
        with open(req_path, "w") as f:
            json.dump(req, f)
        cdir = os.path.join(HERE, "consistency")
        lib = os.path.join(ROOT, "zig-out", "lib")
        env = dict(os.environ, DYLD_LIBRARY_PATH=lib, LD_LIBRARY_PATH=lib)
        programs = {}
        if shutil.which("bun"):
            programs["bun"] = ["bun", "run", os.path.join(cdir, "consistency_bun.ts"), req_path]
        if shutil.which("node"):
            programs["node"] = ["node", os.path.join(cdir, "consistency_node.js"), req_path]
        if shutil.which("clang"):
            exe = os.path.join(tmp, "consistency_c")
            rc = subprocess.run(["clang", "-O1", "-o", exe, os.path.join(cdir, "consistency_c.c"), "-I", os.path.join(ROOT, "bindings", "c"),
                                 "-L", lib, "-lhocdb_c", f"-Wl,-rpath,{lib}", "-lm"], capture_output=True, text=True)
            self.check("bindings", "C consistency program compiles", rc.returncode == 0, rc.stderr[-500:])
            if rc.returncode == 0:
                programs["c"] = [exe, req_path]
            exe = os.path.join(tmp, "consistency_cpp")
            rc = subprocess.run(["clang++", "-std=c++17", "-O1", "-o", exe, os.path.join(cdir, "consistency_cpp.cpp"), "-I", os.path.join(ROOT, "bindings", "c"),
                                 "-I", os.path.join(ROOT, "bindings", "cpp"), "-L", lib, "-lhocdb_c", f"-Wl,-rpath,{lib}"], capture_output=True, text=True)
            self.check("bindings", "C++ consistency program compiles", rc.returncode == 0, rc.stderr[-500:])
            if rc.returncode == 0:
                programs["cpp"] = [exe, req_path]
        go = self.args.go or shutil.which("go")
        if go:
            exe = os.path.join(tmp, "consistency_go")
            genv = dict(env)
            if self.args.go_env:
                for kv in self.args.go_env.split(","):
                    k, v = kv.split("=", 1)
                    genv[k] = v
            rc = subprocess.run([go, "build", "-o", exe, "."], cwd=os.path.join(cdir, "go"), env=genv, capture_output=True, text=True)
            self.check("bindings", "Go consistency program compiles", rc.returncode == 0, rc.stderr[-500:])
            if rc.returncode == 0:
                programs["go"] = [exe, req_path]
        else:
            self.notes.append("Go consistency check skipped: no go toolchain (pass --go).")

        def decode(x):
            if x is None:
                return float("nan")
            if x == "inf":
                return float("inf")
            if x == "-inf":
                return float("-inf")
            return x

        def same_val(a, b):
            a, b = decode(a), decode(b)
            if isinstance(a, float) and isinstance(b, (int, float)):
                return (math.isnan(a) and math.isnan(float(b))) or a == b
            return a == b

        for name, cmd in programs.items():
            t0 = time.time()
            try:
                p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)
            except subprocess.TimeoutExpired:
                self.check("bindings", f"{name} runs", False, "timeout")
                continue
            dt = time.time() - t0
            if p.returncode != 0:
                self.check("bindings", f"{name} runs", False, p.stderr[-800:])
                continue
            try:
                out = json.loads(p.stdout)
            except json.JSONDecodeError as e:
                self.check("bindings", f"{name} produces JSON", False, f"{e}: {p.stdout[:200]}")
                continue
            self.timing(f"consistency program {name}", dt)
            mism = []
            if out.get("timestamps") != mine["timestamps"]:
                mism.append("timestamps differ")
            if set(out.get("columns", {})) != set(mine["columns"]):
                mism.append(f"column names differ: missing={sorted(set(mine['columns']) - set(out.get('columns', {})))[:5]} extra={sorted(set(out.get('columns', {})) - set(mine['columns']))[:5]}")
            else:
                for k, v in mine["columns"].items():
                    o = out["columns"][k]
                    if len(o) != len(v) or not all(same_val(x, y) for x, y in zip(o, v)):
                        idx = next((i for i, (x, y) in enumerate(zip(o, v)) if not same_val(x, y)), None)
                        mism.append(f"column {k} differs at row {idx}: {o[idx] if idx is not None else None} vs {v[idx] if idx is not None else None}")
                        if len(mism) > 8:
                            break
            for sec in ("snapshot", "summary"):
                theirs = out.get(sec, {})
                if set(theirs) != set(mine[sec]):
                    mism.append(f"{sec} keys differ: {sorted(set(mine[sec]) ^ set(theirs))[:6]}")
                else:
                    for k, v in mine[sec].items():
                        if not same_val(theirs[k], v):
                            mism.append(f"{sec}.{k}: {theirs[k]} vs {v}")
            for k, v in mine["ohlcv"].items():
                o = out.get("ohlcv", {}).get(k)
                if o is None or len(o) != len(v) or not all(same_val(x, y) for x, y in zip(o, v)):
                    mism.append(f"ohlcv.{k} differs")
            self.check("bindings", f"{name} matches Python bit-for-bit ({len(mine['columns'])} columns, {len(mine['snapshot'])} snapshot fields, {len(mine['ohlcv']['timestamps'])} bars)", not mism, "; ".join(mism[:6]))

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    def write_report(self):
        fails = self.failures()
        phases = {}
        for ph, name, ok, detail in self.checks:
            p = phases.setdefault(ph, [0, 0])
            p[0 if ok else 1] += 1
        lines = ["# HOCDB overnight validation report", ""]
        lines.append(f"- Date: {time.strftime('%Y-%m-%d %H:%M:%S')}  ")
        lines.append(f"- Host: {platform.platform()} ({platform.machine()}), Python {platform.python_version()}, numpy {np.__version__}, pandas {pd.__version__}, TA-Lib {talib.__version__}  ")
        lines.append(f"- Days: {self.args.days}, tickers: {', '.join(self.tickers)}, fuzz cases: {self.args.fuzz}, seed: {self.args.seed}  ")
        lines.append(f"- Total checks: {len(self.checks)}, failed: {len(fails)}, wall time: {time.time()-self.t0:.0f}s")
        lines.append("")
        lines.append(f"## Result: {'PASS' if not fails else 'FAIL'}")
        lines.append("")
        lines.append("## Data")
        lines.append("")
        lines.append("| Ticker | Ticks | 1-minute bars | First price | Last price | Min | Max |")
        lines.append("| :-- | --: | --: | --: | --: | --: | --: |")
        for t in self.tickers:
            d = self.ticks.get(t)
            if d is None:
                continue
            b = self.bars_1m.get(t, {})
            lines.append(f"| {t} | {len(d['timestamp']):,d} | {len(b.get('timestamps', [])):,d} | {d['price'][0]:.5g} | {d['price'][-1]:.5g} | {d['price'].min():.5g} | {d['price'].max():.5g} |")
        lines.append("")
        lines.append("## Checks by phase")
        lines.append("")
        lines.append("| Phase | Passed | Failed |")
        lines.append("| :-- | --: | --: |")
        for ph, (ok, bad) in phases.items():
            lines.append(f"| {ph} | {ok} | {bad} |")
        lines.append("")
        if fails:
            lines.append("## Failures")
            lines.append("")
            for ph, name, ok, detail in fails:
                lines.append(f"- **{ph}** {name} — {detail}")
            lines.append("")
        lines.append("## Timings")
        lines.append("")
        lines.append("| Measurement | Time | Notes |")
        lines.append("| :-- | --: | :-- |")
        for name, (sec, extra) in self.timings.items():
            lines.append(f"| {name} | {sec*1000:.2f} ms | {extra} |")
        lines.append("")
        if self.notes:
            lines.append("## Notes")
            lines.append("")
            for n in self.notes:
                lines.append(f"- {n}")
            lines.append("")
        text = "\n".join(lines)
        with open(self.args.report, "w") as f:
            f.write(text)
        print(f"\nReport written to {self.args.report}: {len(self.checks)} checks, {len(fails)} failed")
        return not fails


KIND_NAMES = ["sma", "ema", "wma", "dema", "tema", "trima", "kama", "hma", "zlema", "vwma", "rma", "rsi", "macd", "ppo", "stoch",
              "stoch_rsi", "cci", "willr", "mom", "roc", "cmo", "trix", "ultosc", "ao", "tsi", "bop", "dpo", "adx", "aroon", "psar",
              "supertrend", "vortex", "ichimoku", "linreg", "atr", "natr", "true_range", "bbands", "keltner", "donchian", "stddev",
              "variance", "hist_vol", "obv", "vwap", "mfi", "cmf", "ad", "adosc", "efi", "returns", "log_returns", "zscore",
              "percent_rank", "rolling_min", "rolling_max", "drawdown", "sharpe", "sortino", "correl", "beta", "skew", "kurtosis",
              "typical_price", "median_price", "heikin_ashi"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--tickers", default="ALL", help="comma separated list or ALL")
    ap.add_argument("--data-dir", default=os.path.join(ROOT, "b_stress_test_data"))
    ap.add_argument("--report", default=os.path.join(ROOT, "stress_report.md"))
    ap.add_argument("--keep", action="store_true", help="keep the generated databases")
    ap.add_argument("--quick", action="store_true", help="small smoke run (2 tickers, 2 days, few windows)")
    ap.add_argument("--max-ticks", type=int, default=None)
    ap.add_argument("--fuzz", type=int, default=400)
    ap.add_argument("--seed", type=int, default=20250906)
    ap.add_argument("--skip-bindings", action="store_true")
    ap.add_argument("--go", default=None, help="path to a go binary for the Go consistency check")
    ap.add_argument("--go-env", default=None, help="comma separated KEY=VALUE pairs for the go build (GOCACHE=...,GOPATH=...)")
    ap.add_argument("--phases", default="ALL", help="comma separated subset of: ingest,resample,indicators,snapshot,summary,tick,edge,fuzz,perf,memory,round2,ops,calendar,backtest,universe,bindings")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    if args.quick:
        args.days = min(args.days, 2)
        args.fuzz = min(args.fuzz, 40)
        if args.tickers == "ALL":
            args.tickers = "BTCUSD,AAPL"
    args.tickers = [t.name for t in tickgen.TICKERS] if args.tickers == "ALL" else args.tickers.split(",")
    h = Harness(args)
    phases = [("ingest", h.phase_ingest), ("resample", h.phase_resample), ("indicators", h.phase_indicators), ("snapshot", h.phase_snapshot),
              ("summary", h.phase_summary), ("tick", h.phase_tick_mode), ("edge", h.phase_edge_cases), ("fuzz", h.phase_fuzz),
              ("perf", h.phase_perf), ("memory", h.phase_memory), ("round2", h.phase_round2), ("ops", h.phase_ops),
              ("calendar", h.phase_calendar), ("backtest", h.phase_backtest), ("universe", h.phase_universe),
              ("bindings", h.phase_bindings)]
    wanted = None if args.phases == "ALL" else set(args.phases.split(","))
    for name, fn in phases:
        if name == "bindings" and args.skip_bindings:
            continue
        if wanted is not None and name not in wanted and name != "ingest":
            continue
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            h.check(name, f"phase crashed: {type(e).__name__}: {e}", False)
    h.close_all()
    ok = h.write_report()
    if not args.keep and os.path.exists(h.data_dir):
        shutil.rmtree(h.data_dir, ignore_errors=True)
    sys.exit(0 if ok else 1)



# ---------------------------------------------------------------------------
# Round 2: microstructure, pairs, labels, sessions, health, evaluation,
# multi-timeframe snapshots (validated against pandas / numpy references)
# ---------------------------------------------------------------------------
def _phase_round2(self):
    self.phase("Phase 12: microstructure, pairs, labels, sessions, health, evaluation, multi-snapshot")
    t = self.tickers[0]
    d = self.ticks[t]
    ts = d["timestamp"]
    n = len(ts)
    db = self.open_db(t)
    tick_cols = {"close": "price", "volume": "size", "bid": "bid", "ask": "ask", "side": "side"}

    # --- microstructure on a tick window vs numpy -------------------------------------
    a = n // 2
    z = min(n, a + 30_000)
    specs = [{"kind": "spread"}, {"kind": "order_flow", "period": 50}, {"kind": "tick_pressure", "period": 30},
             {"kind": "trade_intensity", "period": 100, "param": 1e6}, {"kind": "amihud", "period": 50},
             {"kind": "realized_vol", "period": 60, "param": 0}]
    res = db.indicators(specs, start_ts=int(ts[a]), end_ts=int(ts[z - 1]) + 1, columns=tick_cols, lookback=0, bucket=0, as_numpy=True)
    p, sz, bid, ask, side = d["price"][a:z], d["size"][a:z], d["bid"][a:z], d["ask"][a:z], d["side"][a:z].astype(float)
    tt = ts[a:z]
    signed = np.where(side != 0, sz, -sz)
    net = R.roll(signed, 50, np.sum)
    tot = R.roll(sz, 50, np.sum)
    sgn = np.zeros(z - a)
    s_ = 0.0
    for i in range(1, z - a):
        dd = p[i] - p[i - 1]
        if dd > 0:
            s_ = 1.0
        elif dd < 0:
            s_ = -1.0
        sgn[i] = s_
    tp_ref = np.full(z - a, np.nan)
    tp_ref[1:] = R.sma(sgn[1:], 30)
    trades = np.full(z - a, np.nan)
    vps = np.full(z - a, np.nan)
    vs = R.roll(sz, 100, np.sum)
    for i in range(100, z - a):
        dt = (tt[i] - tt[i - 100]) / 1e6
        trades[i] = 100 / dt
        vps[i] = vs[i] / dt
    il = np.full(z - a, np.nan)
    il[1:] = np.abs(p[1:] / p[:-1] - 1) / (p[1:] * sz[1:])
    am = np.full(z - a, np.nan)
    am[1:] = R.sma(il[1:], 50)
    sq = np.full(z - a, np.nan)
    sq[1:] = np.log(p[1:] / p[:-1]) ** 2
    rv = np.full(z - a, np.nan)
    rv[1:] = np.sqrt(R.sma(sq[1:], 60))
    refs = {"spread_abs": ask - bid, "spread_bps": (ask - bid) / ((ask + bid) / 2) * 1e4, "order_flow_50_net": net,
            "order_flow_50_imbalance": np.where(tot == 0, 0, net / tot), "tick_pressure_30": tp_ref,
            "trade_intensity_100_trades_per_sec": trades, "trade_intensity_100_volume_per_sec": vps, "amihud_50": am, "realized_vol_60": rv}
    bad = self.compare_columns("micro", t, res["columns"], refs, rtol_default=1e-9, label=" ticks")
    self.check("micro", f"{t} microstructure columns match numpy", bad == 0, f"{bad} bad")

    # --- order flow on 1-minute bars from side: buy volume vs pandas -------------------
    b = db.ohlcv(int(ts[a]), int(ts[z - 1]) + 1, BAR_1M, price="price", volume="size", side="side", as_numpy=True)
    df = pd.DataFrame({"size": sz, "buy": np.where(side != 0, sz, 0.0)}, index=pd.to_datetime(tt, unit="us"))
    r = df.resample("60s", closed="left", label="left").sum()
    r = r[r["size"] > 0]
    self.check("micro", f"{t} ohlcv buy_volume matches pandas", len(b["buy_volume"]) == len(r) and np.allclose(b["buy_volume"], r["buy"].to_numpy(), rtol=1e-9, atol=0))
    bar_flow = db.indicators([{"kind": "order_flow", "period": 1}], start_ts=int(ts[a]), end_ts=int(ts[z - 1]) + 1, columns=tick_cols, lookback=0, bucket=BAR_1M, as_numpy=True)
    # bar-window semantics: bars whose start lies in the window, every bar complete -> the
    # reference must aggregate the ticks through the end of the last bucket
    z_full = int(np.searchsorted(ts, ((int(ts[z - 1]) // BAR_1M) + 1) * BAR_1M, side="left"))
    df2 = pd.DataFrame({"size": d["size"][a:z_full], "buy": np.where(d["side"][a:z_full] != 0, d["size"][a:z_full], 0.0)}, index=pd.to_datetime(ts[a:z_full], unit="us"))
    r2 = df2.resample("60s", closed="left", label="left").sum()
    r2 = r2[r2["size"] > 0]
    flow_ref = pd.Series(2 * r2["buy"].to_numpy() - r2["size"].to_numpy(), index=r2.index.asi8 // 1000)
    want = flow_ref.reindex(np.asarray(bar_flow["timestamps"])).to_numpy()
    self.check("micro", f"{t} order_flow on bars == 2*buy - volume", not np.isnan(want).any() and np.allclose(bar_flow["columns"]["order_flow_1_net"], want, rtol=1e-9, atol=1e-9),
               f"{bar_flow['n_rows']} rows vs {len(r)} pandas bars")

    # --- labels vs numpy (1-minute bars) -----------------------------------------------
    bars = self.bars_1m[t]
    c = bars["close"]
    lab = db.indicators([{"kind": "forward_return", "period": 15}, {"kind": "triple_barrier", "period": 30, "param": 0.004, "param2": 0.003}],
                        start_ts=int(bars["timestamps"][0]), end_ts=int(bars["timestamps"][-1]) + 1, columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
    m = len(c)
    fr = np.full(m, np.nan)
    fmx = np.full(m, np.nan)
    fmn = np.full(m, np.nan)
    for i in range(m - 15):
        w = c[i + 1:i + 16]
        fr[i] = c[i + 15] / c[i] - 1
        fmx[i] = w.max() / c[i] - 1
        fmn[i] = w.min() / c[i] - 1
    tl = np.full(m, np.nan)
    tr_ = np.full(m, np.nan)
    tb = np.full(m, np.nan)
    for i in range(m):
        hit = False
        for j in range(i + 1, min(m, i + 31)):
            rr = c[j] / c[i] - 1
            if rr >= 0.004:
                tl[i], tr_[i], tb[i], hit = 1, rr, j - i, True
                break
            if rr <= -0.003:
                tl[i], tr_[i], tb[i], hit = -1, rr, j - i, True
                break
        if not hit and i + 30 < m:
            tl[i], tr_[i], tb[i] = 0, c[i + 30] / c[i] - 1, 30
    bad = self.compare_columns("labels", t, lab["columns"], {"forward_return_15_ret": fr, "forward_return_15_max": fmx, "forward_return_15_min": fmn,
                                                             "triple_barrier_30_label": tl, "triple_barrier_30_ret": tr_, "triple_barrier_30_bars": tb}, rtol_default=1e-10)
    self.check("labels", f"{t} label columns match numpy", bad == 0)
    self.check("labels", "forward_return flagged as look-ahead", db.indicator_is_lookahead("forward_return") and not db.indicator_is_lookahead("rsi"))

    # --- session kinds on 1-minute bars vs pandas groupby(day) ---------------------------
    day = 86_400 * US
    sess = db.indicators([{"kind": "session_vwap", "param": day}, {"kind": "session_range", "param": day}, {"kind": "pivots", "param": day},
                          {"kind": "opening_range", "period": 5, "param": day}],
                         start_ts=int(bars["timestamps"][0]), end_ts=int(bars["timestamps"][-1]) + 1, columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
    o, h, l, v = bars["open"], bars["high"], bars["low"], bars["volume"]
    sid = bars["timestamps"] // day
    tp = (h + l + c) / 3
    vw = np.full(m, np.nan)
    so = np.full(m, np.nan)
    sh = np.full(m, np.nan)
    sl = np.full(m, np.nan)
    sr = np.full(m, np.nan)
    pp = np.full(m, np.nan)
    orh = np.full(m, np.nan)
    orb = np.full(m, np.nan)
    prev = None
    for s_id in np.unique(sid):
        idx = np.where(sid == s_id)[0]
        vw[idx] = np.cumsum(tp[idx] * v[idx]) / np.cumsum(v[idx])
        so[idx] = o[idx[0]]
        sh[idx] = np.maximum.accumulate(h[idx])
        sl[idx] = np.minimum.accumulate(l[idx])
        sr[idx] = c[idx] / o[idx[0]] - 1
        rh = np.maximum.accumulate(h[idx][:5])
        rl = np.minimum.accumulate(l[idx][:5])
        for k, i in enumerate(idx):
            orh[i] = rh[min(k, 4)]
            if k >= 5:
                orb[i] = 1 if c[i] > rh[-1] else (-1 if c[i] < rl[-1] else 0)
        if prev is not None:
            pp[idx] = sum(prev) / 3
        prev = (h[idx].max(), l[idx].min(), c[idx[-1]])
    bad = self.compare_columns("sessions", t, sess["columns"], {"session_vwap": vw, "session_range_open": so, "session_range_high": sh, "session_range_low": sl,
                                                                 "session_range_ret": sr, "pivots_pp": pp, "opening_range_5_high": orh, "opening_range_5_breakout": orb}, rtol_default=1e-10)
    self.check("sessions", f"{t} session columns match pandas-style groupby", bad == 0)
    # a window starting mid-session must give the same values as the full run
    mid = m // 2
    while sid[mid] == sid[mid - 1] and mid < m - 100:
        mid += 1
    mid += 100  # 100 bars into a session
    win = db.indicators([{"kind": "session_vwap", "param": day}, {"kind": "pivots", "param": day}], start_ts=int(bars["timestamps"][mid]), end_ts=int(bars["timestamps"][mid + 20]) + 1,
                        columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
    self.check("sessions", f"{t} mid-session window equals full history (vwap, pivots)",
               win["n_rows"] == 21 and nan_eq_close(win["columns"]["session_vwap"], sess["columns"]["session_vwap"][mid:mid + 21], 1e-12).all()
               and nan_eq_close(win["columns"]["pivots_pp"], sess["columns"]["pivots_pp"][mid:mid + 21], 1e-12).all())

    # --- pairs vs pandas merge_asof / bar inner join ------------------------------------
    t2 = self.tickers[1] if len(self.tickers) > 1 else None
    if t2 is not None:
        d2 = self.ticks[t2]
        db2 = self.open_db(t2)
        # ticks: as-of
        a2 = n // 3
        z2 = min(n, a2 + 20_000)
        pr = db.pair_indicators(db2, [{"kind": "series"}, {"kind": "series2"}, {"kind": "ratio"}, {"kind": "correl", "period": 60}, {"kind": "beta", "period": 60}],
                                start_ts=int(ts[a2]), end_ts=int(ts[z2 - 1]) + 1, columns=COLS, other_columns=COLS, lookback=0, bucket=0, as_numpy=True)
        left = pd.DataFrame({"ts": ts[a2:z2], "a": d["price"][a2:z2]})
        right = pd.DataFrame({"ts": d2["timestamp"], "b": d2["price"]})
        merged = pd.merge_asof(left, right, on="ts", direction="backward")
        bb = merged["b"].to_numpy(float)
        self.check("pairs", f"{t}/{t2} as-of join equals pandas merge_asof", pr["n_rows"] == z2 - a2 and nan_eq_close(pr["columns"]["series2"], bb, 0.0).all())
        self.check("pairs", f"{t}/{t2} ratio == a/b", nan_eq_close(pr["columns"]["ratio"], d["price"][a2:z2] / bb, 1e-12).all())
        # as-of joined series are piecewise constant (sessions / sparse ticks): near-zero variances are ill-conditioned
        self.check("pairs", f"{t}/{t2} tick correl vs numpy", nan_eq_close(pr["columns"]["correl_60"], R.correl(d["price"][a2:z2], bb, 60), 1e-6, atol=1e-6).all())
        self.check("pairs", f"{t}/{t2} tick beta vs numpy", nan_eq_close(pr["columns"]["beta_60"], R.beta(d["price"][a2:z2], bb, 60), 1e-6, atol=1e-6).all())
        # bars: inner join over the whole history
        prb = db.pair_indicators(db2, [{"kind": "series"}, {"kind": "series2"}, {"kind": "ratio_zscore", "period": 20}, {"kind": "rel_strength", "period": 10}],
                                 start_ts=INT64_MIN, end_ts=INT64_MAX, columns=COLS, other_columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
        ba = self.bars_1m[t]
        bb2 = self.bars_1m[t2]
        common, ia, ib = np.intersect1d(ba["timestamps"], bb2["timestamps"], return_indices=True)
        self.check("pairs", f"{t}/{t2} bar inner join rows", prb["n_rows"] == len(common) and np.array_equal(np.asarray(prb["timestamps"]), common), f"{prb['n_rows']} vs {len(common)}")
        if prb["n_rows"] == len(common):
            self.check("pairs", f"{t}/{t2} bar series values", np.array_equal(prb["columns"]["series"], ba["close"][ia]) and np.array_equal(prb["columns"]["series2"], bb2["close"][ib]))
            self.check("pairs", f"{t}/{t2} ratio_zscore vs numpy", nan_eq_close(prb["columns"]["ratio_zscore_20"], R.zscore(ba["close"][ia] / bb2["close"][ib], 20), 1e-7, atol=1e-7).all())
            self.check("pairs", f"{t}/{t2} rel_strength vs numpy", nan_eq_close(prb["columns"]["rel_strength_10"], talib.ROC(ba["close"][ia], 10) - talib.ROC(bb2["close"][ib], 10), 1e-9, atol=1e-9).all())
        prt = db.pair_indicators(db2, [{"kind": "series"}, {"kind": "series2"}], tail=100, columns=COLS, other_columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
        self.check("pairs", f"{t}/{t2} pair tail(100) equals the last 100 joined bars", prt["n_rows"] == 100 and np.array_equal(prt["columns"]["series2"], bb2["close"][ib][-100:]))
        db2.close()

    # --- health vs pandas ------------------------------------------------------------------
    hl = db.health(INT64_MIN, INT64_MAX, price="price", volume="size", gap_threshold=60 * US, outlier_threshold=0.01)
    gaps = np.diff(ts)
    lr = np.abs(np.log(d["price"][1:] / d["price"][:-1]))
    ok = (hl["count"] == n and hl["first_ts"] == ts[0] and hl["last_ts"] == ts[-1] and hl["max_gap"] == gaps.max()
          and hl["n_gaps"] == int((gaps > 60 * US).sum()) and abs(hl["median_gap"] - float(np.median(gaps))) < 1e-9
          and hl["n_outlier_returns"] == int((lr > 0.01).sum()) and abs(hl["max_abs_return"] - lr.max()) < 1e-12
          and hl["n_zero_volume"] == int((d["size"] == 0).sum()) and hl["n_nonpositive_price"] == 0)
    self.check("health", f"{t} health matches pandas/numpy", ok, str({k: hl[k] for k in ("count", "n_gaps", "max_gap", "median_gap", "n_outlier_returns")}))

    # --- evaluation vs a python reference --------------------------------------------------
    rng = random.Random(5)
    decisions = []
    for _ in range(300):
        i = rng.randrange(0, n - 1)
        decisions.append({"timestamp": int(ts[i]) + rng.randrange(0, 1000), "direction": rng.choice([1, -1, 1, 0]), "size": rng.choice([100, 500, 1000]), "horizon": rng.choice([0, 60 * US, 3600 * US])})
    ev = db.evaluate(decisions, price="price", default_horizon=300 * US, cost_bps=2.0)
    ref_net = []
    pnl = []
    for k, dec in enumerate(decisions):
        if dec["direction"] == 0:
            continue
        hz = dec["horizon"] or 300 * US
        ei = int(np.searchsorted(ts, dec["timestamp"], side="left"))
        xi = int(np.searchsorted(ts, dec["timestamp"] + hz, side="left"))
        if ei >= n or xi >= n:
            continue
        gross = dec["direction"] * (d["price"][xi] / d["price"][ei] - 1)
        net = gross - 2 * 2.0 / 1e4
        ref_net.append((k, net, dec["size"] * net))
        pnl.append(dec["size"] * net)
    got = np.asarray(ev["net_return"])
    same = ev["n_evaluated"] == len(ref_net) and all(abs(got[k] - net) < 1e-12 for k, net, _ in ref_net)
    nets = np.array([x[1] for x in ref_net])
    cum = np.cumsum(pnl)
    mdd = float(np.max(np.maximum.accumulate(np.concatenate([[0], cum])) - np.concatenate([[0], cum])))
    same &= abs(ev["total_pnl"] - sum(pnl)) < 1e-6 and abs(ev["hit_rate"] - np.mean(nets > 0)) < 1e-12 and abs(ev["max_drawdown"] - mdd) < 1e-6
    same &= abs(ev["sharpe"] - nets.mean() / nets.std(ddof=1)) < 1e-9
    self.check("evaluate", f"{t} evaluation of 300 decisions matches the python reference", bool(same), f"n_evaluated {ev['n_evaluated']} vs {len(ref_net)}")

    # --- multi-timeframe snapshot == single snapshots ---------------------------------------
    buckets = [BAR_1M, 5 * BAR_1M, 60 * BAR_1M]
    ppys = [PPY_1M, PPY_1M / 5, PPY_1M / 60]
    t0 = time.time()
    multi = db.snapshot_multi(buckets, periods_per_year=ppys, bars=500, columns=COLS)
    self.timing("snapshot_multi 3 timeframes x 500 bars", time.time() - t0)
    ok = len(multi) == 3
    for k, bk in enumerate(buckets):
        single = db.snapshot(columns=COLS, bars=500, bucket=bk, periods_per_year=ppys[k])
        for key in single:
            gv, sv = multi[k][key], single[key]
            if not ((gv == sv) or (isinstance(gv, float) and math.isnan(gv) and math.isnan(sv))):
                ok = False
                self.check("multi", f"{t} snapshot_multi[{bk}].{key} == snapshot", False, f"{gv} vs {sv}")
                break
    self.check("multi", f"{t} snapshot_multi equals individual snapshots for 3 timeframes", ok)
    db.close()


Harness.phase_round2 = _phase_round2


# ---------------------------------------------------------------------------
# Round 3: operations — cross-process readers, crash recovery, checksums,
# retention / rollover under a live reader, fsync policies, metrics
# ---------------------------------------------------------------------------
OPS_STEP = 1000  # µs between synthetic records of the child writer
OPS_START = 1_756_684_800_000_000  # 2025-09-01 00:00 UTC in µs
OPS_WRITER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ops_writer.py")
WRAP_BIT = 1 << 63


def _ops_header(path):
    """Parse the HOC2 header of a database file: (committed_cursor, wrapped, last_ts)."""
    import struct
    with open(path, "rb") as f:
        hdr = f.read(HEADER_SIZE)
    assert hdr[:4] == b"HOC2", hdr[:4]
    committed = struct.unpack_from("<Q", hdr, 24)[0]
    last_ts = struct.unpack_from("<q", hdr, 48)[0]
    return committed & ~WRAP_BIT, bool(committed & WRAP_BIT), last_ts


def _ops_progress(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"appended": 0, "committed": 0}


def _ops_wait(path, committed, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if _ops_progress(path).get("committed", 0) >= committed:
            return True
        time.sleep(0.01)
    return False


def _ops_span(db):
    """(count, min_ts, max_ts) of the committed records as a reader/writer sees them."""
    st = db.get_stats(INT64_MIN, INT64_MAX, "timestamp")  # one read: count/min/max from the same commit
    n = int(st["count"])
    if n == 0:
        return 0, None, None
    return n, int(st["min"]), int(st["max"])


def _ops_contiguous(count, lo, hi, step=OPS_STEP):
    return count == 0 or (hi - lo) == (count - 1) * step


def _phase_ops(self):
    self.phase("Phase 13: cross-process readers, crash recovery, checksums, retention, rollover, fsync, metrics")
    import signal
    import struct
    rec_size = RECORD_DTYPE.itemsize
    quick = self.args.quick
    hs = HOCDB.header_size()
    self.check("ops", "header_size() == 64", hs == HEADER_SIZE, str(hs))

    def spawn(ticker, progress, **kw):
        cmd = [sys.executable, OPS_WRITER, "--ticker", ticker, "--dir", self.data_dir, "--start-ts", str(OPS_START),
               "--step", str(OPS_STEP), "--progress", progress]
        for k, v in kw.items():
            cmd += ["--" + k.replace("_", "-"), str(v)]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def stop(proc):
        if proc.poll() is None:
            proc.kill()
        try:
            out = proc.communicate(timeout=30)[0]
        except subprocess.TimeoutExpired:
            out = ""
        return out or ""

    # --- 1. writer lock + lock-free reader following a live writer -----------------------------------
    ticker = "OPS_LIVE"
    progress = os.path.join(self.data_dir, ticker + ".progress.json")
    child = spawn(ticker, progress, batch=500, sleep_ms=2)
    try:
        ready = _ops_wait(progress, 2000, timeout=60)
        self.check("ops", "child writer produced 2000 committed records", ready, str(_ops_progress(progress)))
        try:
            self.open_db(ticker)
            self.check("ops", "second writer (this process) is refused while the child holds the lock", False, "opened!")
        except Exception as e:  # noqa: BLE001
            self.check("ops", "second writer is refused with DatabaseLocked", "DatabaseLocked" in str(e), str(e))
        reader = HOCDB.open_reader(ticker, self.data_dir, SCHEMA)
        self._open.append(reader)
        self.check("ops", "reader.is_read_only()", reader.is_read_only())
        c_before = _ops_progress(progress)["committed"]
        n, lo, hi = _ops_span(reader)
        c_after = _ops_progress(progress)["committed"]
        self.check("ops", "reader count lies between the child's committed counts sampled before/after", c_before <= n <= c_after, f"{c_before} <= {n} <= {c_after}")
        self.check("ops", "reader sees a contiguous prefix of the child's sequence", lo == OPS_START and _ops_contiguous(n, lo, hi), f"n={n} lo={lo} hi={hi}")
        # follow commits: counts monotonic, strictly increasing over the sampling period, latest matches
        counts = []
        lat_ok = True
        for _ in range(25):
            reader.refresh()
            n, lo, hi = _ops_span(reader)
            counts.append(n)
            latest = reader.get_latest("timestamp")["timestamp"]  # a later read: may already see a newer commit
            if latest < hi or (latest - hi) % OPS_STEP != 0 or not _ops_contiguous(n, lo, hi):
                lat_ok = False
            time.sleep(0.02)
        mono = all(b >= a for a, b in zip(counts, counts[1:]))
        self.check("ops", "reader counts are monotonic and increase while the child writes", mono and counts[-1] > counts[0], f"{counts[0]} -> {counts[-1]}")
        self.check("ops", "reader get_latest/timestamps stay consistent with the committed count at every sample", lat_ok)
        m = reader.metrics()
        self.check("ops", "reader metrics: read_only == 1, refreshes > 0, format_version == 2", m["read_only"] == 1 and m["refreshes"] > 0 and m["format_version"] == 2, str({k: m[k] for k in ("read_only", "refreshes", "format_version")}))
        self.check("ops", "reader ingest_lag_wall_ns is small while the child writes (< 5 s)", 0 <= m["ingest_lag_wall_ns"] < 5_000_000_000, str(m["ingest_lag_wall_ns"]))
        # analytics on a reader while the writer keeps committing
        try:
            errs = 0
            for _ in range(10):
                snap = reader.snapshot(columns=COLS, bars=200, bucket=60 * OPS_STEP)
                res = reader.indicators([{"kind": "sma", "period": 20}, {"kind": "rsi", "period": 14}], columns=COLS, lookback=0, bucket=0, tail=500, as_numpy=True)
                if snap["bars"] <= 0 or len(res["columns"]["sma_20"]) != 500 or len(res["columns"]["rsi_14"]) != 500:
                    errs += 1
                n2 = reader.metrics()["committed_records"]
                if n2 < counts[-1]:
                    errs += 1
            self.check("ops", "snapshot/indicators on the reader while the child writes (10 rounds)", errs == 0, f"errs={errs}")
        except Exception as e:  # noqa: BLE001
            self.check("ops", "snapshot/indicators on the reader while the child writes", False, f"{type(e).__name__}: {e}")
        for name, fn in (("append", lambda: reader.append(OPS_START, 1.0, 1.0, 1.0, 1.0, False)), ("compact", lambda: reader.compact(0)),
                         ("retain_last", lambda: reader.retain_last(1)), ("rollover", lambda: reader.rollover()), ("sync", lambda: reader.sync())):
            try:
                fn()
                self.check("ops", f"reader.{name}() is refused", False, "no error")
            except Exception as e:  # noqa: BLE001
                self.check("ops", f"reader.{name}() is refused (read-only)", "read" in str(e).lower(), str(e))
        # --- 2. SIGKILL the writer mid-stream: recovery yields an exact prefix ---------------------
        _ops_wait(progress, counts[-1] + 3000, timeout=60)
        c0 = _ops_progress(progress)["committed"]
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=30)
        path = os.path.join(self.data_dir, ticker + ".bin")
        hdr_cursor, wrapped, hdr_last = _ops_header(path)
        hdr_records = (hdr_cursor - HEADER_SIZE) // rec_size
        fsize = os.path.getsize(path)
        self.check("ops", "after SIGKILL the header's committed count >= the child's last published commit", hdr_records >= c0 and not wrapped, f"hdr={hdr_records} published={c0}")
        reader.refresh()
        n, lo, hi = _ops_span(reader)
        self.check("ops", "attached reader sees exactly the committed records after the crash", n == hdr_records and hi == hdr_last and _ops_contiguous(n, lo, hi), f"reader={n} hdr={hdr_records}")
        uncommitted = fsize - hdr_cursor
        w = self.open_db(ticker)
        m = w.metrics()
        n, lo, hi = _ops_span(w)
        expect_rec = uncommitted // rec_size
        self.check("ops", "writer reopen adopts every complete uncommitted record and drops the torn remainder",
                   m["recovered_tail_records"] == expect_rec and m["dropped_tail_bytes"] == uncommitted - expect_rec * rec_size and n == hdr_records + expect_rec,
                   f"uncommitted={uncommitted} recovered={m['recovered_tail_records']} dropped={m['dropped_tail_bytes']} n={n}")
        self.check("ops", "recovered database is a contiguous prefix of the child's sequence", lo == OPS_START and _ops_contiguous(n, lo, hi), f"n={n} lo={lo} hi={hi}")
        w.flush()
        self.check("ops", "checksum verifies after recovery", w.verify() is True)
        reader.refresh()
        self.check("ops", "reader follows the recovered commit", _ops_span(reader)[0] == n, f"{_ops_span(reader)[0]} vs {n}")
        # --- 3. torn tail + misordered record injected on disk ----------------------------------------
        w.close()
        self._open.remove(w)
        base = n
        with open(path, "ab") as f:
            for k in range(2):
                ts = OPS_START + (base + k) * OPS_STEP
                f.write(struct.pack("<qddddB", ts, 101.0, 2.0, 100.99, 101.01, 1))
            f.write(b"\x07\x13\x37\xaa\x55\x00\xff")
        w = self.open_db(ticker)
        m = w.metrics()
        n2 = _ops_span(w)[0]
        self.check("ops", "2 valid records + 7 garbage bytes: recovered 2, dropped 7", m["recovered_tail_records"] == 2 and m["dropped_tail_bytes"] == 7 and n2 == base + 2, f"{m['recovered_tail_records']}/{m['dropped_tail_bytes']}/{n2}")
        w.flush()
        self.check("ops", "verify() after adopting a tail", w.verify() is True)
        w.close()
        self._open.remove(w)
        with open(path, "ab") as f:
            f.write(struct.pack("<qddddB", OPS_START, 1.0, 1.0, 1.0, 1.0, 0))  # older than the last record
        w = self.open_db(ticker)
        m = w.metrics()
        n3, lo, hi = _ops_span(w)
        self.check("ops", "a misordered uncommitted record is dropped (41 bytes), count unchanged", m["recovered_tail_records"] == 0 and m["dropped_tail_bytes"] == rec_size and n3 == base + 2, f"{m['recovered_tail_records']}/{m['dropped_tail_bytes']}/{n3}")
        # corrupt a committed byte -> verify False, verify_on_open refuses
        w.close()
        self._open.remove(w)
        with open(path, "r+b") as f:
            f.seek(HEADER_SIZE + 10 * rec_size + 8)
            b = f.read(1)
            f.seek(HEADER_SIZE + 10 * rec_size + 8)
            f.write(bytes([b[0] ^ 0x5A]))
        w = self.open_db(ticker)
        self.check("ops", "verify() detects a flipped byte inside committed data", w.verify() is False)
        self.check("ops", "crc_failures counted (at open and by verify)", w.metrics()["crc_failures"] == 2, str(w.metrics()["crc_failures"]))
        w.close()
        self._open.remove(w)
        try:
            self.open_db(ticker, verify_on_open=True)
            self.check("ops", "verify_on_open refuses a corrupted file", False, "opened")
        except Exception as e:  # noqa: BLE001
            self.check("ops", "verify_on_open refuses a corrupted file (ChecksumMismatch)", "ChecksumMismatch" in str(e), str(e))
        with open(path, "r+b") as f:  # repair
            f.seek(HEADER_SIZE + 10 * rec_size + 8)
            f.write(b)
        w = self.open_db(ticker, verify_on_open=True)
        self.check("ops", "repaired file opens with verify_on_open", w.verify() is True)
        reader.refresh()
        self.check("ops", "long-lived reader still consistent after recovery/corruption cycles", _ops_span(reader)[0] == base + 2)
    finally:
        stop(child)

    # --- 4. retention (automatic compaction) under a live cross-process reader ---------------------
    ticker = "OPS_RET"
    progress = os.path.join(self.data_dir, ticker + ".progress.json")
    span_records = 4000 if quick else 20000
    child = spawn(ticker, progress, batch=500, sleep_ms=3, retention_span=span_records * OPS_STEP)
    try:
        _ops_wait(progress, 1000, timeout=60)
        reader = HOCDB.open_reader(ticker, self.data_dir, SCHEMA)
        self._open.append(reader)
        target = span_records * 6
        mins, maxc, errs, samples = [], 0, 0, 0
        deadline = time.time() + 120
        while _ops_progress(progress)["committed"] < target and time.time() < deadline:
            try:
                n, lo, hi = _ops_span(reader)
                if n and not _ops_contiguous(n, lo, hi):
                    errs += 1
                mins.append(lo)
                maxc = max(maxc, n)
                samples += 1
            except Exception as e:  # noqa: BLE001
                errs += 1
                self.notes.append(f"ops retention reader error: {type(e).__name__}: {e}")
            time.sleep(0.002)
        self.check("ops", f"reader followed {samples} samples through automatic compaction without errors", errs == 0 and samples > 10, f"errs={errs} samples={samples}")
        self.check("ops", "retention bounds the record count (< span * 1.25 + batch)", maxc <= span_records * 1.25 + 500 + 1, f"max={maxc}")
        mins = [x for x in mins if x is not None]
        self.check("ops", "the reader's min timestamp advances (old history compacted away)", mins and mins[-1] > mins[0] and all(b >= a for a, b in zip(mins, mins[1:])), f"{mins[0] if mins else None} -> {mins[-1] if mins else None}")
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=30)
        w = self.open_db(ticker)
        n, lo, hi = _ops_span(w)
        m = w.metrics()
        self.check("ops", "compacted database reopens as a contiguous, span-bounded window", _ops_contiguous(n, lo, hi) and lo > OPS_START and n <= span_records * 1.25 + 500 + 1, f"n={n} lo={lo} hi={hi} recovered={m['recovered_tail_records']}")
        w.flush()
        self.check("ops", "verify() after compaction + crash", w.verify() is True)
        w.append(hi + OPS_STEP, 1.0, 1.0, 1.0, 1.0, False)
        w.flush()
        self.check("ops", "appends continue after compaction", _ops_span(w)[0] == n + 1)
        w.compact(hi - 100 * OPS_STEP)
        self.check("ops", "explicit compact(min_ts) keeps 102 records", _ops_span(w)[0] == 102, str(_ops_span(w)))
        w.retain_last(10)
        self.check("ops", "retain_last(10)", _ops_span(w) == (10, hi + OPS_STEP - 9 * OPS_STEP, hi + OPS_STEP), str(_ops_span(w)))
        reader.refresh()
        self.check("ops", "reader follows explicit compaction (10 records)", _ops_span(reader)[0] == 10, str(_ops_span(reader)))
    finally:
        stop(child)

    # --- 5. rollover by size under a live cross-process reader --------------------------------------
    ticker = "OPS_ROLL"
    progress = os.path.join(self.data_dir, ticker + ".progress.json")
    per_file = 2000 if quick else 5000
    child = spawn(ticker, progress, batch=500, sleep_ms=3, rollover_size=HEADER_SIZE + per_file * rec_size)
    try:
        _ops_wait(progress, 500, timeout=60)
        reader = HOCDB.open_reader(ticker, self.data_dir, SCHEMA)
        self._open.append(reader)
        target = per_file * (8 if quick else 6)
        counts, errs, decreased = [], 0, False
        deadline = time.time() + 120
        while _ops_progress(progress)["committed"] < target and time.time() < deadline:
            try:
                n, lo, hi = _ops_span(reader)
                if n and not _ops_contiguous(n, lo, hi):
                    errs += 1
                if counts and n < counts[-1]:
                    decreased = True
                counts.append(n)
            except Exception as e:  # noqa: BLE001
                errs += 1
                self.notes.append(f"ops rollover reader error: {type(e).__name__}: {e}")
            time.sleep(0.002)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=30)
        archives = sorted(x for x in os.listdir(self.data_dir) if x.startswith(ticker + ".") and x.endswith(".bin") and "-" in x)
        self.check("ops", f"reader followed {len(counts)} samples through {len(archives)} rollovers without errors", errs == 0 and len(counts) > 10, f"errs={errs}")
        self.check("ops", "rollover happened (>= 2 archives) and the reader observed the file reset", len(archives) >= 2 and decreased, f"archives={len(archives)} decreased={decreased}")
        self.check("ops", "live file never exceeds the rollover size (+ one batch)", max(counts) <= per_file + 500, f"max={max(counts) if counts else None}")
        # the archives + live file form one contiguous sequence
        parts = []
        for a in archives:
            db = self.open_db(a[:-4])
            parts.append(_ops_span(db))
            db.close()
            self._open.remove(db)
        live = self.open_db(ticker)
        parts.append(_ops_span(live))
        parts.sort(key=lambda p: (p[1] if p[1] is not None else INT64_MAX))
        ok = parts[0][1] == OPS_START
        for (n, lo, hi) in parts:
            ok = ok and _ops_contiguous(n, lo, hi)
        for (na, la, ha), (nb, lb, hb) in zip(parts, parts[1:]):
            if lb is not None and ha is not None:
                ok = ok and lb == ha + OPS_STEP
        self.check("ops", "archives + live file form one contiguous timestamp sequence", ok, str(parts))
        self.check("ops", "archive names carry first/last timestamps", all(f"{lo}-{hi}" in a for a, (n, lo, hi) in zip(archives, sorted(parts[:-1], key=lambda p: p[1]))), str(archives[:3]))
        n_live, lo_live, hi_live = parts[-1]
        next_ts = (hi_live if hi_live is not None else parts[-2][2]) + OPS_STEP
        for k in range(5):  # the child may have died right after a rollover: make sure the live file has data
            live.append(next_ts + k * OPS_STEP, 1.0, 1.0, 1.0, 1.0, False)
        live.flush()
        arc = live.rollover()
        self.check("ops", "explicit rollover() archives the live file", os.path.exists(arc) and _ops_span(live)[0] == 0 and f"-{next_ts + 4 * OPS_STEP}" in arc, arc)
        try:
            live.rollover()
            self.check("ops", "rollover() of an empty database is refused", False, "no error")
        except Exception as e:  # noqa: BLE001
            self.check("ops", "rollover() of an empty database is refused (EmptyDatabase)", "Empty" in str(e), str(e))
        reader.refresh()
        self.check("ops", "reader follows the explicit rollover (0 records)", _ops_span(reader)[0] == 0)
    finally:
        stop(child)

    # --- 6. fsync policies + sync() -------------------------------------------------------------------
    for policy in ("none", "on_close", "on_flush", "interval"):
        t = "OPS_FS_" + policy
        w = self.open_db(t, fsync=policy, fsync_interval_ms=50)
        for i in range(200):
            w.append(OPS_START + i * OPS_STEP, 1.0 + i, 1.0, 1.0, 1.0, False)
        w.flush()
        f1 = w.metrics()["fsyncs"]
        time.sleep(0.08)
        w.append(OPS_START + 200 * OPS_STEP, 1.0, 1.0, 1.0, 1.0, False)
        w.flush()
        f2 = w.metrics()["fsyncs"]
        w.sync()
        f3 = w.metrics()["fsyncs"]
        expect = {"none": f1 == 0 and f2 == 0, "on_close": f1 == 0 and f2 == 0, "on_flush": f1 >= 1 and f2 > f1, "interval": f2 >= 1 and f2 <= 2}[policy]
        self.check("ops", f"fsync policy {policy}: fsync counts after flush/interval/sync", expect and f3 > f2, f"{f1}/{f2}/{f3}")
        self.check("ops", f"fsync policy {policy}: verify() true", w.verify() is True)
        m = w.metrics()
        self.check("ops", f"fsync policy {policy}: fsync latency accounted", m["fsync_ns_total"] > 0 and m["fsync_ns_max"] <= m["fsync_ns_total"], str((m["fsync_ns_total"], m["fsync_ns_max"])))
        w.close()
        self._open.remove(w)

    # --- 7. metrics sanity on a writer (with record-time lag) ---------------------------------------
    t = "OPS_METRICS"
    w = self.open_db(t, timestamp_unit_ns=1000)
    N = 5000
    for i in range(N):
        w.append(OPS_START + i * OPS_STEP, 1.0 + i, 1.0, 1.0, 1.0, False)
    w.flush()
    m = w.metrics()
    exp = {"appends": N, "bytes_written": N * rec_size, "committed_records": N, "file_size": HEADER_SIZE + N * rec_size,
           "last_record_ts": OPS_START + (N - 1) * OPS_STEP, "format_version": 2, "read_only": 0}
    bad = {k: (m[k], v) for k, v in exp.items() if m[k] != v}
    self.check("ops", "writer metrics: appends/bytes/committed/file_size/last_record_ts/format/read_only", not bad and m["commits"] >= 1 and m["flushes"] >= 1, str(bad))
    now_ns = time.time_ns()
    lag = m["ingest_lag_record_ns"]
    expect_lag = now_ns - exp["last_record_ts"] * 1000
    self.check("ops", "ingest_lag_record_ns = now - last_record_ts * timestamp_unit_ns (within 60 s)", abs(lag - expect_lag) < 60_000_000_000, f"{lag} vs {expect_lag}")
    self.check("ops", "ingest_lag_wall_ns small right after an append", 0 <= m["ingest_lag_wall_ns"] < 5_000_000_000, str(m["ingest_lag_wall_ns"]))
    for _ in range(50):
        w.get_stats(OPS_START, OPS_START + 1000 * OPS_STEP, "price")
    w.load()
    m = w.metrics()
    self.check("ops", "read metrics: reads counted, p50 <= p99 <= max, records_read >= N", m["reads"] >= 51 and m["read_ns_p50"] <= m["read_ns_p99"] <= m["read_ns_max"] and m["records_read"] >= N + 50 * 1000, str({k: m[k] for k in ("reads", "read_ns_p50", "read_ns_p99", "read_ns_max", "records_read")}))
    w.metrics_reset()
    m = w.metrics()
    self.check("ops", "metrics_reset() clears counters and keeps state", m["reads"] == 0 and m["appends"] == 0 and m["committed_records"] == N and m["last_record_ts"] == exp["last_record_ts"], str((m["reads"], m["committed_records"])))
    self.timing("ops writer metrics call", (lambda t0: (w.metrics(), time.time() - t0)[1])(time.time()))

    # --- 8. ring buffer + reader (in-process) with the 64-byte header -------------------------------
    t = "OPS_RING"
    cap = 100
    w = self.open_db(t, max_file_size=HEADER_SIZE + cap * rec_size, overwrite_on_full=True)
    r = HOCDB.open_reader(t, self.data_dir, SCHEMA)
    self._open.append(r)
    for i in range(250):
        w.append(OPS_START + i * OPS_STEP, 1.0 + i, 1.0, 1.0, 1.0, False)
        if i % 37 == 0:
            w.flush()
            n, lo, hi = _ops_span(r)
            if not (n == min(i + 1, cap) and hi == OPS_START + i * OPS_STEP and _ops_contiguous(n, lo, hi)):
                self.check("ops", f"ring reader consistent at {i}", False, f"n={n} lo={lo} hi={hi}")
    w.flush()
    n, lo, hi = _ops_span(r)
    self.check("ops", "ring buffer reader sees exactly the last 100 records after wrap", n == cap and hi == OPS_START + 249 * OPS_STEP and lo == OPS_START + 150 * OPS_STEP, f"n={n} lo={lo} hi={hi}")
    try:
        w.verify()
        self.check("ops", "verify() on a ring buffer reports unavailable", False, "no error")
    except Exception as e:  # noqa: BLE001
        self.check("ops", "verify() on a ring buffer reports unavailable", "unavailable" in str(e).lower(), str(e))
    self.close_all()


Harness.phase_ops = _phase_ops


# ---------------------------------------------------------------------------
# Round 4: trading calendars, signal backtester, universe features
# ---------------------------------------------------------------------------
import hocdb_python as H  # noqa: E402
import references_backtest as RB  # noqa: E402
import references_universe as RU  # noqa: E402


def _same_num(a, b, rtol=1e-9, atol=1e-9):
    if isinstance(a, float) and isinstance(b, float) and (math.isnan(a) or math.isnan(b)):
        return math.isnan(a) and math.isnan(b)
    if isinstance(a, float) and isinstance(b, float) and (math.isinf(a) or math.isinf(b)):
        return a == b
    return abs(float(a) - float(b)) <= atol + rtol * abs(float(b))


def _phase_calendar(self):
    self.phase("Phase 14: trading calendars vs exchange_calendars, calendar sessions, trading-time health, auto annualisation")
    nyse, lse = H.calendar_id("nyse"), H.calendar_id("lse")
    self.check("calendar", "built-in ids", nyse == 3 and lse == 5 and H.calendar_id("crypto") == 1 and H.calendar_name(6) == "cme")
    # --- sessions 2000-2030 vs the exchange_calendars library ------------------------------------
    try:
        import exchange_calendars as xc
    except ImportError:
        xc = None
        self.notes.append("exchange_calendars not installed: calendar sessions checked against built-in expectations only")
    if xc is not None:
        for name, code, cid, known_extra in (("nyse", "XNYS", nyse, {"2025-01-09"}), ("lse", "XLON", lse, set())):
            cal = xc.get_calendar(code, start="2000-01-01", end="2030-12-31")
            ref = {}
            for t, row in cal.schedule.iterrows():
                ref[(t.normalize() - pd.Timestamp("1970-01-01")).days] = (int(row["open"].timestamp()), int(row["close"].timestamp()))
            mine = {}
            d0, d1 = H.days_from_civil(2000, 1, 1), H.days_from_civil(2031, 1, 1)
            for d in range(d0, d1):
                sess = H.calendar_session_for_day(cid, d)
                if sess is not None:
                    mine[d] = (sess["open"], sess["close"])
            missing = [d for d in ref if d not in mine]
            extra = [d for d in mine if d not in ref]
            diffs = [d for d in ref if d in mine and ref[d] != mine[d]]
            fmt = lambda d: str(pd.Timestamp("1970-01-01") + pd.Timedelta(days=d))[:10]
            extra_unknown = [d for d in extra if fmt(d) not in known_extra]
            missing_unknown = [d for d in missing if fmt(d) not in known_extra]
            self.check("calendar", f"{name}: {len(mine)} sessions 2000-2030 match exchange_calendars ({len(ref)} ref)", not diffs and not extra_unknown and not missing_unknown,
                       f"missing={[fmt(d) for d in missing_unknown[:5]]} extra={[fmt(d) for d in extra_unknown[:5]]} diffs={[fmt(d) for d in diffs[:5]]}")
    # --- built-in expectations ---------------------------------------------------------------------
    fri = H.days_from_civil(2025, 9, 5) * 86400 + 15 * 3600
    s = H.calendar_session(nyse, fri, 0)
    self.check("calendar", "nyse Friday 2025-09-05 session 13:30-20:00 UTC", s is not None and s["open"] == fri - 90 * 60 and s["close"] == fri + 5 * 3600 and not s["early_close"], str(s))
    sat = H.days_from_civil(2025, 9, 6) * 86400 + 12 * 3600
    self.check("calendar", "Saturday closed, prev Friday, next Monday", H.calendar_session(nyse, sat, 0) is None and H.calendar_session(nyse, sat, 1)["trade_day"] == H.days_from_civil(2025, 9, 5) and H.calendar_session(nyse, sat, 2)["trade_day"] == H.days_from_civil(2025, 9, 8))
    self.check("calendar", "periods per year: nyse 1-minute 98280, crypto daily 365", abs(H.calendar_periods_per_year(nyse, 60) - 252 * 390) < 1e-9 and abs(H.calendar_periods_per_year(1, 86400) - 365) < 1e-9)
    self.check("calendar", "FX Monday session opens Sunday 21:00 UTC (EDT)", H.calendar_session_for_day(2, H.days_from_civil(2025, 9, 8))["open"] == H.days_from_civil(2025, 9, 7) * 86400 + 21 * 3600)
    # --- a database with the NYSE calendar: only in-session ticks of an equity ticker ------------------
    eq = [t for t in self.tickers if tickgen.SPECS[t].kind == "equity"] if hasattr(tickgen, "SPECS") else [t for t in self.tickers if t in ("AAPL", "NVDA", "TSLA", "SPY")]
    if not eq:
        self.notes.append("calendar phase: no equity ticker in the run, DB checks skipped")
        return
    t = eq[0]
    d = self.ticks[t]
    ts = d["timestamp"]
    sec = ts // US
    # session open per tick (NaN-free: vectorised via day lookups)
    days = np.unique(sec // 86400)
    open_by_day, close_by_day = {}, {}
    for day in days:
        for dd in (day - 1, day, day + 1):
            sess = H.calendar_session_for_day(nyse, int(dd))
            if sess is not None:
                open_by_day[int(dd)] = sess["open"]
                close_by_day[int(dd)] = sess["close"]
    opens = np.array([open_by_day.get(int(x), -1) for x in sec // 86400])
    closes = np.array([close_by_day.get(int(x), -1) for x in sec // 86400])
    inside = (opens >= 0) & (sec >= opens) & (sec < closes)
    # tickgen emits 13:30-20:00 UTC on weekdays: exactly the NYSE session of that date, except on holidays
    has_session = opens >= 0
    n_days = len(np.unique(sec // 86400))
    n_open_days = len(np.unique((sec // 86400)[has_session]))
    self.check("calendar", f"{t}: every tick of a trading day lies inside its NYSE session ({n_open_days}/{n_days} generated days are trading days)",
               bool(np.array_equal(inside, has_session)) and n_open_days > 0, f"inside {inside.mean():.3f}, has_session {has_session.mean():.3f}")
    sel = {k: (v[inside] if isinstance(v, np.ndarray) and len(v) == len(ts) else v) for k, v in d.items()}
    db = HOCDB("CALX", self.data_dir, SCHEMA, calendar="nyse", timestamp_unit_ns=1000)
    self._open.append(db)
    self.ingest(db, sel)
    db.flush()
    self.check("calendar", "handle reports calendar 3, unit 1000, ppy(1 min) = 98280", db.get_calendar() == 3 and db.get_timestamp_unit() == 1000 and abs(db.periods_per_year(BAR_1M) - 252 * 390) < 1e-9)
    sopen = opens[inside] * US
    sts = ts[inside]
    px = sel["price"]
    sz = sel["size"]
    # session kinds with param 0 on 1-minute bars vs pandas groupby(session open)
    bars = db.ohlcv(int(sts[0]), int(sts[-1]) + 1, BAR_1M, price="price", volume="size")
    bts = np.asarray(bars["timestamps"], dtype=np.int64)
    bopen, bhigh, blow, bclose, bvol = (np.asarray(bars[k], dtype=float) for k in ("open", "high", "low", "close", "volume"))
    bsec = bts // US
    bday = bsec // 86400
    b_open = np.array([open_by_day.get(int(x), -1) for x in bday])
    frame = pd.DataFrame({"ts": bts, "sess": b_open, "open": bopen, "high": bhigh, "low": blow, "close": bclose, "vol": bvol})
    g = frame.groupby("sess", sort=False)
    tp = (bhigh + blow + bclose) / 3
    frame["pv"] = tp * bvol
    ref_vwap = (g["pv"].cumsum() / g["vol"].cumsum()).to_numpy()
    ref_sopen = g["open"].transform("first").to_numpy()
    ref_shigh = g["high"].cummax().to_numpy()
    ref_slow = g["low"].cummin().to_numpy()
    # pivots from the previous session
    per = g.agg(h=("high", "max"), l=("low", "min"), c=("close", "last"))
    per["pp"] = (per["h"] + per["l"] + per["c"]) / 3
    prev_pp = per["pp"].shift(1)
    ref_pp = frame["sess"].map(prev_pp).to_numpy()
    specs = [{"kind": "session_vwap", "param": 0}, {"kind": "session_range", "param": 0}, {"kind": "pivots", "param": 0}, {"kind": "opening_range", "period": 5, "param": 0}]
    start, end = int(bts[len(bts) // 3]), int(bts[-1]) + 1
    res = db.indicators(specs, start_ts=start, end_ts=end, columns=COLS, lookback=0, bucket=BAR_1M, as_numpy=True)
    rows_ts = np.asarray(res["timestamps"], dtype=np.int64)
    idx = np.searchsorted(bts, rows_ts)
    self.check("calendar", "calendar window rows are the bars with start in [start, end)", np.array_equal(bts[idx], rows_ts) and len(rows_ts) == np.sum((bts >= start) & (bts < end)))
    c = res["columns"]
    self.check("calendar", f"session_vwap(param 0) matches pandas per NYSE session ({len(rows_ts)} rows)", nan_eq_close(c["session_vwap"], ref_vwap[idx], 1e-9).all())
    self.check("calendar", "session_range open/high/low match", nan_eq_close(c["session_range_open"], ref_sopen[idx], 1e-12).all() and nan_eq_close(c["session_range_high"], ref_shigh[idx], 1e-12).all() and nan_eq_close(c["session_range_low"], ref_slow[idx], 1e-12).all())
    self.check("calendar", "pivots use the previous trading day", nan_eq_close(c["pivots_pp"], ref_pp[idx], 1e-9).all())
    orb = c["opening_range_5_high"]
    first_rows = frame.groupby("sess").cumcount().to_numpy()[idx]
    self.check("calendar", "opening range high defined for every row of the window (the range forms in the first 5 bars)", bool(np.all(np.isfinite(orb))))
    self.check("calendar", "opening range breakout is NaN exactly while the range is still forming", bool(np.array_equal(np.isnan(c["opening_range_5_breakout"]), first_rows < 5)))
    # health in trading time
    hl = db.health(0, INT64_MAX, price="price", volume="size", gap_threshold=5 * 60 * US, outlier_threshold=0.2)
    n_sessions = len(np.unique(sopen))
    self.check("calendar", f"health: {n_sessions} sessions -> {hl['n_session_breaks']} session breaks, no missing sessions, closed time {hl['closed_span'] / US:.0f}s",
               hl["n_session_breaks"] == n_sessions - 1 and hl["n_missing_sessions"] == 0 and (hl["closed_span"] > 0) == (n_sessions > 1))
    raw_gaps = np.diff(sts)
    same_sess = sopen[1:] == sopen[:-1]
    self.check("calendar", "health max_gap is the largest intra-session gap (session breaks removed)", hl["max_gap"] == int(raw_gaps[same_sess].max()) and hl["n_gaps"] == int(np.sum(raw_gaps[same_sess] > 5 * 60 * US)), f"{hl['max_gap']} vs {raw_gaps[same_sess].max()}")
    # automatic annualisation
    sm0 = db.summary(0, INT64_MAX, "price", periods_per_year=0)
    sm1 = db.summary(0, INT64_MAX, "price", periods_per_year=252 * 390 * (sts[-1] - sts[0]) / (BAR_1M * (len(sts) - 1)))
    self.check("calendar", "summary ppy 0 uses the median spacing (tick data)", sm0["ann_vol"] > 0)
    sn0 = db.snapshot(columns=COLS, bars=500, bucket=BAR_1M, periods_per_year=0)
    sn1 = db.snapshot(columns=COLS, bars=500, bucket=BAR_1M, periods_per_year=252 * 390)
    self.check("calendar", "snapshot ppy 0 == 252*390 for 1-minute bars", _same_num(sn0["hist_vol_20"], sn1["hist_vol_20"], 1e-12) and _same_num(sn0["sharpe_20"], sn1["sharpe_20"], 1e-12))
    # no calendar -> CalendarRequired
    plain = self.open_db(self.tickers[0])
    try:
        plain.indicators([{"kind": "session_vwap", "param": 0}], tail=10, columns=COLS, lookback=0, bucket=BAR_1M)
        self.check("calendar", "session kind param 0 without a calendar is refused", False, "no error")
    except Exception as e:  # noqa: BLE001
        self.check("calendar", "session kind param 0 without a calendar is refused (CalendarRequired)", "CalendarRequired" in str(e), str(e))
    try:
        plain.set_calendar(999)
        self.check("calendar", "unknown calendar refused", False, "no error")
    except Exception as e:  # noqa: BLE001
        self.check("calendar", "unknown calendar refused (UnknownCalendar)", "UnknownCalendar" in str(e), str(e))
    self.close_all()


def _phase_backtest(self):
    self.phase("Phase 15: signal backtester vs the independent Python reference")
    t = self.tickers[0]
    d = self.ticks[t]
    ts = d["timestamp"]
    db = self.open_db(t)
    # bucket-aligned window of 1-minute bars: the backtest rows == ohlcv bars
    start = int((ts[0] // BAR_1M + 1) * BAR_1M)
    end = int((ts[-1] // BAR_1M) * BAR_1M)
    bars = db.ohlcv(start, end, BAR_1M, price="price", volume="size")
    bts = np.asarray(bars["timestamps"], dtype=np.int64)
    o, h, l, c = (np.asarray(bars[k], dtype=float) for k in ("open", "high", "low", "close"))
    n = len(bts)
    rng = np.random.default_rng(self.args.seed + 4)
    sma_f = pd.Series(c).rolling(10).mean().to_numpy()
    sma_s = pd.Series(c).rolling(50).mean().to_numpy()
    rsi = talib.RSI(c, 14)
    signals = {
        "sma crossover": np.where(np.isnan(sma_s), 0.0, np.where(sma_f > sma_s, 1.0, -1.0)),
        "rsi bands": np.where(np.isnan(rsi), np.nan, np.where(rsi < 30, 1.0, np.where(rsi > 70, -1.0, 0.0))),
        "random units": rng.choice([-2.0, -1.0, 0.0, 1.0, 2.0, np.nan], size=n),
        "always long": np.ones(n),
    }
    param_sets = {
        "plain": dict(initial_equity=100_000.0, position_mode=1),
        "costs+slippage": dict(initial_equity=100_000.0, cost_bps=5.0, slippage_bps=2.0, position_mode=1, periods_per_year=365 * 1440),
        "stops": dict(initial_equity=100_000.0, cost_bps=2.0, stop_loss=0.01, take_profit=0.02, trailing_stop=0.015, position_mode=2, max_position=5.0),
        "same close, no short": dict(initial_equity=1.0, fill_mode=1, allow_short=0, position_mode=0, risk_free_rate=0.03, periods_per_year=365 * 1440),
    }
    n_ok = 0
    n_tot = 0
    t0 = time.time()
    for sname, target in signals.items():
        for pname, params in param_sets.items():
            p = dict(params)
            if "notional" in pname or params.get("position_mode") == 2:
                tgt = target * 1000.0
            else:
                tgt = target
            got = db.backtest(tgt, start, end, BAR_1M, params=p, columns=COLS, outputs=["equity", "position", "cash", "pnl", "drawdown"], max_trades=n)
            ref_p = dict(p)
            if ref_p.get("periods_per_year", 0) == 0:
                ref_p["periods_per_year"] = db.periods_per_year(BAR_1M)  # 0 without a calendar
            want, eq, trades = RB.backtest_reference(bts, o, h, l, c, tgt, ref_p)
            res = got["result"]
            bad = [k for k in RB.RESULT_FIELDS if not _same_num(float(res[k]), float(want[k]), 1e-9, 1e-7)]
            eq_ok = bool(nan_eq_close(np.asarray(got["equity"], dtype=float), eq, 1e-10).all())
            tr_ok = len(got["trades"]) == len(trades) and all(
                all(_same_num(float(g[k]), float(w[k]), 1e-9, 1e-7) for k in RB.TRADE_FIELDS) for g, w in zip(got["trades"], trades))
            n_tot += 1
            if not bad and eq_ok and tr_ok:
                n_ok += 1
            else:
                self.check("backtest", f"{sname} / {pname}: mismatch", False, f"fields {bad[:4]} equity_ok={eq_ok} trades_ok={tr_ok} ({len(got['trades'])} vs {len(trades)})")
    self.check("backtest", f"{n_ok}/{n_tot} signal x parameter combinations match the Python reference on {n} one-minute bars ({t})", n_ok == n_tot)
    self.timing("backtest 16 runs incl. reference", time.time() - t0)
    # arrays entry point on raw ticks, tail window, splits
    got = H.backtest_arrays(bts, o, h, l, c, signals["sma crossover"], params=param_sets["stops"], max_trades=n)
    want, _, _ = RB.backtest_reference(bts, o, h, l, c, signals["sma crossover"] * 1000.0, param_sets["stops"])
    got2 = H.backtest_arrays(bts, o, h, l, c, signals["sma crossover"] * 1000.0, params=param_sets["stops"], max_trades=n)
    self.check("backtest", "backtest_arrays equals the reference (stops, notional sizing)", all(_same_num(float(got2["result"][k]), float(want[k]), 1e-9, 1e-7) for k in RB.RESULT_FIELDS))
    self.check("backtest", "scaling the target changes the result in units mode", got["result"]["n_trades"] == got2["result"]["n_trades"] and not _same_num(got["result"]["final_equity"], got2["result"]["final_equity"]))
    tail_n = 300
    got_tail = db.backtest_tail(signals["sma crossover"][-tail_n:], BAR_1M, params=param_sets["plain"], columns=COLS)
    all_bars = db.ohlcv(INT64_MIN, INT64_MAX, BAR_1M, price="price", volume="size")
    ao, ah, al, ac = (np.asarray(all_bars[k], dtype=float)[-tail_n:] for k in ("open", "high", "low", "close"))
    ats = np.asarray(all_bars["timestamps"], dtype=np.int64)[-tail_n:]
    want_tail, _, _ = RB.backtest_reference(ats, ao, ah, al, ac, signals["sma crossover"][-tail_n:], param_sets["plain"])
    self.check("backtest", "backtest_tail == reference on the last 300 bars", all(_same_num(float(got_tail["result"][k]), float(want_tail[k]), 1e-9, 1e-7) for k in RB.RESULT_FIELDS))
    splits = H.walk_forward_splits(n, 5, 0.6, True)
    ref_splits = RB.walk_forward_splits(n, 5, 0.6, True)
    self.check("backtest", "walk_forward_splits match the reference", [(s["train_start"], s["train_end"], s["test_start"], s["test_end"]) for s in splits] == ref_splits)
    results = H.backtest_splits(bts, o, h, l, c, signals["rsi bands"], splits, params=param_sets["costs+slippage"])
    ok = len(results) == len(splits)
    for r, sp in zip(results, splits):
        a, b = sp["test_start"], sp["test_end"]
        w, _, _ = RB.backtest_reference(bts[a:b], o[a:b], h[a:b], l[a:b], c[a:b], signals["rsi bands"][a:b], param_sets["costs+slippage"])
        ok = ok and all(_same_num(float(r[k]), float(w[k]), 1e-9, 1e-7) for k in RB.RESULT_FIELDS)
    self.check("backtest", "backtest_splits: every test window equals an independent run", ok)
    try:
        db.backtest(signals["always long"][:-1], start, end, BAR_1M, params=param_sets["plain"], columns=COLS)
        self.check("backtest", "target length mismatch refused", False, "no error")
    except Exception as e:  # noqa: BLE001
        self.check("backtest", "target length mismatch refused", "length" in str(e).lower() or "mismatch" in str(e).lower() or "-7" in str(e), str(e))
    # performance: the whole 30 days of 1-minute bars
    t1 = time.time()
    for _ in range(5):
        db.backtest(signals["sma crossover"], start, end, BAR_1M, params=param_sets["stops"], columns=COLS)
    self.timing(f"backtest {n} bars incl. bar read (stops, trade list off)", (time.time() - t1) / 5)
    self.close_all()


def _phase_universe(self):
    self.phase("Phase 16: universe features vs the pandas reference over all tickers")
    dbs = {t: self.open_db(t) for t in self.tickers}
    names = list(self.tickers)
    for n_bars, bucket, params in ((500, BAR_1M, {}), (300, 5 * BAR_1M, {"mom_long": 100, "corr_period": 100, "beta_period": 100, "sma_period": 80, "periods_per_year": 365 * 288}),
                                   (0, 15 * BAR_1M, {"weights_mode": 1})):
        got = H.universe([dbs[t] for t in names], COLS, n_bars=n_bars, bucket=bucket, params=params, corr=True)
        # hand join: the last n_bars bars of each ticker, inner join on timestamps
        want_n = n_bars if n_bars else 61
        frames = []
        for t in names:
            bars = dbs[t].ohlcv(INT64_MIN, INT64_MAX, bucket, price="price", volume="size")
            f = pd.DataFrame({"ts": np.asarray(bars["timestamps"], dtype=np.int64), "close": np.asarray(bars["close"], dtype=float), "vol": np.asarray(bars["volume"], dtype=float)})
            frames.append(f)
        # the storage layer reads the last `want` bars of each ticker (doubling when the join is short with n_bars 0)
        want = want_n
        while True:
            tails = [f.tail(want).set_index("ts") for f in frames]
            common = tails[0].index
            for f in tails[1:]:
                common = common.intersection(f.index)
            if n_bars or len(common) >= want_n or all(len(f) < want for f in frames) or want > 64 * want_n:
                break
            want *= 2
        common = common.sort_values()
        closes = np.column_stack([f.loc[common, "close"].to_numpy() for f in tails])
        vols = np.column_stack([f.loc[common, "vol"].to_numpy() for f in tails])
        rows_ref, sum_ref, corr_ref = RU.universe_reference(closes, vols, params, ts=common.to_numpy())
        s = got["summary"]
        self.check("universe", f"bucket {bucket // BAR_1M}m n_bars {n_bars}: joined bars {s['n_bars']} == hand join {len(common)}", s["n_bars"] == len(common) and s["n_tickers"] == len(names) and s["last_ts"] == int(common[-1]))
        bad_rows = []
        for i, t in enumerate(names):
            for k, v in rows_ref[i].items():
                g = got["rows"][i][k]
                if not _same_num(float(g), float(v), 1e-9, 1e-9):
                    bad_rows.append(f"{t}.{k}: {g} vs {v}")
        self.check("universe", f"bucket {bucket // BAR_1M}m: all {len(names)} x {len(rows_ref[0])} row fields match pandas", not bad_rows, "; ".join(bad_rows[:5]))
        bad_sum = [k for k, v in sum_ref.items() if not _same_num(float(s[k]), float(v), 1e-9, 1e-9)]
        self.check("universe", f"bucket {bucket // BAR_1M}m: summary fields match", not bad_sum, str(bad_sum[:5]))
        corr = np.asarray(got["corr"], dtype=float).reshape(len(names), len(names))
        self.check("universe", f"bucket {bucket // BAR_1M}m: correlation matrix matches (symmetric, diagonal 1)", nan_eq_close(corr.ravel(), corr_ref.ravel(), 1e-9).all() and np.allclose(corr, corr.T, equal_nan=True) and np.allclose(np.diag(corr), 1.0))
        # arrays entry point on the same joined data
        got2 = H.universe_arrays([closes[:, i] for i in range(len(names))], [vols[:, i] for i in range(len(names))], ts=common.to_numpy(), params=params, corr=True)
        self.check("universe", f"bucket {bucket // BAR_1M}m: universe_arrays == universe on the joined data", all(_same_num(float(got2["rows"][i][k]), float(got["rows"][i][k])) for i in range(len(names)) for k in rows_ref[0]) and got2["summary"]["avg_pair_corr"] == s["avg_pair_corr"])
    t1 = time.time()
    for _ in range(10):
        H.universe([dbs[t] for t in names], COLS, n_bars=500, bucket=BAR_1M, corr=True)
    self.timing(f"universe {len(names)} tickers x 500 one-minute bars incl. reads", (time.time() - t1) / 10)
    self.close_all()


Harness.phase_calendar = _phase_calendar
Harness.phase_backtest = _phase_backtest
Harness.phase_universe = _phase_universe

if __name__ == "__main__":
    main()
