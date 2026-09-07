"""Independent pandas / numpy reference for the HOCDB universe kernel
(src/universe.zig): cross-sectional momentum / volatility ranks, correlation
matrix, market factor, betas, relative strength, dispersion and breadth over a
watch-list of aligned close series.

Written with pandas idioms on purpose (pct-change style returns, strict
``rolling``-like windows, ``DataFrame.corr(min_periods=3)``, ``Series.rank``)
so that it cross-checks the SIMD kernel rather than mirroring it.

Semantics (identical to the kernel's module documentation):
  * simple returns r[t] = c[t]/c[t-1] - 1 for every return statistic, log
    returns ln(c[t]/c[t-1]) for volatility; non-finite values are missing.
  * market r_m[t] = weighted mean of the finite returns at bar t; weights are
    1 (weights_mode 0) or the mean finite volume over the last corr_period
    bars (weights_mode 1, only with volumes; non-finite / non-positive means
    weigh 0). Market cumulative return = prod(1 + r_m) - 1, market log
    return = ln(1 + r_m).
  * rolling statistics are strict: exactly the last ``period`` observations,
    NaN when fewer exist or when any of them is non-finite.
  * correlations use the last min(corr_period, n_bars - 1) returns,
    pairwise-complete, NaN with fewer than 3 pairs or zero variance,
    clamped to [-1, 1]; diagonal 1.
  * percentile rank = (average_rank - 1) / (m - 1) over the m non-NaN
    entries (0 = lowest, 1 = highest, 0.5 when m == 1); NaN ranked NaN.
  * z_mom_mid uses the population std across finite mom_mid (NaN if 0).
  * breadth = share among tickers where the underlying value is finite.

``universe_reference(closes, volumes, params) -> (rows, summary, corr)``
  closes  : 2-D array-like [n_bars x n_tickers], aligned
  volumes : same shape or None
  params  : dict overriding DEFAULT_PARAMS
  rows    : list of dicts (one per ticker, keys = ROW_FIELDS)
  summary : dict (keys = SUMMARY_FIELDS)
  corr    : numpy [n_tickers x n_tickers]

Run as a script for the self-test whose numbers are also asserted in
src/test_universe.zig ("tied example").
"""
import math
import sys

import numpy as np
import pandas as pd

nan = float("nan")

DEFAULT_PARAMS = dict(
    mom_short=5,
    mom_mid=20,
    mom_long=60,
    vol_period=20,
    corr_period=60,
    sma_period=50,
    beta_period=60,
    periods_per_year=0.0,
    weights_mode=0,
)

ROW_FIELDS = [
    "last_close", "ret_1", "mom_short", "mom_mid", "mom_long", "vol",
    "sma_distance", "beta", "corr_market", "rel_strength", "rank_mom_short",
    "rank_mom_mid", "rank_mom_long", "rank_vol", "rank_rel_strength",
    "z_mom_mid", "avg_corr", "max_corr", "max_corr_index", "idio_vol",
    "volume_ratio",
]

SUMMARY_FIELDS = [
    "n_tickers", "n_bars", "market_ret_1", "market_mom_short",
    "market_mom_mid", "market_mom_long", "market_vol", "dispersion",
    "dispersion_mid", "breadth_sma", "breadth_up", "avg_pair_corr",
    "max_pair_corr", "min_pair_corr", "first_ts", "last_ts",
]


def fin(x):
    """x when finite, NaN otherwise."""
    x = float(x)
    return x if math.isfinite(x) else nan


def pct_rank(values):
    """Percentile rank in [0, 1]: (avg_rank - 1) / (m - 1), NaN excluded."""
    s = pd.Series(np.asarray(values, float))
    r = s.rank(method="average")  # NaN stays NaN
    m = int(s.notna().sum())
    if m == 0:
        return np.full(len(s), nan)
    if m == 1:
        return r.where(r.isna(), 0.5).to_numpy()
    return ((r - 1.0) / (m - 1.0)).to_numpy()


def _tail(frame, k):
    """Last k rows, or None when fewer than k rows exist (strict window)."""
    if k <= 0 or len(frame) < k:
        return None
    return frame.iloc[len(frame) - k:]


def _strict_std(frame, scale):
    return (frame.std(ddof=0, skipna=False) * scale).to_numpy(float)


def universe_reference(closes, volumes=None, params=None, ts=None):
    p = dict(DEFAULT_PARAMS)
    if params:
        p.update(params)
    C = pd.DataFrame(np.asarray(closes, float))
    n, m = C.shape
    V = None if volumes is None else pd.DataFrame(np.asarray(volumes, float))
    if V is not None and V.shape != C.shape:
        raise ValueError("volumes must have the shape of closes")
    ks, km, kl = p["mom_short"], p["mom_mid"], p["mom_long"]
    vp, cp, sp, bp = p["vol_period"], p["corr_period"], p["sma_period"], p["beta_period"]
    ppy = float(p["periods_per_year"])
    scale = math.sqrt(ppy) if ppy > 0 else 1.0

    rows = [{k: (i if k == "max_corr_index" else nan) for k in ROW_FIELDS} for i in range(m)]
    summary = {k: nan for k in SUMMARY_FIELDS}
    summary["n_tickers"], summary["n_bars"] = m, n
    summary["first_ts"] = int(ts[0]) if ts is not None and n > 0 else 0
    summary["last_ts"] = int(ts[-1]) if ts is not None and n > 0 else 0
    corr = np.full((m, m), nan)
    np.fill_diagonal(corr, 1.0)
    if m == 0 or n == 0:
        return rows, summary, corr
    for i in range(m):
        rows[i]["last_close"] = float(C.iloc[-1, i])

    # returns (bar 0 has none); non-finite -> missing
    R = (C / C.shift(1) - 1.0).replace([np.inf, -np.inf], np.nan)
    LR = np.log(C / C.shift(1)).replace([np.inf, -np.inf], np.nan)
    Rr, LRr = R.iloc[1:], LR.iloc[1:]  # the n - 1 returns

    # market weights and factor
    if p["weights_mode"] == 1 and V is not None:
        w = V.iloc[-cp:].mean(skipna=True)  # lenient: all bars when shorter
        w = w.where(np.isfinite(w) & (w > 0), 0.0).to_numpy(float)
    else:
        w = np.ones(m)
    Rn = R.to_numpy(float)
    mask = np.isfinite(Rn)
    with np.errstate(invalid="ignore", divide="ignore"):
        mkt = (np.where(mask, Rn, 0.0) * w[None, :]).sum(axis=1) / (mask * w[None, :]).sum(axis=1)
    mkt = pd.Series(mkt)  # NaN where no ticker is finite (incl. bar 0)
    mr = mkt.iloc[1:]

    # market summary
    summary["market_ret_1"] = fin(mkt.iloc[-1])
    for key, k in (("market_mom_short", ks), ("market_mom_mid", km), ("market_mom_long", kl)):
        t = _tail(mr, k)
        if t is not None:
            summary[key] = fin(np.prod(1.0 + t.to_numpy(float)) - 1.0)
    t = _tail(mr, vp)
    if t is not None:
        summary["market_vol"] = fin(np.log1p(t).std(ddof=0, skipna=False) * scale)

    # per-ticker rolling features (strict windows)
    last = C.iloc[-1].to_numpy(float)
    for key, k in (("mom_short", ks), ("mom_mid", km), ("mom_long", kl)):
        if n - 1 >= k:
            vals = last / C.iloc[-1 - k].to_numpy(float) - 1.0
            for i in range(m):
                rows[i][key] = fin(vals[i])
    for i in range(m):
        rows[i]["ret_1"] = fin(R.iloc[-1, i])
        rows[i]["rel_strength"] = fin(rows[i]["mom_mid"] - summary["market_mom_mid"])
    t = _tail(LRr, vp)
    if t is not None:
        vol = _strict_std(t, scale)
        for i in range(m):
            rows[i]["vol"] = fin(vol[i])
    t = _tail(C, sp)
    if t is not None:
        sma = t.mean(skipna=False).to_numpy(float)
        with np.errstate(invalid="ignore", divide="ignore"):
            dist = (last - sma) / sma
        for i in range(m):
            rows[i]["sma_distance"] = fin(dist[i]) if sma[i] != 0 else nan
    t = _tail(Rr, bp)
    if t is not None:
        X = t.to_numpy(float)
        x = _tail(mr, bp).to_numpy(float)
        xc = x - x.mean()
        var_m = np.mean(xc * xc)
        if var_m > 0:  # False for NaN too
            cov = np.mean((X - X.mean(axis=0)[None, :]) * xc[:, None], axis=0)
            for i in range(m):
                rows[i]["beta"] = fin(cov[i] / var_m)
    cw = min(cp, n - 1)
    Rc = Rr.iloc[len(Rr) - cw:]
    Mc = mr.iloc[len(mr) - cw:]
    for i in range(m):
        c = Rc[i].corr(Mc, min_periods=3) if cw > 0 else nan
        rows[i]["corr_market"] = fin(min(1.0, max(-1.0, c))) if math.isfinite(c) else nan
    t = _tail(Rr, vp)
    if t is not None:
        beta = np.array([rows[i]["beta"] for i in range(m)])
        E = t.to_numpy(float) - beta[None, :] * _tail(mr, vp).to_numpy(float)[:, None]
        idio = np.std(E, axis=0) * scale  # NaN propagates (beta NaN too)
        for i in range(m):
            rows[i]["idio_vol"] = fin(idio[i])
    if V is not None:
        t = _tail(V, vp)
        if t is not None:
            with np.errstate(invalid="ignore", divide="ignore"):
                ratio = V.iloc[-1].to_numpy(float) / t.mean(skipna=False).to_numpy(float)
            for i in range(m):
                rows[i]["volume_ratio"] = fin(ratio[i])

    # correlation matrix: pairwise-complete over the last cw returns
    if cw > 0 and m > 1:
        cm = Rc.corr(min_periods=3).to_numpy(float)
        cm = np.clip(cm, -1.0, 1.0)
        corr = cm
        np.fill_diagonal(corr, 1.0)
    for i in range(m):
        others = np.array([corr[i, j] for j in range(m) if j != i])
        finite = np.isfinite(others)
        if finite.any():
            rows[i]["avg_corr"] = float(others[finite].mean())
            j_best = int(np.nanargmax(others))  # first max on ties
            rows[i]["max_corr"] = float(others[j_best])
            rows[i]["max_corr_index"] = j_best if j_best < i else j_best + 1
    if m > 1:
        upper = corr[np.triu_indices(m, 1)]
        upper = upper[np.isfinite(upper)]
        if upper.size:
            summary["avg_pair_corr"] = float(upper.mean())
            summary["max_pair_corr"] = float(upper.max())
            summary["min_pair_corr"] = float(upper.min())

    # cross-section
    for src, dst in (("mom_short", "rank_mom_short"), ("mom_mid", "rank_mom_mid"),
                     ("mom_long", "rank_mom_long"), ("vol", "rank_vol"),
                     ("rel_strength", "rank_rel_strength")):
        rk = pct_rank([rows[i][src] for i in range(m)])
        for i in range(m):
            rows[i][dst] = float(rk[i])
    mm = pd.Series([rows[i]["mom_mid"] for i in range(m)])
    if mm.notna().any():
        sd = float(mm.std(ddof=0))
        summary["dispersion_mid"] = sd
        if sd > 0:
            z = (mm - mm.mean()) / sd
            for i in range(m):
                rows[i]["z_mom_mid"] = fin(z[i])
    r1 = pd.Series([rows[i]["ret_1"] for i in range(m)])
    if r1.notna().any():
        summary["dispersion"] = float(r1.std(ddof=0))
        summary["breadth_up"] = float((r1 > 0).sum()) / float(r1.notna().sum())
    sd_ = pd.Series([rows[i]["sma_distance"] for i in range(m)])
    if sd_.notna().any():
        summary["breadth_sma"] = float((sd_ > 0).sum()) / float(sd_.notna().sum())
    return rows, summary, corr


# ---------------------------------------------------------------------------
# Tied example (also generated, bit for bit, in src/test_universe.zig)
# ---------------------------------------------------------------------------

def tied_example(n_bars=80, n_tickers=4, nan_prefix=None):
    """Deterministic closes / volumes built only from integer arithmetic and
    IEEE multiplications applied in a fixed order, so Zig and Python produce
    identical bits. ``nan_prefix`` = dict {ticker: bars} blanks the first bars
    of a ticker (late listing)."""
    closes = np.zeros((n_bars, n_tickers))
    volumes = np.zeros((n_bars, n_tickers))
    for i in range(n_tickers):
        c = 100.0 + 10.0 * i
        for t in range(n_bars):
            if t > 0:
                k1 = (7 * t + 3 * i) % 11 - 5
                k2 = (5 * t + i) % 7 - 3
                dr = 0.0005 * (i + 1) + 0.004 * k1 + 0.002 * (i + 1) * k2
                c = c * (1.0 + dr)
            closes[t, i] = c
            volumes[t, i] = 1000.0 * (i + 1) + 100.0 * ((11 * t + 5 * i) % 13)
    if nan_prefix:
        for i, bars in nan_prefix.items():
            closes[:bars, i] = nan
            volumes[:bars, i] = nan
    return closes, volumes


TIED_PARAMS = dict(mom_short=5, mom_mid=20, mom_long=60, vol_period=20,
                   corr_period=60, sma_period=50, beta_period=60,
                   periods_per_year=252.0, weights_mode=0)

# Expected values, asserted here and in src/test_universe.zig (abs 1e-9).
# Keys: ("row", ticker, field) | ("summary", None, field) | ("corr", (i, j), "").
EXPECTED_A = {
    ("row", 0, "mom_short"): -0.011983426780339701,
    ("row", 0, "vol"): 0.21879646344264161,
    ("row", 0, "beta"): 0.24304610360079246,
    ("row", 0, "corr_market"): 0.1158486037889159,
    ("row", 0, "z_mom_mid"): -1.4987602243609328,
    ("row", 0, "idio_vol"): 0.21249457123319324,
    ("row", 0, "volume_ratio"): 1.316614420062696,
    ("row", 1, "sma_distance"): 0.038977353544358884,
    ("row", 1, "rel_strength"): -0.0012511260506085087,
    ("row", 2, "mom_long"): 0.10044578231065526,
    ("row", 2, "rank_vol"): 0.6666666666666666,
    ("row", 2, "avg_corr"): -0.12292024264300071,
    ("row", 3, "beta"): 1.9468547041468531,
    ("row", 3, "max_corr"): 0.05397517554593363,
    ("row", 3, "max_corr_index"): 2,
    ("summary", None, "market_mom_mid"): 0.019707995246545984,
    ("summary", None, "market_vol"): 0.10920164503958005,
    ("summary", None, "dispersion"): 0.013107726728918344,
    ("summary", None, "avg_pair_corr"): -0.14392888331800396,
    ("summary", None, "min_pair_corr"): -0.395160397997002,
    ("corr", (0, 2), ""): -0.395160397997002,
    ("corr", (1, 3), ""): -0.27085962531368346,
}
EXPECTED_B = {
    ("row", 0, "beta"): -0.30791567284597654,
    ("row", 1, "rank_mom_long"): 0.5,
    ("row", 2, "corr_market"): 0.5239365729764366,
    ("row", 3, "mom_long"): nan,
    ("row", 3, "beta"): nan,
    ("row", 3, "idio_vol"): nan,
    ("row", 3, "corr_market"): 0.7900585420715146,
    ("row", 3, "sma_distance"): 0.04738841868195653,
    ("row", 3, "avg_corr"): -0.13675903096642864,
    ("summary", None, "market_ret_1"): 0.013203055652688502,
    ("summary", None, "market_mom_long"): 0.08603224010778687,
    ("summary", None, "market_vol"): 0.13885650431334112,
    ("corr", (0, 3), ""): -0.15506255454506374,
}


def _check(name, got, want, tol=1e-9):
    if isinstance(want, int):
        ok = int(got) == want
    elif math.isnan(want):
        ok = math.isnan(got)
    else:
        ok = abs(got - want) <= tol
    status = "ok " if ok else "BAD"
    print("  %s %-28s got=%.17g want=%.17g" % (status, name, got, want))
    return ok


def _dump(rows, summary, corr):
    for i, r in enumerate(rows):
        print("  ticker %d" % i)
        for k in ROW_FIELDS:
            print("    %-18s %r" % (k, r[k]))
    print("  summary")
    for k in SUMMARY_FIELDS:
        print("    %-18s %r" % (k, summary[k]))
    print("  corr")
    for i in range(len(corr)):
        print("    " + " ".join("%r" % v for v in corr[i]))


def main(argv):
    ok = True
    print("universe reference self-test")
    print("example A: 4 tickers x 80 bars, weights_mode 0, ppy 252")
    ca, va = tied_example()
    rows, summary, corr = universe_reference(ca, va, TIED_PARAMS)
    if "--dump" in argv:
        _dump(rows, summary, corr)
    for name, want in EXPECTED_A.items():
        kind, idx, field = name
        got = rows[idx][field] if kind == "row" else summary[field] if kind == "summary" else corr[idx[0], idx[1]]
        ok &= _check("%s[%s].%s" % (kind, idx, field), got, want)

    print("example B: ticker 3 listed at bar 30 (NaN before), weights_mode 1")
    cb, vb = tied_example(nan_prefix={3: 30})
    pb = dict(TIED_PARAMS, weights_mode=1)
    rows, summary, corr = universe_reference(cb, vb, pb)
    if "--dump" in argv:
        _dump(rows, summary, corr)
    for name, want in EXPECTED_B.items():
        kind, idx, field = name
        got = rows[idx][field] if kind == "row" else summary[field] if kind == "summary" else corr[idx[0], idx[1]]
        ok &= _check("%s[%s].%s" % (kind, idx, field), got, want)

    # rank helper sanity
    r = pct_rank([3.0, 1.0, 2.0, nan, 1.0])
    ok &= np.allclose(r[:3], [1.0, 0.16666666666666666, 0.6666666666666666]) and math.isnan(r[3]) and abs(r[4] - r[1]) < 1e-15
    ok &= float(pct_rank([7.0])[0]) == 0.5
    ok &= np.allclose(pct_rank([2.0, 2.0, 2.0]), [0.5, 0.5, 0.5])
    print("rank helper: %s" % ("ok" if ok else "BAD"))
    print("RESULT: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
