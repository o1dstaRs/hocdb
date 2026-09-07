#!/usr/bin/env python3
"""Generate src/test_indicators_golden.zig.

Reference values come from TA-Lib where TA-Lib defines the indicator and from
explicit numpy implementations of the documented HOCDB conventions otherwise.
Run with a Python that has numpy + TA-Lib (see CONTRIBUTING.md):

    python3 scripts/gen_indicator_golden.py > src/test_indicators_golden.zig
"""
import math
import sys
import numpy as np

try:
    import talib
except ImportError:  # pragma: no cover
    sys.stderr.write("TA-Lib python package required (pip install TA-Lib)\n")
    sys.exit(1)

N = 400
rng = np.random.default_rng(42)

# --- synthetic but realistic OHLCV -------------------------------------------
ret = rng.normal(0.0004, 0.012, N)
close = 100.0 * np.exp(np.cumsum(ret))
open_ = np.empty(N)
open_[0] = 100.0
open_[1:] = close[:-1] * (1 + rng.normal(0, 0.002, N - 1))
wick_up = np.abs(rng.normal(0, 0.006, N))
wick_dn = np.abs(rng.normal(0, 0.006, N))
high = np.maximum(open_, close) * (1 + wick_up)
low = np.minimum(open_, close) * (1 - wick_dn)
volume = np.exp(rng.normal(np.log(1e6), 0.5, N)).round()
bench = 50.0 * np.exp(np.cumsum(0.6 * ret + rng.normal(0.0002, 0.008, N)))
ts = np.arange(N, dtype=np.int64) * 60
half_spread = close * (0.0002 + np.abs(rng.normal(0, 0.0001, N)))
bid = close - half_spread
ask = close + half_spread
side = (rng.random(N) < 0.55).astype(float)

nan = float("nan")

# --- reference helpers (HOCDB conventions) ------------------------------------

def ema_ref(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    start = int(np.argmax(~np.isnan(x))) if np.any(~np.isnan(x)) else len(x)
    if start + p > len(x):
        return out
    k = 2.0 / (p + 1)
    s = np.mean(x[start:start + p])
    out[start + p - 1] = s
    for i in range(start + p, len(x)):
        s += k * (x[i] - s)
        out[i] = s
    return out


def rma_ref(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    start = int(np.argmax(~np.isnan(x)))
    if start + p > len(x):
        return out
    k = 1.0 / p
    s = np.mean(x[start:start + p])
    out[start + p - 1] = s
    for i in range(start + p, len(x)):
        s += k * (x[i] - s)
        out[i] = s
    return out


def sma_ref(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    for i in range(p - 1, len(x)):
        w = x[i - p + 1:i + 1]
        out[i] = nan if np.any(np.isnan(w)) else w.mean()
    return out


def wma_ref(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    w = np.arange(1, p + 1, dtype=float)
    for i in range(p - 1, len(x)):
        win = x[i - p + 1:i + 1]
        out[i] = nan if np.any(np.isnan(win)) else np.dot(win, w) / w.sum()
    return out


def roll(x, p, fn):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    for i in range(p - 1, len(x)):
        w = x[i - p + 1:i + 1]
        out[i] = nan if np.any(np.isnan(w)) else fn(w)
    return out


def tr_ref():
    tr = np.full(N, nan)
    for i in range(1, N):
        pc = close[i - 1]
        tr[i] = max(high[i] - low[i], abs(high[i] - pc), abs(low[i] - pc))
    return tr


def atr_ref(p):
    return rma_ref(tr_ref(), p)


def hma_ref(x, p):
    half = p // 2
    sq = int(math.floor(math.sqrt(p)))
    d = 2 * wma_ref(x, half) - wma_ref(x, p)
    return wma_ref(d, sq)


def zlema_ref(x, p):
    lag = (p - 1) // 2
    d = np.full(N, nan)
    d[lag:] = 2 * x[lag:] - x[:N - lag]
    return ema_ref(d, p)


def vwma_ref(x, v, p):
    return roll(x * v, p, np.sum) / roll(v, p, np.sum)


def vwap_ref(price, v, p):
    if p == 0:
        return np.cumsum(price * v) / np.cumsum(v)
    return roll(price * v, p, np.sum) / roll(v, p, np.sum)


def keltner_ref(p, ap, m):
    mid = ema_ref(close, p)
    a = atr_ref(ap)
    return mid + m * a, mid, mid - m * a


def donchian_ref(p):
    up = roll(high, p, np.max)
    lo = roll(low, p, np.min)
    return up, (up + lo) / 2, lo


def supertrend_ref(p, m):
    a = atr_ref(p)
    line = np.full(N, nan)
    d = np.full(N, nan)
    fu = fl = None
    dr = 1
    for i in range(p, N):
        hl2 = (high[i] + low[i]) / 2
        bu = hl2 + m * a[i]
        bl = hl2 - m * a[i]
        if i == p:
            fu, fl = bu, bl
            dr = 1 if close[i] > bu else (-1 if close[i] < bl else 1)
        else:
            pc = close[i - 1]
            nu = bu if (bu < fu or pc > fu) else fu
            nl = bl if (bl > fl or pc < fl) else fl
            if close[i] > fu:
                dr = 1
            elif close[i] < fl:
                dr = -1
            fu, fl = nu, nl
        d[i] = dr
        line[i] = fl if dr > 0 else fu
    return line, d


def clv_ref():
    r = high - low
    with np.errstate(divide="ignore", invalid="ignore"):
        c = ((close - low) - (high - close)) / r
    c[r == 0] = 0
    return c


def cmf_ref(p):
    mfv = clv_ref() * volume
    return roll(mfv, p, np.sum) / roll(volume, p, np.sum)


def efi_ref(p):
    f = np.full(N, nan)
    f[1:] = (close[1:] - close[:-1]) * volume[1:]
    return ema_ref(f, p)


def vortex_ref(p):
    tr = tr_ref()
    vp = np.full(N, nan)
    vm = np.full(N, nan)
    vp[1:] = np.abs(high[1:] - low[:-1])
    vm[1:] = np.abs(low[1:] - high[:-1])
    st = roll(tr, p, np.sum)
    return roll(vp, p, np.sum) / st, roll(vm, p, np.sum) / st


def dpo_ref(x, p):
    s = sma_ref(x, p)
    sh = p // 2 + 1
    out = np.full(N, nan)
    start = max(p - 1, sh)
    out[start:] = x[start - sh:N - sh] - s[start:]
    return out


def ao_ref(f, s):
    hl2 = (high + low) / 2
    return sma_ref(hl2, f) - sma_ref(hl2, s)


def tsi_ref(lo, sh, sig):
    m = np.full(N, nan)
    m[1:] = close[1:] - close[:-1]
    a = np.abs(m)
    t = 100 * ema_ref(ema_ref(m, lo), sh) / ema_ref(ema_ref(a, lo), sh)
    return t, ema_ref(t, sig)


def cmo_ref(x, p):
    d = np.diff(x)
    up = np.maximum(d, 0)
    dn = np.maximum(-d, 0)
    out = np.full(N, nan)
    for i in range(p, N):
        su = up[i - p:i].sum()
        sd = dn[i - p:i].sum()
        out[i] = 0 if su + sd == 0 else 100 * (su - sd) / (su + sd)
    return out


def ppo_ref(f, s, sig):
    p = (ema_ref(close, f) - ema_ref(close, s)) / ema_ref(close, s) * 100
    sg = ema_ref(p, sig)
    return p, sg, p - sg


def macd_signal_ref():
    m, s, h = talib.MACD(close, 12, 26, 9)
    return m, s, h


def zscore_ref(x, p):
    return (x - sma_ref(x, p)) / roll(x, p, lambda w: w.std())


def hist_vol_ref(x, p, ppy):
    lr = np.full(N, nan)
    lr[1:] = np.log(x[1:] / x[:-1])
    return roll(lr, p, lambda w: w.std(ddof=1)) * (math.sqrt(ppy) if ppy > 0 else 1)


def sharpe_ref(x, p, ppy):
    r = np.full(N, nan)
    r[1:] = x[1:] / x[:-1] - 1
    sc = math.sqrt(ppy) if ppy > 0 else 1
    return roll(r, p, lambda w: w.mean() / w.std(ddof=1)) * sc


def sortino_ref(x, p, ppy):
    r = np.full(N, nan)
    r[1:] = x[1:] / x[:-1] - 1
    sc = math.sqrt(ppy) if ppy > 0 else 1
    return roll(r, p, lambda w: w.mean() / math.sqrt(np.mean(np.minimum(w, 0) ** 2))) * sc


def drawdown_ref(x):
    return x / np.maximum.accumulate(x) - 1


def percent_rank_ref(x, p):
    out = np.full(N, nan)
    for i in range(p, N):
        out[i] = 100.0 * np.sum(x[i - p:i] <= x[i]) / p
    return out


def skew_ref(x, p):
    def f(w):
        m = w.mean()
        v = np.mean((w - m) ** 2)
        return 0 if v == 0 else np.mean((w - m) ** 3) / v ** 1.5
    return roll(x, p, f)


def kurt_ref(x, p):
    def f(w):
        m = w.mean()
        v = np.mean((w - m) ** 2)
        return 0 if v == 0 else np.mean((w - m) ** 4) / v ** 2 - 3
    return roll(x, p, f)


def beta_ref(a, b, p):
    ra = np.full(N, nan)
    rb = np.full(N, nan)
    ra[1:] = a[1:] / a[:-1] - 1
    rb[1:] = b[1:] / b[:-1] - 1
    out = np.full(N, nan)
    for i in range(p, N):
        x = rb[i - p + 1:i + 1]
        y = ra[i - p + 1:i + 1]
        out[i] = np.cov(x, y, ddof=0)[0, 1] / np.var(x)
    return out


def linreg_r2_ref(x, p):
    out = np.full(N, nan)
    k = np.arange(p, dtype=float)
    for i in range(p - 1, N):
        y = x[i - p + 1:i + 1]
        out[i] = np.corrcoef(k, y)[0, 1] ** 2
    return out


def ichimoku_ref(t, kj, s, disp):
    def mid(p):
        return (roll(high, p, np.max) + roll(low, p, np.min)) / 2
    tenkan = mid(t)
    kijun = mid(kj)
    sa_raw = (tenkan + kijun) / 2
    sb_raw = mid(s)
    sa = np.full(N, nan)
    sb = np.full(N, nan)
    sa[disp:] = sa_raw[:N - disp]
    sb[disp:] = sb_raw[:N - disp]
    ch = np.full(N, nan)
    ch[:N - disp] = close[disp:]
    return tenkan, kijun, sa, sb, ch


def heikin_ashi_ref():
    hc = (open_ + high + low + close) / 4
    ho = np.empty(N)
    ho[0] = (open_[0] + close[0]) / 2
    for i in range(1, N):
        ho[i] = (ho[i - 1] + hc[i - 1]) / 2
    hh = np.maximum(high, np.maximum(ho, hc))
    hl = np.minimum(low, np.minimum(ho, hc))
    return ho, hh, hl, hc


def stochrsi_ref(rp, sp, ks, dp):
    r = talib.RSI(close, rp)
    lo = roll(r, sp, np.min)
    hi = roll(r, sp, np.max)
    with np.errstate(divide="ignore", invalid="ignore"):
        fk = (r - lo) / (hi - lo) * 100
    fk[(hi - lo) == 0] = 0
    k = sma_ref(fk, ks)
    d = sma_ref(k, dp)
    return k, d



# --- summary reference -------------------------------------------------------
def summary_ref(x, ppy):
    n = len(x)
    r = x[1:] / x[:-1] - 1
    lr = np.log(x[1:] / x[:-1])
    peak = np.maximum.accumulate(x)
    dd = x / peak - 1
    # longest stretch below a prior peak
    longest = cur = 0
    pk = x[0]
    for v in x:
        if v >= pk:
            pk = v
            cur = 0
        else:
            cur += 1
            longest = max(longest, cur)
    years = (n - 1) / ppy
    ann_return = (x[-1] / x[0]) ** (1 / years) - 1
    sc = math.sqrt(ppy)
    m = r.mean()
    pv = np.mean((r - m) ** 2)
    var95 = np.percentile(r, 5)
    gains = r[r > 0]
    losses = r[r < 0]
    # hurst: log prices, lags 2..min(20, n//4)
    lp = np.log(x)
    xs, ys = [], []
    for lag in range(2, min(20, n // 4) + 1):
        d = lp[lag:] - lp[:-lag]
        v = np.mean((d - d.mean()) ** 2)
        xs.append(math.log(lag)); ys.append(0.5 * math.log(v))
    hurst = np.polyfit(xs, ys, 1)[0]
    dx = x[1:] - x[:-1]
    lam = np.cov(x[:-1], dx, ddof=0)[0, 1] / np.var(x[:-1])
    half_life = -math.log(2) / lam if lam < 0 else nan
    return dict(
        count=n, first=x[0], last=x[-1], min=x.min(), max=x.max(), mean=x.mean(), std=x.std(ddof=1),
        total_return=x[-1] / x[0] - 1, log_return=math.log(x[-1] / x[0]), ann_return=ann_return,
        ann_vol=lr.std(ddof=1) * sc, sharpe=m / r.std(ddof=1) * sc,
        sortino=m / math.sqrt(np.mean(np.minimum(r, 0) ** 2)) * sc,
        max_drawdown=dd.min(), max_drawdown_bars=longest, calmar=ann_return / -dd.min(),
        skew=np.mean((r - m) ** 3) / pv ** 1.5, kurtosis=np.mean((r - m) ** 4) / pv ** 2 - 3,
        var_95=var95, cvar_95=r[r <= var95].mean(), win_rate=len(gains) / len(r),
        avg_gain=gains.mean(), avg_loss=losses.mean(), profit_factor=gains.sum() / -losses.sum(),
        best=r.max(), worst=r.min(), autocorr_1=np.corrcoef(r[1:], r[:-1])[0, 1],
        hurst=hurst, half_life=half_life)


def resample_ref(bucket):
    keys = ts // bucket
    bars = []
    for k in np.unique(keys):
        sel = keys == k
        bars.append((int(k * bucket), open_[sel][0], high[sel].max(), low[sel].min(), close[sel][-1], volume[sel].sum(), int(sel.sum())))
    return bars

# --- microstructure / pairs / labels / sessions ----------------------------------
def order_flow_ref(v, sd, p):
    signed = np.where(sd != 0, v, -v)
    net = roll(signed, p, np.sum)
    tot = roll(v, p, np.sum)
    return net, np.where(tot == 0, 0, net / tot)


def tick_pressure_ref(x, p):
    sgn = np.zeros(N)
    s_ = 0.0
    for i in range(1, N):
        d = x[i] - x[i - 1]
        if d > 0:
            s_ = 1.0
        elif d < 0:
            s_ = -1.0
        sgn[i] = s_
    out = np.full(N, nan)
    out[1:] = sma_ref(sgn[1:], p)
    return out


def trade_intensity_ref(t, v, p, ups):
    tr = np.full(N, nan)
    vo = np.full(N, nan)
    vs = roll(v, p, np.sum)
    for i in range(p, N):
        dt = (t[i] - t[i - p]) / ups
        tr[i] = p / dt
        vo[i] = vs[i] / dt
    return tr, vo


def amihud_ref(x, v, p):
    il = np.full(N, nan)
    il[1:] = np.abs(x[1:] / x[:-1] - 1) / (x[1:] * v[1:])
    out = np.full(N, nan)
    out[1:] = sma_ref(il[1:], p)
    return out


def realized_vol_ref(x, p, ppy):
    sq = np.full(N, nan)
    sq[1:] = np.log(x[1:] / x[:-1]) ** 2
    out = np.full(N, nan)
    out[1:] = np.sqrt(sma_ref(sq[1:], p) * (ppy if ppy > 0 else 1))
    return out


def forward_return_ref(x, h):
    r = np.full(N, nan); mx = np.full(N, nan); mn = np.full(N, nan)
    for i in range(N - h):
        w = x[i + 1:i + h + 1]
        r[i] = x[i + h] / x[i] - 1
        mx[i] = w.max() / x[i] - 1
        mn[i] = w.min() / x[i] - 1
    return r, mx, mn


def triple_barrier_ref(x, h, up, dn):
    lab = np.full(N, nan); r = np.full(N, nan); b = np.full(N, nan)
    for i in range(N):
        hit = False
        for j in range(i + 1, min(N, i + h + 1)):
            rr = x[j] / x[i] - 1
            if rr >= up:
                lab[i], r[i], b[i], hit = 1, rr, j - i, True
                break
            if rr <= -dn:
                lab[i], r[i], b[i], hit = -1, rr, j - i, True
                break
        if not hit and i + h < N:
            lab[i], r[i], b[i] = 0, x[i + h] / x[i] - 1, h
    return lab, r, b


def session_refs(t, o, h, l, c, v, length, offset, or_rows):
    sid = (t - offset) // length
    tp = (h + l + c) / 3
    vwap = np.full(N, nan); so = np.full(N, nan); sh = np.full(N, nan); sl = np.full(N, nan); sr = np.full(N, nan)
    orh = np.full(N, nan); orl = np.full(N, nan); orb = np.full(N, nan)
    pp = np.full(N, nan); r1 = np.full(N, nan); s1 = np.full(N, nan); r2 = np.full(N, nan); s2 = np.full(N, nan)
    prev = None
    for sess in np.unique(sid):
        idx = np.where(sid == sess)[0]
        pv = np.cumsum(tp[idx] * v[idx]); vv = np.cumsum(v[idx])
        vwap[idx] = pv / vv
        so[idx] = o[idx[0]]
        sh[idx] = np.maximum.accumulate(h[idx]); sl[idx] = np.minimum.accumulate(l[idx])
        sr[idx] = c[idx] / o[idx[0]] - 1
        rh = np.maximum.accumulate(h[idx][:or_rows]); rl = np.minimum.accumulate(l[idx][:or_rows])
        for k, i in enumerate(idx):
            if k < or_rows:
                orh[i], orl[i] = rh[k], rl[k]
            else:
                orh[i], orl[i] = rh[-1], rl[-1]
                orb[i] = 1 if c[i] > rh[-1] else (-1 if c[i] < rl[-1] else 0)
        if prev is not None:
            ph, pl, pc = prev
            p_ = (ph + pl + pc) / 3
            pp[idx], r1[idx], s1[idx], r2[idx], s2[idx] = p_, 2 * p_ - pl, 2 * p_ - ph, p_ + (ph - pl), p_ - (ph - pl)
        prev = (h[idx].max(), l[idx].min(), c[idx[-1]])
    return vwap, so, sh, sl, sr, orh, orl, orb, pp, r1, s1, r2, s2


# --- golden table -------------------------------------------------------------
# entries: (name, spec dict, [outputs...]) where outputs is a list of reference
# arrays, one per output of the kind.
K = dict(sma=1, ema=2, wma=3, dema=4, tema=5, trima=6, kama=7, hma=8, zlema=9, vwma=10, rma=11,
         rsi=20, macd=21, ppo=22, stoch=23, stoch_rsi=24, cci=25, willr=26, mom=27, roc=28, cmo=29,
         trix=30, ultosc=31, ao=32, tsi=33, bop=34, dpo=35, adx=40, aroon=41, psar=42, supertrend=43,
         vortex=44, ichimoku=45, linreg=46, atr=60, natr=61, true_range=62, bbands=63, keltner=64,
         donchian=65, stddev=66, variance=67, hist_vol=68, obv=80, vwap=81, mfi=82, cmf=83, ad=84,
         adosc=85, efi=86, returns=100, log_returns=101, zscore=102, percent_rank=103, rolling_min=104,
         rolling_max=105, drawdown=106, sharpe=107, sortino=108, correl=109, beta=110, skew=111,
         kurtosis=112, typical_price=120, median_price=121, heikin_ashi=122,
         spread=130, order_flow=131, tick_pressure=132, trade_intensity=133, amihud=134, realized_vol=135,
         series=140, series2=141, ratio=142, ratio_zscore=143, rel_strength=144, forward_return=150, triple_barrier=151,
         session_vwap=160, session_range=161, opening_range=162, pivots=163)

entries = []


def add(name, kind, outputs, tol=1e-8, **spec):
    entries.append((name, kind, spec, [np.asarray(o, float) for o in outputs], tol))


u, m, l = talib.BBANDS(close, 20, 2, 2, 0)
add("sma_20", "sma", [talib.SMA(close, 20)], period=20)
add("sma_3", "sma", [talib.SMA(close, 3)], period=3)
add("ema_20", "ema", [talib.EMA(close, 20)], period=20)
add("ema_2", "ema", [talib.EMA(close, 2)], period=2)
add("wma_20", "wma", [talib.WMA(close, 20)], period=20)
add("dema_20", "dema", [talib.DEMA(close, 20)], period=20)
add("tema_20", "tema", [talib.TEMA(close, 20)], period=20)
add("trima_20", "trima", [talib.TRIMA(close, 20)], period=20)
add("trima_15", "trima", [talib.TRIMA(close, 15)], period=15)
add("kama_10", "kama", [talib.KAMA(close, 10)], period=10)
add("kama_30", "kama", [talib.KAMA(close, 30)], period=30)
add("hma_20", "hma", [hma_ref(close, 20)], period=20)
add("zlema_20", "zlema", [zlema_ref(close, 20)], period=20)
add("vwma_20", "vwma", [vwma_ref(close, volume, 20)], period=20)
add("rma_14", "rma", [rma_ref(close, 14)], period=14)
add("rsi_14", "rsi", [talib.RSI(close, 14)], period=14)
add("rsi_2", "rsi", [talib.RSI(close, 2)], period=2)
add("macd_default", "macd", list(talib.MACD(close, 12, 26, 9)))
add("macd_5_35_5", "macd", list(talib.MACD(close, 5, 35, 5)), period=5, period2=35, period3=5)
add("ppo_default", "ppo", [talib.PPO(close, 12, 26, 1)] + [ppo_ref(12, 26, 9)[1], ppo_ref(12, 26, 9)[2]])
add("stoch_14_3_3", "stoch", list(talib.STOCH(high, low, close, 14, 3, 0, 3, 0)))
add("stoch_5_1_3", "stoch", list(talib.STOCH(high, low, close, 5, 1, 0, 3, 0)), period=5, period2=1, period3=3)
add("stochrsi_talib", "stoch_rsi", list(talib.STOCHRSI(close, 14, 5, 3, 0)), period=14, period2=5, period3=1, period4=3)
add("stochrsi_default", "stoch_rsi", list(stochrsi_ref(14, 14, 3, 3)))
add("cci_20", "cci", [talib.CCI(high, low, close, 20)], period=20)
add("willr_14", "willr", [talib.WILLR(high, low, close, 14)], period=14)
add("mom_10", "mom", [talib.MOM(close, 10)], period=10)
add("roc_10", "roc", [talib.ROC(close, 10)], period=10)
add("cmo_14", "cmo", [cmo_ref(close, 14)], period=14)
add("trix_15", "trix", [talib.TRIX(close, 15)], period=15)
add("ultosc", "ultosc", [talib.ULTOSC(high, low, close, 7, 14, 28)])
add("ao", "ao", [ao_ref(5, 34)])
add("tsi", "tsi", list(tsi_ref(25, 13, 13)))
add("bop", "bop", [talib.BOP(open_, high, low, close)])
add("dpo_20", "dpo", [dpo_ref(close, 20)], period=20)
add("adx_14", "adx", [talib.ADX(high, low, close, 14), talib.PLUS_DI(high, low, close, 14), talib.MINUS_DI(high, low, close, 14)], period=14)
add("adx_7", "adx", [talib.ADX(high, low, close, 7), talib.PLUS_DI(high, low, close, 7), talib.MINUS_DI(high, low, close, 7)], period=7)
add("aroon_25", "aroon", [talib.AROON(high, low, 25)[1], talib.AROON(high, low, 25)[0], talib.AROONOSC(high, low, 25)], period=25)
add("psar", "psar", [talib.SAR(high, low, 0.02, 0.2), np.full(N, nan)])
add("psar_custom", "psar", [talib.SAR(high, low, 0.03, 0.3), np.full(N, nan)], param=0.03, param2=0.3)
add("supertrend", "supertrend", list(supertrend_ref(10, 3.0)))
add("vortex_14", "vortex", list(vortex_ref(14)), period=14)
add("ichimoku", "ichimoku", list(ichimoku_ref(9, 26, 52, 26)))
add("linreg_20", "linreg", [talib.LINEARREG(close, 20), talib.LINEARREG_SLOPE(close, 20), talib.LINEARREG_INTERCEPT(close, 20), linreg_r2_ref(close, 20)], period=20, tol=1e-7)
add("atr_14", "atr", [talib.ATR(high, low, close, 14)], period=14)
add("natr_14", "natr", [talib.NATR(high, low, close, 14)], period=14)
add("true_range", "true_range", [talib.TRANGE(high, low, close)])
add("bbands_20", "bbands", [u, m, l, (close - l) / (u - l), (u - l) / m], period=20)
add("bbands_10_1.5", "bbands", list(talib.BBANDS(close, 10, 1.5, 1.5, 0)) + [np.full(N, nan)] * 2, period=10, param=1.5)
add("keltner", "keltner", list(keltner_ref(20, 10, 2.0)))
add("donchian_20", "donchian", list(donchian_ref(20)), period=20)
add("stddev_20", "stddev", [talib.STDDEV(close, 20, 1)], period=20)
add("variance_20", "variance", [talib.VAR(close, 20, 1)], period=20)
add("hist_vol_20", "hist_vol", [hist_vol_ref(close, 20, 252)], period=20, param=252)
add("obv", "obv", [talib.OBV(close, volume)])
add("vwap_cum", "vwap", [vwap_ref((high + low + close) / 3, volume, 0)])
add("vwap_20", "vwap", [vwap_ref((high + low + close) / 3, volume, 20)], period=20)
add("mfi_14", "mfi", [talib.MFI(high, low, close, volume, 14)], period=14)
add("cmf_20", "cmf", [cmf_ref(20)], period=20)
add("ad", "ad", [talib.AD(high, low, close, volume)])
add("adosc", "adosc", [talib.ADOSC(high, low, close, volume, 3, 10)])
add("efi_13", "efi", [efi_ref(13)], period=13)
add("returns_1", "returns", [np.concatenate([[nan], close[1:] / close[:-1] - 1])], period=1)
add("returns_5", "returns", [np.concatenate([[nan] * 5, close[5:] / close[:-5] - 1])], period=5)
add("log_returns_1", "log_returns", [np.concatenate([[nan], np.log(close[1:] / close[:-1])])], period=1)
add("zscore_20", "zscore", [zscore_ref(close, 20)], period=20)
add("percent_rank_20", "percent_rank", [percent_rank_ref(close, 20)], period=20)
add("rolling_min_20", "rolling_min", [talib.MIN(close, 20)], period=20)
add("rolling_max_20", "rolling_max", [talib.MAX(close, 20)], period=20)
add("rolling_max_7", "rolling_max", [talib.MAX(close, 7)], period=7)
add("drawdown", "drawdown", [drawdown_ref(close)])
add("sharpe_20", "sharpe", [sharpe_ref(close, 20, 252)], period=20, param=252)
add("sortino_20", "sortino", [sortino_ref(close, 20, 252)], period=20, param=252)
add("correl_20", "correl", [talib.CORREL(close, bench, 20)], period=20, field_index2=1)
add("beta_20", "beta", [beta_ref(close, bench, 20)], period=20, field_index2=1, tol=1e-7)
add("skew_20", "skew", [skew_ref(close, 20)], period=20)
add("kurtosis_20", "kurtosis", [kurt_ref(close, 20)], period=20)
add("typical_price", "typical_price", [talib.TYPPRICE(high, low, close)])
add("median_price", "median_price", [talib.MEDPRICE(high, low)])
add("heikin_ashi", "heikin_ashi", list(heikin_ashi_ref()))

add("spread", "spread", [ask - bid, (ask - bid) / ((ask + bid) / 2) * 1e4])
add("order_flow_20", "order_flow", list(order_flow_ref(volume, side, 20)), period=20)
add("tick_pressure_50", "tick_pressure", [tick_pressure_ref(close, 50)], period=50)
add("trade_intensity_100", "trade_intensity", list(trade_intensity_ref(ts, volume, 100, 1.0)), period=100, param=1.0)
add("amihud_20", "amihud", [amihud_ref(close, volume, 20)], period=20)
add("realized_vol_20", "realized_vol", [realized_vol_ref(close, 20, 252)], period=20, param=252)
add("series", "series", [close])
add("series2", "series2", [bench], field_index2=1)
add("ratio", "ratio", [close / bench], field_index2=1)
add("ratio_zscore_20", "ratio_zscore", [zscore_ref(close / bench, 20)], period=20, field_index2=1)
add("rel_strength_10", "rel_strength", [talib.ROC(close, 10) - talib.ROC(bench, 10)], period=10, field_index2=1)
add("forward_return_5", "forward_return", list(forward_return_ref(close, 5)), period=5)
add("triple_barrier_20", "triple_barrier", list(triple_barrier_ref(close, 20, 0.01, 0.01)), period=20, param=0.01, param2=0.01)
_sess = session_refs(ts, open_, high, low, close, volume, 3600, 600, 5)
add("session_vwap", "session_vwap", [_sess[0]], param=3600, param2=600)
add("session_range", "session_range", list(_sess[1:5]), param=3600, param2=600)
add("opening_range_5", "opening_range", list(_sess[5:8]), period=5, param=3600, param2=600)
add("pivots", "pivots", list(_sess[8:13]), param=3600, param2=600)

# Cross-check TA-Lib BETA orientation against the numpy definition (documentation aid).
_tb = talib.BETA(bench, close, 20)
_nb = beta_ref(close, bench, 20)
assert np.nanmax(np.abs(_tb[25:] - _nb[25:])) < 1e-6, "TA-Lib BETA(real0=bench, real1=asset) != cov/var(bench)"


def fmt(x):
    if x != x:
        return "nan"
    if math.isinf(x):
        return "inf" if x > 0 else "-inf"
    return repr(float(x))


def arr(name, a):
    body = ", ".join(fmt(v) for v in a)
    return f"pub const {name} = [_]f64{{ {body} }};\n"


out = []
out.append("//! GENERATED by scripts/gen_indicator_golden.py -- do not edit by hand.\n")
out.append("//! Golden reference values from TA-Lib %s and numpy for the HOCDB indicator kernels.\n" % talib.__version__)
out.append("const std = @import(\"std\");\nconst ind = @import(\"indicators.zig\");\n\n")
out.append(f"pub const N: usize = {N};\n")
out.append(arr("open", open_))
out.append(arr("high", high))
out.append(arr("low", low))
out.append(arr("close", close))
out.append(arr("volume", volume))
out.append(arr("bench", bench))
out.append(arr("bid", bid))
out.append(arr("ask", ask))
out.append(arr("side", side))
out.append("pub const ts = blk: { var t: [N]i64 = undefined; for (0..N) |i| t[i] = @as(i64, @intCast(i)) * 60; break :blk t; };\n\n")

out.append("""pub const Check = struct { idx: []const usize, expected: []const f64 };
pub const Entry = struct {
    name: []const u8,
    spec: ind.Spec,
    outputs: []const Check,
    tol: f64,
};

""")

entry_lines = []
for name, kind, spec, outputs, tol in entries:
    checks = []
    for o in outputs:
        valid = np.where(~np.isnan(o))[0]
        if len(valid) == 0:
            checks.append("        .{ .idx = &.{}, .expected = &.{} },")
            continue
        first = valid[0]
        idx = sorted(set([first, first + 1, first + 2] + list(range(first, N, 29)) + [N - 3, N - 2, N - 1]))
        idx = [i for i in idx if i < N and not np.isnan(o[i])]
        checks.append("        .{ .idx = &.{ %s }, .expected = &.{ %s } }," % (
            ", ".join(str(i) for i in idx), ", ".join(fmt(o[i]) for i in idx)))
    sp = ", ".join([f".kind = {K[kind]}"] + [f".{k} = {v}" for k, v in spec.items()])
    entry_lines.append("    .{\n        .name = \"%s\",\n        .spec = .{ %s },\n        .tol = %s,\n        .outputs = &.{\n%s\n        },\n    }," % (
        name, sp, fmt(tol), "\n".join("    " + c for c in checks)))

out.append("pub const entries = [_]Entry{\n" + "\n".join(entry_lines) + "\n};\n\n")


sm = summary_ref(close, 252)
out.append("pub const SummaryExpect = struct { name: []const u8, value: f64 };\n")
out.append("pub const summary_expected = [_]SummaryExpect{\n" + "".join(
    "    .{ .name = \"%s\", .value = %s },\n" % (k, fmt(v)) for k, v in sm.items()) + "};\n\n")
bars = resample_ref(300)
out.append("pub const Bar = struct { ts: i64, open: f64, high: f64, low: f64, close: f64, volume: f64, count: f64 };\n")
out.append("pub const resample_300 = [_]Bar{\n" + "".join(
    "    .{ .ts = %d, .open = %s, .high = %s, .low = %s, .close = %s, .volume = %s, .count = %s },\n" % (
        b[0], fmt(b[1]), fmt(b[2]), fmt(b[3]), fmt(b[4]), fmt(b[5]), fmt(b[6])) for b in bars) + "};\n\n")
out.append("""fn closeEnough(got: f64, want: f64, tol: f64) bool {
    if (want != want) return got != got;
    if (std.math.isInf(want)) return got == want;
    const diff = @abs(got - want);
    return diff <= tol * @max(1.0, @abs(want));
}

test "golden indicators vs TA-Lib / numpy references" {
    const a = std.testing.allocator;
    var failures: usize = 0;
    for (entries) |e| {
        const kind = ind.Kind.fromInt(e.spec.kind).?;
        const n_out = ind.outputCount(kind);
        const bufs = try a.alloc(f64, n_out * N);
        defer a.free(bufs);
        var outs: [8][]f64 = undefined;
        for (0..n_out) |j| outs[j] = bufs[j * N .. (j + 1) * N];
        const cols = ind.Columns{ .open = &open, .high = &high, .low = &low, .close = &close, .volume = &volume, .input2 = &bench, .bid = &bid, .ask = &ask, .side = &side, .time = &ts };
        ind.compute(e.spec, cols, outs[0..n_out], a) catch |err| {
            std.debug.print("{s}: compute failed: {s}\\n", .{ e.name, @errorName(err) });
            failures += 1;
            continue;
        };
        for (e.outputs, 0..) |chk, j| {
            for (chk.idx, chk.expected) |i, want| {
                const got = outs[j][i];
                if (!closeEnough(got, want, e.tol)) {
                    std.debug.print("{s} out[{d}][{d}]: got {e} want {e}\\n", .{ e.name, j, i, got, want });
                    failures += 1;
                }
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
}

test "golden summary vs numpy" {
    const a = std.testing.allocator;
    const s = try ind.summary(&close, 252, a);
    var failures: usize = 0;
    inline for (@typeInfo(ind.Summary).@"struct".fields) |f| {
        for (summary_expected) |e| {
            if (std.mem.eql(u8, e.name, f.name)) {
                const got: f64 = if (f.type == u64) @floatFromInt(@field(s, f.name)) else @field(s, f.name);
                if (!closeEnough(got, e.value, 1e-7)) {
                    std.debug.print("summary.{s}: got {e} want {e}\\n", .{ f.name, got, e.value });
                    failures += 1;
                }
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
}

test "golden resample 300" {
    const a = std.testing.allocator;
    const bars = try ind.resampleOhlcv(&ts, &open, &high, &low, &close, &volume, 300, a);
    defer bars.deinit(a);
    try std.testing.expectEqual(resample_300.len, bars.len());
    for (resample_300, 0..) |b, i| {
        try std.testing.expectEqual(b.ts, bars.ts[i]);
        try std.testing.expectApproxEqRel(b.open, bars.open[i], 1e-12);
        try std.testing.expectApproxEqRel(b.high, bars.high[i], 1e-12);
        try std.testing.expectApproxEqRel(b.low, bars.low[i], 1e-12);
        try std.testing.expectApproxEqRel(b.close, bars.close[i], 1e-12);
        try std.testing.expectApproxEqRel(b.volume, bars.volume[i], 1e-12);
        try std.testing.expectEqual(b.count, bars.count[i]);
    }
}
""")

text = "".join(out)
# Format with `zig fmt` when available so the generated file is fmt-clean.
try:
    import subprocess
    r = subprocess.run(["zig", "fmt", "--stdin"], input=text.encode(), capture_output=True)
    if r.returncode == 0 and r.stdout:
        text = r.stdout.decode()
    else:
        sys.stderr.write("warning: zig fmt failed, emitting unformatted output\n")
except FileNotFoundError:
    sys.stderr.write("warning: zig not found, emitting unformatted output\n")
sys.stdout.write(text)
