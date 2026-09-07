"""Reference implementations of the HOCDB indicator conventions (numpy) used by
the stress / validation harness. TA-Lib is used where it defines the indicator;
these functions cover the rest (and the seeding conventions HOCDB documents).

All functions take numpy arrays and return arrays of the same length with NaN
in the warm-up region.
"""
import math

import numpy as np

nan = float("nan")


def ema(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    valid = np.where(~np.isnan(x))[0]
    if len(valid) == 0:
        return out
    start = int(valid[0])
    if start + p > len(x):
        return out
    k = 2.0 / (p + 1)
    s = np.mean(x[start:start + p])
    out[start + p - 1] = s
    for i in range(start + p, len(x)):
        s += k * (x[i] - s)
        out[i] = s
    return out


def rma(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    valid = np.where(~np.isnan(x))[0]
    if len(valid) == 0:
        return out
    start = int(valid[0])
    if start + p > len(x):
        return out
    k = 1.0 / p
    s = np.mean(x[start:start + p])
    out[start + p - 1] = s
    for i in range(start + p, len(x)):
        s += k * (x[i] - s)
        out[i] = s
    return out


def roll(x, p, fn):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    for i in range(p - 1, len(x)):
        w = x[i - p + 1:i + 1]
        out[i] = nan if np.any(np.isnan(w)) else fn(w)
    return out


def sma(x, p):
    x = np.asarray(x, float)
    n = len(x)
    out = np.full(n, nan)
    if p > n:
        return out
    bad = np.isnan(x)
    c = np.cumsum(np.insert(np.where(bad, 0.0, x), 0, 0.0))
    out[p - 1:] = (c[p:] - c[:-p]) / p
    # NaN propagation
    if bad.any():
        for i in range(p - 1, n):
            if bad[i - p + 1:i + 1].any():
                out[i] = nan
    return out


def wma(x, p):
    x = np.asarray(x, float)
    out = np.full(len(x), nan)
    w = np.arange(1, p + 1, dtype=float)
    for i in range(p - 1, len(x)):
        win = x[i - p + 1:i + 1]
        out[i] = nan if np.any(np.isnan(win)) else np.dot(win, w) / w.sum()
    return out


def true_range(h, l, c):
    n = len(c)
    tr = np.full(n, nan)
    pc = c[:-1]
    tr[1:] = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - pc), np.abs(l[1:] - pc)))
    return tr


def atr(h, l, c, p):
    return rma(true_range(h, l, c), p)


def hma(x, p):
    half = p // 2
    sq = int(math.floor(math.sqrt(p)))
    d = 2 * wma(x, half) - wma(x, p)
    return wma(d, sq)


def zlema(x, p):
    n = len(x)
    lag = (p - 1) // 2
    d = np.full(n, nan)
    d[lag:] = 2 * x[lag:] - x[:n - lag]
    return ema(d, p)


def vwma(x, v, p):
    return roll(x * v, p, np.sum) / roll(v, p, np.sum)


def vwap(price, v, p):
    if p == 0:
        return np.cumsum(price * v) / np.cumsum(v)
    return roll(price * v, p, np.sum) / roll(v, p, np.sum)


def keltner(h, l, c, p, ap, m):
    mid = ema(c, p)
    a = atr(h, l, c, ap)
    return mid + m * a, mid, mid - m * a


def donchian(h, l, p):
    up = roll(h, p, np.max)
    lo = roll(l, p, np.min)
    return up, (up + lo) / 2, lo


def supertrend(h, l, c, p, m):
    n = len(c)
    a = atr(h, l, c, p)
    line = np.full(n, nan)
    d = np.full(n, nan)
    fu = fl = None
    dr = 1
    for i in range(p, n):
        hl2 = (h[i] + l[i]) / 2
        bu = hl2 + m * a[i]
        bl = hl2 - m * a[i]
        if i == p:
            fu, fl = bu, bl
            dr = 1 if c[i] > bu else (-1 if c[i] < bl else 1)
        else:
            pc = c[i - 1]
            nu = bu if (bu < fu or pc > fu) else fu
            nl = bl if (bl > fl or pc < fl) else fl
            if c[i] > fu:
                dr = 1
            elif c[i] < fl:
                dr = -1
            fu, fl = nu, nl
        d[i] = dr
        line[i] = fl if dr > 0 else fu
    return line, d


def clv(h, l, c):
    r = h - l
    with np.errstate(divide="ignore", invalid="ignore"):
        v = ((c - l) - (h - c)) / r
    v[r == 0] = 0
    return v


def cmf(h, l, c, v, p):
    mfv = clv(h, l, c) * v
    return roll(mfv, p, np.sum) / roll(v, p, np.sum)


def efi(c, v, p):
    n = len(c)
    f = np.full(n, nan)
    f[1:] = (c[1:] - c[:-1]) * v[1:]
    return ema(f, p)


def vortex(h, l, c, p):
    n = len(c)
    tr = true_range(h, l, c)
    vp = np.full(n, nan)
    vm = np.full(n, nan)
    vp[1:] = np.abs(h[1:] - l[:-1])
    vm[1:] = np.abs(l[1:] - h[:-1])
    st = roll(tr, p, np.sum)
    return roll(vp, p, np.sum) / st, roll(vm, p, np.sum) / st


def dpo(x, p):
    n = len(x)
    s = sma(x, p)
    sh = p // 2 + 1
    out = np.full(n, nan)
    start = max(p - 1, sh)
    out[start:] = x[start - sh:n - sh] - s[start:]
    return out


def ao(h, l, f, s):
    hl2 = (h + l) / 2
    return sma(hl2, f) - sma(hl2, s)


def tsi(c, lo, sh, sig):
    n = len(c)
    m = np.full(n, nan)
    m[1:] = c[1:] - c[:-1]
    a = np.abs(m)
    t = 100 * ema(ema(m, lo), sh) / ema(ema(a, lo), sh)
    return t, ema(t, sig)


def cmo(x, p):
    n = len(x)
    d = np.diff(x)
    up = np.maximum(d, 0)
    dn = np.maximum(-d, 0)
    out = np.full(n, nan)
    for i in range(p, n):
        su = up[i - p:i].sum()
        sd = dn[i - p:i].sum()
        out[i] = 0 if su + sd == 0 else 100 * (su - sd) / (su + sd)
    return out


def ppo(c, f, s, sig):
    p = (ema(c, f) - ema(c, s)) / ema(c, s) * 100
    sg = ema(p, sig)
    return p, sg, p - sg


def zscore(x, p):
    return (x - sma(x, p)) / roll(x, p, lambda w: w.std())


def hist_vol(x, p, ppy):
    n = len(x)
    lr = np.full(n, nan)
    lr[1:] = np.log(x[1:] / x[:-1])
    return roll(lr, p, lambda w: w.std(ddof=1)) * (math.sqrt(ppy) if ppy > 0 else 1)


def sharpe(x, p, ppy):
    n = len(x)
    r = np.full(n, nan)
    r[1:] = x[1:] / x[:-1] - 1
    sc = math.sqrt(ppy) if ppy > 0 else 1
    return roll(r, p, lambda w: w.mean() / w.std(ddof=1)) * sc


def sortino(x, p, ppy):
    n = len(x)
    r = np.full(n, nan)
    r[1:] = x[1:] / x[:-1] - 1
    sc = math.sqrt(ppy) if ppy > 0 else 1
    return roll(r, p, lambda w: w.mean() / math.sqrt(np.mean(np.minimum(w, 0) ** 2))) * sc


def drawdown(x):
    return x / np.maximum.accumulate(x) - 1


def percent_rank(x, p):
    n = len(x)
    out = np.full(n, nan)
    for i in range(p, n):
        out[i] = 100.0 * np.sum(x[i - p:i] <= x[i]) / p
    return out


def skew(x, p):
    def f(w):
        m = w.mean()
        v = np.mean((w - m) ** 2)
        return 0 if v == 0 else np.mean((w - m) ** 3) / v ** 1.5
    return roll(x, p, f)


def kurtosis(x, p):
    def f(w):
        m = w.mean()
        v = np.mean((w - m) ** 2)
        return 0 if v == 0 else np.mean((w - m) ** 4) / v ** 2 - 3
    return roll(x, p, f)


def beta(a, b, p):
    n = len(a)
    ra = np.full(n, nan)
    rb = np.full(n, nan)
    ra[1:] = a[1:] / a[:-1] - 1
    rb[1:] = b[1:] / b[:-1] - 1
    out = np.full(n, nan)
    for i in range(p, n):
        x = rb[i - p + 1:i + 1]
        y = ra[i - p + 1:i + 1]
        vx = np.var(x)
        out[i] = 0 if vx == 0 else np.cov(x, y, ddof=0)[0, 1] / vx
    return out


def correl(a, b, p):
    def f(i):
        x = a[i - p + 1:i + 1]
        y = b[i - p + 1:i + 1]
        d = x.std() * y.std()
        return 0 if d == 0 else np.corrcoef(x, y)[0, 1]
    out = np.full(len(a), nan)
    for i in range(p - 1, len(a)):
        out[i] = f(i)
    return out


def linreg_r2(x, p):
    n = len(x)
    out = np.full(n, nan)
    k = np.arange(p, dtype=float)
    for i in range(p - 1, n):
        y = x[i - p + 1:i + 1]
        if y.std() == 0:
            out[i] = 0
        else:
            out[i] = np.corrcoef(k, y)[0, 1] ** 2
    return out


def ichimoku(h, l, c, t, kj, s, disp):
    n = len(c)

    def mid(p):
        return (roll(h, p, np.max) + roll(l, p, np.min)) / 2
    tenkan = mid(t)
    kijun = mid(kj)
    sa_raw = (tenkan + kijun) / 2
    sb_raw = mid(s)
    sa = np.full(n, nan)
    sb = np.full(n, nan)
    sa[disp:] = sa_raw[:n - disp]
    sb[disp:] = sb_raw[:n - disp]
    ch = np.full(n, nan)
    ch[:n - disp] = c[disp:]
    return tenkan, kijun, sa, sb, ch


def heikin_ashi(o, h, l, c):
    n = len(c)
    hc = (o + h + l + c) / 4
    ho = np.empty(n)
    ho[0] = (o[0] + c[0]) / 2
    for i in range(1, n):
        ho[i] = (ho[i - 1] + hc[i - 1]) / 2
    hh = np.maximum(h, np.maximum(ho, hc))
    hl = np.minimum(l, np.minimum(ho, hc))
    return ho, hh, hl, hc


def stoch_rsi_ref(rsi_series, sp, ks, dp):
    r = rsi_series
    lo = roll(r, sp, np.min)
    hi = roll(r, sp, np.max)
    with np.errstate(divide="ignore", invalid="ignore"):
        fk = (r - lo) / (hi - lo) * 100
    fk[(hi - lo) == 0] = 0
    k = sma(fk, ks)
    d = sma(k, dp)
    return k, d


def summary(x, ppy):
    """Mirror of indicators.summary (see INDICATORS.md)."""
    x = np.asarray(x, float)
    n = len(x)
    r = x[1:] / x[:-1] - 1
    lr = np.log(x[1:] / x[:-1])
    peak = np.maximum.accumulate(x)
    dd = x / peak - 1
    longest = cur = 0
    pk = x[0]
    for v in x:
        if v >= pk:
            pk = v
            cur = 0
        else:
            cur += 1
            longest = max(longest, cur)
    if ppy > 0:
        years = (n - 1) / ppy
        ann_return = (x[-1] / x[0]) ** (1 / years) - 1
    else:
        ann_return = x[-1] / x[0] - 1
    sc = math.sqrt(ppy) if ppy > 0 else 1
    m = r.mean()
    pv = np.mean((r - m) ** 2)
    var95 = np.percentile(r, 5)
    gains = r[r > 0]
    losses = r[r < 0]
    lp = np.log(x)
    xs, ys = [], []
    for lag in range(2, min(20, n // 4) + 1):
        d = lp[lag:] - lp[:-lag]
        v = np.mean((d - d.mean()) ** 2)
        if v <= 0:
            continue
        xs.append(math.log(lag))
        ys.append(0.5 * math.log(v))
    hurst = np.polyfit(xs, ys, 1)[0] if len(xs) >= 2 else nan
    dx = x[1:] - x[:-1]
    vx = np.var(x[:-1])
    lam = np.cov(x[:-1], dx, ddof=0)[0, 1] / vx if vx > 0 else nan
    half_life = -math.log(2) / lam if lam < 0 else nan
    std_r = r.std(ddof=1)
    dd_dev = math.sqrt(np.mean(np.minimum(r, 0) ** 2))
    return dict(
        count=n, first=x[0], last=x[-1], min=x.min(), max=x.max(), mean=x.mean(), std=x.std(ddof=1),
        total_return=x[-1] / x[0] - 1, log_return=math.log(x[-1] / x[0]), ann_return=ann_return,
        ann_vol=lr.std(ddof=1) * sc, sharpe=(m / std_r * sc) if std_r > 0 else nan,
        sortino=(m / dd_dev * sc) if dd_dev > 0 else nan,
        max_drawdown=dd.min(), max_drawdown_bars=longest,
        calmar=(ann_return / -dd.min()) if dd.min() < 0 else nan,
        skew=(np.mean((r - m) ** 3) / pv ** 1.5) if pv > 0 else 0,
        kurtosis=(np.mean((r - m) ** 4) / pv ** 2 - 3) if pv > 0 else 0,
        var_95=var95, cvar_95=r[r <= var95].mean(), win_rate=len(gains) / len(r),
        avg_gain=gains.mean() if len(gains) else 0, avg_loss=losses.mean() if len(losses) else 0,
        profit_factor=(gains.sum() / -losses.sum()) if losses.sum() < 0 else (math.inf if gains.sum() > 0 else nan),
        best=r.max(), worst=r.min(), autocorr_1=np.corrcoef(r[1:], r[:-1])[0, 1],
        hurst=hurst, half_life=half_life)
