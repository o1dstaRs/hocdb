#!/usr/bin/env python3
"""Synthetic but realistic tick data for HOCDB stress / validation runs.

Each ticker gets its own seed, session calendar (24/7 crypto, US equity
hours, FX 24/5), tick size, base volatility, intraday volume/volatility
seasonality, jump process, occasional outages (no ticks) and bid-ask bounce.

    from tickgen import generate, TICKERS
    ticks = generate("BTCUSD", days=30)   # dict of numpy arrays

Timestamps are microseconds since the Unix epoch and strictly increasing.
"""
import math
from typing import Optional
from dataclasses import dataclass

import numpy as np

US = 1_000_000  # microseconds per second
DAY_US = 86_400 * US
START_TS_US = 1_756_684_800 * US  # 2025-09-01 00:00:00 UTC (a Monday)


@dataclass
class TickerSpec:
    name: str
    kind: str  # "crypto" | "equity" | "fx"
    price: float
    tick_size: float
    daily_vol: float  # annualised-ish daily volatility of log price
    ticks_per_sec: float  # mean Poisson rate during active sessions
    base_size: float  # typical trade size
    seed: int
    jump_per_day: float = 0.5  # expected jumps per day
    jump_sigma: float = 0.01  # jump magnitude (log)


TICKERS = [
    TickerSpec("BTCUSD", "crypto", 62_000.0, 0.1, 0.030, 1.2, 0.05, 101, 0.4, 0.012),
    TickerSpec("ETHUSD", "crypto", 3_100.0, 0.01, 0.038, 0.9, 0.8, 102, 0.5, 0.015),
    TickerSpec("SOLUSD", "crypto", 145.0, 0.001, 0.055, 0.6, 12.0, 103, 0.8, 0.02),
    TickerSpec("AAPL", "equity", 228.0, 0.01, 0.014, 2.5, 120.0, 201, 0.15, 0.008),
    TickerSpec("NVDA", "equity", 118.0, 0.01, 0.028, 3.0, 300.0, 202, 0.3, 0.012),
    TickerSpec("TSLA", "equity", 245.0, 0.01, 0.035, 2.8, 150.0, 203, 0.35, 0.015),
    TickerSpec("SPY", "equity", 560.0, 0.01, 0.009, 2.0, 400.0, 204, 0.05, 0.004),
    TickerSpec("EURUSD", "fx", 1.1050, 0.00001, 0.005, 0.8, 1_000_000.0, 301, 0.1, 0.002),
]


def _sessions(spec: TickerSpec, days: int):
    """Yield (start_us, end_us) active windows over `days` days."""
    out = []
    for d in range(days):
        day0 = START_TS_US + d * DAY_US
        weekday = (d + 0) % 7  # START is a Monday -> 0 = Monday
        if spec.kind == "crypto":
            out.append((day0, day0 + DAY_US))
        elif spec.kind == "equity":
            if weekday >= 5:
                continue
            # 13:30 - 20:00 UTC (9:30 - 16:00 ET, EDT)
            out.append((day0 + int(13.5 * 3600) * US, day0 + 20 * 3600 * US))
        elif spec.kind == "fx":
            if weekday == 5:  # Saturday closed
                continue
            if weekday == 6:  # Sunday opens 22:00 UTC
                out.append((day0 + 22 * 3600 * US, day0 + DAY_US))
            elif weekday == 4:  # Friday closes 22:00 UTC
                out.append((day0, day0 + 22 * 3600 * US))
            else:
                out.append((day0, day0 + DAY_US))
    return out


def _seasonality(kind: str, frac_of_session: np.ndarray) -> np.ndarray:
    """Activity multiplier over the session (U-shape for equities)."""
    if kind == "equity":
        return 0.6 + 1.6 * (np.abs(frac_of_session - 0.5) * 2) ** 2
    if kind == "fx":
        return 0.7 + 0.6 * np.sin(np.pi * frac_of_session) ** 2
    # crypto: mild daily cycle
    return 0.8 + 0.4 * np.sin(2 * np.pi * frac_of_session) ** 2


def generate(name: str, days: int = 30, max_ticks: Optional[int] = None):
    spec = next(t for t in TICKERS if t.name == name)
    rng = np.random.default_rng(spec.seed)
    ts_parts, px_parts, sz_parts, bid_parts, ask_parts, side_parts = [], [], [], [], [], []
    log_price = math.log(spec.price)
    per_sec_vol = spec.daily_vol / math.sqrt(86_400)
    regime = 0.0  # slow drift (trend) component
    total = 0
    for s_start, s_end in _sessions(spec, days):
        session_len = (s_end - s_start) / US
        # expected ticks in this session, then Poisson arrivals with seasonality
        n_expected = spec.ticks_per_sec * session_len
        n = int(rng.poisson(n_expected))
        if n == 0:
            continue
        u = np.sort(rng.random(n))
        # thin/thicken by seasonality via rejection sampling
        keep = rng.random(n) < _seasonality(spec.kind, u) / 2.2
        u = u[keep]
        n = len(u)
        if n == 0:
            continue
        # strictly increasing timestamps that stay inside the session
        span = session_len * US - n - 1
        ts = s_start + np.sort((u * span).astype(np.int64)) + np.arange(n, dtype=np.int64)
        # outages: a few random gaps of 1..8 minutes with no ticks
        gaps = rng.integers(0, 4)
        mask = np.ones(n, dtype=bool)
        for _ in range(gaps):
            g0 = rng.integers(0, n)
            glen = int(rng.integers(60, 480)) * US
            mask &= ~((ts >= ts[g0]) & (ts < ts[g0] + glen))
        ts = ts[mask]
        n = len(ts)
        if n == 0:
            continue
        # volatility with seasonality and a slow-moving regime
        frac = (ts - s_start) / (s_end - s_start)
        vol = per_sec_vol * np.sqrt(np.maximum(np.diff(ts, prepend=ts[0] - US) / US, 1e-3))
        vol *= 0.7 + 0.8 * _seasonality(spec.kind, frac)
        regime += rng.normal(0, 0.15) * spec.daily_vol
        regime *= 0.7
        drift = regime / max(n, 1)
        incr = rng.normal(drift, 1.0, n) * vol
        # jumps
        jumps = rng.random(n) < (spec.jump_per_day * session_len / 86_400) / n
        incr[jumps] += rng.normal(0, spec.jump_sigma, jumps.sum()) * np.sign(rng.normal(size=jumps.sum()))
        log_mid = log_price + np.cumsum(incr)
        log_price = log_mid[-1]
        mid = np.exp(log_mid)
        # spread proportional to vol, bid-ask bounce on trade prices
        spread = np.maximum(spec.tick_size, mid * (0.00005 + 2.0 * vol))
        bid = mid - spread / 2
        ask = mid + spread / 2
        side = rng.random(n) < 0.5 + 0.3 * np.tanh(incr / (vol + 1e-12))  # buys follow up-moves
        price = np.where(side, ask, bid)
        price = np.round(price / spec.tick_size) * spec.tick_size
        bid = np.round(bid / spec.tick_size) * spec.tick_size
        ask = np.round(ask / spec.tick_size) * spec.tick_size
        # sizes: lognormal with clustering after jumps
        size = np.exp(rng.normal(math.log(spec.base_size), 0.9, n))
        size[jumps] *= 8
        size = np.round(size / (spec.base_size / 100)) * (spec.base_size / 100)
        size = np.maximum(size, spec.base_size / 100)
        ts_parts.append(ts)
        px_parts.append(price)
        sz_parts.append(size)
        bid_parts.append(bid)
        ask_parts.append(ask)
        side_parts.append(side)
        total += n
        if max_ticks is not None and total >= max_ticks:
            break
    ts = np.concatenate(ts_parts)
    order = np.argsort(ts, kind="stable")
    ts = ts[order]
    assert np.all(np.diff(ts) > 0), "timestamps must be strictly increasing"
    out = {
        "timestamp": ts,
        "price": np.concatenate(px_parts)[order],
        "size": np.concatenate(sz_parts)[order],
        "bid": np.concatenate(bid_parts)[order],
        "ask": np.concatenate(ask_parts)[order],
        "side": np.concatenate(side_parts)[order],
    }
    if max_ticks is not None:
        out = {k: v[:max_ticks] for k, v in out.items()}
    return out


if __name__ == "__main__":
    import sys
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    for t in TICKERS:
        d = generate(t.name, days)
        px = d["price"]
        print(f"{t.name:8s} ticks={len(px):>9,d} first={px[0]:.4f} last={px[-1]:.4f} "
              f"min={px.min():.4f} max={px.max():.4f} span_days={(d['timestamp'][-1]-d['timestamp'][0])/DAY_US:.1f}")
