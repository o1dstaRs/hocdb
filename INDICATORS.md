# HOCDB Indicators & Quantitative Analytics

HOCDB computes technical indicators, OHLCV bars and risk/performance analytics
directly inside the storage engine, in Zig, with SIMD kernels. One call reads
the needed columns once and returns everything requested, so a trading agent
can gather a complete market picture in a single round trip.

All bindings expose the same four entry points:

| Entry point | What it returns | Typical use |
| :--- | :--- | :--- |
| `indicators(specs, range \| tail, options)` | Aligned time series for any set of indicators (one pass over the data) | Charts, backtests, feature vectors |
| `pairIndicators(other, specs, range \| tail, options)` | The same over two databases aligned on time (ratio, spread z-score, relative strength, correlation, beta) | Pairs, beta to a benchmark, cross-asset context |
| `snapshot(options)` / `snapshotMulti(buckets)` | ~100 named values for the latest bar, for one or several bar sizes from one read | LLM / agent prompt in one shot |
| `summary(range, field, periods_per_year)` | 29 scalar risk & performance statistics (Sharpe, Sortino, max drawdown, VaR/CVaR, Hurst, half-life, ...) | Risk checks, regime detection |
| `ohlcv(range, bucket, price, volume, side)` | OHLCV bars aggregated from raw records (tick → bar), with buy volume when a side field exists | Resampling, candlestick data |
| `health(range, price, volume, gap, outlier)` | Gap, staleness, outlier and volume sanity statistics | Feed monitoring before trusting a signal |
| `evaluate(decisions, horizon, cost_bps)` | Hit rate, PnL, Sharpe, drawdown, profit factor of a list of directional decisions | Closing the loop on the agent's own calls |

Any of them can run on raw records (`bucket = 0`) or on bars aggregated on the
fly from ticks (`bucket = N` timestamp units).

## Quick examples

Bun / TypeScript:

```ts
const db = new HOCDB("BTCUSD", "./data", schema);

// Everything an agent needs, one call, ~0.5 ms:
const snap = db.snapshot({ periodsPerYear: 365 * 24 * 12 }); // 5-minute bars
console.log(snap.rsi_14, snap.macd_hist, snap.bb_percent_b, snap.supertrend_dir);

// Explicit batch on 1-minute ticks aggregated into 5-minute bars:
const res = db.indicatorsTail(200, [
  { kind: "ema", period: 21 },
  { kind: "macd" },
  { kind: "bbands", period: 20, param: 2 },
  { kind: "atr", period: 14 },
  { kind: "sma", period: 10, field: "volume", label: "vol_sma" },
], { bucket: 300 });
res.columns["ema_21"]; res.columns["macd_hist"]; res.columns["bbands_20_upper"];
```

Python:

```python
snap = db.snapshot(periods_per_year=252)
res = db.indicators([{"kind": "rsi", "period": 14}, {"kind": "supertrend"}], tail=500)
risk = db.summary(start_ts, end_ts, "close", periods_per_year=252)
bars = db.ohlcv(start_ts, end_ts, bucket=60_000, price="price", volume="qty")
```

C:

```c
HOCDBIndicatorColumns cols = {1, 2, 3, 4, 5};             /* open, high, low, close, volume field indices */
HOCDBIndicatorSpec specs[] = {{HOCDB_IND_RSI, 14, 0, 0, 0, 0, 0, -1, -1},
                              {HOCDB_IND_MACD, 0, 0, 0, 0, 0, 0, -1, -1}};
HOCDBIndicatorResult r;
hocdb_indicators_tail(db, 500, &cols, specs, 2, HOCDB_LOOKBACK_AUTO, 0, &r);
/* r.values is planar: output k = r.values[k * r.n_rows ...] (rsi, macd, signal, hist) */
hocdb_indicators_free(&r);
```

## Concepts

**Columns.** Indicators read the price roles `open`, `high`, `low`, `close`,
`volume`, and the tick roles `bid`, `ask`, `side` (1 = buy) from schema fields
you name (bindings auto-detect fields literally named like that; `price` is
accepted as `close`, `size`/`qty` as `volume`). Only `close` is mandatory;
kinds that need H/L/C or volume report a missing-column error otherwise. A
spec may override its primary input with any field (`field`), e.g. an SMA of
volume or a z-score of open interest, and two-series kinds (`correl`, `beta`)
take the second series from `field2`.

**Window and tail.** Ask for `[start_ts, end_ts)` or for the last `n` rows.
Rows are records, or bars when a bucket is given.

**Lookback (warm-up).** Indicators need history before the first row you ask
for. With `lookback = auto` (default) HOCDB reads the recommended number of
extra rows *before* the window for every spec (exact lookback for finite
windows, plus a decay tail for exponential and Wilder recurrences so that the
first in-window value equals the full-history value to better than 1e-8) and
returns only the window. With `lookback = 0` you get the raw warm-up NaNs.

**Bucket (tick → bar).** With `bucket > 0` records are aggregated into OHLCV
bars of `bucket` timestamp units (floor-aligned, empty buckets omitted) before
anything is computed. Ticks with a single price field become bars with derived
open/high/low; records that already carry OHLCV are re-sampled (first open,
max high, min low, last close, summed volume). A window `[start_ts, end_ts)`
then selects the *bars whose timestamp lies in the window*, every returned
bar is complete, and a tail of `n` means the last `n` existing bars, so
session gaps (nights, weekends, outages) never shrink the result; warm-up is
counted in existing bars as well and trimmed to exactly the requested
lookback, so a tail and the equivalent range give bit-identical results. `ohlcv()` by contrast aggregates exactly the
records in its window, so its first and last bar may be partial when the
window is not bucket-aligned.

**Window-anchored kinds.** OBV, the A/D line, cumulative VWAP (`period` 0)
and drawdown accumulate from the first row *of the window* (or of the tail),
never from warm-up rows read for other indicators, so a day's VWAP is the
VWAP of that day whatever else is in the batch. Ichimoku's chikou span is the
close shifted back by the displacement, so it is NaN for the last
`displacement` rows of any window.

**NaN.** A value that is not yet defined (warm-up) is NaN. Degenerate windows
(zero range, zero variance) follow TA-Lib: 0 for RSI/Stochastic/Williams/CCI,
NaN for ratios whose denominator is 0.

**Output naming (bindings).** `label` if given, else `kind` + `_period` when a
period is given (`sma_20`, `rsi_14`, `macd`). Multi-output kinds append the
output name: `macd_signal`, `bbands_20_percent_b`, `adx_14_plus_di`.

## Indicator catalogue

Parameters: `p` = period, `p2..p4` = further periods, `a`/`b` = `param`/`param2`.
Zero means default. "Needs" lists the columns beyond the primary series.
Warm-up is the exact number of rows before the first defined value.

### Moving averages

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `sma` | 1 | p=20 | | value | p-1 | prefix-sum, SIMD |
| `ema` | 2 | p=20 | | value | p-1 | α=2/(p+1), seeded with SMA of first p (TA-Lib) |
| `wma` | 3 | p=20 | | value | p-1 | linear weights |
| `dema` | 4 | p=20 | | value | 2(p-1) | 2·EMA − EMA(EMA) |
| `tema` | 5 | p=20 | | value | 3(p-1) | 3E1 − 3E2 + E3 |
| `trima` | 6 | p=20 | | value | p-1 | triangular (SMA of SMA, TA-Lib split) |
| `kama` | 7 | p=10, p2=fast 2, p3=slow 30 | | value | p | Kaufman adaptive (TA-Lib) |
| `hma` | 8 | p=20 | | value | p-1+⌊√p⌋ | Hull |
| `zlema` | 9 | p=20 | | value | p-1 | zero-lag EMA |
| `vwma` | 10 | p=20 | volume | value | p-1 | Σpv/Σv |
| `rma` | 11 | p=14 | | value | p-1 | Wilder smoothing α=1/p |

### Momentum

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `rsi` | 20 | p=14 | | value | p | Wilder, TA-Lib seeding |
| `macd` | 21 | p=12, p2=26, p3=9 | | macd, signal, hist | 25 / 33 | TA-Lib EMA alignment |
| `ppo` | 22 | p=12, p2=26, p3=9 | | ppo, signal, hist | 25 / 33 | percent price oscillator (EMA) |
| `stoch` | 23 | p=14, p2=K smooth 3, p3=D 3 | H L C | k, d | 15 / 17 | slow stochastic |
| `stoch_rsi` | 24 | p=RSI 14, p2=stoch 14, p3=K 3, p4=D 3 | | k, d | ~31 | TradingView-style StochRSI |
| `cci` | 25 | p=20 | H L C | value | p-1 | 0.015 · mean abs deviation |
| `willr` | 26 | p=14 | H L C | value | p-1 | Williams %R |
| `mom` | 27 | p=10 | | value | p | x − x[−p] |
| `roc` | 28 | p=10 | | value | p | (x/x[−p] − 1)·100 |
| `cmo` | 29 | p=14 | | value | p | Chande (plain sums, TradingView) |
| `trix` | 30 | p=15 | | value | 3(p-1)+1 | 1-bar ROC of triple EMA ×100 |
| `ultosc` | 31 | p=7, p2=14, p3=28 | H L C | value | max p | Ultimate oscillator |
| `ao` | 32 | p=5, p2=34 | H L | value | p2-1 | Awesome oscillator |
| `tsi` | 33 | p=25, p2=13, p3=signal 13 | | tsi, signal | ~38 | True strength index |
| `bop` | 34 | | O H L C | value | 0 | Balance of power |
| `dpo` | 35 | p=20 | | value | p-1 | Detrended price oscillator |

### Trend

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `adx` | 40 | p=14 | H L C | adx, plus_di, minus_di | 2p-1 / p | Wilder DMI, TA-Lib seeding |
| `aroon` | 41 | p=25 | H L | up, down, osc | p | window of p+1 bars |
| `psar` | 42 | a=0.02, b=max 0.2 | H L | sar, dir (±1) | 1 | Parabolic SAR (TA-Lib) |
| `supertrend` | 43 | p=10, a=mult 3 | H L C | line, dir (±1) | p | ATR bands with carry-forward |
| `vortex` | 44 | p=14 | H L C | plus, minus | p | VI+ / VI− |
| `ichimoku` | 45 | p=9, p2=26, p3=52, p4=disp 26 | H L C | tenkan, kijun, senkou_a, senkou_b, chikou | p-1 … | senkou spans shifted forward by p4 (cloud at bar i), chikou shifted back |
| `linreg` | 46 | p=20 | | value, slope, intercept, r2 | p-1 | least squares over the window, x = 0..p-1 |

### Volatility

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `atr` | 60 | p=14 | H L C | value | p | Wilder ATR (TR from bar 1) |
| `natr` | 61 | p=14 | H L C | value | p | 100·ATR/close |
| `true_range` | 62 | | H L C | value | 1 | |
| `bbands` | 63 | p=20, a=k 2.0 | | upper, middle, lower, percent_b, bandwidth | p-1 | population σ |
| `keltner` | 64 | p=EMA 20, p2=ATR 10, a=mult 2 | H L C | upper, middle, lower | max(p, p2)-ish | |
| `donchian` | 65 | p=20 | H L | upper, middle, lower | p-1 | |
| `stddev` | 66 | p=20 | | value | p-1 | population (ddof 0) |
| `variance` | 67 | p=20 | | value | p-1 | population |
| `hist_vol` | 68 | p=20, a=periods/year (0 = none) | | value | p | sample σ of log returns · √a |

### Volume

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `obv` | 80 | | volume | value | 0 | cumulative from window start |
| `vwap` | 81 | p=0 (cumulative) or rolling p | volume (+H L C → typical price) | value | 0 / p-1 | |
| `mfi` | 82 | p=14 | H L C V | value | p | money flow index |
| `cmf` | 83 | p=20 | H L C V | value | p-1 | Chaikin money flow |
| `ad` | 84 | | H L C V | value | 0 | accumulation/distribution line |
| `adosc` | 85 | p=3, p2=10 | H L C V | value | p2-1 | Chaikin oscillator (TA-Lib seeding) |
| `efi` | 86 | p=13 | volume | value | p | Elder force index (EMA) |

### Statistics & risk

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `returns` | 100 | p=1 | | value | p | simple return over p |
| `log_returns` | 101 | p=1 | | value | p | ln(x/x[−p]) |
| `zscore` | 102 | p=20 | | value | p-1 | (x − SMA)/σ (population) |
| `percent_rank` | 103 | p=20 | | value | p | % of previous p values ≤ current |
| `rolling_min` | 104 | p=20 | | value | p-1 | van Herk–Gil–Werman, O(n) |
| `rolling_max` | 105 | p=20 | | value | p-1 | |
| `drawdown` | 106 | | | value | 0 | x/runningMax − 1 from window start |
| `sharpe` | 107 | p=20, a=periods/year | | value | p | mean(r)/σ(r, ddof 1)·√a, rf = 0 |
| `sortino` | 108 | p=20, a=periods/year | | value | p | downside deviation √mean(min(r,0)²) |
| `correl` | 109 | p=20, field2 | second series | value | p-1 | Pearson |
| `beta` | 110 | p=20, field2 = benchmark | second series | value | p | cov(r_a, r_b)/var(r_b) on 1-period returns |
| `skew` | 111 | p=20 | | value | p-1 | population g1 |
| `kurtosis` | 112 | p=20 | | value | p-1 | excess, population |

### Price transforms

| Kind | Id | Needs | Outputs |
| :-- | :-: | :-- | :-- |
| `typical_price` | 120 | H L C | (H+L+C)/3 |
| `median_price` | 121 | H L | (H+L)/2 |
| `heikin_ashi` | 122 | O H L C | open, high, low, close |

### Microstructure (tick data with `bid`, `ask`, `side` roles)

| Kind | Id | Params (defaults) | Needs | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- | :-- |
| `spread` | 130 | | bid, ask | abs, bps | 0 | quoted spread, absolute and in basis points of the mid |
| `order_flow` | 131 | p=20 | volume + side (ticks) or buy volume (bars) | net, imbalance | p-1 | signed volume sum and (buy−sell)/(buy+sell) ∈ [−1, 1] |
| `tick_pressure` | 132 | p=50 | | value | p | rolling mean of the tick sign (zero-ticks carry the previous sign) |
| `trade_intensity` | 133 | p=100, a=timestamp units per second (1e6) | time, volume | trades_per_sec, volume_per_sec | p | over the last p rows |
| `amihud` | 134 | p=20 | volume | value | p | mean of \|return\| / (price·volume): illiquidity |
| `realized_vol` | 135 | p=20, a=periods/year | | value | p | RMS of 1-row log returns · √a |

With `bucket > 0` and a `side` role, every bar carries its buy volume, so
`order_flow` works on bars as well.

### Pairs and passthrough (second series from `field2` or from another database)

| Kind | Id | Params (defaults) | Outputs | Warm-up | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- |
| `series` | 140 | | value | 0 | the primary input itself (handy to return close alongside indicators) |
| `series2` | 141 | | value | 0 | the aligned second series |
| `ratio` | 142 | | value | 0 | a / b |
| `ratio_zscore` | 143 | p=20 | value | p-1 | z-score of a / b (pairs-trading spread) |
| `rel_strength` | 144 | p=10 | value | p | ROC(a, p) − ROC(b, p) in percent points |

`correl` and `beta` work with a second database too. Alignment: with a bucket
both databases are resampled and inner-joined on bar timestamps (only bars
both traded); on ticks the second series is as-of joined onto the first
database's rows (latest row at or before each timestamp).

### Labels (look-ahead by design, never live features)

| Kind | Id | Params (defaults) | Outputs | Notes |
| :-- | :-: | :-- | :-- | :-- |
| `forward_return` | 150 | p=horizon 1 | ret, max, min | return after p rows plus the maximum favourable / adverse excursion inside the horizon; NaN for the last p rows |
| `triple_barrier` | 151 | p=horizon 20, a=up 0.02, b=down (= up) | label, ret, bars | +1 / −1 when the up / down barrier is hit first within the horizon, 0 at the horizon; NaN when unknown yet |

`isLookahead` / `hocdb_indicator_is_lookahead` flags these kinds so a pipeline
can refuse them as live features.

### Session-anchored (param = session length, param2 = session offset, in timestamp units)

| Kind | Id | Params (defaults) | Needs | Outputs | Notes |
| :-- | :-: | :-- | :-- | :-- | :-- |
| `session_vwap` | 160 | a, b | time, volume (+H/L/C → typical price) | value | VWAP restarted at every session start |
| `session_range` | 161 | a, b | time (+O/H/L) | open, high, low, ret | running session open / high / low and return since the open |
| `opening_range` | 162 | p=5 rows, a, b | time, H L C | high, low, breakout | range of the first p rows of the session, then +1 / −1 / 0 breakout |
| `pivots` | 163 | a, b | time, H L C | pp, r1, s1, r2, s2 | classic floor pivots from the previous session |

Sessions are `[offset + k·length, offset + (k+1)·length)`. For US equities in
microseconds with sessions cut at midnight UTC: length 86 400 000 000, offset
0; to cut at 13:30 UTC: offset 48 600 000 000. The storage layer always reads
back to the session start (and to the previous session for pivots), whatever
window or tail you ask for, so these values are correct from the first row.

### Session-anchored kinds with a trading calendar (param = 0)

With a trading calendar on the database (`calendar` in the config, `set_calendar`, see
[Trading calendars](#trading-calendars)) and a known timestamp unit, `param = 0` makes the four
session kinds use the exchange sessions instead of fixed-length cycles: `session_vwap`, `session_range`
and `opening_range` reset at every session open (09:30 New York for NYSE, 17:00 the evening before for
FX and CME, midnight UTC for crypto), and `pivots` use the previous *trading day* (weekends and
holidays skipped). Rows outside trading hours belong to the session of their local trade date
(pre-market to the coming session, after-hours to the one that just closed). Without a calendar the
call fails with `CalendarRequired` (C: -30).

## Trading calendars

`src/calendar.zig` models exchange sessions, holidays, early closes and daylight-saving rules; all
public functions work in UTC seconds and the storage layer converts database timestamps with the
handle's `timestamp_unit_ns`. Built-in calendars (ids are stable):

| id | name | sessions | notes |
| :-- | :-- | :-- | :-- |
| 1 | `crypto` | 24/7, UTC days | 365 sessions / year |
| 2 | `fx` | Sun 17:00 -> Fri 17:00 New York | one session per trade date from 17:00 the evening before; closed Dec 25 / Jan 1; 260 / year |
| 3 | `nyse` | 09:30-16:00 America/New_York | NYSE holiday rules incl. observed weekend holidays, Good Friday, Juneteenth (2022+), 13:00 early closes, special closures (9/11, Sandy, presidential funerals); 252 / year |
| 4 | `nasdaq` | alias of `nyse` | |
| 5 | `lse` | 08:00-16:30 Europe/London | UK bank holidays incl. substitute days and the 2002-2023 one-off closures, 12:30 early closes; 253 / year |
| 6 | `cme` | 17:00 (previous evening) -> 16:00 America/Chicago | Globex equity-index approximation: closed New Year's Day, Good Friday, Christmas and presidential funerals, 12:00 early closes on other US holidays; define a custom calendar for exact product rules |

NYSE and LSE sessions 2000-2030 were checked against the `exchange_calendars` library: they agree on
every session open and close (the library predates the 2025-01-09 closure).

Custom calendars (`calendar.define`, C `hocdb_calendar_define`) take a weekly template (Monday first,
local seconds relative to the trade date's midnight; the open may be negative for evening opens), a
standard UTC offset, a DST rule (`none`, `us`, `eu`), a holiday list (day numbers) and early closes. They
are process-local; built-in ids and the timestamp unit are persisted in the file header, so readers see
the writer's calendar.

What a calendar changes on a database:

* session kinds with `param = 0` (above);
* `health()` measures gaps in trading time: closed periods are subtracted before the threshold, mean,
  median and maximum, and three fields are added: `closed_span` (closed time inside the range),
  `n_session_breaks` (gaps spanning a session boundary), `n_missing_sessions` (sessions with no rows).
  A whole missing session still counts as a gap of one session length;
* `summary`, `snapshot` and `snapshot_multi` with `periods_per_year = 0` derive it from the calendar:
  intraday bars scale by the session length (252 x 390 one-minute bars for NYSE, 365 x 1440 for
  crypto), daily bars give the sessions per year, longer buckets the calendar buckets per year;
  `periods_per_year(bucket)` exposes the same number for indicator specs and the backtester.

Calendar queries: `session_at / prev / next`, `session_for_day`, `is_open`, `open_seconds_between`,
`sessions_between`, `periods_per_year`, `to_local`, civil-date helpers.

## Signal backtester

`src/backtest.zig` turns a target-position series over bars into an equity curve with costs and
slippage, stop-loss / take-profit / trailing exits, position limits, a trade list and performance
statistics (`backtest`, `backtest_tail`, `backtest_arrays`, `walk_forward_splits`, `backtest_splits`).

* `target[i]` is the desired position at the **end** of row `i` of `indicators(start, end, bucket)` over
  the same window (bucket > 0: bars whose start lies in `[start, end)`; equal to `ohlcv` for
  bucket-aligned bounds), in units, fraction of equity or notional (`position_mode`). NaN holds the
  previous signal; `allow_short = 0` clamps negative targets.
* Fills happen at the next bar's open (`fill_mode` 0, no look-ahead) or the same close (1), slipped
  adversely by `slippage_bps`, paying `cost_bps` per side on the traded notional. A position is only
  re-sized when the signal value changes (no drift rebalancing).
* Stops are checked on every bar against high / low in the order stop-loss, trailing, take-profit;
  they fill at the level or at the open on a gap through it; a stopped position is not re-entered on
  the same signal.
* `equity = cash + position * close`; statistics use population std of bar returns; annualised with
  `periods_per_year` (a database handle fills it from its calendar); `max_drawdown_bars` is the
  longest run below a prior peak; per-trade statistics cover closed trades, an open position is
  reported with `exit_reason` 4.
* Walk-forward helpers split `n` bars into an initial train window and `n_splits` contiguous test
  windows (anchored or rolling); `backtest_splits` evaluates each test window with fresh equity.

The Zig kernel and an independent Python implementation (`scripts/stress/references_backtest.py`)
assert the same hand-checked example, and the stress harness compares them on 30 days of bars.

## Universe (cross-sectional) features

`src/universe.zig` computes, over a watch-list of databases joined on timestamps (`universe`,
`universe_arrays`), per-ticker momentum (`mom_short/mid/long`), volatility, SMA distance, beta and
correlation to an equal- or volume-weighted market factor, relative strength, cross-sectional
percentile ranks and z-scores, average / maximum pair correlation and idiosyncratic volatility, plus
a summary with the market returns, dispersion, breadth and the pair-correlation statistics, and the
full correlation matrix. Rolling statistics use exactly the last `period` observations (NaN when
fewer or non-finite); correlations are pairwise-complete over the last `corr_period` returns; a ticker
with too few bars gets NaN features and is excluded from ranks and the market. With `n_bars = 0` the
storage layer reads enough bars for the longest period even when the join drops rows. Validated
against a pandas reference (`scripts/stress/references_universe.py`).

## Snapshot fields

`snapshot()` computes, from the last 2500 rows by default (enough for every
field to converge; pass `bars` to change), the fields below. Missing columns
leave the dependent fields NaN (e.g. no volume → `obv`, `mfi_14`, `vwap` NaN).

`timestamp`, `bars`, `open`, `high`, `low`, `close`, `volume`,
`sma_5/10/20/50/100/200`, `ema_9/12/21/26/50/200`, `wma_20`, `hma_20`, `vwma_20`, `kama_10`, `tema_20`,
`rsi_14`, `stoch_k`, `stoch_d`, `stochrsi_k`, `stochrsi_d`, `macd`, `macd_signal`, `macd_hist`, `ppo`, `cci_20`, `williams_r_14`, `roc_10`, `mom_10`, `cmo_14`, `trix_15`, `ultosc`, `ao`, `tsi`, `tsi_signal`,
`adx_14`, `plus_di_14`, `minus_di_14`, `aroon_up_25`, `aroon_down_25`, `aroon_osc_25`, `psar`, `psar_dir`, `supertrend`, `supertrend_dir`, `vortex_plus_14`, `vortex_minus_14`, `ichimoku_tenkan`, `ichimoku_kijun`, `ichimoku_senkou_a`, `ichimoku_senkou_b`, `linreg_value_20`, `linreg_slope_20`, `linreg_r2_20`,
`atr_14`, `natr_14`, `true_range`, `bb_upper`, `bb_middle`, `bb_lower`, `bb_percent_b`, `bb_bandwidth`, `keltner_upper/middle/lower`, `donchian_upper_20/middle_20/lower_20`, `stddev_20`, `hist_vol_20`,
`obv`, `vwap`, `mfi_14`, `cmf_20`, `ad`, `adosc`, `efi_13`,
`return_1/5/10/20`, `log_return_1`, `zscore_20`, `percent_rank_20`, `high_20`, `low_20`, `high_250`, `low_250`, `drawdown`, `sharpe_20`, `sortino_20`, `skew_20`, `kurtosis_20`.

The EMA ladder is computed with one vectorised recurrence across periods, the
SMA ladder from a single prefix sum, and every field shares one column read.

## Multi-timeframe snapshots

`snapshotMulti(buckets, periods_per_year)` (C: `hocdb_snapshot_multi`) reads
the tail once and returns one snapshot per bar size, e.g. 1m / 5m / 1h / 1d,
each over the last `n_bars` existing bars. It is the recommended way to give an
agent the same picture at several horizons.

## Health

`health(range, price, volume, gap_threshold, outlier_threshold)` returns
`count`, `first_ts`, `last_ts`, `span`, `mean_gap`, `median_gap`, `max_gap`,
`max_gap_at`, `n_gaps` (gaps above the threshold: session breaks and outages),
`n_nonpositive_price`, `n_nan_price`, `n_outlier_returns` (|log return| above
the threshold), `first_outlier_at`, `max_abs_return`, `n_zero_volume`,
`n_negative_volume`. Compare `last_ts` with the current time to detect a stale
feed.

## Decision evaluation

`evaluate(decisions, default_horizon, cost_bps)` takes a list of
`(timestamp, direction ±1, size, horizon)` decisions, enters at the first
price at or after the decision time, exits at the first price at or after
time + horizon, charges `cost_bps` per side, and returns `n_decisions`,
`n_evaluated`, `n_long`, `n_short`, `hit_rate`, `avg_return`,
`avg_net_return`, `total_pnl`, `total_cost`, `sharpe` (per decision),
`profit_factor`, `max_drawdown` (of cumulative PnL), `avg_win`, `avg_loss`,
`best`, `worst`, and long / short hit rates and average returns, plus optional
per-decision entry price, exit price and net return. Log the agent's decisions
into a HOCDB database (timestamp, direction, size, horizon) and evaluate them
against the price database to measure the agent itself.

## Summary fields

`count`, `first`, `last`, `min`, `max`, `mean`, `std` (sample), `total_return`,
`log_return`, `ann_return` (geometric, needs periods/year), `ann_vol` (sample σ of
log returns · √ppy), `sharpe`, `sortino`, `max_drawdown` (≤ 0),
`max_drawdown_bars` (longest stretch under water), `calmar`, `skew`, `kurtosis`
(excess; both of simple returns), `var_95`, `cvar_95` (historical, 5th
percentile of returns — a negative number), `win_rate`, `avg_gain`, `avg_loss`,
`profit_factor`, `best`, `worst`, `autocorr_1` (lag-1 autocorrelation of
returns), `hurst` (variance-of-lags estimator on log prices, lags 2..20),
`half_life` (Ornstein-Uhlenbeck mean-reversion half-life in bars, NaN if the
series does not mean-revert).

## Precomputed vs on demand

Indicators are computed on demand from the stored columns rather than at
insert time. A single column read plus the O(n) kernels costs microseconds for
typical windows (see below), every parameter stays free (any period, any
field, any bar size) and nothing has to be recomputed or migrated when a
strategy changes. If a hot path needs sub-microsecond latest values, keep the
`snapshot()` result cached per bar and refresh it once per bar close.

## Performance (Apple M-series, ReleaseFast, 1 000 000 bars)

| Kernel | Throughput |
| :-- | --: |
| SMA(20) | ~610 M records/s |
| EMA(20) | ~610 M records/s |
| RSI(14) | ~270 M records/s |
| MACD(12,26,9) | ~240 M records/s |
| Bollinger(20) (exact two-pass variance) | ~210 M records/s |
| ATR(14) | ~530 M records/s |
| ADX(14) | ~165 M records/s |
| Stochastic(14,3,3) | ~165 M records/s |
| Rolling max(50) | ~830 M records/s |
| OBV | ~730 M records/s |
| MFI(14) | ~210 M records/s |
| Linear regression(20) | ~190 M records/s |
| `snapshot()` (2500 bars, ~100 fields) | ~0.5 ms |
| `summary()` (1 000 000 bars, 29 statistics) | ~60 ms |

On real tick databases (30 days, 1.4 M ticks per crypto ticker, see
`scripts/stress`): 1-minute bars from 1.4 M ticks in ~55 ms, a 64-indicator
batch over 43 000 1-minute bars in ~110 ms including the tick read and
resampling, snapshots from ticks in 8–30 ms (equities span session gaps and
read more), ingestion from Python at ~4 M records/s.

Run `zig build bench -Doptimize=ReleaseFast` for the `[INDICATOR BENCHMARK]`
section on your machine.

Elementwise transforms, prefix-sum windows, rolling extremes and window scans
use `@Vector` SIMD with the lane width of the target CPU; sequential
recurrences (EMA, Wilder, KAMA, SAR, Supertrend) are scalar O(1)/row and are
vectorised *across* instances where several are requested (the snapshot's EMA
ladder). Rolling sums use prefix sums restarted every 1024 rows (error bounded by
the block, not the series length); rolling variance is an exact two-pass
window for periods up to 64 and a periodically re-seeded Welford recurrence
beyond; skew/kurtosis use an exact per-window mean; regression sums are
periodically re-seeded. Accuracy is ~1e-12 relative on million-row series
at any price level.

## Correctness

* `src/test_indicators_golden.zig` checks every kernel against TA-Lib 0.7
  (where TA-Lib defines the indicator) and against explicit numpy references
  of the documented conventions otherwise, plus the summary and the
  resampler. Regenerate with `python3 scripts/gen_indicator_golden.py >
  src/test_indicators_golden.zig` (needs numpy and the `TA-Lib` wheel).
* `src/test_indicators.zig` checks SIMD kernels against naive scalar code on
  long series, warm-up lengths, in-place aliasing, empty/short inputs, and
  snapshot vs individual kernels.
* `src/test_indicators_db.zig` covers the storage integration (window/tail
  trimming, field overrides, bucket resampling, ring-buffer wrap).
* `bindings/c/test/test_indicators.c` and each binding's `test_indicators.*`
  cover the ABI and the wrappers.
* `scripts/stress/run_overnight.sh` runs the end-to-end validation on 30 days
  of synthetic ticks for 8 tickers: resampling vs pandas, every indicator vs
  TA-Lib on the resulting bars, window/tail/lookback semantics, snapshot and
  summary, tick-mode kinds, edge cases, ring buffer, fuzzing, performance,
  memory growth and bit-for-bit consistency across all bindings. It writes
  `stress_report.md`.

## C ABI reference

See `bindings/c/hocdb.h`, section "Technical indicators and quantitative
analytics": `hocdb_indicators`, `hocdb_indicators_tail`,
`hocdb_indicators_free`, `hocdb_indicator_*` registry helpers, `hocdb_ohlcv`,
`hocdb_summary`, `hocdb_snapshot`, and the `hocdb_summary_field_*` /
`hocdb_snapshot_field_*` introspection functions that let bindings decode the
result structs by name without hard-coding field lists.
