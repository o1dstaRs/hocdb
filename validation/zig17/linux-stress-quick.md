# HOCDB overnight validation report

- Date: 2026-10-23 16:07:43
- Host: Linux-6.12.54-linuxkit-aarch64-with-glibc2.39 (aarch64), Python 3.12.3, numpy 1.26.4, pandas 2.2.3, TA-Lib 0.7.1
- Days: 2, tickers: BTCUSD, AAPL, fuzz cases: 40, seed: 20250906
- Total checks: 544, failed: 0, wall time: 12s

## Result: PASS

## Data

| Ticker | Ticks | 1-minute bars | First price | Last price | Min | Max |
| :-- | --: | --: | --: | --: | --: | --: |
| BTCUSD | 93,725 | 2,870 | 62016 | 55847 | 55751 | 63798 |
| AAPL | 57,991 | 757 | 228.05 | 225.29 | 222.93 | 228.62 |

## Checks by phase

| Phase | Passed | Failed |
| :-- | --: | --: |
| ingest | 6 | 0 |
| resample | 64 | 0 |
| indicators | 198 | 0 |
| windows | 12 | 0 |
| tails | 10 | 0 |
| snapshot | 6 | 0 |
| summary | 8 | 0 |
| tick-mode | 42 | 0 |
| edge | 30 | 0 |
| ring | 6 | 0 |
| fuzz | 2 | 0 |
| memory | 1 | 0 |
| micro | 12 | 0 |
| labels | 8 | 0 |
| sessions | 10 | 0 |
| pairs | 9 | 0 |
| health | 1 | 0 |
| evaluate | 1 | 0 |
| multi | 1 | 0 |
| ops | 66 | 0 |
| calendar | 21 | 0 |
| backtest | 7 | 0 |
| universe | 15 | 0 |
| bindings | 8 | 0 |

## Timings

| Measurement | Time | Notes |
| :-- | --: | :-- |
| ingest BTCUSD | 42.46 ms | 93,725 records, 2,207,411 rec/s |
| ingest AAPL | 26.87 ms | 57,991 records, 2,157,975 rec/s |
| ohlcv 1m BTCUSD | 0.49 ms | 93,725 ticks -> 2,870 bars |
| ohlcv 1m AAPL | 0.29 ms | 57,991 ticks -> 757 bars |
| batch 74 specs on 2,870 1m bars (BTCUSD) | 3.03 ms |  |
| reference computation (BTCUSD) | 458.16 ms |  |
| batch 74 specs on 757 1m bars (AAPL) | 1.18 ms |  |
| reference computation (AAPL) | 118.16 ms |  |
| snapshot BTCUSD | 2.35 ms | 101 fields |
| snapshot AAPL | 0.65 ms | 101 fields |
| summary BTCUSD (93,725 ticks) | 7.91 ms |  |
| summary AAPL (57,991 ticks) | 3.61 ms |  |
| tick-mode batch BTCUSD (20,000 ticks, 17 specs) | 2.29 ms |  |
| tick-mode 4 specs over all 93,725 ticks (BTCUSD) | 2.34 ms | 40,088,838 ticks/s |
| tick-mode batch AAPL (20,000 ticks, 17 specs) | 2.78 ms |  |
| tick-mode 4 specs over all 57,991 ticks (AAPL) | 1.21 ms | 48,022,089 ticks/s |
| reopen (index rebuild) | 0.98 ms | 93,725 records |
| snapshot 2500 x 1m bars from ticks (median) | 2.28 ms | max 2.55 ms |
| snapshot 2500 ticks (median) | 0.72 ms | max 0.85 ms |
| tail(500) x 74 specs on 1m bars (median) | 4.68 ms | max 5.60 ms |
| tail(200) x 4 common specs on 1m bars (median) | 0.21 ms | max 0.34 ms |
| latest EMA(21) on ticks (median) | 0.01 ms | max 0.03 ms |
| ohlcv 1m over all 93,725 ticks (median) | 1.19 ms | 79,039,998 ticks/s |
| summary over all 93,725 ticks (median) | 8.63 ms | 10,860,791 ticks/s |
| get_stats over all 93,725 ticks (median) | 0.32 ms | 297,382,014 ticks/s |
| memory phase (300 iterations of batch+snapshot) | 380.73 ms | max RSS growth 0.0 MB |
| snapshot_multi 3 timeframes x 500 bars | 1.88 ms |  |
| ops writer metrics call | 0.04 ms |  |
| backtest 16 runs incl. reference | 141.01 ms |  |
| backtest 2868 bars incl. bar read (stops, trade list off) | 0.70 ms |  |
| universe 2 tickers x 500 one-minute bars incl. reads | 1.09 ms |  |
| consistency program bun | 58.60 ms |  |
| consistency program node | 83.67 ms |  |
| consistency program c | 46.87 ms |  |
| consistency program cpp | 47.89 ms |  |
| consistency program go | 27.71 ms |  |

## Notes

- Total ticks ingested: 151,716 across 2 tickers, 2 days.
- Fuzz validation errors by message: 'indicators failed: missing column: the indicator needs open/' x9
