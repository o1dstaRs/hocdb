# HOCDB overnight validation report

- Date: 2026-10-23 09:06:11
- Host: macOS-15.7.4-arm64-arm-64bit (arm64), Python 3.12.14, numpy 1.26.4, pandas 2.2.3, TA-Lib 0.7.1
- Days: 30, tickers: BTCUSD, ETHUSD, SOLUSD, AAPL, NVDA, TSLA, SPY, EURUSD, fuzz cases: 2000, seed: 20250906
- Total checks: 1775, failed: 0, wall time: 76s

## Result: PASS

## Data

| Ticker | Ticks | 1-minute bars | First price | Last price | Min | Max |
| :-- | --: | --: | --: | --: | --: | --: |
| BTCUSD | 1,406,945 | 43,016 | 62016 | 56329 | 47652 | 63798 |
| ETHUSD | 1,055,442 | 42,992 | 3100.4 | 1897.8 | 1841.5 | 3295.1 |
| SOLUSD | 703,479 | 43,039 | 144.9 | 165.55 | 108.34 | 178.36 |
| AAPL | 650,053 | 8,466 | 228.05 | 209.34 | 209.33 | 235.56 |
| NVDA | 776,776 | 8,440 | 118.01 | 111.21 | 110.96 | 124.76 |
| TSLA | 725,566 | 8,415 | 244.97 | 280.64 | 239.34 | 302.38 |
| SPY | 520,824 | 8,465 | 559.89 | 545.51 | 536.55 | 565.84 |
| EURUSD | 687,738 | 31,533 | 1.105 | 1.1252 | 1.0814 | 1.1263 |

## Checks by phase

| Phase | Passed | Failed |
| :-- | --: | --: |
| ingest | 24 | 0 |
| resample | 256 | 0 |
| indicators | 792 | 0 |
| windows | 216 | 0 |
| tails | 40 | 0 |
| snapshot | 24 | 0 |
| summary | 56 | 0 |
| tick-mode | 168 | 0 |
| edge | 30 | 0 |
| ring | 7 | 0 |
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
| ingest BTCUSD | 577.06 ms | 1,406,945 records, 2,438,139 rec/s |
| ingest ETHUSD | 405.69 ms | 1,055,442 records, 2,601,565 rec/s |
| ingest SOLUSD | 272.13 ms | 703,479 records, 2,585,102 rec/s |
| ingest AAPL | 257.64 ms | 650,053 records, 2,523,095 rec/s |
| ingest NVDA | 292.89 ms | 776,776 records, 2,652,073 rec/s |
| ingest TSLA | 244.10 ms | 725,566 records, 2,972,473 rec/s |
| ingest SPY | 174.20 ms | 520,824 records, 2,989,730 rec/s |
| ingest EURUSD | 287.37 ms | 687,738 records, 2,393,225 rec/s |
| ohlcv 1m BTCUSD | 9.14 ms | 1,406,945 ticks -> 43,016 bars |
| ohlcv 1m ETHUSD | 6.65 ms | 1,055,442 ticks -> 42,992 bars |
| ohlcv 1m SOLUSD | 5.79 ms | 703,479 ticks -> 43,039 bars |
| ohlcv 1m AAPL | 4.35 ms | 650,053 ticks -> 8,466 bars |
| ohlcv 1m NVDA | 5.28 ms | 776,776 ticks -> 8,440 bars |
| ohlcv 1m TSLA | 4.75 ms | 725,566 ticks -> 8,415 bars |
| ohlcv 1m SPY | 3.57 ms | 520,824 ticks -> 8,465 bars |
| ohlcv 1m EURUSD | 4.98 ms | 687,738 ticks -> 31,533 bars |
| batch 74 specs on 43,016 1m bars (BTCUSD) | 30.13 ms |  |
| reference computation (BTCUSD) | 5113.48 ms |  |
| batch 74 specs on 42,992 1m bars (ETHUSD) | 27.07 ms |  |
| reference computation (ETHUSD) | 5640.53 ms |  |
| batch 74 specs on 43,039 1m bars (SOLUSD) | 23.55 ms |  |
| reference computation (SOLUSD) | 5082.56 ms |  |
| batch 74 specs on 8,466 1m bars (AAPL) | 8.21 ms |  |
| reference computation (AAPL) | 982.71 ms |  |
| batch 74 specs on 8,440 1m bars (NVDA) | 9.31 ms |  |
| reference computation (NVDA) | 1008.14 ms |  |
| batch 74 specs on 8,415 1m bars (TSLA) | 8.64 ms |  |
| reference computation (TSLA) | 996.88 ms |  |
| batch 74 specs on 8,465 1m bars (SPY) | 7.61 ms |  |
| reference computation (SPY) | 995.99 ms |  |
| batch 74 specs on 31,533 1m bars (EURUSD) | 19.10 ms |  |
| reference computation (EURUSD) | 3686.88 ms |  |
| snapshot BTCUSD | 2.20 ms | 101 fields |
| snapshot ETHUSD | 1.84 ms | 101 fields |
| snapshot SOLUSD | 1.40 ms | 101 fields |
| snapshot AAPL | 3.89 ms | 101 fields |
| snapshot NVDA | 4.31 ms | 101 fields |
| snapshot TSLA | 4.36 ms | 101 fields |
| snapshot SPY | 3.56 ms | 101 fields |
| snapshot EURUSD | 1.69 ms | 101 fields |
| summary BTCUSD (1,406,945 ticks) | 244.21 ms |  |
| summary ETHUSD (1,055,442 ticks) | 66.15 ms |  |
| summary SOLUSD (703,479 ticks) | 45.46 ms |  |
| summary AAPL (650,053 ticks) | 30.16 ms |  |
| summary NVDA (776,776 ticks) | 34.01 ms |  |
| summary TSLA (725,566 ticks) | 42.64 ms |  |
| summary SPY (520,824 ticks) | 27.42 ms |  |
| summary EURUSD (687,738 ticks) | 39.01 ms |  |
| tick-mode batch BTCUSD (20,000 ticks, 17 specs) | 1.61 ms |  |
| tick-mode 4 specs over all 1,406,945 ticks (BTCUSD) | 22.71 ms | 61,958,287 ticks/s |
| tick-mode batch ETHUSD (20,000 ticks, 17 specs) | 1.93 ms |  |
| tick-mode 4 specs over all 1,055,442 ticks (ETHUSD) | 19.13 ms | 55,160,423 ticks/s |
| tick-mode batch SOLUSD (20,000 ticks, 17 specs) | 1.93 ms |  |
| tick-mode 4 specs over all 703,479 ticks (SOLUSD) | 12.49 ms | 56,337,205 ticks/s |
| tick-mode batch AAPL (20,000 ticks, 17 specs) | 2.18 ms |  |
| tick-mode 4 specs over all 650,053 ticks (AAPL) | 10.65 ms | 61,015,081 ticks/s |
| tick-mode batch NVDA (20,000 ticks, 17 specs) | 2.29 ms |  |
| tick-mode 4 specs over all 776,776 ticks (NVDA) | 13.36 ms | 58,159,458 ticks/s |
| tick-mode batch TSLA (20,000 ticks, 17 specs) | 1.87 ms |  |
| tick-mode 4 specs over all 725,566 ticks (TSLA) | 12.85 ms | 56,481,893 ticks/s |
| tick-mode batch SPY (20,000 ticks, 17 specs) | 2.21 ms |  |
| tick-mode 4 specs over all 520,824 ticks (SPY) | 9.28 ms | 56,152,332 ticks/s |
| tick-mode batch EURUSD (20,000 ticks, 17 specs) | 2.08 ms |  |
| tick-mode 4 specs over all 687,738 ticks (EURUSD) | 10.63 ms | 64,692,688 ticks/s |
| reopen (index rebuild) | 9.32 ms | 1,406,945 records |
| snapshot 2500 x 1m bars from ticks (median) | 2.14 ms | max 2.78 ms |
| snapshot 2500 ticks (median) | 0.43 ms | max 0.47 ms |
| tail(500) x 74 specs on 1m bars (median) | 3.26 ms | max 3.56 ms |
| tail(200) x 4 common specs on 1m bars (median) | 0.16 ms | max 0.32 ms |
| latest EMA(21) on ticks (median) | 0.01 ms | max 0.04 ms |
| ohlcv 1m over all 1,406,945 ticks (median) | 13.21 ms | 106,487,250 ticks/s |
| summary over all 1,406,945 ticks (median) | 94.02 ms | 14,964,237 ticks/s |
| get_stats over all 1,406,945 ticks (median) | 3.39 ms | 415,564,353 ticks/s |
| memory phase (3000 iterations of batch+snapshot) | 5587.48 ms | max RSS growth 0.0 MB |
| snapshot_multi 3 timeframes x 500 bars | 11.30 ms |  |
| ops writer metrics call | 0.02 ms |  |
| backtest 16 runs incl. reference | 1456.52 ms |  |
| backtest 43014 bars incl. bar read (stops, trade list off) | 10.23 ms |  |
| universe 8 tickers x 500 one-minute bars incl. reads | 4.06 ms |  |
| consistency program bun | 152.66 ms |  |
| consistency program node | 167.03 ms |  |
| consistency program c | 157.36 ms |  |
| consistency program cpp | 160.63 ms |  |
| consistency program go | 135.48 ms |  |

## Notes

- Total ticks ingested: 6,526,823 across 8 tickers, 30 days.
- Fuzz validation errors by message: 'indicators failed: missing column: the indicator needs open/' x555
