# For the AI agents to follow

## Agent Conventions

Only append to this file if needed, do not overwrite existing content.

### Test Data Folders
When creating tests for bindings, always use the following naming convention for the data directory:
`b_<language>_test_data`

Examples:
- `b_python_test_data`
- `b_bun_test_data`
- `b_node_test_data`
- `b_c_test_data`
- `b_cpp_test_data`
- `b_go_test_data`

This ensures consistency and makes it easier to clean up test artifacts.

### Test Binaries
When compiling test binaries (e.g., for C/C++), always output them to the `test_binaries` directory in the project root.
Example: `test_binaries/test_cpp_verify`

### Verification
On every change to the codebase (especially core engine or bindings), you MUST run the verification script to ensure all bindings are working correctly:

```bash
./verify_all.sh
```

### Running Core Tests
To run the Zig core tests (including integrity and unit tests):
```bash
zig build test --summary all
```

### Benchmarking
When modifying the core engine (`src/root.zig`), you MUST run benchmarks to ensure no performance regressions.

Command:
```bash
zig build bench -Doptimize=ReleaseFast
```

**Target Performance (Apple Silicon M-series):**
| Metric | Target |
| :--- | :--- |
| **Write Throughput** | > 8,000,000 ops/sec |
| **Read Throughput** | > 150,000,000 ops/sec |
| **Aggregation Speed** | > 400,000,000 records/sec |

If performance drops significantly below these targets, investigate immediately.

### Zig Documentation Helper
If you get stuck on Zig specifics or need to check the implementation of standard library modules (e.g., `fs`, `mem`, `heap`), use the `documentify.sh` script.

This script extracts the source code of specified Zig standard library modules and packages them into a single XML context file (`zig_context.xml`) that you can read.

**Usage:**
1.  Edit `documentify.sh` to include the modules you need in the `MODULES` array (default: `fs`, `mem`, `heap`).
2.  Run the script:
    ```bash
    ./documentify.sh
    ```
3.  Read the generated `zig_context.xml` file to understand the Zig implementation.

This is extremely useful for avoiding hallucinations about Zig's standard library.

### Indicators / Analytics (src/indicators.zig)
The indicator kernels live in `src/indicators.zig` (pure slice functions, SIMD via `@Vector`),
the storage integration in `DynamicTimeSeriesDB` (`readColumns`, `indicatorsRange`, `indicatorsTail`,
`ohlcv`, `summary`, `snapshot`), and the C ABI in `src/c_bindings.zig` + `bindings/c/hocdb.h`.
Reference documentation: `INDICATORS.md`.

Rules when touching indicators:
- Correctness is checked against TA-Lib / numpy goldens in `src/test_indicators_golden.zig`. That file is
  GENERATED — never edit it by hand. After changing a kernel's convention or adding a kind, extend
  `scripts/gen_indicator_golden.py` and regenerate: `python3 scripts/gen_indicator_golden.py > src/test_indicators_golden.zig`
  (needs a Python with `numpy<2`, `pandas` and the `TA-Lib` wheel, e.g. in a venv).
- Adding a kind: add the enum value + `all_kinds` entry, defaults in `resolve`, `outputCount`/`outputNames`,
  `needs`, `warmup`, the `compute` dispatch, a golden entry, the C header enum, INDICATORS.md, and each
  binding's kind table if it keeps one.
- Warm-up values must be NaN; a value once defined must stay defined (checked by `src/test_indicators.zig`).
- Prefer SIMD (`@Vector`) for elementwise / prefix-sum / window kernels; keep recurrences O(1) per row.
- Run `zig build bench -Doptimize=ReleaseFast` and compare the `[INDICATOR BENCHMARK]` section
  (targets: SMA/EMA > 400M records/s, RSI/MACD/BBANDS > 150M records/s, snapshot(2500 bars) < 2 ms,
  backtest(1M bars with stops) > 200M bars/s, universe(50 tickers x 500 bars) < 300 us).
- Binding tests: `bindings/<lang>/test/test_indicators.*`, data dir `b_<lang>_test_indicators`.
- Column roles are open/high/low/close/volume plus tick-level bid/ask/side; `HOCDBIndicatorColumns` has 8 int64 fields
  (always initialise all of them in C). Kinds 130+ are microstructure, pairs (`series2`/`ratio`/... use `field2` or a
  second database via `pairRange`/`pairTail`), look-ahead labels (`isLookahead`, never live features) and session-anchored
  kinds (`param` = session length is mandatory; the storage layer reads back to the session start automatically).
  Bucket-mode windows select complete bars whose start lies in the window; tails count existing (joined) bars; OBV/A/D/
  cumulative VWAP/drawdown are anchored at the window start.
- `scripts/stress/overnight_validation.py` phase "round2" validates all of the above against pandas/numpy; keep it green.

### Stress / end-to-end validation (scripts/stress)
`./scripts/stress/run_overnight.sh` generates 30 days of synthetic ticks for 8 tickers (`tickgen.py`),
ingests them and validates the whole indicator stack from every angle (`overnight_validation.py`):
resampling vs pandas, every indicator vs TA-Lib / numpy references (`references.py`), windows / tails /
lookback semantics, snapshot and summary, tick-mode and field overrides, edge cases, ring buffer and reopen,
randomized fuzzing, performance, memory growth, bit-for-bit cross-binding consistency
(`consistency/` programs for Bun, Node, C, C++, Go), round-2 analytics, and the "ops" phase
(`ops_writer.py` child process: cross-process readers, SIGKILL recovery, checksums, retention/rollover under
a live reader, fsync policies, metrics). It writes `stress_report.md`. Use `--quick` for a
smoke run. Run it after any change to the indicator kernels, the storage engine's read paths, or a binding.

### Storage format, durability and readers (src/root.zig)
Files are "HOC2": a 64-byte header (magic, version, schema hash, record size, committed cursor | wrap bit at offset 24,
CRC32C, max file size, last timestamp, last commit wall-clock). `flush()` writes buffered records, then `commit()`
publishes the cursor (readers key on that aligned 8-byte word), then the fsync policy applies. Readers
(`openReader`) take no lock and call `refresh()` from every read entry point; they follow compaction/rollover by
inode change. Writers use `tryLock` (never block). Recovery adopts a valid uncommitted tail and truncates the rest.
`rewriteLogical` is the one primitive behind compaction, retention, legacy migration (temp file + fsync + rename).
Ring capacity: `max_file_size = HEADER_SIZE (64) + N * record_size` — use `DB.HEADER_SIZE` in tests, never 12.
Tests: `src/test_storage.zig`, `bindings/c/test/test_storage.c`, each binding's `test_storage.*`, harness phase "ops".

### Calendars, backtester, universe (src/calendar.zig, src/backtest.zig, src/universe.zig)
- `calendar.zig`: UTC-second sessions; NYSE/LSE holiday rules are validated against `exchange_calendars`
  (venv) by the harness phase "calendar". Add one-off closures to the static lists, never hand-edit sessions.
- Session kinds with `param = 0` need `Columns.session_starts` (filled by the storage layer from the calendar);
  `sessionFloor` handles the lookback. Health gaps are trading-time gaps when a calendar is set.
- `backtest.zig` and `scripts/stress/references_backtest.py` are two independent implementations of one
  documented semantics; both assert the tied example (`test_backtest.zig`). Change semantics in both + the docs.
- `universe.zig` / `references_universe.py` likewise (tied examples A/B). The DB join is `joinSources` in root.zig.
- Tests: `test_calendar_db.zig`, `test_backtest.zig`, `test_backtest_db.zig`, `test_universe.zig`,
  `test_universe_db.zig`, C tests `test_calendar.c` / `test_backtest.c` / `test_universe.c`, binding `test_round4.*`,
  harness phases "calendar", "backtest", "universe".
