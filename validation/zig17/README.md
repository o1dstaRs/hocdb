# Zig 0.17 migration: validation and performance

Validated 2026-10-23. Baseline: commit `2d70588`, Zig 0.15.2. Candidate:
this working tree, Zig 0.17.0. Both performance libraries use ReleaseFast.

The migration preserves the C ABI and HOC2 format, introduces caller-owned
`std.Io` entry points for Zig, and keeps the existing embedding APIs synchronous.
The compiler is pinned in `.zig-version`; `scripts/zig.sh` downloads and verifies
it when needed. CI covers Linux and macOS.

## Controlled performance comparison

Apple M4 Max, 64 GiB, macOS 15.7.4 arm64. Five runs per library, alternating
order; values below are medians. One million 48-byte records, fixed queries and
operation counts, warm filesystem cache. Every run validates its results and
produces the same checksum. [Raw samples, library hashes and workload hash](performance.json).

**Overall: +28.1% throughput**, the equally weighted geometric mean of
the ten workload speedups. The gate passed: overall improvement and no individual
median regression greater than 10%. Bulk load is effectively unchanged; all other
measured medians improved. This score describes these workloads on this machine.

| Workload | Zig 0.15.2 | Zig 0.17.0 | Speedup |
| :-- | --: | --: | --: |
| Buffered append (records/s) | 3,682,997 | 4,412,751 | 1.198x |
| Bulk load (records/s) | 433,234,268 | 432,883,567 | 0.999x |
| Range query (queries/s) | 127,427 | 254,621 | 1.998x |
| Filtered range query (queries/s) | 72,008 | 96,588 | 1.341x |
| Wrapped-ring query (queries/s) | 91,416 | 127,891 | 1.399x |
| Storage aggregation (records/s) | 355,783,257 | 389,450,563 | 1.095x |
| SMA + EMA + RSI batch (rows/s) | 53,273,383 | 54,323,322 | 1.020x |
| Snapshot, 2,500 bars (calls/s) | 1,580 | 1,717 | 1.087x |
| Flush-on-write (records/s) | 111,981 | 175,356 | 1.566x |
| Auto-increment append (records/s) | 3,412,410 | 4,768,035 | 1.397x |

Flush-on-write uses the existing default close-time fsync policy. It measures
record publication, not a durable fsync after every row. Write measurements show
more filesystem timing variation than read/compute measurements; raw samples are
included. The original timed benchmark also ran successfully; its read-throughput
numerator now counts retained records after a ring wrap. Its per-append clock
instrumentation differs between compiler versions, so the fixed C workload above
is the comparison used for the acceptance gate. [Full core benchmark](core-benchmark.txt).

The main optimizations are hardware CRC32C on supported ARM CPUs (portable and
x86 fallbacks retained), bounded block reads for timestamp searches, one or two
reads directly into the final bulk allocation, and three metadata writes per
commit instead of six. Auto-increment uses a small stack buffer for common
records and a tested allocation fallback for large records.

## Functional verification

| Check | Result |
| :-- | :-- |
| macOS arm64 core | 99/99 in Debug, ReleaseSafe, ReleaseFast and ReleaseSmall |
| Ubuntu 24.04 arm64 core | 99/99 in all four modes |
| Linux x86_64 + CRC32C, musl, QEMU | 99/99, ReleaseSafe |
| macOS bindings | 49 standalone suites + 16 Go tests in Debug, ReleaseSafe and ReleaseFast |
| Ubuntu arm64 bindings | 49 standalone suites + 16 Go tests, ReleaseSafe |
| macOS and Linux CLI examples | Built and round-tripped five records |
| Full reference/stress run | 1,775 checks, zero failures |
| Linux reference/stress smoke run | 544 checks, zero failures |
| Old/new HOC2 interoperability | Bidirectional reopen/append for linear, ring and auto-increment files; CRC and calendar metadata |
| Formatting, shell syntax, Python compilation | Passed |

The full run ingested **6,526,823 ticks over 30 days across eight tickers**, tested
2,000 fuzz cases, and performed 3,000 repeated batch/snapshot iterations with no
measured increase in process high-water RSS. It compares indicator and summary
outputs against the pinned TA-Lib/numpy/pandas references, and all bindings
bit-for-bit. It also exercises calendars, backtests, universe joins, retention,
rollover, checksums, fsync policies, live readers and SIGKILL recovery.

- [Full 30-day report](stress-full.md)
- [Linux smoke report](linux-stress-quick.md)
- [Binding summary](bindings-summary.txt)
- [x86 emulation results](core-x86-emulated.txt)
- [Format compatibility](format-compatibility.txt)

Tests added during the migration cover CRC offsets/tails/streaming, sparse-index
boundaries and final-flush ring wrapping, padded/reordered typed records, maximum
timestamps, committed-only reader loads, rejected appends, allocation cleanup,
large auto-increment records, and foreign-thread callers. They caught and fixed
stale ring index bounds, state changes after rejected appends, schema allocation
cleanup, and a missing Node percentile flag in synchronous/worker APIs. Previously
omitted binding suites are now part of `verify_all.sh`.

## File locking and validation scope

A direct `flock` probe found that this host's Docker Desktop shared-folder mount
accepted conflicting exclusive locks. The same probe on Docker's Linux filesystem
correctly refused the second lock. Writers now verify this property and return
`FileLocksUnsupported` on the broken mount; a real container check and an injected
I/O regression test confirm refusal. Use a Linux filesystem or Docker volume for
database files in that environment. [Refusal check](bind-mount-lock-check.txt).

Linux arm64 ran natively inside Docker; x86_64 ran under QEMU, not native x86
hardware. Crash tests use SIGKILL; physical power-loss behavior was not tested.
Reference packages are pinned in `scripts/stress/requirements.txt` on Python 3.12;
TA-Lib 0.7.1 retains the project's existing indicator conventions.

## Reproduction

```sh
./scripts/zig.sh build
./scripts/zig.sh build test --summary all
./scripts/zig.sh build test -Doptimize=ReleaseSafe --summary all
./scripts/zig.sh build test -Doptimize=ReleaseFast --summary all
./scripts/zig.sh build test -Doptimize=ReleaseSmall --summary all
./verify_all.sh
VERIFY_OPTIMIZE=ReleaseSafe ./verify_all.sh
VERIFY_OPTIMIZE=ReleaseFast ./verify_all.sh
./scripts/stress/run_overnight.sh --fuzz 2000
./scripts/zig.sh build bench -Doptimize=ReleaseFast
```

For performance, build the baseline C library with Zig 0.15.2 ReleaseFast and the
candidate with Zig 0.17.0 ReleaseFast, then run `scripts/compare_performance.py`
with `--baseline /old/zig-out/lib --runs 5 --output performance.json`.
`GO`, `PYTHON`, `STRESS_PYTHON`, `STRESS_VENV` and `ZIG` can select local tools.
The validation used Node 24.20.0, Bun 1.4.4 and Go 1.27.3.
