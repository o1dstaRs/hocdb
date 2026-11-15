<p align="center">
  <img src="assets/hocdb_trademark_256.png" alt="HOCDB" width="160">
</p>

# HOCDB

An embedded time-series database in Zig, with a native JSON command line for
people and agents. Store timestamped records, query ranges, and compute
statistics and market indicators locally.

[Download v0.1.1](https://github.com/o1dstaRs/hocdb/releases/tag/v0.1.1) ·
[Quickstart](docs/QUICKSTART.md) · [Agent guide](docs/AGENT_GUIDE.md) ·
[Analytics reference](INDICATORS.md) · [Project page](https://zolotukhin.ai/hocdb/)

## Start in a minute

Download the Linux x86_64 or macOS Apple Silicon archive from the release page,
verify its SHA256, and extract it. Run `./bin/hocdb --help` from the extracted
folder, or put the binary on your PATH following the [installation guide](docs/QUICKSTART.md).

```bash
hocdb init --db ./market --preset ticks --timestamp-unit s
echo '[{"timestamp":1700000000,"price":100,"volume":2},{"timestamp":1700000060,"price":110,"volume":3}]' \
  | hocdb append --db ./market
hocdb query --db ./market
hocdb stats --db ./market --field price
hocdb snapshot --db ./market
```

Use a **new** directory for init; existing directories are never overwritten.
The parent directory must exist. The ticks preset stores timestamp, price and
volume; ohlcv stores timestamp, open, high, low, close and volume. Custom schemas
are supported with `--schema schema.json`.

Read commands return JSON and use read-only handles. No CLI command deletes
data. Append validates the complete batch before writing and flushes/fsyncs
before reporting success. Timestamps must increase strictly; ranges include
start and exclude end. The [quickstart](docs/QUICKSTART.md) covers units, limits,
custom schemas and recovery.

## Built for agent tools

```bash
hocdb describe                   # JSON command contract, defaults and limits
hocdb info --db ./market          # schema, timestamp unit and record count
hocdb health --db ./market        # gaps, outliers and volume quality
```

Success is `{"ok":true,"command":"...","data":...}` on stdout. Failure is
`{"ok":false,"error":{"code":"...","message":"..."}}` on stderr with exit 1.
Help is plain text. Commands never prompt.

Queries default to a 100-row limit and fail if more match, so an agent cannot
accidentally dump the entire database into its context. Large integers become
decimal strings outside JavaScript's safe range. Undefined indicator warm-up
values become JSON null. See the [agent guide](docs/AGENT_GUIDE.md) and
[machine-readable contract](src/cli-schema.json).

## Embed the engine

The library uses fixed-size records, timestamp indexing, buffered append,
CRC32C integrity checks, SIMD aggregation and the HOC2 storage format. It offers
one writer with concurrent readers, configurable fsync, optional ring buffers,
retention and rollover. Schemas remain fixed after creation.

| Language | Guide |
| --- | --- |
| Zig | Import the hocdb module from build.zig |
| C / C++ | [C API](bindings/c/README.md), [C++ wrapper](bindings/cpp/README.md) |
| Python | [ctypes binding](bindings/python/README.md) |
| Go | [CGO binding](bindings/go/README.md) |
| Node.js | [N-API binding](bindings/node/README.md) |
| Bun | [FFI binding](bindings/bun/README.md) |

Bindings are distributed as source and need the native libraries. The release
archives contain the standalone CLI, not prebuilt language packages.

```bash
git clone --branch v0.1.1 https://github.com/o1dstaRs/hocdb.git
cd hocdb
./scripts/zig.sh build -Doptimize=ReleaseSafe
./scripts/zig.sh build c-bindings bindings -Doptimize=ReleaseFast
```

The compiler is pinned to Zig **0.17.1**. scripts/zig.sh uses an installed matching
compiler or downloads it to a user cache and verifies the official SHA256.
It does not replace your system compiler.

A complete Python example, run from the checkout after building c-bindings:

```python
from bindings.python.hocdb_python import HOCDB, HOCDBField, FieldTypes

schema = [HOCDBField("timestamp", FieldTypes.I64),
          HOCDBField("price", FieldTypes.F64),
          HOCDBField("volume", FieldTypes.F64)]
db = HOCDB("EXAMPLE", "./example-data", schema, overwrite_on_full=False)
try:
    db.append({"timestamp": 1700000000, "price": 100.0, "volume": 2.0})
    db.flush()
    print(db.get_stats(1700000000, 1700000060, "price"))
finally:
    db.close()
```

Use a fresh example-data directory for that one-shot example. For Zig projects:

```bash
zig fetch --save https://github.com/o1dstaRs/hocdb/archive/refs/tags/v0.1.1.tar.gz
```

In build.zig, obtain the dependency with `b.dependency("hocdb", .{ .target = target,
.optimize = optimize })` and add `dep.module("hocdb")` to your root module with
`addImport("hocdb", ...)`. See src/main.zig for the CLI entry point and src/root.zig
for exported engine types. Zig embedders may provide caller-owned std.Io through
initWithIo/openReaderWithIo; default APIs use synchronous I/O.

## Indicators and analytics

The library includes 83 technical, microstructure, pair, label and session
indicators, OHLCV resampling, risk summaries, exchange calendars, backtesting
and universe analysis. Snapshot computes roughly 100 named values in one call.
The CLI exposes snapshot and health; language bindings expose the broader API.

[INDICATORS.md](INDICATORS.md) defines the conventions, warm-up, column roles,
look-ahead labels and annualization. Kernel tests compare against TA-Lib or
explicit NumPy references. Look-ahead labels belong in offline training and
validation, not live features.

## Limits and evidence

HOCDB is an embedded library with a CLI, not a network database. It has fixed
schemas and one writer per database. CLI databases use a 2 GiB capacity without
circular overwrite. CLI manifests do not automatically discover existing
binding-created databases. macOS release binaries are unsigned.

Use a filesystem that enforces exclusive file locks. Docker Desktop shared
folders can return FileLocksUnsupported; use a Linux filesystem or Docker volume.

Performance depends on the workload, hardware and durability policy. The
[Zig migration report](validation/zig17/README.md) includes hardware, workload,
raw samples and validation results. No cross-database speed ranking is claimed.

## Development

```bash
./scripts/zig.sh build test --summary all
./scripts/zig.sh build -Doptimize=ReleaseSafe
python3 tests/test_cli.py
./verify_all.sh
```

The verification script requires Node.js, Bun, Python, Go and a C/C++ compiler.
[AGENT.md](AGENT.md) describes contributor checks; [CHANGELOG.md](CHANGELOG.md)
records release changes. HOCDB is [MIT licensed](LICENSE).
