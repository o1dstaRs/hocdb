# Start with HOCDB

HOCDB stores timestamped records on your machine and computes statistics and
indicators over them. Use its command line without writing application code,
or embed the library through Zig, C, C++, Python, Go, Node.js or Bun.

## Install the command line

Download the archive for your machine from
[v0.1.1](https://github.com/o1dstaRs/hocdb/releases/tag/v0.1.1):

| Machine | Archive |
| --- | --- |
| Linux x86_64 (static musl binary) | hocdb-0.1.1-linux-x86_64.tar.gz |
| macOS 13+ on Apple Silicon | hocdb-0.1.1-macos-aarch64.tar.gz |

Download SHA256SUMS from the same release. From the download directory, verify
your archive with shasum -a 256 (macOS) or sha256sum (Linux), comparing the
result with SHA256SUMS. Extract the archive, then run these commands
**inside the extracted folder**:

~~~bash
./bin/hocdb --version
mkdir -p "$HOME/.local/bin"
install -m 755 ./bin/hocdb "$HOME/.local/bin/hocdb"
export PATH="$HOME/.local/bin:$PATH"
~~~

Add that PATH line to your shell profile to keep it for later sessions.
The CLI has no language-runtime requirement. macOS binaries are unsigned;
if macOS blocks a downloaded binary, follow the system's Privacy & Security
prompt after checking its source and checksum.

To build from source instead:

~~~bash
git clone --branch v0.1.1 https://github.com/o1dstaRs/hocdb.git
cd hocdb
./scripts/zig.sh build -Doptimize=ReleaseSafe
./zig-out/bin/hocdb --help
~~~

The script uses the pinned Zig 0.17.1 compiler, downloading and checking its
official SHA256 if needed. Use ./zig-out/bin/hocdb instead of hocdb below.

## Store your first records

Choose a directory that does not already exist. Its parent must exist.

~~~bash
hocdb init --db ./market --preset ticks --timestamp-unit s
echo '[{"timestamp":1700000000,"price":100,"volume":2},{"timestamp":1700000060,"price":110,"volume":3}]' \
  | hocdb append --db ./market
hocdb query --db ./market
hocdb stats --db ./market --field price
~~~

The last command returns:

~~~json
{"ok":true,"command":"stats","data":{"count":2,"min":100,"max":110,"sum":210,"mean":105}}
~~~

The ticks preset has timestamp (i64), price (f64) and volume (f64). Use the ohlcv
preset for timestamp, open, high, low, close and volume. Timestamps must increase
strictly, starting at Unix epoch 0 or later. Ranges include --start and exclude --end, in the unit selected at init:
s, ms, us or ns.

Each directory contains hocdb.json (schema and timestamp unit) and series.bin
(the HOC2 database). Keep them together. The schema is fixed after creation.
CLI databases hold up to 2 GiB and never overwrite old records automatically.

## Read a useful summary

~~~bash
hocdb info --db ./market
hocdb snapshot --db ./market
hocdb health --db ./market --gap-threshold 120
hocdb query --db ./market --start 1700000000 --end 1700000060
~~~

Snapshot computes the latest indicator values from up to 2,500 records by
default. Two records are insufficient for RSI or a 200-period moving average:
those values are null. More history produces more defined indicators.

Use --bucket with a width in stored timestamp units to build bars before the
snapshot. For second timestamps, --bucket 300 means five-minute bars. Set
--periods-per-year explicitly when comparing annualized risk statistics.

Queries default to a maximum of 100 rows. If more match, the command fails
without silently truncating them. Narrow the time range or set --limit up to
10000. Use a language binding for bulk reads.

## Import your own schema

Write a JSON array to schema.json:

~~~json
[
  {"name":"timestamp","type":"i64"},
  {"name":"temperature","type":"f64"},
  {"name":"active","type":"bool"}
]
~~~

~~~bash
hocdb init --db ./sensors --schema schema.json --timestamp-unit ms
echo '{"timestamp":1700000000000,"temperature":21.5,"active":true}' \
  | hocdb append --db ./sensors
hocdb stats --db ./sensors --field temperature
~~~

Supported types: i64, u64, f64, u8, bool and string (up to 128 UTF-8 bytes).
The first field must be timestamp:i64. Names must be unique; input records
must contain exactly the schema fields. Market snapshots and health checks
expect a numeric close or price field; arbitrary schemas can use query and stats.

## Recovery and automation

Run hocdb describe for a JSON command contract or read the
[agent guide](AGENT_GUIDE.md). Errors contain a stable code and recovery guidance.
Read commands are read-only. No CLI command drops or deletes a database.

HOCDB supports one writer per database with concurrent readers. A competing
writer receives DatabaseLocked. A successful append flushes and fsyncs its
batch. Invalid records or timestamp order are rejected before writing input
records; an I/O failure can leave a partial batch, so inspect info/query before
retrying an append.
