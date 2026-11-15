HOCDB's first tagged release makes its embedded time-series engine available
through a native, non-interactive JSON command line.

Download the static Linux x86_64 archive or the macOS 13+ Apple Silicon
archive and verify it against SHA256SUMS. Extract it and run bin/hocdb --help; no compiler or language
runtime is needed for the CLI.

~~~bash
hocdb init --db ./market --preset ticks
echo '{"timestamp":1700000000,"price":100,"volume":2}' | hocdb append --db ./market
hocdb query --db ./market
hocdb snapshot --db ./market
hocdb describe
~~~

The CLI has explicit database paths, persisted schema presets or custom schemas,
validated batches, read-only queries, bounded JSON results and errors with
recovery guidance. Large integers retain their precision; undefined indicator
values are null. Running hocdb no longer executes or deletes demo data.

- [Quickstart](https://github.com/o1dstaRs/hocdb/blob/v0.1.1/docs/QUICKSTART.md)
- [Agent guide](https://github.com/o1dstaRs/hocdb/blob/v0.1.1/docs/AGENT_GUIDE.md)
- [Command contract](https://github.com/o1dstaRs/hocdb/blob/v0.1.1/src/cli-schema.json)
- [Analytics reference](https://github.com/o1dstaRs/hocdb/blob/v0.1.1/INDICATORS.md)
- [Project page](https://zolotukhin.ai/hocdb/)

The existing HOC2 storage format and C ABI are unchanged. Zig embedders and
C/C++/Python/Go/Node.js/Bun bindings remain available from source with the pinned
Zig 0.17.1 toolchain.

Release archives use ReleaseSafe and must pass the core suite plus black-box
CLI tests on both platforms, including tests of the extracted archive.

Known limits: fixed schemas, one writer per database, 2 GiB CLI databases,
unsigned macOS binaries, no Windows binary, no MCP/HTTP server. CLI directories
use a manifest and do not automatically discover existing binding-created
databases. Bindings need a native library build; no new npm/PyPI publication is
part of this release. Version 0.1.1 preserves the library's existing version.
