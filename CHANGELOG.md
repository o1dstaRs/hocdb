# Changelog

## 0.1.1 — 2026-11-15

First tagged public release, using the version already recorded by the library.

- Native JSON CLI: init, append, info, query, stats, snapshot, health, describe
  and --version. Running hocdb without arguments now prints help.
- Explicit paths, schema presets, custom JSON schemas, timestamp units, validated
  batches, bounded queries, read-only analysis and actionable JSON errors.
- Lossless large integer output and null for undefined indicator warm-up.
- No automatic deletion or circular overwrite in the CLI.
- Linux x86_64 and macOS Apple Silicon CLI archives with SHA256 checksums.
- Quickstart, agent guide, machine-readable command contract and release workflow.

The release also includes the existing embedded HOC2 storage engine, Zig
0.17.1 migration, six language bindings, technical indicators, calendars,
backtesting and universe analytics. The CLI does not change the storage format
or C ABI.

Limits: one writer per database; fixed schemas; CLI databases are limited to
2 GiB. macOS binaries are unsigned. Language bindings are distributed as source
and need native libraries built with Zig. This release does not publish new
packages to npm or PyPI.
