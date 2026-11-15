# HOCDB repository guidance

Read AGENT.md before changing code; it contains the existing contributor rules
and required core/binding verification. For using HOCDB as an agent tool, read
docs/AGENT_GUIDE.md and run hocdb describe instead.

- Use ./scripts/zig.sh for the pinned Zig toolchain.
- Build the CLI with ./scripts/zig.sh build; validate it with python3 tests/test_cli.py.
- Keep src/cli-schema.json, CLI help, documentation and tests consistent.
- Do not introduce implicit database paths, destructive startup behavior,
  unbounded query output or non-JSON output in data commands.
- Release packaging and validation live in .github/workflows/release.yml and
  scripts/package_release.sh. Releases are drafted after both platforms pass.
