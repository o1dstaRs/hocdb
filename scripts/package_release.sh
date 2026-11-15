#!/usr/bin/env bash
# Package a tested native CLI. Run from any directory after zig build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VERSION="$(python3 -c 'import json; print(json.load(open("package.json"))["version"])')"
PLATFORM="${1:?Usage: scripts/package_release.sh <linux-x86_64|macos-aarch64> [output-directory]}"
case "$PLATFORM" in linux-x86_64|macos-aarch64) ;; *) echo "Unsupported release platform" >&2; exit 1 ;; esac
OUTPUT="${2:-dist}"
mkdir -p "$OUTPUT"
OUTPUT="$(cd "$OUTPUT" && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
NAME="hocdb-$VERSION-$PLATFORM"
mkdir -p "$STAGE/$NAME/bin" "$STAGE/$NAME/docs"
cp zig-out/bin/hocdb "$STAGE/$NAME/bin/hocdb"
cp LICENSE README.md CHANGELOG.md "$STAGE/$NAME/"
cp docs/QUICKSTART.md docs/AGENT_GUIDE.md "$STAGE/$NAME/docs/"
cp src/cli-schema.json "$STAGE/$NAME/docs/commands.json"
"$STAGE/$NAME/bin/hocdb" --version
COPYFILE_DISABLE=1 tar -czf "$OUTPUT/$NAME.tar.gz" -C "$STAGE" "$NAME"
python3 - "$OUTPUT/$NAME.tar.gz" <<'PY'
import hashlib, pathlib, sys
p = pathlib.Path(sys.argv[1])
p.with_suffix(p.suffix + ".sha256").write_text(hashlib.sha256(p.read_bytes()).hexdigest() + "  " + p.name + "\n")
print(p.name)
PY
