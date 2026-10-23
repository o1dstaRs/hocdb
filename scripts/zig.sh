#!/bin/bash
# Use the pinned release without replacing any system Zig installation.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="$(cat "$ROOT/.zig-version")"
if command -v zig >/dev/null && [ "$(zig version)" = "$VERSION" ]; then
    exec zig "$@"
fi
CACHE="${HOCDB_TOOLCHAIN_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/hocdb-toolchains}"
TOOLCHAIN="$CACHE/zig-$VERSION"
if [ ! -x "$TOOLCHAIN/zig" ]; then
    python3 - "$VERSION" "$TOOLCHAIN" <<'PY'
import hashlib
import json
import pathlib
import platform
import sys
import tarfile
import tempfile
import urllib.request

version, destination = sys.argv[1], pathlib.Path(sys.argv[2])
arch = {"arm64": "aarch64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine())
os_name = {"Darwin": "macos", "Linux": "linux"}.get(platform.system())
if os_name is None:
    raise SystemExit("Install the pinned Zig release manually on this platform.")
with urllib.request.urlopen("https://ziglang.org/download/index.json") as response:
    info = json.load(response)[version][f"{arch}-{os_name}"]
destination.parent.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(dir=destination.parent) as temp:
    temp = pathlib.Path(temp)
    archive = temp / "zig.tar.xz"
    print(f"Downloading Zig {version} for {arch}-{os_name}...", file=sys.stderr)
    urllib.request.urlretrieve(info["tarball"], archive)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != info["shasum"]:
        raise SystemExit("Zig archive SHA256 does not match the official release index.")
    with tarfile.open(archive) as contents:
        contents.extractall(temp, filter="data")
    extracted = next(p for p in temp.iterdir() if p.is_dir())
    # A concurrent invocation may have completed the same installation.
    try:
        extracted.rename(destination)
    except OSError:
        if not (destination / "zig").is_file():
            raise
PY
fi
exec "$TOOLCHAIN/zig" "$@"
