#!/bin/bash
# Exercise every standalone binding regression suite with one consistent build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
ZIG="${ZIG:-$ROOT/scripts/zig.sh}"
GO="${GO:-go}"
PYTHON="${PYTHON:-python3}"
OPTIMIZE="${VERIFY_OPTIMIZE:-Debug}"

for tool in "$ZIG" "$GO" "$PYTHON" bun node clang clang++; do
    command -v "$tool" >/dev/null || { echo "Required verification tool missing: $tool" >&2; exit 1; }
done

export PYTHONPATH="$ROOT/bindings/python${PYTHONPATH:+:$PYTHONPATH}"
export DYLD_LIBRARY_PATH="$ROOT/zig-out/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="$ROOT/zig-out/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mkdir -p test_binaries

"$ZIG" build c-bindings bindings -Doptimize="$OPTIMIZE"
for suite in bindings/bun/test/test*.ts; do
    echo "[Bun] $suite"
    bun run "$suite"
done
for suite in bindings/python/test/test*.py; do
    echo "[Python] $suite"
    if [ "$suite" = bindings/python/test/test.py ]; then
        "$PYTHON" "$suite" --no-benchmark
    else
        "$PYTHON" "$suite"
    fi
done
(cd bindings/go && "$GO" test -count=1 -v ./test/...)

for suite in bindings/c/test/*.c; do
    case "$suite" in *bench*) continue ;; esac
    binary="test_binaries/c_$(basename "$suite" .c)"
    echo "[C] $suite"
    clang -std=c11 -pthread "$suite" -o "$binary" -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,"$ROOT/zig-out/lib" -lm
    "$binary"
done
for suite in bindings/c/test/*.cpp bindings/cpp/test/*.cpp; do
    case "$suite" in *bench*) continue ;; esac
    binary="test_binaries/cpp_$(basename "$(dirname "$(dirname "$suite")")")_$(basename "$suite" .cpp)"
    echo "[C++] $suite"
    clang++ -std=c++17 "$suite" -o "$binary" -I bindings/c -I bindings/cpp -L zig-out/lib -lhocdb_c -Wl,-rpath,"$ROOT/zig-out/lib"
    "$binary"
done
for suite in bindings/node/test/test*.js; do
    echo "[Node] $suite"
    node "$suite"
done

echo "ALL BINDINGS VERIFIED SUCCESSFULLY ($OPTIMIZE)"
