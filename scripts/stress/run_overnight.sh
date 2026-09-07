#!/bin/bash
# Full validation of the HOCDB indicator stack on 30 days of synthetic ticks.
#
#   ./scripts/stress/run_overnight.sh                # full run (~30-60 min)
#   ./scripts/stress/run_overnight.sh --quick        # smoke run (a few minutes)
#   ./scripts/stress/run_overnight.sh --days 7 --tickers BTCUSD,AAPL
#
# Creates ./.venv-stress with numpy/pandas/TA-Lib on first use, builds the C
# and Node libraries, then runs scripts/stress/overnight_validation.py and
# writes stress_report.md at the repo root. Any extra arguments are passed to
# the Python driver (see --help). Set GO=/path/to/go to include the Go
# consistency check when go is not on PATH.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
VENV="${STRESS_VENV:-$ROOT/.venv-stress}"
if [ ! -x "$VENV/bin/python" ]; then
  echo "Creating $VENV with numpy, pandas and TA-Lib..."
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q "numpy<2" "pandas<2.3" TA-Lib
fi
"$VENV/bin/python" -c "import numpy, pandas, talib" || { echo "venv is missing numpy/pandas/TA-Lib"; exit 1; }
echo "Building libraries..."
zig build c-bindings >/dev/null
zig build bindings >/dev/null
GO_ARGS=()
if [ -n "${GO:-}" ]; then
  GO_ARGS=(--go "$GO")
elif command -v go >/dev/null 2>&1; then
  GO_ARGS=(--go "$(command -v go)")
fi
exec "$VENV/bin/python" scripts/stress/overnight_validation.py "${GO_ARGS[@]}" "$@"
