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
  "${STRESS_PYTHON:-python3.12}" -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q -r scripts/stress/requirements.txt
fi
"$VENV/bin/python" - <<'PY'
import sys
import numpy, pandas, talib
if sys.version_info >= (3, 13):
    raise SystemExit("Reference validation requires Python 3.12 with NumPy 1.26.4; set STRESS_PYTHON or STRESS_VENV.")
if talib.__version__ != "0.7.1":
    raise SystemExit("Reference conventions require TA-Lib==0.7.1; newer TA-Lib versions change MFI semantics.")
PY
echo "Building libraries..."
"${ZIG:-$ROOT/scripts/zig.sh}" build c-bindings bindings -Doptimize=ReleaseFast >/dev/null
GO_ARGS=()
if [ -n "${GO:-}" ]; then
  GO_ARGS=(--go "$GO")
elif command -v go >/dev/null 2>&1; then
  GO_ARGS=(--go "$(command -v go)")
fi
exec "$VENV/bin/python" scripts/stress/overnight_validation.py "${GO_ARGS[@]}" "$@"
