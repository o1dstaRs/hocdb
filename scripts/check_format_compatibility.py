#!/usr/bin/env python3
"""Reopen/append HOC2 files between two libraries in both directions.

python3 scripts/check_format_compatibility.py --baseline /old/libhocdb_c.dylib \
    --candidate zig-out/lib/libhocdb_c.dylib
"""
import argparse
import os
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def worker(directory, step):
    sys.path.insert(0, str(ROOT / "bindings/python"))
    from hocdb_python import HOCDB, HOCDBField, FieldTypes
    schema = [HOCDBField("timestamp", FieldTypes.I64),
              HOCDBField("price", FieldTypes.F64), HOCDBField("volume", FieldTypes.F64)]
    for kind in ("linear", "ring", "auto"):
        ring, auto = kind == "ring", kind == "auto"
        db = HOCDB(kind, directory, schema,
                   max_file_size=64 + (17 if ring else 200) * 24,
                   overwrite_on_full=ring, auto_increment=auto,
                   verify_on_open=not ring, timestamp_unit_ns=1000, calendar="nyse")
        try:
            if step:
                rows = db.load()
                first = max(0, step * 50 - 17) if ring else 0
                assert len(rows) == step * 50 - first
                for i, row in enumerate(rows, first):
                    assert row == {"timestamp": i + 1 if auto else 1000 + i * 7,
                                   "price": i + 0.5, "volume": float(i + 10)}, (kind, i, row)
            for i in range(step * 50, (step + 1) * 50):
                assert db.append(0 if auto else 1000 + i * 7, i + 0.5, float(i + 10))
            db.flush()
            if not ring:
                assert db.verify()
            assert db.get_calendar() == 3
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--candidate", type=pathlib.Path)
    parser.add_argument("--worker", type=pathlib.Path)
    parser.add_argument("--step", type=int)
    args = parser.parse_args()
    if args.worker is not None:
        worker(str(args.worker), args.step)
        return
    if args.baseline is None or args.candidate is None:
        parser.error("--baseline and --candidate are required")
    baseline, candidate = args.baseline.resolve(), args.candidate.resolve()
    for order in ((baseline, candidate, baseline), (candidate, baseline, candidate)):
        with tempfile.TemporaryDirectory(prefix="b_python_test_data_compat_") as directory:
            for step, library in enumerate(order):
                env = dict(os.environ, HOCDB_LIB_PATH=str(library))
                subprocess.run([sys.executable, __file__, "--worker", directory, "--step", str(step)],
                               env=env, check=True)
    print("PASS: both directions, three opens each, linear/ring/auto-increment, CRC and calendar metadata")


if __name__ == "__main__":
    main()
