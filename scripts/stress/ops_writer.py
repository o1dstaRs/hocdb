#!/usr/bin/env python3
"""
Child writer process for the "ops" phase of overnight_validation.py.

Appends synthetic records (timestamp = start + i * step) in batches, flushes (commits) after every batch
and publishes its progress atomically to --progress as JSON {"appended": n, "committed": n}. The parent
process attaches lock-free readers, kills this process with SIGKILL at random points and verifies crash
recovery, retention and rollover from the outside.
"""
import argparse
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "bindings", "python"))
from hocdb_python import HOCDB, HOCDBField, FieldTypes  # noqa: E402

SCHEMA = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("price", FieldTypes.F64),
    HOCDBField("size", FieldTypes.F64),
    HOCDBField("bid", FieldTypes.F64),
    HOCDBField("ask", FieldTypes.F64),
    HOCDBField("side", FieldTypes.BOOL),
]


def publish(path, appended, committed):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"appended": appended, "committed": committed, "wall_ns": time.time_ns()}, f)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--start-ts", type=int, required=True)
    ap.add_argument("--step", type=int, default=1000)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--count", type=int, default=0, help="stop after this many records (0 = run until killed)")
    ap.add_argument("--sleep-ms", type=float, default=2.0, help="pause after every batch")
    ap.add_argument("--fsync", default="on_close")
    ap.add_argument("--fsync-interval-ms", type=int, default=0)
    ap.add_argument("--retention-span", type=int, default=0)
    ap.add_argument("--rollover-size", type=int, default=0)
    ap.add_argument("--timestamp-unit-ns", type=int, default=0)
    ap.add_argument("--progress", required=True)
    args = ap.parse_args()

    db = HOCDB(args.ticker, args.dir, SCHEMA, fsync=args.fsync, fsync_interval_ms=args.fsync_interval_ms,
               retention_span=args.retention_span, rollover_size=args.rollover_size,
               timestamp_unit_ns=args.timestamp_unit_ns)
    committed = db.metrics()["committed_records"]
    i = 0
    publish(args.progress, i, i)
    print(f"ready committed={committed}", flush=True)
    while args.count == 0 or i < args.count:
        n = args.batch if args.count == 0 else min(args.batch, args.count - i)
        for _ in range(n):
            ts = args.start_ts + i * args.step
            price = 100.0 + 5.0 * math.sin(i / 700.0) + 0.01 * ((i * 7919) % 97 - 48)
            db.append(ts, price, float(1 + (i * 31) % 50), price - 0.01, price + 0.01, (i % 3) == 0)
            i += 1
        db.flush()
        publish(args.progress, i, i)
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)
    db.close()
    publish(args.progress, i, i)
    print(f"done appended={i}", flush=True)


if __name__ == "__main__":
    main()
