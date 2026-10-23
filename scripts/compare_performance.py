#!/usr/bin/env python3
"""Compare identical fixed workloads against two ReleaseFast C libraries.

python3 scripts/compare_performance.py --baseline /path/to/old/zig-out/lib \
    --candidate zig-out/lib --runs 5 --output performance.json

Runs alternate between libraries to reduce order/thermal bias. No thresholds
are inferred from a different machine or workload. A nonzero exit flags an
overall geometric-mean regression or any individual median regression >10%.
"""
import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import statistics
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=pathlib.Path)
    parser.add_argument("--candidate", default=ROOT / "zig-out/lib", type=pathlib.Path)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    if args.runs < 3:
        parser.error("use at least three runs")
    binaries = ROOT / "test_binaries"
    binaries.mkdir(exist_ok=True)
    samples = {"baseline": [], "candidate": []}
    libraries = {}
    for label, lib in (("baseline", args.baseline), ("candidate", args.candidate)):
        lib = lib.resolve()
        library = lib / ("libhocdb_c.dylib" if platform.system() == "Darwin" else "libhocdb_c.so")
        libraries[label] = {"path": str(library), "sha256": hashlib.sha256(library.read_bytes()).hexdigest()}
        subprocess.run(["clang", "-O3", "-std=c11", str(ROOT / "scripts/bench_native.c"),
                        "-I", str(ROOT / "bindings/c"), "-L", str(lib), "-lhocdb_c",
                        f"-Wl,-rpath,{lib}", "-o", str(binaries / f"bench_{label}")], check=True)
    for run in range(args.runs):
        order = ["baseline", "candidate"] if run % 2 == 0 else ["candidate", "baseline"]
        for label in order:
            with tempfile.TemporaryDirectory(prefix="b_c_test_data_perf_") as data:
                lib_dir = str(pathlib.Path(libraries[label]["path"]).parent)
                env = dict(os.environ, DYLD_LIBRARY_PATH=lib_dir, LD_LIBRARY_PATH=lib_dir)
                sample = json.loads(subprocess.check_output([str(binaries / f"bench_{label}"), data], text=True, env=env))
            samples[label].append(sample)
            print(f"{label} {run + 1}/{args.runs}: {sample}", flush=True)
    checksums = {s["checksum"] for group in samples.values() for s in group}
    assert len(checksums) == 1, "workloads produced different checksums"
    medians = {label: {key: statistics.median(s[key] for s in group)
                       for key in group[0] if key != "checksum"}
               for label, group in samples.items()}
    ratios = {key: medians["candidate"][key] / value for key, value in medians["baseline"].items()}
    overall = math.exp(statistics.mean(math.log(value) for value in ratios.values()))
    result = {"runs": args.runs, "platform": platform.platform(), "libraries": libraries,
              "workload_sha256": hashlib.sha256((ROOT / "scripts/bench_native.c").read_bytes()).hexdigest(),
              "samples": samples, "medians": medians,
              "speedup": ratios, "geometric_mean_speedup": overall}
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"speedup": ratios, "geometric_mean_speedup": overall}, indent=2))
    return 0 if overall >= 1 and min(ratios.values()) >= 0.9 else 1


if __name__ == "__main__":
    raise SystemExit(main())
