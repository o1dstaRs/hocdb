"""Black-box CLI contract and data-safety tests; only Python's stdlib is needed."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get("HOCDB_BIN", ROOT / "zig-out/bin/hocdb")).resolve()


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="b_cli_test_data_")
        self.root = Path(self.temp.name)
        self.db = self.root / "market"

    def tearDown(self):
        self.temp.cleanup()

    def run_cli(self, *args, input=None, error=None):
        proc = subprocess.run(
            [str(BIN), *map(str, args)], input=input, text=True,
            capture_output=True, cwd=self.root, timeout=30,
        )
        self.assertEqual(proc.returncode, 1 if error else 0, proc.stderr)
        self.assertEqual(proc.stdout if error else proc.stderr, "")
        raw = proc.stderr if error else proc.stdout
        result = json.loads(raw, parse_constant=lambda v: self.fail(f"Non-JSON number: {v}"))
        self.assertEqual(result["ok"], error is None)
        if error:
            self.assertEqual(result["error"]["code"], error, result)
            self.assertTrue(result["error"]["message"])
        return result.get("data")

    def init(self, *args):
        return self.run_cli("init", "--db", self.db, *args)

    def append(self, records, **kwargs):
        return self.run_cli("append", "--db", self.db, input=json.dumps(records), **kwargs)

    def record(self, ts=1700000000, price=100):
        return dict(timestamp=ts, price=price, volume=2)

    def test_help_has_no_side_effects(self):
        data = self.root / "data"
        data.mkdir()
        sentinel = data / "important.txt"
        sentinel.write_text("keep")
        for args in [[], ["--help"], ["query", "--help"]]:
            proc = subprocess.run([str(BIN), *args], cwd=self.root, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0)
            self.assertIn("Usage:", proc.stdout)
            self.assertEqual(proc.stderr, "")
        self.assertEqual(sentinel.read_text(), "keep")
        self.assertEqual(list(self.root.iterdir()), [data])

    def test_discovery_and_validation(self):
        self.assertEqual(self.run_cli("--version")["version"], "0.1.1")
        commands = self.run_cli("describe")["commands"]
        self.assertEqual({k for k, v in commands.items() if v["mutates"]}, {"init", "append"})
        self.run_cli("drop", error="UnknownCommand")
        self.run_cli("query", error="MissingDatabase")
        self.run_cli("query", "--db", error="MissingOptionValue")
        self.run_cli("query", "--db", self.db, "--oops", "yes", error="UnknownOption")
        self.run_cli("init", "--db", self.db, "--db", self.db, error="DuplicateOption")
        self.run_cli("init", "--db", self.db, "--preset", "wrong", error="UnknownPreset")
        self.assertFalse(self.db.exists())

    def test_roundtrip_and_readonly_commands(self):
        self.init()
        records = [self.record(), self.record(1700000060, 110)]
        self.assertEqual(self.append(records), {"appended": 2, "records": 2})
        before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.db.iterdir()}
        self.assertEqual(self.run_cli("query", "--db", self.db)["records"], records)
        self.assertEqual(self.run_cli("query", "--db", self.db, "--end", "1700000060")["records"], records[:1])
        self.assertEqual(self.run_cli("query", "--db", self.db, "--start", "1700000060")["records"], records[1:])
        stats = self.run_cli("stats", "--db", self.db)
        self.assertEqual(stats, dict(count=2, min=100, max=110, sum=210, mean=105))
        snap = self.run_cli("snapshot", "--db", self.db)
        self.assertIsNone(snap["rsi_14"])
        self.assertEqual(snap["close"], 110)
        self.assertEqual(self.run_cli("health", "--db", self.db)["count"], 2)
        self.assertTrue(self.run_cli("info", "--db", self.db)["read_only"])
        after = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.db.iterdir()}
        self.assertEqual(before, after)
        self.run_cli("init", "--db", self.db, error="PathAlreadyExists")
        self.assertEqual(self.run_cli("info", "--db", self.db)["records"], 2)

    def test_rejected_batches_do_not_append(self):
        self.init()
        self.append(self.record())
        bad_records = [
            [self.record(1700000060), {"timestamp": 1700000120, "price": "wrong", "volume": 2}],
            [self.record(1700000060), {"timestamp": 1700000120, "price": 10}],
            [self.record(1700000060), dict(self.record(1700000120), extra=True)],
        ]
        for records in bad_records:
            self.append(records, error="InvalidRecord")
        for records in [
            [self.record(1700000060), self.record(1700000060)],
            [self.record(1700000060), self.record(1700000001)],
            self.record(),
        ]:
            self.append(records, error="TimestampOutOfOrder")
        self.run_cli("append", "--db", self.db, input='{"timestamp":1700000060,"price":1e999,"volume":2}', error="InvalidRecord")
        self.assertEqual(self.run_cli("info", "--db", self.db)["records"], 1)

    def test_limits_empty_and_missing_data(self):
        self.run_cli("query", "--db", self.db, error="FileNotFound")
        self.assertFalse(self.db.exists())
        self.init()
        self.assertEqual(self.run_cli("query", "--db", self.db)["count"], 0)
        stats = self.run_cli("stats", "--db", self.db)
        self.assertEqual(stats["count"], 0)
        self.assertIsNone(stats["mean"])
        self.append([self.record(), self.record(1700000060)])
        self.run_cli("query", "--db", self.db, "--limit", "1", error="BufferTooSmall")
        self.run_cli("query", "--db", self.db, "--limit", "10001", error="InvalidNumber")
        self.run_cli("query", "--db", self.db, "--start", "2", "--end", "1", error="InvalidRange")
        self.run_cli("snapshot", "--db", self.db, "--bars", "0", error="InvalidNumber")
        self.run_cli("snapshot", "--db", self.db, "--periods-per-year", "nan", error="InvalidNumber")
        (self.db / "series.bin").unlink()
        self.append(self.record(), error="FileNotFound")
        self.assertFalse((self.db / "series.bin").exists())

    def test_custom_schema_and_lossless_integers(self):
        schema = self.root / "schema.json"
        schema.write_text(json.dumps([
            {"name": "timestamp", "type": "i64"}, {"name": "value", "type": "u64"},
            {"name": "active", "type": "bool"}, {"name": "label", "type": "string"},
            {"name": "code", "type": "u8"},
        ]))
        self.init("--schema", schema, "--timestamp-unit", "ns")
        record = dict(timestamp="1700000000000000000", value="18446744073709551615", active=True, label="café", code=255)
        self.append(record)
        self.assertEqual(self.run_cli("query", "--db", self.db)["records"], [record])
        self.append(dict(record, timestamp="1700000000000000001", code=256), error="InvalidRecord")
        self.append(dict(record, timestamp="9223372036854775807"), error="InvalidRecord")
        self.append(dict(record, timestamp="-1"), error="InvalidRecord")
        numeric = dict(record, timestamp=1700000000000000001, value=18446744073709551615)
        self.append(numeric)
        self.assertEqual(self.run_cli("query", "--db", self.db)["records"][1],
                         dict(record, timestamp="1700000000000000001"))

    def test_string_columns_are_not_market_prices(self):
        schema = self.root / "schema.json"
        schema.write_text('[{"name":"timestamp","type":"i64"},{"name":"price","type":"string"}]')
        self.init("--schema", schema)
        self.append({"timestamp": 1, "price": "not a number"})
        for command in ["stats", "snapshot", "health"]:
            self.run_cli(command, "--db", self.db, error="InvalidFieldType")

    def test_ohlcv_and_file_input(self):
        self.init("--preset", "ohlcv", "--timestamp-unit", "ms")
        records = [dict(timestamp=1700000000000 + i*60000, open=100+i, high=102+i, low=99+i, close=101+i, volume=2) for i in range(250)]
        path = self.root / "records.json"
        path.write_text(json.dumps(records))
        self.run_cli("append", "--db", self.db, "--input", path)
        snap = self.run_cli("snapshot", "--db", self.db, "--periods-per-year", "365")
        self.assertEqual(snap["bars"], 250)
        self.assertEqual(snap["rsi_14"], 100)
        self.assertEqual(self.run_cli("query", "--db", self.db, "--limit", "250")["records"], records)


if __name__ == "__main__":
    unittest.main()
