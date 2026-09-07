# Cross-binding consistency programs

Each program opens an existing HOCDB tick database (schema: `timestamp i64`,
`price f64`, `size f64`, `bid f64`, `ask f64`, `side bool`), executes the
request described by a JSON file and prints a JSON document with the results,
so that `scripts/stress/overnight_validation.py` can compare every binding
bit-for-bit against the Python binding.

Request (`request.json`):

```json
{
  "ticker": "BTCUSD", "dir": "/abs/path/to/b_stress_test_data",
  "tail": 1000, "bucket": 60000000,
  "specs": [{"kind": "rsi", "period": 14}, {"kind": "macd"}],
  "snapshot": {"bars": 2500, "bucket": 60000000, "periods_per_year": 525600},
  "summary": {"start": 0, "end": 9223372036854775807, "field": "price", "periods_per_year": 525600},
  "ohlcv": {"start": 0, "end": 9223372036854775807, "bucket": 3600000000}
}
```

Response: `{"binding": "...", "timestamps": [...], "columns": {name: [...]},
"snapshot": {...}, "summary": {...}, "ohlcv": {"timestamps": [...], "open":
[...], ...}}`. NaN is encoded as `null`, infinities as the strings `"inf"` /
`"-inf"`, integers as JSON integers, doubles with full round-trip precision.

Usage: `<program> request.json > out.json` (see overnight_validation.py for
the exact build/run commands).
