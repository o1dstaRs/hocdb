from hocdb_python import HOCDB, HOCDBField, FieldTypes
import os
import shutil

TEST_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "b_python_test_data")
TICKER = "TEST_PYTHON_AGG"

if os.path.exists(TEST_DIR):
    shutil.rmtree(TEST_DIR)

schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("value", FieldTypes.F64)
]

db = HOCDB(TICKER, TEST_DIR, schema, flush_on_write=True)

print("Appending data...")
print("Appending data...")
db.append(100, 10.0)
db.append(200, 20.0)
db.append(300, 30.0)

print("Testing get_latest...")
latest = db.get_latest(1)
print("Latest:", latest)
if latest['value'] != 30.0 or latest['timestamp'] != 300:
    raise RuntimeError(f"get_latest failed: {latest}")

print("Testing get_stats...")
stats = db.get_stats(0, 400, 1)
print("Stats:", stats)

if stats['count'] != 3: raise RuntimeError(f"Count mismatch: {stats['count']}")
if stats['min'] != 10.0: raise RuntimeError(f"Min mismatch: {stats['min']}")
if stats['max'] != 30.0: raise RuntimeError(f"Max mismatch: {stats['max']}")
if stats['sum'] != 60.0: raise RuntimeError(f"Sum mismatch: {stats['sum']}")
if stats['mean'] != 20.0: raise RuntimeError(f"Mean mismatch: {stats['mean']}")

db.close()
print("Python Aggregation Test Passed!")
