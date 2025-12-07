import os
import shutil
from hocdb_python import HOCDB, HOCDBField, FieldTypes

TICKER = "TEST_QUERY_PYTHON"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "b_python_test_data")

# Cleanup
if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)

# Define Schema
schema = [
    HOCDBField("timestamp", FieldTypes.I64),
    HOCDBField("value", FieldTypes.F64)
]

print("Initializing DB...")
db = HOCDB(TICKER, DATA_DIR, schema, max_file_size=1024*1024, overwrite_on_full=True)

print("Appending data...")
# Append 100, 200, 300, 400, 500
db.append(100, 1.0)
db.append(200, 2.0)
db.append(300, 3.0)
db.append(400, 4.0)
db.append(500, 5.0)

print("Querying range 200 to 450...")
records = db.query(200, 450)

if records is None:
    raise RuntimeError("Query returned None")

count = len(records)
print(f"Query result count: {count}")

if count != 3:
    raise RuntimeError(f"Expected 3 records, got {count}")

# Verify content
expected_ts = [200, 300, 400]
expected_val = [2.0, 3.0, 4.0]

for i, record in enumerate(records):
    ts = record['timestamp']
    val = record['value']
    print(f"Record {i}: ts={ts}, val={val}")
    
    if ts != expected_ts[i]:
        raise RuntimeError(f"Expected ts {expected_ts[i]}, got {ts}")
    if val != expected_val[i]:
        raise RuntimeError(f"Expected val {expected_val[i]}, got {val}")

print("Querying with filter (value=3.0)...")
filtered_data = db.query(0, 1000, {'value': 3.0})

if filtered_data is None:
    raise RuntimeError("Filtered query returned None")

f_count = len(filtered_data)
print(f"Filtered result count: {f_count}")

if f_count != 1:
    raise RuntimeError(f"Expected 1 record, got {f_count}")

record = filtered_data[0]
ts = record['timestamp']
val = record['value']
print(f"Filtered Record: ts={ts}, val={val}")

if ts != 300 or val != 3.0:
    raise RuntimeError(f"Expected ts=300, val=3.0, got ts={ts}, val={val}")

print("✅ Python Query Test Passed!")

db.close()
if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)
