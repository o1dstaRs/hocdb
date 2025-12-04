import os
import shutil
from hocdb_python import HOCDB, HOCDBField, FieldTypes, create_record_bytes

TICKER = "TEST_QUERY_PYTHON"
DATA_DIR = "test_query_python_data"

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
db.append(create_record_bytes(schema, 100, 1.0))
db.append(create_record_bytes(schema, 200, 2.0))
db.append(create_record_bytes(schema, 300, 3.0))
db.append(create_record_bytes(schema, 400, 4.0))
db.append(create_record_bytes(schema, 500, 5.0))

print("Querying range 200 to 450...")
data = db.query(200, 450)

if data is None:
    raise RuntimeError("Query returned None")

record_size = 16 # 8 + 8
count = len(data) // record_size
print(f"Query result count: {count}")

if count != 3:
    raise RuntimeError(f"Expected 3 records, got {count}")

# Verify content (manual parsing for test)
import struct
for i in range(count):
    offset = i * record_size
    ts = struct.unpack('<q', data[offset:offset+8])[0]
    val = struct.unpack('<d', data[offset+8:offset+16])[0]
    print(f"Record {i}: ts={ts}, val={val}")
    
    expected_ts = [200, 300, 400][i]
    if ts != expected_ts:
        raise RuntimeError(f"Expected ts {expected_ts}, got {ts}")

print("✅ Python Query Test Passed!")

db.close()
if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)
