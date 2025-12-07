import os
import shutil
import sys
from hocdb_python import HOCDB, HOCDBField, FieldTypes

TICKER = "TEST_PY_FILTER"
DATA_DIR = "b_python_test_filter_syntax"

def cleanup():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

def run_test():
    cleanup()
    try:
        schema = [
            HOCDBField("timestamp", FieldTypes.I64),
            HOCDBField("price", FieldTypes.F64),
            HOCDBField("event", FieldTypes.I64)
        ]

        print("Initializing DB...")
        db = HOCDB(TICKER, DATA_DIR, schema)

        print("Appending data...")
        # 1. event = 0
        db.append(struct.pack('<qdq', 100, 1.0, 0))
        # 2. event = 1
        db.append(struct.pack('<qdq', 200, 2.0, 1))
        # 3. event = 2
        db.append(struct.pack('<qdq', 300, 3.0, 2))

        # Query with new syntax: { "event": 1 }
        print("Querying with filter { 'event': 1 }...")
        
        # Test single dict
        data = db.query(0, 1000, {"event": 1})
        if not data:
            raise RuntimeError("Query returned no data")
            
        record_size = 8 + 8 + 8
        count = len(data) // record_size
        print(f"Results count: {count}")
        
        if count != 1:
            raise RuntimeError(f"Expected 1 result, got {count}")
            
        ts, price, event = struct.unpack('<qdq', data)
        print(f"Result: TS={ts}, Event={event}")
        
        if event != 1:
            raise RuntimeError(f"Expected event 1, got {event}")
            
        # Test list of dicts
        print("Querying with filter list [{ 'event': 2 }]...")
        data = db.query(0, 1000, [{"event": 2}])
        if not data:
             raise RuntimeError("Query returned no data")
        
        ts, price, event = struct.unpack('<qdq', data)
        if event != 2:
             raise RuntimeError(f"Expected event 2, got {event}")

        print("✅ Python Filter Syntax Test Passed!")
        
    except Exception as e:
        print(f"Test Failed: {e}")
        sys.exit(1)
    finally:
        if 'db' in locals():
            db.close()
        cleanup()

import struct
if __name__ == "__main__":
    run_test()
