#!/usr/bin/env python3
"""
Debug script for HOCDB Python bindings
"""
import os
import sys
from hocdb_python import HOCDB, HOCDBField, FieldTypes, create_record_bytes

def debug_test():
    """Simple debug test"""
    print("=== HOCDB Python Bindings Debug Test ===")
    
    # Define schema
    schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("price", FieldTypes.F64),
        HOCDBField("volume", FieldTypes.F64)
    ]

    # Create database instance in test data directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    db_path = os.path.join(project_root, "b_python_test_data")
    os.makedirs(db_path, exist_ok=True)

    print(f"Database path: {db_path}")
    
    try:
        db = HOCDB("DEBUG_TEST", db_path, schema)
        print("✓ Database initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize database: {e}")
        import traceback
        traceback.print_exc()
        return

    # Try to create and append a record
    try:
        record = create_record_bytes(schema, 1620000000, 50000.0, 1.5)
        print(f"✓ Created record bytes: {len(record)} bytes")
        
        success = db.append(record)
        print(f"✓ Append result: {success} (should be True for success)")
        
        # Try second record
        record2 = create_record_bytes(schema, 1620000001, 50001.0, 1.6)
        success2 = db.append(record2)
        print(f"✓ Second append result: {success2}")
        
    except Exception as e:
        print(f"✗ Failed during append operation: {e}")
        import traceback
        traceback.print_exc()
        db.close()
        return

    # Flush and load
    try:
        db.flush()
        print("✓ Flushed successfully")
        
        data = db.load()
        if data:
            print(f"✓ Loaded {len(data)} bytes of data")
            record_size = 8 + 8 + 8  # i64 + f64 + f64
            records_loaded = len(data) // record_size
            print(f"✓ Calculated {records_loaded} records")
        else:
            print("✗ Failed to load data")
    except Exception as e:
        print(f"✗ Failed during load operation: {e}")
        import traceback
        traceback.print_exc()
        db.close()
        return

    db.close()
    print("✓ Debug test completed successfully")
    

if __name__ == "__main__":
    debug_test()