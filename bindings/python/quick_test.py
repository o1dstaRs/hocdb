"""
Quick performance test for HOCDB Python bindings
"""
import os
import sys
import time
from hocdb_python import HOCDB, HOCDBField, FieldTypes, create_record_bytes

def quick_performance_test():
    """Quick performance test that takes less time"""
    print("Running quick Python performance test...")
    
    # Define schema similar to Zig benchmark
    schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("usd", FieldTypes.F64),
        HOCDBField("volume", FieldTypes.F64)
    ]

    # Create database instance
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    db_path = os.path.join(project_root, "b_python_test_data")
    db = HOCDB("PYTHON_QUICK_BENCH", db_path, schema)

    # Do a smaller write test (instead of 30 seconds, do fixed count)
    num_records = 100000  # Reduced for quick test
    print(f"Writing {num_records} records...")
    
    start_time = time.time()
    for i in range(num_records):
        record = create_record_bytes(schema, i, float(i), float(i))
        success = db.append(record)
        if not success:
            print(f"Failed to append record {i}")
            break
    write_time = time.time() - start_time
    
    ops_per_sec = num_records / write_time
    record_size = 8 + 8 + 8  # 24 bytes
    mb_per_sec = (ops_per_sec * record_size) / (1024 * 1024)
    
    print(f"[WRITE] {num_records} records in {write_time:.2f}s")
    print(f"Throughput: {ops_per_sec:.2f} ops/sec")
    print(f"Bandwidth:  {mb_per_sec:.2f} MB/sec")

    # Load test
    print("Loading data...")
    load_start = time.time()
    data = db.load()
    load_time = time.time() - load_start
    
    if data:
        records_loaded = len(data) // record_size
        load_ops_per_sec = records_loaded / load_time
        load_mb_per_sec = (load_ops_per_sec * record_size) / (1024 * 1024)
        print(f"[READ/LOAD] {records_loaded} records")
        print(f"Time: {load_time:.4f}s")
        print(f"Throughput: {load_ops_per_sec:.2f} ops/sec")
        print(f"Bandwidth:  {load_mb_per_sec:.2f} MB/sec")
    
    db.close()
    
    # The aggregation test would be similar but in Python, which is inherently slower
    print("Quick performance test completed.\n")


def main():
    """Run the quick test"""
    print("Starting quick HOCDB Python bindings performance test...\n")
    
    # Make sure we're in the right directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    try:
        quick_performance_test()
        print("Quick performance test completed successfully!")
        
        # Show comparison with Zig benchmark results
        print("\nPERFORMANCE COMPARISON:")
        print("Zig (native):      ~14,000,000 ops/sec writes")
        print("Python (ctypes):   (results from above test)")
        print("\nNote: Python performance is limited by ctypes overhead,")
        print("but the underlying database performance remains the same.")
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()