"""
Test script for HOCDB Python bindings
"""
import os
import sys
import time
from hocdb_python import HOCDB, HOCDBField, FieldTypes, create_record_bytes

def test_basic_functionality():
    """Test basic database operations"""
    print("Testing basic functionality...")

    import os
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

    db = HOCDB("BTC_USD", db_path, schema)
    
    # Test appending records
    print("Appending records...")
    for i in range(1000):
        timestamp = 1620000000 + i
        price = 50000.0 + (i * 10.5)
        volume = 1.0 + (i * 0.1)
        
        record = create_record_bytes(schema, timestamp, price, volume)
        success = db.append(record)
        if not success:
            print(f"Failed to append record {i}")
            break
    
    print("Flushing database...")
    db.flush()
    
    # Load and verify data
    print("Loading data...")
    data = db.load()
    if data:
        print(f"Successfully loaded {len(data)} bytes of data")
        
        # Calculate expected record size and number of records
        record_size = 8 + 8 + 8  # i64 + f64 + f64 = 24 bytes
        expected_records = len(data) // record_size
        print(f"Expected ~{1000} records, got {expected_records} records")
    else:
        print("Failed to load data")
    
    # Close the database
    db.close()
    print("Basic functionality test completed.\n")


def time_based_write_benchmark():
    """Time-based write benchmark similar to Zig benchmark"""
    print("Running Python Write Benchmark...")

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
    db = HOCDB("PYTHON_BENCH", db_path, schema)

    # Benchmark for 30 seconds like in Zig
    duration_seconds = 30
    start_time = time.time()

    print(f"Duration: {duration_seconds} seconds")
    print("Target: As many writes as possible")

    record = create_record_bytes(schema, 0, 0.0, 0.0)  # Template record
    total_records = 0

    while time.time() - start_time < duration_seconds:
        # Create record with current values
        current_record = create_record_bytes(schema, total_records, float(total_records), float(total_records))
        success = db.append(current_record)
        if not success:
            print(f"Failed to append record {total_records}")
            break
        total_records += 1

        # Progress update every 10000 records
        if total_records % 10000 == 0:
            elapsed = time.time() - start_time
            percent = (elapsed / duration_seconds) * 100
            print(f"\rProgress: {percent:.1f}% ({total_records} records)", end='', flush=True)

    print()  # New line after progress
    elapsed_time = time.time() - start_time

    # Calculate performance metrics similar to Zig
    record_size = 8 + 8 + 8  # i64 + f64 + f64 = 24 bytes
    ops_per_sec = total_records / elapsed_time
    mb_per_sec = (ops_per_sec * record_size) / (1024 * 1024)

    print(f"\n[WRITE] {total_records} records in {elapsed_time:.2f}s")
    print(f"Throughput: {ops_per_sec:.2f} ops/sec")
    print(f"Bandwidth:  {mb_per_sec:.2f} MB/sec")

    db.close()
    print("Write benchmark completed.\n")

    return total_records, elapsed_time


def load_aggregate_benchmark(total_records_estimate):
    """Load and aggregate benchmark similar to Zig benchmark"""
    print("Running Python Load & Aggregate Benchmark...")

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
    db = HOCDB("PYTHON_BENCH", db_path, schema)

    # Load benchmark
    start_time = time.time()
    data = db.load()
    load_time = time.time() - start_time

    if data:
        record_size = 8 + 8 + 8  # 24 bytes
        records_loaded = len(data) // record_size
        print(f"\n[READ/LOAD] {records_loaded} records")
        print(f"Time: {load_time:.4f}s")
        load_ops_per_sec = records_loaded / load_time
        load_mb_per_sec = (load_ops_per_sec * record_size) / (1024 * 1024)
        print(f"Throughput: {load_ops_per_sec:.2f} ops/sec")
        print(f"Bandwidth:  {load_mb_per_sec:.2f} MB/sec")

        # Aggregation benchmark
        aggregation_start = time.time()

        frame_count = 0
        i = 0
        frame_size = 1000

        # Prevent compiler optimization
        total_volume_checksum = 0.0

        # Convert raw bytes to records for aggregation
        record_size = 24  # bytes per record
        records = []

        # Parse raw data into records
        for idx in range(0, len(data), record_size):
            chunk = data[idx:idx+record_size]
            if len(chunk) == record_size:
                # Unpack the record: i64 timestamp, f64 usd, f64 volume
                import struct
                timestamp, usd, volume = struct.unpack('<qdd', chunk)
                records.append((timestamp, usd, volume))

        # Perform aggregation on parsed records
        while i + frame_size <= len(records):
            frame = records[i:i + frame_size]
            usd_sum = 0.0
            vol_sum = 0.0

            for _, usd, volume in frame:
                usd_sum += usd
                vol_sum += volume

            usd_mean = usd_sum / frame_size
            total_volume_checksum += vol_sum + usd_mean  # Use values
            frame_count += 1
            i += frame_size

        aggregation_time = time.time() - aggregation_start
        frames_per_sec = frame_count / aggregation_time
        records_per_sec = (frame_count * frame_size) / aggregation_time

        print(f"\n[AGGREGATION] {frame_count} frames (1000 records each)")
        print(f"Time: {aggregation_time:.6f}s")
        print(f"Throughput: {frames_per_sec:.2f} frames/sec")
        print(f"Processing: {records_per_sec:.2f} records/sec")
        print(f"Checksum:   {total_volume_checksum:.2f}")

    db.close()
    print("Load & Aggregate benchmark completed.\n")


def benchmark_performance():
    """Run performance benchmarks similar to Zig"""
    print("Running Python Performance Benchmarks (similar to Zig)...")
    print("Record Size: 24 bytes")  # i64 + f64 + f64

    # Run write benchmark
    total_records, elapsed_time = time_based_write_benchmark()

    # Run load and aggregate benchmark
    load_aggregate_benchmark(total_records)


def test_schema_validation():
    """Test schema validation and error handling"""
    print("Testing schema validation...")

    # Define schema
    schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("value", FieldTypes.F64)
    ]

    # Create database instance
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    db_path = os.path.join(project_root, "b_python_test_data")
    db = HOCDB("SCHEMA_TEST", db_path, schema)
    
    # Valid record
    valid_record = create_record_bytes(schema, 1620000000, 100.5)
    success = db.append(valid_record)
    print(f"Valid record append: {'Success' if success else 'Failed'}")
    
    # Try to append invalid record (wrong number of fields)
    try:
        invalid_record = create_record_bytes([HOCDBField("only", FieldTypes.I64)], 123)
        success = db.append(invalid_record)
        print(f"Invalid record append: {'Unexpectedly Succeeded' if success else 'Failed as expected'}")
    except ValueError as e:
        print(f"Invalid record caught error as expected: {e}")
    
    db.close()
    print("Schema validation test completed.\n")


def main():
    """Run all tests"""
    print("Starting HOCDB Python bindings tests...\n")

    # Make sure we're in the right directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    try:
        test_basic_functionality()
        benchmark_performance()  # Run the new performance benchmark
        test_schema_validation()
        print("All tests completed successfully!")
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()