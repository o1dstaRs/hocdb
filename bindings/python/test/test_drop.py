"""
Test for drop() method - closes database and deletes data files
"""
import os
import sys
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hocdb_python import HOCDB, HOCDBField, FieldTypes


def test_drop():
    """Test that drop() closes the database and deletes data files"""
    test_dir = "../../../b_python_test_drop"
    ticker = "DROP_TEST"

    # Clean up from any previous run
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    # Define schema
    schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("price", FieldTypes.F64),
        HOCDBField("volume", FieldTypes.F64)
    ]

    # Create database and add data
    db = HOCDB(ticker, test_dir, schema)

    db.append({"timestamp": 1620000000, "price": 50000.0, "volume": 1.5})
    db.append({"timestamp": 1620000001, "price": 50001.0, "volume": 1.6})
    db.flush()

    # Verify data directory exists and has files
    assert os.path.exists(test_dir), "Data directory should exist after writes"

    # Get list of files before drop
    files_before = []
    for root, dirs, files in os.walk(test_dir):
        for f in files:
            files_before.append(os.path.join(root, f))

    assert len(files_before) > 0, "Data files should exist before drop"
    print(f"Files before drop: {files_before}")

    # Drop the database
    db.drop()

    # Verify handle is None
    assert db.handle is None, "Handle should be None after drop"

    # Verify data files are deleted
    files_after = []
    if os.path.exists(test_dir):
        for root, dirs, files in os.walk(test_dir):
            for f in files:
                files_after.append(os.path.join(root, f))

    print(f"Files after drop: {files_after}")

    # The data files should be deleted (directory might still exist but be empty or removed)
    # Check that at least the main data file is gone
    data_file_exists = any(ticker in f for f in files_after)
    assert not data_file_exists, f"Data files for {ticker} should be deleted after drop"

    print("PASS: drop() test successful")

    # Clean up
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


if __name__ == "__main__":
    test_drop()
