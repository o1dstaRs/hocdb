import os
import shutil
import struct
from hocdb_python import HOCDB, HOCDBField, FieldTypes

def test_auto_increment():
    ticker = "TEST_AUTO_INC_PY"
    path = "test_auto_inc_py_data"
    
    if os.path.exists(path):
        shutil.rmtree(path)
        
    schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("value", FieldTypes.F64)
    ]
    
    # Initialize with auto_increment=True
    db = HOCDB(ticker, path, schema, auto_increment=True)
    
    # Append records with dummy timestamp
    for i in range(10):
        # Create record bytes manually
        # timestamp (8 bytes) + value (8 bytes)
        # We put 0 for timestamp
        record = struct.pack('<qd', 0, float(i))
        db.append(record)
        
    # Load and verify
    data = db.load()
    assert len(data) == 10 * 16
    
    for i in range(10):
        offset = i * 16
        ts, val = struct.unpack_from('<qd', data, offset)
        assert ts == i + 1
        assert val == float(i)
        
    db.close()
    
    # Reopen and append more
    db = HOCDB(ticker, path, schema, auto_increment=True)
    for i in range(10, 15):
        record = struct.pack('<qd', 999, float(i))
        db.append(record)
        
    data = db.load()
    assert len(data) == 15 * 16
    
    for i in range(15):
        offset = i * 16
        ts, val = struct.unpack_from('<qd', data, offset)
        assert ts == i + 1
        assert val == float(i)
        
    db.close()
    shutil.rmtree(path)
    print("Python Auto-Increment Test Passed!")

if __name__ == "__main__":
    test_auto_increment()
