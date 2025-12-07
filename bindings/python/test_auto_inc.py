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
        # Pass values directly - timestamp (ignored due to auto_inc) + value
        db.append(0, float(i))
        
    # Load and verify
    data = db.load()
    assert len(data) == 10
    
    for i, record in enumerate(data):
        assert record['timestamp'] == i + 1
        assert record['value'] == float(i)
        
    db.close()
    
    # Reopen and append more
    db = HOCDB(ticker, path, schema, auto_increment=True)
    for i in range(10, 15):
        db.append(999, float(i))
        
    data = db.load()
    assert len(data) == 15
    
    for i, record in enumerate(data):
        assert record['timestamp'] == i + 1
        assert record['value'] == float(i)
        
    db.close()
    shutil.rmtree(path)
    print("Python Auto-Increment Test Passed!")

if __name__ == "__main__":
    test_auto_increment()
