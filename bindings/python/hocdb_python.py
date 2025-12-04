"""
Python bindings for HOCDB - High-Performance Time Series Database
"""
import ctypes
import ctypes.util
import os
from typing import Union, Optional, Any
import sys
import struct


class FieldTypes:
    """Field type constants matching the C API"""
    I64 = 1
    F64 = 2
    U64 = 3


class HOCDBField:
    """Represents a field in the database schema"""
    def __init__(self, name: str, field_type: int):
        self.name = name
        self.type = field_type


class HOCDB:
    """Python wrapper for HOCDB C API"""
    
    def __init__(self, ticker: str, path: str, schema: list, max_file_size: Optional[int] = None, 
                 overwrite_on_full: bool = False, flush_on_write: bool = False):
        """
        Initialize the database with dynamic schema
        
        Args:
            ticker: Ticker symbol
            path: Directory path for data
            schema: List of HOCDBField objects defining the schema
            max_file_size: Maximum file size (0 for default)
            overwrite_on_full: Whether to overwrite when full
            flush_on_write: Whether to flush on every write
        """
        # Load the C library - first try to find it in zig-out/lib
        lib_path = self._find_library()
        if not lib_path:
            raise RuntimeError("HOCDB C library not found. Please build with 'zig build c-bindings'")
        
        self.lib = ctypes.CDLL(lib_path)
        
        # Define function signatures
        self._define_function_signatures()
        
        # Prepare schema for C API
        self._schema = schema
        c_schema_fields = []
        for field in schema:
            c_field = CField()
            c_field.name = field.name.encode('utf-8')
            c_field.type = field.type
            c_schema_fields.append(c_field)
        
        # Create array of CField structs
        CFieldArray = CField * len(c_schema_fields)
        c_schema_array = CFieldArray(*c_schema_fields)
        
        # Convert Python values to C types
        ticker_bytes = ticker.encode('utf-8')
        path_bytes = path.encode('utf-8')
        
        max_size = max_file_size if max_file_size is not None else 0
        overwrite_val = 1 if overwrite_on_full else 0
        flush_val = 1 if flush_on_write else 0
        
        # Call C API
        self.handle = self.lib.hocdb_init(
            ticker_bytes,
            path_bytes,
            c_schema_array,
            len(c_schema_fields),
            max_size,
            overwrite_val,
            flush_val
        )
        
        if not self.handle:
            raise RuntimeError("Failed to initialize HOCDB")
    
    def _find_library(self) -> Optional[str]:
        """Find the HOCDB C library"""
        # Try common locations
        possible_paths = [
            './zig-out/lib/libhocdb_c.dylib',  # Current directory
            './libhocdb_c.dylib',
            './zig-out/lib/libhocdb_c.so',    # Linux
            './libhocdb_c.so',
            os.path.join(os.path.dirname(__file__), '../..', 'zig-out/lib/libhocdb_c.dylib'),
            os.path.join(os.path.dirname(__file__), '../..', 'zig-out/lib/libhocdb_c.so'),
        ]
        
        for path in possible_paths:
            abs_path = os.path.abspath(path)
            if os.path.exists(abs_path):
                return abs_path
        
        # Try to find it using ctypes.util
        lib_name = ctypes.util.find_library('hocdb_c')
        if lib_name:
            return lib_name
            
        return None
    
    def _define_function_signatures(self):
        """Define C function signatures"""
        # hocdb_init function
        self.lib.hocdb_init.argtypes = [
            ctypes.c_char_p,          # ticker
            ctypes.c_char_p,          # path
            ctypes.POINTER(CField),   # schema
            ctypes.c_size_t,          # schema_len
            ctypes.c_longlong,        # max_file_size
            ctypes.c_int,             # overwrite_on_full
            ctypes.c_int              # flush_on_write
        ]
        self.lib.hocdb_init.restype = ctypes.c_void_p
        
        # hocdb_append function
        self.lib.hocdb_append.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.c_void_p,          # data
            ctypes.c_size_t           # len
        ]
        self.lib.hocdb_append.restype = ctypes.c_int
        
        # hocdb_flush function
        self.lib.hocdb_flush.argtypes = [ctypes.c_void_p]
        self.lib.hocdb_flush.restype = ctypes.c_int
        
        # hocdb_load function
        self.lib.hocdb_load.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.POINTER(ctypes.c_size_t)  # out_len
        ]
        self.lib.hocdb_load.restype = ctypes.c_void_p
        
        # hocdb_free function
        self.lib.hocdb_free.argtypes = [ctypes.c_void_p]
        self.lib.hocdb_free.restype = None
        
        # hocdb_close function
        self.lib.hocdb_close.argtypes = [ctypes.c_void_p]
        self.lib.hocdb_close.restype = None

        # hocdb_query function
        self.lib.hocdb_query.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.c_longlong,        # start_ts
            ctypes.c_longlong,        # end_ts
            ctypes.POINTER(ctypes.c_size_t)  # out_len
        ]
        self.lib.hocdb_query.restype = ctypes.c_void_p
    
    def append(self, record_data: bytes) -> bool:
        """
        Append a raw record to the database
        
        Args:
            record_data: Raw bytes of the record matching the schema
            
        Returns:
            True if successful, False otherwise
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        result = self.lib.hocdb_append(
            self.handle,
            record_data,
            len(record_data)
        )
        return result == 0
    
    def flush(self) -> bool:
        """Flush the database (force write to disk)"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        result = self.lib.hocdb_flush(self.handle)
        return result == 0
    
    def load(self) -> Optional[bytes]:
        """
        Load all records into memory with zero-copy
        
        Returns:
            Raw bytes of all records, or None on failure
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        out_len = ctypes.c_size_t()
        data_ptr = self.lib.hocdb_load(self.handle, ctypes.byref(out_len))
        
        if not data_ptr:
            return None
        
        try:
            # Copy data from C memory to Python bytes
            data = ctypes.string_at(data_ptr, out_len.value)
            return data
        finally:
            # Free the C-allocated memory
            self.lib.hocdb_free(data_ptr)

    def query(self, start_ts: int, end_ts: int) -> Optional[bytes]:
        """
        Query records in a time range
        
        Args:
            start_ts: Start timestamp (inclusive)
            end_ts: End timestamp (exclusive)
            
        Returns:
            Raw bytes of records in range, or None on failure
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        out_len = ctypes.c_size_t()
        data_ptr = self.lib.hocdb_query(self.handle, start_ts, end_ts, ctypes.byref(out_len))
        
        if not data_ptr:
            # If length is 0, it might be just empty result, but query returns null on error?
            # Zig query returns slice. If empty, len=0, ptr might be non-null (dangling) or null?
            # Zig C export returns null on error.
            # If empty result, it returns pointer to empty slice.
            # Let's assume null means error.
            # Wait, if empty, it might return null?
            # Zig: `if (data.len == 0) return null;` ? No.
            # Zig `query` returns `[]u8`.
            # If I catch error, I return null.
            return None
        
        try:
            # Copy data from C memory to Python bytes
            data = ctypes.string_at(data_ptr, out_len.value)
            return data
        finally:
            # Free the C-allocated memory
            self.lib.hocdb_free(data_ptr)
    
    def close(self):
        """Close and free the database handle"""
        if self.handle:
            self.lib.hocdb_close(self.handle)
            self.handle = None


# Define the CField struct for the C API
class CField(ctypes.Structure):
    """C-compatible field definition"""
    _fields_ = [
        ("name", ctypes.c_char_p),
        ("type", ctypes.c_int),
    ]


def create_record_bytes(schema: list, *values) -> bytes:
    """
    Create raw bytes for a record based on the schema and values
    
    Args:
        schema: List of HOCDBField objects
        *values: Values for each field in order
        
    Returns:
        Raw bytes representation of the record
    """
    if len(values) != len(schema):
        raise ValueError(f"Number of values ({len(values)}) doesn't match schema length ({len(schema)})")
    
    record_bytes = b""
    
    for field, value in zip(schema, values):
        if field.type == FieldTypes.I64:
            # Convert to int64 and pack as little-endian
            record_bytes += struct.pack('<q', int(value))
        elif field.type == FieldTypes.F64:
            # Pack as double (f64) little-endian
            record_bytes += struct.pack('<d', float(value))
        elif field.type == FieldTypes.U64:
            # Pack as uint64 little-endian
            record_bytes += struct.pack('<Q', int(value))
        else:
            raise ValueError(f"Unsupported field type: {field.type}")
    
    return record_bytes


# Example usage
if __name__ == "__main__":
    # Define schema
    schema = [
        HOCDBField("timestamp", FieldTypes.I64),
        HOCDBField("price", FieldTypes.F64),
        HOCDBField("volume", FieldTypes.F64)
    ]
    
    # Create database instance
    db = HOCDB("BTC_USD", "python_test_data", schema)
    
    # Create and append some records
    record1 = create_record_bytes(schema, 1620000000, 50000.0, 1.5)
    record2 = create_record_bytes(schema, 1620000001, 50001.0, 1.6)
    
    db.append(record1)
    db.append(record2)
    
    # Load and print data
    data = db.load()
    if data:
        print(f"Loaded {len(data)} bytes of data")
    
    # Close the database
    db.close()