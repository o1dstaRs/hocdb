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
    BOOL = 6


class HOCDBField:
    """Represents a field in the database schema"""
    def __init__(self, name: str, field_type: int):
        self.name = name
        self.type = field_type


class HOCDB:
    """Python wrapper for HOCDB C API"""
    
    def __init__(self, ticker: str, path: str, schema: list, max_file_size: Optional[int] = None, 
                 overwrite_on_full: bool = False, flush_on_write: bool = False, auto_increment: bool = False):
        """
        Initialize the database with dynamic schema
        
        Args:
            ticker: Ticker symbol
            path: Directory path for data
            schema: List of HOCDBField objects defining the schema
            max_file_size: Maximum file size (0 for default)
            overwrite_on_full: Whether to overwrite when full
            overwrite_on_full: Whether to overwrite when full
            flush_on_write: Whether to flush on every write
            auto_increment: Whether to auto-increment timestamp
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
        self._field_map = {field.name: (i, field.type) for i, field in enumerate(schema)}
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
        auto_inc_val = 1 if auto_increment else 0
        
        # Call C API
        self.handle = self.lib.hocdb_init(
            ticker_bytes,
            path_bytes,
            c_schema_array,
            len(c_schema_fields),
            max_size,
            overwrite_val,
            flush_val,
            auto_inc_val
        )
        
        if not self.handle:
            raise RuntimeError("Failed to initialize HOCDB")

        # Build struct format string for packing/unpacking
        self._struct_fmt = self._build_struct_format()
        self._record_size = struct.calcsize(self._struct_fmt)

    def _build_struct_format(self) -> str:
        """Build the struct format string from the schema"""
        fmt = "<"  # Little-endian
        for field in self._schema:
            if field.type == FieldTypes.I64:
                fmt += "q"
            elif field.type == FieldTypes.F64:
                fmt += "d"
            elif field.type == FieldTypes.U64:
                fmt += "Q"
            elif field.type == FieldTypes.BOOL:
                fmt += "?"
            else:
                raise ValueError(f"Unsupported field type: {field.type}")
        return fmt

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
            '../zig-out/lib/libhocdb_c.dylib', # From bindings/python
            '../libhocdb_c.dylib',
            '../zig-out/lib/libhocdb_c.so',
            '../libhocdb_c.so',
        ]
        
        # Also check environment variable
        env_path = os.environ.get('HOCDB_LIB_PATH')
        if env_path:
            possible_paths.insert(0, env_path)
            
        for path in possible_paths:
            if os.path.exists(path):
                return os.path.abspath(path)
                
        # Fallback to system search
        return ctypes.util.find_library('hocdb_c')
    
    def _define_function_signatures(self):
        """Define argument and return types for C functions"""
        # CField struct definition
        global CField
        class CField(ctypes.Structure):
            _fields_ = [
                ("name", ctypes.c_char_p),
                ("type", ctypes.c_int)
            ]
        
        # hocdb_init function
        self.lib.hocdb_init.argtypes = [
            ctypes.c_char_p,          # ticker
            ctypes.c_char_p,          # path
            ctypes.POINTER(CField),   # schema
            ctypes.c_size_t,          # schema_len
            ctypes.c_size_t,          # max_file_size
            ctypes.c_int,             # overwrite_on_full
            ctypes.c_int,             # flush_on_write
            ctypes.c_int              # auto_increment
        ]
        self.lib.hocdb_init.restype = ctypes.c_void_p
        
        # hocdb_append function
        self.lib.hocdb_append.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.c_char_p,          # record_bytes
            ctypes.c_size_t           # record_len
        ]
        self.lib.hocdb_append.restype = ctypes.c_int
        
        # hocdb_flush function
        self.lib.hocdb_flush.argtypes = [ctypes.c_void_p]
        self.lib.hocdb_flush.restype = ctypes.c_int
        
        # hocdb_load function
        self.lib.hocdb_load.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.POINTER(ctypes.c_size_t) # out_len
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
            ctypes.POINTER(HOCDBFilter), # filters
            ctypes.c_size_t,          # filters_len
            ctypes.POINTER(ctypes.c_size_t)  # out_len
        ]
        self.lib.hocdb_query.restype = ctypes.c_void_p

        # hocdb_get_stats function
        self.lib.hocdb_get_stats.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.c_longlong,        # start_ts
            ctypes.c_longlong,        # end_ts
            ctypes.c_size_t,          # field_index
            ctypes.c_void_p           # out_stats
        ]
        self.lib.hocdb_get_stats.restype = ctypes.c_int

        # hocdb_get_latest function
        self.lib.hocdb_get_latest.argtypes = [
            ctypes.c_void_p,          # handle
            ctypes.c_size_t,          # field_index
            ctypes.POINTER(ctypes.c_double), # out_val
            ctypes.POINTER(ctypes.c_longlong) # out_ts
        ]
        self.lib.hocdb_get_latest.restype = ctypes.c_int
    
    def append(self, *args) -> bool:
        """
        Append a record to the database.
        
        Args:
            *args: Values corresponding to the schema fields.
                   Can be passed as separate arguments or as a single dictionary/tuple/list.
            
        Returns:
            True if successful, False otherwise
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        values = args
        if len(args) == 1:
            if isinstance(args[0], (list, tuple)):
                values = args[0]
            elif isinstance(args[0], dict):
                # Extract values in schema order
                values = []
                for field in self._schema:
                    if field.name in args[0]:
                        values.append(args[0][field.name])
                    else:
                        raise ValueError(f"Missing field in dictionary: {field.name}")
                values = tuple(values)
            elif hasattr(args[0], '__dict__'): # Object
                 values = []
                 for field in self._schema:
                    if hasattr(args[0], field.name):
                        values.append(getattr(args[0], field.name))
                    else:
                         raise ValueError(f"Missing attribute in object: {field.name}")
                 values = tuple(values)

        if len(values) != len(self._schema):
             # Special case: if auto_increment is on, we might skip the first field (timestamp)
             # But the C API expects the full record structure including the timestamp placeholder.
             # The user might pass N-1 arguments.
             # However, for simplicity and consistency with C API which expects full record,
             # let's assume user must pass a placeholder for timestamp if auto_increment is on,
             # OR we handle it here.
             # The C `hocdb_append` takes raw bytes. If auto_inc is on, it overwrites the timestamp.
             # So we must provide *some* bytes for it.
             # If user provided N-1 args and auto_inc is on, we can prepend 0.
             # But checking `auto_increment` flag stored in python class is needed.
             # I didn't store `auto_increment` in `__init__`. I should have.
             # For now, strict length check.
             raise ValueError(f"Expected {len(self._schema)} arguments, got {len(values)}")

        try:
            record_data = struct.pack(self._struct_fmt, *values)
        except struct.error as e:
            raise ValueError(f"Failed to pack record: {e}")
        
        result = self.lib.hocdb_append(
            self.handle,
            record_data,
            len(record_data)
        )
        if result == -2:
            raise ValueError("Append failed: Invalid Record Size")
        if result == -3:
            raise ValueError("Append failed: Timestamp Not Monotonic - timestamps must be strictly increasing")
        return result == 0
    
    def flush(self) -> bool:
        """Flush the database (force write to disk)"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        result = self.lib.hocdb_flush(self.handle)
        return result == 0
    
    def load(self) -> list[dict]:
        """
        Load all records into memory and unpack them.
        
        Returns:
            List of dictionaries representing the records.
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        out_len = ctypes.c_size_t()
        data_ptr = self.lib.hocdb_load(self.handle, ctypes.byref(out_len))
        
        if not data_ptr:
            return []
        
        try:
            # Copy data from C memory to Python bytes
            data = ctypes.string_at(data_ptr, out_len.value)
            return self._unpack_records(data)
        finally:
            # Free the C-allocated memory
            self.lib.hocdb_free(data_ptr)

    def _unpack_records(self, data: bytes) -> list[dict]:
        """Unpack raw bytes into a list of dictionaries"""
        records = []
        for i in range(0, len(data), self._record_size):
            chunk = data[i:i+self._record_size]
            if len(chunk) < self._record_size:
                break
            values = struct.unpack(self._struct_fmt, chunk)
            record = {}
            for j, field in enumerate(self._schema):
                record[field.name] = values[j]
            records.append(record)
        return records

    def query(self, start_ts: int, end_ts: int, filters: Optional[list] = None) -> list[dict]:
        """
        Query records in a time range with optional filters
        
        Args:
            start_ts: Start timestamp (inclusive)
            end_ts: End timestamp (inclusive)
            filters: Optional list of dicts with 'field_index' and 'value'
            
        Returns:
            List of dictionaries representing the matching records
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        c_filters = None
        filters_len = 0
        
        if filters:
            # Normalize to list if single dict passed
            if isinstance(filters, dict):
                filters = [filters]
            
            filters_len = len(filters)
            c_filters_array = HOCDBFilter * filters_len
            c_filters = c_filters_array()
            
            for i, f in enumerate(filters):
                # Handle convenient syntax { "field": value }
                if len(f) == 1 and 'field_index' not in f:
                    key = next(iter(f))
                    val = f[key]
                    if key not in self._field_map:
                        raise ValueError(f"Unknown field in filter: {key}")
                    
                    idx, f_type = self._field_map[key]
                    c_filters[i].field_index = idx
                    
                    if isinstance(val, int):
                        c_filters[i].type = 1 # I64
                        c_filters[i].val_i64 = val
                    elif isinstance(val, float):
                        c_filters[i].type = 2 # F64
                        c_filters[i].val_f64 = val
                    elif isinstance(val, str):
                        c_filters[i].type = 5 # String
                        c_filters[i].val_string = val.encode('utf-8')
                    elif isinstance(val, bool):
                        c_filters[i].type = 6 # Bool
                        c_filters[i].val_bool = val
                    else:
                         raise ValueError(f"Unsupported value type for filter: {type(val)}")
                else:
                    # Legacy syntax
                    c_filters[i].field_index = f['field_index']
                    val = f['value']
                    if isinstance(val, int):
                        c_filters[i].type = 1 # I64
                        c_filters[i].val_i64 = val
                    elif isinstance(val, float):
                        c_filters[i].type = 2 # F64
                        c_filters[i].val_f64 = val
                    elif isinstance(val, str):
                        c_filters[i].type = 5 # String
                        c_filters[i].val_string = val.encode('utf-8')
                    elif isinstance(val, bool):
                        c_filters[i].type = 6 # Bool
                        c_filters[i].val_bool = val
        
        out_len = ctypes.c_size_t()
        data_ptr = self.lib.hocdb_query(
            self.handle,
            start_ts,
            end_ts,
            c_filters,
            filters_len,
            ctypes.byref(out_len)
        )
        
        if not data_ptr:
            return []
        
        try:
            # Copy data from C memory to Python bytes
            data = ctypes.string_at(data_ptr, out_len.value)
            return self._unpack_records(data)
            return data
        finally:
            # Free the C-allocated memory
            self.lib.hocdb_free(data_ptr)
    
    def _resolve_field_index(self, field: Union[int, str]) -> int:
        """Resolve field name or index to index"""
        if isinstance(field, int):
            return field
        if isinstance(field, str):
            if field not in self._field_map:
                raise ValueError(f"Unknown field: {field}")
            return self._field_map[field][0]
        raise ValueError(f"Field must be int or str, got {type(field)}")

    def get_stats(self, start_ts: int, end_ts: int, field: Union[int, str]) -> dict:
        """
        Get statistics for a specific field within a time range.
        
        Args:
            start_ts: Start timestamp
            end_ts: End timestamp
            field: Field index (int) or name (str)
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        field_index = self._resolve_field_index(field)
        
        class HOCDBStats(ctypes.Structure):
            _fields_ = [
                ("min", ctypes.c_double),
                ("max", ctypes.c_double),
                ("sum", ctypes.c_double),
                ("count", ctypes.c_uint64),
                ("mean", ctypes.c_double),
            ]

        stats = HOCDBStats()
        res = self.lib.hocdb_get_stats(self.handle, start_ts, end_ts, field_index, ctypes.byref(stats))
        if res != 0:
            raise RuntimeError("get_stats failed")
        
        return {
            "min": stats.min,
            "max": stats.max,
            "sum": stats.sum,
            "count": stats.count,
            "mean": stats.mean
        }

    def get_latest(self, field: Union[int, str]) -> dict:
        """
        Get the latest value and timestamp for a specific field.
        
        Args:
            field: Field index (int) or name (str)
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        
        field_index = self._resolve_field_index(field)
        
        val = ctypes.c_double()
        ts = ctypes.c_longlong()
        
        res = self.lib.hocdb_get_latest(self.handle, field_index, ctypes.byref(val), ctypes.byref(ts))
        if res != 0:
            raise RuntimeError("get_latest failed")
            
        return {
            "value": val.value,
            "timestamp": ts.value
        }

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


class HOCDBFilter(ctypes.Structure):
    """C-compatible filter definition"""
    _fields_ = [
        ("field_index", ctypes.c_size_t),
        ("type", ctypes.c_int),
        ("val_i64", ctypes.c_longlong),
        ("val_f64", ctypes.c_double),
        ("val_u64", ctypes.c_uint64),
        ("val_string", ctypes.c_char * 128),
        ("val_bool", ctypes.c_bool),
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
        elif field.type == FieldTypes.BOOL:
            # Pack as bool (1 byte)
            record_bytes += struct.pack('?', bool(value))
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