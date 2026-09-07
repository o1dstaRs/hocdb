"""
Python bindings for HOCDB - High-Performance Time Series Database
"""
import ctypes
import ctypes.util
import os
from typing import Union, Optional, Any
import sys
import struct

try:
    import numpy as _np  # optional: enables as_numpy=True in the indicator API
except ImportError:  # pragma: no cover
    _np = None


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


class FsyncPolicy:
    """fsync policy constants matching HOCDB_FSYNC_* (HOCDB(fsync=...) accepts the names or the ints)"""
    NONE = 0       # never: the OS decides when data reaches the disk
    ON_CLOSE = 1   # once on close (default)
    ON_FLUSH = 2   # after every flush (commit)
    INTERVAL = 3   # at most every fsync_interval_ms, and on close
    NAMES = {"none": NONE, "on_close": ON_CLOSE, "on_flush": ON_FLUSH, "interval": INTERVAL}


class HOCDB:
    """Python wrapper for HOCDB C API"""

    def __init__(self, ticker: str, path: str, schema: list, max_file_size: Optional[int] = None,
                 overwrite_on_full: bool = False, flush_on_write: bool = False, auto_increment: bool = False,
                 fsync: Union[str, int] = "on_close", fsync_interval_ms: int = 0, verify_on_open: bool = False,
                 retention_span: int = 0, rollover_size: int = 0, auto_migrate: bool = True,
                 timestamp_unit_ns: int = 0, index_stride: int = 0, calendar: Union[int, str, None] = 0):
        """
        Open or create a database as a WRITER (hocdb_init_ex). A writer holds an exclusive lock on the
        file; a second writer on the same ticker/path fails immediately with "DatabaseLocked".
        Use HOCDB.open_reader() to attach to a database another process writes.

        Args:
            ticker: Ticker symbol
            path: Directory path for data
            schema: List of HOCDBField objects defining the schema
            max_file_size: Maximum file size in bytes (None / 0 = default 2 GiB). A ring buffer holding N
                           records is `HOCDB.header_size() + N * record_size` bytes (64-byte header).
            overwrite_on_full: Ring buffer: overwrite the oldest records when the file is full
            flush_on_write: Whether to flush on every write
            auto_increment: Whether to auto-increment timestamp
            fsync: Durability policy: "none" | "on_close" (default) | "on_flush" | "interval"
                   (or FsyncPolicy.* / the ints 0-3)
            fsync_interval_ms: Interval of the "interval" policy in milliseconds (0 = default 1000)
            verify_on_open: Recompute the CRC32C checksum when opening; a corrupted file fails with
                            "ChecksumMismatch" instead of opening
            retention_span: Drop records older than `last timestamp - retention_span` (timestamp units;
                            compaction runs once the excess exceeds 25%); 0 = off
            rollover_size: Archive the file as `<ticker>.<first_ts>-<last_ts>.bin` and continue with an
                           empty one once it exceeds this many bytes; 0 = off
            auto_migrate: Rewrite legacy "HOC1" files to the current "HOC2" format on open (default True)
            timestamp_unit_ns: Nanoseconds per timestamp unit (e.g. 1_000_000_000 for seconds); enables
                               the ingest_lag_record_ns metric; 0 = unknown
            index_stride: Sparse index stride in records (0 = default 1024)
            calendar: Trading calendar of the database: a built-in id (Calendars.NYSE = 3, ...), a name
                      ("crypto", "fx", "nyse", "nasdaq", "lse", "cme" or a calendar_define()d one) or 0 = none.
                      Built-in ids and timestamp_unit_ns are persisted in the file header (a reopen without
                      options and readers pick them up). With a calendar and a timestamp unit the session
                      kinds accept param 0 (calendar sessions), health() measures gaps in trading time
                      and summary() / snapshot() / backtest() derive periods_per_year 0 from the calendar.

        Raises:
            ValueError: unknown calendar name (checked before opening)
            RuntimeError: "Failed to initialize HOCDB: <error name>" with the hocdb_last_error() name
                          ("DatabaseLocked", "SchemaMismatch", "ChecksumMismatch", "UnknownCalendar", ...)
        """
        self._setup(ticker, path, schema)

        config = HOCDBConfig()
        config.max_file_size = _non_negative_int("max_file_size", max_file_size or 0)
        config.overwrite_on_full = 1 if overwrite_on_full else 0
        config.flush_on_write = 1 if flush_on_write else 0
        config.auto_increment = 1 if auto_increment else 0
        config.fsync_policy = _fsync_policy(fsync)
        config.fsync_interval_ms = _non_negative_int("fsync_interval_ms", fsync_interval_ms)
        config.verify_on_open = 1 if verify_on_open else 0
        config.retention_span = _non_negative_int("retention_span", retention_span)
        config.rollover_size = _non_negative_int("rollover_size", rollover_size)
        config.auto_migrate = 1 if auto_migrate else 0
        config.timestamp_unit_ns = _non_negative_int("timestamp_unit_ns", timestamp_unit_ns)
        config.index_stride = _non_negative_int("index_stride", index_stride)
        config.calendar = _resolve_calendar(self.lib, calendar)
        self.config = config

        # Call C API (hocdb_init_ex; the legacy hocdb_init stays usable as self.lib.hocdb_init)
        self.handle = self.lib.hocdb_init_ex(
            self._ticker_bytes,
            self._path_bytes,
            self._c_schema,
            len(self._schema),
            ctypes.byref(config)
        )

        if not self.handle:
            raise RuntimeError(f"Failed to initialize HOCDB: {_last_error(self.lib)}")

    @classmethod
    def open_reader(cls, ticker: str, path: str, schema: list) -> "HOCDB":
        """
        Attach as a lock-free READER to a database that another process (or handle) writes.

        Readers take no lock and see only committed data: every read entry point (query, load, get_stats,
        get_latest, indicators, ...) re-reads the writer's committed cursor, refresh() does it explicitly.
        They follow files the writer compacts or rolls over. append / flush-as-write / sync / compact /
        retain_last / rollover / drop raise RuntimeError (the handle is read-only). Requires the current
        file format (legacy files are migrated the first time a writer opens them).

        Args:
            ticker: Ticker symbol
            path: Directory path for data
            schema: List of HOCDBField objects defining the schema (must match the file)

        Returns:
            An HOCDB instance with read_only == True

        Raises:
            RuntimeError: "Failed to open HOCDB reader: <error name>" (hocdb_last_error())
        """
        self = cls.__new__(cls)
        self._setup(ticker, path, schema)
        self.read_only = True
        self.handle = self.lib.hocdb_open_reader(
            self._ticker_bytes,
            self._path_bytes,
            self._c_schema,
            len(self._schema)
        )
        if not self.handle:
            raise RuntimeError(f"Failed to open HOCDB reader: {_last_error(self.lib)}")
        return self

    def _setup(self, ticker: str, path: str, schema: list):
        """Load the C library, define the signatures and prepare the schema (shared by writers and readers)"""
        # Load the C library - first try to find it in zig-out/lib
        lib_path = self._find_library()
        if not lib_path:
            raise RuntimeError("HOCDB C library not found. Please build with 'zig build c-bindings'")

        self.lib = ctypes.CDLL(lib_path)

        # Define function signatures
        self._define_function_signatures()

        self.handle = None
        self.read_only = False
        self.config = None
        self.ticker = ticker
        self.path = path

        # Prepare schema for C API
        self._schema = list(schema)
        if not self._schema:
            raise ValueError("schema must contain at least one HOCDBField")
        self._field_map = {field.name: (i, field.type) for i, field in enumerate(self._schema)}
        # Build struct format string for packing/unpacking (validates the field types)
        self._struct_fmt = self._build_struct_format()
        self._record_size = struct.calcsize(self._struct_fmt)

        c_schema_fields = []
        for field in self._schema:
            c_field = CField()
            c_field.name = field.name.encode('utf-8')
            c_field.type = field.type
            c_schema_fields.append(c_field)

        # Create array of CField structs (kept alive with the instance)
        CFieldArray = CField * len(c_schema_fields)
        self._c_schema = CFieldArray(*c_schema_fields)

        # Convert Python values to C types
        self._ticker_bytes = ticker.encode('utf-8')
        self._path_bytes = path.encode('utf-8')

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
        return _find_library_path()

    def _define_function_signatures(self):
        """Define argument and return types for C functions"""
        # hocdb_init function (legacy entry point, kept usable; the constructor uses hocdb_init_ex)
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
            ctypes.c_uint32,          # flags
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

        # hocdb_drop function
        self.lib.hocdb_drop.argtypes = [ctypes.c_void_p]
        self.lib.hocdb_drop.restype = ctypes.c_int

        # indicator / analytics functions (shared with the module-level registry helpers)
        _define_indicator_signatures(self.lib)
        # durability, readers, maintenance and metrics
        _define_storage_signatures(self.lib)
        # trading calendars, signal backtester, universe features
        _define_calendar_signatures(self.lib)
        _define_backtest_signatures(self.lib)
        _define_universe_signatures(self.lib)

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
        if result == -10:
            _raise_storage_error(result, "append")
        return result == 0

    def flush(self) -> bool:
        """
        Flush the database: commit the buffered records (they become visible to readers and durable
        according to the fsync policy). On a reader handle this is the same as refresh().
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")

        result = self.lib.hocdb_flush(self.handle)
        if result == -10:
            _raise_storage_error(result, "flush")
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

    def get_stats(self, start_ts: int, end_ts: int, field: Union[int, str], compute_percentiles: bool = False) -> dict:
        """
        Get statistics for a specific field within a time range.
        
        Args:
            start_ts: Start timestamp
            end_ts: End timestamp
            field: Field index (int) or name (str)
            compute_percentiles: Whether to compute percentiles (slower due to sorting)
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
                ("p50", ctypes.c_double),
                ("p90", ctypes.c_double),
                ("p95", ctypes.c_double),
                ("p99", ctypes.c_double),
            ]

        stats = HOCDBStats()
        flags = 1 if compute_percentiles else 0
        res = self.lib.hocdb_get_stats(self.handle, start_ts, end_ts, field_index, flags, ctypes.byref(stats))
        if res != 0:
            raise RuntimeError("get_stats failed")
        
        return {
            "min": stats.min,
            "max": stats.max,
            "sum": stats.sum,
            "count": stats.count,
            "mean": stats.mean,
            "p50": stats.p50,
            "p90": stats.p90,
            "p95": stats.p95,
            "p99": stats.p99
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

    # ------------------------------------------------------------------
    # Technical indicators and quantitative analytics
    # ------------------------------------------------------------------

    def _resolve_columns(self, columns: Optional[dict]) -> "HOCDBIndicatorColumns":
        """
        Map the OHLCV / quote roles (open, high, low, close, volume, bid, ask, side) to field indices.

        columns=None auto-detects fields literally named like the roles; a field named 'price' is
        used as close when there is no 'close', and 'size' / 'qty' as volume when there is no 'volume'.
        Otherwise columns is a dict {role: field name or index}; 'close' is required.
        """
        cols = HOCDBIndicatorColumns(*([-1] * len(_COLUMN_ROLES)))
        if columns is None:
            for role in _COLUMN_ROLES:
                if role in self._field_map:
                    setattr(cols, role, self._field_map[role][0])
            if cols.close < 0 and 'price' in self._field_map:
                cols.close = self._field_map['price'][0]
            if cols.volume < 0:
                for alias in ('size', 'qty'):
                    if alias in self._field_map:
                        cols.volume = self._field_map[alias][0]
                        break
            if cols.close < 0:
                raise ValueError("No close column found: the schema has no field named 'close' or 'price'. "
                                 "Pass columns={'close': <field name or index>, ...}")
            return cols
        if not isinstance(columns, dict):
            raise ValueError("columns must be a dict like {'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'}")
        for role, field in columns.items():
            if role not in _COLUMN_ROLES:
                raise ValueError(f"Unknown column role {role!r}; expected one of {', '.join(_COLUMN_ROLES)}")
            if field is not None:
                setattr(cols, role, self._resolve_field_index(field))
        if cols.close < 0:
            raise ValueError("columns must include 'close' (field name or index)")
        return cols

    def indicators(self, specs, start_ts: Optional[int] = None, end_ts: Optional[int] = None,
                   tail: Optional[int] = None, columns: Optional[dict] = None,
                   lookback: Union[str, int] = "auto", bucket: int = 0, as_numpy: bool = False) -> dict:
        """
        Compute a batch of technical indicators in one pass.

        Args:
            specs: List of spec dicts, e.g. {"kind": "rsi", "period": 14} or
                   {"kind": "sma", "period": 10, "field": "volume", "label": "vol_sma"}.
                   Keys: kind (name or id), period, period2, period3, period4, param, param2,
                   field / field2 (field name or index; default: the close column), label.
                   Zero / missing periods and params select the documented defaults.
            start_ts, end_ts: Window [start_ts, end_ts)  -- OR --
            tail: Number of last rows (bars when bucket > 0) to return.
            columns: {open, high, low, close, volume} -> field name or index (default: auto-detect).
            lookback: "auto" (recommended per-spec warm-up read before the window) or an int (0 = none).
            bucket: 0 = one row per record; > 0 = aggregate records into OHLCV bars of that
                    many timestamp units first (tick -> bar).
            as_numpy: Return numpy arrays instead of lists (when numpy is installed).

        Returns:
            {"timestamps": [...], "n_rows": int, "columns": {name: [...]}}.
            Column names: the spec label if given, else "<kind>" or "<kind>_<period>";
            multi-output kinds append "_<output>" (e.g. "macd_signal", "bbands_20_upper").
            NaN marks the warm-up region (value not defined yet).
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        _check_indicator_window(start_ts, end_ts, tail, bucket)
        cols = self._resolve_columns(columns)
        c_specs, names = self._indicator_specs(specs)
        lb = _resolve_lookback(lookback)

        res = HOCDBIndicatorResult()
        if tail is not None:
            rc = self.lib.hocdb_indicators_tail(self.handle, tail, ctypes.byref(cols), c_specs, len(c_specs),
                                                lb, bucket, ctypes.byref(res))
        else:
            rc = self.lib.hocdb_indicators(self.handle, start_ts, end_ts, ctypes.byref(cols), c_specs, len(c_specs),
                                           lb, bucket, ctypes.byref(res))
        if rc != 0:
            _raise_indicator_error(rc, "indicators")
        return self._indicator_result(res, names, as_numpy, "indicators")

    def pair_indicators(self, other: "HOCDB", specs, start_ts: Optional[int] = None, end_ts: Optional[int] = None,
                        tail: Optional[int] = None, columns: Optional[dict] = None,
                        other_columns: Optional[dict] = None, lookback: Union[str, int] = "auto",
                        bucket: int = 0, as_numpy: bool = False) -> dict:
        """
        Compute indicators over this database aligned with a second database (pairs / relative value).

        The close column of `other` is the second input series: `series2`, `ratio`, `ratio_zscore`,
        `rel_strength`, `correl` and `beta` use both databases; every other kind runs on this one.
        With bucket > 0 both databases are resampled into bars and inner-joined on bar timestamps;
        on ticks (bucket 0) each row of this database is paired with the latest row of `other` at or
        before it (as-of join).

        Args:
            other: Another open HOCDB instance (database B).
            specs, start_ts, end_ts, tail, lookback, bucket, as_numpy: As in indicators().
            columns: Column roles of THIS database (default: auto-detect).
            other_columns: Column roles of `other`, resolved against its schema (default: auto-detect).

        Returns:
            {"timestamps": [...], "n_rows": int, "columns": {name: [...]}} exactly like indicators().
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        if not isinstance(other, HOCDB):
            raise ValueError("other must be another HOCDB instance (the second database of the pair)")
        if not other.handle:
            raise RuntimeError("The other database is not initialized (closed?)")
        _check_indicator_window(start_ts, end_ts, tail, bucket)
        cols_a = self._resolve_columns(columns)
        cols_b = other._resolve_columns(other_columns)
        c_specs, names = self._indicator_specs(specs)
        lb = _resolve_lookback(lookback)

        res = HOCDBIndicatorResult()
        if tail is not None:
            rc = self.lib.hocdb_pair_indicators_tail(self.handle, ctypes.byref(cols_a), other.handle, ctypes.byref(cols_b),
                                                     tail, c_specs, len(c_specs), lb, bucket, ctypes.byref(res))
        else:
            rc = self.lib.hocdb_pair_indicators(self.handle, ctypes.byref(cols_a), other.handle, ctypes.byref(cols_b),
                                                start_ts, end_ts, c_specs, len(c_specs), lb, bucket, ctypes.byref(res))
        if rc != 0:
            _raise_indicator_error(rc, "pair_indicators")
        return self._indicator_result(res, names, as_numpy, "pair_indicators")

    def _indicator_specs(self, specs):
        """Convert spec dicts into a C array of HOCDBIndicatorSpec plus the result column names"""
        if isinstance(specs, dict):
            specs = [specs]
        specs = list(specs)
        if not specs:
            raise ValueError("specs must contain at least one indicator spec")
        c_specs = (HOCDBIndicatorSpec * len(specs))(
            *[_spec_to_c(self.lib, s, self._resolve_field_index) for s in specs])
        return c_specs, _column_names(self.lib, specs, c_specs)

    def _indicator_result(self, res: "HOCDBIndicatorResult", names: list, as_numpy: bool, what: str) -> dict:
        """Copy a HOCDBIndicatorResult into Python lists (or numpy arrays) and free the C buffers"""
        use_numpy = as_numpy and _np is not None
        try:
            n = res.n_rows
            if res.n_outputs != len(names):
                raise RuntimeError(f"{what}: expected {len(names)} outputs, the C API returned {res.n_outputs}")
            # Copy everything out of the C buffers before freeing them
            timestamps = _copy_c_array(res.timestamps, n, use_numpy)
            planar = _copy_c_array(res.values, n * res.n_outputs, use_numpy)
        finally:
            self.lib.hocdb_indicators_free(ctypes.byref(res))

        out_columns = {}
        for k, name in enumerate(names):
            out_columns[name] = planar[k * n:(k + 1) * n]
        return {"timestamps": timestamps, "n_rows": n, "columns": out_columns}

    def ohlcv(self, start_ts: int, end_ts: int, bucket: int, price: Union[int, str] = "close",
              volume: Optional[Union[int, str]] = None, side: Optional[Union[int, str]] = None,
              as_numpy: bool = False) -> dict:
        """
        Aggregate records in [start_ts, end_ts) into OHLCV bars of `bucket` timestamp units.

        Args:
            start_ts: Start timestamp (inclusive)
            end_ts: End timestamp (exclusive)
            bucket: Bar size in timestamp units (> 0)
            price: Price field (name or index) used for open/high/low/close
            volume: Volume field (name or index), optional; without it `volume` is the record count
            side: Trade side field (name or index; 1 / True = buy), optional; with it the result also
                  contains `buy_volume`, the per-bar volume of the buy-side records
            as_numpy: Return numpy arrays instead of lists (when numpy is installed)

        Returns:
            dict with keys timestamps, open, high, low, close, volume, count (arrays) and n_bars (int),
            plus buy_volume when `side` is given
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        if isinstance(bucket, bool) or not isinstance(bucket, int) or bucket <= 0:
            raise ValueError("bucket must be a positive int (bar size in timestamp units)")
        price_field = self._resolve_field_index(price)
        volume_field = -1 if volume is None else self._resolve_field_index(volume)
        side_field = -1 if side is None else self._resolve_field_index(side)
        use_numpy = as_numpy and _np is not None

        bars = HOCDBBarsEx()
        rc = self.lib.hocdb_ohlcv_ex(self.handle, start_ts, end_ts, price_field, volume_field, side_field, bucket,
                                     ctypes.byref(bars))
        if rc != 0:
            _raise_indicator_error(rc, "ohlcv")
        try:
            n = bars.n_bars
            out = {"timestamps": _copy_c_array(bars.timestamps, n, use_numpy), "n_bars": n}
            for name in ("open", "high", "low", "close", "volume", "count"):
                out[name] = _copy_c_array(getattr(bars, name), n, use_numpy)
            if side_field >= 0:
                out["buy_volume"] = _copy_c_array(bars.buy_volume, n, use_numpy)
        finally:
            self.lib.hocdb_ohlcv_ex_free(ctypes.byref(bars))
        return out

    def summary(self, start_ts: int, end_ts: int, field: Union[int, str], periods_per_year: float = 0.0) -> dict:
        """
        Scalar performance / risk summary of a field over [start_ts, end_ts).

        Args:
            start_ts: Start timestamp (inclusive)
            end_ts: End timestamp (exclusive)
            field: Field index (int) or name (str)
            periods_per_year: Annualisation factor for ann_return / ann_vol / sharpe / sortino (0 = none)

        Returns:
            dict with the 29 summary fields (count, first, last, min, max, mean, std, total_return,
            log_return, ann_return, ann_vol, sharpe, sortino, max_drawdown, max_drawdown_bars, calmar,
            skew, kurtosis, var_95, cvar_95, win_rate, avg_gain, avg_loss, profit_factor, best, worst,
            autocorr_1, hurst, half_life), decoded via the C struct introspection.
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        field_index = self._resolve_field_index(field)
        buf = ctypes.create_string_buffer(self.lib.hocdb_summary_size())
        rc = self.lib.hocdb_summary(self.handle, start_ts, end_ts, field_index, float(periods_per_year),
                                    ctypes.cast(buf, ctypes.c_void_p))
        if rc != 0:
            _raise_indicator_error(rc, "summary")
        return _decode_struct(self.lib, "summary", buf)

    def snapshot(self, columns: Optional[dict] = None, bars: int = 0, bucket: int = 0,
                 periods_per_year: float = 0.0) -> dict:
        """
        One-shot snapshot of ~100 indicators for the latest bar (a ready-made input for an LLM / trading agent).

        Args:
            columns: {open, high, low, close, volume} -> field name or index (default: auto-detect)
            bars: Records (bars when bucket > 0) to use; 0 = recommended 2500 (every field converges)
            bucket: 0 = one bar per record; > 0 = aggregate records into bars of that many timestamp units
            periods_per_year: Annualisation for volatility / Sharpe / Sortino (0 = none)

        Returns:
            dict of all snapshot fields: `timestamp` (int), `bars` (int) and one float per indicator
            (sma_20, ema_200, rsi_14, macd_hist, adx_14, bb_upper, ...). NaN = not enough data yet.
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        if isinstance(bars, bool) or not isinstance(bars, int) or bars < 0:
            raise ValueError("bars must be a non-negative int (0 = recommended)")
        cols = self._resolve_columns(columns)
        buf = ctypes.create_string_buffer(self.lib.hocdb_snapshot_size())
        rc = self.lib.hocdb_snapshot(self.handle, ctypes.byref(cols), bars, bucket, float(periods_per_year),
                                     ctypes.cast(buf, ctypes.c_void_p))
        if rc != 0:
            _raise_indicator_error(rc, "snapshot")
        return _decode_struct(self.lib, "snapshot", buf)

    def snapshot_multi(self, buckets, periods_per_year=None, bars: int = 0, columns: Optional[dict] = None) -> list:
        """
        Snapshots for several bar sizes from a single read of the data (multi-timeframe view).

        Args:
            buckets: Bar sizes in timestamp units, one per snapshot (e.g. [60_000_000, 300_000_000] for 1-minute
                     and 5-minute bars on microsecond timestamps); every bucket must be > 0
            periods_per_year: Annualisation factor per bucket (a list as long as `buckets`; default: 0 = none)
            bars: Bars to use per bucket; 0 = recommended 2500 (every field converges)
            columns: {open, high, low, close, volume, ...} -> field name or index (default: auto-detect)

        Returns:
            List of snapshot dicts in `buckets` order, each with the same fields as snapshot()
            (`bars` reports how many bars actually existed for that bucket).
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        if isinstance(bars, bool) or not isinstance(bars, int) or bars < 0:
            raise ValueError("bars must be a non-negative int (0 = recommended)")
        if isinstance(buckets, (str, bytes, dict)) or not hasattr(buckets, "__iter__"):
            raise ValueError("buckets must be a list of bar sizes (timestamp units)")
        buckets = list(buckets)
        if not buckets:
            raise ValueError("buckets must contain at least one bar size")
        for b in buckets:
            if isinstance(b, bool) or not isinstance(b, int) or b <= 0:
                raise ValueError(f"Every bucket must be a positive int (bar size in timestamp units), got {b!r}")
        if periods_per_year is None:
            ppy = [0.0] * len(buckets)
        else:
            if isinstance(periods_per_year, (str, bytes, dict)) or not hasattr(periods_per_year, "__iter__"):
                raise ValueError("periods_per_year must be a list with one annualisation factor per bucket")
            ppy = [float(p) for p in periods_per_year]
            if len(ppy) != len(buckets):
                raise ValueError(f"periods_per_year has {len(ppy)} entries but buckets has {len(buckets)}")
        cols = self._resolve_columns(columns)
        n = len(buckets)
        c_buckets = (ctypes.c_int64 * n)(*buckets)
        c_ppy = (ctypes.c_double * n)(*ppy)
        size = self.lib.hocdb_snapshot_size()
        buf = ctypes.create_string_buffer(size * n)
        rc = self.lib.hocdb_snapshot_multi(self.handle, ctypes.byref(cols), bars, c_buckets, n, c_ppy,
                                           ctypes.cast(buf, ctypes.c_void_p))
        if rc != 0:
            _raise_indicator_error(rc, "snapshot_multi")
        return [_decode_struct(self.lib, "snapshot", buf, k * size) for k in range(n)]

    def health(self, start_ts: int, end_ts: int, price: Union[int, str] = "close",
               volume: Optional[Union[int, str]] = None, gap_threshold: int = 0,
               outlier_threshold: float = 0.0) -> dict:
        """
        Data-quality statistics of the records in [start_ts, end_ts).

        Args:
            start_ts: Start timestamp (inclusive)
            end_ts: End timestamp (exclusive)
            price: Price field (name or index)
            volume: Volume field (name or index), optional (counts zero / negative volumes when given)
            gap_threshold: Gaps between consecutive timestamps above this many units are counted in n_gaps
            outlier_threshold: |log return| above this is counted in n_outlier_returns (0 = never)

        Returns:
            dict with the 19 health fields (count, first_ts, last_ts, span, mean_gap, median_gap, max_gap,
            max_gap_at, n_gaps, n_nonpositive_price, n_nan_price, n_outlier_returns, first_outlier_at,
            max_abs_return, n_zero_volume, n_negative_volume, closed_span, n_session_breaks,
            n_missing_sessions), decoded via the C struct introspection. The last three need a trading
            calendar and a timestamp unit on the handle: closed time inside [first, last] (timestamp units),
            gaps spanning a session boundary and sessions without any row; 0 without a calendar.
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        price_field = self._resolve_field_index(price)
        volume_field = -1 if volume is None else self._resolve_field_index(volume)
        if isinstance(gap_threshold, bool) or not isinstance(gap_threshold, int) or gap_threshold < 0:
            raise ValueError("gap_threshold must be a non-negative int (timestamp units)")
        out = _new_struct(self.lib, "health", HOCDBHealth)
        rc = self.lib.hocdb_health(self.handle, start_ts, end_ts, price_field, volume_field, gap_threshold,
                                   float(outlier_threshold), ctypes.byref(out))
        if rc != 0:
            _raise_indicator_error(rc, "health")
        return _decode_struct(self.lib, "health", out)

    def evaluate(self, decisions, price: Union[int, str] = "close", default_horizon: int = 0,
                 cost_bps: float = 0.0) -> dict:
        """
        Evaluate trading decisions against the recorded prices (hit rate, PnL, Sharpe, drawdown, ...).

        Args:
            decisions: List of dicts {"timestamp": int, "direction": +1 long / -1 short / 0 flat (ignored),
                       "size": position size in currency units (default 1), "horizon": holding time in
                       timestamp units (default 0 = use default_horizon)}. Entry is the first price at or
                       after `timestamp`, exit the first price at or after `timestamp + horizon`.
            price: Price field (name or index)
            default_horizon: Horizon (timestamp units) for decisions whose horizon is 0
            cost_bps: Transaction cost in basis points, charged per side

        Returns:
            dict with the 20 evaluation fields (n_decisions, n_evaluated, n_long, n_short, hit_rate, avg_return,
            avg_net_return, total_pnl, total_cost, sharpe, profit_factor, max_drawdown, avg_win, avg_loss, best,
            worst, long_hit_rate, short_hit_rate, long_avg_return, short_avg_return) plus the per-decision lists
            "entry", "exit" and "net_return" (NaN where a decision could not be evaluated).
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        price_field = self._resolve_field_index(price)
        if isinstance(default_horizon, bool) or not isinstance(default_horizon, int) or default_horizon < 0:
            raise ValueError("default_horizon must be a non-negative int (timestamp units)")
        if isinstance(decisions, dict):
            decisions = [decisions]
        c_decisions = [_decision_to_c(d) for d in decisions]
        n = len(c_decisions)
        _check_struct_size(self.lib, "decision", HOCDBDecision)
        # NULL arrays for an empty decision list (the C API accepts n = 0 with NULL pointers)
        c_array = (HOCDBDecision * n)(*c_decisions) if n else None
        entry = (ctypes.c_double * n)() if n else None
        exit_ = (ctypes.c_double * n)() if n else None
        net = (ctypes.c_double * n)() if n else None
        out = _new_struct(self.lib, "evaluation", HOCDBEvaluation)
        rc = self.lib.hocdb_evaluate(self.handle, price_field, c_array, n, default_horizon, float(cost_bps),
                                     ctypes.byref(out), entry, exit_, net)
        if rc != 0:
            _raise_indicator_error(rc, "evaluate")
        result = _decode_struct(self.lib, "evaluation", out)
        result["entry"] = list(entry) if n else []
        result["exit"] = list(exit_) if n else []
        result["net_return"] = list(net) if n else []
        return result

    def indicator_kinds(self) -> list:
        """Names of all supported indicator kinds (same as the module-level indicator_kinds())"""
        return _indicator_kinds(self.lib)

    def indicator_outputs(self, kind) -> list:
        """Output names of an indicator kind (name or id), e.g. ["macd", "signal", "hist"]"""
        return _indicator_outputs(self.lib, kind)

    def indicator_warmup(self, spec: dict) -> int:
        """Recommended warm-up rows for a spec dict (field names are resolved against this schema)"""
        return _indicator_warmup(self.lib, spec, self._resolve_field_index)

    def indicator_is_lookahead(self, kind) -> bool:
        """True when the kind (name or id) uses future rows, i.e. it is a label such as forward_return / triple_barrier"""
        return _indicator_is_lookahead(self.lib, kind)

    # ------------------------------------------------------------------
    # Trading calendar and timestamp unit of the handle
    # ------------------------------------------------------------------

    def set_calendar(self, calendar: Union[int, str, None]) -> int:
        """
        Set the trading calendar of this handle: a built-in id (Calendars.NYSE = 3), a name ("nyse") or the
        id of a calendar_define()d calendar; 0 / None removes it. Writers persist built-in ids in the file
        header; on readers the setting is local to the handle. Returns the id.

        Raises:
            ValueError: unknown name or id ("UnknownCalendar")
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        cid = _resolve_calendar(self.lib, calendar)
        rc = self.lib.hocdb_set_calendar(self.handle, cid)
        if rc != 0:
            _raise_indicator_error(rc, "set_calendar")
        return cid

    def get_calendar(self) -> int:
        """Calendar id of this handle (0 = none); get_calendar_name() gives the name"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        return self.lib.hocdb_get_calendar(self.handle)

    def get_calendar_name(self) -> Optional[str]:
        """Name of this handle's calendar ("nyse", ...), None without a calendar"""
        cid = self.get_calendar()
        return _calendar_name(self.lib, cid) if cid else None

    def set_timestamp_unit(self, unit_ns: int) -> bool:
        """
        Set the timestamp unit of this handle in nanoseconds per unit (1000 = microseconds, 1_000_000 =
        milliseconds, 1_000_000_000 = seconds; 0 = unknown). Writers persist it in the file header. Returns True.
        """
        self._storage_call("set_timestamp_unit", self.lib.hocdb_set_timestamp_unit,
                           _non_negative_int("unit_ns", unit_ns))
        return True

    def get_timestamp_unit(self) -> int:
        """Nanoseconds per timestamp unit of this handle (0 = unknown)"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        return self.lib.hocdb_get_timestamp_unit(self.handle)

    def periods_per_year(self, bucket: int) -> float:
        """
        Bars per year for bars of `bucket` timestamp units, derived from the handle's calendar and timestamp
        unit (0.0 when either is unknown). This is the value summary() / snapshot() / snapshot_multi() /
        backtest() use when their periods_per_year is 0.
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        if isinstance(bucket, bool) or not isinstance(bucket, int):
            raise ValueError("bucket must be an int (bar size in timestamp units)")
        return self.lib.hocdb_periods_per_year(self.handle, bucket)

    # ------------------------------------------------------------------
    # Signal backtester
    # ------------------------------------------------------------------

    def backtest(self, target, start_ts: int, end_ts: int, bucket: int = 0, params: Optional[dict] = None,
                 columns: Optional[dict] = None, outputs=None, max_trades: Optional[int] = None,
                 as_numpy: bool = False) -> dict:
        """
        Backtest a target-position series over the rows of [start_ts, end_ts).

        target[i] is the desired position at the END of row i of indicators(specs, start_ts, end_ts,
        bucket=bucket) over the same window: with bucket > 0 the bars whose start lies in [start_ts, end_ts)
        (the same bars as ohlcv() for bucket-aligned bounds), with bucket 0 the raw records. Compute the
        signals with indicators() over the same window and pass one target per row; len(target) must equal
        the row count (ValueError otherwise). The unit of the target is params["position_mode"]: units,
        fraction of equity (1.0 = 100% long) or notional. NaN = keep the previous signal. Fills happen at the
        next bar's open (fill_mode 0, no look-ahead) or the same close (1) with adverse slippage and cost_bps
        per side; stop_loss / take_profit / trailing_stop exit intrabar and a stopped position is not
        re-entered on the same signal. periods_per_year 0 is derived from the handle's calendar and
        timestamp unit (periods_per_year()).

        Args:
            target: Desired positions, one per row (list or numpy array)
            start_ts, end_ts: Window [start_ts, end_ts)
            bucket: 0 = one row per record; > 0 = bars of that many timestamp units
            params: dict of backtest parameters; missing keys take the C defaults (backtest_params_default()):
                    initial_equity (1.0), cost_bps, slippage_bps, stop_loss, take_profit, trailing_stop
                    (fractions, 0 = none), max_position (cap on |units|, 0 = none), position_mode
                    (0 / "units", 1 / "fraction", 2 / "notional"), fill_mode (0 / "next_open", 1 / "same_close"),
                    periods_per_year (0 = from the calendar), allow_short (True), risk_free_rate (annual)
            columns: {open, high, low, close, volume} -> field name or index (default: auto-detect);
                     open / high / low are optional (without them fills happen at the close and stops trigger
                     on the close)
            outputs: Per-bar series to return in addition to the result: any of "equity", "position", "cash",
                     "pnl", "drawdown" (a list / tuple / set of names, one name, or True for all of them)
            max_trades: Return up to this many trades under "trades" (n_trades in the result counts all of them)
            as_numpy: Return the per-bar series as numpy arrays (when numpy is installed)

        Returns:
            {"result": {the 32 result fields: n_bars, n_trades, n_long_trades, n_short_trades, final_equity,
                        total_return, ann_return, ann_vol, sharpe, sortino, calmar, max_drawdown,
                        max_drawdown_bars, avg_drawdown, win_rate, profit_factor, avg_trade_return, avg_win,
                        avg_loss, best_trade, worst_trade, avg_holding_bars, exposure, long_share, turnover,
                        total_cost, total_slippage, n_stop_exits, n_take_profit_exits, n_trailing_exits,
                        gross_pnl, net_pnl},
             "trades": [{entry_ts, exit_ts (0 = still open), direction, entry_price, exit_price, size, pnl,
                         ret, bars, exit_reason (0 signal, 1 stop_loss, 2 take_profit, 3 trailing,
                         4 end of data)}, ...]  (only when max_trades is given),
             "equity": [...], "position": [...], ...  (one array per requested output)}
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        _check_bucket(bucket)
        cols = self._resolve_columns(columns)
        c_target, n = _c_double_array("target", target)
        c_params = _backtest_params(self.lib, params)
        c_outputs, out_arrays = _backtest_outputs(outputs, n)
        c_trades, cap = _trade_buffer(self.lib, max_trades)
        out = _new_struct(self.lib, "backtest_result", HOCDBBacktestResult)
        rc = self.lib.hocdb_backtest(self.handle, ctypes.byref(cols), start_ts, end_ts, bucket, c_target, n,
                                     ctypes.byref(c_params), c_outputs, c_trades, cap, ctypes.byref(out))
        if rc != 0:
            _raise_indicator_error(rc, "backtest")
        return _backtest_dict(self.lib, out, c_trades, max_trades, out_arrays, as_numpy)

    def backtest_tail(self, target, bucket: int = 0, params: Optional[dict] = None, columns: Optional[dict] = None,
                      outputs=None, max_trades: Optional[int] = None, as_numpy: bool = False) -> dict:
        """
        Backtest over the last len(target) bars (bucket > 0) or records (bucket 0): target[i] is the desired
        position at the end of row i of indicators(..., tail=len(target), bucket=bucket). Same options and
        result as backtest().
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        _check_bucket(bucket)
        cols = self._resolve_columns(columns)
        c_target, n = _c_double_array("target", target)
        c_params = _backtest_params(self.lib, params)
        c_outputs, out_arrays = _backtest_outputs(outputs, n)
        c_trades, cap = _trade_buffer(self.lib, max_trades)
        out = _new_struct(self.lib, "backtest_result", HOCDBBacktestResult)
        rc = self.lib.hocdb_backtest_tail(self.handle, ctypes.byref(cols), bucket, c_target, n, ctypes.byref(c_params),
                                          c_outputs, c_trades, cap, ctypes.byref(out))
        if rc != 0:
            _raise_indicator_error(rc, "backtest_tail")
        return _backtest_dict(self.lib, out, c_trades, max_trades, out_arrays, as_numpy)

    # ------------------------------------------------------------------
    # Durability, readers, maintenance and metrics
    # ------------------------------------------------------------------

    def _storage_call(self, what: str, fn, *args) -> int:
        """Call a storage C function on the handle and map a negative return code to an exception"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        rc = fn(self.handle, *args)
        if rc < 0:
            _raise_storage_error(rc, what)
        return rc

    def refresh(self) -> bool:
        """
        Readers: pick up the writer's latest commit (re-read the committed cursor and follow a compacted /
        rolled-over file). Every read entry point does this automatically; call it to make the effect
        explicit. Writers: a no-op. Returns True.
        """
        self._storage_call("refresh", self.lib.hocdb_refresh)
        return True

    def is_read_only(self) -> bool:
        """True when this handle is a lock-free reader (opened with HOCDB.open_reader)"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        return bool(self.lib.hocdb_is_read_only(self.handle))

    def sync(self) -> bool:
        """Flush and fsync now, whatever the fsync policy (writers only). Returns True; raises on a reader."""
        self._storage_call("sync", self.lib.hocdb_sync)
        return True

    def verify(self) -> bool:
        """
        Recompute the CRC32C checksum of the committed data and compare it with the stored one
        (the pending buffer is flushed first).

        Returns:
            True when the checksum matches, False on a MISMATCH (the crc_failures metric is incremented)

        Raises:
            RuntimeError: "checksum unavailable ..." for a ring buffer (overwrite_on_full), a legacy HOC1
                          file or a file whose tail was adopted by crash recovery (C code -20)
        """
        rc = self._storage_call("verify", self.lib.hocdb_verify)
        return rc == 1

    def compact(self, min_ts: int) -> bool:
        """
        Keep only the records with timestamp >= min_ts (the file is rewritten atomically; readers follow).
        Writers only. Returns True.
        """
        if isinstance(min_ts, bool) or not isinstance(min_ts, int):
            raise ValueError("min_ts must be an int (timestamp)")
        self._storage_call("compact", self.lib.hocdb_compact, min_ts)
        return True

    def retain_last(self, n: int) -> bool:
        """Keep only the last n records (the file is rewritten atomically; readers follow). Writers only. Returns True."""
        self._storage_call("retain_last", self.lib.hocdb_retain_last, _non_negative_int("n", n))
        return True

    def rollover(self) -> str:
        """
        Archive the current file as `<ticker>.<first_ts>-<last_ts>.bin` in the same directory and continue
        with an empty file (timestamps stay monotonic across files; readers follow). Writers only.

        Returns:
            The path of the archive file. Open it as a normal database with
            HOCDB(<archive file name without .bin>, path, schema).
        """
        buf = ctypes.create_string_buffer(4096)
        self._storage_call("rollover", self.lib.hocdb_rollover, buf, len(buf))
        return buf.value.decode('utf-8')

    def metrics(self) -> dict:
        """
        Operational counters and state of this handle, decoded via the hocdb_metrics_field_* introspection.

        Returns:
            dict with the 30 metrics fields as Python ints: appends, bytes_written, flushes, commits, fsyncs,
            fsync_ns_total, fsync_ns_max, reads, read_ns_total, read_ns_max, read_ns_last, read_ns_p50,
            read_ns_p99, records_read, refreshes, recovered_tail_records, dropped_tail_bytes, crc_failures,
            compactions, rollovers, migrations, last_append_wall_ns, last_commit_wall_ns, last_record_ts,
            ingest_lag_wall_ns, ingest_lag_record_ns, committed_records, file_size, format_version, read_only
        """
        if not self.handle:
            raise RuntimeError("Database not initialized")
        out = _new_struct(self.lib, "metrics", HOCDBMetrics)
        rc = self.lib.hocdb_metrics(self.handle, ctypes.byref(out))
        if rc != 0:
            _raise_storage_error(rc, "metrics")
        return _decode_struct(self.lib, "metrics", out)

    def metrics_reset(self):
        """Reset the counters of metrics(); the state fields (last_record_ts, committed_records, ...) are kept"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        self.lib.hocdb_metrics_reset(self.handle)

    def format_version(self) -> int:
        """File format version of the open database: 1 = legacy "HOC1", 2 = current "HOC2" (64-byte header)"""
        if not self.handle:
            raise RuntimeError("Database not initialized")
        return self.lib.hocdb_format_version(self.handle)

    @staticmethod
    def header_size() -> int:
        """Bytes reserved by the file header (64): a ring buffer for N records is header_size() + N * record_size"""
        return header_size()

    def close(self):
        """Close and free the database handle"""
        if self.handle:
            self.lib.hocdb_close(self.handle)
            self.handle = None

    def drop(self):
        """Close the database and delete all data files from disk (writers only)"""
        if self.handle:
            if self.read_only:
                _raise_storage_error(-10, "drop")
            rc = self.lib.hocdb_drop(self.handle)
            self.handle = None
            if rc != 0:
                raise RuntimeError(f"drop failed: error code {rc}")


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


# ---------------------------------------------------------------------------
# Durability, readers, maintenance and metrics
# ---------------------------------------------------------------------------

class HOCDBConfig(ctypes.Structure):
    """C-compatible HOCDBConfig passed to hocdb_init_ex (80 bytes; ctypes inserts the 4 padding bytes
    after auto_migrate). Zero means "default" for every field except auto_migrate (1 = migrate)."""
    _fields_ = [
        ("max_file_size", ctypes.c_int64),        # 0 = default (2 GiB)
        ("overwrite_on_full", ctypes.c_int),      # ring buffer when full
        ("flush_on_write", ctypes.c_int),
        ("auto_increment", ctypes.c_int),
        ("fsync_policy", ctypes.c_int),           # FsyncPolicy.*
        ("fsync_interval_ms", ctypes.c_uint32),   # 0 = default 1000
        ("verify_on_open", ctypes.c_int),         # recompute the checksum when opening
        ("retention_span", ctypes.c_int64),       # drop records older than last - span; 0 = off
        ("rollover_size", ctypes.c_uint64),       # archive the file above this many bytes; 0 = off
        ("auto_migrate", ctypes.c_int),           # rewrite legacy HOC1 files on open
        ("timestamp_unit_ns", ctypes.c_uint64),   # ns per timestamp unit for ingest-lag reporting; 0 = unknown
        ("index_stride", ctypes.c_uint64),        # 0 = default 1024
        ("calendar", ctypes.c_uint32),            # trading calendar id (calendar_id("nyse")), 0 = none
        ("reserved0", ctypes.c_uint32),
    ]


class HOCDBMetrics(ctypes.Structure):
    """C-compatible HOCDBMetrics (30 x 8 bytes); decoded by name via the hocdb_metrics_field_* introspection"""
    _fields_ = [
        ("appends", ctypes.c_uint64),
        ("bytes_written", ctypes.c_uint64),
        ("flushes", ctypes.c_uint64),
        ("commits", ctypes.c_uint64),
        ("fsyncs", ctypes.c_uint64),
        ("fsync_ns_total", ctypes.c_uint64),
        ("fsync_ns_max", ctypes.c_uint64),
        ("reads", ctypes.c_uint64),
        ("read_ns_total", ctypes.c_uint64),
        ("read_ns_max", ctypes.c_uint64),
        ("read_ns_last", ctypes.c_uint64),
        ("read_ns_p50", ctypes.c_uint64),
        ("read_ns_p99", ctypes.c_uint64),
        ("records_read", ctypes.c_uint64),
        ("refreshes", ctypes.c_uint64),
        ("recovered_tail_records", ctypes.c_uint64),
        ("dropped_tail_bytes", ctypes.c_uint64),
        ("crc_failures", ctypes.c_uint64),
        ("compactions", ctypes.c_uint64),
        ("rollovers", ctypes.c_uint64),
        ("migrations", ctypes.c_uint64),
        ("last_append_wall_ns", ctypes.c_int64),
        ("last_commit_wall_ns", ctypes.c_int64),
        ("last_record_ts", ctypes.c_int64),
        ("ingest_lag_wall_ns", ctypes.c_int64),    # now - last commit (readers) / last append (writers)
        ("ingest_lag_record_ns", ctypes.c_int64),  # now - last record time, when timestamp_unit_ns is set
        ("committed_records", ctypes.c_uint64),
        ("file_size", ctypes.c_uint64),
        ("format_version", ctypes.c_uint64),
        ("read_only", ctypes.c_uint64),
    ]


assert ctypes.sizeof(HOCDBConfig) == 80, "HOCDBConfig must be 80 bytes (C ABI)"
assert ctypes.sizeof(HOCDBMetrics) == 240, "HOCDBMetrics must be 240 bytes (C ABI)"

_STORAGE_ERROR_MESSAGES = {
    -1: "out of memory or I/O error",
    -10: "the handle is a read-only reader (opened with HOCDB.open_reader); open a writer with HOCDB(...) to write",
    -11: "DatabaseLocked: another writer holds the file",
    -12: "ChecksumMismatch: the stored CRC32C does not match the committed data",
    -20: "checksum unavailable: the file has no valid stored checksum (a ring buffer with overwrite_on_full "
         "or a legacy HOC1 file); a writer's flush() after new appends, compact() or rollover() restores it",
    -21: "EmptyDatabase: the database has no records (nothing to roll over)",
}


# ---------------------------------------------------------------------------
# Technical indicators and quantitative analytics
# ---------------------------------------------------------------------------

class IndicatorKinds:
    """Indicator kind ids matching the C API (kind names such as "rsi" are accepted everywhere too)"""
    # moving averages
    SMA = 1
    EMA = 2
    WMA = 3
    DEMA = 4
    TEMA = 5
    TRIMA = 6
    KAMA = 7
    HMA = 8
    ZLEMA = 9
    VWMA = 10
    RMA = 11
    # momentum
    RSI = 20
    MACD = 21
    PPO = 22
    STOCH = 23
    STOCH_RSI = 24
    CCI = 25
    WILLR = 26
    MOM = 27
    ROC = 28
    CMO = 29
    TRIX = 30
    ULTOSC = 31
    AO = 32
    TSI = 33
    BOP = 34
    DPO = 35
    # trend
    ADX = 40
    AROON = 41
    PSAR = 42
    SUPERTREND = 43
    VORTEX = 44
    ICHIMOKU = 45
    LINREG = 46
    # volatility
    ATR = 60
    NATR = 61
    TRUE_RANGE = 62
    BBANDS = 63
    KELTNER = 64
    DONCHIAN = 65
    STDDEV = 66
    VARIANCE = 67
    HIST_VOL = 68
    # volume
    OBV = 80
    VWAP = 81
    MFI = 82
    CMF = 83
    AD = 84
    ADOSC = 85
    EFI = 86
    # statistics / risk
    RETURNS = 100
    LOG_RETURNS = 101
    ZSCORE = 102
    PERCENT_RANK = 103
    ROLLING_MIN = 104
    ROLLING_MAX = 105
    DRAWDOWN = 106
    SHARPE = 107
    SORTINO = 108
    CORREL = 109
    BETA = 110
    SKEW = 111
    KURTOSIS = 112
    # price transforms
    TYPICAL_PRICE = 120
    MEDIAN_PRICE = 121
    HEIKIN_ASHI = 122
    # microstructure (ticks with bid / ask / side)
    SPREAD = 130
    ORDER_FLOW = 131
    TICK_PRESSURE = 132
    TRADE_INTENSITY = 133
    AMIHUD = 134
    REALIZED_VOL = 135
    # pairs / passthrough (second series = field2 or the other database of pair_indicators)
    SERIES = 140
    SERIES2 = 141
    RATIO = 142
    RATIO_ZSCORE = 143
    REL_STRENGTH = 144
    # labels: look-ahead by design (NaN at the end of every window)
    FORWARD_RETURN = 150
    TRIPLE_BARRIER = 151
    # session-anchored: param = session length (mandatory), param2 = session offset (timestamp units)
    SESSION_VWAP = 160
    SESSION_RANGE = 161
    OPENING_RANGE = 162
    PIVOTS = 163


class HOCDBIndicatorColumns(ctypes.Structure):
    """C-compatible HOCDBIndicatorColumns: field indices of the OHLCV and quote roles (-1 = absent);
    bid / ask / side (1 = buy, 0 = sell) are the tick-level inputs of the microstructure kinds"""
    _fields_ = [
        ("open", ctypes.c_int64),
        ("high", ctypes.c_int64),
        ("low", ctypes.c_int64),
        ("close", ctypes.c_int64),
        ("volume", ctypes.c_int64),
        ("bid", ctypes.c_int64),
        ("ask", ctypes.c_int64),
        ("side", ctypes.c_int64),
    ]


class HOCDBIndicatorSpec(ctypes.Structure):
    """C-compatible HOCDBIndicatorSpec (ctypes inserts the 4 padding bytes before `param`)"""
    _fields_ = [
        ("kind", ctypes.c_uint32),
        ("period", ctypes.c_uint32),
        ("period2", ctypes.c_uint32),
        ("period3", ctypes.c_uint32),
        ("period4", ctypes.c_uint32),
        ("param", ctypes.c_double),
        ("param2", ctypes.c_double),
        ("field_index", ctypes.c_int64),
        ("field_index2", ctypes.c_int64),
    ]


class HOCDBIndicatorResult(ctypes.Structure):
    """C-compatible HOCDBIndicatorResult; `values` is planar (output k = values[k*n_rows:(k+1)*n_rows])"""
    _fields_ = [
        ("timestamps", ctypes.POINTER(ctypes.c_int64)),
        ("values", ctypes.POINTER(ctypes.c_double)),
        ("n_rows", ctypes.c_size_t),
        ("n_outputs", ctypes.c_size_t),
    ]


class HOCDBBars(ctypes.Structure):
    """C-compatible HOCDBBars (OHLCV bars produced by hocdb_ohlcv)"""
    _fields_ = [
        ("timestamps", ctypes.POINTER(ctypes.c_int64)),
        ("open", ctypes.POINTER(ctypes.c_double)),
        ("high", ctypes.POINTER(ctypes.c_double)),
        ("low", ctypes.POINTER(ctypes.c_double)),
        ("close", ctypes.POINTER(ctypes.c_double)),
        ("volume", ctypes.POINTER(ctypes.c_double)),
        ("count", ctypes.POINTER(ctypes.c_double)),
        ("n_bars", ctypes.c_size_t),
    ]


class HOCDBBarsEx(ctypes.Structure):
    """C-compatible HOCDBBarsEx: the HOCDBBars layout plus per-bar buy volume at the end
    (NULL when no side field was given); produced by hocdb_ohlcv_ex"""
    _fields_ = HOCDBBars._fields_ + [
        ("buy_volume", ctypes.POINTER(ctypes.c_double)),
    ]


class HOCDBHealth(ctypes.Structure):
    """C-compatible HOCDBHealth (data-quality statistics); decoded by name via the hocdb_health_field_* introspection"""
    _fields_ = [
        ("count", ctypes.c_uint64),
        ("first_ts", ctypes.c_int64),
        ("last_ts", ctypes.c_int64),
        ("span", ctypes.c_int64),
        ("mean_gap", ctypes.c_double),
        ("median_gap", ctypes.c_double),
        ("max_gap", ctypes.c_int64),
        ("max_gap_at", ctypes.c_int64),
        ("n_gaps", ctypes.c_uint64),
        ("n_nonpositive_price", ctypes.c_uint64),
        ("n_nan_price", ctypes.c_uint64),
        ("n_outlier_returns", ctypes.c_uint64),
        ("first_outlier_at", ctypes.c_int64),
        ("max_abs_return", ctypes.c_double),
        ("n_zero_volume", ctypes.c_uint64),
        ("n_negative_volume", ctypes.c_uint64),
        ("closed_span", ctypes.c_int64),
        ("n_session_breaks", ctypes.c_uint64),
        ("n_missing_sessions", ctypes.c_uint64),
    ]


class HOCDBDecision(ctypes.Structure):
    """C-compatible HOCDBDecision: one trading decision for evaluate()"""
    _fields_ = [
        ("timestamp", ctypes.c_int64),
        ("direction", ctypes.c_double),  # +1 long, -1 short, 0 flat (ignored)
        ("size", ctypes.c_double),       # position size in currency units
        ("horizon", ctypes.c_int64),     # timestamp units; 0 = default_horizon
    ]


class HOCDBEvaluation(ctypes.Structure):
    """C-compatible HOCDBEvaluation; decoded by name via the hocdb_evaluation_field_* introspection"""
    _fields_ = [
        ("n_decisions", ctypes.c_uint64),
        ("n_evaluated", ctypes.c_uint64),
        ("n_long", ctypes.c_uint64),
        ("n_short", ctypes.c_uint64),
        ("hit_rate", ctypes.c_double),
        ("avg_return", ctypes.c_double),
        ("avg_net_return", ctypes.c_double),
        ("total_pnl", ctypes.c_double),
        ("total_cost", ctypes.c_double),
        ("sharpe", ctypes.c_double),
        ("profit_factor", ctypes.c_double),
        ("max_drawdown", ctypes.c_double),
        ("avg_win", ctypes.c_double),
        ("avg_loss", ctypes.c_double),
        ("best", ctypes.c_double),
        ("worst", ctypes.c_double),
        ("long_hit_rate", ctypes.c_double),
        ("short_hit_rate", ctypes.c_double),
        ("long_avg_return", ctypes.c_double),
        ("short_avg_return", ctypes.c_double),
    ]


# ---------------------------------------------------------------------------
# Trading calendars, signal backtester and universe features
# ---------------------------------------------------------------------------

class Calendars:
    """Built-in trading calendar ids (HOCDB_CALENDAR_*); the names are accepted wherever an id is"""
    NONE = 0
    CRYPTO = 1   # 24/7, UTC days
    FX = 2       # Sunday 17:00 -> Friday 17:00 New York
    NYSE = 3     # 09:30-16:00 America/New_York, NYSE holidays and early closes
    NASDAQ = 4   # alias of nyse
    LSE = 5      # 08:00-16:30 Europe/London
    CME = 6      # Globex equity-index schedule (approximation)
    NAMES = ("crypto", "fx", "nyse", "nasdaq", "lse", "cme")


class DstRule:
    """Daylight-saving rules of calendar_define() (HOCDB_DST_*); the names are accepted too"""
    NONE = 0
    US = 1   # second Sunday of March -> first Sunday of November
    EU = 2   # last Sunday of March -> last Sunday of October
    NAMES = {"none": NONE, "us": US, "eu": EU}


class HOCDBSession(ctypes.Structure):
    """C-compatible HOCDBSession: one resolved trading session (UTC seconds)"""
    _fields_ = [
        ("open", ctypes.c_int64),         # inclusive
        ("close", ctypes.c_int64),        # exclusive
        ("trade_day", ctypes.c_int64),    # days since 1970-01-01 (local trade date)
        ("early_close", ctypes.c_uint64), # 1 when the session closes early
    ]


class HOCDBDaySession(ctypes.Structure):
    """C-compatible HOCDBDaySession: a weekday's window in local seconds relative to the trade date's midnight"""
    _fields_ = [
        ("open_sec", ctypes.c_int32),   # may be negative (session starts the evening before)
        ("close_sec", ctypes.c_int32),  # close <= open means no session on that weekday
    ]


class HOCDBEarlyClose(ctypes.Structure):
    """C-compatible HOCDBEarlyClose: an early close on one trade date"""
    _fields_ = [
        ("day", ctypes.c_int32),        # days since 1970-01-01 (local)
        ("close_sec", ctypes.c_int32),  # close on that day (local seconds)
    ]


class HOCDBBacktestParams(ctypes.Structure):
    """C-compatible HOCDBBacktestParams (96 bytes); filled from a dict by the backtest calls"""
    _fields_ = [
        ("initial_equity", ctypes.c_double),   # <= 0 -> 1.0
        ("cost_bps", ctypes.c_double),         # per side, on traded notional
        ("slippage_bps", ctypes.c_double),     # adverse price move per side
        ("stop_loss", ctypes.c_double),        # fraction of the entry price, 0 = none
        ("take_profit", ctypes.c_double),      # fraction, 0 = none
        ("trailing_stop", ctypes.c_double),    # fraction from the best price since entry, 0 = none
        ("max_position", ctypes.c_double),     # cap on |units|, 0 = none
        ("position_mode", ctypes.c_uint64),    # 0 units, 1 fraction of equity, 2 notional
        ("fill_mode", ctypes.c_uint64),        # 0 next open, 1 same close
        ("periods_per_year", ctypes.c_double), # 0 = none (a database handle fills it from its calendar)
        ("allow_short", ctypes.c_uint64),      # 0 clamps negative targets to 0
        ("risk_free_rate", ctypes.c_double),   # annual, for sharpe / sortino
    ]


class HOCDBBacktestResult(ctypes.Structure):
    """C-compatible HOCDBBacktestResult (32 fields); decoded by name via the hocdb_backtest_result_field_* introspection"""
    _fields_ = [
        ("n_bars", ctypes.c_uint64),
        ("n_trades", ctypes.c_uint64),
        ("n_long_trades", ctypes.c_uint64),
        ("n_short_trades", ctypes.c_uint64),
        ("final_equity", ctypes.c_double),
        ("total_return", ctypes.c_double),
        ("ann_return", ctypes.c_double),
        ("ann_vol", ctypes.c_double),
        ("sharpe", ctypes.c_double),
        ("sortino", ctypes.c_double),
        ("calmar", ctypes.c_double),
        ("max_drawdown", ctypes.c_double),
        ("max_drawdown_bars", ctypes.c_uint64),
        ("avg_drawdown", ctypes.c_double),
        ("win_rate", ctypes.c_double),
        ("profit_factor", ctypes.c_double),
        ("avg_trade_return", ctypes.c_double),
        ("avg_win", ctypes.c_double),
        ("avg_loss", ctypes.c_double),
        ("best_trade", ctypes.c_double),
        ("worst_trade", ctypes.c_double),
        ("avg_holding_bars", ctypes.c_double),
        ("exposure", ctypes.c_double),
        ("long_share", ctypes.c_double),
        ("turnover", ctypes.c_double),
        ("total_cost", ctypes.c_double),
        ("total_slippage", ctypes.c_double),
        ("n_stop_exits", ctypes.c_uint64),
        ("n_take_profit_exits", ctypes.c_uint64),
        ("n_trailing_exits", ctypes.c_uint64),
        ("gross_pnl", ctypes.c_double),
        ("net_pnl", ctypes.c_double),
    ]


class HOCDBTrade(ctypes.Structure):
    """C-compatible HOCDBTrade (10 fields); decoded by name via the hocdb_trade_field_* introspection"""
    _fields_ = [
        ("entry_ts", ctypes.c_int64),
        ("exit_ts", ctypes.c_int64),       # 0 = still open at the end
        ("direction", ctypes.c_int64),     # +1 long, -1 short
        ("entry_price", ctypes.c_double),
        ("exit_price", ctypes.c_double),
        ("size", ctypes.c_double),
        ("pnl", ctypes.c_double),
        ("ret", ctypes.c_double),
        ("bars", ctypes.c_uint64),
        ("exit_reason", ctypes.c_uint64),  # 0 signal, 1 stop_loss, 2 take_profit, 3 trailing, 4 end of data
    ]


class HOCDBBacktestOutputs(ctypes.Structure):
    """C-compatible HOCDBBacktestOutputs: optional per-bar output buffers (each NULL or n entries)"""
    _fields_ = [(name, ctypes.POINTER(ctypes.c_double)) for name in ("equity", "position", "cash", "pnl", "drawdown")]


class HOCDBSplit(ctypes.Structure):
    """C-compatible HOCDBSplit: a walk-forward train / test index range (ends exclusive)"""
    _fields_ = [
        ("train_start", ctypes.c_uint64),
        ("train_end", ctypes.c_uint64),
        ("test_start", ctypes.c_uint64),
        ("test_end", ctypes.c_uint64),
    ]


class HOCDBUniverseParams(ctypes.Structure):
    """C-compatible HOCDBUniverseParams (72 bytes); filled from a dict by the universe calls"""
    _fields_ = [
        ("mom_short", ctypes.c_uint64),        # default 5 bars
        ("mom_mid", ctypes.c_uint64),          # 20
        ("mom_long", ctypes.c_uint64),         # 60
        ("vol_period", ctypes.c_uint64),       # 20
        ("corr_period", ctypes.c_uint64),      # 60
        ("sma_period", ctypes.c_uint64),       # 50
        ("beta_period", ctypes.c_uint64),      # 60
        ("periods_per_year", ctypes.c_double), # 0 = no annualisation of vol
        ("weights_mode", ctypes.c_uint64),     # 0 equal-weight market, 1 volume-weighted
    ]


class HOCDBUniverseRow(ctypes.Structure):
    """C-compatible HOCDBUniverseRow (21 fields, one per ticker); decoded via hocdb_universe_row_field_*"""
    _fields_ = [
        ("last_close", ctypes.c_double),
        ("ret_1", ctypes.c_double),
        ("mom_short", ctypes.c_double),
        ("mom_mid", ctypes.c_double),
        ("mom_long", ctypes.c_double),
        ("vol", ctypes.c_double),
        ("sma_distance", ctypes.c_double),
        ("beta", ctypes.c_double),
        ("corr_market", ctypes.c_double),
        ("rel_strength", ctypes.c_double),
        ("rank_mom_short", ctypes.c_double),
        ("rank_mom_mid", ctypes.c_double),
        ("rank_mom_long", ctypes.c_double),
        ("rank_vol", ctypes.c_double),
        ("rank_rel_strength", ctypes.c_double),
        ("z_mom_mid", ctypes.c_double),
        ("avg_corr", ctypes.c_double),
        ("max_corr", ctypes.c_double),
        ("max_corr_index", ctypes.c_uint64),   # index of the most correlated other ticker
        ("idio_vol", ctypes.c_double),
        ("volume_ratio", ctypes.c_double),
    ]


class HOCDBUniverseSummary(ctypes.Structure):
    """C-compatible HOCDBUniverseSummary (16 fields); decoded via hocdb_universe_summary_field_*"""
    _fields_ = [
        ("n_tickers", ctypes.c_uint64),
        ("n_bars", ctypes.c_uint64),           # joined bars actually used
        ("market_ret_1", ctypes.c_double),
        ("market_mom_short", ctypes.c_double),
        ("market_mom_mid", ctypes.c_double),
        ("market_mom_long", ctypes.c_double),
        ("market_vol", ctypes.c_double),
        ("dispersion", ctypes.c_double),
        ("dispersion_mid", ctypes.c_double),
        ("breadth_sma", ctypes.c_double),
        ("breadth_up", ctypes.c_double),
        ("avg_pair_corr", ctypes.c_double),
        ("max_pair_corr", ctypes.c_double),
        ("min_pair_corr", ctypes.c_double),
        ("first_ts", ctypes.c_int64),
        ("last_ts", ctypes.c_int64),
    ]


_BACKTEST_PARAM_KEYS = ("initial_equity", "cost_bps", "slippage_bps", "stop_loss", "take_profit", "trailing_stop",
                        "max_position", "position_mode", "fill_mode", "periods_per_year", "allow_short",
                        "risk_free_rate")
_POSITION_MODES = {"units": 0, "fraction": 1, "notional": 2}
_FILL_MODES = {"next_open": 0, "same_close": 1}
_BACKTEST_OUTPUT_NAMES = ("equity", "position", "cash", "pnl", "drawdown")
_SPLIT_KEYS = ("train_start", "train_end", "test_start", "test_end")
_UNIVERSE_PARAM_KEYS = ("mom_short", "mom_mid", "mom_long", "vol_period", "corr_period", "sma_period",
                        "beta_period", "periods_per_year", "weights_mode")
_WEIGHTS_MODES = {"equal": 0, "volume": 1}
_SESSION_WHICH = {"at": 0, "prev": 1, "previous": 1, "next": 2}

HOCDB_LOOKBACK_AUTO = ctypes.c_size_t(-1).value  # SIZE_MAX: recommended per-spec warm-up

_COLUMN_ROLES = ("open", "high", "low", "close", "volume", "bid", "ask", "side")
_SPEC_KEYS = ("kind", "period", "period2", "period3", "period4", "param", "param2", "field", "field2", "label")
_DECISION_KEYS = ("timestamp", "direction", "size", "horizon")
_STRUCT_FIELD_CTYPES = {1: ctypes.c_int64, 2: ctypes.c_double, 3: ctypes.c_uint64}
_INDICATOR_ERROR_MESSAGES = {
    -1: "out of memory",
    -2: "invalid parameter (unknown indicator kind, bad period / param, or invalid backtest / universe params)",
    -3: "missing column: the indicator needs open/high/low/volume data that is not available (check `columns`)",
    -4: "invalid field index",
    -5: "per-spec 'field' overrides are not supported when bucket > 0",
    -6: "too many columns",
    -7: "series length mismatch (backtest: len(target) must equal the number of rows of the window; "
        "arrays: every series must have the same length)",
    -30: "CalendarRequired: a session kind with param 0 (calendar sessions) needs a handle with a trading "
         "calendar and a timestamp unit (HOCDB(..., calendar='nyse', timestamp_unit_ns=1000) or set_calendar() + "
         "set_timestamp_unit()); otherwise pass param = session length in timestamp units",
    -31: "UnknownCalendar: no calendar with that id (built-in ids 1-6: crypto, fx, nyse, nasdaq, lse, cme; "
         "calendar_define() returns the ids of custom calendars)",
}

_registry_lib_cache = None


def _find_library_path() -> Optional[str]:
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


def _define_indicator_signatures(lib):
    """Define argument and return types for the indicator / analytics C functions"""
    columns_p = ctypes.POINTER(HOCDBIndicatorColumns)
    spec_p = ctypes.POINTER(HOCDBIndicatorSpec)
    result_p = ctypes.POINTER(HOCDBIndicatorResult)

    # hocdb_indicators(handle, start_ts, end_ts, cols, specs, n_specs, lookback, bucket, out)
    lib.hocdb_indicators.argtypes = [
        ctypes.c_void_p, ctypes.c_longlong, ctypes.c_longlong, columns_p, spec_p,
        ctypes.c_size_t, ctypes.c_size_t, ctypes.c_longlong, result_p
    ]
    lib.hocdb_indicators.restype = ctypes.c_int

    # hocdb_indicators_tail(handle, n_last, cols, specs, n_specs, lookback, bucket, out)
    lib.hocdb_indicators_tail.argtypes = [
        ctypes.c_void_p, ctypes.c_size_t, columns_p, spec_p,
        ctypes.c_size_t, ctypes.c_size_t, ctypes.c_longlong, result_p
    ]
    lib.hocdb_indicators_tail.restype = ctypes.c_int

    lib.hocdb_indicators_free.argtypes = [result_p]
    lib.hocdb_indicators_free.restype = None

    # registry helpers
    lib.hocdb_indicator_output_count.argtypes = [ctypes.c_uint32]
    lib.hocdb_indicator_output_count.restype = ctypes.c_size_t
    lib.hocdb_indicator_output_name.argtypes = [ctypes.c_uint32, ctypes.c_size_t]
    lib.hocdb_indicator_output_name.restype = ctypes.c_char_p
    lib.hocdb_indicator_name.argtypes = [ctypes.c_uint32]
    lib.hocdb_indicator_name.restype = ctypes.c_char_p
    lib.hocdb_indicator_kind_from_name.argtypes = [ctypes.c_char_p]
    lib.hocdb_indicator_kind_from_name.restype = ctypes.c_uint32
    lib.hocdb_indicator_kinds.argtypes = [ctypes.POINTER(ctypes.c_uint32), ctypes.c_size_t]
    lib.hocdb_indicator_kinds.restype = ctypes.c_size_t
    lib.hocdb_indicator_warmup.argtypes = [spec_p]
    lib.hocdb_indicator_warmup.restype = ctypes.c_size_t
    lib.hocdb_indicator_is_lookahead.argtypes = [ctypes.c_uint32]
    lib.hocdb_indicator_is_lookahead.restype = ctypes.c_int

    # hocdb_pair_indicators(a, cols_a, b, cols_b, start_ts, end_ts, specs, n_specs, lookback, bucket, out)
    lib.hocdb_pair_indicators.argtypes = [
        ctypes.c_void_p, columns_p, ctypes.c_void_p, columns_p, ctypes.c_longlong, ctypes.c_longlong,
        spec_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_longlong, result_p
    ]
    lib.hocdb_pair_indicators.restype = ctypes.c_int

    # hocdb_pair_indicators_tail(a, cols_a, b, cols_b, n_last, specs, n_specs, lookback, bucket, out)
    lib.hocdb_pair_indicators_tail.argtypes = [
        ctypes.c_void_p, columns_p, ctypes.c_void_p, columns_p, ctypes.c_size_t,
        spec_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_longlong, result_p
    ]
    lib.hocdb_pair_indicators_tail.restype = ctypes.c_int

    # hocdb_ohlcv(handle, start_ts, end_ts, price_field, volume_field, bucket, out)
    lib.hocdb_ohlcv.argtypes = [
        ctypes.c_void_p, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_size_t,
        ctypes.c_longlong, ctypes.c_longlong, ctypes.POINTER(HOCDBBars)
    ]
    lib.hocdb_ohlcv.restype = ctypes.c_int
    lib.hocdb_ohlcv_free.argtypes = [ctypes.POINTER(HOCDBBars)]
    lib.hocdb_ohlcv_free.restype = None

    # hocdb_ohlcv_ex(handle, start_ts, end_ts, price_field, volume_field, side_field, bucket, out)
    lib.hocdb_ohlcv_ex.argtypes = [
        ctypes.c_void_p, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_size_t,
        ctypes.c_longlong, ctypes.c_longlong, ctypes.c_longlong, ctypes.POINTER(HOCDBBarsEx)
    ]
    lib.hocdb_ohlcv_ex.restype = ctypes.c_int
    lib.hocdb_ohlcv_ex_free.argtypes = [ctypes.POINTER(HOCDBBarsEx)]
    lib.hocdb_ohlcv_ex_free.restype = None

    # hocdb_health(handle, start_ts, end_ts, price_field, volume_field, gap_threshold, outlier_threshold, out)
    lib.hocdb_health.argtypes = [
        ctypes.c_void_p, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_size_t, ctypes.c_longlong,
        ctypes.c_longlong, ctypes.c_double, ctypes.POINTER(HOCDBHealth)
    ]
    lib.hocdb_health.restype = ctypes.c_int

    # hocdb_evaluate(handle, price_field, decisions, n, default_horizon, cost_bps, out, out_entry, out_exit, out_net)
    double_p = ctypes.POINTER(ctypes.c_double)
    lib.hocdb_evaluate.argtypes = [
        ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(HOCDBDecision), ctypes.c_size_t, ctypes.c_longlong,
        ctypes.c_double, ctypes.POINTER(HOCDBEvaluation), double_p, double_p, double_p
    ]
    lib.hocdb_evaluate.restype = ctypes.c_int
    lib.hocdb_decision_size.argtypes = []
    lib.hocdb_decision_size.restype = ctypes.c_size_t

    # hocdb_summary(handle, start_ts, end_ts, field_index, periods_per_year, out)
    lib.hocdb_summary.argtypes = [
        ctypes.c_void_p, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_size_t,
        ctypes.c_double, ctypes.c_void_p
    ]
    lib.hocdb_summary.restype = ctypes.c_int

    # hocdb_snapshot(handle, cols, n_bars, bucket, periods_per_year, out)
    lib.hocdb_snapshot.argtypes = [
        ctypes.c_void_p, columns_p, ctypes.c_size_t, ctypes.c_longlong,
        ctypes.c_double, ctypes.c_void_p
    ]
    lib.hocdb_snapshot.restype = ctypes.c_int

    # hocdb_snapshot_multi(handle, cols, n_bars, buckets, n_buckets, periods_per_year, out[n_buckets])
    lib.hocdb_snapshot_multi.argtypes = [
        ctypes.c_void_p, columns_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_int64), ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_double), ctypes.c_void_p
    ]
    lib.hocdb_snapshot_multi.restype = ctypes.c_int

    # struct introspection: hocdb_{summary,snapshot,health,evaluation}_{size,field_count,field_name,field_offset,field_type}
    _define_struct_introspection(lib, ("summary", "snapshot", "health", "evaluation"))


def _define_struct_introspection(lib, prefixes):
    """Signatures of hocdb_<prefix>_{size,field_count,field_name,field_offset,field_type} for each prefix"""
    for prefix in prefixes:
        size_fn = getattr(lib, f"hocdb_{prefix}_size")
        size_fn.argtypes = []
        size_fn.restype = ctypes.c_size_t
        count_fn = getattr(lib, f"hocdb_{prefix}_field_count")
        count_fn.argtypes = []
        count_fn.restype = ctypes.c_size_t
        name_fn = getattr(lib, f"hocdb_{prefix}_field_name")
        name_fn.argtypes = [ctypes.c_size_t]
        name_fn.restype = ctypes.c_char_p
        offset_fn = getattr(lib, f"hocdb_{prefix}_field_offset")
        offset_fn.argtypes = [ctypes.c_size_t]
        offset_fn.restype = ctypes.c_size_t
        type_fn = getattr(lib, f"hocdb_{prefix}_field_type")
        type_fn.argtypes = [ctypes.c_size_t]
        type_fn.restype = ctypes.c_int


def _define_storage_signatures(lib):
    """Define argument and return types of the durability / reader / maintenance / metrics C functions"""
    # hocdb_init_ex(ticker, path, schema, schema_len, config)
    lib.hocdb_init_ex.argtypes = [
        ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CField), ctypes.c_size_t, ctypes.POINTER(HOCDBConfig)
    ]
    lib.hocdb_init_ex.restype = ctypes.c_void_p

    # hocdb_open_reader(ticker, path, schema, schema_len)
    lib.hocdb_open_reader.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CField), ctypes.c_size_t]
    lib.hocdb_open_reader.restype = ctypes.c_void_p

    lib.hocdb_last_error.argtypes = []
    lib.hocdb_last_error.restype = ctypes.c_char_p
    lib.hocdb_header_size.argtypes = []
    lib.hocdb_header_size.restype = ctypes.c_size_t
    lib.hocdb_format_version.argtypes = [ctypes.c_void_p]
    lib.hocdb_format_version.restype = ctypes.c_int
    lib.hocdb_is_read_only.argtypes = [ctypes.c_void_p]
    lib.hocdb_is_read_only.restype = ctypes.c_int

    for name in ("hocdb_sync", "hocdb_refresh", "hocdb_verify"):
        fn = getattr(lib, name)
        fn.argtypes = [ctypes.c_void_p]
        fn.restype = ctypes.c_int

    # hocdb_compact(handle, min_ts) / hocdb_retain_last(handle, n) / hocdb_rollover(handle, out_path, cap)
    lib.hocdb_compact.argtypes = [ctypes.c_void_p, ctypes.c_longlong]
    lib.hocdb_compact.restype = ctypes.c_int
    lib.hocdb_retain_last.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.hocdb_retain_last.restype = ctypes.c_int
    lib.hocdb_rollover.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    lib.hocdb_rollover.restype = ctypes.c_int

    # hocdb_metrics(handle, out) / hocdb_metrics_reset(handle) + hocdb_metrics_{size,field_*} introspection
    lib.hocdb_metrics.argtypes = [ctypes.c_void_p, ctypes.POINTER(HOCDBMetrics)]
    lib.hocdb_metrics.restype = ctypes.c_int
    lib.hocdb_metrics_reset.argtypes = [ctypes.c_void_p]
    lib.hocdb_metrics_reset.restype = None
    _define_struct_introspection(lib, ("metrics",))


def _define_calendar_signatures(lib):
    """Define argument and return types of the trading-calendar C functions"""
    session_p = ctypes.POINTER(HOCDBSession)
    lib.hocdb_calendar_id.argtypes = [ctypes.c_char_p]
    lib.hocdb_calendar_id.restype = ctypes.c_uint32
    lib.hocdb_calendar_name.argtypes = [ctypes.c_uint32, ctypes.c_char_p, ctypes.c_size_t]
    lib.hocdb_calendar_name.restype = ctypes.c_int
    lib.hocdb_calendar_session.argtypes = [ctypes.c_uint32, ctypes.c_int64, ctypes.c_int, session_p]
    lib.hocdb_calendar_session.restype = ctypes.c_int
    lib.hocdb_calendar_session_for_day.argtypes = [ctypes.c_uint32, ctypes.c_int64, session_p]
    lib.hocdb_calendar_session_for_day.restype = ctypes.c_int
    lib.hocdb_calendar_is_open.argtypes = [ctypes.c_uint32, ctypes.c_int64]
    lib.hocdb_calendar_is_open.restype = ctypes.c_int
    for name in ("hocdb_calendar_open_seconds", "hocdb_calendar_sessions_between"):
        fn = getattr(lib, name)
        fn.argtypes = [ctypes.c_uint32, ctypes.c_int64, ctypes.c_int64]
        fn.restype = ctypes.c_int64
    lib.hocdb_calendar_periods_per_year.argtypes = [ctypes.c_uint32, ctypes.c_double]
    lib.hocdb_calendar_periods_per_year.restype = ctypes.c_double
    lib.hocdb_calendar_to_local.argtypes = [ctypes.c_uint32, ctypes.c_int64]
    lib.hocdb_calendar_to_local.restype = ctypes.c_int64
    lib.hocdb_days_from_civil.argtypes = [ctypes.c_int64, ctypes.c_uint32, ctypes.c_uint32]
    lib.hocdb_days_from_civil.restype = ctypes.c_int64
    lib.hocdb_civil_from_days.argtypes = [ctypes.c_int64, ctypes.POINTER(ctypes.c_int64),
                                          ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_uint32)]
    lib.hocdb_civil_from_days.restype = None
    # hocdb_calendar_define(name, weekly[7], utc_offset_sec, dst_rule, holidays, n_holidays, early, n_early, sessions_per_year)
    lib.hocdb_calendar_define.argtypes = [
        ctypes.c_char_p, ctypes.POINTER(HOCDBDaySession), ctypes.c_int32, ctypes.c_int, ctypes.POINTER(ctypes.c_int32),
        ctypes.c_size_t, ctypes.POINTER(HOCDBEarlyClose), ctypes.c_size_t, ctypes.c_double
    ]
    lib.hocdb_calendar_define.restype = ctypes.c_int64
    # per-handle calendar and timestamp unit
    lib.hocdb_set_calendar.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    lib.hocdb_set_calendar.restype = ctypes.c_int
    lib.hocdb_get_calendar.argtypes = [ctypes.c_void_p]
    lib.hocdb_get_calendar.restype = ctypes.c_uint32
    lib.hocdb_set_timestamp_unit.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.hocdb_set_timestamp_unit.restype = ctypes.c_int
    lib.hocdb_get_timestamp_unit.argtypes = [ctypes.c_void_p]
    lib.hocdb_get_timestamp_unit.restype = ctypes.c_uint64
    lib.hocdb_periods_per_year.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    lib.hocdb_periods_per_year.restype = ctypes.c_double


def _define_backtest_signatures(lib):
    """Define argument and return types of the signal-backtester C functions"""
    dbl_p = ctypes.POINTER(ctypes.c_double)
    i64_p = ctypes.POINTER(ctypes.c_int64)
    cols_p = ctypes.POINTER(HOCDBIndicatorColumns)
    params_p = ctypes.POINTER(HOCDBBacktestParams)
    outputs_p = ctypes.POINTER(HOCDBBacktestOutputs)
    trades_p = ctypes.POINTER(HOCDBTrade)
    result_p = ctypes.POINTER(HOCDBBacktestResult)
    split_p = ctypes.POINTER(HOCDBSplit)
    lib.hocdb_backtest_params_default.argtypes = [params_p]
    lib.hocdb_backtest_params_default.restype = None
    # hocdb_backtest(handle, cols, start_ts, end_ts, bucket, target, n, params, outputs, trades, trades_cap, out)
    lib.hocdb_backtest.argtypes = [
        ctypes.c_void_p, cols_p, ctypes.c_int64, ctypes.c_int64, ctypes.c_int64, dbl_p, ctypes.c_size_t,
        params_p, outputs_p, trades_p, ctypes.c_size_t, result_p
    ]
    lib.hocdb_backtest.restype = ctypes.c_int
    # hocdb_backtest_tail(handle, cols, bucket, target, n, params, outputs, trades, trades_cap, out)
    lib.hocdb_backtest_tail.argtypes = [
        ctypes.c_void_p, cols_p, ctypes.c_int64, dbl_p, ctypes.c_size_t, params_p, outputs_p, trades_p,
        ctypes.c_size_t, result_p
    ]
    lib.hocdb_backtest_tail.restype = ctypes.c_int
    # hocdb_backtest_arrays(ts, open, high, low, close, n, target, params, outputs, trades, trades_cap, out)
    lib.hocdb_backtest_arrays.argtypes = [
        i64_p, dbl_p, dbl_p, dbl_p, dbl_p, ctypes.c_size_t, dbl_p, params_p, outputs_p, trades_p,
        ctypes.c_size_t, result_p
    ]
    lib.hocdb_backtest_arrays.restype = ctypes.c_int
    # hocdb_walk_forward_splits(n, n_splits, train_frac, anchored, out, cap)
    lib.hocdb_walk_forward_splits.argtypes = [ctypes.c_size_t, ctypes.c_size_t, ctypes.c_double, ctypes.c_int,
                                              split_p, ctypes.c_size_t]
    lib.hocdb_walk_forward_splits.restype = ctypes.c_size_t
    # hocdb_backtest_splits_arrays(ts, open, high, low, close, n, target, params, splits, n_splits, results)
    lib.hocdb_backtest_splits_arrays.argtypes = [
        i64_p, dbl_p, dbl_p, dbl_p, dbl_p, ctypes.c_size_t, dbl_p, params_p, split_p, ctypes.c_size_t, result_p
    ]
    lib.hocdb_backtest_splits_arrays.restype = ctypes.c_int
    lib.hocdb_backtest_params_size.argtypes = []
    lib.hocdb_backtest_params_size.restype = ctypes.c_size_t
    _define_struct_introspection(lib, ("backtest_result", "trade"))


def _define_universe_signatures(lib):
    """Define argument and return types of the universe (cross-sectional) C functions"""
    dbl_p = ctypes.POINTER(ctypes.c_double)
    dbl_pp = ctypes.POINTER(dbl_p)
    params_p = ctypes.POINTER(HOCDBUniverseParams)
    rows_p = ctypes.POINTER(HOCDBUniverseRow)
    summary_p = ctypes.POINTER(HOCDBUniverseSummary)
    lib.hocdb_universe_params_default.argtypes = [params_p]
    lib.hocdb_universe_params_default.restype = None
    # hocdb_universe(handles, n, cols, n_bars, bucket, params, rows, corr, out)
    lib.hocdb_universe.argtypes = [
        ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t, ctypes.POINTER(HOCDBIndicatorColumns), ctypes.c_size_t,
        ctypes.c_int64, params_p, rows_p, dbl_p, summary_p
    ]
    lib.hocdb_universe.restype = ctypes.c_int
    # hocdb_universe_arrays(closes, volumes, n_tickers, n_bars, ts, params, rows, corr, out)
    lib.hocdb_universe_arrays.argtypes = [
        dbl_pp, dbl_pp, ctypes.c_size_t, ctypes.c_size_t, ctypes.POINTER(ctypes.c_int64), params_p, rows_p, dbl_p,
        summary_p
    ]
    lib.hocdb_universe_arrays.restype = ctypes.c_int
    lib.hocdb_universe_params_size.argtypes = []
    lib.hocdb_universe_params_size.restype = ctypes.c_size_t
    _define_struct_introspection(lib, ("universe_row", "universe_summary"))


def _registry_lib():
    """Library handle for the module-level helpers (loaded once, lazily)"""
    global _registry_lib_cache
    if _registry_lib_cache is None:
        lib_path = _find_library_path()
        if not lib_path:
            raise RuntimeError("HOCDB C library not found. Please build with 'zig build c-bindings'")
        lib = ctypes.CDLL(lib_path)
        _define_indicator_signatures(lib)
        _define_storage_signatures(lib)
        _define_calendar_signatures(lib)
        _define_backtest_signatures(lib)
        _define_universe_signatures(lib)
        _registry_lib_cache = lib
    return _registry_lib_cache


def _last_error(lib) -> str:
    """Name of the error of the last failed open on this thread (hocdb_last_error), "" if none"""
    name = lib.hocdb_last_error()
    return name.decode('utf-8') if name else ""


def _raise_storage_error(rc: int, what: str):
    """Map a C error code of the storage API (-10 read-only, -11 locked, -12 checksum, -20 unavailable) to an exception"""
    msg = _STORAGE_ERROR_MESSAGES.get(rc, f"error code {rc}")
    raise RuntimeError(f"{what} failed: {msg}")


def _fsync_policy(fsync) -> int:
    """Resolve HOCDB(fsync=...) given as a name ("on_flush") or an int (FsyncPolicy.ON_FLUSH)"""
    if isinstance(fsync, str):
        key = fsync.strip().lower().replace("-", "_").replace(" ", "_")
        if key in FsyncPolicy.NAMES:
            return FsyncPolicy.NAMES[key]
        raise ValueError(f"Unknown fsync policy {fsync!r}; expected one of {', '.join(FsyncPolicy.NAMES)} or an int 0-3")
    if isinstance(fsync, int) and not isinstance(fsync, bool) and 0 <= fsync <= 3:
        return fsync
    raise ValueError(f"fsync must be one of {', '.join(FsyncPolicy.NAMES)} or an int 0-3, got {fsync!r}")


def _non_negative_int(name: str, value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative int, got {value!r}")
    return value


def header_size() -> int:
    """Bytes reserved by the file header of newly created files (64); also HOCDB.header_size()"""
    return _registry_lib().hocdb_header_size()


def last_error() -> str:
    """Error name of the last failed open on this thread ("DatabaseLocked", "SchemaMismatch", ...), "" if none"""
    return _last_error(_registry_lib())


def _raise_indicator_error(rc: int, what: str):
    """Map a C error code of the indicator API to an exception with a readable message"""
    msg = _INDICATOR_ERROR_MESSAGES.get(rc, f"error code {rc}")
    if rc == -1:
        raise MemoryError(f"{what} failed: {msg}")
    if rc in _INDICATOR_ERROR_MESSAGES:
        raise ValueError(f"{what} failed: {msg}")
    raise RuntimeError(f"{what} failed: {msg}")


def _resolve_lookback(lookback) -> int:
    if lookback is None or lookback == "auto":
        return HOCDB_LOOKBACK_AUTO
    if isinstance(lookback, int) and not isinstance(lookback, bool) and lookback >= 0:
        return lookback
    raise ValueError(f"lookback must be 'auto' or a non-negative int, got {lookback!r}")


def _resolve_kind(lib, kind) -> int:
    """Resolve an indicator kind given as a name ("rsi") or an id (IndicatorKinds.RSI)"""
    if isinstance(kind, str):
        kind_id = lib.hocdb_indicator_kind_from_name(kind.encode('utf-8'))
        if kind_id == 0:
            raise ValueError(f"Unknown indicator kind: {kind!r}")
        return kind_id
    if isinstance(kind, int) and not isinstance(kind, bool):
        if kind <= 0 or kind > 0xFFFFFFFF or lib.hocdb_indicator_name(kind) is None:
            raise ValueError(f"Unknown indicator kind id: {kind}")
        return kind
    raise ValueError(f"Indicator kind must be a name or an id, got {type(kind)}")


def _spec_to_c(lib, spec: dict, resolve_field) -> HOCDBIndicatorSpec:
    """Convert a spec dict ({"kind": "rsi", "period": 14, ...}) into a HOCDBIndicatorSpec"""
    if not isinstance(spec, dict):
        raise ValueError("Each indicator spec must be a dict, e.g. {'kind': 'rsi', 'period': 14}")
    if "kind" not in spec:
        raise ValueError("Indicator spec is missing 'kind'")
    for key in spec:
        if key not in _SPEC_KEYS:
            raise ValueError(f"Unknown indicator spec key {key!r}; expected one of {', '.join(_SPEC_KEYS)}")
    c_spec = HOCDBIndicatorSpec()
    c_spec.kind = _resolve_kind(lib, spec["kind"])
    for key in ("period", "period2", "period3", "period4"):
        value = spec.get(key) or 0
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"Indicator spec {key!r} must be a non-negative int, got {value!r}")
        setattr(c_spec, key, value)
    c_spec.param = float(spec.get("param") or 0.0)
    c_spec.param2 = float(spec.get("param2") or 0.0)
    field = spec.get("field")
    field2 = spec.get("field2")
    c_spec.field_index = -1 if field is None else resolve_field(field)
    c_spec.field_index2 = -1 if field2 is None else resolve_field(field2)
    return c_spec


def _output_names(lib, kind_id: int) -> list:
    count = lib.hocdb_indicator_output_count(kind_id)
    return [lib.hocdb_indicator_output_name(kind_id, i).decode('utf-8') for i in range(count)]


def _spec_label(lib, spec: dict, kind_id: int) -> str:
    """Column label of a spec: its 'label', else '<kind>' or '<kind>_<period>'"""
    label = spec.get("label")
    if label is not None:
        if not isinstance(label, str) or not label:
            raise ValueError(f"Indicator spec 'label' must be a non-empty string, got {label!r}")
        return label
    name = lib.hocdb_indicator_name(kind_id).decode('utf-8')
    period = spec.get("period")
    if period:
        return f"{name}_{period}"
    return name


def _column_names(lib, specs: list, c_specs) -> list:
    """Result column names in output order (multi-output kinds append '_<output>', except the
    output named like the kind itself, which keeps the bare label)"""
    names = []
    for spec, c_spec in zip(specs, c_specs):
        label = _spec_label(lib, spec, c_spec.kind)
        outputs = _output_names(lib, c_spec.kind)
        if len(outputs) == 1:
            names.append(label)
        else:
            # the output named like the kind keeps the bare label (macd, macd_signal, macd_hist)
            kind_name = lib.hocdb_indicator_name(c_spec.kind).decode('utf-8')
            names.extend(label if output == kind_name else f"{label}_{output}" for output in outputs)
    seen = set()
    for name in names:
        if name in seen:
            raise ValueError(f"Duplicate result column {name!r}; give one of the specs a distinct 'label'")
        seen.add(name)
    return names


def _copy_c_array(ptr, n: int, use_numpy: bool):
    """Copy n elements out of a C buffer into a list (or a numpy array)"""
    if use_numpy:
        if n == 0 or not ptr:
            return _np.zeros(0, dtype=_np.dtype(ptr._type_))
        return _np.ctypeslib.as_array(ptr, shape=(n,)).copy()
    if n == 0 or not ptr:
        return []
    return ptr[:n]


def _decode_struct(lib, prefix: str, buf, base: int = 0) -> dict:
    """Decode a HOCDBSummary / HOCDBSnapshot / HOCDBHealth / HOCDBEvaluation / HOCDBMetrics / HOCDBBacktestResult /
    HOCDBTrade / HOCDBUniverseRow / HOCDBUniverseSummary buffer (a ctypes Structure, a ctypes array of them or a
    string buffer; `base` = byte offset of the struct inside the buffer) into a dict using the
    hocdb_<prefix>_field_* introspection, so field names and types always come from the loaded library"""
    field_count = getattr(lib, f"hocdb_{prefix}_field_count")()
    field_name = getattr(lib, f"hocdb_{prefix}_field_name")
    field_offset = getattr(lib, f"hocdb_{prefix}_field_offset")
    field_type = getattr(lib, f"hocdb_{prefix}_field_type")
    out = {}
    for i in range(field_count):
        ctype = _STRUCT_FIELD_CTYPES.get(field_type(i))
        if ctype is None:
            raise RuntimeError(f"Unknown {prefix} field type {field_type(i)} for field {i}")
        out[field_name(i).decode('utf-8')] = ctype.from_buffer(buf, base + field_offset(i)).value
    return out


def _check_struct_size(lib, prefix: str, cls):
    """Guard against ABI drift: the binding's ctypes Structure must be exactly as large as hocdb_<prefix>_size()"""
    expected = getattr(lib, f"hocdb_{prefix}_size")()
    if ctypes.sizeof(cls) != expected:
        raise RuntimeError(f"HOCDB C library ABI mismatch: {cls.__name__} is {expected} bytes in the library but "
                           f"{ctypes.sizeof(cls)} bytes in this binding (rebuild the library or update the binding)")


def _new_struct(lib, prefix: str, cls):
    """A zeroed output struct of the given class, after checking its size against the library"""
    _check_struct_size(lib, prefix, cls)
    return cls()


def _check_indicator_window(start_ts, end_ts, tail, bucket):
    """Validate the start_ts/end_ts-or-tail window and the bucket shared by indicators() and pair_indicators()"""
    has_range = start_ts is not None or end_ts is not None
    if has_range and (start_ts is None or end_ts is None):
        raise ValueError("start_ts and end_ts must be given together")
    if has_range == (tail is not None):
        raise ValueError("Pass either start_ts/end_ts or tail (exactly one of them)")
    if tail is not None and (isinstance(tail, bool) or not isinstance(tail, int) or tail < 0):
        raise ValueError("tail must be a non-negative int")
    if isinstance(bucket, bool) or not isinstance(bucket, int) or bucket < 0:
        raise ValueError("bucket must be a non-negative int (0 = one row per record)")


def _decision_to_c(decision) -> HOCDBDecision:
    """Convert a decision dict ({"timestamp": ts, "direction": 1, "size": 1000, "horizon": 60}) into a HOCDBDecision"""
    if not isinstance(decision, dict):
        raise ValueError("Each decision must be a dict, e.g. {'timestamp': 1620000000, 'direction': 1, "
                         "'size': 1000, 'horizon': 3600}")
    for key in decision:
        if key not in _DECISION_KEYS:
            raise ValueError(f"Unknown decision key {key!r}; expected one of {', '.join(_DECISION_KEYS)}")
    for key in ("timestamp", "direction"):
        if key not in decision:
            raise ValueError(f"Decision is missing {key!r}")
    timestamp = decision["timestamp"]
    if isinstance(timestamp, bool) or not isinstance(timestamp, int):
        raise ValueError(f"Decision 'timestamp' must be an int, got {timestamp!r}")
    horizon = decision.get("horizon") or 0
    if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 0:
        raise ValueError(f"Decision 'horizon' must be a non-negative int (timestamp units), got {horizon!r}")
    size = decision.get("size")
    try:
        direction = float(decision["direction"])
        size = 1.0 if size is None else float(size)
    except (TypeError, ValueError):
        raise ValueError(f"Decision 'direction' and 'size' must be numbers, got {decision!r}") from None
    return HOCDBDecision(timestamp, direction, size, horizon)


def _indicator_kinds(lib) -> list:
    total = lib.hocdb_indicator_kinds(None, 0)
    ids = (ctypes.c_uint32 * max(total, 1))()
    lib.hocdb_indicator_kinds(ids, total)
    return [lib.hocdb_indicator_name(ids[i]).decode('utf-8') for i in range(total)]


def _indicator_outputs(lib, kind) -> list:
    return _output_names(lib, _resolve_kind(lib, kind))


def _indicator_warmup(lib, spec: dict, resolve_field) -> int:
    c_spec = _spec_to_c(lib, spec, resolve_field)
    return lib.hocdb_indicator_warmup(ctypes.byref(c_spec))


def _indicator_is_lookahead(lib, kind) -> bool:
    return bool(lib.hocdb_indicator_is_lookahead(_resolve_kind(lib, kind)))


def _warmup_field_resolver(field):
    # The warm-up does not depend on the field: accept indices, ignore names (no schema at module level)
    return field if isinstance(field, int) and not isinstance(field, bool) else -1


def indicator_kinds() -> list:
    """Names of all indicator kinds supported by the C library (e.g. ["sma", "ema", ..., "heikin_ashi"])"""
    return _indicator_kinds(_registry_lib())


def indicator_outputs(kind) -> list:
    """Output names of an indicator kind (name or id), e.g. indicator_outputs("macd") -> ["macd", "signal", "hist"]"""
    return _indicator_outputs(_registry_lib(), kind)


def indicator_warmup(spec: dict) -> int:
    """Recommended warm-up rows for a spec dict (after applying defaults), e.g. {"kind": "ema", "period": 200}"""
    return _indicator_warmup(_registry_lib(), spec, _warmup_field_resolver)


def indicator_is_lookahead(kind) -> bool:
    """True when an indicator kind (name or id) uses future rows: the labels forward_return / triple_barrier.
    Such columns must never be fed to a model as features for the same row (look-ahead bias)."""
    return _indicator_is_lookahead(_registry_lib(), kind)


# ---------------------------------------------------------------------------
# Trading calendars (module-level; all times are UTC seconds)
# ---------------------------------------------------------------------------

def _resolve_calendar(lib, calendar) -> int:
    """Resolve a calendar given as an id (int), a name (str) or None (= 0); an unknown name raises ValueError"""
    if calendar is None:
        return 0
    if isinstance(calendar, str):
        cid = lib.hocdb_calendar_id(calendar.encode('utf-8'))
        if cid == 0:
            raise ValueError(f"Unknown calendar {calendar!r} (UnknownCalendar); built-in names: "
                             f"{', '.join(Calendars.NAMES)}, custom ones come from calendar_define()")
        return cid
    if isinstance(calendar, int) and not isinstance(calendar, bool) and 0 <= calendar <= 0xFFFFFFFF:
        return calendar
    raise ValueError(f"calendar must be an id (int) or a name (str), got {calendar!r}")


def _calendar_name(lib, cid: int) -> Optional[str]:
    buf = ctypes.create_string_buffer(256)
    rc = lib.hocdb_calendar_name(cid, buf, len(buf))
    return buf.value.decode('utf-8') if rc > 0 else None


def _known_calendar(lib, what: str, calendar) -> int:
    """Resolve a calendar id / name and check that the library knows it (for functions without an error return)"""
    cid = _resolve_calendar(lib, calendar)
    if _calendar_name(lib, cid) is None:
        raise ValueError(f"{what} failed: {_INDICATOR_ERROR_MESSAGES[-31]} (id {cid})")
    return cid


def _session_dict(s: HOCDBSession) -> dict:
    return {"open": s.open, "close": s.close, "trade_day": s.trade_day, "early_close": bool(s.early_close)}


def _int_arg(name: str, value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an int, got {value!r}")
    return value


def calendar_id(name: str) -> int:
    """Id of a calendar by name ("crypto", "fx", "nyse", "nasdaq", "lse", "cme", case-insensitive, or a
    calendar_define()d one); 0 when unknown"""
    if not isinstance(name, str):
        raise ValueError(f"name must be a str, got {name!r}")
    return _registry_lib().hocdb_calendar_id(name.encode('utf-8'))


def calendar_name(cid: int) -> Optional[str]:
    """Name of a calendar id ("nyse", ...), None for an unknown id"""
    lib = _registry_lib()
    cid = _int_arg("id", cid)
    if cid < 0 or cid > 0xFFFFFFFF:
        return None
    return _calendar_name(lib, cid)


def calendar_session(calendar, utc_sec: int, which=0) -> Optional[dict]:
    """
    Session lookup: which = 0 / "at" the session containing utc_sec, 1 / "prev" that or the previous one,
    2 / "next" that or the next one. Returns {"open", "close", "trade_day", "early_close"} (UTC seconds, the
    close exclusive, trade_day = days since 1970-01-01) or None when there is no such session.
    Raises ValueError for an unknown calendar.
    """
    lib = _registry_lib()
    cid = _resolve_calendar(lib, calendar)
    if isinstance(which, str):
        key = which.strip().lower()
        if key not in _SESSION_WHICH:
            raise ValueError(f"which must be 0 / 'at', 1 / 'prev' or 2 / 'next', got {which!r}")
        which = _SESSION_WHICH[key]
    elif isinstance(which, bool) or not isinstance(which, int) or which not in (0, 1, 2):
        raise ValueError(f"which must be 0 / 'at', 1 / 'prev' or 2 / 'next', got {which!r}")
    s = HOCDBSession()
    rc = lib.hocdb_calendar_session(cid, _int_arg("utc_sec", utc_sec), which, ctypes.byref(s))
    if rc < 0:
        _raise_indicator_error(rc, "calendar_session")
    return _session_dict(s) if rc == 1 else None


def calendar_session_for_day(calendar, day: int) -> Optional[dict]:
    """Session of a trade date (days since 1970-01-01, see days_from_civil()); None when the calendar is closed that day"""
    lib = _registry_lib()
    cid = _resolve_calendar(lib, calendar)
    s = HOCDBSession()
    rc = lib.hocdb_calendar_session_for_day(cid, _int_arg("day", day), ctypes.byref(s))
    if rc < 0:
        _raise_indicator_error(rc, "calendar_session_for_day")
    return _session_dict(s) if rc == 1 else None


def calendar_is_open(calendar, utc_sec: int) -> bool:
    """True when the calendar is trading at the UTC second"""
    lib = _registry_lib()
    rc = lib.hocdb_calendar_is_open(_resolve_calendar(lib, calendar), _int_arg("utc_sec", utc_sec))
    if rc < 0:
        _raise_indicator_error(rc, "calendar_is_open")
    return rc == 1


def calendar_open_seconds(calendar, a: int, b: int) -> int:
    """Seconds of trading time inside [a, b) (UTC seconds)"""
    lib = _registry_lib()
    cid = _known_calendar(lib, "calendar_open_seconds", calendar)
    return lib.hocdb_calendar_open_seconds(cid, _int_arg("a", a), _int_arg("b", b))


def calendar_sessions_between(calendar, a: int, b: int) -> int:
    """Number of sessions opening inside [a, b) (UTC seconds)"""
    lib = _registry_lib()
    cid = _known_calendar(lib, "calendar_sessions_between", calendar)
    return lib.hocdb_calendar_sessions_between(cid, _int_arg("a", a), _int_arg("b", b))


def calendar_periods_per_year(calendar, bucket_sec: float) -> float:
    """Bars per year for bars of bucket_sec seconds (nyse, 60 -> 252 * 390; crypto, 86400 -> 365)"""
    lib = _registry_lib()
    cid = _known_calendar(lib, "calendar_periods_per_year", calendar)
    return lib.hocdb_calendar_periods_per_year(cid, float(bucket_sec))


def calendar_to_local(calendar, utc_sec: int) -> int:
    """Local wall-clock seconds of a UTC instant in the calendar's time zone (daylight saving applied)"""
    lib = _registry_lib()
    cid = _known_calendar(lib, "calendar_to_local", calendar)
    return lib.hocdb_calendar_to_local(cid, _int_arg("utc_sec", utc_sec))


def days_from_civil(year: int, month: int, day: int) -> int:
    """Days since 1970-01-01 of a civil date (the day numbers of sessions, holidays and early closes)"""
    year, month, day = _int_arg("year", year), _int_arg("month", month), _int_arg("day", day)
    if not (1 <= month <= 12) or not (1 <= day <= 31):
        raise ValueError(f"invalid civil date {year}-{month}-{day}")
    return _registry_lib().hocdb_days_from_civil(year, month, day)


def civil_from_days(days: int) -> tuple:
    """(year, month, day) of a day number (days since 1970-01-01)"""
    y, m, d = ctypes.c_int64(), ctypes.c_uint32(), ctypes.c_uint32()
    _registry_lib().hocdb_civil_from_days(_int_arg("days", days), ctypes.byref(y), ctypes.byref(m), ctypes.byref(d))
    return (y.value, m.value, d.value)


def _day_session_to_c(entry, index: int) -> HOCDBDaySession:
    if entry is None:
        return HOCDBDaySession(0, 0)
    if isinstance(entry, dict):
        for key in entry:
            if key not in ("open_sec", "close_sec"):
                raise ValueError(f"weekly[{index}]: unknown key {key!r}; expected open_sec and close_sec")
        pair = (entry.get("open_sec", 0), entry.get("close_sec", 0))
    elif isinstance(entry, (list, tuple)) and len(entry) == 2:
        pair = tuple(entry)
    else:
        raise ValueError(f"weekly[{index}] must be {{'open_sec': .., 'close_sec': ..}}, (open_sec, close_sec) or None")
    return HOCDBDaySession(_int_arg(f"weekly[{index}].open_sec", pair[0]), _int_arg(f"weekly[{index}].close_sec", pair[1]))


def _early_close_to_c(entry, index: int) -> HOCDBEarlyClose:
    if isinstance(entry, dict):
        for key in entry:
            if key not in ("day", "close_sec"):
                raise ValueError(f"early_closes[{index}]: unknown key {key!r}; expected day and close_sec")
        if "day" not in entry or "close_sec" not in entry:
            raise ValueError(f"early_closes[{index}] needs day and close_sec")
        pair = (entry["day"], entry["close_sec"])
    elif isinstance(entry, (list, tuple)) and len(entry) == 2:
        pair = tuple(entry)
    else:
        raise ValueError(f"early_closes[{index}] must be {{'day': .., 'close_sec': ..}} or (day, close_sec)")
    return HOCDBEarlyClose(_int_arg(f"early_closes[{index}].day", pair[0]), _int_arg(f"early_closes[{index}].close_sec", pair[1]))


def calendar_define(name: str, weekly, utc_offset_sec: int = 0, dst_rule=0, holidays=None, early_closes=None,
                    sessions_per_year: float = 0.0) -> int:
    """
    Register a custom trading calendar in this process and return its id (>= 32). Redefining a name reuses its id.

    Args:
        name: Calendar name (non-empty; usable wherever a calendar id or name is accepted)
        weekly: 7 entries, Monday first: {"open_sec": .., "close_sec": ..} / (open_sec, close_sec) in local
                seconds relative to the trade date's midnight (open may be negative for sessions that start the
                evening before), or None for no session on that weekday (close <= open means the same)
        utc_offset_sec: Standard UTC offset of the local time zone in seconds (e.g. 9 * 3600 for UTC+9)
        dst_rule: "none" | "us" | "eu" (or DstRule.* / 0-2)
        holidays: List of full-closure day numbers (days_from_civil())
        early_closes: List of {"day": day number, "close_sec": local close} or (day, close_sec)
        sessions_per_year: Sessions per year for annualisation (> 0, e.g. 252)

    Raises:
        ValueError: invalid definition (empty name, bad weekly / dst_rule, sessions_per_year <= 0)
        RuntimeError: the registry of custom calendars (32 entries) is full
    """
    lib = _registry_lib()
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty str")
    if isinstance(weekly, (str, bytes, dict)) or not hasattr(weekly, "__iter__"):
        raise ValueError("weekly must be a list of 7 entries (Monday first)")
    weekly = list(weekly)
    if len(weekly) != 7:
        raise ValueError(f"weekly must have 7 entries (Monday first), got {len(weekly)}")
    c_weekly = (HOCDBDaySession * 7)(*[_day_session_to_c(e, i) for i, e in enumerate(weekly)])
    if isinstance(dst_rule, str):
        key = dst_rule.strip().lower()
        if key not in DstRule.NAMES:
            raise ValueError(f"dst_rule must be one of {', '.join(DstRule.NAMES)} or an int 0-2, got {dst_rule!r}")
        dst = DstRule.NAMES[key]
    elif isinstance(dst_rule, int) and not isinstance(dst_rule, bool) and 0 <= dst_rule <= 2:
        dst = dst_rule
    else:
        raise ValueError(f"dst_rule must be one of {', '.join(DstRule.NAMES)} or an int 0-2, got {dst_rule!r}")
    hol = [] if holidays is None else [_int_arg(f"holidays[{i}]", h) for i, h in enumerate(holidays)]
    c_hol = (ctypes.c_int32 * len(hol))(*hol) if hol else None
    early = [] if early_closes is None else [_early_close_to_c(e, i) for i, e in enumerate(early_closes)]
    c_early = (HOCDBEarlyClose * len(early))(*early) if early else None
    try:
        spy = float(sessions_per_year)
    except (TypeError, ValueError):
        raise ValueError(f"sessions_per_year must be a number > 0, got {sessions_per_year!r}") from None
    if not spy > 0:
        raise ValueError(f"sessions_per_year must be > 0 (e.g. 252), got {sessions_per_year!r}")
    rc = lib.hocdb_calendar_define(name.encode('utf-8'), c_weekly, _int_arg("utc_offset_sec", utc_offset_sec), dst,
                                   c_hol, len(hol), c_early, len(early), spy)
    if rc == -1:
        raise RuntimeError("calendar_define failed: the registry of custom calendars (32 entries) is full")
    if rc <= 0:
        raise ValueError("calendar_define failed: invalid calendar definition")
    return rc


# ---------------------------------------------------------------------------
# Signal backtester (module-level: caller-provided arrays, walk-forward splits)
# ---------------------------------------------------------------------------

def _check_bucket(bucket):
    if isinstance(bucket, bool) or not isinstance(bucket, int) or bucket < 0:
        raise ValueError("bucket must be a non-negative int (0 = one row per record)")


def _c_double_array(name: str, values, n: Optional[int] = None, allow_none: bool = False):
    """Copy a sequence of numbers (list / tuple / array / numpy) into a ctypes double array -> (array, length)"""
    if values is None:
        if allow_none:
            return None, n
        raise ValueError(f"{name} must be a sequence of numbers")
    if _np is not None and isinstance(values, _np.ndarray):
        arr = _np.ascontiguousarray(values, dtype=_np.float64).reshape(-1)
        length = int(arr.size)
        c = (ctypes.c_double * length).from_buffer_copy(arr) if length else (ctypes.c_double * 0)()
    else:
        if isinstance(values, (str, bytes, dict)) or not hasattr(values, "__iter__"):
            raise ValueError(f"{name} must be a sequence of numbers")
        try:
            seq = [float(v) for v in values]
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be a sequence of numbers") from None
        length = len(seq)
        c = (ctypes.c_double * length)(*seq)
    if n is not None and length != n:
        raise ValueError(f"length mismatch: {name} has {length} entries but {n} were expected")
    return c, length


def _c_int64_array(name: str, values, n: Optional[int] = None, allow_none: bool = False):
    """Copy a sequence of ints (list / tuple / numpy) into a ctypes int64 array -> (array, length)"""
    if values is None:
        if allow_none:
            return None, n
        raise ValueError(f"{name} must be a sequence of ints")
    if _np is not None and isinstance(values, _np.ndarray):
        arr = _np.ascontiguousarray(values, dtype=_np.int64).reshape(-1)
        length = int(arr.size)
        c = (ctypes.c_int64 * length).from_buffer_copy(arr) if length else (ctypes.c_int64 * 0)()
    else:
        if isinstance(values, (str, bytes, dict)) or not hasattr(values, "__iter__"):
            raise ValueError(f"{name} must be a sequence of ints")
        seq = []
        for v in values:
            if isinstance(v, bool) or not isinstance(v, int):
                if _np is not None and isinstance(v, _np.integer):
                    v = int(v)
                else:
                    raise ValueError(f"{name} must be a sequence of ints, got {v!r}")
            seq.append(v)
        length = len(seq)
        c = (ctypes.c_int64 * length)(*seq)
    if n is not None and length != n:
        raise ValueError(f"length mismatch: {name} has {length} entries but {n} were expected")
    return c, length


def _copy_out_array(arr, use_numpy: bool):
    """A ctypes double array -> list (or numpy array)"""
    if use_numpy:
        return _np.frombuffer(arr, dtype=_np.float64).copy() if len(arr) else _np.zeros(0, dtype=_np.float64)
    return list(arr)


def _mode_value(key: str, value, names: dict) -> int:
    """Resolve an enum-like param given as a name ("fraction") or an int"""
    if isinstance(value, str):
        k = value.strip().lower().replace("-", "_").replace(" ", "_")
        if k not in names:
            raise ValueError(f"{key} must be one of {', '.join(names)} or an int 0-{len(names) - 1}, got {value!r}")
        return names[k]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{key} must be one of {', '.join(names)} or an int 0-{len(names) - 1}, got {value!r}")
    return value


def backtest_params_default() -> dict:
    """The default backtest parameters as a dict (initial_equity 1.0, allow_short True, fill_mode 0, ...)"""
    return _params_dict(_backtest_params(_registry_lib(), None), _BACKTEST_PARAM_KEYS)


def universe_params_default() -> dict:
    """The default universe parameters as a dict (mom_short 5, mom_mid 20, mom_long 60, vol_period 20, ...)"""
    return _params_dict(_universe_params(_registry_lib(), None), _UNIVERSE_PARAM_KEYS)


def _params_dict(struct, keys) -> dict:
    out = {key: getattr(struct, key) for key in keys}
    if "allow_short" in out:
        out["allow_short"] = bool(out["allow_short"])
    return out


def _backtest_params(lib, params) -> HOCDBBacktestParams:
    """A HOCDBBacktestParams with the C defaults, overridden by the entries of a dict"""
    _check_struct_size(lib, "backtest_params", HOCDBBacktestParams)
    if isinstance(params, HOCDBBacktestParams):
        return params
    p = HOCDBBacktestParams()
    lib.hocdb_backtest_params_default(ctypes.byref(p))
    if params is None:
        return p
    if not isinstance(params, dict):
        raise ValueError("params must be a dict, e.g. {'initial_equity': 10000, 'cost_bps': 5, 'position_mode': 'fraction'}")
    for key, value in params.items():
        if key not in _BACKTEST_PARAM_KEYS:
            raise ValueError(f"Unknown backtest param {key!r}; expected one of {', '.join(_BACKTEST_PARAM_KEYS)}")
        if value is None:
            continue
        if key == "position_mode":
            p.position_mode = _mode_value(key, value, _POSITION_MODES)
        elif key == "fill_mode":
            p.fill_mode = _mode_value(key, value, _FILL_MODES)
        elif key == "allow_short":
            p.allow_short = 1 if value else 0
        else:
            try:
                setattr(p, key, float(value))
            except (TypeError, ValueError):
                raise ValueError(f"backtest param {key!r} must be a number, got {value!r}") from None
    return p


def _backtest_outputs(outputs, n: int):
    """-> (HOCDBBacktestOutputs or None, {name: ctypes double array}) for the requested per-bar series"""
    if outputs is None or outputs is False:
        return None, {}
    if outputs is True:
        names = list(_BACKTEST_OUTPUT_NAMES)
    elif isinstance(outputs, str):
        names = [outputs]
    elif isinstance(outputs, dict) or not hasattr(outputs, "__iter__"):
        raise ValueError(f"outputs must be a list of names among {', '.join(_BACKTEST_OUTPUT_NAMES)} (or True for all)")
    else:
        names = list(outputs)
    arrays = {}
    c = HOCDBBacktestOutputs()
    for name in names:
        if name not in _BACKTEST_OUTPUT_NAMES:
            raise ValueError(f"Unknown backtest output {name!r}; expected one of {', '.join(_BACKTEST_OUTPUT_NAMES)}")
        if name in arrays:
            continue
        arr = (ctypes.c_double * n)()
        arrays[name] = arr
        setattr(c, name, ctypes.cast(arr, ctypes.POINTER(ctypes.c_double)))
    return c, arrays


def _trade_buffer(lib, max_trades):
    """-> (ctypes array of HOCDBTrade or None, capacity)"""
    if max_trades is None:
        return None, 0
    cap = _non_negative_int("max_trades", max_trades)
    _check_struct_size(lib, "trade", HOCDBTrade)
    return ((HOCDBTrade * cap)() if cap else None), cap


def _backtest_dict(lib, out, c_trades, max_trades, out_arrays: dict, as_numpy: bool) -> dict:
    result = _decode_struct(lib, "backtest_result", out)
    d = {"result": result}
    if max_trades is not None:
        k = min(result["n_trades"], max_trades)
        size = ctypes.sizeof(HOCDBTrade)
        d["trades"] = [_decode_struct(lib, "trade", c_trades, i * size) for i in range(k)]
    use_numpy = as_numpy and _np is not None
    for name, arr in out_arrays.items():
        d[name] = _copy_out_array(arr, use_numpy)
    return d


def _bar_arrays(ts, open, high, low, close, target):
    """The ctypes arrays of backtest_arrays() / backtest_splits() -> (c_ts, c_open, c_high, c_low, c_close, c_target, n)"""
    c_ts, n = _c_int64_array("ts", ts)
    c_close, _ = _c_double_array("close", close, n)
    c_open, _ = _c_double_array("open", open, n, allow_none=True)
    c_high, _ = _c_double_array("high", high, n, allow_none=True)
    c_low, _ = _c_double_array("low", low, n, allow_none=True)
    c_target, _ = _c_double_array("target", target, n)
    return c_ts, c_open, c_high, c_low, c_close, c_target, n


def backtest_arrays(ts, open, high, low, close, target, params: Optional[dict] = None, outputs=None,
                    max_trades: Optional[int] = None, as_numpy: bool = False) -> dict:
    """
    Backtest a target-position series on caller-provided bars (no database): the same kernel as
    HOCDB.backtest(). `ts`, `close` and `target` are sequences of the same length (lists or numpy arrays);
    `open`, `high` and `low` may be None (then fills happen at the close and stops trigger on the close).
    target[i] is the desired position at the end of bar i; see HOCDB.backtest() for `params` (defaults from
    backtest_params_default(); periods_per_year 0 = no annualisation here), `outputs`, `max_trades` and the
    result dict ({"result": {...}, "trades": [...], "equity": [...], ...}).
    """
    lib = _registry_lib()
    c_ts, c_open, c_high, c_low, c_close, c_target, n = _bar_arrays(ts, open, high, low, close, target)
    c_params = _backtest_params(lib, params)
    c_outputs, out_arrays = _backtest_outputs(outputs, n)
    c_trades, cap = _trade_buffer(lib, max_trades)
    out = _new_struct(lib, "backtest_result", HOCDBBacktestResult)
    rc = lib.hocdb_backtest_arrays(c_ts, c_open, c_high, c_low, c_close, n, c_target, ctypes.byref(c_params),
                                   c_outputs, c_trades, cap, ctypes.byref(out))
    if rc != 0:
        _raise_indicator_error(rc, "backtest_arrays")
    return _backtest_dict(lib, out, c_trades, max_trades, out_arrays, as_numpy)


def walk_forward_splits(n: int, n_splits: int, train_frac: float, anchored: bool = True) -> list:
    """
    Walk-forward index ranges over n bars: the first train window is floor(train_frac * n) bars and the test
    windows tile the rest in n_splits pieces; anchored=True expands the train window from 0, False rolls it.
    Returns a list of {"train_start", "train_end", "test_start", "test_end"} (ends exclusive), e.g.
    walk_forward_splits(100, 4, 0.5, True)[0] == {0, 50, 50, 62}.
    """
    lib = _registry_lib()
    n = _non_negative_int("n", n)
    n_splits = _non_negative_int("n_splits", n_splits)
    cap = max(n_splits, 1)
    buf = (HOCDBSplit * cap)()
    k = lib.hocdb_walk_forward_splits(n, n_splits, float(train_frac), 1 if anchored else 0, buf, cap)
    return [{key: getattr(buf[i], key) for key in _SPLIT_KEYS} for i in range(k)]


def _split_to_c(split, index: int) -> HOCDBSplit:
    if isinstance(split, dict):
        for key in split:
            if key not in _SPLIT_KEYS:
                raise ValueError(f"splits[{index}]: unknown key {key!r}; expected {', '.join(_SPLIT_KEYS)}")
        values = [split.get(key, 0) for key in _SPLIT_KEYS]
    elif isinstance(split, (list, tuple)) and len(split) == 4:
        values = list(split)
    else:
        raise ValueError(f"splits[{index}] must be a dict with {', '.join(_SPLIT_KEYS)} (see walk_forward_splits()) "
                         "or a (train_start, train_end, test_start, test_end) tuple")
    return HOCDBSplit(*[_non_negative_int(f"splits[{index}].{key}", v) for key, v in zip(_SPLIT_KEYS, values)])


def backtest_splits(ts, open, high, low, close, target, splits, params: Optional[dict] = None) -> list:
    """
    Run the backtest on every test window of `splits` (from walk_forward_splits()) independently, each with
    fresh initial equity, on the same arrays as backtest_arrays(). Returns one result dict per split.
    """
    lib = _registry_lib()
    if isinstance(splits, (str, bytes, dict)) or not hasattr(splits, "__iter__"):
        raise ValueError("splits must be a list of split dicts (see walk_forward_splits())")
    splits = list(splits)
    c_ts, c_open, c_high, c_low, c_close, c_target, n = _bar_arrays(ts, open, high, low, close, target)
    if not splits:
        return []
    c_splits = (HOCDBSplit * len(splits))(*[_split_to_c(s, i) for i, s in enumerate(splits)])
    c_params = _backtest_params(lib, params)
    _check_struct_size(lib, "backtest_result", HOCDBBacktestResult)
    results = (HOCDBBacktestResult * len(splits))()
    rc = lib.hocdb_backtest_splits_arrays(c_ts, c_open, c_high, c_low, c_close, n, c_target, ctypes.byref(c_params),
                                          c_splits, len(splits), results)
    if rc < 0:
        _raise_indicator_error(rc, "backtest_splits")
    size = ctypes.sizeof(HOCDBBacktestResult)
    return [_decode_struct(lib, "backtest_result", results, i * size) for i in range(rc)]


# ---------------------------------------------------------------------------
# Universe (cross-sectional) features
# ---------------------------------------------------------------------------

def _universe_params(lib, params) -> HOCDBUniverseParams:
    """A HOCDBUniverseParams with the C defaults, overridden by the entries of a dict"""
    _check_struct_size(lib, "universe_params", HOCDBUniverseParams)
    if isinstance(params, HOCDBUniverseParams):
        return params
    p = HOCDBUniverseParams()
    lib.hocdb_universe_params_default(ctypes.byref(p))
    if params is None:
        return p
    if not isinstance(params, dict):
        raise ValueError("params must be a dict, e.g. {'mom_long': 30, 'corr_period': 30, 'weights_mode': 'volume'}")
    for key, value in params.items():
        if key not in _UNIVERSE_PARAM_KEYS:
            raise ValueError(f"Unknown universe param {key!r}; expected one of {', '.join(_UNIVERSE_PARAM_KEYS)}")
        if value is None:
            continue
        if key == "periods_per_year":
            try:
                p.periods_per_year = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"universe param 'periods_per_year' must be a number, got {value!r}") from None
        elif key == "weights_mode":
            p.weights_mode = _mode_value(key, value, _WEIGHTS_MODES)
        else:
            setattr(p, key, _non_negative_int(key, value))
    return p


def _universe_dict(lib, out, rows, n: int, c_corr) -> dict:
    summary = _decode_struct(lib, "universe_summary", out)
    size = ctypes.sizeof(HOCDBUniverseRow)
    d = {"summary": summary, "rows": [_decode_struct(lib, "universe_row", rows, i * size) for i in range(n)]}
    d["corr"] = [list(c_corr[i * n:(i + 1) * n]) for i in range(n)] if c_corr is not None else None
    return d


def universe(dbs, columns: Optional[dict] = None, n_bars: int = 0, bucket: int = 0, params: Optional[dict] = None,
             corr: bool = True) -> dict:
    """
    Cross-sectional features over a watch-list of open databases with the same column roles: the last n_bars
    bars (bucket > 0; n_bars 0 = enough for the longest period) or records of every database are
    inner-joined on timestamps, then per-ticker momentum / volatility / relative-strength percentile ranks,
    betas and correlations to an equal- or volume-weighted market factor, and universe-level dispersion /
    breadth / average pair correlation are computed for the last bar.

    Args:
        dbs: List of HOCDB instances (writers or readers), one per ticker, in the order of the result rows
        columns: {open, high, low, close, volume, ...} -> field name or index, resolved against every database
                 (the field indices must agree; default: auto-detect, 'close' / 'price' required)
        n_bars: Bars (bucket > 0) or records per database to use; 0 = enough for the longest period
        bucket: 0 = one row per record; > 0 = bars of that many timestamp units
        params: dict of universe parameters; missing keys take the C defaults (universe_params_default()):
                mom_short (5), mom_mid (20), mom_long (60), vol_period (20), corr_period (60), sma_period (50),
                beta_period (60), periods_per_year (0 = no annualisation of vol), weights_mode
                (0 / "equal", 1 / "volume": needs volume columns)
        corr: Also return the n x n correlation matrix of returns

    Returns:
        {"summary": {n_tickers, n_bars (joined bars used), market_ret_1, market_mom_short, market_mom_mid,
                     market_mom_long, market_vol, dispersion, dispersion_mid, breadth_sma, breadth_up,
                     avg_pair_corr, max_pair_corr, min_pair_corr, first_ts, last_ts},
         "rows": [{last_close, ret_1, mom_short, mom_mid, mom_long, vol, sma_distance, beta, corr_market,
                   rel_strength, rank_mom_short, rank_mom_mid, rank_mom_long, rank_vol, rank_rel_strength,
                   z_mom_mid, avg_corr, max_corr, max_corr_index, idio_vol, volume_ratio} per database],
         "corr": [[...], ...] (n x n nested lists) or None when corr=False}
        NaN marks features a ticker has too few bars for; ranks are in [0, 1] with 1 = highest.
    """
    if isinstance(dbs, (str, bytes, dict)) or not hasattr(dbs, "__iter__"):
        raise ValueError("dbs must be a list of HOCDB instances")
    dbs = list(dbs)
    if not dbs:
        raise ValueError("dbs must contain at least one HOCDB instance")
    for db in dbs:
        if not isinstance(db, HOCDB):
            raise ValueError(f"dbs must contain HOCDB instances, got {type(db).__name__}")
        if not db.handle:
            raise RuntimeError("Database not initialized (closed?)")
    _check_bucket(bucket)
    n_bars = _non_negative_int("n_bars", n_bars)
    lib = dbs[0].lib
    cols = dbs[0]._resolve_columns(columns)
    for db in dbs[1:]:
        other = db._resolve_columns(columns)
        if bytes(other) != bytes(cols):
            raise ValueError(f"the column roles of {db.ticker!r} resolve to different field indices than those of "
                             f"{dbs[0].ticker!r}; universe() needs the same column layout in every database "
                             "(pass columns={'close': <index>, ...})")
    n = len(dbs)
    handles = (ctypes.c_void_p * n)(*[db.handle for db in dbs])
    c_params = _universe_params(lib, params)
    _check_struct_size(lib, "universe_row", HOCDBUniverseRow)
    rows = (HOCDBUniverseRow * n)()
    c_corr = (ctypes.c_double * (n * n))() if corr else None
    out = _new_struct(lib, "universe_summary", HOCDBUniverseSummary)
    rc = lib.hocdb_universe(handles, n, ctypes.byref(cols), n_bars, bucket, ctypes.byref(c_params), rows, c_corr,
                            ctypes.byref(out))
    if rc != 0:
        _raise_indicator_error(rc, "universe")
    return _universe_dict(lib, out, rows, n, c_corr)


def universe_arrays(closes, volumes=None, ts=None, params: Optional[dict] = None, corr: bool = True) -> dict:
    """
    The same features as universe() on caller-provided aligned series: `closes` is a list of n_tickers close
    sequences of the same length (already joined on one time axis), `volumes` an optional list aligned the
    same way, `ts` the optional shared timestamps (only for summary first_ts / last_ts). Returns the same
    {"summary", "rows", "corr"} dict.
    """
    lib = _registry_lib()
    if isinstance(closes, (str, bytes, dict)) or not hasattr(closes, "__iter__"):
        raise ValueError("closes must be a list of close series (one per ticker)")
    closes = list(closes)
    n = len(closes)
    if n == 0:
        raise ValueError("closes must contain at least one series")
    c_closes = []
    n_bars = None
    for i, series in enumerate(closes):
        arr, n_bars = _c_double_array(f"closes[{i}]", series, n_bars)
        c_closes.append(arr)
    dbl_p = ctypes.POINTER(ctypes.c_double)
    close_ptrs = (dbl_p * n)(*[ctypes.cast(a, dbl_p) for a in c_closes])
    vol_ptrs = None
    c_vols = []
    if volumes is not None:
        if isinstance(volumes, (str, bytes, dict)) or not hasattr(volumes, "__iter__"):
            raise ValueError("volumes must be a list of volume series aligned with closes, or None")
        volumes = list(volumes)
        if len(volumes) != n:
            raise ValueError(f"length mismatch: volumes has {len(volumes)} series but closes has {n}")
        for i, series in enumerate(volumes):
            arr, _ = _c_double_array(f"volumes[{i}]", series, n_bars)
            c_vols.append(arr)
        vol_ptrs = (dbl_p * n)(*[ctypes.cast(a, dbl_p) for a in c_vols])
    c_ts, _ = _c_int64_array("ts", ts, n_bars, allow_none=True)
    c_params = _universe_params(lib, params)
    _check_struct_size(lib, "universe_row", HOCDBUniverseRow)
    rows = (HOCDBUniverseRow * n)()
    c_corr = (ctypes.c_double * (n * n))() if corr else None
    out = _new_struct(lib, "universe_summary", HOCDBUniverseSummary)
    rc = lib.hocdb_universe_arrays(close_ptrs, vol_ptrs, n, n_bars, c_ts, ctypes.byref(c_params), rows, c_corr,
                                   ctypes.byref(out))
    if rc != 0:
        _raise_indicator_error(rc, "universe_arrays")
    return _universe_dict(lib, out, rows, n, c_corr)


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