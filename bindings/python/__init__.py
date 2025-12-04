"""
HOCDB Python Bindings - High-Performance Time Series Database

This package provides Python bindings for HOCDB using ctypes to interface
with the C API of the Zig implementation.
"""

from .hocdb_python import HOCDB, HOCDBField, FieldTypes, create_record_bytes

__version__ = "0.1.0"
__author__ = "Heroes of Crypto AI"

__all__ = [
    'HOCDB',
    'HOCDBField', 
    'FieldTypes',
    'create_record_bytes'
]