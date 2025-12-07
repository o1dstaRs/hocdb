/*
Package hocdb provides Go bindings for HOCDB - High-Performance Time Series Database.

The package uses CGO to interface with the underlying C library. Before using the package,
ensure that the HOCDB C library (libhocdb_c) is built and available in the system library path.
You can build the C library by running:

	zig build c-bindings

Example usage:

	package main

	import (
	    "fmt"
	    "hocdb"
	)

	func main() {
	    // Define schema
	    schema := []hocdb.Field{
	        {Name: "timestamp", Type: hocdb.TypeI64},
	        {Name: "price", Type: hocdb.TypeF64},
	        {Name: "volume", Type: hocdb.TypeF64},
	    }

	    // Create database instance
	    db, err := hocdb.New("BTC_USD", "./go_test_data", schema, hocdb.Options{
	        MaxFileSize:   0, // Use default
	        OverwriteFull: false,
	        FlushOnWrite:  false,
	    })
	    if err != nil {
	        panic(err)
	    }
	    defer db.Close()

	    // Create and append a record
	    record, err := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
	    if err != nil {
	        panic(err)
	    }
	    err = db.Append(record)
	    if err != nil {
	        panic(err)
	    }

	    // Load all data
	    data, err := db.Load()
	    if err != nil {
	        panic(err)
	    }
	    fmt.Printf("Loaded %d bytes of data\n", len(data))
	}
*/
package hocdb

/*
#cgo CFLAGS: -I../../bindings/c
#cgo LDFLAGS: -L../../zig-out/lib -lhocdb_c
#include "hocdb.h"
#include <stdlib.h>
*/
import "C"
import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"
)

// FieldType represents the type of data stored in each field
type FieldType int

const (
	TypeI64    FieldType = 1 // Signed 64-bit integer
	TypeF64    FieldType = 2 // 64-bit floating point
	TypeU64    FieldType = 3 // Unsigned 64-bit integer
	TypeString FieldType = 5 // Fixed 128-byte string
	TypeBool   FieldType = 6 // Boolean (1 byte)
)

// Field defines a field in the database schema
type Field struct {
	Name string
	Type FieldType
}

// Stats represents statistics for a field in a time range
type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count uint64
	Mean  float64
}

// Latest represents the latest value and timestamp for a field
type Latest struct {
	Value     float64
	Timestamp int64
}

// Filter represents a filter condition for queries
type Filter struct {
	FieldIndex int
	Value      interface{}
}

// Options contains configuration options for the database
type Options struct {
	MaxFileSize   int64
	OverwriteFull bool
	FlushOnWrite  bool
	AutoIncrement bool
}

// DB represents a connection to an HOCDB database
type DB struct {
	handle   C.HOCDBHandle
	fieldMap map[string]int
}

// New creates a new HOCDB instance with the specified schema
func New(ticker, path string, schema []Field, options Options) (*DB, error) {
	// Convert Go strings to C strings
	tickerC := C.CString(ticker)
	defer C.free(unsafe.Pointer(tickerC))

	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))

	// Convert Go schema to C schema
	cSchema := make([]C.CField, len(schema))
	for i, field := range schema {
		cSchema[i].name = C.CString(field.Name)
		cSchema[i]._type = C.int(field.Type)
		// Note: We free these C strings after the hocdb_init call
	}

	// Create C array of CField
	var cSchemaPtr *C.CField
	if len(cSchema) > 0 {
		cSchemaPtr = &cSchema[0]
	}

	// Convert options
	maxFileSize := C.int64_t(options.MaxFileSize)
	overwriteOnFull := C.int(0)
	if options.OverwriteFull {
		overwriteOnFull = 1
	}
	flushOnWrite := C.int(0)
	if options.FlushOnWrite {
		flushOnWrite = 1
	}
	autoIncrement := C.int(0)
	if options.AutoIncrement {
		autoIncrement = 1
	}

	// Call C API
	handle := C.hocdb_init(
		tickerC,
		pathC,
		cSchemaPtr,
		C.size_t(len(schema)),
		maxFileSize,
		overwriteOnFull,
		flushOnWrite,
		autoIncrement,
	)

	// Free the C strings we created for schema names
	for i := range cSchema {
		C.free(unsafe.Pointer(cSchema[i].name))
	}

	if handle == nil {
		return nil, errors.New("failed to initialize HOCDB")
	}

	fieldMap := make(map[string]int)
	for i, field := range schema {
		fieldMap[field.Name] = i
	}

	return &DB{handle: handle, fieldMap: fieldMap}, nil
}

// Append adds a raw record to the database
func (db *DB) Append(data []byte) error {
	if db.handle == nil {
		return errors.New("database not initialized")
	}

	var dataPtr unsafe.Pointer
	if len(data) > 0 {
		dataPtr = unsafe.Pointer(&data[0])
	}

	result := C.hocdb_append(
		db.handle,
		dataPtr,
		C.size_t(len(data)),
	)

	if result != 0 {
		return errors.New("failed to append data to HOCDB")
	}

	return nil
}

// Flush forces a write of all pending data to disk
func (db *DB) Flush() error {
	if db.handle == nil {
		return errors.New("database not initialized")
	}

	result := C.hocdb_flush(db.handle)

	if result != 0 {
		return errors.New("failed to flush HOCDB")
	}

	return nil
}

// Load retrieves all records from the database
func (db *DB) Load() ([]byte, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}

	var outLen C.size_t
	dataPtr := C.hocdb_load(db.handle, &outLen)

	if dataPtr == nil {
		return nil, errors.New("failed to load data from HOCDB")
	}

	defer C.hocdb_free(dataPtr)

	// Copy data from C memory to Go slice
	data := C.GoBytes(dataPtr, C.int(outLen))

	return data, nil
}

// Query retrieves records within the specified time range [startTs, endTs) with optional filters
// Filters can be passed as []Filter or map[string]interface{}
func (db *DB) Query(startTs, endTs int64, filters interface{}) ([]byte, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}

	var parsedFilters []Filter

	if filters != nil {
		switch v := filters.(type) {
		case []Filter:
			parsedFilters = v
		case map[string]interface{}:
			for key, val := range v {
				idx, ok := db.fieldMap[key]
				if !ok {
					return nil, fmt.Errorf("unknown field in filter: %s", key)
				}
				parsedFilters = append(parsedFilters, Filter{
					FieldIndex: idx,
					Value:      val,
				})
			}
		default:
			return nil, errors.New("invalid filters type: expected []Filter or map[string]interface{}")
		}
	}

	// Convert Go filters to C filters
	var cFiltersPtr *C.HOCDBFilter
	if len(parsedFilters) > 0 {
		cFilters := make([]C.HOCDBFilter, len(parsedFilters))
		for i, f := range parsedFilters {
			cFilters[i].field_index = C.size_t(f.FieldIndex)
			switch v := f.Value.(type) {
			case int64:
				cFilters[i]._type = C.int(TypeI64)
				cFilters[i].val_i64 = C.int64_t(v)
			case int:
				cFilters[i]._type = C.int(TypeI64)
				cFilters[i].val_i64 = C.int64_t(v)
			case float64:
				cFilters[i]._type = C.int(TypeF64)
				cFilters[i].val_f64 = C.double(v)
			case uint64:
				cFilters[i]._type = C.int(TypeU64)
				cFilters[i].val_u64 = C.uint64_t(v)
			case string:
				cFilters[i]._type = C.int(TypeString)
				// Copy string to fixed buffer
				cStr := C.CString(v)
				// We need to copy manually because val_string is a fixed array
				// This is tricky in CGO directly to a struct field array.
				// Let's use a helper or unsafe copy.
				// Safe way:
				var buf [128]byte
				copy(buf[:], v)
				// We can't assign Go array to C array directly easily.
				// We have to cast.
				// Actually, CGO maps char[128] to [128]C.char
				for j := 0; j < 128 && j < len(v); j++ {
					cFilters[i].val_string[j] = C.char(v[j])
				}
				cFilters[i].val_string[min(127, len(v))] = 0 // Null terminate just in case
				C.free(unsafe.Pointer(cStr))                 // Not used actually
			case bool:
				cFilters[i]._type = C.int(TypeBool)
				cFilters[i].val_bool = C.bool(v)
			default:
				return nil, errors.New("unsupported filter value type")
			}
		}
		cFiltersPtr = &cFilters[0]
	}

	var outLen C.size_t
	dataPtr := C.hocdb_query(
		db.handle,
		C.int64_t(startTs),
		C.int64_t(endTs),
		cFiltersPtr,
		C.size_t(len(parsedFilters)),
		&outLen,
	)

	if dataPtr == nil {
		// Query returning nil could mean error or empty result
		// We'll treat it as empty for now (could be changed to return an error)
		return []byte{}, nil
	}

	defer C.hocdb_free(dataPtr)

	// Copy data from C memory to Go slice
	data := C.GoBytes(dataPtr, C.int(outLen))

	return data, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetStats returns statistics for a specific field within a time range
func (db *DB) GetStats(startTs, endTs int64, fieldIndex int) (*Stats, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}

	var outStats C.HOCDBStats
	result := C.hocdb_get_stats(
		db.handle,
		C.int64_t(startTs),
		C.int64_t(endTs),
		C.size_t(fieldIndex),
		&outStats,
	)

	if result != 0 {
		return nil, errors.New("failed to get stats from HOCDB")
	}

	stats := &Stats{
		Min:   float64(outStats.min),
		Max:   float64(outStats.max),
		Sum:   float64(outStats.sum),
		Count: uint64(outStats.count),
		Mean:  float64(outStats.mean),
	}

	return stats, nil
}

// GetLatest returns the latest value and timestamp for a specific field
func (db *DB) GetLatest(fieldIndex int) (*Latest, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}

	var outVal C.double
	var outTs C.int64_t

	result := C.hocdb_get_latest(
		db.handle,
		C.size_t(fieldIndex),
		&outVal,
		&outTs,
	)

	if result != 0 {
		return nil, errors.New("failed to get latest value from HOCDB")
	}

	latest := &Latest{
		Value:     float64(outVal),
		Timestamp: int64(outTs),
	}

	return latest, nil
}

// Close closes the database connection and frees resources
func (db *DB) Close() {
	if db.handle != nil {
		C.hocdb_close(db.handle)
		db.handle = nil
	}
}

// CreateRecordBytes creates raw bytes for a record based on the schema and values
// This function helps convert Go values to the required binary format
func CreateRecordBytes(schema []Field, values ...interface{}) ([]byte, error) {
	if len(values) != len(schema) {
		return nil, errors.New("number of values doesn't match schema length")
	}

	var record []byte

	for i, field := range schema {
		value := values[i]

		switch field.Type {
		case TypeI64:
			var val int64
			switch v := value.(type) {
			case int64:
				val = v
			case int:
				val = int64(v)
			case int32:
				val = int64(v)
			default:
				return nil, errors.New("invalid type for I64 field")
			}

			// Convert to little-endian bytes
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(val))
			record = append(record, bytes...)

		case TypeF64:
			var val float64
			switch v := value.(type) {
			case float64:
				val = v
			case float32:
				val = float64(v)
			case int:
				val = float64(v)
			default:
				return nil, errors.New("invalid type for F64 field")
			}

			// Convert float64 to little-endian bytes
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, math.Float64bits(val))
			record = append(record, bytes...)

		case TypeU64:
			var val uint64
			switch v := value.(type) {
			case uint64:
				val = v
			case uint:
				val = uint64(v)
			case int:
				if v < 0 {
					return nil, errors.New("negative value for U64 field")
				}
				val = uint64(v)
			default:
				return nil, errors.New("invalid type for U64 field")
			}

			// Convert to little-endian bytes
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, val)
			record = append(record, bytes...)

		case TypeString:
			var val string
			switch v := value.(type) {
			case string:
				val = v
			default:
				return nil, errors.New("invalid type for String field")
			}

			// Pad with zeros to 128 bytes
			bytes := make([]byte, 128)
			copy(bytes, val)
			record = append(record, bytes...)

		case TypeBool:
			var val bool
			switch v := value.(type) {
			case bool:
				val = v
			default:
				return nil, errors.New("invalid type for Bool field")
			}

			// Convert to 1 byte
			var b byte
			if val {
				b = 1
			}
			record = append(record, b)

		default:
			return nil, errors.New("unsupported field type")
		}
	}

	return record, nil
}
