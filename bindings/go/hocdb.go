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
	"reflect"
	"runtime"
	"strings"
	"sync"
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
	P50   float64
	P90   float64
	P95   float64
	P99   float64
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

// Fsync policies for Options.Fsync. The engine commits (makes data visible to
// readers) on every flush; the policy decides when the committed bytes are
// also fsync'ed to stable storage.
const (
	FsyncNone     = "none"     // never fsync; the OS decides when the bytes reach the disk
	FsyncOnClose  = "on_close" // fsync once on Close (default)
	FsyncOnFlush  = "on_flush" // fsync after every Flush / commit
	FsyncInterval = "interval" // fsync at most once per Options.FsyncIntervalMs, and on Close
)

// Options contains configuration options for the database (passed to
// hocdb_init_ex). The zero value selects the defaults: a linear 2 GiB file,
// no ring buffer, fsync on close, legacy files migrated in place.
type Options struct {
	MaxFileSize   int64 // Maximum file size in bytes (0 = default 2 GiB). Ring buffers: HeaderSize() + N * recordSize holds exactly N records
	OverwriteFull bool  // Ring buffer: overwrite the oldest records when the file is full
	FlushOnWrite  bool  // Flush (commit) after every Append
	AutoIncrement bool  // Auto-increment timestamps

	// Durability, maintenance and metrics.
	Fsync              string // fsync policy: "" or FsyncOnClose (default), FsyncNone, FsyncOnFlush, FsyncInterval; the numbers "0".."3" are accepted too
	FsyncIntervalMs    uint32 // FsyncInterval: at most one fsync per interval in milliseconds (0 = 1000)
	VerifyOnOpen       bool   // Recompute the CRC32C checksum when opening; a mismatch makes New fail with "ChecksumMismatch"
	RetentionSpan      int64  // Drop records older than lastTimestamp - RetentionSpan (timestamp units) once the excess exceeds 25%; 0 = off
	RolloverSize       uint64 // Archive the file as <ticker>.<first_ts>-<last_ts>.bin once it grows above this many bytes and continue with an empty file; 0 = off
	DisableAutoMigrate bool   // Do not rewrite legacy HOC1 files on open (New then fails with "LegacyFormatNeedsMigration"); the default migrates them in place
	TimestampUnitNs    uint64 // Nanoseconds per timestamp unit, used for the ingest-lag metrics; 0 = unknown
	IndexStride        uint64 // Records per index entry (0 = default 1024)

	// Trading calendar. Calendar is the id (CalendarCrypto ... CalendarCME or
	// an id returned by CalendarDefine); CalendarName names it instead
	// ("nyse", ...) and is resolved with CalendarID before opening, so an
	// unknown name makes New fail without touching the file. Built-in ids and
	// TimestampUnitNs are persisted in the file header: a file created with a
	// calendar reports it on every later open, readers included.
	Calendar     uint32 // Trading calendar id, 0 = none
	CalendarName string // Trading calendar name; "" = use Calendar (both set = they must agree)
}

// ErrReadOnly is wrapped in the error of every write or maintenance operation
// (Append, Sync, Compact, RetainLast, Rollover) attempted on a handle opened
// with OpenReader; Flush on a reader is a Refresh. Test for it with errors.Is.
var ErrReadOnly = errors.New("database is read-only: this handle is a reader opened with OpenReader")

// ErrChecksumUnavailable is wrapped in the error of Verify when the file has
// no checksum to compare against: ring buffers (OverwriteFull) and legacy HOC1
// files. Test for it with errors.Is.
var ErrChecksumUnavailable = errors.New("checksum unavailable: ring buffers and legacy HOC1 files carry no CRC32C checksum")

var errNotInitialized = errors.New("database not initialized")

// DB represents a connection to an HOCDB database
type DB struct {
	handle   C.HOCDBHandle
	fieldMap map[string]int
}

// fsyncPolicy maps Options.Fsync to the HOCDB_FSYNC_* constant.
func fsyncPolicy(s string) (C.int, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", FsyncOnClose, "1":
		return C.HOCDB_FSYNC_ON_CLOSE, nil
	case FsyncNone, "0":
		return C.HOCDB_FSYNC_NONE, nil
	case FsyncOnFlush, "2":
		return C.HOCDB_FSYNC_ON_FLUSH, nil
	case FsyncInterval, "3":
		return C.HOCDB_FSYNC_INTERVAL, nil
	}
	return 0, fmt.Errorf("invalid fsync policy %q: use %q, %q, %q or %q", s, FsyncNone, FsyncOnClose, FsyncOnFlush, FsyncInterval)
}

func cBool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// cConfig converts Options to the C configuration struct.
func cConfig(options Options) (C.HOCDBConfig, error) {
	var cfg C.HOCDBConfig
	policy, err := fsyncPolicy(options.Fsync)
	if err != nil {
		return cfg, err
	}
	if options.MaxFileSize < 0 {
		return cfg, errors.New("MaxFileSize must be >= 0")
	}
	if options.RetentionSpan < 0 {
		return cfg, errors.New("RetentionSpan must be >= 0")
	}
	cfg.max_file_size = C.int64_t(options.MaxFileSize)
	cfg.overwrite_on_full = cBool(options.OverwriteFull)
	cfg.flush_on_write = cBool(options.FlushOnWrite)
	cfg.auto_increment = cBool(options.AutoIncrement)
	cfg.fsync_policy = policy
	cfg.fsync_interval_ms = C.uint32_t(options.FsyncIntervalMs)
	cfg.verify_on_open = cBool(options.VerifyOnOpen)
	cfg.retention_span = C.int64_t(options.RetentionSpan)
	cfg.rollover_size = C.uint64_t(options.RolloverSize)
	cfg.auto_migrate = cBool(!options.DisableAutoMigrate)
	cfg.timestamp_unit_ns = C.uint64_t(options.TimestampUnitNs)
	cfg.index_stride = C.uint64_t(options.IndexStride)
	calendar := options.Calendar
	if options.CalendarName != "" {
		id := CalendarID(options.CalendarName)
		if id == 0 {
			return cfg, fmt.Errorf("unknown calendar name %q: %w", options.CalendarName, ErrUnknownCalendar)
		}
		if calendar != 0 && calendar != id {
			return cfg, fmt.Errorf("Options.Calendar (%d) and Options.CalendarName (%q = %d) disagree", calendar, options.CalendarName, id)
		}
		calendar = id
	}
	cfg.calendar = C.uint32_t(calendar)
	return cfg, nil
}

// cSchema is the C image of a schema; free releases the field-name strings.
type cSchema struct {
	fields []C.CField
}

func newCSchema(schema []Field) *cSchema {
	s := &cSchema{fields: make([]C.CField, len(schema))}
	for i, field := range schema {
		s.fields[i].name = C.CString(field.Name)
		s.fields[i]._type = C.int(field.Type)
	}
	return s
}

func (s *cSchema) ptr() *C.CField {
	if len(s.fields) == 0 {
		return nil
	}
	return &s.fields[0]
}

func (s *cSchema) free() {
	for i := range s.fields {
		C.free(unsafe.Pointer(s.fields[i].name))
	}
}

// open runs one of the C open functions and turns a NULL handle into an error
// carrying hocdb_last_error() ("DatabaseLocked", "SchemaMismatch",
// "ChecksumMismatch", "LegacyFormatNeedsMigration", ...).
func open(what, ticker, path string, schema []Field, call func(*C.char, *C.char, *C.CField, C.size_t) C.HOCDBHandle) (*DB, error) {
	// hocdb_last_error reports the last failed open on the calling OS thread:
	// stay on one thread between the open and the lookup.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	tickerC := C.CString(ticker)
	defer C.free(unsafe.Pointer(tickerC))
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))
	cs := newCSchema(schema)
	defer cs.free()

	handle := call(tickerC, pathC, cs.ptr(), C.size_t(len(schema)))
	if handle == nil {
		name := C.GoString(C.hocdb_last_error())
		if name == "" {
			return nil, fmt.Errorf("failed to %s HOCDB %q in %q", what, ticker, path)
		}
		return nil, fmt.Errorf("failed to %s HOCDB %q in %q: %s", what, ticker, path, name)
	}

	fieldMap := make(map[string]int, len(schema))
	for i, field := range schema {
		fieldMap[field.Name] = i
	}
	return &DB{handle: handle, fieldMap: fieldMap}, nil
}

// New opens or creates a database as its WRITER. A writer holds an exclusive
// lock on the file: a second writer on the same ticker and path fails
// immediately with an error naming "DatabaseLocked" (other names:
// "SchemaMismatch", "ChecksumMismatch" with Options.VerifyOnOpen,
// "LegacyFormatNeedsMigration" with Options.DisableAutoMigrate). Records
// written after the last commit by a crashed writer are adopted on open when
// they are complete and in timestamp order; torn or misordered bytes are
// truncated (see Metrics recovered_tail_records / dropped_tail_bytes).
func New(ticker, path string, schema []Field, options Options) (*DB, error) {
	cfg, err := cConfig(options)
	if err != nil {
		return nil, err
	}
	return open("open", ticker, path, schema, func(t, p *C.char, s *C.CField, n C.size_t) C.HOCDBHandle {
		return C.hocdb_init_ex(t, p, s, n, &cfg)
	})
}

// OpenReader attaches to a database that another process (or goroutine)
// writes, without taking any lock. Every read entry point re-reads the
// writer's committed cursor, so only flushed data is visible; Refresh does it
// explicitly. Readers follow files the writer compacts or rolls over. Append,
// Flush, Sync, Compact, RetainLast and Rollover return an error wrapping
// ErrReadOnly. The file must be in the current format: legacy HOC1 files are
// migrated the first time a writer opens them.
func OpenReader(ticker, path string, schema []Field) (*DB, error) {
	return open("open reader for", ticker, path, schema, func(t, p *C.char, s *C.CField, n C.size_t) C.HOCDBHandle {
		return C.hocdb_open_reader(t, p, s, n)
	})
}

// HeaderSize returns the number of bytes reserved by the file header (64):
// a ring buffer of Options.MaxFileSize = HeaderSize() + N * recordSize holds
// exactly N records.
func HeaderSize() int {
	return int(C.hocdb_header_size())
}

// opError maps the return code of a maintenance call to an error (nil for 0).
func opError(op string, rc C.int) error {
	switch rc {
	case 0:
		return nil
	case -1:
		return fmt.Errorf("%s failed: out of memory or I/O error", op)
	case -2:
		return fmt.Errorf("%s failed: invalid parameter", op)
	case -10:
		return fmt.Errorf("%s failed: %w", op, ErrReadOnly)
	case -11:
		return fmt.Errorf("%s failed: DatabaseLocked (another writer holds the file)", op)
	case -12:
		return fmt.Errorf("%s failed: ChecksumMismatch", op)
	case -20:
		return fmt.Errorf("%s failed: %w", op, ErrChecksumUnavailable)
	case -21:
		return fmt.Errorf("%s failed: the database is empty", op)
	case -30:
		return fmt.Errorf("%s failed: %w", op, ErrCalendarRequired)
	case -31:
		return fmt.Errorf("%s failed: %w", op, ErrUnknownCalendar)
	default:
		return fmt.Errorf("%s failed with code %d", op, int(rc))
	}
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
		switch result {
		case -2:
			return errors.New("append failed: invalid record size")
		case -3:
			return errors.New("append failed: timestamp not monotonic - timestamps must be strictly increasing")
		case -10:
			return fmt.Errorf("append failed: %w", ErrReadOnly)
		}
		return errors.New("failed to append data to HOCDB")
	}

	return nil
}

// Flush writes all pending data to the file and commits it: the records
// become visible to readers. Whether the bytes are also fsync'ed depends on
// Options.Fsync (use Sync to force it). On a reader Flush behaves like Refresh.
func (db *DB) Flush() error {
	if db.handle == nil {
		return errNotInitialized
	}

	result := C.hocdb_flush(db.handle)

	if result != 0 {
		if C.hocdb_is_read_only(db.handle) != 0 {
			return fmt.Errorf("flush failed: %w", ErrReadOnly)
		}
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
func (db *DB) GetStats(startTs, endTs int64, fieldIndex int, computePercentiles bool) (*Stats, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}

	flags := C.uint32_t(0)
	if computePercentiles {
		flags = 1
	}

	var outStats C.HOCDBStats
	result := C.hocdb_get_stats(
		db.handle,
		C.int64_t(startTs),
		C.int64_t(endTs),
		C.size_t(fieldIndex),
		flags,
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
		P50:   float64(outStats.p50),
		P90:   float64(outStats.p90),
		P95:   float64(outStats.p95),
		P99:   float64(outStats.p99),
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

// GetStatsByName returns statistics for a specific field by name within a time range
func (db *DB) GetStatsByName(startTs, endTs int64, fieldName string, computePercentiles bool) (*Stats, error) {
	idx, ok := db.fieldMap[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field: %s", fieldName)
	}
	return db.GetStats(startTs, endTs, idx, computePercentiles)
}

// GetLatestByName returns the latest value and timestamp for a specific field by name
func (db *DB) GetLatestByName(fieldName string) (*Latest, error) {
	idx, ok := db.fieldMap[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field: %s", fieldName)
	}
	return db.GetLatest(idx)
}

// Close closes the database connection and frees resources
func (db *DB) Close() {
	if db.handle != nil {
		C.hocdb_close(db.handle)
		db.handle = nil
	}
}

// Drop closes the database and deletes the data file
func (db *DB) Drop() {
	if db.handle != nil {
		C.hocdb_drop(db.handle)
		db.handle = nil
	}
}

// ---------------------------------------------------------------------------
// Durability, readers, maintenance and metrics
// ---------------------------------------------------------------------------

// Sync flushes (commits) pending data and fsyncs the file now, regardless of
// Options.Fsync. Readers get an error wrapping ErrReadOnly.
func (db *DB) Sync() error {
	if db.handle == nil {
		return errNotInitialized
	}
	return opError("sync", C.hocdb_sync(db.handle))
}

// Refresh makes a reader pick up the writer's latest commit. Every read
// already does this implicitly; Refresh is for callers that want to observe
// the cursor move explicitly. It is a no-op for writers.
func (db *DB) Refresh() error {
	if db.handle == nil {
		return errNotInitialized
	}
	return opError("refresh", C.hocdb_refresh(db.handle))
}

// Verify recomputes the CRC32C checksum of the committed data and compares it
// with the stored one: true when they match, false on a MISMATCH. Ring
// buffers and legacy HOC1 files carry no checksum; Verify then returns an
// error wrapping ErrChecksumUnavailable.
func (db *DB) Verify() (bool, error) {
	if db.handle == nil {
		return false, errNotInitialized
	}
	rc := C.hocdb_verify(db.handle)
	switch rc {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, opError("verify", rc)
}

// Compact rewrites the file keeping only the records with timestamp >= minTs.
// Readers follow the rewritten file automatically. Readers get an error
// wrapping ErrReadOnly.
func (db *DB) Compact(minTs int64) error {
	if db.handle == nil {
		return errNotInitialized
	}
	return opError("compact", C.hocdb_compact(db.handle, C.int64_t(minTs)))
}

// RetainLast rewrites the file keeping only the last n records. Readers get
// an error wrapping ErrReadOnly.
func (db *DB) RetainLast(n uint64) error {
	if db.handle == nil {
		return errNotInitialized
	}
	return opError("retain_last", C.hocdb_retain_last(db.handle, C.uint64_t(n)))
}

// Rollover archives the current file as <ticker>.<first_ts>-<last_ts>.bin in
// the data directory and continues with an empty file; timestamps stay
// monotonic across the files. It returns the path of the archive, which can
// be opened as an ordinary database (ticker = archive file name without
// ".bin", same path). Readers get an error wrapping ErrReadOnly.
func (db *DB) Rollover() (string, error) {
	if db.handle == nil {
		return "", errNotInitialized
	}
	buf := make([]byte, 4096)
	out := (*C.char)(unsafe.Pointer(&buf[0]))
	if err := opError("rollover", C.hocdb_rollover(db.handle, out, C.size_t(len(buf)))); err != nil {
		return "", err
	}
	return C.GoString(out), nil
}

// Metrics returns the operational counters of the handle by name (the 30
// fields of HOCDBMetrics, decoded through the library's introspection API):
// appends, bytes_written, flushes, commits, fsyncs, fsync_ns_total,
// fsync_ns_max, reads, read_ns_total, read_ns_max, read_ns_last, read_ns_p50,
// read_ns_p99, records_read, refreshes, recovered_tail_records,
// dropped_tail_bytes, crc_failures, compactions, rollovers, migrations,
// last_append_wall_ns, last_commit_wall_ns, last_record_ts,
// ingest_lag_wall_ns, ingest_lag_record_ns, committed_records, file_size,
// format_version and read_only. Every value is returned as int64 (the uint64
// counters fit; a double field, should one be added, is truncated).
func (db *DB) Metrics() (map[string]int64, error) {
	if db.handle == nil {
		return nil, errNotInitialized
	}

	size := C.hocdb_metrics_size()
	buf := C.malloc(size)
	defer C.free(buf)

	if err := opError("metrics", C.hocdb_metrics(db.handle, (*C.HOCDBMetrics)(buf))); err != nil {
		return nil, err
	}

	raw := C.GoBytes(buf, C.int(size))
	n := int(C.hocdb_metrics_field_count())
	out := make(map[string]int64, n)
	for i := 0; i < n; i++ {
		off := int(C.hocdb_metrics_field_offset(C.size_t(i)))
		if off < 0 || off+8 > len(raw) {
			continue
		}
		name := C.GoString(C.hocdb_metrics_field_name(C.size_t(i)))
		bits := binary.LittleEndian.Uint64(raw[off : off+8])
		switch C.hocdb_metrics_field_type(C.size_t(i)) {
		case 1, 3: // int64, uint64
			out[name] = int64(bits)
		case 2: // double
			out[name] = int64(math.Float64frombits(bits))
		}
	}
	return out, nil
}

// MetricsReset zeroes the counters of Metrics; state fields such as
// last_record_ts, committed_records, file_size, format_version and read_only
// keep their values.
func (db *DB) MetricsReset() {
	if db.handle != nil {
		C.hocdb_metrics_reset(db.handle)
	}
}

// FormatVersion returns the on-disk format of the open file: 2 for the
// current 64-byte "HOC2" header, 1 for a legacy "HOC1" file (only possible
// with Options.DisableAutoMigrate). 0 when the database is closed.
func (db *DB) FormatVersion() int {
	if db.handle == nil {
		return 0
	}
	return int(C.hocdb_format_version(db.handle))
}

// IsReadOnly reports whether the handle was opened with OpenReader.
func (db *DB) IsReadOnly() bool {
	if db.handle == nil {
		return false
	}
	return C.hocdb_is_read_only(db.handle) != 0
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

// ---------------------------------------------------------------------------
// Technical indicators and quantitative analytics
// ---------------------------------------------------------------------------

// IndicatorColumns names the fields that play the OHLCV and tick-quote roles
// for the indicator functions. Close is required; leave the others "" when the
// schema has no such field (indicators that need them then return an error).
// Bid, Ask and Side (1/true = buy, 0/false = sell) are tick-level roles used by
// the microstructure kinds; Side also yields per-bar buy volume when bucketing.
type IndicatorColumns struct {
	Open   string
	High   string
	Low    string
	Close  string
	Volume string
	Bid    string
	Ask    string
	Side   string
}

// IndicatorSpec describes one indicator to compute. Zero periods/params select
// the documented defaults (RSI 14, MACD 12/26/9, BBANDS 20 x 2.0, ...); see
// INDICATORS.md at the repository root for the full table.
type IndicatorSpec struct {
	Kind    string  // Indicator name, case-insensitive ("sma", "rsi", "macd", ...); see IndicatorKinds
	Period  int     // 0 = default; the horizon for forward_return / triple_barrier; rows for opening_range
	Period2 int     // Second period (MACD slow, STOCH %D, ...)
	Period3 int     // Third period (MACD signal, ULTOSC, ...)
	Period4 int     // Fourth period (STOCH_RSI %D, ICHIMOKU displacement)
	Param   float64 // BBANDS k, KELTNER/SUPERTREND multiplier, PSAR acceleration, periods-per-year for HIST_VOL/SHARPE/SORTINO/REALIZED_VOL, timestamp units per second for TRADE_INTENSITY (default 1e6), up-barrier fraction for TRIPLE_BARRIER (default 0.02), session length in timestamp units for SESSION_VWAP/SESSION_RANGE/OPENING_RANGE/PIVOTS (0 = the handle's trading calendar sessions; ErrCalendarRequired without one)
	Param2  float64 // PSAR max acceleration, TRIPLE_BARRIER down-barrier fraction (default = Param), session offset in timestamp units for the session kinds
	Field   string  // "" = the close column; otherwise run a single-series indicator on this field
	Field2  string  // Second series for SERIES2/RATIO/RATIO_ZSCORE/REL_STRENGTH/CORREL/BETA in single-database calls; in PairIndicators the other database's close is the second series
	Label   string  // Output name; "" = kind name plus "_<Period>" when Period > 0 (e.g. "sma_20", "rsi", "macd")
}

// LookbackAuto can be passed through Lookback to request the recommended
// per-spec warm-up explicitly (the same as leaving IndicatorOptions.Lookback nil).
const LookbackAuto = -1

// Lookback returns a pointer suitable for IndicatorOptions.Lookback, e.g.
// hocdb.Lookback(0) to disable the automatic warm-up.
func Lookback(n int) *int { return &n }

// IndicatorOptions configures Indicators and IndicatorsTail.
type IndicatorOptions struct {
	// Columns maps the OHLCV / quote roles to schema fields. nil = auto-detect
	// fields named open/high/low/close/volume/bid/ask/side; a field named
	// "price" is used as close when there is no "close" field, and "size" or
	// "qty" as volume when there is no "volume" field.
	Columns *IndicatorColumns
	// Lookback is the number of extra records (bars when Bucket > 0) read
	// before the window so that the first in-window values are converged.
	// nil or a negative value (LookbackAuto) = the recommended per-spec
	// warm-up; 0 = none (the first rows of the window are NaN while the
	// indicator warms up); n > 0 = exactly n rows. Warm-up rows are not returned.
	Lookback *int
	// Bucket = 0: one output row per record. Bucket > 0: records are first
	// aggregated into OHLCV bars of that many timestamp units (tick -> bar);
	// per-spec Field overrides are rejected in that mode.
	Bucket int64
}

// PairOptions configures PairIndicators and PairIndicatorsTail. The embedded
// IndicatorOptions apply to the receiver database (its Columns, Lookback and
// Bucket); OtherColumns maps the roles of the other database (nil =
// auto-detect there, like IndicatorOptions.Columns).
type PairOptions struct {
	IndicatorOptions
	OtherColumns *IndicatorColumns
}

// IndicatorResult is a batch of computed indicator series. Every slice has
// NRows elements; NaN marks the warm-up region where a value is not yet defined.
type IndicatorResult struct {
	Timestamps []int64
	NRows      int
	Columns    map[string][]float64 // Output name -> series (see IndicatorSpec.Label for the naming rule)
	Names      []string             // Output names in spec / output order
}

// Bars holds OHLCV bars produced by OHLCV and OHLCVSide. All non-nil slices
// have the same length.
type Bars struct {
	Timestamps []int64 // Bar start (aligned to the bucket)
	Open       []float64
	High       []float64
	Low        []float64
	Close      []float64
	Volume     []float64 // Record count per bar when no volume field is given
	Count      []float64 // Records per bar
	BuyVolume  []float64 // Volume of the buy-side records per bar (OHLCVSide); nil when no side field was given
}

// SnapshotOptions configures Snapshot.
type SnapshotOptions struct {
	Columns        *IndicatorColumns // nil = auto-detect (see IndicatorOptions.Columns)
	Bars           int               // Records (bars when Bucket > 0) to use; 0 = recommended (2500, enough for every field to converge)
	Bucket         int64             // 0 = one bar per record; > 0 = aggregate records into bars of this many timestamp units first
	PeriodsPerYear float64           // Annualisation for volatility / Sharpe / Sortino; 0 = none
}

// SnapshotMultiOptions configures SnapshotMulti: one snapshot per entry of
// Buckets, all computed from a single read of the data.
type SnapshotMultiOptions struct {
	Columns        *IndicatorColumns // nil = auto-detect (see IndicatorOptions.Columns)
	Bars           int               // Bars per snapshot; 0 = recommended (2500)
	Buckets        []int64           // Bar size (timestamp units) of each snapshot; at least one; 0 = one bar per record
	PeriodsPerYear []float64         // Annualisation of each snapshot (same length as Buckets); nil = none
}

// Snapshot is a one-shot view of ~100 indicators for the latest bar.
type Snapshot struct {
	Timestamp int64              // Timestamp of the latest bar
	Bars      uint64             // Number of bars the snapshot was computed from
	Fields    map[string]float64 // Every other snapshot field by name (open, close, rsi_14, ema_200, macd, ...); NaN = not enough data
}

// Decision is one trading decision for Evaluate: entry at the first price at
// or after Timestamp, exit at the first price at or after Timestamp + Horizon.
type Decision struct {
	Timestamp int64   // Entry time (timestamp units of the database)
	Direction float64 // +1 long, -1 short, 0 flat (counted in n_decisions but not evaluated)
	Size      float64 // Position size in currency units; 0 = 1
	Horizon   int64   // Holding period in timestamp units; 0 = the default horizon passed to Evaluate
}

// Evaluation is the result of Evaluate: the aggregate statistics by name plus
// the per-decision entry price, exit price and net return (NaN where a
// decision could not be evaluated, e.g. flat or beyond the end of the data).
type Evaluation struct {
	Fields    map[string]float64 // n_decisions, n_evaluated, n_long, n_short, hit_rate, avg_return, avg_net_return, total_pnl, total_cost, sharpe, profit_factor, max_drawdown, avg_win, avg_loss, best, worst, long_/short_hit_rate, long_/short_avg_return
	Entry     []float64          // One entry per decision, in input order
	Exit      []float64
	NetReturn []float64 // Return after costs (cost_bps per side)
}

// indicatorError maps the C error codes of the indicator API to errors.
func indicatorError(rc C.int) error {
	switch rc {
	case -1:
		return errors.New("indicator computation failed: out of memory")
	case -2:
		return errors.New("invalid indicator spec: unknown kind or bad period/parameter")
	case -3:
		return errors.New("missing column: an indicator needs a column that is not available (close is required; e.g. ATR/ADX/STOCH need high and low, OBV/MFI/VWAP need volume, SPREAD needs bid and ask, ORDER_FLOW/TICK_PRESSURE need side)")
	case -4:
		return errors.New("invalid field index for an indicator column or field override")
	case -5:
		return errors.New("per-spec field overrides are not supported when bucket > 0")
	case -6:
		return errors.New("too many columns")
	case -7:
		return errors.New("series length mismatch")
	case -30:
		return ErrCalendarRequired
	case -31:
		return ErrUnknownCalendar
	default:
		return fmt.Errorf("indicator computation failed with code %d", int(rc))
	}
}

// indicatorKind resolves an indicator name to its stable id.
func indicatorKind(name string) (C.uint32_t, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	kind := C.hocdb_indicator_kind_from_name(cName)
	if kind == 0 {
		return 0, fmt.Errorf("unknown indicator kind: %q", name)
	}
	return kind, nil
}

// fieldIndex looks a schema field up by name.
func (db *DB) fieldIndex(name string) (int64, error) {
	idx, ok := db.fieldMap[name]
	if !ok {
		return -1, fmt.Errorf("unknown field: %s", name)
	}
	return int64(idx), nil
}

// optionalFieldIndex is fieldIndex with "" mapping to -1 (role absent).
func (db *DB) optionalFieldIndex(name string) (int64, error) {
	if name == "" {
		return -1, nil
	}
	return db.fieldIndex(name)
}

// resolveColumns converts IndicatorColumns (or auto-detects them) into the C
// struct of field indices (all eight roles; -1 = absent).
func (db *DB) resolveColumns(cols *IndicatorColumns) (C.HOCDBIndicatorColumns, error) {
	var out C.HOCDBIndicatorColumns
	out.open, out.high, out.low, out.close, out.volume, out.bid, out.ask, out.side = -1, -1, -1, -1, -1, -1, -1, -1

	if cols == nil {
		lookup := func(names ...string) C.int64_t {
			for _, name := range names {
				if idx, ok := db.fieldMap[name]; ok {
					return C.int64_t(idx)
				}
			}
			return -1
		}
		out.open = lookup("open")
		out.high = lookup("high")
		out.low = lookup("low")
		out.close = lookup("close", "price")
		out.volume = lookup("volume", "size", "qty")
		out.bid = lookup("bid")
		out.ask = lookup("ask")
		out.side = lookup("side")
		if out.close < 0 {
			return out, errors.New("no close column found: the schema has no field named \"close\" or \"price\"; set Columns explicitly")
		}
		return out, nil
	}

	if cols.Close == "" {
		return out, errors.New("missing close column: IndicatorColumns.Close is required")
	}
	roles := []struct {
		dst  *C.int64_t
		name string
	}{
		{&out.open, cols.Open}, {&out.high, cols.High}, {&out.low, cols.Low}, {&out.close, cols.Close},
		{&out.volume, cols.Volume}, {&out.bid, cols.Bid}, {&out.ask, cols.Ask}, {&out.side, cols.Side},
	}
	for _, r := range roles {
		idx, err := db.optionalFieldIndex(r.name)
		if err != nil {
			return out, err
		}
		*r.dst = C.int64_t(idx)
	}
	return out, nil
}

// convertSpec validates an IndicatorSpec and converts it to the C struct.
func (db *DB) convertSpec(spec IndicatorSpec) (C.HOCDBIndicatorSpec, error) {
	var out C.HOCDBIndicatorSpec
	kind, err := indicatorKind(spec.Kind)
	if err != nil {
		return out, err
	}
	if spec.Period < 0 || spec.Period2 < 0 || spec.Period3 < 0 || spec.Period4 < 0 {
		return out, errors.New("indicator periods must be >= 0")
	}
	out.kind = kind
	out.period = C.uint32_t(spec.Period)
	out.period2 = C.uint32_t(spec.Period2)
	out.period3 = C.uint32_t(spec.Period3)
	out.period4 = C.uint32_t(spec.Period4)
	out.param = C.double(spec.Param)
	out.param2 = C.double(spec.Param2)
	out.field_index = -1
	out.field_index2 = -1
	if spec.Field != "" {
		idx, err := db.fieldIndex(spec.Field)
		if err != nil {
			return out, err
		}
		out.field_index = C.int64_t(idx)
	}
	if spec.Field2 != "" {
		idx, err := db.fieldIndex(spec.Field2)
		if err != nil {
			return out, err
		}
		out.field_index2 = C.int64_t(idx)
	}
	return out, nil
}

// indicatorColumnNames applies the naming rule: label = Label, or the kind
// name plus "_<Period>" when Period > 0. Single-output kinds use the label as
// the column name; multi-output kinds use "<label>_<output>", except that the
// output named like the kind itself (macd, ppo, adx, tsi) is just the label
// (e.g. macd, macd_signal, macd_hist).
func indicatorColumnNames(spec IndicatorSpec, kind C.uint32_t) []string {
	kindName := C.GoString(C.hocdb_indicator_name(kind))
	label := spec.Label
	if label == "" {
		label = kindName
		if spec.Period > 0 {
			label = fmt.Sprintf("%s_%d", label, spec.Period)
		}
	}
	n := int(C.hocdb_indicator_output_count(kind))
	if n <= 1 {
		return []string{label}
	}
	names := make([]string, n)
	for i := 0; i < n; i++ {
		output := C.GoString(C.hocdb_indicator_output_name(kind, C.size_t(i)))
		if output == kindName {
			names[i] = label
		} else {
			names[i] = label + "_" + output
		}
	}
	return names
}

// runIndicators is the shared implementation of Indicators, IndicatorsTail,
// PairIndicators and PairIndicatorsTail. other == nil runs the single-database
// calls; otherwise the receiver is database A and other is database B.
func (db *DB) runIndicators(other *DB, tail bool, nLast int, startTs, endTs int64, specs []IndicatorSpec, opts *IndicatorOptions, otherColumns *IndicatorColumns) (*IndicatorResult, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	if other != nil && other.handle == nil {
		return nil, errors.New("other database not initialized")
	}
	if len(specs) == 0 {
		return nil, errors.New("no indicator specs given")
	}
	if opts == nil {
		opts = &IndicatorOptions{}
	}
	if opts.Bucket < 0 {
		return nil, errors.New("bucket must be >= 0")
	}
	if tail && nLast < 0 {
		return nil, errors.New("tail length must be >= 0")
	}

	cols, err := db.resolveColumns(opts.Columns)
	if err != nil {
		return nil, err
	}
	var otherCols C.HOCDBIndicatorColumns
	if other != nil {
		if otherCols, err = other.resolveColumns(otherColumns); err != nil {
			return nil, fmt.Errorf("other database: %w", err)
		}
	}

	cSpecs := make([]C.HOCDBIndicatorSpec, len(specs))
	names := make([]string, 0, len(specs))
	seen := make(map[string]bool, len(specs))
	for i, spec := range specs {
		cs, err := db.convertSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("indicator spec %d: %w", i, err)
		}
		cSpecs[i] = cs
		for _, name := range indicatorColumnNames(spec, cs.kind) {
			if seen[name] {
				return nil, fmt.Errorf("indicator spec %d: duplicate output column %q; set IndicatorSpec.Label", i, name)
			}
			seen[name] = true
			names = append(names, name)
		}
	}

	lookback := ^C.size_t(0) // HOCDB_LOOKBACK_AUTO
	if opts.Lookback != nil && *opts.Lookback >= 0 {
		lookback = C.size_t(*opts.Lookback)
	}
	nSpecs := C.size_t(len(cSpecs))
	bucket := C.int64_t(opts.Bucket)

	var res C.HOCDBIndicatorResult
	var rc C.int
	switch {
	case other != nil && tail:
		rc = C.hocdb_pair_indicators_tail(db.handle, &cols, other.handle, &otherCols, C.size_t(nLast), &cSpecs[0], nSpecs, lookback, bucket, &res)
	case other != nil:
		rc = C.hocdb_pair_indicators(db.handle, &cols, other.handle, &otherCols, C.int64_t(startTs), C.int64_t(endTs), &cSpecs[0], nSpecs, lookback, bucket, &res)
	case tail:
		rc = C.hocdb_indicators_tail(db.handle, C.size_t(nLast), &cols, &cSpecs[0], nSpecs, lookback, bucket, &res)
	default:
		rc = C.hocdb_indicators(db.handle, C.int64_t(startTs), C.int64_t(endTs), &cols, &cSpecs[0], nSpecs, lookback, bucket, &res)
	}
	if rc != 0 {
		return nil, indicatorError(rc)
	}
	defer C.hocdb_indicators_free(&res)

	nRows := int(res.n_rows)
	nOutputs := int(res.n_outputs)
	if nOutputs != len(names) {
		return nil, fmt.Errorf("unexpected number of indicator outputs: got %d, expected %d", nOutputs, len(names))
	}

	// Copy everything out of C memory before hocdb_indicators_free runs.
	out := &IndicatorResult{
		Timestamps: make([]int64, nRows),
		NRows:      nRows,
		Columns:    make(map[string][]float64, len(names)),
		Names:      names,
	}
	if nRows > 0 && res.timestamps != nil {
		copy(out.Timestamps, unsafe.Slice((*int64)(unsafe.Pointer(res.timestamps)), nRows))
	}
	var values []float64 // planar: output k is values[k*nRows : (k+1)*nRows]
	if nRows > 0 && nOutputs > 0 && res.values != nil {
		values = unsafe.Slice((*float64)(unsafe.Pointer(res.values)), nRows*nOutputs)
	}
	for k, name := range names {
		col := make([]float64, nRows)
		if values != nil {
			copy(col, values[k*nRows:(k+1)*nRows])
		}
		out.Columns[name] = col
	}
	return out, nil
}

// Indicators computes a batch of indicators over the time window
// [startTs, endTs) in a single pass over the data. opts may be nil.
func (db *DB) Indicators(specs []IndicatorSpec, startTs, endTs int64, opts *IndicatorOptions) (*IndicatorResult, error) {
	return db.runIndicators(nil, false, 0, startTs, endTs, specs, opts, nil)
}

// IndicatorsTail computes a batch of indicators for the last n records (or
// the last n bars when opts.Bucket > 0). opts may be nil.
func (db *DB) IndicatorsTail(n int, specs []IndicatorSpec, opts *IndicatorOptions) (*IndicatorResult, error) {
	return db.runIndicators(nil, true, n, 0, 0, specs, opts, nil)
}

// pairOptions splits PairOptions into the receiver's options and the other
// database's column mapping.
func pairOptions(opts *PairOptions) (*IndicatorOptions, *IndicatorColumns) {
	if opts == nil {
		return nil, nil
	}
	return &opts.IndicatorOptions, opts.OtherColumns
}

// PairIndicators computes indicators over this database (A) aligned with
// another open database (B) over [startTs, endTs): B's close column is the
// second series of series2, ratio, ratio_zscore, rel_strength, correl and
// beta, while single-series kinds run on A. With opts.Bucket > 0 both are
// resampled to bars and inner-joined on bar timestamps; on ticks B is as-of
// joined onto A's rows (latest B row at or before each A row). opts may be nil.
func (db *DB) PairIndicators(other *DB, specs []IndicatorSpec, startTs, endTs int64, opts *PairOptions) (*IndicatorResult, error) {
	if other == nil {
		return nil, errors.New("other database is nil")
	}
	io, oc := pairOptions(opts)
	return db.runIndicators(other, false, 0, startTs, endTs, specs, io, oc)
}

// PairIndicatorsTail is PairIndicators for the last n rows (or bars when
// opts.Bucket > 0) of this database. opts may be nil.
func (db *DB) PairIndicatorsTail(other *DB, n int, specs []IndicatorSpec, opts *PairOptions) (*IndicatorResult, error) {
	if other == nil {
		return nil, errors.New("other database is nil")
	}
	io, oc := pairOptions(opts)
	return db.runIndicators(other, true, n, 0, 0, specs, io, oc)
}

// OHLCV aggregates the records in [startTs, endTs) into bars of `bucket`
// timestamp units using priceField as the price. volumeField may be "" (the
// Volume slice then holds the record count per bar). Bars.BuyVolume is nil;
// use OHLCVSide to get it.
func (db *DB) OHLCV(startTs, endTs int64, bucket int64, priceField string, volumeField string) (*Bars, error) {
	return db.ohlcv(startTs, endTs, bucket, priceField, volumeField, "")
}

// OHLCVSide is OHLCV with a side field (true/1 = buy): the returned bars also
// carry BuyVolume, the volume (or record count when volumeField is "") of the
// buy-side records in each bar. sideField "" behaves like OHLCV.
func (db *DB) OHLCVSide(startTs, endTs int64, bucket int64, priceField, volumeField, sideField string) (*Bars, error) {
	return db.ohlcv(startTs, endTs, bucket, priceField, volumeField, sideField)
}

func (db *DB) ohlcv(startTs, endTs int64, bucket int64, priceField, volumeField, sideField string) (*Bars, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	if bucket <= 0 {
		return nil, errors.New("bucket must be > 0")
	}
	price, err := db.fieldIndex(priceField)
	if err != nil {
		return nil, err
	}
	volume, err := db.optionalFieldIndex(volumeField)
	if err != nil {
		return nil, err
	}
	side, err := db.optionalFieldIndex(sideField)
	if err != nil {
		return nil, err
	}

	var cb C.HOCDBBarsEx
	rc := C.hocdb_ohlcv_ex(db.handle, C.int64_t(startTs), C.int64_t(endTs), C.size_t(price), C.int64_t(volume), C.int64_t(side), C.int64_t(bucket), &cb)
	if rc != 0 {
		return nil, indicatorError(rc)
	}
	defer C.hocdb_ohlcv_ex_free(&cb)

	n := int(cb.n_bars)
	bars := &Bars{
		Timestamps: make([]int64, n),
		Open:       make([]float64, n),
		High:       make([]float64, n),
		Low:        make([]float64, n),
		Close:      make([]float64, n),
		Volume:     make([]float64, n),
		Count:      make([]float64, n),
	}
	if side >= 0 {
		bars.BuyVolume = make([]float64, n)
	}
	if n == 0 {
		return bars, nil
	}
	if cb.timestamps != nil {
		copy(bars.Timestamps, unsafe.Slice((*int64)(unsafe.Pointer(cb.timestamps)), n))
	}
	copyDoubles := func(dst []float64, src *C.double) {
		if src != nil && dst != nil {
			copy(dst, unsafe.Slice((*float64)(unsafe.Pointer(src)), n))
		}
	}
	copyDoubles(bars.Open, cb.open)
	copyDoubles(bars.High, cb.high)
	copyDoubles(bars.Low, cb.low)
	copyDoubles(bars.Close, cb.close)
	copyDoubles(bars.Volume, cb.volume)
	copyDoubles(bars.Count, cb.count)
	copyDoubles(bars.BuyVolume, cb.buy_volume)
	return bars, nil
}

// decodeIntrospectedField reads one field of a C struct image by offset and
// introspection type (1 = int64, 2 = double, 3 = uint64) as a float64.
func decodeIntrospectedField(raw []byte, off, typ int) (float64, bool) {
	if off < 0 || off+8 > len(raw) {
		return 0, false
	}
	bits := binary.LittleEndian.Uint64(raw[off : off+8])
	switch typ {
	case 1:
		return float64(int64(bits)), true
	case 2:
		return math.Float64frombits(bits), true
	case 3:
		return float64(bits), true
	}
	return 0, false
}

// introspectedFields decodes a C struct image into a name -> value map using
// the library's field introspection (count / name / offset / type). Integer
// fields are exact in float64 up to 2^53.
func introspectedFields(raw []byte, n int, name func(int) string, offset func(int) int, typ func(int) int) map[string]float64 {
	out := make(map[string]float64, n)
	for i := 0; i < n; i++ {
		if v, ok := decodeIntrospectedField(raw, offset(i), typ(i)); ok {
			out[name(i)] = v
		}
	}
	return out
}

// Summary computes a scalar performance / risk summary (count, mean, std,
// total_return, sharpe, max_drawdown, win_rate, hurst, ...) of a field over
// [startTs, endTs). periodsPerYear annualises returns and volatility (0 = no
// annualisation). The field names are those reported by the C library's
// introspection API; count is returned as a float64.
func (db *DB) Summary(startTs, endTs int64, field string, periodsPerYear float64) (map[string]float64, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	idx, err := db.fieldIndex(field)
	if err != nil {
		return nil, err
	}

	size := C.hocdb_summary_size()
	buf := C.malloc(size)
	defer C.free(buf)

	rc := C.hocdb_summary(db.handle, C.int64_t(startTs), C.int64_t(endTs), C.size_t(idx), C.double(periodsPerYear), (*C.HOCDBSummary)(buf))
	if rc != 0 {
		return nil, indicatorError(rc)
	}

	raw := C.GoBytes(buf, C.int(size))
	return introspectedFields(raw, int(C.hocdb_summary_field_count()),
		func(i int) string { return C.GoString(C.hocdb_summary_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_summary_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_summary_field_type(C.size_t(i))) }), nil
}

// Health computes data-quality statistics of the records in [startTs, endTs):
// count, first_ts, last_ts, span, mean_gap, median_gap, max_gap, max_gap_at,
// n_gaps (gaps above gapThreshold timestamp units), n_nonpositive_price,
// n_nan_price, n_outlier_returns (|log return| above outlierThreshold),
// first_outlier_at, max_abs_return, n_zero_volume and n_negative_volume (the
// volume counts need volumeField; "" = no volume). Timestamps and counts are
// returned as float64 and are exact up to 2^53.
func (db *DB) Health(startTs, endTs int64, priceField, volumeField string, gapThreshold int64, outlierThreshold float64) (map[string]float64, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	price, err := db.fieldIndex(priceField)
	if err != nil {
		return nil, err
	}
	volume, err := db.optionalFieldIndex(volumeField)
	if err != nil {
		return nil, err
	}

	size := C.hocdb_health_size()
	buf := C.malloc(size)
	defer C.free(buf)

	rc := C.hocdb_health(db.handle, C.int64_t(startTs), C.int64_t(endTs), C.size_t(price), C.int64_t(volume), C.int64_t(gapThreshold), C.double(outlierThreshold), (*C.HOCDBHealth)(buf))
	if rc != 0 {
		return nil, indicatorError(rc)
	}

	raw := C.GoBytes(buf, C.int(size))
	return introspectedFields(raw, int(C.hocdb_health_field_count()),
		func(i int) string { return C.GoString(C.hocdb_health_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_health_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_health_field_type(C.size_t(i))) }), nil
}

// Evaluate scores a list of trading decisions against the price history in
// priceField: each decision enters at the first price at or after its
// Timestamp and exits at the first price at or after Timestamp + Horizon
// (defaultHorizon when Horizon is 0), paying costBps basis points per side.
// A Decision.Size of 0 counts as 1. Decisions that cannot be evaluated (flat
// direction, or no price after the exit time) are counted in n_decisions
// only and have NaN entries in the per-decision slices. decisions may be empty.
func (db *DB) Evaluate(decisions []Decision, priceField string, defaultHorizon int64, costBps float64) (*Evaluation, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	price, err := db.fieldIndex(priceField)
	if err != nil {
		return nil, err
	}

	n := len(decisions)
	cDecisions := make([]C.HOCDBDecision, n)
	for i, d := range decisions {
		size := d.Size
		if size == 0 {
			size = 1
		}
		cDecisions[i] = C.HOCDBDecision{
			timestamp: C.int64_t(d.Timestamp),
			direction: C.double(d.Direction),
			size:      C.double(size),
			horizon:   C.int64_t(d.Horizon),
		}
	}
	ev := &Evaluation{
		Entry:     make([]float64, n),
		Exit:      make([]float64, n),
		NetReturn: make([]float64, n),
	}
	var decPtr *C.HOCDBDecision
	var entryPtr, exitPtr, netPtr *C.double
	if n > 0 {
		decPtr = &cDecisions[0]
		entryPtr = (*C.double)(unsafe.Pointer(&ev.Entry[0]))
		exitPtr = (*C.double)(unsafe.Pointer(&ev.Exit[0]))
		netPtr = (*C.double)(unsafe.Pointer(&ev.NetReturn[0]))
	}

	size := C.hocdb_evaluation_size()
	buf := C.malloc(size)
	defer C.free(buf)

	rc := C.hocdb_evaluate(db.handle, C.size_t(price), decPtr, C.size_t(n), C.int64_t(defaultHorizon), C.double(costBps), (*C.HOCDBEvaluation)(buf), entryPtr, exitPtr, netPtr)
	if rc != 0 {
		return nil, indicatorError(rc)
	}

	raw := C.GoBytes(buf, C.int(size))
	ev.Fields = introspectedFields(raw, int(C.hocdb_evaluation_field_count()),
		func(i int) string { return C.GoString(C.hocdb_evaluation_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_evaluation_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_evaluation_field_type(C.size_t(i))) })
	return ev, nil
}

// decodeSnapshot decodes one HOCDBSnapshot image using the introspection API.
func decodeSnapshot(raw []byte) *Snapshot {
	n := int(C.hocdb_snapshot_field_count())
	snap := &Snapshot{Fields: make(map[string]float64, n)}
	for i := 0; i < n; i++ {
		name := C.GoString(C.hocdb_snapshot_field_name(C.size_t(i)))
		off := int(C.hocdb_snapshot_field_offset(C.size_t(i)))
		typ := int(C.hocdb_snapshot_field_type(C.size_t(i)))
		if off < 0 || off+8 > len(raw) {
			continue
		}
		switch {
		case typ == 1 && name == "timestamp":
			snap.Timestamp = int64(binary.LittleEndian.Uint64(raw[off : off+8]))
		case typ == 3 && name == "bars":
			snap.Bars = binary.LittleEndian.Uint64(raw[off : off+8])
		default:
			if v, ok := decodeIntrospectedField(raw, off, typ); ok {
				snap.Fields[name] = v
			}
		}
	}
	return snap
}

// Snapshot computes ~100 indicators for the latest bar in one call (useful
// for dashboards and LLM agents). opts may be nil for the defaults: columns
// auto-detected, 2500 records, no bucketing, no annualisation.
func (db *DB) Snapshot(opts *SnapshotOptions) (*Snapshot, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	if opts == nil {
		opts = &SnapshotOptions{}
	}
	if opts.Bars < 0 {
		return nil, errors.New("bars must be >= 0")
	}
	if opts.Bucket < 0 {
		return nil, errors.New("bucket must be >= 0")
	}
	cols, err := db.resolveColumns(opts.Columns)
	if err != nil {
		return nil, err
	}

	size := C.hocdb_snapshot_size()
	buf := C.malloc(size)
	defer C.free(buf)

	rc := C.hocdb_snapshot(db.handle, &cols, C.size_t(opts.Bars), C.int64_t(opts.Bucket), C.double(opts.PeriodsPerYear), (*C.HOCDBSnapshot)(buf))
	if rc != 0 {
		return nil, indicatorError(rc)
	}
	return decodeSnapshot(C.GoBytes(buf, C.int(size))), nil
}

// SnapshotMulti computes one Snapshot per bar size in opts.Buckets from a
// single read of the data (e.g. 1-minute, 5-minute and 1-hour views for a
// multi-timeframe agent). The result is in the order of opts.Buckets; entry k
// covers the last opts.Bars bars of opts.Buckets[k], annualised with
// opts.PeriodsPerYear[k] (0 / nil = none).
func (db *DB) SnapshotMulti(opts *SnapshotMultiOptions) ([]*Snapshot, error) {
	if db.handle == nil {
		return nil, errors.New("database not initialized")
	}
	if opts == nil || len(opts.Buckets) == 0 {
		return nil, errors.New("SnapshotMulti needs at least one bucket")
	}
	if opts.Bars < 0 {
		return nil, errors.New("bars must be >= 0")
	}
	if opts.PeriodsPerYear != nil && len(opts.PeriodsPerYear) != len(opts.Buckets) {
		return nil, fmt.Errorf("PeriodsPerYear has %d entries, Buckets has %d", len(opts.PeriodsPerYear), len(opts.Buckets))
	}
	nb := len(opts.Buckets)
	buckets := make([]C.int64_t, nb)
	ppy := make([]C.double, nb)
	for i, b := range opts.Buckets {
		if b < 0 {
			return nil, errors.New("buckets must be >= 0")
		}
		buckets[i] = C.int64_t(b)
		if opts.PeriodsPerYear != nil {
			ppy[i] = C.double(opts.PeriodsPerYear[i])
		}
	}
	cols, err := db.resolveColumns(opts.Columns)
	if err != nil {
		return nil, err
	}

	size := int(C.hocdb_snapshot_size())
	buf := C.malloc(C.size_t(size * nb))
	defer C.free(buf)

	rc := C.hocdb_snapshot_multi(db.handle, &cols, C.size_t(opts.Bars), &buckets[0], C.size_t(nb), &ppy[0], (*C.HOCDBSnapshot)(buf))
	if rc != 0 {
		return nil, indicatorError(rc)
	}

	raw := C.GoBytes(buf, C.int(size*nb))
	out := make([]*Snapshot, nb)
	for k := range out {
		out[k] = decodeSnapshot(raw[k*size : (k+1)*size])
	}
	return out, nil
}

// IndicatorKinds returns the names of all supported indicator kinds
// ("sma", "ema", ..., "heikin_ashi", "spread", "order_flow", ..., "pivots").
func IndicatorKinds() []string {
	total := int(C.hocdb_indicator_kinds(nil, 0))
	if total <= 0 {
		return nil
	}
	ids := make([]C.uint32_t, total)
	got := int(C.hocdb_indicator_kinds(&ids[0], C.size_t(total)))
	ids = ids[:min(got, total)]
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		names = append(names, C.GoString(C.hocdb_indicator_name(id)))
	}
	return names
}

// IndicatorOutputs returns the output names of a kind, e.g. ["macd",
// "signal", "hist"] for "macd"; single-output kinds report ["value"]. nil for
// an unknown kind.
func IndicatorOutputs(kind string) []string {
	k, err := indicatorKind(kind)
	if err != nil {
		return nil
	}
	n := int(C.hocdb_indicator_output_count(k))
	out := make([]string, n)
	for i := range out {
		out[i] = C.GoString(C.hocdb_indicator_output_name(k, C.size_t(i)))
	}
	return out
}

// IndicatorIsLookahead reports whether a kind reads future rows: the label
// kinds "forward_return" and "triple_barrier" describe what happens AFTER each
// row and must not be used as features of that row. false for an unknown kind.
func IndicatorIsLookahead(kind string) bool {
	k, err := indicatorKind(kind)
	if err != nil {
		return false
	}
	return C.hocdb_indicator_is_lookahead(k) != 0
}

// IndicatorWarmup returns the recommended number of warm-up rows for a spec
// (the amount an automatic lookback reads before the window). 0 for an
// invalid spec.
func IndicatorWarmup(spec IndicatorSpec) int {
	k, err := indicatorKind(spec.Kind)
	if err != nil || spec.Period < 0 || spec.Period2 < 0 || spec.Period3 < 0 || spec.Period4 < 0 {
		return 0
	}
	cs := C.HOCDBIndicatorSpec{
		kind:         k,
		period:       C.uint32_t(spec.Period),
		period2:      C.uint32_t(spec.Period2),
		period3:      C.uint32_t(spec.Period3),
		period4:      C.uint32_t(spec.Period4),
		param:        C.double(spec.Param),
		param2:       C.double(spec.Param2),
		field_index:  -1,
		field_index2: -1,
	}
	return int(C.hocdb_indicator_warmup(&cs))
}

// ---------------------------------------------------------------------------
// Trading calendars
// ---------------------------------------------------------------------------

// Built-in trading calendar ids for Options.Calendar, SetCalendar and the
// Calendar* functions. Custom calendars registered with CalendarDefine get
// ids from 32 upwards. All calendar times are UTC seconds; database
// timestamps are converted with the handle's timestamp unit.
const (
	CalendarNone   uint32 = 0 // no calendar
	CalendarCrypto uint32 = 1 // 24/7, UTC days, 365 sessions a year
	CalendarFX     uint32 = 2 // Sunday 17:00 - Friday 17:00 New York time, one session per trade date
	CalendarNYSE   uint32 = 3 // 09:30-16:00 America/New_York, NYSE holidays, 13:00 early closes
	CalendarNasdaq uint32 = 4 // alias of NYSE
	CalendarLSE    uint32 = 5 // 08:00-16:30 Europe/London, UK bank holidays, 12:30 early closes
	CalendarCME    uint32 = 6 // Globex equity-index schedule (approximation)
)

// Daylight-saving rules for CalendarDefine.
const (
	DstNone = "none" // fixed UTC offset all year
	DstUS   = "us"   // second Sunday of March to first Sunday of November (US rules)
	DstEU   = "eu"   // last Sunday of March to last Sunday of October at 01:00 UTC (EU rules)
)

// Which session CalendarSession looks up.
const (
	SessionAt   = 0 // the session containing the time; nil when the calendar is closed
	SessionPrev = 1 // that session, or the previous one when closed
	SessionNext = 2 // that session, or the next one when closed
)

// ErrCalendarRequired is returned (wrapped) when a session kind (session_vwap,
// session_range, opening_range, pivots) runs with Param = 0 on a handle that
// has no trading calendar or no timestamp unit (engine code -30). Open the
// database with Options.Calendar / Options.CalendarName and
// Options.TimestampUnitNs, or call SetCalendar and SetTimestampUnit. Test for
// it with errors.Is.
var ErrCalendarRequired = errors.New("CalendarRequired: a session kind with Param = 0 needs a handle with a trading calendar and a timestamp unit (Options.Calendar / SetCalendar and Options.TimestampUnitNs / SetTimestampUnit)")

// ErrUnknownCalendar is returned (wrapped) for a calendar id or name the
// engine does not know (engine code -31). Test for it with errors.Is.
var ErrUnknownCalendar = errors.New("UnknownCalendar: no trading calendar with this id or name")

// Session is one resolved trading session. Times are UTC seconds.
type Session struct {
	Open       int64 // UTC seconds, inclusive
	Close      int64 // UTC seconds, exclusive
	TradeDay   int64 // local trade date as days since 1970-01-01 (see CivilFromDays)
	EarlyClose bool  // the session closes early on this day
}

// DaySession is one weekday's trading window for CalendarDefine, in local
// seconds relative to the local midnight of the trade date. OpenSec may be
// negative for sessions that start the evening before (FX, CME); CloseSec may
// exceed 86400. CloseSec <= OpenSec means no session.
type DaySession struct {
	OpenSec  int32
	CloseSec int32
}

// EarlyClose is an early-closing day for CalendarDefine.
type EarlyClose struct {
	Day      int32 // local days since 1970-01-01 (DaysFromCivil)
	CloseSec int32 // close on that day, local seconds relative to midnight
}

// goSession converts the C session struct.
func goSession(s *C.HOCDBSession) *Session {
	return &Session{
		Open:       int64(s.open),
		Close:      int64(s.close),
		TradeDay:   int64(s.trade_day),
		EarlyClose: s.early_close != 0,
	}
}

// calendarKnown reports whether the engine has a calendar with this id.
func calendarKnown(id uint32) bool {
	return C.hocdb_calendar_name(C.uint32_t(id), nil, 0) != 0
}

// unknownCalendar builds the error for an unknown id.
func unknownCalendar(id uint32) error {
	return fmt.Errorf("calendar id %d: %w", id, ErrUnknownCalendar)
}

// dstRule maps the DstNone / DstUS / DstEU names (or "0".."2") to the engine's constant.
func dstRule(s string) (C.int, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", DstNone, "0":
		return C.HOCDB_DST_NONE, nil
	case DstUS, "1":
		return C.HOCDB_DST_US, nil
	case DstEU, "2":
		return C.HOCDB_DST_EU, nil
	}
	return 0, fmt.Errorf("invalid DST rule %q: use %q, %q or %q", s, DstNone, DstUS, DstEU)
}

// CalendarID returns the id of a built-in ("crypto", "fx", "nyse", "nasdaq",
// "lse", "cme"; case-insensitive) or custom calendar name; 0 when unknown.
func CalendarID(name string) uint32 {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return uint32(C.hocdb_calendar_id(cName))
}

// CalendarName returns the name of a calendar id; "" when unknown.
func CalendarName(id uint32) string {
	var buf [256]C.char
	n := C.hocdb_calendar_name(C.uint32_t(id), (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)))
	if n <= 0 {
		return ""
	}
	return C.GoStringN(&buf[0], n)
}

// CalendarSession looks a session up: which = SessionAt returns the session
// containing utcSec, SessionPrev that or the previous one, SessionNext that
// or the next one. It returns (nil, nil) when there is no such session and an
// error wrapping ErrUnknownCalendar for an unknown id.
func CalendarSession(id uint32, utcSec int64, which int) (*Session, error) {
	if which < SessionAt || which > SessionNext {
		return nil, fmt.Errorf("invalid session selector %d: use SessionAt, SessionPrev or SessionNext", which)
	}
	var s C.HOCDBSession
	rc := C.hocdb_calendar_session(C.uint32_t(id), C.int64_t(utcSec), C.int(which), &s)
	switch {
	case rc == 1:
		return goSession(&s), nil
	case rc == 0:
		return nil, nil
	case rc == -31:
		return nil, unknownCalendar(id)
	}
	return nil, fmt.Errorf("calendar session lookup failed with code %d", int(rc))
}

// CalendarSessionForDay returns the session of a local trade date (days since
// 1970-01-01, see DaysFromCivil), or (nil, nil) when the calendar is closed
// that day (weekend, holiday).
func CalendarSessionForDay(id uint32, day int64) (*Session, error) {
	var s C.HOCDBSession
	rc := C.hocdb_calendar_session_for_day(C.uint32_t(id), C.int64_t(day), &s)
	switch {
	case rc == 1:
		return goSession(&s), nil
	case rc == 0:
		return nil, nil
	case rc == -31:
		return nil, unknownCalendar(id)
	}
	return nil, fmt.Errorf("calendar session lookup failed with code %d", int(rc))
}

// CalendarIsOpen reports whether the calendar is trading at utcSec.
func CalendarIsOpen(id uint32, utcSec int64) (bool, error) {
	rc := C.hocdb_calendar_is_open(C.uint32_t(id), C.int64_t(utcSec))
	switch {
	case rc == 1:
		return true, nil
	case rc == 0:
		return false, nil
	case rc == -31:
		return false, unknownCalendar(id)
	}
	return false, fmt.Errorf("calendar lookup failed with code %d", int(rc))
}

// CalendarOpenSeconds returns the number of trading seconds in [a, b).
func CalendarOpenSeconds(id uint32, a, b int64) (int64, error) {
	if !calendarKnown(id) {
		return 0, unknownCalendar(id)
	}
	return int64(C.hocdb_calendar_open_seconds(C.uint32_t(id), C.int64_t(a), C.int64_t(b))), nil
}

// CalendarSessionsBetween returns the number of sessions opening in [a, b).
func CalendarSessionsBetween(id uint32, a, b int64) (int64, error) {
	if !calendarKnown(id) {
		return 0, unknownCalendar(id)
	}
	return int64(C.hocdb_calendar_sessions_between(C.uint32_t(id), C.int64_t(a), C.int64_t(b))), nil
}

// CalendarPeriodsPerYear returns the number of bars per year for bars of
// bucketSec seconds (e.g. 252 * 390 for one-minute NYSE bars, 365 for daily
// crypto bars), the annualisation factor of Summary, Snapshot and the
// backtester.
func CalendarPeriodsPerYear(id uint32, bucketSec float64) (float64, error) {
	if !calendarKnown(id) {
		return 0, unknownCalendar(id)
	}
	return float64(C.hocdb_calendar_periods_per_year(C.uint32_t(id), C.double(bucketSec))), nil
}

// CalendarToLocal converts UTC seconds to the calendar's local wall-clock
// seconds (standard offset plus daylight saving).
func CalendarToLocal(id uint32, utcSec int64) (int64, error) {
	if !calendarKnown(id) {
		return 0, unknownCalendar(id)
	}
	return int64(C.hocdb_calendar_to_local(C.uint32_t(id), C.int64_t(utcSec))), nil
}

// DaysFromCivil returns the number of days since 1970-01-01 of a civil date
// (0 for an invalid month or day). Multiply by 86400 for UTC seconds.
func DaysFromCivil(year, month, day int) int64 {
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return 0
	}
	return int64(C.hocdb_days_from_civil(C.int64_t(year), C.uint32_t(month), C.uint32_t(day)))
}

// CivilFromDays is the inverse of DaysFromCivil.
func CivilFromDays(days int64) (year, month, day int) {
	var y C.int64_t
	var m, d C.uint32_t
	C.hocdb_civil_from_days(C.int64_t(days), &y, &m, &d)
	return int(y), int(m), int(d)
}

// CalendarDefine registers a custom calendar in this process and returns its
// id (>= 32; redefining a name reuses its id). weekly lists the sessions
// Monday first (nil = no session that weekday) in local seconds; utcOffsetSec
// is the standard UTC offset (e.g. 9 * 3600 for Tokyo); dstRule is DstNone,
// DstUS or DstEU; holidays are full closures (DaysFromCivil day numbers);
// earlyCloses override the close on the given days; sessionsPerYear feeds
// CalendarPeriodsPerYear. The registry holds 32 custom calendars.
func CalendarDefine(name string, weekly [7]*DaySession, utcOffsetSec int32, dstRule string, holidays []int32, earlyCloses []EarlyClose, sessionsPerYear float64) (uint32, error) {
	if name == "" {
		return 0, errors.New("calendar name must not be empty")
	}
	rule, err := dstRuleValue(dstRule)
	if err != nil {
		return 0, err
	}
	var w [7]C.HOCDBDaySession
	for i, d := range weekly {
		if d != nil {
			w[i].open_sec = C.int32_t(d.OpenSec)
			w[i].close_sec = C.int32_t(d.CloseSec)
		}
	}
	var hol *C.int32_t
	if len(holidays) > 0 {
		hol = (*C.int32_t)(unsafe.Pointer(&holidays[0]))
	}
	var early *C.HOCDBEarlyClose
	if len(earlyCloses) > 0 {
		ce := make([]C.HOCDBEarlyClose, len(earlyCloses))
		for i, e := range earlyCloses {
			ce[i].day = C.int32_t(e.Day)
			ce[i].close_sec = C.int32_t(e.CloseSec)
		}
		early = &ce[0]
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	id := C.hocdb_calendar_define(cName, &w[0], C.int32_t(utcOffsetSec), rule, hol, C.size_t(len(holidays)), early, C.size_t(len(earlyCloses)), C.double(sessionsPerYear))
	switch {
	case id > 0:
		return uint32(id), nil
	case id == 0:
		return 0, fmt.Errorf("invalid calendar definition %q (empty name, no trading weekday, or bad sessions)", name)
	}
	return 0, errors.New("calendar registry full (32 custom calendars per process)")
}

// dstRuleValue is dstRule under a name that does not clash with the parameter.
func dstRuleValue(s string) (C.int, error) { return dstRule(s) }

// SetCalendar sets the trading calendar of this handle (an error wrapping
// ErrUnknownCalendar for an unknown id; CalendarNone clears it). Writers
// persist built-in ids in the file header; on readers the setting is local
// to the handle.
func (db *DB) SetCalendar(id uint32) error {
	if db.handle == nil {
		return errNotInitialized
	}
	return opError("set_calendar", C.hocdb_set_calendar(db.handle, C.uint32_t(id)))
}

// SetCalendarName is SetCalendar with a calendar name (see CalendarID).
func (db *DB) SetCalendarName(name string) error {
	id := CalendarID(name)
	if id == 0 {
		return fmt.Errorf("calendar %q: %w", name, ErrUnknownCalendar)
	}
	return db.SetCalendar(id)
}

// Calendar returns the trading calendar id of this handle (CalendarNone = 0
// when there is none).
func (db *DB) Calendar() uint32 {
	if db.handle == nil {
		return CalendarNone
	}
	return uint32(C.hocdb_get_calendar(db.handle))
}

// CalendarName returns the name of this handle's trading calendar ("" when
// there is none).
func (db *DB) CalendarName() string {
	return CalendarName(db.Calendar())
}

// SetTimestampUnit sets the number of nanoseconds per timestamp unit (1e9
// seconds, 1e6 milliseconds, 1000 microseconds, 1 nanoseconds; 0 = unknown).
// Writers persist it in the file header. The unit converts timestamps to
// calendar time for the session kinds, Health and PeriodsPerYear.
func (db *DB) SetTimestampUnit(unitNs uint64) error {
	if db.handle == nil {
		return errNotInitialized
	}
	return opError("set_timestamp_unit", C.hocdb_set_timestamp_unit(db.handle, C.uint64_t(unitNs)))
}

// TimestampUnit returns the nanoseconds per timestamp unit of this handle
// (0 = unknown).
func (db *DB) TimestampUnit() uint64 {
	if db.handle == nil {
		return 0
	}
	return uint64(C.hocdb_get_timestamp_unit(db.handle))
}

// PeriodsPerYear returns the bars per year for bars of `bucket` timestamp
// units according to the handle's calendar and timestamp unit, i.e. the
// annualisation Summary, Snapshot and Backtest use when their periodsPerYear
// is 0. It returns 0 when the calendar or the unit is unknown.
func (db *DB) PeriodsPerYear(bucket int64) float64 {
	if db.handle == nil {
		return 0
	}
	return float64(C.hocdb_periods_per_year(db.handle, C.int64_t(bucket)))
}

// ---------------------------------------------------------------------------
// Struct introspection (backtest results, trades, universe rows / summary)
// ---------------------------------------------------------------------------

// introField is one field of a C struct as reported by the library's
// introspection API.
type introField struct {
	name   string // C field name (snake_case)
	goName string // PascalCase name of the matching Go struct field
	off    int
	typ    int // 1 int64, 2 double, 3 uint64
}

// fieldTable loads a struct's introspection table once.
type fieldTable struct {
	once   sync.Once
	fields []introField
	load   func() []introField
}

func (t *fieldTable) get() []introField {
	t.once.Do(func() { t.fields = t.load() })
	return t.fields
}

func introspectFields(n int, name func(int) string, offset func(int) int, typ func(int) int) []introField {
	out := make([]introField, 0, n)
	for i := 0; i < n; i++ {
		cName := name(i)
		out = append(out, introField{name: cName, goName: snakeToPascal(cName), off: offset(i), typ: typ(i)})
	}
	return out
}

// snakeToPascal turns "max_drawdown_bars" into "MaxDrawdownBars".
func snakeToPascal(s string) string {
	var b strings.Builder
	upper := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' {
			upper = true
			continue
		}
		if upper && c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		upper = false
		b.WriteByte(c)
	}
	return b.String()
}

// decodeStruct fills the exported fields of dst (a pointer to a struct whose
// field names are the PascalCase form of the C names) from a C struct image;
// integer fields are decoded exactly. With withMap it also returns every
// field by its C name as float64 (integers exact up to 2^53).
func decodeStruct(raw []byte, fields []introField, dst interface{}, withMap bool) map[string]float64 {
	v := reflect.ValueOf(dst).Elem()
	var m map[string]float64
	if withMap {
		m = make(map[string]float64, len(fields))
	}
	for _, f := range fields {
		if f.off < 0 || f.off+8 > len(raw) {
			continue
		}
		bits := binary.LittleEndian.Uint64(raw[f.off : f.off+8])
		if withMap {
			if x, ok := decodeIntrospectedField(raw, f.off, f.typ); ok {
				m[f.name] = x
			}
		}
		fv := v.FieldByName(f.goName)
		if !fv.IsValid() || !fv.CanSet() {
			continue
		}
		switch fv.Kind() {
		case reflect.Float64:
			switch f.typ {
			case 2:
				fv.SetFloat(math.Float64frombits(bits))
			case 1:
				fv.SetFloat(float64(int64(bits)))
			default:
				fv.SetFloat(float64(bits))
			}
		case reflect.Int, reflect.Int64:
			if f.typ == 2 {
				fv.SetInt(int64(math.Float64frombits(bits)))
			} else {
				fv.SetInt(int64(bits))
			}
		case reflect.Uint, reflect.Uint64:
			if f.typ == 2 {
				fv.SetUint(uint64(math.Float64frombits(bits)))
			} else {
				fv.SetUint(bits)
			}
		case reflect.Bool:
			fv.SetBool(bits != 0)
		}
	}
	return m
}

var backtestResultFields = fieldTable{load: func() []introField {
	return introspectFields(int(C.hocdb_backtest_result_field_count()),
		func(i int) string { return C.GoString(C.hocdb_backtest_result_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_backtest_result_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_backtest_result_field_type(C.size_t(i))) })
}}

var tradeFields = fieldTable{load: func() []introField {
	return introspectFields(int(C.hocdb_trade_field_count()),
		func(i int) string { return C.GoString(C.hocdb_trade_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_trade_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_trade_field_type(C.size_t(i))) })
}}

var universeRowFields = fieldTable{load: func() []introField {
	return introspectFields(int(C.hocdb_universe_row_field_count()),
		func(i int) string { return C.GoString(C.hocdb_universe_row_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_universe_row_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_universe_row_field_type(C.size_t(i))) })
}}

var universeSummaryFields = fieldTable{load: func() []introField {
	return introspectFields(int(C.hocdb_universe_summary_field_count()),
		func(i int) string { return C.GoString(C.hocdb_universe_summary_field_name(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_universe_summary_field_offset(C.size_t(i))) },
		func(i int) int { return int(C.hocdb_universe_summary_field_type(C.size_t(i))) })
}}

// ---------------------------------------------------------------------------
// Signal backtester
// ---------------------------------------------------------------------------

// Position modes for BacktestParams.PositionMode: the unit of the target
// series. The numbers "0".."2" are accepted as well.
const (
	PositionUnits    = "units"    // target = units of the instrument (default)
	PositionFraction = "fraction" // target = fraction of current equity (1 = 100% long, -0.5 = 50% short)
	PositionNotional = "notional" // target = notional in currency
)

// Fill modes for BacktestParams.FillMode. The numbers "0" / "1" are accepted
// as well.
const (
	FillNextOpen  = "next_open"  // the change implied by target[i] fills at open[i+1] (default, no look-ahead)
	FillSameClose = "same_close" // it fills at close[i]
)

// Trade.ExitReason values.
const (
	ExitSignal     = 0 // the target series closed or flipped the position
	ExitStopLoss   = 1
	ExitTakeProfit = 2
	ExitTrailing   = 3
	ExitEndOfData  = 4 // still open at the end (ExitTs 0, ExitPrice = last close, unrealised Pnl)
)

// ExitReasonName returns the name of a Trade.ExitReason value
// ("signal", "stop_loss", "take_profit", "trailing", "end_of_data").
func ExitReasonName(reason int) string {
	switch reason {
	case ExitSignal:
		return "signal"
	case ExitStopLoss:
		return "stop_loss"
	case ExitTakeProfit:
		return "take_profit"
	case ExitTrailing:
		return "trailing"
	case ExitEndOfData:
		return "end_of_data"
	}
	return fmt.Sprintf("unknown(%d)", reason)
}

// BacktestParams configures the backtester. Start from DefaultBacktestParams()
// (initial equity 1, no costs, shorts allowed, fills at the next open): the
// zero value is NOT the default (AllowShort false clamps negative targets).
type BacktestParams struct {
	InitialEquity  float64 // Starting equity (<= 0 -> 1)
	CostBps        float64 // Commission per side in basis points of the traded notional
	SlippageBps    float64 // Adverse price move per side in basis points
	StopLoss       float64 // Fraction of the entry price, 0 = none
	TakeProfit     float64 // Fraction of the entry price, 0 = none
	TrailingStop   float64 // Fraction from the best price since entry, 0 = none
	MaxPosition    float64 // Cap on |units|, 0 = none
	PositionMode   string  // PositionUnits (""), PositionFraction or PositionNotional
	FillMode       string  // FillNextOpen ("") or FillSameClose
	PeriodsPerYear float64 // Annualisation for ann_return / ann_vol / sharpe / sortino; 0 = none (Backtest fills it from the handle's calendar)
	AllowShort     bool    // false clamps negative targets to 0
	RiskFreeRate   float64 // Annual risk-free rate for sharpe / sortino
}

// DefaultBacktestParams returns the engine's defaults (hocdb_backtest_params_default).
func DefaultBacktestParams() BacktestParams {
	var cp C.HOCDBBacktestParams
	C.hocdb_backtest_params_default(&cp)
	return BacktestParams{
		InitialEquity:  float64(cp.initial_equity),
		CostBps:        float64(cp.cost_bps),
		SlippageBps:    float64(cp.slippage_bps),
		StopLoss:       float64(cp.stop_loss),
		TakeProfit:     float64(cp.take_profit),
		TrailingStop:   float64(cp.trailing_stop),
		MaxPosition:    float64(cp.max_position),
		PositionMode:   positionModeName(uint64(cp.position_mode)),
		FillMode:       fillModeName(uint64(cp.fill_mode)),
		PeriodsPerYear: float64(cp.periods_per_year),
		AllowShort:     cp.allow_short != 0,
		RiskFreeRate:   float64(cp.risk_free_rate),
	}
}

func positionModeName(mode uint64) string {
	switch mode {
	case 1:
		return PositionFraction
	case 2:
		return PositionNotional
	}
	return PositionUnits
}

func fillModeName(mode uint64) string {
	if mode == 1 {
		return FillSameClose
	}
	return FillNextOpen
}

func positionModeValue(s string) (C.uint64_t, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", PositionUnits, "0":
		return 0, nil
	case PositionFraction, "1":
		return 1, nil
	case PositionNotional, "2":
		return 2, nil
	}
	return 0, fmt.Errorf("invalid position mode %q: use %q, %q or %q", s, PositionUnits, PositionFraction, PositionNotional)
}

func fillModeValue(s string) (C.uint64_t, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", FillNextOpen, "0":
		return 0, nil
	case FillSameClose, "1":
		return 1, nil
	}
	return 0, fmt.Errorf("invalid fill mode %q: use %q or %q", s, FillNextOpen, FillSameClose)
}

// backtestParamsC converts BacktestParams (nil = DefaultBacktestParams) to the C struct.
func backtestParamsC(p *BacktestParams) (C.HOCDBBacktestParams, error) {
	var cp C.HOCDBBacktestParams
	if p == nil {
		C.hocdb_backtest_params_default(&cp)
		return cp, nil
	}
	pm, err := positionModeValue(p.PositionMode)
	if err != nil {
		return cp, err
	}
	fm, err := fillModeValue(p.FillMode)
	if err != nil {
		return cp, err
	}
	cp.initial_equity = C.double(p.InitialEquity)
	cp.cost_bps = C.double(p.CostBps)
	cp.slippage_bps = C.double(p.SlippageBps)
	cp.stop_loss = C.double(p.StopLoss)
	cp.take_profit = C.double(p.TakeProfit)
	cp.trailing_stop = C.double(p.TrailingStop)
	cp.max_position = C.double(p.MaxPosition)
	cp.position_mode = pm
	cp.fill_mode = fm
	cp.periods_per_year = C.double(p.PeriodsPerYear)
	if p.AllowShort {
		cp.allow_short = 1
	}
	cp.risk_free_rate = C.double(p.RiskFreeRate)
	return cp, nil
}

// BacktestResult holds the performance statistics of one backtest (the 32
// fields of HOCDBBacktestResult, decoded by name through the library's
// introspection API; Fields carries every field by its C name as well).
// Per-trade statistics (WinRate, ProfitFactor, AvgTradeReturn, AvgWin,
// AvgLoss, BestTrade, WorstTrade, AvgHoldingBars) cover closed trades only;
// AvgWin / AvgLoss / BestTrade / WorstTrade are in currency.
type BacktestResult struct {
	NBars, NTrades, NLongTrades, NShortTrades                                                  int
	FinalEquity, TotalReturn, AnnReturn, AnnVol, Sharpe, Sortino, Calmar, MaxDrawdown          float64
	MaxDrawdownBars                                                                            int // longest run of consecutive bars below a prior equity peak
	AvgDrawdown, WinRate, ProfitFactor, AvgTradeReturn, AvgWin, AvgLoss, BestTrade, WorstTrade float64
	AvgHoldingBars, Exposure, LongShare, Turnover, TotalCost, TotalSlippage                    float64
	NStopExits, NTakeProfitExits, NTrailingExits                                               int
	GrossPnl, NetPnl                                                                           float64
	Fields                                                                                     map[string]float64 // every result field by its C name (n_bars, final_equity, sharpe, ...)
}

// Trade is one round trip of the backtester: a trade opens when the position
// goes from 0 to non-zero or flips sign and closes when it returns to 0 or
// flips; increases / decreases in between belong to the same trade.
type Trade struct {
	EntryTs    int64   // timestamp of the entry bar
	ExitTs     int64   // timestamp of the exit bar; 0 = still open at the end
	Direction  int     // +1 long, -1 short
	EntryPrice float64 // units-weighted average of the slipped entry-side fills
	ExitPrice  float64 // the same over the exit-side fills (last close when still open)
	Size       float64 // total units accumulated on the entry side
	Pnl        float64 // net realised cash flow (costs and slippage included)
	Ret        float64 // Pnl / (Size * EntryPrice)
	Bars       int     // exit bar index - entry bar index
	ExitReason int     // ExitSignal, ExitStopLoss, ExitTakeProfit, ExitTrailing or ExitEndOfData
}

// BacktestOptions selects the optional outputs of Backtest, BacktestTail and
// BacktestArrays. nil = statistics only.
type BacktestOptions struct {
	Columns   *IndicatorColumns // Backtest / BacktestTail: the OHLCV roles (nil = auto-detect, see IndicatorOptions.Columns)
	Equity    bool              // return the per-bar equity curve (cash + position * close after the fills of the bar)
	Position  bool              // return the per-bar position in units
	Cash      bool              // return the per-bar cash
	Pnl       bool              // return the per-bar profit and loss
	Drawdown  bool              // return the per-bar drawdown (positive fraction below the running peak)
	MaxTrades int               // > 0: return up to this many trades in BacktestReport.Trades (Result.NTrades counts all)
}

// BacktestReport is the result of Backtest, BacktestTail and BacktestArrays.
// Trades is nil unless BacktestOptions.MaxTrades > 0; the per-bar slices are
// nil unless requested and otherwise have Result.NBars elements.
type BacktestReport struct {
	Result   BacktestResult
	Trades   []Trade
	Equity   []float64
	Position []float64
	Cash     []float64
	Pnl      []float64
	Drawdown []float64
}

// Split is one walk-forward window: [TrainStart, TrainEnd) trains,
// [TestStart, TestEnd) tests (bar indices, ends exclusive).
type Split struct {
	TrainStart int
	TrainEnd   int
	TestStart  int
	TestEnd    int
}

// backtestError maps the engine codes of the backtester to errors.
func backtestError(rc C.int) error {
	switch rc {
	case -2:
		return errors.New("invalid backtest parameters")
	case -3:
		return errors.New("missing close column: the backtester needs a close column (open/high/low are optional)")
	case -7:
		return errors.New("target length mismatch: len(target) must equal the number of rows the window holds (the rows Indicators returns over the same window and bucket)")
	}
	return indicatorError(rc)
}

// backtestBuffers holds the C-side buffers of one backtest call. The
// HOCDBBacktestOutputs struct itself lives in C memory too: cgo refuses a Go
// pointer to memory that holds pointers.
type backtestBuffers struct {
	n       int
	outs    unsafe.Pointer // *C.HOCDBBacktestOutputs, nil when nothing was requested
	outPtrs []unsafe.Pointer
	trades  unsafe.Pointer
	cap     int
	result  unsafe.Pointer
}

func newBacktestBuffers(n int, opts *BacktestOptions) *backtestBuffers {
	b := &backtestBuffers{n: n, result: C.malloc(C.hocdb_backtest_result_size())}
	alloc := func(want bool) *C.double {
		if !want || n == 0 {
			return nil
		}
		p := C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(C.double(0))))
		b.outPtrs = append(b.outPtrs, p)
		return (*C.double)(p)
	}
	if opts != nil {
		outs := (*C.HOCDBBacktestOutputs)(C.calloc(1, C.size_t(unsafe.Sizeof(C.HOCDBBacktestOutputs{}))))
		outs.equity = alloc(opts.Equity)
		outs.position = alloc(opts.Position)
		outs.cash = alloc(opts.Cash)
		outs.pnl = alloc(opts.Pnl)
		outs.drawdown = alloc(opts.Drawdown)
		if len(b.outPtrs) > 0 {
			b.outs = unsafe.Pointer(outs)
		} else {
			C.free(unsafe.Pointer(outs))
		}
		if opts.MaxTrades > 0 {
			b.cap = opts.MaxTrades
			b.trades = C.malloc(C.size_t(b.cap) * C.hocdb_trade_size())
		}
	}
	return b
}

func (b *backtestBuffers) free() {
	for _, p := range b.outPtrs {
		C.free(p)
	}
	if b.trades != nil {
		C.free(b.trades)
	}
	if b.outs != nil {
		C.free(b.outs)
	}
	C.free(b.result)
}

func (b *backtestBuffers) outputs() *C.HOCDBBacktestOutputs {
	return (*C.HOCDBBacktestOutputs)(b.outs)
}

// out returns one of the requested per-bar columns (nil when not requested).
func (b *backtestBuffers) col(pick func(*C.HOCDBBacktestOutputs) *C.double) *C.double {
	if b.outs == nil {
		return nil
	}
	return pick((*C.HOCDBBacktestOutputs)(b.outs))
}

func (b *backtestBuffers) tradesPtr() *C.HOCDBTrade {
	return (*C.HOCDBTrade)(b.trades)
}

func (b *backtestBuffers) resultPtr() *C.HOCDBBacktestResult {
	return (*C.HOCDBBacktestResult)(b.result)
}

// decodeBacktestResult decodes one HOCDBBacktestResult image.
func decodeBacktestResult(raw []byte) BacktestResult {
	var r BacktestResult
	r.Fields = decodeStruct(raw, backtestResultFields.get(), &r, true)
	return r
}

// report copies everything out of the C buffers.
func (b *backtestBuffers) report() *BacktestReport {
	rep := &BacktestReport{Result: decodeBacktestResult(C.GoBytes(b.result, C.int(C.hocdb_backtest_result_size())))}
	copyOut := func(p *C.double) []float64 {
		if p == nil {
			return nil
		}
		out := make([]float64, b.n)
		copy(out, unsafe.Slice((*float64)(unsafe.Pointer(p)), b.n))
		return out
	}
	rep.Equity = copyOut(b.col(func(o *C.HOCDBBacktestOutputs) *C.double { return o.equity }))
	rep.Position = copyOut(b.col(func(o *C.HOCDBBacktestOutputs) *C.double { return o.position }))
	rep.Cash = copyOut(b.col(func(o *C.HOCDBBacktestOutputs) *C.double { return o.cash }))
	rep.Pnl = copyOut(b.col(func(o *C.HOCDBBacktestOutputs) *C.double { return o.pnl }))
	rep.Drawdown = copyOut(b.col(func(o *C.HOCDBBacktestOutputs) *C.double { return o.drawdown }))
	if b.trades != nil {
		nt := rep.Result.NTrades
		if nt > b.cap {
			nt = b.cap
		}
		size := int(C.hocdb_trade_size())
		raw := C.GoBytes(b.trades, C.int(size*nt))
		rep.Trades = make([]Trade, nt)
		for i := range rep.Trades {
			decodeStruct(raw[i*size:(i+1)*size], tradeFields.get(), &rep.Trades[i], false)
		}
	}
	return rep
}

// seriesPtr validates a float64 series of the arrays entry points and returns
// its C pointer (nil for an absent optional series).
func seriesPtr(name string, s []float64, n int, optional bool) (*C.double, error) {
	if len(s) == 0 && optional {
		return nil, nil
	}
	if len(s) != n {
		return nil, fmt.Errorf("%s has %d elements, ts has %d: length mismatch", name, len(s), n)
	}
	return (*C.double)(unsafe.Pointer(&s[0])), nil
}

// arraySeries validates the series of the arrays entry points.
func arraySeries(ts []int64, open, high, low, close, target []float64) (t *C.int64_t, op, hi, lo, cl, tg *C.double, err error) {
	n := len(ts)
	if n == 0 {
		return nil, nil, nil, nil, nil, nil, errors.New("ts is empty: pass at least one bar")
	}
	if op, err = seriesPtr("open", open, n, true); err != nil {
		return
	}
	if hi, err = seriesPtr("high", high, n, true); err != nil {
		return
	}
	if lo, err = seriesPtr("low", low, n, true); err != nil {
		return
	}
	if cl, err = seriesPtr("close", close, n, false); err != nil {
		return
	}
	if tg, err = seriesPtr("target", target, n, false); err != nil {
		return
	}
	t = (*C.int64_t)(unsafe.Pointer(&ts[0]))
	return
}

// backtest is the shared implementation of Backtest and BacktestTail.
func (db *DB) backtest(tail bool, target []float64, startTs, endTs, bucket int64, params *BacktestParams, opts *BacktestOptions) (*BacktestReport, error) {
	if db.handle == nil {
		return nil, errNotInitialized
	}
	if bucket < 0 {
		return nil, errors.New("bucket must be >= 0")
	}
	if len(target) == 0 {
		return nil, errors.New("target is empty: pass one desired position per row of the window")
	}
	cp, err := backtestParamsC(params)
	if err != nil {
		return nil, err
	}
	var columns *IndicatorColumns
	if opts != nil {
		columns = opts.Columns
	}
	cols, err := db.resolveColumns(columns)
	if err != nil {
		return nil, err
	}
	b := newBacktestBuffers(len(target), opts)
	defer b.free()
	tgt := (*C.double)(unsafe.Pointer(&target[0]))
	var rc C.int
	if tail {
		rc = C.hocdb_backtest_tail(db.handle, &cols, C.int64_t(bucket), tgt, C.size_t(len(target)), &cp, b.outputs(), b.tradesPtr(), C.size_t(b.cap), b.resultPtr())
	} else {
		rc = C.hocdb_backtest(db.handle, &cols, C.int64_t(startTs), C.int64_t(endTs), C.int64_t(bucket), tgt, C.size_t(len(target)), &cp, b.outputs(), b.tradesPtr(), C.size_t(b.cap), b.resultPtr())
	}
	if rc != 0 {
		return nil, backtestError(rc)
	}
	return b.report(), nil
}

// Backtest runs a target-position series against the bars of [startTs,
// endTs): exactly the rows Indicators returns over the same window and
// bucket (bucket > 0: bars of that many timestamp units whose start lies in
// the window, the same bars OHLCV returns for bucket-aligned bounds; bucket
// 0: one row per record). Compute the signals with Indicators over the same
// window and pass one target per row: target[i] is the desired position at
// the END of row i (units, fraction of equity or notional per
// params.PositionMode; NaN = hold the previous signal); it fills at the next
// bar's open by default, and a position closed by a stop is not re-entered
// until the target changes to a different non-zero value. len(target) must
// equal the row count (an error mentioning the length otherwise). params nil
// = DefaultBacktestParams; a PeriodsPerYear of 0 is filled from the handle's
// calendar (see PeriodsPerYear). opts nil = statistics only.
func (db *DB) Backtest(target []float64, startTs, endTs, bucket int64, params *BacktestParams, opts *BacktestOptions) (*BacktestReport, error) {
	return db.backtest(false, target, startTs, endTs, bucket, params, opts)
}

// BacktestTail is Backtest over the last len(target) bars (bucket > 0) or
// records of the database.
func (db *DB) BacktestTail(target []float64, bucket int64, params *BacktestParams, opts *BacktestOptions) (*BacktestReport, error) {
	return db.backtest(true, target, 0, 0, bucket, params, opts)
}

// BacktestArrays runs the backtester on caller-provided bars: ts, close and
// target must have the same length; open, high and low may be nil (fills
// then happen at the close and stops trigger on the close, without intrabar
// checks). params nil = DefaultBacktestParams; opts nil = statistics only.
func BacktestArrays(ts []int64, open, high, low, close, target []float64, params *BacktestParams, opts *BacktestOptions) (*BacktestReport, error) {
	t, op, hi, lo, cl, tg, err := arraySeries(ts, open, high, low, close, target)
	if err != nil {
		return nil, err
	}
	cp, err := backtestParamsC(params)
	if err != nil {
		return nil, err
	}
	b := newBacktestBuffers(len(ts), opts)
	defer b.free()
	rc := C.hocdb_backtest_arrays(t, op, hi, lo, cl, C.size_t(len(ts)), tg, &cp, b.outputs(), b.tradesPtr(), C.size_t(b.cap), b.resultPtr())
	if rc != 0 {
		return nil, backtestError(rc)
	}
	return b.report(), nil
}

// WalkForwardSplits returns walk-forward windows over n bars: the first
// train window is floor(trainFrac * n) bars and the test windows tile the
// rest in nSplits pieces; anchored keeps the train window starting at 0 and
// growing, otherwise it rolls with a fixed length. nil when n or nSplits is 0.
func WalkForwardSplits(n, nSplits int, trainFrac float64, anchored bool) []Split {
	if n <= 0 || nSplits <= 0 {
		return nil
	}
	buf := make([]C.HOCDBSplit, nSplits)
	k := int(C.hocdb_walk_forward_splits(C.size_t(n), C.size_t(nSplits), C.double(trainFrac), cBool(anchored), &buf[0], C.size_t(nSplits)))
	if k > nSplits {
		k = nSplits
	}
	out := make([]Split, k)
	for i := 0; i < k; i++ {
		out[i] = Split{
			TrainStart: int(buf[i].train_start),
			TrainEnd:   int(buf[i].train_end),
			TestStart:  int(buf[i].test_start),
			TestEnd:    int(buf[i].test_end),
		}
	}
	return out
}

// BacktestSplits runs the test window of every split independently (fresh
// equity each) over caller-provided bars (see BacktestArrays for the series)
// and returns one BacktestResult per split, in order.
func BacktestSplits(ts []int64, open, high, low, close, target []float64, splits []Split, params *BacktestParams) ([]BacktestResult, error) {
	t, op, hi, lo, cl, tg, err := arraySeries(ts, open, high, low, close, target)
	if err != nil {
		return nil, err
	}
	cp, err := backtestParamsC(params)
	if err != nil {
		return nil, err
	}
	if len(splits) == 0 {
		return []BacktestResult{}, nil
	}
	n := len(ts)
	cs := make([]C.HOCDBSplit, len(splits))
	for i, s := range splits {
		if s.TrainStart < 0 || s.TrainEnd < s.TrainStart || s.TestStart < 0 || s.TestEnd < s.TestStart || s.TrainEnd > n || s.TestEnd > n {
			return nil, fmt.Errorf("split %d %+v is outside the %d bars", i, s, n)
		}
		cs[i] = C.HOCDBSplit{
			train_start: C.uint64_t(s.TrainStart),
			train_end:   C.uint64_t(s.TrainEnd),
			test_start:  C.uint64_t(s.TestStart),
			test_end:    C.uint64_t(s.TestEnd),
		}
	}
	size := int(C.hocdb_backtest_result_size())
	buf := C.malloc(C.size_t(size * len(splits)))
	defer C.free(buf)
	rc := C.hocdb_backtest_splits_arrays(t, op, hi, lo, cl, C.size_t(n), tg, &cp, &cs[0], C.size_t(len(splits)), (*C.HOCDBBacktestResult)(buf))
	if rc < 0 {
		return nil, backtestError(rc)
	}
	k := int(rc)
	if k > len(splits) {
		k = len(splits)
	}
	raw := C.GoBytes(buf, C.int(size*k))
	out := make([]BacktestResult, k)
	for i := range out {
		out[i] = decodeBacktestResult(raw[i*size : (i+1)*size])
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Universe (cross-sectional) features
// ---------------------------------------------------------------------------

// Market weighting for UniverseParams.WeightsMode. The numbers "0" / "1" are
// accepted as well.
const (
	WeightsEqual  = "equal"  // equal-weight market factor (default)
	WeightsVolume = "volume" // weights = mean volume over CorrPeriod (needs volume columns; falls back to equal without them)
)

// UniverseParams configures Universe and UniverseArrays (periods in bars).
// Start from DefaultUniverseParams() for the engine's defaults.
type UniverseParams struct {
	MomShort       int     // momentum horizon, default 5
	MomMid         int     // default 20
	MomLong        int     // default 60
	VolPeriod      int     // volatility window, default 20
	CorrPeriod     int     // correlation window, default 60
	SmaPeriod      int     // SMA distance window, default 50
	BetaPeriod     int     // beta window, default 60
	PeriodsPerYear float64 // annualises vol / idio_vol / market_vol with sqrt(PeriodsPerYear); 0 = none
	WeightsMode    string  // WeightsEqual ("") or WeightsVolume
}

// DefaultUniverseParams returns the engine's defaults (hocdb_universe_params_default).
func DefaultUniverseParams() UniverseParams {
	var cp C.HOCDBUniverseParams
	C.hocdb_universe_params_default(&cp)
	mode := WeightsEqual
	if cp.weights_mode == 1 {
		mode = WeightsVolume
	}
	return UniverseParams{
		MomShort:       int(cp.mom_short),
		MomMid:         int(cp.mom_mid),
		MomLong:        int(cp.mom_long),
		VolPeriod:      int(cp.vol_period),
		CorrPeriod:     int(cp.corr_period),
		SmaPeriod:      int(cp.sma_period),
		BetaPeriod:     int(cp.beta_period),
		PeriodsPerYear: float64(cp.periods_per_year),
		WeightsMode:    mode,
	}
}

func weightsModeValue(s string) (C.uint64_t, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", WeightsEqual, "0":
		return 0, nil
	case WeightsVolume, "1":
		return 1, nil
	}
	return 0, fmt.Errorf("invalid weights mode %q: use %q or %q", s, WeightsEqual, WeightsVolume)
}

// universeParamsC converts UniverseParams (nil = DefaultUniverseParams) to the C struct.
func universeParamsC(p *UniverseParams) (C.HOCDBUniverseParams, error) {
	var cp C.HOCDBUniverseParams
	if p == nil {
		C.hocdb_universe_params_default(&cp)
		return cp, nil
	}
	periods := []struct {
		name string
		v    int
		dst  *C.uint64_t
	}{
		{"MomShort", p.MomShort, &cp.mom_short}, {"MomMid", p.MomMid, &cp.mom_mid}, {"MomLong", p.MomLong, &cp.mom_long},
		{"VolPeriod", p.VolPeriod, &cp.vol_period}, {"CorrPeriod", p.CorrPeriod, &cp.corr_period},
		{"SmaPeriod", p.SmaPeriod, &cp.sma_period}, {"BetaPeriod", p.BetaPeriod, &cp.beta_period},
	}
	for _, q := range periods {
		if q.v < 0 {
			return cp, fmt.Errorf("UniverseParams.%s must be >= 0", q.name)
		}
		*q.dst = C.uint64_t(q.v)
	}
	mode, err := weightsModeValue(p.WeightsMode)
	if err != nil {
		return cp, err
	}
	cp.periods_per_year = C.double(p.PeriodsPerYear)
	cp.weights_mode = mode
	return cp, nil
}

// UniverseRow holds one ticker's cross-sectional features for the last bar
// (the 21 fields of HOCDBUniverseRow, decoded by name; NaN where the ticker
// has too few bars). Ranks are percentile ranks in [0, 1] across the tickers
// (1 = highest).
type UniverseRow struct {
	LastClose       float64 // close at the last bar
	Ret1            float64 // last simple return
	MomShort        float64 // close[n-1] / close[n-1-MomShort] - 1
	MomMid          float64
	MomLong         float64
	Vol             float64 // population std of the last VolPeriod log returns (annualised with PeriodsPerYear)
	SmaDistance     float64 // (close - sma) / sma over SmaPeriod bars
	Beta            float64 // beta to the market factor over BetaPeriod returns
	CorrMarket      float64 // correlation with the market factor over CorrPeriod returns
	RelStrength     float64 // MomMid minus the market's MomMid
	RankMomShort    float64
	RankMomMid      float64
	RankMomLong     float64
	RankVol         float64
	RankRelStrength float64
	ZMomMid         float64 // cross-sectional z-score of MomMid
	AvgCorr         float64 // mean correlation with the other tickers
	MaxCorr         float64 // largest such correlation
	MaxCorrIndex    int     // index (into Rows) of the most correlated other ticker
	IdioVol         float64 // std of the residual return r - beta * r_market over VolPeriod (annualised)
	VolumeRatio     float64 // last volume / mean volume over VolPeriod bars; NaN without volumes
	Fields          map[string]float64
}

// UniverseSummary holds the universe-level statistics for the last bar (the
// 16 fields of HOCDBUniverseSummary, decoded by name).
type UniverseSummary struct {
	NTickers       int
	NBars          int // joined bars actually used
	MarketRet1     float64
	MarketMomShort float64
	MarketMomMid   float64
	MarketMomLong  float64
	MarketVol      float64
	Dispersion     float64 // cross-sectional std of Ret1
	DispersionMid  float64 // cross-sectional std of MomMid
	BreadthSma     float64 // share of tickers above their SMA
	BreadthUp      float64 // share of tickers with Ret1 > 0
	AvgPairCorr    float64
	MaxPairCorr    float64
	MinPairCorr    float64
	FirstTs        int64 // first / last joined timestamp (0 without timestamps)
	LastTs         int64
	Fields         map[string]float64
}

// UniverseReport is the result of Universe and UniverseArrays: Rows[i] is
// ticker i (the order of the databases / closes), Corr the n x n correlation
// matrix of returns (pairwise-complete, NaN with fewer than 3 common bars).
type UniverseReport struct {
	Summary UniverseSummary
	Rows    []UniverseRow
	Corr    [][]float64
}

// universeError maps the engine codes of the universe calls to errors.
func universeError(rc C.int) error {
	switch rc {
	case -2:
		return errors.New("invalid universe parameters")
	case -3:
		return errors.New("missing close column: every database needs a close column (IndicatorColumns.Close, or a field named close / price)")
	case -7:
		return errors.New("series length mismatch: every close / volume series must have the same length")
	}
	return indicatorError(rc)
}

// universeBuffers holds the C-side output buffers of one universe call.
type universeBuffers struct {
	n       int
	rows    unsafe.Pointer
	corr    unsafe.Pointer
	summary unsafe.Pointer
}

func newUniverseBuffers(n int) *universeBuffers {
	return &universeBuffers{
		n:       n,
		rows:    C.malloc(C.size_t(n) * C.hocdb_universe_row_size()),
		corr:    C.malloc(C.size_t(n*n) * C.size_t(unsafe.Sizeof(C.double(0)))),
		summary: C.malloc(C.hocdb_universe_summary_size()),
	}
}

func (b *universeBuffers) free() {
	C.free(b.rows)
	C.free(b.corr)
	C.free(b.summary)
}

func (b *universeBuffers) report() *UniverseReport {
	rep := &UniverseReport{Rows: make([]UniverseRow, b.n), Corr: make([][]float64, b.n)}
	rsize := int(C.hocdb_universe_row_size())
	raw := C.GoBytes(b.rows, C.int(rsize*b.n))
	for i := range rep.Rows {
		rep.Rows[i].Fields = decodeStruct(raw[i*rsize:(i+1)*rsize], universeRowFields.get(), &rep.Rows[i], true)
	}
	rep.Summary.Fields = decodeStruct(C.GoBytes(b.summary, C.int(C.hocdb_universe_summary_size())), universeSummaryFields.get(), &rep.Summary, true)
	corr := unsafe.Slice((*float64)(b.corr), b.n*b.n)
	for i := range rep.Corr {
		rep.Corr[i] = make([]float64, b.n)
		copy(rep.Corr[i], corr[i*b.n:(i+1)*b.n])
	}
	return rep
}

// Universe computes the cross-sectional features of a watch-list of open
// databases (same column roles: cols nil = auto-detect on every database;
// the roles must resolve to the same field indices everywhere). The last
// nBars bars of `bucket` timestamp units (bucket 0: records; nBars 0: enough
// for the longest period) of every database are inner-joined on timestamps
// before the features are computed for the last joined bar. params nil =
// DefaultUniverseParams.
func Universe(dbs []*DB, cols *IndicatorColumns, nBars int, bucket int64, params *UniverseParams) (*UniverseReport, error) {
	if len(dbs) == 0 {
		return nil, errors.New("Universe needs at least one database")
	}
	if nBars < 0 {
		return nil, errors.New("nBars must be >= 0")
	}
	if bucket < 0 {
		return nil, errors.New("bucket must be >= 0")
	}
	handles := make([]C.HOCDBHandle, len(dbs))
	var ccols C.HOCDBIndicatorColumns
	for i, db := range dbs {
		if db == nil || db.handle == nil {
			return nil, fmt.Errorf("database %d is nil or not initialized", i)
		}
		c, err := db.resolveColumns(cols)
		if err != nil {
			return nil, fmt.Errorf("database %d: %w", i, err)
		}
		if i == 0 {
			ccols = c
		} else if c != ccols {
			return nil, fmt.Errorf("database %d maps the column roles to different field indices than database 0: use the same schema (or explicit IndicatorColumns that resolve identically) for every database", i)
		}
		handles[i] = db.handle
	}
	cp, err := universeParamsC(params)
	if err != nil {
		return nil, err
	}
	b := newUniverseBuffers(len(dbs))
	defer b.free()
	rc := C.hocdb_universe(&handles[0], C.size_t(len(dbs)), &ccols, C.size_t(nBars), C.int64_t(bucket), &cp, (*C.HOCDBUniverseRow)(b.rows), (*C.double)(b.corr), (*C.HOCDBUniverseSummary)(b.summary))
	if rc != 0 {
		return nil, universeError(rc)
	}
	return b.report(), nil
}

// cMatrix copies m series of n values into one C block and returns the block
// and a C array of the m row pointers (both to be freed by the caller).
func cMatrix(rows [][]float64, n int) (block, ptrs unsafe.Pointer) {
	m := len(rows)
	block = C.malloc(C.size_t(m*n) * C.size_t(unsafe.Sizeof(C.double(0))))
	ptrs = C.malloc(C.size_t(m) * C.size_t(unsafe.Sizeof(uintptr(0))))
	pslice := unsafe.Slice((**C.double)(ptrs), m)
	for i, r := range rows {
		dst := unsafe.Add(block, i*n*int(unsafe.Sizeof(C.double(0))))
		copy(unsafe.Slice((*float64)(dst), n), r)
		pslice[i] = (*C.double)(dst)
	}
	return block, ptrs
}

// UniverseArrays is Universe on caller-provided aligned series: closes[i] is
// ticker i's close series (all the same length), volumes may be nil (or one
// series per ticker, same length) and ts may be nil (it only feeds
// Summary.FirstTs / LastTs). params nil = DefaultUniverseParams.
func UniverseArrays(closes, volumes [][]float64, ts []int64, params *UniverseParams) (*UniverseReport, error) {
	m := len(closes)
	if m == 0 {
		return nil, errors.New("closes is empty: pass one close series per ticker")
	}
	n := len(closes[0])
	if n == 0 {
		return nil, errors.New("closes[0] is empty: pass at least one bar")
	}
	for i, c := range closes {
		if len(c) != n {
			return nil, fmt.Errorf("closes[%d] has %d bars, closes[0] has %d: length mismatch", i, len(c), n)
		}
	}
	if volumes != nil {
		if len(volumes) != m {
			return nil, fmt.Errorf("volumes has %d series, closes has %d: length mismatch", len(volumes), m)
		}
		for i, v := range volumes {
			if len(v) != n {
				return nil, fmt.Errorf("volumes[%d] has %d bars, closes[0] has %d: length mismatch", i, len(v), n)
			}
		}
	}
	if ts != nil && len(ts) != n {
		return nil, fmt.Errorf("ts has %d entries, closes[0] has %d bars: length mismatch", len(ts), n)
	}
	cp, err := universeParamsC(params)
	if err != nil {
		return nil, err
	}
	cBlock, cPtrs := cMatrix(closes, n)
	defer C.free(cBlock)
	defer C.free(cPtrs)
	var vPtrs **C.double
	if volumes != nil {
		vBlock, vp := cMatrix(volumes, n)
		defer C.free(vBlock)
		defer C.free(vp)
		vPtrs = (**C.double)(vp)
	}
	var tsPtr *C.int64_t
	if ts != nil {
		tsPtr = (*C.int64_t)(unsafe.Pointer(&ts[0]))
	}
	b := newUniverseBuffers(m)
	defer b.free()
	rc := C.hocdb_universe_arrays((**C.double)(cPtrs), vPtrs, C.size_t(m), C.size_t(n), tsPtr, &cp, (*C.HOCDBUniverseRow)(b.rows), (*C.double)(b.corr), (*C.HOCDBUniverseSummary)(b.summary))
	if rc != 0 {
		return nil, universeError(rc)
	}
	return b.report(), nil
}
