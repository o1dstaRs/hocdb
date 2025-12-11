# Go Bindings for HOCDB

Go bindings for HOCDB - High-Performance Time Series Database.

## Prerequisites

Before using the Go bindings, you need to build the C library:

```bash
zig build c-bindings
```

This will create the necessary C library in `zig-out/lib/` that the Go bindings will link against.

## Installation

To use the Go bindings in your project, you can add it as a module dependency. Since this is a local project, you may need to reference it directly or use Go workspace functionality:

```bash
# If using Go workspace (Go 1.18+)
go work init
go work use . 
go work use ./bindings/go

# Or if referencing directly in your project
go mod edit -replace=hocdb=./bindings/go
```

## Usage

```go
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
    db, err := hocdb.New("BTC_USD", "./go_example_data", schema, hocdb.Options{
        MaxFileSize:   0, // Use default
        OverwriteFull: false,
        FlushOnWrite:  false,
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create and append records
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
```

## API Documentation

### Types

- `TypeI64`: 64-bit signed integer field type
- `TypeF64`: 64-bit floating point field type  
- `TypeU64`: 64-bit unsigned integer field type

### Functions

#### `New(ticker, path string, schema []Field, options Options) (*DB, error)`

Creates a new HOCDB instance with the specified schema.

#### `CreateRecordBytes(schema []Field, values ...interface{}) ([]byte, error)`

Creates raw bytes for a record based on the schema and values. This helps convert Go values to the required binary format.

#### `Append(data []byte) error`

Appends raw record data to the database.

#### `Load() ([]byte, error)`

Loads all records from the database.

#### `Query(startTs, endTs int64) ([]byte, error)`

Queries records within the specified time range [startTs, endTs).

#### `GetStats(startTs, endTs int64, fieldIndex int) (*Stats, error)`

Returns statistics for a specific field within a time range.

#### `GetStatsByName(startTs, endTs int64, fieldName string) (*Stats, error)`

Returns statistics for a specific field (by name) within a time range.

#### `GetLatest(fieldIndex int) (*Latest, error)`

Returns the latest value and timestamp for a specific field.

#### `GetLatestByName(fieldName string) (*Latest, error)`

Returns the latest value and timestamp for a specific field (by name).

#### `Flush() error`

Forces a write of all pending data to disk.

#### `Close()`

Closes the database and frees resources.

## Building and Testing

To test the bindings, run from the Go bindings directory:

```bash
cd bindings/go
go mod tidy  # if needed
go test -v
```

## Architecture

The Go bindings use CGO to interface with the underlying C library. The `hocdb.h` header file provides the C API that is wrapped by the Go code in `hocdb.go`.

- CGO CFLAGS: `-I../../bindings/c` (to find hocdb.h)
- CGO LDFLAGS: `-L../../zig-out/lib -lhocdb_c` (to link with the C library)