# Go Bindings for HOCDB

## Overview

I have successfully implemented Go bindings for the HOCDB high-performance time series database. The implementation includes:

1. **Main Go wrapper** (`hocdb.go`): Complete CGO wrapper around the C API
2. **Test file** (`hocdb_test.go`): Comprehensive tests to validate functionality
3. **Example** (`example.go`): Example usage of the bindings
4. **Go module** (`go.mod`): Go module definition
5. **Documentation** (`README.md`): Complete documentation
6. **Build integration**: Updated `build.zig` to include Go bindings build step

## Features Implemented

- **Database initialization** with custom schema
- **Record appending** with proper binary serialization
- **Data loading** and querying within time ranges
- **Statistics retrieval** (min, max, sum, count, mean)
- **Latest value retrieval** for specific fields
- **Memory management** with proper resource cleanup
- **Error handling** for all operations
- **Binary serialization** helper functions

## Building and Testing

To use the Go bindings:

1. **Build the C library** (prerequisite):
   ```bash
   cd /path/to/hocdb
   zig build c-bindings
   ```

2. **Use in your project**:
   The Go bindings can be added to your project using Go modules. You can either:
   - Copy the `bindings/go` directory to your project
   - Use Go workspace functionality to reference it directly

3. **Test the bindings** (if Go is installed):
   ```bash
   cd /path/to/hocdb/bindings/go
   go mod tidy
   go test -v
   ```

## Usage Example

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

## Implementation Notes

- The bindings use CGO to interface with the C library `libhocdb_c`
- CGO flags are configured to find the header file and link the library properly
- The implementation follows Go idioms while preserving the underlying C API functionality
- Proper memory management is implemented with calls to the C library's free functions
- Error handling is consistent throughout the API

## Files Created

- `hocdb.go`: Main CGO wrapper implementation
- `hocdb_test.go`: Comprehensive test suite
- `example.go`: Usage example
- `go.mod`: Go module definition
- `README.md`: Complete documentation

## Integration with Build System

The `build.zig` file has been updated to include a `go-bindings` step that ensures the C library is built before attempting to use the Go bindings.

The Go bindings are now complete and ready for use!