# HOCDB Go Bindings

Go bindings for HOCDB - The World's Most Performant Time-Series Database.

## Prerequisites

Before using the Go bindings, build the C library:

```bash
zig build c-bindings
```

This creates the necessary C library in `zig-out/lib/` that the Go bindings link against.

## Installation

```bash
# If using Go workspace (Go 1.18+)
go work init
go work use .
go work use ./bindings/go

# Or reference directly in your project
go mod edit -replace=hocdb=./bindings/go
```

## Quick Start

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
        {Name: "active", Type: hocdb.TypeBool},
    }

    // Create database instance
    db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{})
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create and append records
    record, _ := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5, true)
    db.Append(record)

    record, _ = hocdb.CreateRecordBytes(schema, int64(1620000001), 50100.0, 2.0, true)
    db.Append(record)

    db.Flush()

    // Get statistics
    stats, _ := db.GetStatsByName(1620000000, 1620001000, "price", false)
    fmt.Printf("Price: min=%.2f, max=%.2f, mean=%.2f\n", stats.Min, stats.Max, stats.Mean)

    // Get latest value
    latest, _ := db.GetLatestByName("price")
    fmt.Printf("Latest: %.2f at %d\n", latest.Value, latest.Timestamp)
}
```

## API Reference

### Field Types

```go
const (
    TypeI64    FieldType = 1  // Signed 64-bit integer
    TypeF64    FieldType = 2  // 64-bit floating point
    TypeU64    FieldType = 3  // Unsigned 64-bit integer
    TypeString FieldType = 5  // Fixed 128-byte string
    TypeBool   FieldType = 6  // Boolean (1 byte)
)
```

### Data Structures

```go
// Field defines a field in the database schema
type Field struct {
    Name string
    Type FieldType
}

// Options contains configuration options for the database
type Options struct {
    MaxFileSize   int64  // Max file size (0 = default)
    OverwriteFull bool   // Ring buffer mode
    FlushOnWrite  bool   // Flush on every write
    AutoIncrement bool   // Auto-increment timestamps
}

// Stats represents statistics for a field in a time range
type Stats struct {
    Min   float64
    Max   float64
    Sum   float64
    Count uint64
    Mean  float64
    P50   float64  // 50th percentile (if requested)
    P90   float64  // 90th percentile
    P95   float64  // 95th percentile
    P99   float64  // 99th percentile
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
```

---

### `New(ticker, path string, schema []Field, options Options) (*DB, error)`

Create a new HOCDB instance.

```go
schema := []hocdb.Field{
    {Name: "timestamp", Type: hocdb.TypeI64},
    {Name: "price", Type: hocdb.TypeF64},
    {Name: "volume", Type: hocdb.TypeF64},
}

// Basic initialization
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{})

// With ring buffer (100MB, overwrite when full)
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{
    MaxFileSize:   100 * 1024 * 1024,
    OverwriteFull: true,
})

// With auto-increment timestamps
db, err := hocdb.New("BTC_USD", "./data", schema, hocdb.Options{
    AutoIncrement: true,
})
```

---

### `CreateRecordBytes(schema []Field, values ...interface{}) ([]byte, error)`

Create raw bytes for a record based on the schema. This helper converts Go values to the required binary format.

```go
schema := []hocdb.Field{
    {Name: "timestamp", Type: hocdb.TypeI64},
    {Name: "price", Type: hocdb.TypeF64},
    {Name: "volume", Type: hocdb.TypeF64},
}

record, err := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
if err != nil {
    panic(err)
}
```

**Supported value types:**
- `TypeI64`: `int64`, `int`, `int32`
- `TypeF64`: `float64`, `float32`, `int`
- `TypeU64`: `uint64`, `uint`, non-negative `int`
- `TypeString`: `string` (padded to 128 bytes)
- `TypeBool`: `bool`

---

### `Append(data []byte) error`

Append raw record data to the database.

```go
record, _ := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
err := db.Append(record)
if err != nil {
    // Handle error (invalid size, non-monotonic timestamp)
}
```

---

### `Flush() error`

Force buffered data to be written to disk.

```go
err := db.Flush()
```

---

### `Load() ([]byte, error)`

Load all records from the database.

```go
data, err := db.Load()
if err != nil {
    panic(err)
}
fmt.Printf("Loaded %d bytes\n", len(data))
```

---

### `Query(startTs, endTs int64, filters interface{}) ([]byte, error)`

Query records within a timestamp range with optional filters.

**Filter formats:**
- `[]Filter`: Array of Filter structs
- `map[string]interface{}`: Map of field name to value

```go
// Query without filters
data, err := db.Query(1620000000, 1620001000, nil)

// Query with map filter
filters := map[string]interface{}{
    "price": 50000.0,
    "active": true,
}
data, err := db.Query(1620000000, 1620001000, filters)

// Query with Filter slice
filters := []hocdb.Filter{
    {FieldIndex: 1, Value: 50000.0},
}
data, err := db.Query(1620000000, 1620001000, filters)
```

---

### `GetStats(startTs, endTs int64, fieldIndex int, computePercentiles bool) (*Stats, error)`

Get statistics for a field by index.

```go
// Basic stats
stats, err := db.GetStats(1620000000, 1620001000, 1, false)
fmt.Printf("Min: %.2f, Max: %.2f, Mean: %.2f\n", stats.Min, stats.Max, stats.Mean)

// With percentiles
stats, err := db.GetStats(1620000000, 1620001000, 1, true)
fmt.Printf("P99: %.2f\n", stats.P99)
```

### `GetStatsByName(startTs, endTs int64, fieldName string, computePercentiles bool) (*Stats, error)`

Get statistics for a field by name.

```go
stats, err := db.GetStatsByName(1620000000, 1620001000, "price", true)
```

---

### `GetLatest(fieldIndex int) (*Latest, error)`

Get the latest value and timestamp for a field by index.

```go
latest, err := db.GetLatest(1)
fmt.Printf("Latest: %.2f at %d\n", latest.Value, latest.Timestamp)
```

### `GetLatestByName(fieldName string) (*Latest, error)`

Get the latest value and timestamp for a field by name.

```go
latest, err := db.GetLatestByName("price")
```

---

### `Close()`

Close the database and free resources.

```go
db.Close()
```

---

### `Drop()`

Close the database and delete all data files.

```go
// WARNING: This permanently deletes all data!
db.Drop()
```

---

## Complete Example

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
        {Name: "is_buy", Type: hocdb.TypeBool},
    }

    // Initialize with ring buffer
    db, err := hocdb.New("ETH_USD", "./market_data", schema, hocdb.Options{
        MaxFileSize:   100 * 1024 * 1024, // 100MB
        OverwriteFull: true,
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Append some trades
    trades := []struct {
        ts     int64
        price  float64
        volume float64
        isBuy  bool
    }{
        {1620000000, 2500.0, 10.0, true},
        {1620000001, 2501.5, 5.0, false},
        {1620000002, 2502.0, 15.0, true},
    }

    for _, t := range trades {
        record, _ := hocdb.CreateRecordBytes(schema, t.ts, t.price, t.volume, t.isBuy)
        if err := db.Append(record); err != nil {
            fmt.Printf("Append failed: %v\n", err)
        }
    }
    db.Flush()

    // Query buy orders only
    filters := map[string]interface{}{
        "is_buy": true,
    }
    data, _ := db.Query(1620000000, 1620001000, filters)
    fmt.Printf("Buy orders: %d bytes\n", len(data))

    // Get price statistics with percentiles
    stats, _ := db.GetStatsByName(1620000000, 1620001000, "price", true)
    fmt.Printf("Price: min=%.2f, max=%.2f, mean=%.2f, p99=%.2f\n",
        stats.Min, stats.Max, stats.Mean, stats.P99)

    // Get latest price
    latest, _ := db.GetLatestByName("price")
    fmt.Printf("Latest: %.2f at %d\n", latest.Value, latest.Timestamp)

    // Load all data
    allData, _ := db.Load()
    fmt.Printf("Total data: %d bytes\n", len(allData))
}
```

## Architecture

The Go bindings use CGO to interface with the underlying C library:

- **CGO CFLAGS**: `-I../../bindings/c` (to find hocdb.h)
- **CGO LDFLAGS**: `-L../../zig-out/lib -lhocdb_c` (to link with the C library)

## Testing

```bash
cd bindings/go
go mod tidy
go test -v
```
