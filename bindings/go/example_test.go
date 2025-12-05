package hocdb

import (
	"fmt"
	"os"
)

func Example() {
	// Define schema
	schema := []Field{
		{Name: "timestamp", Type: TypeI64},
		{Name: "price", Type: TypeF64},
		{Name: "volume", Type: TypeF64},
	}

	// Create test directory
	testDir := "../../b_go_test_data"
	// os.RemoveAll(testDir) // Clean start for example
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		panic(err)
	}
	// defer os.RemoveAll(testDir) // Keep data for inspection

	// Create database instance
	db, err := New("BTC_USD", testDir, schema, Options{
		MaxFileSize:   0, // Use default
		OverwriteFull: false,
		FlushOnWrite:  false,
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create and append records
	record, err := CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
	if err != nil {
		panic(err)
	}
	err = db.Append(record)
	if err != nil {
		panic(err)
	}

	record, err = CreateRecordBytes(schema, int64(1620000001), 50001.0, 1.6)
	if err != nil {
		panic(err)
	}
	err = db.Append(record)
	if err != nil {
		panic(err)
	}

	// Flush to ensure data is on disk
	db.Flush()

	// Load all data
	data, err := db.Load()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d bytes of data\n", len(data))

	// Query data
	qdata, err := db.Query(1620000000, 1620000002, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Queried %d bytes of data in range\n", len(qdata))

	// Get latest price
	latest, err := db.GetLatest(1) // Get latest price
	if err != nil {
		panic(err)
	}
	fmt.Printf("Latest price: %.1f at timestamp %d\n", latest.Value, latest.Timestamp)

	// Get stats for price field
	stats, err := db.GetStats(1620000000, 1620000002, 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Price stats - Min: %.1f, Max: %.1f, Count: %d\n", stats.Min, stats.Max, stats.Count)

	// Output:
	// Loaded 48 bytes of data
	// Queried 48 bytes of data in range
	// Latest price: 50001.0 at timestamp 1620000001
	// Price stats - Min: 50000.0, Max: 50001.0, Count: 2
}
