package hocdb_test

import (
	"fmt"
	"hocdb"
	"os"
	"testing"
)

func TestFilterSyntax(t *testing.T) {
	ticker := "TEST_GO_FILTER"
	dataDir := "../../../b_go_test_data_filter_syntax"

	// Cleanup
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "event", Type: hocdb.TypeI64},
	}

	db, err := hocdb.New(ticker, dataDir, schema, hocdb.Options{})
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer db.Close()

	// Append data
	// 1. event = 0
	rec1, _ := hocdb.CreateRecordBytes(schema, int64(100), 1.0, int64(0))
	db.Append(rec1)
	// 2. event = 1
	rec2, _ := hocdb.CreateRecordBytes(schema, int64(200), 2.0, int64(1))
	db.Append(rec2)
	// 3. event = 2
	rec3, _ := hocdb.CreateRecordBytes(schema, int64(300), 3.0, int64(2))
	db.Append(rec3)

	// Query with map filter: { "event": 1 }
	fmt.Println("Querying with filter map { event: 1 }...")
	filters := map[string]interface{}{
		"event": int64(1),
	}

	data, err := db.Query(0, 1000, filters)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	recordSize := 8 + 8 + 8
	count := len(data) / recordSize
	if count != 1 {
		t.Fatalf("Expected 1 result, got %d", count)
	}

	fmt.Println("✅ Go Filter Syntax Test Passed!")
}
