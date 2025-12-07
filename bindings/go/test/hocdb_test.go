package hocdb_test

import (
	"encoding/binary"
	"hocdb"
	"math"
	"os"
	"testing"
)

func TestHOCDB(t *testing.T) {
	// Define schema
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "volume", Type: hocdb.TypeF64},
	}

	// Create test directory
	testDir := "../../../b_go_test_data"
	os.RemoveAll(testDir)
	err := os.MkdirAll(testDir, 0755)
	if err != nil && !os.IsExist(err) {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create database instance
	db, err := hocdb.New("TEST_BTC_USD", testDir, schema, hocdb.Options{
		MaxFileSize:   0, // Use default
		OverwriteFull: false,
		FlushOnWrite:  false,
	})
	if err != nil {
		t.Fatalf("Failed to create HOCDB instance: %v", err)
	}
	defer db.Close()

	// Create and append a record
	record, err := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, 1.5)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	err = db.Append(record)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	// Append another record
	record, err = hocdb.CreateRecordBytes(schema, int64(1620000001), 50001.0, 1.6)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	err = db.Append(record)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	// Test flush
	err = db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Load all data
	data, err := db.Load()
	if err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}

	t.Logf("Loaded %d bytes of data", len(data))

	// Query data
	qdata, err := db.Query(1620000000, 1620000002, nil)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}

	t.Logf("Queried %d bytes of data in range", len(qdata))

	// Test GetLatest
	latest, err := db.GetLatest(1) // Get latest price
	if err != nil {
		t.Fatalf("Failed to get latest: %v", err)
	}

	if latest.Value != 50001.0 {
		t.Errorf("Expected latest price 50001.0, got %f", latest.Value)
	}

	if latest.Timestamp != 1620000001 {
		t.Errorf("Expected latest timestamp 1620000001, got %d", latest.Timestamp)
	}

	// Test GetStats
	stats, err := db.GetStats(1620000000, 1620000002, 1) // Get stats for price field
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.Count != 2 {
		t.Errorf("Expected count 2, got %d", stats.Count)
	}

	if stats.Min != 50000.0 {
		t.Errorf("Expected min 50000.0, got %f", stats.Min)
	}

	if stats.Max != 50001.0 {
		t.Errorf("Expected max 50001.0, got %f", stats.Max)
	}
}

func TestCreateRecordBytes(t *testing.T) {
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "volume", Type: hocdb.TypeU64},
	}

	// Test creating a record with mixed types
	record, err := hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0, uint64(1500))
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	// Check that record has the right size (8 bytes per field)
	expectedSize := 8 * len(schema)
	if len(record) != expectedSize {
		t.Errorf("Expected record size %d, got %d", expectedSize, len(record))
	}

	// Test error case: wrong number of values
	_, err = hocdb.CreateRecordBytes(schema, int64(1620000000), 50000.0) // Missing one value
	if err == nil {
		t.Error("Expected error for mismatched schema/value count")
	}
	if err == nil {
		t.Error("Expected error for mismatched schema/value count")
	}
}

func TestQueryFiltering(t *testing.T) {
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "event", Type: hocdb.TypeString},
	}

	testDir := "../../../b_go_test_data_filter"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	db, err := hocdb.New("FILTER_TEST", testDir, schema, hocdb.Options{})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Append records
	// 1. Deposit
	rec1, _ := hocdb.CreateRecordBytes(schema, int64(100), 100.0, "deposit")
	db.Append(rec1)
	// 2. Withdraw
	rec2, _ := hocdb.CreateRecordBytes(schema, int64(200), 50.0, "withdraw")
	db.Append(rec2)
	// 3. Deposit
	rec3, _ := hocdb.CreateRecordBytes(schema, int64(300), 200.0, "deposit")
	db.Append(rec3)

	db.Flush()

	// Filter by event = "deposit"
	filters := map[string]interface{}{
		"event": "deposit",
	}

	data, err := db.Query(0, 1000, filters)
	if err != nil {
		t.Fatalf("Failed to query with filter: %v", err)
	}

	// Expect 2 records (rec1 and rec3)
	recordSize := 8 + 8 + 128
	if len(data) != 2*recordSize {
		t.Errorf("Expected %d bytes (2 records), got %d", 2*recordSize, len(data))
	}
}

func TestAutoIncrement(t *testing.T) {
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "value", Type: hocdb.TypeF64},
	}

	testDir := "../../../b_go_test_auto_inc"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 1. Initialize with AutoIncrement = true
	db, err := hocdb.New("TEST_AUTO_INC", testDir, schema, hocdb.Options{AutoIncrement: true})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Append 10 records with dummy timestamp
	for i := 0; i < 10; i++ {
		record, _ := hocdb.CreateRecordBytes(schema, int64(0), float64(i))
		err := db.Append(record)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}
	db.Close()

	// 2. Reopen and verify
	db, err = hocdb.New("TEST_AUTO_INC", testDir, schema, hocdb.Options{AutoIncrement: true})
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}

	data, err := db.Load()
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	recordSize := 16 // 8 + 8
	count := len(data) / recordSize
	if count != 10 {
		t.Errorf("Expected 10 records, got %d", count)
	}

	// Verify content
	// We need to parse bytes manually or use a helper.
	// Go doesn't have easy struct casting from bytes like C.
	// We can use binary.LittleEndian.
	for i := 0; i < 10; i++ {
		offset := i * recordSize
		ts := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		valBits := binary.LittleEndian.Uint64(data[offset+8 : offset+16])
		val := math.Float64frombits(valBits)

		if ts != int64(i+1) {
			t.Errorf("Record %d: Expected timestamp %d, got %d", i, i+1, ts)
		}
		if val != float64(i) {
			t.Errorf("Record %d: Expected value %f, got %f", i, float64(i), val)
		}
	}
	db.Close()
}

func BenchmarkAppend(b *testing.B) {
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "volume", Type: hocdb.TypeF64},
	}
	testDir := "../../../b_go_test_data"
	os.MkdirAll(testDir, 0755)
	// defer os.RemoveAll(testDir)

	db, _ := hocdb.New("BENCH_APPEND", testDir, schema, hocdb.Options{})
	defer db.Close()

	record, _ := hocdb.CreateRecordBytes(schema, int64(100), 10.0, 20.0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Append(record)
	}
}

func BenchmarkLoad(b *testing.B) {
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "volume", Type: hocdb.TypeF64},
	}
	testDir := "../../../b_go_test_data"
	os.MkdirAll(testDir, 0755)
	// defer os.RemoveAll(testDir)

	db, _ := hocdb.New("BENCH_LOAD", testDir, schema, hocdb.Options{})
	defer db.Close()

	record, _ := hocdb.CreateRecordBytes(schema, int64(100), 10.0, 20.0)
	for i := 0; i < 10000; i++ {
		db.Append(record)
	}
	db.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Load()
	}
}

func BenchmarkGetStats(b *testing.B) {
	schema := []hocdb.Field{
		{Name: "timestamp", Type: hocdb.TypeI64},
		{Name: "price", Type: hocdb.TypeF64},
		{Name: "volume", Type: hocdb.TypeF64},
	}
	testDir := "../../../b_go_test_data"
	os.MkdirAll(testDir, 0755)
	// defer os.RemoveAll(testDir)

	db, _ := hocdb.New("BENCH_STATS", testDir, schema, hocdb.Options{})
	defer db.Close()

	// Append 100k records
	for i := 0; i < 100000; i++ {
		record, _ := hocdb.CreateRecordBytes(schema, int64(i), float64(i), float64(i))
		db.Append(record)
	}
	db.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.GetStats(0, 100000, 1)
	}
}

func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Cleanup
	// os.RemoveAll("./b_go_test_data")

	// Exit with the same code as the tests
	os.Exit(code)
}
