package hocdb_test

import (
	"errors"
	"hocdb"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Durability, lock-free readers, maintenance and metrics (mirrors
// bindings/c/test/test_storage.c).

var storageSchema = []hocdb.Field{
	{Name: "timestamp", Type: hocdb.TypeI64},
	{Name: "value", Type: hocdb.TypeF64},
}

const storageRecordSize = 8 + 8

// storageCount returns the number of records the handle currently sees.
func storageCount(t *testing.T, db *hocdb.DB, what string) uint64 {
	t.Helper()
	st, err := db.GetStats(math.MinInt64, math.MaxInt64, 1, false)
	if err != nil {
		t.Fatalf("%s: GetStats failed: %v", what, err)
	}
	return st.Count
}

// storageMin returns the smallest value (== timestamp in this data set) the handle sees.
func storageMin(t *testing.T, db *hocdb.DB, what string) float64 {
	t.Helper()
	st, err := db.GetStats(math.MinInt64, math.MaxInt64, 1, false)
	if err != nil {
		t.Fatalf("%s: GetStats failed: %v", what, err)
	}
	return st.Min
}

// storageAppend appends n records with timestamps from, from+1, ... and value == timestamp.
func storageAppend(t *testing.T, db *hocdb.DB, from int64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ts := from + int64(i)
		rec, err := hocdb.CreateRecordBytes(storageSchema, ts, float64(ts))
		if err != nil {
			t.Fatalf("CreateRecordBytes(%d) failed: %v", ts, err)
		}
		if err := db.Append(rec); err != nil {
			t.Fatalf("Append(%d) failed: %v", ts, err)
		}
	}
}

func storageMetrics(t *testing.T, db *hocdb.DB, what string) map[string]int64 {
	t.Helper()
	m, err := db.Metrics()
	if err != nil {
		t.Fatalf("%s: Metrics failed: %v", what, err)
	}
	return m
}

func TestStorage(t *testing.T) {
	testDir := "../../../b_go_test_storage"
	os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	if hocdb.HeaderSize() != 64 {
		t.Fatalf("HeaderSize() = %d, want 64", hocdb.HeaderSize())
	}

	opts := hocdb.Options{Fsync: hocdb.FsyncOnFlush, TimestampUnitNs: 1_000_000_000}

	// 1. writer: append, flush, verify, metrics
	w, err := hocdb.New("T", testDir, storageSchema, opts)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer w.Close()
	if v := w.FormatVersion(); v != 2 {
		t.Errorf("writer FormatVersion() = %d, want 2", v)
	}
	if w.IsReadOnly() {
		t.Errorf("writer IsReadOnly() = true, want false")
	}
	storageAppend(t, w, 1, 1000)
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if ok, err := w.Verify(); err != nil || !ok {
		t.Fatalf("Verify() = %v, %v; want true, nil", ok, err)
	}
	m := storageMetrics(t, w, "writer")
	if len(m) != 30 {
		t.Errorf("Metrics() has %d fields, want 30: %v", len(m), m)
	}
	for name, want := range map[string]int64{
		"appends":           1000,
		"bytes_written":     1000 * storageRecordSize,
		"committed_records": 1000,
		"last_record_ts":    1000,
		"format_version":    2,
		"read_only":         0,
	} {
		if got := m[name]; got != want {
			t.Errorf("writer metrics[%q] = %d, want %d", name, got, want)
		}
	}
	for _, name := range []string{"flushes", "commits", "fsyncs"} {
		if m[name] < 1 {
			t.Errorf("writer metrics[%q] = %d, want >= 1", name, m[name])
		}
	}
	if m["ingest_lag_wall_ns"] < 0 {
		t.Errorf("writer metrics[ingest_lag_wall_ns] = %d, want >= 0", m["ingest_lag_wall_ns"])
	}
	if m["ingest_lag_record_ns"] == 0 {
		t.Errorf("writer metrics[ingest_lag_record_ns] = 0, want != 0 with TimestampUnitNs set")
	}

	// 2. a second writer is refused
	if w2, err := hocdb.New("T", testDir, storageSchema, opts); err == nil {
		w2.Close()
		t.Fatalf("second writer opened; want an error naming DatabaseLocked")
	} else if !strings.Contains(err.Error(), "DatabaseLocked") {
		t.Errorf("second writer error = %q, want it to mention DatabaseLocked", err)
	}

	// 3. lock-free reader in the same process
	r, err := hocdb.OpenReader("T", testDir, storageSchema)
	if err != nil {
		t.Fatalf("OpenReader failed: %v", err)
	}
	defer r.Close()
	if !r.IsReadOnly() {
		t.Errorf("reader IsReadOnly() = false, want true")
	}
	if v := r.FormatVersion(); v != 2 {
		t.Errorf("reader FormatVersion() = %d, want 2", v)
	}
	if n := storageCount(t, r, "reader"); n != 1000 {
		t.Errorf("reader sees %d records, want 1000", n)
	}
	storageAppend(t, w, 1001, 500)
	if err := r.Refresh(); err != nil {
		t.Fatalf("reader Refresh failed: %v", err)
	}
	if n := storageCount(t, r, "reader before flush"); n != 1000 {
		t.Errorf("uncommitted records visible: reader sees %d, want 1000", n)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("reader Refresh failed: %v", err)
	}
	if n := storageCount(t, r, "reader after flush"); n != 1500 {
		t.Errorf("reader sees %d records after the commit, want 1500", n)
	}
	latest, err := r.GetLatest(1)
	if err != nil {
		t.Fatalf("reader GetLatest failed: %v", err)
	}
	if latest.Timestamp != 1500 {
		t.Errorf("reader GetLatest timestamp = %d, want 1500", latest.Timestamp)
	}

	// writes and maintenance on the reader are read-only errors
	bad, _ := hocdb.CreateRecordBytes(storageSchema, int64(9999), 0.0)
	if err := r.Append(bad); err == nil {
		t.Errorf("reader Append succeeded; want a read-only error")
	} else if !errors.Is(err, hocdb.ErrReadOnly) || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("reader Append error = %q, want ErrReadOnly mentioning read-only", err)
	}
	if err := r.Sync(); !errors.Is(err, hocdb.ErrReadOnly) {
		t.Errorf("reader Sync error = %v, want ErrReadOnly", err)
	}
	if err := r.Compact(0); !errors.Is(err, hocdb.ErrReadOnly) {
		t.Errorf("reader Compact error = %v, want ErrReadOnly", err)
	}
	if err := r.RetainLast(1); !errors.Is(err, hocdb.ErrReadOnly) {
		t.Errorf("reader RetainLast error = %v, want ErrReadOnly", err)
	}
	if _, err := r.Rollover(); !errors.Is(err, hocdb.ErrReadOnly) {
		t.Errorf("reader Rollover error = %v, want ErrReadOnly", err)
	}
	rm := storageMetrics(t, r, "reader")
	if rm["read_only"] != 1 {
		t.Errorf("reader metrics[read_only] = %d, want 1", rm["read_only"])
	}
	if rm["refreshes"] < 1 {
		t.Errorf("reader metrics[refreshes] = %d, want >= 1", rm["refreshes"])
	}
	if rm["reads"] < 1 {
		t.Errorf("reader metrics[reads] = %d, want >= 1", rm["reads"])
	}
	if rm["committed_records"] != 1500 {
		t.Errorf("reader metrics[committed_records] = %d, want 1500", rm["committed_records"])
	}

	// Refresh is a no-op for writers; MetricsReset keeps the state fields
	if err := w.Refresh(); err != nil {
		t.Errorf("writer Refresh failed: %v", err)
	}
	w.MetricsReset()
	m = storageMetrics(t, w, "writer after reset")
	if m["appends"] != 0 || m["flushes"] != 0 {
		t.Errorf("MetricsReset left appends=%d flushes=%d, want 0", m["appends"], m["flushes"])
	}
	if m["last_record_ts"] != 1500 || m["committed_records"] != 1500 {
		t.Errorf("MetricsReset changed state: last_record_ts=%d committed_records=%d, want 1500/1500", m["last_record_ts"], m["committed_records"])
	}

	// 4. compaction and retention; the reader follows the rewritten file
	if err := w.Compact(1001); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if n := storageCount(t, w, "writer after compact"); n != 500 {
		t.Errorf("writer sees %d records after Compact(1001), want 500", n)
	}
	if min := storageMin(t, w, "writer after compact"); min != 1001 {
		t.Errorf("min timestamp after Compact(1001) = %v, want 1001", min)
	}
	if ok, err := w.Verify(); err != nil || !ok {
		t.Errorf("Verify() after compaction = %v, %v; want true, nil", ok, err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("reader Refresh failed: %v", err)
	}
	if n := storageCount(t, r, "reader after compact"); n != 500 {
		t.Errorf("reader sees %d records after compaction, want 500", n)
	}
	if err := w.RetainLast(100); err != nil {
		t.Fatalf("RetainLast failed: %v", err)
	}
	if n := storageCount(t, w, "writer after retain"); n != 100 {
		t.Errorf("writer sees %d records after RetainLast(100), want 100", n)
	}
	if n := storageCount(t, r, "reader after retain"); n != 100 { // auto-refresh
		t.Errorf("reader sees %d records after RetainLast(100), want 100", n)
	}

	// 5. rollover
	archive, err := w.Rollover()
	if err != nil {
		t.Fatalf("Rollover failed: %v", err)
	}
	t.Logf("archive: %s", archive)
	if !strings.HasSuffix(archive, ".bin") || !strings.Contains(filepath.Base(archive), "T.") {
		t.Errorf("Rollover path = %q, want <dir>/T.<first>-<last>.bin", archive)
	}
	if filepath.Base(archive) != "T.1401-1500.bin" {
		t.Errorf("Rollover archive name = %q, want T.1401-1500.bin", filepath.Base(archive))
	}
	if _, err := os.Stat(archive); err != nil {
		t.Errorf("archive file missing: %v", err)
	}
	if n := storageCount(t, w, "writer after rollover"); n != 0 {
		t.Errorf("writer sees %d records after Rollover, want 0", n)
	}
	storageAppend(t, w, 1501, 10)
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush after rollover failed: %v", err)
	}
	if n := storageCount(t, r, "reader after rollover"); n != 10 { // reader follows the new file
		t.Errorf("reader sees %d records after Rollover, want 10", n)
	}
	m = storageMetrics(t, w, "writer after rollover")
	if m["rollovers"] != 1 || m["compactions"] != 2 {
		t.Errorf("metrics rollovers=%d compactions=%d, want 1/2", m["rollovers"], m["compactions"])
	}
	archiveTicker := strings.TrimSuffix(filepath.Base(archive), ".bin")
	a, err := hocdb.New(archiveTicker, testDir, storageSchema, hocdb.Options{})
	if err != nil {
		t.Fatalf("opening the archive %q failed: %v", archiveTicker, err)
	}
	if n := storageCount(t, a, "archive"); n != 100 {
		t.Errorf("archive holds %d records, want 100", n)
	}
	a.Close()

	// 6. crash simulation: a valid uncommitted tail is adopted, torn bytes are dropped
	r.Close()
	w.Close()
	dataFile := filepath.Join(testDir, "T.bin")
	f, err := os.OpenFile(dataFile, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	for _, ts := range []int64{1511, 1512} {
		rec, _ := hocdb.CreateRecordBytes(storageSchema, ts, float64(ts-1510))
		if _, err := f.Write(rec); err != nil {
			t.Fatalf("write tail record: %v", err)
		}
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("write torn bytes: %v", err)
	}
	f.Close()
	w3, err := hocdb.New("T", testDir, storageSchema, opts)
	if err != nil {
		t.Fatalf("reopen after crash failed: %v", err)
	}
	if n := storageCount(t, w3, "writer after crash"); n != 12 {
		t.Errorf("writer sees %d records after recovery, want 12", n)
	}
	m = storageMetrics(t, w3, "writer after crash")
	if m["recovered_tail_records"] != 2 || m["dropped_tail_bytes"] != 5 {
		t.Errorf("recovery metrics recovered_tail_records=%d dropped_tail_bytes=%d, want 2/5", m["recovered_tail_records"], m["dropped_tail_bytes"])
	}
	if err := w3.Flush(); err != nil {
		t.Fatalf("Flush after recovery failed: %v", err)
	}
	if ok, err := w3.Verify(); err != nil || !ok {
		t.Errorf("Verify() after recovery = %v, %v; want true, nil", ok, err)
	}
	w3.Close()

	// VerifyOnOpen refuses a corrupted file; without it the file still opens
	f, err = os.OpenFile(dataFile, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, int64(hocdb.HeaderSize()+3*storageRecordSize+8)); err != nil {
		t.Fatalf("corrupt data file: %v", err)
	}
	f.Close()
	vopts := opts
	vopts.VerifyOnOpen = true
	if w4, err := hocdb.New("T", testDir, storageSchema, vopts); err == nil {
		w4.Close()
		t.Errorf("VerifyOnOpen opened a corrupted file; want an error naming ChecksumMismatch")
	} else if !strings.Contains(err.Error(), "ChecksumMismatch") {
		t.Errorf("VerifyOnOpen error = %q, want it to mention ChecksumMismatch", err)
	}
	w5, err := hocdb.New("T", testDir, storageSchema, opts)
	if err != nil {
		t.Fatalf("reopen of the corrupted file failed: %v", err)
	}
	if ok, err := w5.Verify(); ok || err != nil {
		t.Errorf("Verify() on the corrupted file = %v, %v; want false, nil (MISMATCH)", ok, err)
	}
	m = storageMetrics(t, w5, "corrupted file")
	if m["crc_failures"] != 2 { // counted once at open and once by Verify
		t.Errorf("metrics[crc_failures] = %d, want 2", m["crc_failures"])
	}
	if n := storageCount(t, w5, "corrupted file"); n != 12 {
		t.Errorf("corrupted file still readable: %d records, want 12", n)
	}
	w5.Close()

	// 7. ring buffer: HeaderSize() + 50 * recordSize holds exactly 50 records
	ring, err := hocdb.New("RING", testDir, storageSchema, hocdb.Options{
		MaxFileSize:   int64(hocdb.HeaderSize() + 50*storageRecordSize),
		OverwriteFull: true,
	})
	if err != nil {
		t.Fatalf("New(ring) failed: %v", err)
	}
	defer ring.Close()
	storageAppend(t, ring, 1, 80)
	if err := ring.Flush(); err != nil {
		t.Fatalf("ring Flush failed: %v", err)
	}
	if n := storageCount(t, ring, "ring"); n != 50 {
		t.Errorf("ring buffer holds %d records after 80 appends, want 50", n)
	}
	if min := storageMin(t, ring, "ring"); min != 31 {
		t.Errorf("ring buffer oldest record = %v, want 31", min)
	}
	if _, err := ring.Verify(); !errors.Is(err, hocdb.ErrChecksumUnavailable) {
		t.Errorf("ring Verify error = %v, want ErrChecksumUnavailable", err)
	}
}

func TestStorageOptions(t *testing.T) {
	testDir := "../../../b_go_test_storage_options"
	os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// invalid fsync policy is rejected before touching the file
	if db, err := hocdb.New("O", testDir, storageSchema, hocdb.Options{Fsync: "bogus"}); err == nil {
		db.Close()
		t.Errorf("New accepted Fsync: \"bogus\"")
	} else if !strings.Contains(err.Error(), "fsync") {
		t.Errorf("invalid fsync error = %q, want it to mention fsync", err)
	}

	// every spelling of the policies is accepted, including the numbers 0..3
	for _, policy := range []string{"", hocdb.FsyncNone, hocdb.FsyncOnClose, hocdb.FsyncOnFlush, hocdb.FsyncInterval, "0", "1", "2", "3", " On_Flush "} {
		db, err := hocdb.New("O", testDir, storageSchema, hocdb.Options{Fsync: policy, FsyncIntervalMs: 50, IndexStride: 256})
		if err != nil {
			t.Fatalf("New(Fsync: %q) failed: %v", policy, err)
		}
		db.Close()
	}

	// Sync fsyncs immediately whatever the policy
	db, err := hocdb.New("O", testDir, storageSchema, hocdb.Options{Fsync: hocdb.FsyncNone})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	storageAppend(t, db, 1, 5)
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	m := storageMetrics(t, db, "sync")
	if m["fsyncs"] < 1 || m["committed_records"] != 5 {
		t.Errorf("after Sync: fsyncs=%d committed_records=%d, want >= 1 / 5", m["fsyncs"], m["committed_records"])
	}
	db.Close()

	// a reader needs an existing database
	if r, err := hocdb.OpenReader("MISSING", testDir, storageSchema); err == nil {
		r.Close()
		t.Errorf("OpenReader opened a database that does not exist")
	} else {
		t.Logf("OpenReader on a missing database: %v", err)
	}
}
