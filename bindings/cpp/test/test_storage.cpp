// C++ wrapper test for durability, lock-free readers, maintenance and metrics
// (hocdb::Database, round 3). Mirrors bindings/c/test/test_storage.c.
#include "hocdb_cpp.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#define CHECK(cond, msg)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::cerr << "FAIL: " << msg << " (line " << __LINE__ << ")\n";          \
      return 1;                                                                \
    }                                                                          \
  } while (0)

// Expects `expr` to throw hocdb::Exception whose message contains `needle`.
#define EXPECT_THROW_MSG(expr, needle, msg)                                    \
  do {                                                                         \
    bool thrown_ = false;                                                      \
    std::string what_;                                                         \
    try {                                                                      \
      (void)(expr);                                                            \
    } catch (const hocdb::Exception &e) {                                      \
      thrown_ = true;                                                          \
      what_ = e.what();                                                        \
    }                                                                          \
    CHECK(thrown_, msg);                                                       \
    CHECK(contains(what_, needle),                                             \
          std::string(msg) + ": message \"" + what_ +                          \
              "\" does not mention \"" + needle + "\"");                       \
  } while (0)

// The C ABI struct passed to hocdb_init_ex is 72 bytes (see hocdb.h).
static_assert(sizeof(HOCDBConfig) == 80, "HOCDBConfig must be 80 bytes");
static_assert(hocdb::FsyncPolicy::None == static_cast<hocdb::FsyncPolicy>(HOCDB_FSYNC_NONE) &&
                  hocdb::FsyncPolicy::OnClose == static_cast<hocdb::FsyncPolicy>(HOCDB_FSYNC_ON_CLOSE) &&
                  hocdb::FsyncPolicy::OnFlush == static_cast<hocdb::FsyncPolicy>(HOCDB_FSYNC_ON_FLUSH) &&
                  hocdb::FsyncPolicy::Interval == static_cast<hocdb::FsyncPolicy>(HOCDB_FSYNC_INTERVAL),
              "FsyncPolicy values match HOCDB_FSYNC_*");

struct Rec {
  int64_t timestamp;
  double value;
};
static_assert(sizeof(Rec) == 16, "record size");

static const char *kDir = "b_cpp_test_storage";
static const int64_t kMin = std::numeric_limits<int64_t>::min();
static const int64_t kMax = std::numeric_limits<int64_t>::max();

static bool contains(const std::string &s, const std::string &needle) {
  return s.find(needle) != std::string::npos;
}

static bool endsWith(const std::string &s, const std::string &suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static void appendN(hocdb::Database &db, int64_t from, int n) {
  for (int i = 0; i < n; ++i) {
    db.append(Rec{from + i, static_cast<double>(from + i)});
  }
}

static uint64_t count(hocdb::Database &db) {
  return db.getStats(kMin, kMax, "timestamp").count;
}

static int run() {
  const std::vector<hocdb::Field> schema = {{"timestamp", HOCDB_TYPE_I64},
                                            {"value", HOCDB_TYPE_F64}};
  const std::string file = std::string(kDir) + "/T.bin";

  // 7 (part 1). header size and struct layouts
  CHECK(hocdb::Database::headerSize() == 64, "headerSize() == 64");
  CHECK(hocdb_metrics_size() == sizeof(HOCDBMetrics), "HOCDBMetrics layout matches the library");
  CHECK(hocdb_metrics_field_count() == 30, "30 metrics fields");

  hocdb::Config cfg;
  cfg.fsync = hocdb::FsyncPolicy::OnFlush;
  cfg.timestamp_unit_ns = 1000000000ULL; // seconds

  // 1. writer: 1000 records, flush, verify, metrics
  hocdb::Database w("T", kDir, schema, cfg);
  CHECK(w.is_valid() && !w.isReadOnly() && w.formatVersion() == 2, "writer flags");
  appendN(w, 1, 1000);
  w.flush();
  CHECK(w.verify(), "checksum ok after flush");
  HOCDBMetrics m = w.metrics();
  CHECK(m.appends == 1000 && m.flushes >= 1 && m.fsyncs >= 1 && m.commits >= 1, "writer counters");
  CHECK(m.bytes_written == 1000 * sizeof(Rec), "bytes written");
  CHECK(m.committed_records == 1000 && m.read_only == 0 && m.format_version == 2 && m.last_record_ts == 1000,
        "writer state");
  CHECK(m.ingest_lag_wall_ns >= 0 && m.ingest_lag_record_ns != 0, "lag fields");
  const std::map<std::string, double> mm = w.metricsMap();
  CHECK(mm.size() == 30, "metricsMap has 30 fields");
  CHECK(mm.at("appends") == 1000 && mm.at("committed_records") == 1000 && mm.at("format_version") == 2 &&
            mm.at("read_only") == 0 && mm.at("last_record_ts") == 1000 &&
            mm.at("bytes_written") == static_cast<double>(m.bytes_written),
        "metricsMap values");
  w.refresh(); // no-op for writers
  w.sync();
  CHECK(w.metrics().fsyncs >= m.fsyncs, "sync does not lose fsyncs");

  // 2. a second writer is refused (both constructors go through hocdb_init_ex)
  EXPECT_THROW_MSG(hocdb::Database("T", kDir, schema, cfg), "DatabaseLocked", "second writer refused");
  EXPECT_THROW_MSG(hocdb::Database("T", kDir, schema), "DatabaseLocked", "legacy constructor refused too");
  CHECK(hocdb::Database::lastError() == "DatabaseLocked", "lastError()");

  // 3. lock-free reader in the same process
  hocdb::Database r = hocdb::Database::openReader("T", kDir, schema);
  CHECK(r.is_valid() && r.isReadOnly() && r.formatVersion() == 2, "reader flags");
  CHECK(count(r) == 1000, "reader sees 1000");
  appendN(w, 1001, 500); // not flushed
  r.refresh();
  CHECK(count(r) == 1000, "uncommitted data is invisible");
  w.flush();
  r.refresh();
  CHECK(count(r) == 1500, "reader sees 1500 after the commit");
  CHECK(r.getLatest("value").second == 1500, "reader latest (auto refresh)");
  EXPECT_THROW_MSG(r.append(Rec{9999, 0}), "read-only", "reader append");
  r.flush(); // readers have nothing to flush: a no-op, like refresh() on writers
  EXPECT_THROW_MSG(r.sync(), "read-only", "reader sync");
  EXPECT_THROW_MSG(r.compact(0), "read-only", "reader compact");
  EXPECT_THROW_MSG(r.retainLast(1), "read-only", "reader retainLast");
  EXPECT_THROW_MSG(r.rollover(), "read-only", "reader rollover");
  CHECK(count(r) == 1500 && count(w) == 1500, "refused writes change nothing");
  const HOCDBMetrics rm = r.metrics();
  CHECK(rm.read_only == 1 && rm.refreshes >= 1 && rm.reads >= 3 && rm.appends == 0, "reader metrics");
  CHECK(r.metricsMap().at("read_only") == 1 && r.metricsMap().at("refreshes") >= 1, "reader metricsMap");
  w.metricsReset();
  m = w.metrics();
  CHECK(m.appends == 0 && m.flushes == 0 && m.last_record_ts == 1500 && m.committed_records == 1500,
        "metricsReset zeroes the counters and keeps the state");

  // 4. compaction and retention; the reader follows the rewritten file
  w.compact(1001);
  HOCDBStats ts = w.getStats(kMin, kMax, "timestamp");
  CHECK(ts.count == 500 && ts.min == 1001.0, "compacted to timestamp >= 1001");
  CHECK(w.verify(), "checksum ok after compaction");
  r.refresh();
  CHECK(count(r) == 500, "reader follows compaction");
  w.retainLast(100);
  ts = w.getStats(kMin, kMax, "timestamp");
  CHECK(ts.count == 100 && ts.min == 1401.0 && ts.max == 1500.0, "retainLast keeps the last 100");
  r.refresh();
  CHECK(count(r) == 100, "reader follows retainLast");

  // 5. rollover: archive + empty live file
  const std::string archive = w.rollover();
  CHECK(endsWith(archive, ".bin"), "archive path ends with .bin");
  CHECK(contains(archive, "T."), "archive path contains the ticker");
  CHECK(contains(archive, "T.1401-1500.bin"), "archive name is <ticker>.<first_ts>-<last_ts>.bin");
  CHECK(std::filesystem::exists(archive), "archive file exists");
  CHECK(count(w) == 0, "writer is empty after rollover");
  appendN(w, 1501, 10);
  w.flush();
  CHECK(count(w) == 10, "appending after rollover works");
  r.refresh();
  CHECK(count(r) == 10, "reader follows rollover");
  m = w.metrics();
  CHECK(m.rollovers == 1 && m.compactions == 2, "maintenance counters");
  {
    const std::string archive_ticker = std::filesystem::path(archive).stem().string();
    CHECK(archive_ticker == "T.1401-1500", "archive ticker");
    hocdb::Database a(archive_ticker, kDir, schema); // an archive is an ordinary database
    ts = a.getStats(kMin, kMax, "timestamp");
    CHECK(ts.count == 100 && ts.min == 1401.0 && ts.max == 1500.0, "archive content");
    CHECK(a.verify(), "archive checksum");
  }

  // 6. crash simulation: a valid uncommitted tail is adopted, torn bytes dropped
  r.close();
  w.close();
  CHECK(!r.is_valid() && !w.is_valid(), "closed");
  {
    std::ofstream f(file, std::ios::binary | std::ios::app);
    CHECK(f.good(), "open the raw data file");
    const Rec r1{1511, 1}, r2{1512, 2};
    f.write(reinterpret_cast<const char *>(&r1), sizeof r1);
    f.write(reinterpret_cast<const char *>(&r2), sizeof r2);
    const unsigned char torn[5] = {1, 2, 3, 4, 5};
    f.write(reinterpret_cast<const char *>(torn), sizeof torn);
  }
  {
    hocdb::Database w3("T", kDir, schema, cfg);
    CHECK(count(w3) == 12, "tail adopted: 10 + 2");
    m = w3.metrics();
    CHECK(m.recovered_tail_records == 2 && m.dropped_tail_bytes == 5, "recovery counters");
    CHECK(w3.getLatest("value").second == 1512, "adopted records are readable");
    w3.flush();
    CHECK(w3.verify(), "checksum ok after the flush that commits the tail");
  }

  // 6b. checksum: verify_on_open refuses a corrupted file; verify() reports false
  {
    std::fstream f(file, std::ios::binary | std::ios::in | std::ios::out);
    CHECK(f.good(), "reopen the raw data file");
    f.seekp(static_cast<std::streamoff>(64 + 3 * sizeof(Rec) + 8));
    const char b = static_cast<char>(0xFF);
    f.write(&b, 1);
  }
  hocdb::Config vcfg = cfg;
  vcfg.verify_on_open = true;
  EXPECT_THROW_MSG(hocdb::Database("T", kDir, schema, vcfg), "ChecksumMismatch", "verify_on_open refuses a corrupted file");
  {
    hocdb::Database w5("T", kDir, schema, cfg);
    CHECK(count(w5) == 12, "corrupted file is still readable");
    CHECK(!w5.verify(), "verify() reports the mismatch as false");
    m = w5.metrics();
    CHECK(m.crc_failures == 2 && m.committed_records > 0, "crc_failures counted at open and by verify()");
  }

  // 7 (part 2). ring buffer sized 64 + 50 * record_size holds exactly 50 records
  {
    hocdb::Config ring;
    ring.max_file_size = static_cast<int64_t>(hocdb::Database::headerSize() + 50 * sizeof(Rec));
    ring.overwrite_on_full = true;
    hocdb::Database rb("RING", kDir, schema, ring);
    appendN(rb, 1, 80);
    rb.flush();
    ts = rb.getStats(kMin, kMax, "timestamp");
    CHECK(ts.count == 50, "ring buffer holds exactly 50 records after 80 appends");
    CHECK(ts.min == 31.0 && ts.max == 80.0, "ring buffer keeps the newest 50");
    EXPECT_THROW_MSG(rb.verify(), "checksum unavailable", "ring buffers have no checksum");
  }

  // legacy constructor keeps working; Database is movable; bad fsync rejected
  {
    hocdb::Database legacy("OLD", kDir, schema, 0, true, true, false);
    legacy.append(Rec{1, 1.0});
    CHECK(count(legacy) == 1 && legacy.formatVersion() == 2 && !legacy.isReadOnly(),
          "legacy constructor -> current format writer");
    hocdb::Database moved = std::move(legacy);
    CHECK(!legacy.is_valid() && moved.is_valid() && count(moved) == 1, "move");
    hocdb::Config bad;
    bad.fsync = static_cast<hocdb::FsyncPolicy>(7);
    EXPECT_THROW_MSG(hocdb::Database("BAD", kDir, schema, bad), "fsync", "invalid fsync policy rejected");
  }
  EXPECT_THROW_MSG(hocdb::Database::openReader("MISSING", kDir, schema), "reader", "reader of a missing file fails");

  return 0;
}

int main() {
  std::filesystem::remove_all(kDir);
  std::filesystem::create_directories(kDir);
  int rc = 1;
  try {
    rc = run();
  } catch (const std::exception &e) {
    std::cerr << "FAIL: exception: " << e.what() << "\n";
    rc = 1;
  }
  std::filesystem::remove_all(kDir);
  if (rc == 0) {
    std::cout << "C++ storage API test passed\n";
  }
  return rc;
}
