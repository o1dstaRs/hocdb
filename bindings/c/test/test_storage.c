// C API test for durability, lock-free readers, maintenance and metrics.
#include "hocdb.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(cond, msg)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      printf("FAIL: %s (line %d)\n", msg, __LINE__);                          \
      return 1;                                                                \
    }                                                                          \
  } while (0)

typedef struct { int64_t ts; double value; } Rec;

static int append_n(HOCDBHandle db, int64_t from, int n) {
  for (int i = 0; i < n; i++) {
    Rec r = {from + i, (double)(from + i)};
    if (hocdb_append(db, &r, sizeof r) != 0) return 0;
  }
  return 1;
}

int main(void) {
  const char *dir = "b_c_test_storage";
  char cmd[256];
  snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
  system(cmd);
  CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"value", HOCDB_TYPE_F64}};
  CHECK(hocdb_header_size() == 64, "header size");

  HOCDBConfig cfg = {0};
  cfg.fsync_policy = HOCDB_FSYNC_ON_FLUSH;
  cfg.timestamp_unit_ns = 1000000000ULL; // seconds
  cfg.auto_migrate = 1;
  HOCDBHandle w = hocdb_init_ex("T", dir, schema, 2, &cfg);
  CHECK(w != NULL, "init_ex");
  CHECK(hocdb_format_version(w) == 2 && hocdb_is_read_only(w) == 0, "writer format/version");
  CHECK(append_n(w, 1, 1000), "append");
  CHECK(hocdb_flush(w) == 0, "flush");
  CHECK(hocdb_verify(w) == 1, "checksum ok");

  // second writer refused
  HOCDBHandle w2 = hocdb_init_ex("T", dir, schema, 2, &cfg);
  CHECK(w2 == NULL && strcmp(hocdb_last_error(), "DatabaseLocked") == 0, "second writer -> DatabaseLocked");

  // reader
  HOCDBHandle r = hocdb_open_reader("T", dir, schema, 2);
  CHECK(r != NULL, "open_reader");
  CHECK(hocdb_is_read_only(r) == 1, "reader flag");
  HOCDBStats st;
  CHECK(hocdb_get_stats(r, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 1000, "reader sees 1000");
  CHECK(append_n(w, 1001, 500), "append more");
  CHECK(hocdb_get_stats(r, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 1000, "uncommitted invisible");
  CHECK(hocdb_flush(w) == 0, "flush 2");
  CHECK(hocdb_refresh(r) == 0, "refresh");
  CHECK(hocdb_get_stats(r, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 1500, "reader sees 1500");
  Rec bad = {9999, 0};
  CHECK(hocdb_append(r, &bad, sizeof bad) == -10, "reader append -> -10");
  CHECK(hocdb_sync(r) == -10 && hocdb_compact(r, 0) == -10, "reader maintenance -> -10");
  double val; int64_t ts;
  CHECK(hocdb_get_latest(r, 1, &val, &ts) == 0 && ts == 1500, "reader latest (auto refresh)");

  // metrics
  HOCDBMetrics m;
  CHECK(hocdb_metrics_size() == sizeof(HOCDBMetrics), "metrics struct size");
  CHECK(hocdb_metrics_field_count() == 30, "metrics field count");
  CHECK(strcmp(hocdb_metrics_field_name(0), "appends") == 0 && hocdb_metrics_field_type(0) == 3, "metrics introspection");
  CHECK(hocdb_metrics(w, &m) == 0, "metrics");
  CHECK(m.appends == 1500 && m.flushes == 2 && m.fsyncs == 2 && m.commits == 2 && m.bytes_written == 1500 * sizeof(Rec), "writer counters");
  CHECK(m.last_record_ts == 1500 && m.committed_records == 1500 && m.format_version == 2 && m.read_only == 0, "writer state");
  CHECK(m.ingest_lag_wall_ns >= 0 && m.ingest_lag_record_ns != 0, "lag fields");
  CHECK(hocdb_metrics(r, &m) == 0 && m.read_only == 1 && m.reads >= 3 && m.read_ns_p50 > 0 && m.refreshes >= 1, "reader counters");
  hocdb_metrics_reset(w);
  CHECK(hocdb_metrics(w, &m) == 0 && m.appends == 0 && m.last_record_ts == 1500, "metrics reset keeps state");

  // compaction: reader follows the rewritten file
  CHECK(hocdb_compact(w, 1001) == 0, "compact");
  CHECK(hocdb_get_stats(w, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 500 && st.min == 1001.0, "compacted");
  CHECK(hocdb_verify(w) == 1, "checksum after compaction");
  CHECK(hocdb_refresh(r) == 0 && hocdb_get_stats(r, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 500, "reader follows compaction");
  CHECK(hocdb_retain_last(w, 100) == 0, "retain_last");
  CHECK(hocdb_get_stats(r, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 100, "reader follows retain");

  // rollover
  char archive[512];
  CHECK(hocdb_rollover(w, archive, sizeof archive) == 0, "rollover");
  CHECK(strstr(archive, "T.1401-1500.bin") != NULL, "archive name");
  CHECK(hocdb_get_stats(w, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 0, "empty after rollover");
  CHECK(append_n(w, 1501, 10) && hocdb_flush(w) == 0, "append after rollover");
  CHECK(hocdb_get_stats(r, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 10, "reader follows rollover");
  CHECK(hocdb_metrics(w, &m) == 0 && m.rollovers == 1 && m.compactions == 2, "maintenance counters");
  HOCDBHandle a = hocdb_init("T.1401-1500", dir, schema, 2, 0, 0, 0, 0);
  CHECK(a != NULL, "archive opens");
  CHECK(hocdb_get_stats(a, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 100, "archive content");
  hocdb_close(a);

  hocdb_close(r);
  hocdb_close(w);

  // crash simulation: uncommitted valid tail is adopted, torn bytes dropped
  {
    char path[256];
    snprintf(path, sizeof path, "%s/T.bin", dir);
    FILE *f = fopen(path, "r+b");
    CHECK(f != NULL, "open file");
    fseek(f, 0, SEEK_END);
    Rec r1 = {1511, 1}, r2 = {1512, 2};
    fwrite(&r1, sizeof r1, 1, f);
    fwrite(&r2, sizeof r2, 1, f);
    unsigned char torn[5] = {1, 2, 3, 4, 5};
    fwrite(torn, 1, 5, f);
    fclose(f);
  }
  HOCDBHandle w3 = hocdb_init_ex("T", dir, schema, 2, &cfg);
  CHECK(w3 != NULL, "reopen after crash");
  CHECK(hocdb_get_stats(w3, INT64_MIN, INT64_MAX, 1, 0, &st) == 0 && st.count == 12, "tail adopted");
  CHECK(hocdb_metrics(w3, &m) == 0 && m.recovered_tail_records == 2 && m.dropped_tail_bytes == 5, "recovery counters");
  hocdb_close(w3);

  // verify_on_open refuses a corrupted file
  {
    char path[256];
    snprintf(path, sizeof path, "%s/T.bin", dir);
    FILE *f = fopen(path, "r+b");
    fseek(f, 64 + 3 * sizeof(Rec) + 8, SEEK_SET);
    unsigned char b = 0xFF;
    fwrite(&b, 1, 1, f);
    fclose(f);
  }
  HOCDBConfig vcfg = cfg;
  vcfg.verify_on_open = 1;
  HOCDBHandle w4 = hocdb_init_ex("T", dir, schema, 2, &vcfg);
  CHECK(w4 == NULL && strcmp(hocdb_last_error(), "ChecksumMismatch") == 0, "verify_on_open -> ChecksumMismatch");
  HOCDBHandle w5 = hocdb_init_ex("T", dir, schema, 2, &cfg);
  CHECK(w5 != NULL && hocdb_verify(w5) == 0, "corrupted data: verify reports the mismatch, data stays readable");
  CHECK(hocdb_metrics(w5, &m) == 0 && m.crc_failures == 2 && m.committed_records > 0, "crc_failures counted at open and by verify");
  hocdb_close(w5);

  system(cmd);
  printf("C storage API test passed\n");
  return 0;
}
