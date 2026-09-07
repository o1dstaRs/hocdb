// C cross-binding consistency program (see README.md). Minimal JSON parsing
// for the request: the harness writes a flat, predictable document.
#include "hocdb.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *slurp(const char *path) {
  FILE *f = fopen(path, "rb");
  if (!f) return NULL;
  fseek(f, 0, SEEK_END);
  long n = ftell(f);
  fseek(f, 0, SEEK_SET);
  char *buf = malloc(n + 1);
  fread(buf, 1, n, f);
  buf[n] = 0;
  fclose(f);
  return buf;
}

static const char *find_key(const char *json, const char *key) {
  char pat[128];
  snprintf(pat, sizeof pat, "\"%s\"", key);
  const char *p = strstr(json, pat);
  if (!p) return NULL;
  p = strchr(p + strlen(pat), ':');
  return p ? p + 1 : NULL;
}

static long long get_int(const char *json, const char *key) {
  const char *p = find_key(json, key);
  return p ? strtoll(p, NULL, 10) : 0;
}

static double get_num(const char *json, const char *key) {
  const char *p = find_key(json, key);
  return p ? strtod(p, NULL) : 0;
}

static void get_str(const char *json, const char *key, char *out, size_t cap) {
  const char *p = find_key(json, key);
  out[0] = 0;
  if (!p) return;
  p = strchr(p, '"');
  if (!p) return;
  p++;
  const char *e = strchr(p, '"');
  size_t n = (size_t)(e - p);
  if (n >= cap) n = cap - 1;
  memcpy(out, p, n);
  out[n] = 0;
}

static void emit_num(double x) {
  if (isnan(x)) printf("null");
  else if (isinf(x)) printf(x > 0 ? "\"inf\"" : "\"-inf\"");
  else printf("%.17g", x);
}

int main(int argc, char **argv) {
  if (argc < 2) return 2;
  char *json = slurp(argv[1]);
  if (!json) return 2;
  char ticker[64], dir[1024];
  get_str(json, "ticker", ticker, sizeof ticker);
  get_str(json, "dir", dir, sizeof dir);
  long long tail = get_int(json, "tail");
  long long bucket = get_int(json, "bucket");

  CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64}, {"size", HOCDB_TYPE_F64},
                     {"bid", HOCDB_TYPE_F64},       {"ask", HOCDB_TYPE_F64},   {"side", HOCDB_TYPE_BOOL}};
  HOCDBHandle db = hocdb_init(ticker, dir, schema, 6, 0, 0, 0, 0);
  if (!db) { fprintf(stderr, "init failed\n"); return 1; }
  HOCDBIndicatorColumns cols = {-1, -1, -1, 1, 2, -1, -1, -1};

  // specs: parse the "specs" array of objects
  const char *sp = find_key(json, "specs");
  HOCDBIndicatorSpec specs[256];
  char labels[256][64];
  size_t n_specs = 0;
  const char *p = strchr(sp, '[') + 1;
  while (n_specs < 256) {
    const char *obj = strchr(p, '{');
    const char *arr_end = strchr(p, ']');
    if (!obj || (arr_end && arr_end < obj)) break;
    const char *end = strchr(obj, '}');
    size_t len = (size_t)(end - obj + 1);
    char tmp[512];
    if (len >= sizeof tmp) len = sizeof tmp - 1;
    memcpy(tmp, obj, len);
    tmp[len] = 0;
    char kind[64];
    get_str(tmp, "kind", kind, sizeof kind);
    HOCDBIndicatorSpec s = {0};
    s.kind = hocdb_indicator_kind_from_name(kind);
    s.period = (uint32_t)get_int(tmp, "period");
    s.period2 = (uint32_t)get_int(tmp, "period2");
    s.period3 = (uint32_t)get_int(tmp, "period3");
    s.period4 = (uint32_t)get_int(tmp, "period4");
    s.param = get_num(tmp, "param");
    s.param2 = get_num(tmp, "param2");
    s.field_index = -1;
    s.field_index2 = -1;
    specs[n_specs] = s;
    // naming rule: label = kind[_period]; outputs appended unless named like the kind
    if (s.period) snprintf(labels[n_specs], 64, "%s_%u", kind, s.period);
    else snprintf(labels[n_specs], 64, "%s", kind);
    n_specs++;
    p = end + 1;
  }

  HOCDBIndicatorResult res;
  int rc = hocdb_indicators_tail(db, (size_t)tail, &cols, specs, n_specs, HOCDB_LOOKBACK_AUTO, bucket, &res);
  if (rc != 0) { fprintf(stderr, "indicators_tail failed %d\n", rc); return 1; }
  printf("{\"binding\":\"c\",\"timestamps\":[");
  for (size_t i = 0; i < res.n_rows; i++) printf("%s%lld", i ? "," : "", (long long)res.timestamps[i]);
  printf("],\"columns\":{");
  size_t k = 0;
  int first = 1;
  for (size_t si = 0; si < n_specs; si++) {
    size_t cnt = hocdb_indicator_output_count(specs[si].kind);
    const char *kname = hocdb_indicator_name(specs[si].kind);
    for (size_t j = 0; j < cnt; j++, k++) {
      const char *oname = hocdb_indicator_output_name(specs[si].kind, j);
      char col[160];
      if (cnt == 1 || strcmp(oname, kname) == 0) snprintf(col, sizeof col, "%s", labels[si]);
      else snprintf(col, sizeof col, "%s_%s", labels[si], oname);
      printf("%s\"%s\":[", first ? "" : ",", col);
      first = 0;
      for (size_t i = 0; i < res.n_rows; i++) {
        if (i) printf(",");
        emit_num(res.values[k * res.n_rows + i]);
      }
      printf("]");
    }
  }
  printf("}");
  hocdb_indicators_free(&res);

  // snapshot
  const char *sn = find_key(json, "snapshot");
  HOCDBSnapshot snap;
  rc = hocdb_snapshot(db, &cols, (size_t)get_int(sn, "bars"), get_int(sn, "bucket"), get_num(sn, "periods_per_year"), &snap);
  if (rc != 0) { fprintf(stderr, "snapshot failed %d\n", rc); return 1; }
  printf(",\"snapshot\":{");
  size_t nf = hocdb_snapshot_field_count();
  for (size_t i = 0; i < nf; i++) {
    const char *name = hocdb_snapshot_field_name(i);
    size_t off = hocdb_snapshot_field_offset(i);
    int t = hocdb_snapshot_field_type(i);
    printf("%s\"%s\":", i ? "," : "", name);
    const unsigned char *base = (const unsigned char *)&snap;
    if (t == 1) { int64_t v; memcpy(&v, base + off, 8); printf("%lld", (long long)v); }
    else if (t == 3) { uint64_t v; memcpy(&v, base + off, 8); printf("%llu", (unsigned long long)v); }
    else { double v; memcpy(&v, base + off, 8); emit_num(v); }
  }
  printf("}");

  // summary
  const char *su = find_key(json, "summary");
  HOCDBSummary sum;
  char field[64];
  get_str(su, "field", field, sizeof field);
  int64_t fidx = hocdb_get_field_index(db, field);
  rc = hocdb_summary(db, get_int(su, "start"), get_int(su, "end"), (size_t)fidx, get_num(su, "periods_per_year"), &sum);
  if (rc != 0) { fprintf(stderr, "summary failed %d\n", rc); return 1; }
  printf(",\"summary\":{");
  nf = hocdb_summary_field_count();
  for (size_t i = 0; i < nf; i++) {
    const char *name = hocdb_summary_field_name(i);
    size_t off = hocdb_summary_field_offset(i);
    int t = hocdb_summary_field_type(i);
    printf("%s\"%s\":", i ? "," : "", name);
    const unsigned char *base = (const unsigned char *)&sum;
    if (t == 1) { int64_t v; memcpy(&v, base + off, 8); printf("%lld", (long long)v); }
    else if (t == 3) { uint64_t v; memcpy(&v, base + off, 8); printf("%llu", (unsigned long long)v); }
    else { double v; memcpy(&v, base + off, 8); emit_num(v); }
  }
  printf("}");

  // ohlcv
  const char *oh = find_key(json, "ohlcv");
  HOCDBBars bars;
  rc = hocdb_ohlcv(db, get_int(oh, "start"), get_int(oh, "end"), 1, 2, get_int(oh, "bucket"), &bars);
  if (rc != 0) { fprintf(stderr, "ohlcv failed %d\n", rc); return 1; }
  printf(",\"ohlcv\":{\"timestamps\":[");
  for (size_t i = 0; i < bars.n_bars; i++) printf("%s%lld", i ? "," : "", (long long)bars.timestamps[i]);
  const char *names[] = {"open", "high", "low", "close", "volume", "count"};
  double *arrs[] = {bars.open, bars.high, bars.low, bars.close, bars.volume, bars.count};
  for (int a = 0; a < 6; a++) {
    printf("],\"%s\":[", names[a]);
    for (size_t i = 0; i < bars.n_bars; i++) { if (i) printf(","); emit_num(arrs[a][i]); }
  }
  printf("]}}\n");
  hocdb_ohlcv_free(&bars);
  hocdb_close(db);
  free(json);
  return 0;
}
