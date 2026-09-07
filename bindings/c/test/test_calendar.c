// C ABI test for trading calendars: session lookups, custom calendars, and a
// database with a calendar (calendar session kinds, trading-time health,
// automatic periods_per_year, header persistence).
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../hocdb.h"

static int failures = 0;
#define CHECK(cond, msg) do { if (!(cond)) { printf("  FAIL: %s\n", msg); failures++; } } while (0)

typedef struct { int64_t timestamp; double open, high, low, close, volume; } Bar;
#define US 1000000LL

static int64_t utc(int64_t y, unsigned m, unsigned d, unsigned h, unsigned mi) {
  return hocdb_days_from_civil(y, m, d) * 86400 + (int64_t)h * 3600 + (int64_t)mi * 60;
}

int main(void) {
  // --- built-in calendars ---------------------------------------------------
  CHECK(hocdb_calendar_id("nyse") == HOCDB_CALENDAR_NYSE && hocdb_calendar_id("LSE") == HOCDB_CALENDAR_LSE, "calendar ids");
  CHECK(hocdb_calendar_id("nope") == 0, "unknown name");
  char name[16];
  CHECK(hocdb_calendar_name(HOCDB_CALENDAR_CRYPTO, name, sizeof name) == 6 && strcmp(name, "crypto") == 0, "calendar name");
  CHECK(hocdb_calendar_name(99, name, sizeof name) == 0, "unknown id name");
  HOCDBSession s;
  int64_t fri = utc(2025, 9, 5, 15, 0); // Friday 11:00 New York, open
  CHECK(hocdb_calendar_is_open(HOCDB_CALENDAR_NYSE, fri) == 1, "nyse open Friday 11:00");
  CHECK(hocdb_calendar_session(HOCDB_CALENDAR_NYSE, fri, 0, &s) == 1 && s.open == utc(2025, 9, 5, 13, 30) && s.close == utc(2025, 9, 5, 20, 0) && s.early_close == 0, "nyse session Friday");
  int64_t sat = utc(2025, 9, 6, 12, 0);
  CHECK(hocdb_calendar_session(HOCDB_CALENDAR_NYSE, sat, 0, &s) == 0, "closed Saturday");
  CHECK(hocdb_calendar_session(HOCDB_CALENDAR_NYSE, sat, 1, &s) == 1 && s.trade_day == hocdb_days_from_civil(2025, 9, 5), "previous session Friday");
  CHECK(hocdb_calendar_session(HOCDB_CALENDAR_NYSE, sat, 2, &s) == 1 && s.trade_day == hocdb_days_from_civil(2025, 9, 8), "next session Monday");
  CHECK(hocdb_calendar_session_for_day(HOCDB_CALENDAR_NYSE, hocdb_days_from_civil(2025, 7, 4), &s) == 0, "Independence Day closed");
  CHECK(hocdb_calendar_session_for_day(HOCDB_CALENDAR_NYSE, hocdb_days_from_civil(2025, 11, 28), &s) == 1 && s.early_close == 1 && s.close == utc(2025, 11, 28, 18, 0), "Black Friday early close");
  CHECK(hocdb_calendar_session(99, fri, 0, &s) == -31, "unknown id -> -31");
  CHECK(hocdb_calendar_open_seconds(HOCDB_CALENDAR_NYSE, utc(2025, 8, 29, 15, 0), utc(2025, 9, 2, 15, 0)) == 5 * 3600 + 5400, "open seconds over Labor Day weekend");
  CHECK(hocdb_calendar_sessions_between(HOCDB_CALENDAR_NYSE, utc(2025, 1, 1, 0, 0), utc(2026, 1, 1, 0, 0)) == 250, "250 NYSE sessions in 2025");
  CHECK(fabs(hocdb_calendar_periods_per_year(HOCDB_CALENDAR_NYSE, 60) - 252.0 * 390) < 1e-9, "ppy 1-minute nyse");
  CHECK(fabs(hocdb_calendar_periods_per_year(HOCDB_CALENDAR_CRYPTO, 86400) - 365.0) < 1e-9, "ppy daily crypto");
  CHECK(hocdb_calendar_to_local(HOCDB_CALENDAR_NYSE, utc(2025, 9, 5, 15, 0)) == utc(2025, 9, 5, 11, 0), "to_local EDT");
  int64_t y; unsigned m, d;
  hocdb_civil_from_days(hocdb_days_from_civil(2024, 2, 29), &y, &m, &d);
  CHECK(y == 2024 && m == 2 && d == 29, "civil round trip");
  CHECK(hocdb_calendar_session(HOCDB_CALENDAR_FX, utc(2025, 9, 7, 22, 0), 0, &s) == 1 && s.open == utc(2025, 9, 7, 21, 0), "fx Sunday evening open");

  // --- custom calendar --------------------------------------------------------
  HOCDBDaySession weekly[7] = {{36000, 54000}, {36000, 54000}, {36000, 54000}, {36000, 54000}, {0, 0}, {0, 0}, {0, 0}};
  int32_t hol[1] = {(int32_t)hocdb_days_from_civil(2025, 9, 9)};
  HOCDBEarlyClose early[1] = {{(int32_t)hocdb_days_from_civil(2025, 9, 10), 43200}};
  int64_t cid = hocdb_calendar_define("c_custom", weekly, 9 * 3600, HOCDB_DST_NONE, hol, 1, early, 1, 200);
  CHECK(cid >= 32, "custom id");
  CHECK(hocdb_calendar_id("c_custom") == (uint32_t)cid, "custom id by name");
  CHECK(hocdb_calendar_session_for_day((uint32_t)cid, hocdb_days_from_civil(2025, 9, 8), &s) == 1 && s.open == utc(2025, 9, 8, 1, 0), "custom Monday 10:00 UTC+9");
  CHECK(hocdb_calendar_session_for_day((uint32_t)cid, hocdb_days_from_civil(2025, 9, 9), &s) == 0, "custom holiday");
  CHECK(hocdb_calendar_session_for_day((uint32_t)cid, hocdb_days_from_civil(2025, 9, 10), &s) == 1 && s.close == utc(2025, 9, 10, 3, 0), "custom early close");
  CHECK(hocdb_calendar_session_for_day((uint32_t)cid, hocdb_days_from_civil(2025, 9, 12), &s) == 0, "custom no Friday");
  CHECK(hocdb_calendar_define("", weekly, 0, 0, NULL, 0, NULL, 0, 200) == 0, "invalid custom");

  // --- database with a calendar ---------------------------------------------------
  const char *dir = "b_c_test_calendar";
  system("rm -rf b_c_test_calendar");
  CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"open", HOCDB_TYPE_F64}, {"high", HOCDB_TYPE_F64}, {"low", HOCDB_TYPE_F64}, {"close", HOCDB_TYPE_F64}, {"volume", HOCDB_TYPE_F64}};
  HOCDBConfig cfg;
  memset(&cfg, 0, sizeof cfg);
  cfg.auto_migrate = 1;
  cfg.timestamp_unit_ns = 1000;
  cfg.calendar = HOCDB_CALENDAR_NYSE;
  CHECK(sizeof(HOCDBConfig) == 80, "config size 80");
  HOCDBConfig bad = cfg;
  bad.calendar = 999;
  CHECK(hocdb_init_ex("T", dir, schema, 6, &bad) == NULL && strcmp(hocdb_last_error(), "UnknownCalendar") == 0, "unknown calendar refused");
  HOCDBHandle w = hocdb_init_ex("T", dir, schema, 6, &cfg);
  CHECK(w != NULL, "open with calendar");
  CHECK(hocdb_get_calendar(w) == HOCDB_CALENDAR_NYSE && hocdb_get_timestamp_unit(w) == 1000, "calendar + unit on handle");
  CHECK(fabs(hocdb_periods_per_year(w, 60 * US) - 252.0 * 390) < 1e-9, "handle ppy");
  // Thu 2025-09-04, Fri 2025-09-05, Tue 2025-09-09 (Monday missing), one-minute bars
  int64_t days[3] = {hocdb_days_from_civil(2025, 9, 4), hocdb_days_from_civil(2025, 9, 5), hocdb_days_from_civil(2025, 9, 9)};
  double p = 100.0;
  double fri_open = 0, thu_h = -1, thu_l = 1e18, thu_c = 0;
  for (int k = 0; k < 3; k++) {
    HOCDBSession ss;
    hocdb_calendar_session_for_day(HOCDB_CALENDAR_NYSE, days[k], &ss);
    for (int64_t t = ss.open; t < ss.close; t += 60) {
      double o = p;
      p *= 1.0 + 0.001 * sin((double)(t % 977)) ;
      Bar b = {t * US, o, (o > p ? o : p) * 1.001, (o < p ? o : p) * 0.999, p, 500 + (double)(t % 97)};
      if (k == 1 && t == ss.open) fri_open = o;
      if (k == 0) { if (b.high > thu_h) thu_h = b.high; if (b.low < thu_l) thu_l = b.low; thu_c = b.close; }
      CHECK(hocdb_append(w, &b, sizeof b) == 0, "append bar");
    }
  }
  hocdb_flush(w);
  HOCDBIndicatorColumns cols = {1, 2, 3, 4, 5, -1, -1, -1};
  HOCDBIndicatorSpec specs[2];
  memset(specs, 0, sizeof specs);
  specs[0].kind = HOCDB_IND_SESSION_RANGE; specs[0].param = 0; specs[0].field_index = -1; specs[0].field_index2 = -1;
  specs[1].kind = HOCDB_IND_PIVOTS; specs[1].param = 0; specs[1].field_index = -1; specs[1].field_index2 = -1;
  HOCDBSession fs;
  hocdb_calendar_session_for_day(HOCDB_CALENDAR_NYSE, days[1], &fs);
  HOCDBIndicatorResult res;
  int rc = hocdb_indicators(w, (fs.open + 100 * 60) * US, fs.close * US, &cols, specs, 2, 0, 0, &res);
  CHECK(rc == 0 && res.n_rows == 290 && res.n_outputs == 9, "calendar session kinds range");
  if (rc == 0) {
    CHECK(fabs(res.values[0] - fri_open) < 1e-12 && fabs(res.values[289] - fri_open) < 1e-12, "session open = Friday's first bar");
    double pp = (thu_h + thu_l + thu_c) / 3.0;
    CHECK(fabs(res.values[4 * 290] - pp) < 1e-9, "pivot from Thursday");
    hocdb_indicators_free(&res);
  }
  HOCDBHealth h;
  CHECK(hocdb_health(w, 0, INT64_MAX, 4, 5, 5 * 60 * US, 0.2, &h) == 0, "health");
  CHECK(h.count == 3 * 390 && h.n_session_breaks == 2 && h.n_missing_sessions == 1 && h.n_gaps == 1 && h.max_gap == (60 + 390 * 60) * US && h.closed_span > 0, "trading-time health");
  CHECK(hocdb_health_field_count() == 19 && strcmp(hocdb_health_field_name(18), "n_missing_sessions") == 0, "health introspection 19 fields");
  HOCDBSummary sm0, sm1;
  CHECK(hocdb_summary(w, 0, INT64_MAX, 4, 0, &sm0) == 0 && hocdb_summary(w, 0, INT64_MAX, 4, 252.0 * 390, &sm1) == 0, "summary");
  CHECK(fabs(sm0.ann_vol - sm1.ann_vol) < 1e-12 && sm0.ann_vol > 0, "auto periods_per_year in summary");
  // a session kind without a calendar is refused with -30
  HOCDBConfig plain;
  memset(&plain, 0, sizeof plain);
  plain.auto_migrate = 1;
  HOCDBHandle w2 = hocdb_init_ex("P", dir, schema, 6, &plain);
  Bar one = {1, 1, 1, 1, 1, 1};
  hocdb_append(w2, &one, sizeof one);
  hocdb_flush(w2);
  CHECK(hocdb_indicators_tail(w2, 1, &cols, specs, 1, 0, 0, &res) == -30, "CalendarRequired -> -30");
  CHECK(hocdb_set_calendar(w2, 999) == -31 && hocdb_set_calendar(w2, HOCDB_CALENDAR_CRYPTO) == 0 && hocdb_get_calendar(w2) == HOCDB_CALENDAR_CRYPTO, "set_calendar");
  CHECK(hocdb_set_timestamp_unit(w2, 1000000000ULL) == 0 && hocdb_get_timestamp_unit(w2) == 1000000000ULL, "set_timestamp_unit");
  CHECK(fabs(hocdb_periods_per_year(w2, 86400) - 365.0) < 1e-9, "ppy after set (seconds unit, daily)");
  hocdb_close(w2);
  hocdb_close(w);
  // persistence: reopen without config, and a reader
  HOCDBHandle w3 = hocdb_init_ex("T", dir, schema, 6, &plain);
  CHECK(w3 != NULL && hocdb_get_calendar(w3) == HOCDB_CALENDAR_NYSE && hocdb_get_timestamp_unit(w3) == 1000, "calendar + unit persisted");
  HOCDBHandle r = hocdb_open_reader("T", dir, schema, 6);
  CHECK(r != NULL && hocdb_get_calendar(r) == HOCDB_CALENDAR_NYSE && fabs(hocdb_periods_per_year(r, 60 * US) - 252.0 * 390) < 1e-9, "reader sees the calendar");
  HOCDBHandle p2 = hocdb_init_ex("P", dir, schema, 6, &plain);
  CHECK(p2 != NULL && hocdb_get_calendar(p2) == HOCDB_CALENDAR_CRYPTO && hocdb_get_timestamp_unit(p2) == 1000000000ULL, "set_calendar/unit persisted");
  hocdb_close(p2);
  hocdb_close(r);
  hocdb_close(w3);
  system("rm -rf b_c_test_calendar");
  if (failures) { printf("C calendar API test FAILED (%d)\n", failures); return 1; }
  printf("C calendar API test passed\n");
  return 0;
}
