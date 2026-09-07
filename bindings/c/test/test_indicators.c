// C API test for the indicator / analytics functions.
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

typedef struct {
  int64_t timestamp;
  double open, high, low, close, volume;
} Bar;

static double lcg(uint64_t *s) {
  *s = *s * 6364136223846793005ULL + 1442695040888963407ULL;
  return (double)(*s >> 11) / 9007199254740992.0;
}

int main(void) {
  const char *dir = "b_c_test_indicators";
  CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"open", HOCDB_TYPE_F64},
                     {"high", HOCDB_TYPE_F64},      {"low", HOCDB_TYPE_F64},
                     {"close", HOCDB_TYPE_F64},     {"volume", HOCDB_TYPE_F64}};
  char cmd[256];
  snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
  system(cmd);
  HOCDBHandle db = hocdb_init("IND", dir, schema, 6, 0, 0, 0, 0);
  CHECK(db != NULL, "init");

  const int N = 3000;
  uint64_t seed = 7;
  double p = 100.0;
  for (int i = 0; i < N; i++) {
    double o = p;
    p *= exp((lcg(&seed) - 0.5) * 0.02);
    Bar b = {1000 + (int64_t)i * 60, o, fmax(o, p) * 1.003, fmin(o, p) * 0.997, p, 1000.0 + (i % 50)};
    CHECK(hocdb_append(db, &b, sizeof b) == 0, "append");
  }
  hocdb_flush(db);

  HOCDBIndicatorColumns cols = {1, 2, 3, 4, 5, -1, -1, -1};

  // --- registry ---
  CHECK(hocdb_indicator_kind_from_name("rsi") == HOCDB_IND_RSI, "kind from name");
  CHECK(hocdb_indicator_kind_from_name("MACD") == HOCDB_IND_MACD, "kind from name is case-insensitive");
  CHECK(hocdb_indicator_kind_from_name("nope") == 0, "unknown kind");
  CHECK(hocdb_indicator_output_count(HOCDB_IND_MACD) == 3, "macd outputs");
  CHECK(strcmp(hocdb_indicator_output_name(HOCDB_IND_MACD, 1), "signal") == 0, "macd output name");
  CHECK(strcmp(hocdb_indicator_name(HOCDB_IND_BBANDS), "bbands") == 0, "kind name");
  CHECK(hocdb_indicator_output_name(HOCDB_IND_MACD, 3) == NULL, "out of range output name");
  uint32_t kinds[128];
  size_t nk = hocdb_indicator_kinds(kinds, 128);
  CHECK(nk == 83, "number of kinds");
  for (size_t i = 0; i < nk; i++) {
    CHECK(hocdb_indicator_name(kinds[i]) != NULL, "every kind has a name");
    CHECK(hocdb_indicator_output_count(kinds[i]) >= 1, "every kind has outputs");
  }
  HOCDBIndicatorSpec ema200 = {HOCDB_IND_EMA, 200, 0, 0, 0, 0, 0, -1, -1};
  CHECK(hocdb_indicator_warmup(&ema200) > 200, "warmup > period for EMA");

  // --- batch over a range ---
  HOCDBIndicatorSpec specs[] = {
      {HOCDB_IND_SMA, 20, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_MACD, 0, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_RSI, 14, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_BBANDS, 20, 0, 0, 0, 2.0, 0, -1, -1},
      {HOCDB_IND_ATR, 14, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_OBV, 0, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_SMA, 10, 0, 0, 0, 0, 0, 5, -1}, // SMA of volume
  };
  size_t n_specs = sizeof specs / sizeof specs[0];
  HOCDBIndicatorResult res;
  int64_t start_ts = 1000 + 1000 * 60, end_ts = 1000 + 1500 * 60;
  int rc = hocdb_indicators(db, start_ts, end_ts, &cols, specs, n_specs, HOCDB_LOOKBACK_AUTO, 0, &res);
  CHECK(rc == 0, "hocdb_indicators");
  CHECK(res.n_rows == 500, "500 rows");
  CHECK(res.n_outputs == 1 + 3 + 1 + 5 + 1 + 1 + 1, "13 outputs");
  CHECK(res.timestamps[0] == start_ts, "first ts");
  CHECK(res.timestamps[499] == end_ts - 60, "last ts");
  for (size_t i = 0; i < res.n_rows; i++) {
    CHECK(!isnan(res.values[i]), "sma converged (auto lookback)");
    CHECK(res.values[4 * res.n_rows + i] >= 0 && res.values[4 * res.n_rows + i] <= 100, "rsi in [0,100]");
    double up = res.values[5 * res.n_rows + i], mid = res.values[6 * res.n_rows + i], lo = res.values[7 * res.n_rows + i];
    CHECK(up >= mid && mid >= lo, "bbands ordered");
    CHECK(res.values[9 * res.n_rows + i] > 0, "atr positive");
  }
  // explicit lookback 0 -> NaN warm-up inside the window
  HOCDBIndicatorResult res0;
  rc = hocdb_indicators(db, start_ts, end_ts, &cols, specs, 1, 0, 0, &res0);
  CHECK(rc == 0 && res0.n_rows == 500, "lookback 0");
  CHECK(isnan(res0.values[0]) && isnan(res0.values[18]) && !isnan(res0.values[19]), "NaN warm-up with lookback 0");
  hocdb_indicators_free(&res0);
  hocdb_indicators_free(&res);
  CHECK(res.values == NULL && res.n_rows == 0, "free resets");

  // --- tail ---
  rc = hocdb_indicators_tail(db, 5, &cols, specs, 3, HOCDB_LOOKBACK_AUTO, 0, &res);
  CHECK(rc == 0 && res.n_rows == 5 && res.n_outputs == 5, "tail");
  CHECK(res.timestamps[4] == 1000 + (int64_t)(N - 1) * 60, "tail last ts");
  hocdb_indicators_free(&res);

  // --- bucket (tick -> 5-minute bars) ---
  rc = hocdb_indicators_tail(db, 10, &cols, specs, 1, 0, 300, &res);
  CHECK(rc == 0 && res.n_rows == 10, "tail with bucket");
  CHECK(res.timestamps[1] - res.timestamps[0] == 300, "bar spacing");
  CHECK(res.timestamps[0] % 300 == 0, "bar alignment");
  hocdb_indicators_free(&res);

  // --- errors ---
  HOCDBIndicatorSpec bad = {9999, 0, 0, 0, 0, 0, 0, -1, -1};
  rc = hocdb_indicators_tail(db, 10, &cols, &bad, 1, 0, 0, &res);
  CHECK(rc == -2, "bad kind -> -2");
  HOCDBIndicatorColumns noclose = {-1, -1, -1, -1, -1, -1, -1, -1};
  rc = hocdb_indicators_tail(db, 10, &noclose, specs, 1, 0, 0, &res);
  CHECK(rc == -3, "missing close -> -3");
  HOCDBIndicatorColumns closeonly = {-1, -1, -1, 4, -1, -1, -1, -1};
  rc = hocdb_indicators_tail(db, 10, &closeonly, &specs[4], 1, 0, 0, &res); // ATR needs H/L
  CHECK(rc == -3, "missing high/low -> -3");
  HOCDBIndicatorSpec badfield = {HOCDB_IND_SMA, 5, 0, 0, 0, 0, 0, 42, -1};
  rc = hocdb_indicators_tail(db, 10, &cols, &badfield, 1, 0, 0, &res);
  CHECK(rc == -4, "bad field index -> -4");

  // --- ohlcv ---
  HOCDBBars bars;
  rc = hocdb_ohlcv(db, INT64_MIN, INT64_MAX, 4, 5, 300, &bars);
  CHECK(rc == 0 && bars.n_bars > 500, "ohlcv");
  for (size_t i = 0; i < bars.n_bars; i++) {
    CHECK(bars.high[i] >= bars.low[i], "bar high >= low");
    CHECK(bars.high[i] >= bars.close[i] && bars.low[i] <= bars.close[i], "close within bar");
    CHECK(bars.count[i] >= 1, "bar count");
  }
  hocdb_ohlcv_free(&bars);
  CHECK(bars.n_bars == 0 && bars.close == NULL, "ohlcv free resets");

  // --- summary ---
  HOCDBSummary sum;
  CHECK(hocdb_summary_size() == sizeof(HOCDBSummary), "summary struct size matches header");
  CHECK(hocdb_summary_field_count() == 29, "summary field count");
  CHECK(strcmp(hocdb_summary_field_name(0), "count") == 0, "summary field 0");
  CHECK(strcmp(hocdb_summary_field_name(28), "half_life") == 0, "summary last field");
  CHECK(hocdb_summary_field_offset(1) == offsetof(HOCDBSummary, first), "summary offset");
  CHECK(hocdb_summary_field_type(0) == 3 && hocdb_summary_field_type(1) == 2, "summary field types");
  rc = hocdb_summary(db, INT64_MIN, INT64_MAX, 4, 252.0, &sum);
  CHECK(rc == 0, "summary");
  CHECK(sum.count == (uint64_t)N, "summary count");
  CHECK(sum.max_drawdown <= 0 && sum.max_drawdown >= -1, "max drawdown range");
  CHECK(sum.win_rate >= 0 && sum.win_rate <= 1, "win rate range");
  CHECK(!isnan(sum.sharpe) && !isnan(sum.hurst), "summary fields computed");

  // --- snapshot ---
  HOCDBSnapshot snap;
  CHECK(hocdb_snapshot_size() == sizeof(HOCDBSnapshot), "snapshot struct size matches header");
  CHECK(hocdb_snapshot_field_offset(hocdb_snapshot_field_count() - 1) == offsetof(HOCDBSnapshot, kurtosis_20), "snapshot last field offset");
  CHECK(strcmp(hocdb_snapshot_field_name(2), "open") == 0, "snapshot field name");
  CHECK(hocdb_snapshot_field_type(0) == 1 && hocdb_snapshot_field_type(1) == 3 && hocdb_snapshot_field_type(2) == 2, "snapshot field types");
  rc = hocdb_snapshot(db, &cols, 0, 0, 252.0, &snap);
  CHECK(rc == 0, "snapshot");
  CHECK(snap.bars == 2500, "snapshot bars");
  CHECK(snap.timestamp == 1000 + (int64_t)(N - 1) * 60, "snapshot ts");
  CHECK(!isnan(snap.ema_200) && !isnan(snap.adx_14) && !isnan(snap.mfi_14) && !isnan(snap.supertrend), "snapshot fields");
  CHECK(snap.rsi_14 >= 0 && snap.rsi_14 <= 100, "snapshot rsi");
  CHECK(fabs(snap.close - p) < 1e-9, "snapshot close is latest");
  rc = hocdb_snapshot(db, &cols, 50, 300, 252.0, &snap);
  CHECK(rc == 0 && snap.bars == 50 && isnan(snap.sma_200) && !isnan(snap.sma_20), "snapshot with bucket");

  // ------------------------------------------------------------------
  // Pairs, microstructure, labels, sessions, health, evaluation, multi
  // ------------------------------------------------------------------
  CHECK(sizeof(HOCDBIndicatorColumns) == 64, "columns struct has 8 roles");
  CHECK(hocdb_indicator_kind_from_name("order_flow") == HOCDB_IND_ORDER_FLOW, "new kind names resolve");
  CHECK(hocdb_indicator_is_lookahead(HOCDB_IND_FORWARD_RETURN) == 1 && hocdb_indicator_is_lookahead(HOCDB_IND_SMA) == 0, "lookahead flag");
  CHECK(hocdb_indicator_output_count(HOCDB_IND_PIVOTS) == 5, "pivots outputs");
  CHECK(hocdb_indicator_kinds(NULL, 0) == 83, "83 kinds");

  // a tick database with quotes and sides
  const char *tdir = "b_c_test_indicators_ticks";
  snprintf(cmd, sizeof cmd, "rm -rf %s", tdir);
  system(cmd);
  CField tschema[] = {{"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64}, {"size", HOCDB_TYPE_F64},
                      {"bid", HOCDB_TYPE_F64},       {"ask", HOCDB_TYPE_F64},   {"side", HOCDB_TYPE_BOOL}};
  typedef struct __attribute__((packed)) { int64_t ts; double price, size, bid, ask; uint8_t side; } Tick;
  CHECK(sizeof(Tick) == 41, "tick record size");
  HOCDBHandle ta = hocdb_init("PAIR_A", tdir, tschema, 6, 0, 0, 0, 0);
  HOCDBHandle tb = hocdb_init("PAIR_B", tdir, tschema, 6, 0, 0, 0, 0);
  CHECK(ta && tb, "tick dbs");
  double pa = 100.0, pb = 50.0;
  for (int i = 0; i < 6000; i++) {
    pa *= exp((lcg(&seed) - 0.5) * 0.004);
    pb *= exp((lcg(&seed) - 0.5) * 0.004);
    Tick x = {1000000LL * i, pa, 1.0 + i % 4, pa * 0.999, pa * 1.001, (uint8_t)(i % 3 != 0)};
    CHECK(hocdb_append(ta, &x, sizeof x) == 0, "append A");
    if (i % 2 == 0) {  // B trades every 2 seconds
      Tick y = {1000000LL * i + 300000, pb, 2.0, pb * 0.999, pb * 1.001, (uint8_t)(i % 2)};
      CHECK(hocdb_append(tb, &y, sizeof y) == 0, "append B");
    }
  }
  hocdb_flush(ta);
  hocdb_flush(tb);
  HOCDBIndicatorColumns tcols = {-1, -1, -1, 1, 2, 3, 4, 5};

  HOCDBIndicatorSpec micro[] = {
      {HOCDB_IND_SPREAD, 0, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_ORDER_FLOW, 10, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_TRADE_INTENSITY, 10, 0, 0, 0, 1e6, 0, -1, -1},
      {HOCDB_IND_TICK_PRESSURE, 20, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_SESSION_VWAP, 0, 0, 0, 0, 600e6, 0, -1, -1}, // 10-minute sessions
      {HOCDB_IND_FORWARD_RETURN, 5, 0, 0, 0, 0, 0, -1, -1},
  };
  rc = hocdb_indicators_tail(ta, 100, &tcols, micro, 6, HOCDB_LOOKBACK_AUTO, 0, &res);
  CHECK(rc == 0 && res.n_rows == 100 && res.n_outputs == 2 + 2 + 2 + 1 + 1 + 3, "microstructure batch");
  for (size_t i = 0; i < 100; i++) {
    CHECK(fabs(res.values[1 * res.n_rows + i] - 20.0) < 1e-9, "spread 20 bps");
    CHECK(fabs(res.values[4 * res.n_rows + i] - 1.0) < 1e-9, "1 trade per second");
    double imb = res.values[3 * res.n_rows + i];
    CHECK(imb >= -1 && imb <= 1, "imbalance range");
  }
  CHECK(isnan(res.values[8 * res.n_rows + 99]) && !isnan(res.values[8 * res.n_rows + 90]), "forward return NaN at the end");
  hocdb_indicators_free(&res);
  HOCDBIndicatorSpec sess_bad = {HOCDB_IND_SESSION_VWAP, 0, 0, 0, 0, 0, 0, -1, -1};
  rc = hocdb_indicators_tail(ta, 10, &tcols, &sess_bad, 1, 0, 0, &res);
  CHECK(rc == -30, "session kinds with param 0 need a trading calendar (-30)");

  // pairs: as-of on ticks, inner join on 10-second bars
  HOCDBIndicatorSpec pair[] = {
      {HOCDB_IND_SERIES, 0, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_SERIES2, 0, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_RATIO, 0, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_CORREL, 30, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_BETA, 30, 0, 0, 0, 0, 0, -1, -1},
      {HOCDB_IND_REL_STRENGTH, 10, 0, 0, 0, 0, 0, -1, -1},
  };
  rc = hocdb_pair_indicators_tail(ta, &tcols, tb, &tcols, 50, pair, 6, HOCDB_LOOKBACK_AUTO, 0, &res);
  CHECK(rc == 0 && res.n_rows == 50 && res.n_outputs == 6, "pair tail on ticks");
  for (size_t i = 0; i < 50; i++) {
    double a = res.values[i], b = res.values[res.n_rows + i], r = res.values[2 * res.n_rows + i];
    CHECK(fabs(a / b - r) < 1e-12, "ratio = a / b");
    CHECK(!isnan(res.values[3 * res.n_rows + i]), "correl defined");
  }
  hocdb_indicators_free(&res);
  rc = hocdb_pair_indicators(ta, &tcols, tb, &tcols, 1000000LL * 1000, 1000000LL * 2000, pair, 6, 0, 10000000, &res);
  CHECK(rc == 0 && res.n_rows == 100, "pair range on 10 s bars: 100 bars");
  CHECK(res.timestamps[0] == 1000000LL * 1000 && res.timestamps[1] - res.timestamps[0] == 10000000, "bar alignment");
  hocdb_indicators_free(&res);

  // extended bars with buy volume
  HOCDBBarsEx bx;
  rc = hocdb_ohlcv_ex(ta, INT64_MIN, INT64_MAX, 1, 2, 5, 60000000, &bx);
  CHECK(rc == 0 && bx.n_bars == 100 && bx.buy_volume != NULL, "ohlcv_ex");
  for (size_t i = 0; i < bx.n_bars; i++) CHECK(bx.buy_volume[i] <= bx.volume[i] && bx.buy_volume[i] >= 0, "buy volume within volume");
  hocdb_ohlcv_ex_free(&bx);
  rc = hocdb_ohlcv_ex(ta, INT64_MIN, INT64_MAX, 1, 2, -1, 60000000, &bx);
  CHECK(rc == 0 && bx.buy_volume == NULL, "ohlcv_ex without side");
  hocdb_ohlcv_ex_free(&bx);

  // health
  HOCDBHealth hl;
  CHECK(hocdb_health_size() == sizeof(HOCDBHealth), "health struct size");
  CHECK(hocdb_health_field_count() == 19 && strcmp(hocdb_health_field_name(0), "count") == 0, "health introspection");
  rc = hocdb_health(ta, INT64_MIN, INT64_MAX, 1, 2, 5000000, 0.05, &hl);
  CHECK(rc == 0 && hl.count == 6000 && hl.n_gaps == 0 && hl.median_gap == 1000000.0 && hl.n_outlier_returns == 0, "health values");

  // evaluation
  HOCDBDecision dec[] = {{1000000LL * 100, 1, 1000, 60000000}, {1000000LL * 200, -1, 500, 0}, {1000000LL * 5990, 1, 100, 60000000}, {1000000LL * 300, 0, 1, 0}};
  HOCDBEvaluation ev;
  double ent[4], ex[4], net[4];
  CHECK(hocdb_evaluation_size() == sizeof(HOCDBEvaluation) && hocdb_decision_size() == sizeof(HOCDBDecision), "evaluation struct sizes");
  CHECK(hocdb_evaluation_field_count() == 20, "evaluation field count");
  rc = hocdb_evaluate(ta, 1, dec, 4, 120000000, 5.0, &ev, ent, ex, net);
  CHECK(rc == 0 && ev.n_decisions == 4 && ev.n_evaluated == 2 && ev.n_long == 2 && ev.n_short == 1, "evaluate counts");
  CHECK(!isnan(net[0]) && !isnan(net[1]) && isnan(net[2]) && isnan(net[3]), "per-decision outputs");
  CHECK(fabs(net[0] - (ex[0] / ent[0] - 1.0 - 0.001)) < 1e-12, "net return = gross - 2 x 5 bps");
  rc = hocdb_evaluate(ta, 1, NULL, 0, 1, 0, &ev, NULL, NULL, NULL);
  CHECK(rc == 0 && ev.n_evaluated == 0 && isnan(ev.hit_rate), "empty evaluation");

  // multi-timeframe snapshots
  int64_t buckets[] = {60000000, 300000000};
  double ppys[] = {525600, 105120};
  HOCDBSnapshot multi[2];
  rc = hocdb_snapshot_multi(ta, &tcols, 50, buckets, 2, ppys, multi);
  CHECK(rc == 0 && multi[0].bars == 50 && multi[1].bars == 20, "snapshot_multi (only 100 minutes of data -> 20 five-minute bars)");
  HOCDBSnapshot single;
  hocdb_snapshot(ta, &tcols, 50, 60000000, 525600, &single);
  CHECK(single.rsi_14 == multi[0].rsi_14 && single.timestamp == multi[0].timestamp, "multi equals single");

  hocdb_close(ta);
  hocdb_close(tb);
  system(cmd);

  hocdb_close(db);
  snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
  system(cmd);
  printf("C indicator API test passed\n");
  return 0;
}
