// C ABI test for the signal backtester: tied example on arrays, database range / tail
// windows, walk-forward splits and struct introspection.
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../hocdb.h"

static int failures = 0;
#define CHECK(cond, msg) do { if (!(cond)) { printf("  FAIL: %s\n", msg); failures++; } } while (0)
static int near(double a, double b, double tol) { return fabs(a - b) <= tol; }

typedef struct { int64_t timestamp; double price; double size; } Tick;

int main(void) {
  // --- tied example (same numbers as scripts/stress/references_backtest.py) ---
  const int64_t ts[12] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  const double open[12] = {100.0, 101.0, 102.0, 98.0, 95.0, 97.0, 99.0, 103.0, 104.0, 102.0, 100.0, 99.0};
  const double high[12] = {101.0, 103.0, 103.0, 99.0, 97.0, 99.0, 104.0, 105.0, 105.0, 103.0, 101.0, 100.0};
  const double low[12] = {99.0, 100.0, 96.0, 93.0, 94.0, 96.0, 98.0, 102.0, 101.0, 99.0, 98.0, 97.0};
  const double close[12] = {100.5, 102.5, 97.0, 94.0, 96.5, 98.5, 103.5, 104.5, 102.0, 100.0, 98.5, 99.5};
  const double target[12] = {1, 1, 1, 1, 1, 2, 2, 2, -1, -1, -1, -1};
  HOCDBBacktestParams p;
  hocdb_backtest_params_default(&p);
  CHECK(p.initial_equity == 1.0 && p.allow_short == 1 && p.fill_mode == 0, "params default");
  p.initial_equity = 1000; p.cost_bps = 10; p.slippage_bps = 5; p.stop_loss = 0.05; p.periods_per_year = 252;
  double equity[12], position[12];
  HOCDBBacktestOutputs outs = {equity, position, NULL, NULL, NULL};
  HOCDBTrade trades[8];
  HOCDBBacktestResult r;
  CHECK(hocdb_backtest_arrays(ts, open, high, low, close, 12, target, &p, &outs, trades, 8, &r) == 0, "arrays rc");
  CHECK(r.n_bars == 12 && r.n_trades == 3 && r.n_long_trades == 2 && r.n_short_trades == 1, "tied counts");
  CHECK(near(r.final_equity, 1002.4465295364876, 1e-9) && near(r.max_drawdown, 0.006637024271452185, 1e-12) && r.max_drawdown_bars == 4, "tied equity / drawdown");
  CHECK(near(r.sharpe, 0.961805823133203, 1e-9) && near(r.turnover, 0.7010422825639875, 1e-9) && near(r.total_cost, 0.7009464760125, 1e-9), "tied stats");
  CHECK(r.n_stop_exits == 1 && trades[0].exit_reason == 1 && near(trades[0].exit_price, 95.9499760125, 1e-9) && trades[2].exit_ts == 0 && trades[2].exit_reason == 4, "tied trades");
  CHECK(near(equity[1], 1001.3484495, 1e-9) && position[6] == 2.0 && position[9] == -1.0, "per-bar outputs");
  CHECK(sizeof(HOCDBBacktestParams) == hocdb_backtest_params_size() && sizeof(HOCDBBacktestResult) == hocdb_backtest_result_size() && sizeof(HOCDBTrade) == hocdb_trade_size(), "struct sizes");
  CHECK(hocdb_backtest_result_field_count() == 32 && strcmp(hocdb_backtest_result_field_name(31), "net_pnl") == 0 && hocdb_backtest_result_field_type(31) == 2 && hocdb_backtest_result_field_type(0) == 3, "result introspection");
  CHECK(hocdb_trade_field_count() == 10 && strcmp(hocdb_trade_field_name(9), "exit_reason") == 0 && hocdb_trade_field_offset(3) == 24, "trade introspection");
  CHECK(hocdb_backtest_arrays(ts, open, high, low, close, 12, target, &p, NULL, NULL, 0, &r) == 0 && r.n_trades == 3, "no outputs / no trade buffer");
  HOCDBBacktestParams bad = p; bad.position_mode = 9;
  CHECK(hocdb_backtest_arrays(ts, open, high, low, close, 12, target, &bad, NULL, NULL, 0, &r) == -2, "bad params -> -2");
  // walk-forward
  HOCDBSplit splits[8];
  size_t k = hocdb_walk_forward_splits(100, 4, 0.5, 1, splits, 8);
  CHECK(k == 4 && splits[0].train_start == 0 && splits[0].train_end == 50 && splits[0].test_start == 50 && splits[0].test_end == 62 && splits[3].test_end == 100, "walk-forward splits");
  int64_t ts100[100]; double c100[100], t100[100];
  for (int i = 0; i < 100; i++) { ts100[i] = i + 1; c100[i] = 100 + 0.25 * i; t100[i] = 1; }
  HOCDBBacktestResult rs[4];
  HOCDBBacktestParams sp; hocdb_backtest_params_default(&sp); sp.fill_mode = 1; sp.position_mode = 1;
  CHECK(hocdb_backtest_splits_arrays(ts100, NULL, NULL, NULL, c100, 100, t100, &sp, splits, 4, rs) == 4, "splits run");
  CHECK(rs[0].n_bars == 12 && near(rs[0].total_return, c100[61] / c100[50] - 1.0, 1e-12), "split result");

  // --- database windows ---
  const char *dir = "b_c_test_backtest";
  system("rm -rf b_c_test_backtest");
  CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64}, {"size", HOCDB_TYPE_F64}};
  HOCDBConfig cfg; memset(&cfg, 0, sizeof cfg); cfg.auto_migrate = 1; cfg.calendar = HOCDB_CALENDAR_CRYPTO; cfg.timestamp_unit_ns = 1000000000ULL;
  HOCDBHandle db = hocdb_init_ex("B", dir, schema, 3, &cfg);
  CHECK(db != NULL, "open");
  double price = 100.0;
  for (int i = 0; i < 20000; i++) {
    price *= 1.0 + 0.0005 * sin(i * 0.37) + 0.0002 * cos(i * 0.11);
    Tick t = {1700000000LL + (int64_t)i * 7, price, 1.0 + (i % 5)};
    hocdb_append(db, &t, sizeof t);
  }
  hocdb_flush(db);
  HOCDBIndicatorColumns cols = {-1, -1, -1, 1, 2, -1, -1, -1};
  int64_t start = 1700000100LL, end = start + 24 * 3600; // bucket-aligned (300 s bars)
  HOCDBBarsEx bars;
  CHECK(hocdb_ohlcv_ex(db, start, end, 1, 2, -1, 300, &bars) == 0 && bars.n_bars == 288, "ohlcv bars");
  double *tgt = malloc(bars.n_bars * sizeof(double));
  for (size_t i = 0; i < bars.n_bars; i++) tgt[i] = (i >= 5 && bars.close[i] > bars.close[i - 5]) ? 1.0 : (i >= 5 ? -1.0 : 0.0);
  HOCDBBacktestParams dp; hocdb_backtest_params_default(&dp); dp.initial_equity = 10000; dp.cost_bps = 5; dp.position_mode = 1;
  HOCDBBacktestResult dr, kr;
  CHECK(hocdb_backtest(db, &cols, start, end, 300, tgt, bars.n_bars, &dp, NULL, NULL, 0, &dr) == 0, "db backtest rc");
  HOCDBBacktestParams kp = dp; kp.periods_per_year = 365.0 * 288.0; // the handle derives it from its crypto calendar
  CHECK(hocdb_backtest_arrays(bars.timestamps, bars.open, bars.high, bars.low, bars.close, bars.n_bars, tgt, &kp, NULL, NULL, 0, &kr) == 0, "kernel rc");
  CHECK(dr.n_bars == kr.n_bars && dr.final_equity == kr.final_equity && dr.sharpe == kr.sharpe && dr.n_trades == kr.n_trades && dr.ann_vol > 0, "db window == ohlcv bars + calendar ppy");
  CHECK(hocdb_backtest(db, &cols, start, end, 300, tgt, bars.n_bars - 1, &dp, NULL, NULL, 0, &dr) == -7, "length mismatch -> -7");
  HOCDBBacktestResult tr;
  CHECK(hocdb_backtest_tail(db, &cols, 300, tgt + bars.n_bars - 50, 50, &dp, NULL, NULL, 0, &tr) == 0 && tr.n_bars == 50, "tail window");
  HOCDBBarsEx all;
  CHECK(hocdb_ohlcv_ex(db, INT64_MIN, INT64_MAX, 1, 2, -1, 300, &all) == 0 && all.n_bars >= 50, "all bars");
  size_t m = all.n_bars;
  CHECK(hocdb_backtest_arrays(all.timestamps + m - 50, all.open + m - 50, all.high + m - 50, all.low + m - 50, all.close + m - 50, 50, tgt + bars.n_bars - 50, &kp, NULL, NULL, 0, &kr) == 0 && kr.final_equity == tr.final_equity, "tail == last 50 bars");
  hocdb_ohlcv_ex_free(&all);
  hocdb_ohlcv_ex_free(&bars);
  free(tgt);
  hocdb_close(db);
  system("rm -rf b_c_test_backtest");
  if (failures) { printf("C backtest API test FAILED (%d)\n", failures); return 1; }
  printf("C backtest API test passed\n");
  return 0;
}
