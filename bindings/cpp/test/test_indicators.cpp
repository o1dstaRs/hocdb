// C++ wrapper test for the indicator / analytics API (hocdb::Database).
#include "hocdb_cpp.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#define CHECK(cond, msg)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::cerr << "FAIL: " << msg << " (line " << __LINE__ << ")\n";          \
      return 1;                                                                \
    }                                                                          \
  } while (0)

#define EXPECT_THROW(expr, msg)                                                \
  do {                                                                         \
    bool thrown = false;                                                       \
    try {                                                                      \
      (void)(expr);                                                            \
    } catch (const hocdb::Exception &) {                                       \
      thrown = true;                                                           \
    }                                                                          \
    CHECK(thrown, msg);                                                        \
  } while (0)

struct __attribute__((packed)) Bar {
  int64_t timestamp;
  double open, high, low, close, volume;
};

struct __attribute__((packed)) Tick {
  int64_t timestamp;
  double price, size, bid, ask;
  uint8_t side;
};
static_assert(sizeof(Tick) == 41, "tick record size");

struct __attribute__((packed)) TsValue {
  int64_t timestamp;
  double value;
};
static_assert(sizeof(HOCDBIndicatorColumns) == 8 * sizeof(int64_t),
              "HOCDBIndicatorColumns has 8 roles");

static const char *kDir = "b_cpp_test_indicators";

static double lcg(uint64_t &s) {
  s = s * 6364136223846793005ULL + 1442695040888963407ULL;
  return static_cast<double>(s >> 11) / 9007199254740992.0;
}

static bool has(const std::vector<std::string> &names, const std::string &n) {
  return std::find(names.begin(), names.end(), n) != names.end();
}

static bool near(double a, double b, double rel) {
  return std::fabs(a - b) <= rel * std::max(std::fabs(a), std::fabs(b));
}

static bool sameValue(double a, double b) {
  return (std::isnan(a) && std::isnan(b)) || a == b;
}

static double mean(const std::vector<double> &v, size_t last, size_t n) {
  double s = 0;
  for (size_t i = last + 1 - n; i <= last; ++i) {
    s += v[i];
  }
  return s / static_cast<double>(n);
}

static int run() {
  std::vector<hocdb::Field> schema = {
      {"timestamp", HOCDB_TYPE_I64}, {"open", HOCDB_TYPE_F64},
      {"high", HOCDB_TYPE_F64},      {"low", HOCDB_TYPE_F64},
      {"close", HOCDB_TYPE_F64},     {"volume", HOCDB_TYPE_F64}};
  hocdb::Database db("IND", std::string(kDir) + "/bars", schema);

  const int N = 3000;
  uint64_t seed = 7;
  double p = 100.0;
  std::vector<double> closes, volumes;
  for (int i = 0; i < N; i++) {
    double o = p;
    p *= std::exp((lcg(seed) - 0.5) * 0.02);
    Bar b{1000 + static_cast<int64_t>(i) * 60, o, std::max(o, p) * 1.003,
          std::min(o, p) * 0.997, p, 1000.0 + (i % 50)};
    db.append(b);
    closes.push_back(p);
    volumes.push_back(b.volume);
  }
  db.flush();
  const int64_t last_ts = 1000 + static_cast<int64_t>(N - 1) * 60;

  // --- registry ---
  std::cout << "Testing registry helpers...\n";
  auto kinds = hocdb::Database::indicatorKinds();
  CHECK(kinds.size() == 83, "number of kinds");
  CHECK(has(kinds, "rsi") && has(kinds, "heikin_ashi"), "kind names");
  CHECK(has(kinds, "spread") && has(kinds, "order_flow") && has(kinds, "series2") &&
            has(kinds, "forward_return") && has(kinds, "pivots"),
        "new kind names");
  CHECK(hocdb::Database::indicatorOutputs("MACD") ==
            std::vector<std::string>({"macd", "signal", "hist"}),
        "macd outputs (case-insensitive)");
  CHECK(hocdb::Database::indicatorOutputs("sma") ==
            std::vector<std::string>({"value"}),
        "single-output kinds report one output named \"value\"");
  CHECK(hocdb::Database::indicatorOutputs("pivots").size() == 5, "pivots outputs");
  EXPECT_THROW(hocdb::Database::indicatorOutputs("nope"), "unknown kind outputs");
  CHECK(hocdb::Database::indicatorWarmup({"ema", 200}) > 200, "warmup > period for EMA");
  CHECK(hocdb::Database::indicatorWarmup({"sma", 20}) > 0, "warmup for SMA");
  EXPECT_THROW(hocdb::Database::indicatorWarmup({"nope"}), "unknown kind warmup");
  CHECK(hocdb::Database::indicatorIsLookahead("forward_return") &&
            hocdb::Database::indicatorIsLookahead("triple_barrier"),
        "labels are look-ahead");
  CHECK(!hocdb::Database::indicatorIsLookahead("sma") &&
            !hocdb::Database::indicatorIsLookahead("session_vwap"),
        "regular kinds are not look-ahead");
  EXPECT_THROW(hocdb::Database::indicatorIsLookahead("nope"), "unknown kind lookahead");

  // --- batch over a range ---
  std::cout << "Testing indicators() over a range...\n";
  std::vector<hocdb::IndicatorSpec> specs = {
      {"sma", 20},
      {"macd"},
      {"rsi", 14},
      {"bbands", 20, 0, 0, 0, 2.0},
      {"atr", 14},
      {"obv"},
      {"sma", 10, 0, 0, 0, 0, 0, "volume"}, // SMA of volume
  };
  const int64_t start_ts = 1000 + 1000 * 60, end_ts = 1000 + 1500 * 60;
  auto res = db.indicators(specs, start_ts, end_ts);
  CHECK(res.n_rows == 500, "500 rows");
  CHECK(res.timestamps.size() == 500, "500 timestamps");
  CHECK(res.names.size() == 13 && res.outputs.size() == 13, "13 outputs");
  const char *expected[] = {"sma_20",           "macd",
                            "macd_signal",      "macd_hist",
                            "rsi_14",           "bbands_20_upper",
                            "bbands_20_middle", "bbands_20_lower",
                            "bbands_20_percent_b", "bbands_20_bandwidth",
                            "atr_14",           "obv",
                            "sma_10"};
  for (size_t i = 0; i < 13; ++i) {
    CHECK(res.names[i] == expected[i], std::string("column name ") + expected[i]);
    CHECK(res.outputs[i].size() == 500, "output length");
  }
  CHECK(res.timestamps[0] == start_ts, "first ts");
  CHECK(res.timestamps[499] == end_ts - 60, "last ts");
  const auto &sma20 = res.column("sma_20");
  const auto &rsi = res.column("rsi_14");
  const auto &up = res.column("bbands_20_upper");
  const auto &mid = res.column("bbands_20_middle");
  const auto &lo = res.column("bbands_20_lower");
  const auto &atr = res.column("atr_14");
  for (size_t i = 0; i < res.n_rows; i++) {
    CHECK(!std::isnan(sma20[i]), "sma converged (auto lookback)");
    CHECK(rsi[i] >= 0 && rsi[i] <= 100, "rsi in [0,100]");
    CHECK(up[i] >= mid[i] && mid[i] >= lo[i], "bbands ordered");
    CHECK(atr[i] > 0, "atr positive");
  }
  EXPECT_THROW(res.column("nope"), "unknown column");

  // sma_20 against a local reference (mean of the last 20 closes)
  for (size_t j : {0u, 1u, 137u, 499u}) {
    const size_t row = 1000 + j;
    CHECK(near(sma20[j], mean(closes, row, 20), 1e-9), "sma_20 matches reference");
  }
  const auto &vsma = res.column("sma_10");
  CHECK(near(vsma[0], mean(volumes, 1000, 10), 1e-9), "sma_10 of volume matches reference");

  // explicit lookback 0 -> NaN warm-up inside the window
  hocdb::IndicatorOptions no_lookback;
  no_lookback.lookback = 0;
  auto res0 = db.indicators({{"sma", 20}}, start_ts, end_ts, no_lookback);
  CHECK(res0.n_rows == 500, "lookback 0 rows");
  const auto &sma0 = res0.column("sma_20");
  CHECK(std::isnan(sma0[0]) && std::isnan(sma0[18]) && !std::isnan(sma0[19]),
        "NaN warm-up with lookback 0");
  CHECK(near(sma0[19], sma20[19], 1e-12), "converged values equal");

  // explicit columns + label override
  hocdb::IndicatorOptions explicit_cols;
  explicit_cols.columns = hocdb::IndicatorColumns{"open", "high", "low", "close", "volume"};
  auto resl = db.indicators({{"sma", 20, 0, 0, 0, 0, 0, "", "", "fast"}}, start_ts, end_ts, explicit_cols);
  CHECK(resl.names == std::vector<std::string>({"fast"}), "label override");
  double max_rel = 0;
  for (size_t i = 0; i < sma20.size(); ++i) {
    max_rel = std::max(max_rel, std::fabs(resl.column("fast")[i] - sma20[i]) / std::fabs(sma20[i]));
  }
  CHECK(max_rel <= 1e-9, "explicit columns give the same series (max rel diff " + std::to_string(max_rel) + ")");
  auto resa = db.indicatorsTail(5, {{"adx", 14}, {"macd", 0, 0, 0, 0, 0, 0, "", "", "m"}});
  CHECK(resa.names == std::vector<std::string>({"adx_14", "adx_14_plus_di", "adx_14_minus_di", "m", "m_signal", "m_hist"}),
        "multi-output naming with label");
  EXPECT_THROW(db.indicatorsTail(5, {{"sma", 20}, {"sma", 20}}), "duplicate output column");
  CHECK(db.indicatorsTail(5, {{"sma", 20}, {"sma", 20, 0, 0, 0, 0, 0, "volume", "", "vol_sma"}}).names.size() == 2,
        "duplicate resolved with label");

  // zero rows (window before the first record)
  auto empty = db.indicators({{"sma", 5}}, 0, 500);
  CHECK(empty.n_rows == 0 && empty.timestamps.empty(), "empty window");
  CHECK(empty.outputs.size() == 1 && empty.outputs[0].empty(), "empty outputs");

  // --- tail ---
  std::cout << "Testing indicatorsTail()...\n";
  auto tail = db.indicatorsTail(5, {{"sma", 20}, {"macd"}, {"rsi", 14}});
  CHECK(tail.n_rows == 5 && tail.names.size() == 5, "tail");
  CHECK(tail.timestamps[4] == last_ts, "tail last ts");
  CHECK(near(tail.column("sma_20")[4], mean(closes, N - 1, 20), 1e-9), "tail sma");

  // --- bucket (tick -> 5-minute bars) ---
  hocdb::IndicatorOptions bucketed;
  bucketed.bucket = 300;
  bucketed.lookback = 0;
  auto tailb = db.indicatorsTail(10, {{"sma", 20}}, bucketed);
  CHECK(tailb.n_rows == 10, "tail with bucket");
  CHECK(tailb.timestamps[1] - tailb.timestamps[0] == 300, "bar spacing");
  CHECK(tailb.timestamps[0] % 300 == 0, "bar alignment");

  // --- errors ---
  std::cout << "Testing errors...\n";
  EXPECT_THROW(db.indicatorsTail(10, {{"nope"}}), "unknown kind name");
  hocdb::IndicatorOptions no_close;
  no_close.columns = hocdb::IndicatorColumns{"", "", "", "", ""};
  EXPECT_THROW(db.indicatorsTail(10, {{"sma", 5}}, no_close), "missing close");
  hocdb::IndicatorOptions close_only;
  close_only.columns = hocdb::IndicatorColumns{"", "", "", "close", ""};
  EXPECT_THROW(db.indicatorsTail(10, {{"atr", 14}}, close_only), "atr needs high/low");
  CHECK(db.indicatorsTail(10, {{"sma", 5}}, close_only).n_rows == 10, "close-only is enough for sma");
  EXPECT_THROW(db.indicatorsTail(10, {{"sma", 5, 0, 0, 0, 0, 0, "nope"}}), "invalid field name");
  EXPECT_THROW(db.indicatorsTail(10, {{"sma", 5, 0, 0, 0, 0, 0, "volume"}}, bucketed), "field override with bucket");
  hocdb::IndicatorOptions bad_col;
  bad_col.columns = hocdb::IndicatorColumns{"", "", "", "nope", ""};
  EXPECT_THROW(db.indicatorsTail(10, {{"sma", 5}}, bad_col), "invalid column name");

  // --- ohlcv ---
  std::cout << "Testing ohlcv()...\n";
  auto bars = db.ohlcv(INT64_MIN, INT64_MAX, 300, "close", "volume");
  const size_t nb = bars.timestamps.size();
  CHECK(nb > 500, "ohlcv bar count");
  CHECK(bars.open.size() == nb && bars.high.size() == nb && bars.low.size() == nb &&
            bars.close.size() == nb && bars.volume.size() == nb && bars.count.size() == nb,
        "ohlcv lengths");
  CHECK(bars.buy_volume.empty(), "ohlcv without side: no buy_volume");
  for (size_t i = 0; i < nb; i++) {
    CHECK(bars.high[i] >= bars.low[i], "bar high >= low");
    CHECK(bars.high[i] >= bars.close[i] && bars.low[i] <= bars.close[i], "close within bar");
    CHECK(bars.count[i] >= 1, "bar count");
  }
  auto bars_nv = db.ohlcv(INT64_MIN, INT64_MAX, 300, "close");
  CHECK(bars_nv.timestamps == bars.timestamps, "ohlcv without volume: same bars");
  CHECK(bars_nv.volume == bars_nv.count, "ohlcv without volume: volume is the record count");
  EXPECT_THROW(db.ohlcv(INT64_MIN, INT64_MAX, 300, "nope"), "ohlcv unknown field");
  EXPECT_THROW(db.ohlcv(INT64_MIN, INT64_MAX, 300, "close", "volume", "nope"), "ohlcv unknown side field");

  // --- summary ---
  std::cout << "Testing summary()...\n";
  auto sum = db.summary(INT64_MIN, INT64_MAX, "close", 252.0);
  CHECK(sum.count == static_cast<uint64_t>(N), "summary count");
  CHECK(sum.max_drawdown <= 0 && sum.max_drawdown >= -1, "max drawdown range");
  CHECK(sum.win_rate >= 0 && sum.win_rate <= 1, "win rate range");
  CHECK(std::isfinite(sum.sharpe) && std::isfinite(sum.hurst), "summary fields computed");
  auto summ = db.summaryMap(INT64_MIN, INT64_MAX, "close", 252.0);
  CHECK(summ.size() == 29, "summary map has 29 fields");
  CHECK(summ.at("count") == static_cast<double>(N), "summary map count");
  CHECK(summ.at("sharpe") == sum.sharpe && summ.at("max_drawdown") == sum.max_drawdown &&
            summ.at("half_life") == sum.half_life,
        "summary map matches struct");
  EXPECT_THROW(db.summary(INT64_MIN, INT64_MAX, "nope"), "summary unknown field");

  // --- snapshot ---
  std::cout << "Testing snapshot()...\n";
  CHECK(hocdb_snapshot_size() == sizeof(HOCDBSnapshot), "snapshot struct size matches library");
  auto snap = db.snapshot(nullptr, 0, 0, 252.0);
  CHECK(snap.bars == 2500, "snapshot bars");
  CHECK(snap.timestamp == last_ts, "snapshot ts");
  CHECK(snap.rsi_14 >= 0 && snap.rsi_14 <= 100, "snapshot rsi");
  CHECK(std::isfinite(snap.ema_200) && std::isfinite(snap.adx_14) &&
            std::isfinite(snap.mfi_14) && std::isfinite(snap.supertrend),
        "snapshot fields");
  CHECK(std::fabs(snap.close - p) < 1e-9, "snapshot close is latest");
  auto snapm = db.snapshotMap(nullptr, 0, 0, 252.0);
  CHECK(snapm.size() >= 90, "snapshot map >= 90 fields");
  CHECK(snapm.at("bars") == 2500 && snapm.at("timestamp") == static_cast<double>(last_ts),
        "snapshot map ints");
  CHECK(snapm.at("close") == snap.close && snapm.at("rsi_14") == snap.rsi_14 &&
            snapm.at("kurtosis_20") == snap.kurtosis_20,
        "snapshot map matches struct");
  hocdb::IndicatorColumns cols{"open", "high", "low", "close", "volume"};
  auto snapb = db.snapshot(&cols, 50, 300, 252.0);
  CHECK(snapb.bars == 50, "snapshot with bucket: bars");
  CHECK(std::isnan(snapb.sma_200) && !std::isnan(snapb.sma_20), "snapshot with bucket: warm-up NaN");
  auto snapbm = db.snapshotMap(&cols, 50, 300);
  CHECK(snapbm.at("bars") == 50 && std::isnan(snapbm.at("sma_200")) && !std::isnan(snapbm.at("sma_20")),
        "snapshot map with bucket");
  hocdb::IndicatorColumns no_close_cols{"", "", "", "", ""};
  EXPECT_THROW(db.snapshot(&no_close_cols), "snapshot without close");

  // --- "price" is used as close when the schema has no "close" ---
  std::cout << "Testing close auto-detection...\n";
  {
    std::vector<hocdb::Field> tick_schema = {{"timestamp", HOCDB_TYPE_I64},
                                             {"price", HOCDB_TYPE_F64}};
    hocdb::Database tdb("PX", std::string(kDir) + "/price", tick_schema);
    for (int i = 0; i < 100; i++) {
      tdb.append(TsValue{1000 + i, 100.0 + i});
    }
    tdb.flush();
    auto r = tdb.indicatorsTail(5, {{"sma", 3}});
    CHECK(r.n_rows == 5, "price as close: rows");
    CHECK(near(r.column("sma_3")[4], 198.0, 1e-12), "price as close: sma_3");
    EXPECT_THROW(tdb.indicatorsTail(5, {{"atr", 14}}), "price as close: atr needs high/low");

    std::vector<hocdb::Field> bad_schema = {{"timestamp", HOCDB_TYPE_I64},
                                            {"value", HOCDB_TYPE_F64}};
    hocdb::Database vdb("VAL", std::string(kDir) + "/value", bad_schema);
    vdb.append(TsValue{1, 1.0});
    vdb.flush();
    EXPECT_THROW(vdb.indicatorsTail(1, {{"sma", 1}}), "no close/price column");
    hocdb::IndicatorOptions value_col;
    value_col.columns = hocdb::IndicatorColumns{"", "", "", "value", ""};
    CHECK(vdb.indicatorsTail(1, {{"sma", 1}}, value_col).n_rows == 1, "explicit close column");
  }

  // ------------------------------------------------------------------
  // Pairs, microstructure, labels, sessions, health, evaluation, multi
  // ------------------------------------------------------------------
  std::cout << "Testing tick databases (bid/ask/side roles)...\n";
  std::vector<hocdb::Field> tick_schema = {
      {"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64},
      {"size", HOCDB_TYPE_F64},      {"bid", HOCDB_TYPE_F64},
      {"ask", HOCDB_TYPE_F64},       {"side", HOCDB_TYPE_BOOL}};
  const std::string tick_dir = std::string(kDir) + "/ticks";
  hocdb::Database ta("PAIR_A", tick_dir, tick_schema);
  hocdb::Database tb("PAIR_B", tick_dir, tick_schema);
  CHECK(ta.get_record_size() == 41 && tb.get_record_size() == 41, "tick record size");
  const int NT = 6000;
  const int64_t SEC = 1000000; // microseconds per second
  std::vector<double> pa_close, pa_size;
  std::vector<bool> pa_side;
  double pa = 100.0, pb = 50.0;
  for (int i = 0; i < NT; i++) {
    pa *= std::exp((lcg(seed) - 0.5) * 0.004);
    pb *= std::exp((lcg(seed) - 0.5) * 0.004);
    Tick x{SEC * i, pa, 1.0 + i % 4, pa * 0.999, pa * 1.001, static_cast<uint8_t>(i % 3 != 0)};
    ta.append(x);
    pa_close.push_back(x.price);
    pa_size.push_back(x.size);
    pa_side.push_back(x.side != 0);
    if (i % 2 == 0) { // B trades every 2 seconds, 300 ms after A
      Tick y{SEC * i + 300000, pb, 2.0, pb * 0.999, pb * 1.001, static_cast<uint8_t>(i % 2)};
      tb.append(y);
    }
  }
  ta.flush();
  tb.flush();
  const int64_t last_tick_ts = SEC * (NT - 1);

  // --- microstructure / session / label kinds on ticks (auto-detected roles) ---
  std::cout << "Testing microstructure indicators...\n";
  std::vector<hocdb::IndicatorSpec> micro = {
      {"spread"},
      {"order_flow", 10},
      {"trade_intensity", 10, 0, 0, 0, 1e6},
      {"tick_pressure", 20},
      {"session_vwap", 0, 0, 0, 0, 600e6}, // 10-minute sessions
      {"forward_return", 5},
  };
  auto mres = ta.indicatorsTail(100, micro);
  CHECK(mres.n_rows == 100 && mres.names.size() == 11, "microstructure batch: 100 rows, 11 outputs");
  const std::vector<std::string> micro_names = {
      "spread_abs", "spread_bps", "order_flow_10_net", "order_flow_10_imbalance",
      "trade_intensity_10_trades_per_sec", "trade_intensity_10_volume_per_sec",
      "tick_pressure_20", "session_vwap",
      "forward_return_5_ret", "forward_return_5_max", "forward_return_5_min"};
  CHECK(mres.names == micro_names, "microstructure column names");
  CHECK(mres.timestamps[99] == last_tick_ts, "microstructure tail ends at the last tick");
  const auto &spread_bps = mres.column("spread_bps");
  const auto &tps = mres.column("trade_intensity_10_trades_per_sec");
  const auto &imb = mres.column("order_flow_10_imbalance");
  const auto &fwd = mres.column("forward_return_5_ret");
  for (size_t i = 0; i < 100; ++i) {
    CHECK(std::fabs(spread_bps[i] - 20.0) < 1e-9, "spread is 20 bps");
    CHECK(std::fabs(tps[i] - 1.0) < 1e-9, "1 trade per second");
    CHECK(imb[i] >= -1 && imb[i] <= 1, "imbalance in [-1, 1]");
    CHECK(std::fabs(mres.column("spread_abs")[i] - 0.002 * pa_close[NT - 100 + i]) < 1e-9, "absolute spread");
  }
  for (size_t i = 0; i < 95; ++i) {
    CHECK(std::isfinite(fwd[i]), "forward return defined before the last 5 rows");
  }
  for (size_t i = 95; i < 100; ++i) {
    CHECK(std::isnan(fwd[i]), "forward return NaN in the last 5 rows (look-ahead)");
  }
  CHECK(near(fwd[0], pa_close[NT - 100 + 5] / pa_close[NT - 100] - 1.0, 1e-9), "forward_return_5_ret matches reference");
  EXPECT_THROW(ta.indicatorsTail(10, {{"session_vwap"}}), "session kinds need param = session length");
  // explicit 5-role columns leave bid/ask absent -> spread has no input
  hocdb::IndicatorOptions five_roles;
  five_roles.columns = hocdb::IndicatorColumns{"", "", "", "price", "size"};
  EXPECT_THROW(ta.indicatorsTail(10, {{"spread"}}, five_roles), "spread needs bid/ask columns");
  hocdb::IndicatorOptions eight_roles;
  eight_roles.columns = hocdb::IndicatorColumns{"", "", "", "price", "size", "bid", "ask", "side"};
  auto mres8 = ta.indicatorsTail(100, micro, eight_roles);
  CHECK(mres8.names == micro_names && mres8.column("order_flow_10_net") == mres.column("order_flow_10_net"),
        "explicit bid/ask/side columns give the same result");
  hocdb::IndicatorOptions bad_side;
  bad_side.columns = hocdb::IndicatorColumns{"", "", "", "price", "size", "bid", "ask", "nope"};
  EXPECT_THROW(ta.indicatorsTail(10, {{"spread"}}, bad_side), "invalid side column name");

  // --- pairs ---
  std::cout << "Testing pairIndicators()...\n";
  std::vector<hocdb::IndicatorSpec> pair = {
      {"series"}, {"series2"}, {"ratio"}, {"correl", 30}, {"rel_strength", 10}};
  auto pres = ta.pairIndicatorsTail(tb, 50, pair);
  CHECK(pres.n_rows == 50 && pres.names.size() == 5, "pair tail on ticks: 50 rows, 5 outputs");
  CHECK(pres.names == std::vector<std::string>({"series", "series2", "ratio", "correl_30", "rel_strength_10"}),
        "pair column names");
  CHECK(pres.timestamps[49] == last_tick_ts && pres.timestamps[0] == SEC * (NT - 50), "pair rows are A's ticks");
  const auto &s1 = pres.column("series");
  const auto &s2 = pres.column("series2");
  const auto &ratio = pres.column("ratio");
  const auto &correl = pres.column("correl_30");
  for (size_t i = 0; i < 50; ++i) {
    CHECK(std::fabs(s1[i] / s2[i] - ratio[i]) < 1e-12, "ratio = series / series2");
    CHECK(std::isfinite(correl[i]), "correl defined");
    CHECK(s1[i] == pa_close[NT - 50 + i], "series is A's price");
  }
  hocdb::PairOptions pair_bars;
  pair_bars.bucket = 10 * SEC;
  auto prange = ta.pairIndicators(tb, pair, 1000 * SEC, 2000 * SEC, pair_bars);
  CHECK(prange.n_rows == 100, "pair range on 10 s bars: 100 bars");
  CHECK(prange.timestamps[0] == 1000 * SEC && prange.timestamps[99] == 1990 * SEC, "pair bar window");
  for (size_t i = 0; i < 100; ++i) {
    CHECK(prange.timestamps[i] % (10 * SEC) == 0, "pair bars aligned");
    if (i > 0) {
      CHECK(prange.timestamps[i] - prange.timestamps[i - 1] == 10 * SEC, "pair bars 10 s apart");
    }
    CHECK(std::fabs(prange.column("series")[i] / prange.column("series2")[i] - prange.column("ratio")[i]) < 1e-12,
          "bar ratio = series / series2");
  }
  hocdb::PairOptions pair_explicit;
  pair_explicit.columns = hocdb::IndicatorColumns{"", "", "", "price", "size", "bid", "ask", "side"};
  pair_explicit.other_columns = hocdb::IndicatorColumns{"", "", "", "price", "size", "bid", "ask", "side"};
  auto pres2 = ta.pairIndicatorsTail(tb, 50, pair, pair_explicit);
  CHECK(pres2.column("ratio") == ratio, "explicit other_columns give the same result");
  hocdb::PairOptions other_bad;
  other_bad.other_columns = hocdb::IndicatorColumns{"", "", "", "nope", ""};
  EXPECT_THROW(ta.pairIndicatorsTail(tb, 50, pair, other_bad), "invalid other column name");
  EXPECT_THROW(ta.pairIndicatorsTail(tb, 50, {{"ratio", 0, 0, 0, 0, 0, 0, "", "size"}}), "field2 is rejected in pair calls");
  {
    hocdb::Database vdb("VAL2", std::string(kDir) + "/value2",
                        {{"timestamp", HOCDB_TYPE_I64}, {"value", HOCDB_TYPE_F64}});
    vdb.append(TsValue{1, 2.5});
    vdb.flush();
    EXPECT_THROW(ta.pairIndicatorsTail(vdb, 50, pair), "other database without close/price column");
    hocdb::PairOptions other_value;
    other_value.other_columns = hocdb::IndicatorColumns{"", "", "", "value", ""};
    auto pv = ta.pairIndicatorsTail(vdb, 50, {{"series2"}}, other_value);
    CHECK(pv.n_rows == 50 && pv.column("series2")[49] == 2.5, "explicit other close column (as-of joined)");
  }

  // --- ohlcv with side -> buy volume ---
  std::cout << "Testing ohlcv() with side...\n";
  auto bx = ta.ohlcv(INT64_MIN, INT64_MAX, 60 * SEC, "price", "size", "side");
  CHECK(bx.timestamps.size() == 100 && bx.buy_volume.size() == 100, "ohlcv with side: 100 bars with buy_volume");
  for (size_t k = 0; k < 100; ++k) {
    double vol = 0, buy = 0;
    for (size_t i = 60 * k; i < 60 * (k + 1); ++i) {
      vol += pa_size[i];
      if (pa_side[i]) {
        buy += pa_size[i];
      }
    }
    CHECK(bx.buy_volume[k] >= 0 && bx.buy_volume[k] <= bx.volume[k], "buy volume within volume");
    CHECK(near(bx.volume[k], vol, 1e-12) && near(bx.buy_volume[k], buy, 1e-12), "bar volume / buy volume match reference");
    CHECK(bx.count[k] == 60, "60 ticks per minute bar");
  }
  auto bx_ns = ta.ohlcv(INT64_MIN, INT64_MAX, 60 * SEC, "price", "size");
  CHECK(bx_ns.timestamps == bx.timestamps && bx_ns.volume == bx.volume, "ohlcv without side: same bars");
  CHECK(bx_ns.buy_volume.empty(), "ohlcv without side: no buy_volume");

  // --- health ---
  std::cout << "Testing health()...\n";
  CHECK(hocdb_health_size() == sizeof(HOCDBHealth), "health struct size matches library");
  auto hl = ta.health(INT64_MIN, INT64_MAX, "price", "size", 5 * SEC, 0.05);
  CHECK(hl.count == 6000, "health count");
  CHECK(hl.n_gaps == 0 && hl.median_gap == 1e6 && hl.max_gap == SEC && hl.mean_gap == 1e6, "health gaps");
  CHECK(hl.n_outlier_returns == 0 && hl.n_nonpositive_price == 0 && hl.n_nan_price == 0, "health price checks");
  CHECK(hl.first_ts == 0 && hl.last_ts == 5999 * SEC && hl.span == 5999 * SEC, "health timestamps");
  CHECK(hl.n_zero_volume == 0 && hl.n_negative_volume == 0, "health volume checks");
  auto hlm = ta.healthMap(INT64_MIN, INT64_MAX, "price", "size", 5 * SEC, 0.05);
  CHECK(hlm.size() == 19, "health map has 19 fields");
  CHECK(hlm.at("count") == 6000 && hlm.at("n_gaps") == 0 && hlm.at("median_gap") == 1e6 &&
            hlm.at("last_ts") == 5999e6 && hlm.at("max_abs_return") == hl.max_abs_return,
        "health map matches struct");
  auto hl_nv = ta.health(INT64_MIN, INT64_MAX, "price");
  CHECK(hl_nv.count == 6000 && hl_nv.n_zero_volume == 0, "health without volume");
  EXPECT_THROW(ta.health(INT64_MIN, INT64_MAX, "nope"), "health unknown field");

  // --- evaluation ---
  std::cout << "Testing evaluate()...\n";
  CHECK(hocdb_evaluation_size() == sizeof(HOCDBEvaluation) && hocdb_decision_size() == sizeof(HOCDBDecision),
        "evaluation struct sizes match library");
  std::vector<hocdb::Decision> decisions = {
      {100 * SEC, +1, 1000, 60 * SEC},
      {200 * SEC, -1, 500, 0},       // default horizon
      {5990 * SEC, +1, 100, 60 * SEC}, // no exit price: not evaluated
      {300 * SEC, 0},                // flat: not evaluated
  };
  auto ev = ta.evaluate(decisions, "price", 120 * SEC, 5.0);
  CHECK(ev.stats.n_decisions == 4 && ev.stats.n_evaluated == 2 && ev.stats.n_long == 2 && ev.stats.n_short == 1,
        "evaluate counts");
  CHECK(ev.entry.size() == 4 && ev.exit.size() == 4 && ev.net_return.size() == 4, "per-decision arrays");
  CHECK(std::isfinite(ev.net_return[0]) && std::isfinite(ev.net_return[1]) &&
            std::isnan(ev.net_return[2]) && std::isnan(ev.net_return[3]),
        "net return defined only for evaluated decisions");
  CHECK(std::isnan(ev.entry[2]) && std::isnan(ev.exit[3]), "entry / exit NaN when not evaluated");
  CHECK(std::fabs(ev.net_return[0] - (ev.exit[0] / ev.entry[0] - 1.0 - 0.001)) < 1e-12,
        "net return = gross - 2 x 5 bps");
  CHECK(ev.entry[0] == pa_close[100] && ev.exit[0] == pa_close[160], "entry / exit prices of decision 0");
  CHECK(ev.entry[1] == pa_close[200] && ev.exit[1] == pa_close[320], "default horizon applied to decision 1");
  CHECK(std::isfinite(ev.stats.total_cost) && ev.stats.total_cost > 0, "total cost");
  auto evm = hocdb::Database::evaluationMap(ev.stats);
  CHECK(evm.size() == 20, "evaluation map has 20 fields");
  CHECK(evm.at("n_decisions") == 4 && evm.at("n_evaluated") == 2 && evm.at("n_short") == 1 &&
            evm.at("total_cost") == ev.stats.total_cost && sameValue(evm.at("hit_rate"), ev.stats.hit_rate),
        "evaluation map matches struct");
  auto ev0 = ta.evaluate({}, "price");
  CHECK(ev0.stats.n_decisions == 0 && ev0.stats.n_evaluated == 0 && std::isnan(ev0.stats.hit_rate),
        "empty evaluation");
  CHECK(ev0.entry.empty() && ev0.exit.empty() && ev0.net_return.empty(), "empty per-decision arrays");
  EXPECT_THROW(ta.evaluate(decisions, "nope"), "evaluate unknown field");

  // --- multi-timeframe snapshots ---
  std::cout << "Testing snapshotMulti()...\n";
  const std::vector<int64_t> buckets = {60 * SEC, 300 * SEC};
  const std::vector<double> ppys = {525600, 105120};
  auto multi = ta.snapshotMulti(buckets, ppys, 50);
  CHECK(multi.size() == 2, "two snapshots");
  CHECK(multi[0].bars == 50, "1-minute snapshot uses 50 bars");
  CHECK(multi[1].bars == 20, "5-minute snapshot has only 20 bars (100 minutes of data)");
  auto single = ta.snapshot(nullptr, 50, 60 * SEC, 525600);
  CHECK(single.timestamp == multi[0].timestamp && single.rsi_14 == multi[0].rsi_14 &&
            sameValue(single.sharpe_20, multi[0].sharpe_20),
        "multi equals single (struct)");
  auto single_map = ta.snapshotMap(nullptr, 50, 60 * SEC, 525600);
  auto multi_map = ta.snapshotMultiMap(buckets, ppys, 50);
  CHECK(multi_map.size() == 2 && multi_map[0].size() == single_map.size(), "two snapshot maps");
  for (const auto &[name, value] : single_map) {
    CHECK(sameValue(multi_map[0].at(name), value), "multi equals single field by field: " + name);
  }
  CHECK(multi_map[1].at("bars") == 20 && multi_map[1].at("timestamp") == static_cast<double>(multi[1].timestamp),
        "second snapshot map");
  CHECK(ta.snapshotMulti(buckets, {}, 50)[0].bars == 50, "periods_per_year defaults to 0");
  CHECK(ta.snapshotMulti({}).empty() && ta.snapshotMultiMap({}).empty(), "no buckets -> no snapshots");
  EXPECT_THROW(ta.snapshotMulti(buckets, {525600}, 50), "periods_per_year length mismatch");
  hocdb::IndicatorColumns tick_cols{"", "", "", "price", "size", "bid", "ask", "side"};
  CHECK(ta.snapshotMulti(buckets, ppys, 50, &tick_cols)[1].bars == 20, "snapshotMulti with explicit columns");
  EXPECT_THROW(ta.snapshotMulti(buckets, ppys, 50, &no_close_cols), "snapshotMulti without close");

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
    std::cout << "C++ indicator API test passed\n";
  }
  return rc;
}
