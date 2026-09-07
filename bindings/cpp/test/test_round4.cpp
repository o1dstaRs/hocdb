// Round 4: trading calendars, signal backtester and universe features.
//   clang++ -std=c++17 bindings/cpp/test/test_round4.cpp -o test_binaries/test_cpp_round4 \
//     -I bindings/c -I bindings/cpp -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib && ./test_binaries/test_cpp_round4
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "hocdb_cpp.h"

static int failures = 0;
static const int64_t US = 1000000;
static const char *DATA_DIR = "b_cpp_test_round4";

static void check(bool cond, const char *msg) {
  if (cond) {
    printf("  ok: %s\n", msg);
  } else {
    printf("  FAIL: %s\n", msg);
    failures++;
  }
}

static bool near(double a, double b, double tol = 1e-9) {
  if (std::isnan(a) || std::isnan(b)) return std::isnan(a) && std::isnan(b);
  return std::fabs(a - b) <= tol + 1e-9 * std::fabs(b);
}

template <typename Fn>
static void expectError(Fn fn, const char *msg, const char *needle = nullptr) {
  try {
    fn();
  } catch (const hocdb::Exception &e) {
    if (needle && std::string(e.what()).find(needle) == std::string::npos) {
      printf("  FAIL: %s -> wrong message: %s\n", msg, e.what());
      failures++;
    } else {
      printf("  ok: %s -> %s\n", msg, e.what());
    }
    return;
  }
  printf("  FAIL: %s (no exception)\n", msg);
  failures++;
}

static int64_t utc(int64_t y, unsigned m, unsigned d, int64_t hh = 0, int64_t mm = 0) {
  return hocdb::daysFromCivil(y, m, d) * 86400 + hh * 3600 + mm * 60;
}

static std::vector<hocdb::Field> schema() {
  return {{"timestamp", HOCDB_TYPE_I64}, {"open", HOCDB_TYPE_F64}, {"high", HOCDB_TYPE_F64},
          {"low", HOCDB_TYPE_F64},       {"close", HOCDB_TYPE_F64}, {"volume", HOCDB_TYPE_F64}};
}

struct Bar {
  int64_t timestamp;
  double open, high, low, close, volume;
};

static hocdb::IndicatorColumns columns() {
  hocdb::IndicatorColumns c;
  c.open = "open";
  c.high = "high";
  c.low = "low";
  c.close = "close";
  c.volume = "volume";
  return c;
}

static void testCalendars() {
  printf("Trading calendars...\n");
  uint32_t nyse = hocdb::calendarId("nyse");
  check(nyse == 3 && hocdb::calendarId("LSE") == 5 && hocdb::calendarId("nope") == 0, "calendar ids by name");
  check(hocdb::calendarName(1) == "crypto" && hocdb::calendarName(99).empty(), "calendar names");
  check(static_cast<uint32_t>(hocdb::Calendar::Nyse) == 3 && static_cast<uint32_t>(hocdb::Calendar::Cme) == 6, "Calendar enum");
  int64_t fri = utc(2025, 9, 5, 15, 0);
  auto s = hocdb::calendarSession(nyse, fri);
  check(s.has_value() && s->open == utc(2025, 9, 5, 13, 30) && s->close == utc(2025, 9, 5, 20, 0) && !s->early_close
            && s->trade_day == hocdb::daysFromCivil(2025, 9, 5), "NYSE Friday session 13:30-20:00 UTC");
  check(hocdb::calendarIsOpen(nyse, fri) && !hocdb::calendarIsOpen(nyse, utc(2025, 9, 6, 12, 0)), "isOpen");
  int64_t sat = utc(2025, 9, 6, 12, 0);
  check(!hocdb::calendarSession(nyse, sat).has_value(), "Saturday has no session");
  check(hocdb::calendarSession(nyse, sat, hocdb::SessionWhich::Previous)->trade_day == hocdb::daysFromCivil(2025, 9, 5),
        "previous session is Friday");
  check(hocdb::calendarSession(nyse, sat, hocdb::SessionWhich::Next)->trade_day == hocdb::daysFromCivil(2025, 9, 8),
        "next session is Monday");
  check(!hocdb::calendarSessionForDay(nyse, hocdb::daysFromCivil(2025, 7, 4)).has_value(), "Independence Day is closed");
  auto bf = hocdb::calendarSessionForDay(nyse, hocdb::daysFromCivil(2025, 11, 28));
  check(bf->early_close && bf->close == utc(2025, 11, 28, 18, 0), "Black Friday early close");
  check(hocdb::calendarOpenSeconds(nyse, utc(2025, 8, 29, 15, 0), utc(2025, 9, 2, 15, 0)) == 5 * 3600 + 5400,
        "trading seconds across the Labor Day weekend");
  check(hocdb::calendarSessionsBetween(nyse, utc(2025, 1, 1), utc(2026, 1, 1)) == 250, "250 NYSE sessions in 2025");
  check(near(hocdb::calendarPeriodsPerYear(nyse, 60), 252.0 * 390) && near(hocdb::calendarPeriodsPerYear(1, 86400), 365),
        "periods per year");
  check(hocdb::calendarToLocal(nyse, fri) == utc(2025, 9, 5, 11, 0), "UTC -> New York local");
  auto c = hocdb::civilFromDays(hocdb::daysFromCivil(2024, 2, 29));
  check(c.year == 2024 && c.month == 2 && c.day == 29, "civil date round trip");
  check(hocdb::calendarSessionForDay(hocdb::calendarId("fx"), hocdb::daysFromCivil(2025, 9, 8))->open == utc(2025, 9, 7, 21, 0),
        "FX Monday opens Sunday 17:00 New York");
  expectError([&] { return hocdb::calendarSession(999, fri); }, "unknown calendar id", "UnknownCalendar");

  std::array<std::optional<HOCDBDaySession>, 7> weekly{};
  for (int i = 0; i < 4; i++) weekly[i] = HOCDBDaySession{10 * 3600, 15 * 3600};
  std::vector<int32_t> holidays{static_cast<int32_t>(hocdb::daysFromCivil(2025, 9, 9))};
  std::vector<HOCDBEarlyClose> early{{static_cast<int32_t>(hocdb::daysFromCivil(2025, 9, 10)), 12 * 3600}};
  uint32_t cid = hocdb::calendarDefine("cpp_custom", weekly, 9 * 3600, hocdb::DstRule::None, holidays, early, 200);
  check(cid >= 32 && hocdb::calendarId("cpp_custom") == cid, "custom calendar registered");
  check(hocdb::calendarSessionForDay(cid, hocdb::daysFromCivil(2025, 9, 8))->open == utc(2025, 9, 8, 1, 0), "custom Monday 10:00 UTC+9");
  check(!hocdb::calendarSessionForDay(cid, hocdb::daysFromCivil(2025, 9, 9)).has_value(), "custom holiday");
  check(hocdb::calendarSessionForDay(cid, hocdb::daysFromCivil(2025, 9, 10))->close == utc(2025, 9, 10, 3, 0), "custom early close");
  check(!hocdb::calendarSessionForDay(cid, hocdb::daysFromCivil(2025, 9, 12)).has_value(), "custom week has no Friday");
  expectError([&] { return hocdb::calendarDefine("", weekly, 0); }, "calendarDefine with an empty name");
}

// One-minute bars for every minute of the given NYSE trade dates (µs timestamps).
static void fillSessions(hocdb::Database &db, const std::vector<int64_t> &days, double seed,
                         double *first_open_out = nullptr, double *hi_out = nullptr, double *lo_out = nullptr,
                         double *close_out = nullptr, size_t want_day = 0) {
  double p = 100.0 * seed;
  uint32_t nyse = hocdb::calendarId("nyse");
  for (size_t k = 0; k < days.size(); k++) {
    auto s = hocdb::calendarSessionForDay(nyse, days[k]);
    double hi = -1e18, lo = 1e18, last = 0, first_open = 0;
    for (int64_t t = s->open; t < s->close; t += 60) {
      double o = p;
      p *= 1.0 + 0.0007 * std::sin(static_cast<double>(t) / 613.0) + 0.0003 * std::cos(static_cast<double>(t) / 97.0);
      double bh = std::max(o, p) * 1.0005, bl = std::min(o, p) * 0.9995;
      Bar bar{t * US, o, bh, bl, p, 500.0 + static_cast<double>(t % 97)};
      db.append(&bar, sizeof bar);
      if (t == s->open) first_open = o;
      hi = std::max(hi, bh);
      lo = std::min(lo, bl);
      last = p;
    }
    if (k == want_day) {
      if (first_open_out) *first_open_out = first_open;
      if (hi_out) *hi_out = hi;
      if (lo_out) *lo_out = lo;
      if (close_out) *close_out = last;
    }
  }
  db.flush();
}

static void testCalendarDatabase() {
  printf("Database with a trading calendar...\n");
  hocdb::Config bad;
  bad.calendar = 999;
  expectError([&] { return hocdb::Database("BAD", DATA_DIR, schema(), bad); }, "unknown calendar at open", "UnknownCalendar");

  hocdb::Config cfg;
  cfg.calendar = hocdb::calendarId("nyse");
  cfg.timestamp_unit_ns = 1000;
  hocdb::Database db("CAL", DATA_DIR, schema(), cfg);
  check(db.calendar() == 3 && db.timestampUnit() == 1000, "handle reports the calendar and unit");
  check(near(db.periodsPerYear(60 * US), 252.0 * 390) && near(db.periodsPerYear(86400 * US), 252), "handle periodsPerYear");
  int64_t thu = hocdb::daysFromCivil(2025, 9, 4), fri = hocdb::daysFromCivil(2025, 9, 5), tue = hocdb::daysFromCivil(2025, 9, 9);
  double thu_hi = 0, thu_lo = 0, thu_close = 0, fri_open = 0, fri_hi = 0, fri_lo = 0, fri_close = 0;
  {
    // fill and capture Thursday's range, then Friday's
    double p = 100.0;
    uint32_t nyse = hocdb::calendarId("nyse");
    const int64_t days[3] = {thu, fri, tue};
    for (int k = 0; k < 3; k++) {
      auto s = hocdb::calendarSessionForDay(nyse, days[k]);
      double hi = -1e18, lo = 1e18, last = 0, first_open = 0;
      for (int64_t t = s->open; t < s->close; t += 60) {
        double o = p;
        p *= 1.0 + 0.0007 * std::sin(static_cast<double>(t) / 613.0) + 0.0003 * std::cos(static_cast<double>(t) / 97.0);
        double bh = std::max(o, p) * 1.0005, bl = std::min(o, p) * 0.9995;
        Bar bar{t * US, o, bh, bl, p, 500.0 + static_cast<double>(t % 97)};
        db.append(&bar, sizeof bar);
        if (t == s->open) first_open = o;
        hi = std::max(hi, bh);
        lo = std::min(lo, bl);
        last = p;
      }
      if (k == 0) { thu_hi = hi; thu_lo = lo; thu_close = last; }
      if (k == 1) { fri_open = first_open; fri_hi = hi; fri_lo = lo; fri_close = last; }
    }
    db.flush();
  }

  auto s_fri = hocdb::calendarSessionForDay(hocdb::calendarId("nyse"), fri);
  hocdb::IndicatorColumns cols = columns();
  std::vector<hocdb::IndicatorSpec> specs(3);
  specs[0].kind = "session_range";
  specs[1].kind = "pivots";
  specs[2].kind = "session_vwap";
  for (auto &sp : specs) sp.param = 0; // 0 = the database's trading calendar
  hocdb::IndicatorOptions opts;
  opts.columns = cols;
  opts.lookback = size_t{0};
  auto res = db.indicators(specs, (s_fri->open + 100 * 60) * US, s_fri->close * US, opts);
  check(res.n_rows == 290, "window has 290 rows");
  const auto &open_col = res.column("session_range_open");
  check(std::all_of(open_col.begin(), open_col.end(), [&](double v) { return near(v, fri_open, 1e-12); }),
        "session_range open == Friday's first bar even though the window starts later");
  const auto &pp = res.column("pivots_pp");
  check(std::all_of(pp.begin(), pp.end(), [&](double v) { return near(v, (thu_hi + thu_lo + thu_close) / 3.0); }),
        "pivots come from Thursday (previous trading day)");
  const auto &vwap = res.column("session_vwap");
  check(std::all_of(vwap.begin(), vwap.end(), [](double v) { return std::isfinite(v) && v > 0; }), "session vwap defined");

  auto s_tue = hocdb::calendarSessionForDay(hocdb::calendarId("nyse"), tue);
  std::vector<hocdb::IndicatorSpec> pv(1);
  pv[0].kind = "pivots";
  pv[0].param = 0;
  auto res2 = db.indicators(pv, s_tue->open * US, s_tue->close * US, opts);
  check(near(res2.column("pivots_pp")[0], (fri_hi + fri_lo + fri_close) / 3.0), "Tuesday's pivots skip the missing Monday");

  auto h = db.healthMap(0, INT64_MAX, "close", "volume", 5 * 60 * US, 0.2);
  check(h.size() == 19 && h["n_session_breaks"] == 2 && h["n_missing_sessions"] == 1, "health: 19 fields, 2 breaks, 1 missing session");
  check(h["n_gaps"] == 1 && h["max_gap"] == static_cast<double>((60 + 390 * 60) * US) && h["closed_span"] > 0,
        "health: the missing session is the only real gap");

  auto autoSum = db.summary(0, INT64_MAX, "close", 0);
  auto explicitSum = db.summary(0, INT64_MAX, "close", 252.0 * 390);
  check(near(autoSum.ann_vol, explicitSum.ann_vol, 1e-12) && autoSum.ann_vol > 0, "summary annualises from the calendar");

  hocdb::Database plain("PLAIN", DATA_DIR, schema(), hocdb::Config{});
  fillSessions(plain, {thu}, 1.1);
  expectError([&] { return plain.indicatorsTail(10, pv, opts); }, "session kind param 0 without a calendar", "CalendarRequired");
  expectError([&] { plain.setCalendar(999u); }, "setCalendar with an unknown id", "UnknownCalendar");
  expectError([&] { plain.setCalendar(std::string("nope")); }, "setCalendar with an unknown name", "UnknownCalendar");
  plain.setCalendar(std::string("crypto"));
  plain.setTimestampUnit(1000);
  check(plain.calendar() == 1 && near(plain.periodsPerYear(60 * US), 365.0 * 1440), "setCalendar / setTimestampUnit");
}

static void testBacktest() {
  printf("Signal backtester...\n");
  auto d = hocdb::backtestDefaults();
  check(d.initial_equity == 1.0 && d.allow_short == 1 && d.fill_mode == 0, "default params");
  std::vector<int64_t> ts{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  std::vector<double> open{100, 101, 102, 98, 95, 97, 99, 103, 104, 102, 100, 99};
  std::vector<double> high{101, 103, 103, 99, 97, 99, 104, 105, 105, 103, 101, 100};
  std::vector<double> low{99, 100, 96, 93, 94, 96, 98, 102, 101, 99, 98, 97};
  std::vector<double> close{100.5, 102.5, 97, 94, 96.5, 98.5, 103.5, 104.5, 102, 100, 98.5, 99.5};
  std::vector<double> target{1, 1, 1, 1, 1, 2, 2, 2, -1, -1, -1, -1};
  auto p = hocdb::backtestDefaults();
  p.initial_equity = 1000;
  p.cost_bps = 10;
  p.slippage_bps = 5;
  p.stop_loss = 0.05;
  p.periods_per_year = 252;
  hocdb::BacktestOutputs outs;
  outs.equity = outs.position = outs.cash = outs.pnl = outs.drawdown = true;
  outs.max_trades = 8;
  auto r = hocdb::backtestArrays(ts, &open, &high, &low, close, target, &p, outs);
  check(r.result.n_bars == 12 && r.result.n_trades == 3 && r.result.n_long_trades == 2 && r.result.n_short_trades == 1
            && r.result.n_stop_exits == 1, "tied example trade counts");
  check(near(r.result.final_equity, 1002.4465295364876) && near(r.result.max_drawdown, 0.006637024271452185)
            && r.result.max_drawdown_bars == 4 && near(r.result.sharpe, 0.961805823133203)
            && near(r.result.turnover, 0.7010422825639875) && near(r.result.total_cost, 0.7009464760125),
        "tied example statistics");
  const double eq_ref[12] = {1000, 1001.3484495, 995.8484495, 994.7024755365, 994.7024755365, 994.7024755365,
                             1003.4053765365, 1005.4053765365, 1000.4053765365, 1001.9465295365, 1003.4465295365, 1002.4465295365};
  bool eq_ok = r.equity.size() == 12;
  for (size_t i = 0; i < r.equity.size(); i++) eq_ok = eq_ok && near(r.equity[i], eq_ref[i]);
  check(eq_ok, "equity curve");
  check(r.position[6] == 2.0 && r.position[9] == -1.0 && r.drawdown.size() == 12 && r.cash.size() == 12, "per-bar outputs");
  check(r.trades.size() == 3 && r.trades[0].entry_ts == 2 && r.trades[0].exit_ts == 4
            && r.trades[0].exit_reason == static_cast<uint64_t>(hocdb::ExitReason::StopLoss)
            && near(r.trades[0].exit_price, 95.9499760125), "first trade stopped out");
  check(r.trades[2].exit_ts == 0 && r.trades[2].exit_reason == static_cast<uint64_t>(hocdb::ExitReason::EndOfData)
            && r.trades[2].direction == -1, "last trade is still open at the end");
  auto rmap = hocdb::backtestResultMap(r.result);
  auto tmap = hocdb::tradeMap(r.trades[0]);
  check(rmap.size() == 32 && near(rmap["sharpe"], r.result.sharpe) && tmap.size() == 10 && tmap["exit_reason"] == 1,
        "generic result / trade maps");
  auto badp = p;
  badp.position_mode = 9;
  expectError([&] { return hocdb::backtestArrays(ts, &open, &high, &low, close, target, &badp); }, "invalid position_mode");
  std::vector<double> shortTarget(11, 1.0);
  expectError([&] { return hocdb::backtestArrays(ts, &open, &high, &low, close, shortTarget, &p); }, "target length mismatch");

  auto sp = hocdb::walkForwardSplits(100, 4, 0.5, true);
  check(sp.size() == 4 && sp[0].train_start == 0 && sp[0].train_end == 50 && sp[0].test_start == 50 && sp[0].test_end == 62
            && sp[3].test_end == 100, "anchored walk-forward splits");
  auto rolling = hocdb::walkForwardSplits(100, 4, 0.5, false);
  check(rolling[3].train_start == 36 && rolling[3].train_end == 86, "rolling walk-forward splits");
  std::vector<int64_t> rampTs(100);
  std::vector<double> ramp(100), ones(100, 1.0);
  for (int i = 0; i < 100; i++) { rampTs[i] = i + 1; ramp[i] = 100 + 0.25 * i; }
  auto sparams = hocdb::backtestDefaults();
  sparams.fill_mode = 1;
  sparams.position_mode = 1;
  auto results = hocdb::backtestSplits(rampTs, nullptr, nullptr, nullptr, ramp, ones, sp, &sparams);
  bool split_ok = results.size() == 4;
  for (size_t i = 0; i < results.size(); i++) {
    split_ok = split_ok && near(results[i].total_return, ramp[sp[i].test_end - 1] / ramp[sp[i].test_start] - 1.0);
  }
  check(split_ok, "backtestSplits runs every test window independently");

  // database windows
  hocdb::Config cfg;
  cfg.calendar = hocdb::calendarId("crypto");
  cfg.timestamp_unit_ns = 1000000000ULL;
  hocdb::Database db("BT", DATA_DIR, schema(), cfg);
  double price = 100.0;
  for (int i = 0; i < 6000; i++) {
    price *= 1.0 + 0.0006 * std::sin(i * 0.37) + 0.0002 * std::cos(i * 0.11);
    int64_t t = 1700000000LL + static_cast<int64_t>(i) * 7;
    Bar b{t, price, price * 1.001, price * 0.999, price, 10.0 + (i % 5)};
    db.append(&b, sizeof b);
  }
  db.flush();
  int64_t start = 1700000100LL, end = start + 6 * 3600;
  auto bars = db.ohlcv(start, end, 300, "close", "volume");
  size_t n = bars.timestamps.size();
  check(n == 72, "6 hours of 5-minute bars");
  std::vector<double> dtarget(n, 0.0);
  for (size_t i = 5; i < n; i++) dtarget[i] = bars.close[i] > bars.close[i - 5] ? 1.0 : -1.0;
  auto dp = hocdb::backtestDefaults();
  dp.initial_equity = 10000;
  dp.cost_bps = 5;
  dp.position_mode = static_cast<uint64_t>(hocdb::PositionMode::Fraction);
  hocdb::IndicatorColumns cols = columns();
  hocdb::BacktestOutputs douts;
  douts.equity = true;
  douts.max_trades = n;
  auto dres = db.backtest(dtarget, start, end, 300, &dp, douts, &cols);
  auto kp = dp;
  kp.periods_per_year = 365.0 * 288.0; // the handle derives this from its crypto calendar
  auto kres = hocdb::backtestArrays(bars.timestamps, &bars.open, &bars.high, &bars.low, bars.close, dtarget, &kp);
  check(dres.result.n_bars == n && dres.result.final_equity == kres.result.final_equity
            && dres.result.sharpe == kres.result.sharpe && dres.result.ann_vol > 0,
        "db backtest == kernel on ohlcv bars with the calendar's periods_per_year");
  check(dres.equity.size() == n && dres.trades.size() == dres.result.n_trades, "db backtest outputs and trade list");
  std::vector<double> tail(dtarget.end() - 20, dtarget.end());
  auto tres = db.backtestTail(tail, 300, &dp, {}, &cols);
  check(tres.result.n_bars == 20, "backtestTail window");
  std::vector<double> shortT(dtarget.begin(), dtarget.end() - 1);
  expectError([&] { return db.backtest(shortT, start, end, 300, &dp, {}, &cols); }, "db target length mismatch");
}

static void testUniverse() {
  printf("Universe features...\n");
  auto d = hocdb::universeDefaults();
  check(d.mom_short == 5 && d.mom_mid == 20 && d.corr_period == 60, "default params");
  const size_t n = 120;
  std::vector<double> c0(n), c1(n), v0(n);
  std::vector<int64_t> ts(n);
  for (size_t i = 0; i < n; i++) {
    double x = static_cast<double>(i);
    c0[i] = 100 + 5 * std::sin(x * 0.2) + 0.1 * x;
    c1[i] = 50 + 3 * std::cos(x * 0.15) - 0.05 * x;
    v0[i] = 1000 + static_cast<double>(i % 7) * 10;
    ts[i] = 1000 + static_cast<int64_t>(i) * 60;
  }
  auto params = hocdb::universeDefaults();
  params.mom_long = 30;
  params.corr_period = 30;
  params.beta_period = 30;
  params.sma_period = 20;
  std::vector<std::vector<double>> closes{c0, c1, c0};
  std::vector<std::vector<double>> volumes{v0, std::vector<double>(n, 2000.0), std::vector<double>(n, 500.0)};
  auto u = hocdb::universeArrays(closes, &volumes, &ts, &params, true);
  check(u.summary.n_tickers == 3 && u.summary.n_bars == n && u.summary.first_ts == 1000 && u.summary.last_ts == ts[n - 1],
        "summary basics");
  check(near(u.correlation(0, 2), 1.0) && near(u.correlation(2, 0), 1.0) && u.correlation(0, 0) == 1.0
            && near(u.correlation(0, 1), u.correlation(1, 0)), "correlation matrix");
  check(u.rows[0].max_corr_index == 2 && u.rows[2].max_corr_index == 0, "most correlated partner");
  check(u.rows[0].rank_mom_mid == u.rows[2].rank_mom_mid && std::isfinite(u.rows[1].beta), "ranks tie; beta defined");
  check(hocdb::universeRowMap(u.rows[0]).size() == 21 && hocdb::universeSummaryMap(u.summary).size() == 16,
        "21 row fields, 16 summary fields");
  auto noVol = hocdb::universeArrays(closes, nullptr, nullptr, &params, false);
  check(std::isnan(noVol.rows[0].volume_ratio) && noVol.summary.first_ts == 0 && noVol.corr.empty(),
        "without volumes / timestamps / correlation matrix");

  std::vector<hocdb::Database> dbs;
  dbs.reserve(3);
  for (int k = 0; k < 3; k++) {
    dbs.emplace_back("U" + std::to_string(k), DATA_DIR, schema(), hocdb::Config{});
    const std::vector<double> &series = (k == 1) ? c1 : c0;
    for (size_t i = 0; i < n; i++) {
      if (k == 2 && i % 7 == 6) continue; // this ticker is missing every 7th bar
      Bar b{ts[i], series[i], series[i], series[i], series[i], v0[i]};
      dbs.back().append(&b, sizeof b);
    }
    dbs.back().flush();
  }
  std::vector<const hocdb::Database *> refs{&dbs[0], &dbs[1], &dbs[2]};
  hocdb::IndicatorColumns cols = columns();
  auto got = hocdb::universe(refs, &cols, n, 0, &params, true);
  std::vector<size_t> joined;
  for (size_t i = 0; i < n; i++) {
    if (i % 7 != 6) joined.push_back(i);
  }
  check(got.summary.n_bars == joined.size() && got.summary.last_ts == ts[joined.back()],
        "inner join over 3 databases");
  std::vector<std::vector<double>> jc(3), jv(3);
  std::vector<int64_t> jts;
  for (size_t idx : joined) {
    jc[0].push_back(c0[idx]);
    jc[1].push_back(c1[idx]);
    jc[2].push_back(c0[idx]);
    for (int k = 0; k < 3; k++) jv[k].push_back(v0[idx]);
    jts.push_back(ts[idx]);
  }
  auto hand = hocdb::universeArrays(jc, &jv, &jts, &params, true);
  bool same = true;
  for (size_t i = 0; i < 3; i++) {
    auto a = hocdb::universeRowMap(got.rows[i]);
    auto b = hocdb::universeRowMap(hand.rows[i]);
    for (const auto &kv : b) same = same && near(a[kv.first], kv.second);
  }
  check(same, "db join == hand-joined arrays (all row fields)");
  auto autoBars = hocdb::universe(refs, &cols, 0, 0, &params, false);
  check(autoBars.summary.n_bars >= 31, "n_bars 0 reads enough bars for the longest period");
  expectError([&] { return hocdb::universe({}, &cols); }, "universe with no databases");
}

int main() {
  std::string rm = std::string("rm -rf ") + DATA_DIR;
  if (system(rm.c_str()) != 0) { /* the directory may not exist */ }
  try {
    testCalendars();
    testCalendarDatabase();
    testBacktest();
    testUniverse();
  } catch (const std::exception &e) {
    printf("  FAIL: unexpected exception: %s\n", e.what());
    failures++;
  }
  if (system(rm.c_str()) != 0) { /* ignore */ }
  if (failures) {
    printf("C++ round-4 test FAILED (%d)\n", failures);
    return 1;
  }
  printf("C++ round 4 test passed\n");
  return 0;
}
