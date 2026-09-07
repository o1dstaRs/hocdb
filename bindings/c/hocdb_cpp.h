#ifndef HOCDB_CPP_H
#define HOCDB_CPP_H

#include "hocdb.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <array>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace hocdb {

/**
 * @brief Exception class for HOCDB errors
 */
class Exception : public std::runtime_error {
public:
  explicit Exception(const std::string &message)
      : std::runtime_error(message) {}
};

/**
 * @brief Field definition for schema
 */
struct Field {
  std::string name;
  int type; // 1=i64, 2=f64, 3=u64
};

/* ------------------------------------------------------------------------- */
/* Durability, readers, maintenance and metrics                              */
/* ------------------------------------------------------------------------- */

/**
 * @brief When the writer fsyncs its data file (Config::fsync).
 */
enum class FsyncPolicy {
  None = 0,     /**< never: the OS decides when data reaches the disk */
  OnClose = 1,  /**< once when the database is closed (default) */
  OnFlush = 2,  /**< after every flush (commit) */
  Interval = 3, /**< at most every Config::fsync_interval_ms, and on close */
};

/**
 * @brief Full database configuration of a writer (the C HOCDBConfig passed to
 * hocdb_init_ex). Every field has a default, so
 * `hocdb::Config cfg; cfg.fsync = hocdb::FsyncPolicy::OnFlush;` is all that
 * is needed to change one option.
 *
 * Ring-buffer capacity: a file holds exactly N records when
 * `max_file_size = Database::headerSize() (64) + N * record_size`.
 */
struct Config {
  int64_t max_file_size = 0;      /**< bytes; 0 = default (2 GiB) */
  bool overwrite_on_full = false; /**< ring buffer: overwrite the oldest records when full */
  bool flush_on_write = false;    /**< commit after every append */
  bool auto_increment = false;    /**< the database assigns the timestamps */
  FsyncPolicy fsync = FsyncPolicy::OnClose;
  uint32_t fsync_interval_ms = 0; /**< FsyncPolicy::Interval period; 0 = 1000 */
  bool verify_on_open = false;    /**< recompute the CRC32C on open; the open fails with "ChecksumMismatch" */
  int64_t retention_span = 0;     /**< keep only the last span (timestamp units); compaction runs once the excess exceeds 25%; 0 = off */
  uint64_t rollover_size = 0;     /**< archive the file once it exceeds this many bytes; 0 = off */
  bool auto_migrate = true;       /**< rewrite legacy HOC1 files in place the first time a writer opens them */
  uint64_t timestamp_unit_ns = 0; /**< nanoseconds per timestamp unit, for the ingest_lag_record_ns metric; 0 = unknown */
  uint64_t index_stride = 0;      /**< records per index entry; 0 = default 1024 */
  uint32_t calendar = 0;          /**< trading calendar id (hocdb_calendar_id), 0 = none; built-in ids are persisted */
};

/* ------------------------------------------------------------------------- */
/* Technical indicators and quantitative analytics                            */
/* ------------------------------------------------------------------------- */

/**
 * @brief One indicator request.
 *
 * `kind` is an indicator name such as "sma", "rsi" or "macd" (case-insensitive,
 * resolved at runtime; see INDICATORS.md for the full table). Zero periods /
 * params select the documented defaults (RSI 14, MACD 12/26/9, BBANDS 20 x
 * 2.0, ...).
 *
 * `param` is the BBANDS k, the KELTNER/SUPERTREND multiplier, the PSAR
 * acceleration, the periods-per-year for hist_vol/sharpe/sortino/realized_vol,
 * the timestamp units per second for trade_intensity (default 1e6), the
 * up-barrier fraction for triple_barrier (default 0.02) and the session length
 * (timestamp units, MANDATORY) for session_vwap/session_range/opening_range/
 * pivots. `param2` is the PSAR max acceleration, the triple_barrier
 * down-barrier fraction (default = up) or the session offset.
 *
 * `field` runs a single-series indicator on that DB field instead of the
 * close column ("" = close); `field2` is the second series (benchmark) for
 * correl/beta/series2/ratio/ratio_zscore/rel_strength in single-database
 * calls (in pairIndicators() the second series is always the other
 * database). `label` overrides the output column name.
 *
 * forward_return and triple_barrier are labels: they use FUTURE rows and
 * are NaN at the end of every window (see Database::indicatorIsLookahead).
 */
struct IndicatorSpec {
  std::string kind;
  uint32_t period = 0;
  uint32_t period2 = 0;
  uint32_t period3 = 0;
  uint32_t period4 = 0;
  double param = 0;
  double param2 = 0;
  std::string field{};
  std::string field2{};
  std::string label{};
};

/**
 * @brief Column roles given as field names ("" = not available).
 * `close` is required; the other roles are only needed by the indicators
 * that use them (e.g. ATR needs high/low, OBV needs volume, spread needs
 * bid/ask, order_flow needs volume + side). `side` is 1 = buy, 0 = sell;
 * with bucket > 0 it also yields per-bar buy volume.
 */
struct IndicatorColumns {
  std::string open{};
  std::string high{};
  std::string low{};
  std::string close{};
  std::string volume{};
  std::string bid{};
  std::string ask{};
  std::string side{};
};

/**
 * @brief Options shared by indicators() and indicatorsTail().
 */
struct IndicatorOptions {
  /** Column roles. nullopt auto-detects fields literally named
   * open/high/low/close/volume/bid/ask/side; a field named "price" is used as
   * close when the schema has no "close", and "size" or "qty" as volume when
   * there is no "volume". */
  std::optional<IndicatorColumns> columns;
  /** Extra records (bars when bucket > 0) read before the window so that the
   * first in-window values are converged. nullopt = recommended per-spec
   * warm-up (HOCDB_LOOKBACK_AUTO); 0 = none (NaN warm-up inside the window). */
  std::optional<size_t> lookback;
  /** 0 = one row per record; > 0 = records are first aggregated into OHLCV
   * bars of that many timestamp units (tick -> bar). Per-spec `field`
   * overrides are not allowed with bucket > 0. */
  int64_t bucket = 0;
};

/**
 * @brief Options of pairIndicators() / pairIndicatorsTail(): the base
 * options apply to THIS database; `other_columns` are the column roles of
 * the other database (nullopt = auto-detect there, like `columns`).
 */
struct PairOptions : IndicatorOptions {
  std::optional<IndicatorColumns> other_columns;
};

/**
 * @brief Result of indicators() / indicatorsTail(). `outputs[k]` is the series
 * named `names[k]`; every series holds `n_rows` values aligned with
 * `timestamps`. NaN marks the warm-up region (value not yet defined).
 */
struct IndicatorResult {
  std::vector<int64_t> timestamps;
  std::vector<std::string> names;
  std::vector<std::vector<double>> outputs;
  size_t n_rows = 0;

  /**
   * @brief Series by output name (e.g. "sma_20", "macd_signal")
   * @throws Exception when no output has that name
   */
  const std::vector<double> &column(const std::string &name) const {
    for (size_t i = 0; i < names.size(); ++i) {
      if (names[i] == name) {
        return outputs[i];
      }
    }
    throw Exception("Unknown indicator output column: " + name);
  }
};

/**
 * @brief OHLCV bars returned by ohlcv(). `volume` is the record count per bar
 * when no volume field was given; `count` is always the record count.
 * `buy_volume` (volume of the records whose side is 1) is filled only when a
 * side field was given and is empty otherwise.
 */
struct Bars {
  std::vector<int64_t> timestamps;
  std::vector<double> open;
  std::vector<double> high;
  std::vector<double> low;
  std::vector<double> close;
  std::vector<double> volume;
  std::vector<double> count;
  std::vector<double> buy_volume;
};

/**
 * @brief One trading decision for evaluate(): entry at the first price at or
 * after `timestamp`, exit at the first price at or after
 * `timestamp + horizon`.
 */
struct Decision {
  int64_t timestamp;
  double direction;    /**< +1 long, -1 short, 0 flat (counted, not evaluated) */
  double size = 1;     /**< position size in currency units */
  int64_t horizon = 0; /**< timestamp units; 0 = the call's default_horizon */
};

/**
 * @brief Result of evaluate(): the aggregate statistics (see HOCDBEvaluation
 * in hocdb.h, or Database::evaluationMap) plus one entry price, exit price
 * and net return per decision (NaN where the decision was not evaluated).
 */
struct EvaluationResult {
  HOCDBEvaluation stats{};
  std::vector<double> entry;
  std::vector<double> exit;
  std::vector<double> net_return;
};

/**
 * @brief C++ wrapper class for the HOCDB database
 */
/* ------------------------------------------------------------------------- */
/* Trading calendars (round 4)                                                */
/* ------------------------------------------------------------------------- */

/** Built-in calendar ids (calendarId(name) also resolves custom ones). */
enum class Calendar : uint32_t {
  None = HOCDB_CALENDAR_NONE,
  Crypto = HOCDB_CALENDAR_CRYPTO,
  Fx = HOCDB_CALENDAR_FX,
  Nyse = HOCDB_CALENDAR_NYSE,
  Nasdaq = HOCDB_CALENDAR_NASDAQ,
  Lse = HOCDB_CALENDAR_LSE,
  Cme = HOCDB_CALENDAR_CME,
};

/** Daylight-saving rule of a custom calendar. */
enum class DstRule : int {
  None = HOCDB_DST_NONE,
  Us = HOCDB_DST_US,
  Eu = HOCDB_DST_EU,
};

/** One resolved trading session; all times are UTC seconds. */
struct Session {
  int64_t open = 0;       /**< UTC seconds, inclusive */
  int64_t close = 0;      /**< UTC seconds, exclusive */
  int64_t trade_day = 0;  /**< days since 1970-01-01 (local trade date) */
  bool early_close = false;
};

/** Which session calendarSession() looks up. */
enum class SessionWhich : int { At = 0, Previous = 1, Next = 2 };

/** A civil date (proleptic Gregorian). */
struct CivilDate {
  int64_t year = 0;
  unsigned month = 0;
  unsigned day = 0;
};

/** Id of a built-in or custom calendar by name; 0 when unknown. */
inline uint32_t calendarId(const std::string &name) {
  return hocdb_calendar_id(name.c_str());
}

/** Name of a calendar id, empty when unknown. */
inline std::string calendarName(uint32_t id) {
  char buf[64];
  int n = hocdb_calendar_name(id, buf, sizeof buf);
  return n > 0 ? std::string(buf, static_cast<size_t>(n)) : std::string();
}

namespace detail {
inline Session sessionFromC(const HOCDBSession &s) {
  Session out;
  out.open = s.open;
  out.close = s.close;
  out.trade_day = s.trade_day;
  out.early_close = s.early_close != 0;
  return out;
}

[[noreturn]] inline void throwUnknownCalendar(const char *what) {
  throw Exception(std::string(what) +
                  " failed: UnknownCalendar (no calendar with that id; "
                  "built-in ids 1-6, custom ones come from calendarDefine())");
}
} // namespace detail

/**
 * @brief The session containing `utc_sec` (At), that or the previous one
 * (Previous), or that or the next one (Next).
 * @return the session, or std::nullopt when there is none
 * @throws Exception for an unknown calendar id
 */
inline std::optional<Session>
calendarSession(uint32_t calendar, int64_t utc_sec,
                SessionWhich which = SessionWhich::At) {
  HOCDBSession s{};
  int rc = hocdb_calendar_session(calendar, utc_sec, static_cast<int>(which), &s);
  if (rc < 0) {
    detail::throwUnknownCalendar("calendarSession");
  }
  if (rc == 0) {
    return std::nullopt;
  }
  return detail::sessionFromC(s);
}

/** @brief The session of a trade date (days since 1970-01-01, local). */
inline std::optional<Session> calendarSessionForDay(uint32_t calendar,
                                                    int64_t day) {
  HOCDBSession s{};
  int rc = hocdb_calendar_session_for_day(calendar, day, &s);
  if (rc < 0) {
    detail::throwUnknownCalendar("calendarSessionForDay");
  }
  if (rc == 0) {
    return std::nullopt;
  }
  return detail::sessionFromC(s);
}

/** @brief Whether the market is open at `utc_sec`. */
inline bool calendarIsOpen(uint32_t calendar, int64_t utc_sec) {
  int rc = hocdb_calendar_is_open(calendar, utc_sec);
  if (rc < 0) {
    detail::throwUnknownCalendar("calendarIsOpen");
  }
  return rc == 1;
}

/** @brief Seconds of trading time inside [a, b). */
inline int64_t calendarOpenSeconds(uint32_t calendar, int64_t a, int64_t b) {
  int64_t v = hocdb_calendar_open_seconds(calendar, a, b);
  if (v < 0) {
    detail::throwUnknownCalendar("calendarOpenSeconds");
  }
  return v;
}

/** @brief Number of sessions opening inside [a, b). */
inline int64_t calendarSessionsBetween(uint32_t calendar, int64_t a, int64_t b) {
  int64_t v = hocdb_calendar_sessions_between(calendar, a, b);
  if (v < 0) {
    detail::throwUnknownCalendar("calendarSessionsBetween");
  }
  return v;
}

/** @brief Bars per year for bars of `bucket_sec` seconds (0 when unknown). */
inline double calendarPeriodsPerYear(uint32_t calendar, double bucket_sec) {
  return hocdb_calendar_periods_per_year(calendar, bucket_sec);
}

/** @brief Local wall-clock seconds of a UTC instant (with daylight saving). */
inline int64_t calendarToLocal(uint32_t calendar, int64_t utc_sec) {
  return hocdb_calendar_to_local(calendar, utc_sec);
}

/** @brief Days since 1970-01-01 of a civil date. */
inline int64_t daysFromCivil(int64_t year, unsigned month, unsigned day) {
  return hocdb_days_from_civil(year, month, day);
}

/** @brief The civil date of a day number. */
inline CivilDate civilFromDays(int64_t days) {
  CivilDate c;
  hocdb_civil_from_days(days, &c.year, &c.month, &c.day);
  return c;
}

/**
 * @brief Register a custom, process-local calendar (redefining a name reuses
 * its id).
 * @param weekly 7 entries, Monday first; std::nullopt = no session that day.
 *   Times are local seconds relative to the trade date's midnight; the open
 *   may be negative for sessions starting the evening before.
 * @param utc_offset_sec standard (non-DST) offset, seconds east of UTC
 * @param holidays full closures as day numbers (see daysFromCivil)
 * @return the new calendar id
 * @throws Exception on invalid input or when the registry (32) is full
 */
inline uint32_t
calendarDefine(const std::string &name,
               const std::array<std::optional<HOCDBDaySession>, 7> &weekly,
               int32_t utc_offset_sec = 0, DstRule dst = DstRule::None,
               const std::vector<int32_t> &holidays = {},
               const std::vector<HOCDBEarlyClose> &early_closes = {},
               double sessions_per_year = 252.0) {
  HOCDBDaySession week[7];
  for (size_t i = 0; i < 7; ++i) {
    week[i] = weekly[i] ? *weekly[i] : HOCDBDaySession{0, 0};
  }
  int64_t id = hocdb_calendar_define(
      name.c_str(), week, utc_offset_sec, static_cast<int>(dst),
      holidays.empty() ? nullptr : holidays.data(), holidays.size(),
      early_closes.empty() ? nullptr : early_closes.data(), early_closes.size(),
      sessions_per_year);
  if (id <= 0) {
    throw Exception("calendarDefine failed: " +
                    std::string(id == 0 ? "invalid definition (name, weekly "
                                          "template or sessions_per_year)"
                                        : "the calendar registry is full"));
  }
  return static_cast<uint32_t>(id);
}

/* ------------------------------------------------------------------------- */
/* Signal backtester and universe features (round 4)                          */
/* ------------------------------------------------------------------------- */

/** Unit of the backtester's target values. */
enum class PositionMode : uint64_t { Units = 0, Fraction = 1, Notional = 2 };
/** When a position change is executed. */
enum class FillMode : uint64_t { NextOpen = 0, SameClose = 1 };
/** Why a trade was closed (Trade::exit_reason). */
enum class ExitReason : uint64_t {
  Signal = 0,
  StopLoss = 1,
  TakeProfit = 2,
  Trailing = 3,
  EndOfData = 4,
};

/** Which per-bar columns a backtest should return, and how many trades. */
struct BacktestOutputs {
  bool equity = false;
  bool position = false;
  bool cash = false;
  bool pnl = false;
  bool drawdown = false;
  size_t max_trades = 0; /**< 0 = no trade list (n_trades still counts all) */
};

/** Everything one backtest produced. */
struct BacktestReport {
  HOCDBBacktestResult result{};
  std::vector<HOCDBTrade> trades;
  std::vector<double> equity, position, cash, pnl, drawdown;
};

/** Defaults of the backtester (initial equity 1, no costs, next-open fills). */
inline HOCDBBacktestParams backtestDefaults() {
  HOCDBBacktestParams p{};
  hocdb_backtest_params_default(&p);
  return p;
}

/** Defaults of the universe features (5 / 20 / 60 bar momentum, ...). */
inline HOCDBUniverseParams universeDefaults() {
  HOCDBUniverseParams p{};
  hocdb_universe_params_default(&p);
  return p;
}

/** What universe() / universeArrays() produced. */
struct UniverseReport {
  HOCDBUniverseSummary summary{};
  std::vector<HOCDBUniverseRow> rows; /**< one per ticker, in input order */
  std::vector<double> corr;           /**< n x n row-major, empty when not asked */

  /** Correlation between tickers i and j (NaN when the matrix was not asked). */
  double correlation(size_t i, size_t j) const {
    size_t n = rows.size();
    if (corr.size() != n * n || i >= n || j >= n) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return corr[i * n + j];
  }
};

class Database {
private:
  HOCDBHandle handle_;
  size_t record_size_;
  std::map<std::string, size_t> field_map_;

public:
  /**
   * @brief Open (or create) a database as its WRITER with the basic options.
   * Same as the Config constructor with these four fields set; every other
   * option keeps its default (fsync on close, auto_migrate on, ...).
   * @param ticker Ticker symbol (data file `<path>/<ticker>.bin`)
   * @param path Directory path for data
   * @param schema Vector of Field definitions
   * @param max_file_size Maximum file size (0 for default)
   * @param overwrite_on_full Whether to overwrite when full (ring buffer)
   * @param flush_on_write Whether to flush on every write
   * @param auto_increment Whether to auto-increment timestamp
   * @throws Exception if the open fails; the message ends with the engine's
   * error name ("DatabaseLocked" when another writer holds the file,
   * "SchemaMismatch", "ChecksumMismatch", ...)
   */
  Database(const std::string &ticker, const std::string &path,
           const std::vector<Field> &schema, int64_t max_file_size = 0,
           bool overwrite_on_full = true, bool flush_on_write = false,
           bool auto_increment = false)
      : handle_(nullptr), record_size_(0) {
    Config config;
    config.max_file_size = max_file_size;
    config.overwrite_on_full = overwrite_on_full;
    config.flush_on_write = flush_on_write;
    config.auto_increment = auto_increment;
    open(ticker, path, schema, &config);
  }

  /**
   * @brief Open (or create) a database as its WRITER with the full
   * configuration (hocdb_init_ex). A writer holds an exclusive lock on the
   * file: a second writer on the same ticker / path fails immediately with
   * "DatabaseLocked" instead of blocking. Legacy HOC1 files are migrated in
   * place when config.auto_migrate is true (default).
   * @throws Exception if the open fails; the message ends with the engine's
   * error name ("DatabaseLocked", "SchemaMismatch", "ChecksumMismatch",
   * "LegacyFormatNeedsMigration", ...)
   */
  Database(const std::string &ticker, const std::string &path,
           const std::vector<Field> &schema, const Config &config)
      : handle_(nullptr), record_size_(0) {
    open(ticker, path, schema, &config);
  }

  /**
   * @brief Attach as a lock-free READER to a database another process (or
   * another handle in this process) writes. The reader takes no lock; every
   * read method re-reads the writer's committed cursor, so it sees exactly
   * the data the writer has flushed (refresh() does that explicitly). It
   * follows compaction and rollover automatically. Writes (append, flush,
   * sync, compact, retainLast, rollover) throw an Exception saying the
   * handle is read-only. Requires the current file format: legacy files
   * are migrated the first time a writer opens them.
   * @throws Exception if the open fails (message includes the error name)
   */
  static Database openReader(const std::string &ticker,
                             const std::string &path,
                             const std::vector<Field> &schema) {
    return Database(ReaderTag{}, ticker, path, schema);
  }

  /**
   * @brief Destructor - closes the database
   */
  ~Database() {
    if (handle_) {
      hocdb_close(handle_);
    }
  }

  /**
   * @brief Move constructor
   */
  Database(Database &&other) noexcept
      : handle_(other.handle_), record_size_(other.record_size_),
        field_map_(std::move(other.field_map_)) {
    other.handle_ = nullptr;
  }

  /**
   * @brief Move assignment operator
   */
  Database &operator=(Database &&other) noexcept {
    if (this != &other) {
      if (handle_) {
        hocdb_close(handle_);
      }
      handle_ = other.handle_;
      record_size_ = other.record_size_;
      field_map_ = std::move(other.field_map_);
      other.handle_ = nullptr;
    }
    return *this;
  }

  /**
   * @brief Copy constructor is deleted
   */
  Database(const Database &) = delete;

  /**
   * @brief Copy assignment operator is deleted
   */
  Database &operator=(const Database &) = delete;

  /**
   * @brief Append a raw record to the database
   * @param data Pointer to data
   * @param len Length of data
   * @throws Exception if append fails or length mismatch
   */
  void append(const void *data, size_t len) {
    if (len != record_size_) {
      throw Exception("Data length mismatch with schema record size");
    }
    int res = hocdb_append(handle_, data, len);
    if (res != 0) {
      if (res == -2)
        throw Exception("Append failed: Invalid Record Size");
      if (res == -3)
        throw Exception("Append failed: Timestamp Not Monotonic - timestamps "
                        "must be strictly increasing");
      if (res == -10)
        throw readOnlyError("append");
      throw Exception("Failed to append record to HOCDB");
    }
  }

  /**
   * @brief Append a struct to the database (template)
   * @param record The struct to append
   * @throws Exception if append fails or size mismatch
   */
  template <typename T> void append(const T &record) {
    append(&record, sizeof(T));
  }

  /**
   * @brief Flush the database (force write to disk)
   * @throws Exception if flush fails
   */
  void flush() {
    if (hocdb_flush(handle_) != 0) {
      if (hocdb_is_read_only(handle_) == 1) {
        throw readOnlyError("flush");
      }
      throw Exception("Failed to flush HOCDB");
    }
  }

  /**
   * @brief Load all records into memory with zero-copy
   * @return std::pair containing pointer to raw bytes and total length in bytes
   *
   * IMPORTANT: The returned pointer is valid only until the next operation on
   * the database or until the database is closed. The caller is responsible for
   * calling free_data() to free the memory.
   */
  std::vector<uint8_t> load() {
    size_t len = 0;
    void *data = hocdb_load(handle_, &len);
    if (!data && len > 0) {
      throw Exception("Failed to load data from HOCDB");
    }
    if (!data && len == 0) {
      return {};
    }

    std::vector<uint8_t> result(static_cast<uint8_t *>(data),
                                static_cast<uint8_t *>(data) + len);
    hocdb_free(data);
    return result;
  }

  /**
   * @brief Query records in a time range with optional filters
   * @param start_ts Start timestamp
   * @param end_ts End timestamp
   * @param filters Vector of HOCDBFilter structs to apply
   * @return std::vector<uint8_t> containing the raw bytes of the matching
   * records
   */
  /**
   * @brief Query records in a time range with optional filters
   * @param start_ts Start timestamp
   * @param end_ts End timestamp
   * @param filters Vector of HOCDBFilter structs to apply
   * @return std::vector<uint8_t> containing the raw bytes of the matching
   * records
   */
  std::vector<uint8_t> query(int64_t start_ts, int64_t end_ts,
                             const std::vector<HOCDBFilter> &filters = {}) {
    size_t out_len = 0;
    const HOCDBFilter *filters_ptr = filters.empty() ? nullptr : filters.data();
    void *data = hocdb_query(handle_, start_ts, end_ts, filters_ptr,
                             filters.size(), &out_len);
    if (!data) {
      return {}; // Return empty vector on failure or empty result
    }

    // Copy data to vector
    std::vector<uint8_t> result(static_cast<uint8_t *>(data),
                                static_cast<uint8_t *>(data) + out_len);

    // Free C memory
    hocdb_free(data);

    return result;
  }

  /**
   * @brief Query records using a map of field names to values
   * @param start_ts Start timestamp
   * @param end_ts End timestamp
   * @param filters Map of field name to value (variant: i64, f64, u64, string)
   */
  using FilterValue =
      std::variant<int64_t, double, uint64_t, std::string, bool>;
  std::vector<uint8_t>
  query(int64_t start_ts, int64_t end_ts,
        const std::map<std::string, FilterValue> &filters) {
    std::vector<HOCDBFilter> c_filters;
    c_filters.reserve(filters.size());

    for (const auto &[name, val] : filters) {
      auto it = field_map_.find(name);
      if (it == field_map_.end()) {
        throw Exception("Unknown field in filter: " + name);
      }

      HOCDBFilter f;
      f.field_index = it->second;

      if (std::holds_alternative<int64_t>(val)) {
        f.type = HOCDB_TYPE_I64;
        f.val_i64 = std::get<int64_t>(val);
      } else if (std::holds_alternative<double>(val)) {
        f.type = HOCDB_TYPE_F64;
        f.val_f64 = std::get<double>(val);
      } else if (std::holds_alternative<uint64_t>(val)) {
        f.type = HOCDB_TYPE_U64;
        f.val_u64 = std::get<uint64_t>(val);
      } else if (std::holds_alternative<std::string>(val)) {
        f.type = HOCDB_TYPE_STRING;
        std::string s = std::get<std::string>(val);
        strncpy(f.val_string, s.c_str(), 127);
        f.val_string[127] = '\0';
      } else if (std::holds_alternative<bool>(val)) {
        f.type = HOCDB_TYPE_BOOL;
        f.val_bool = std::get<bool>(val);
      }
      c_filters.push_back(f);
    }

    return query(start_ts, end_ts, c_filters);
  }

  /**
   * @brief Get statistics for a specific field within a time range.
   * @param start_ts Start timestamp
   * @param end_ts End timestamp
   * @param field_index Index of the field to get statistics for
   * @return HOCDBStats struct containing min, max, sum, count, and avg
   * @throws std::runtime_error if getting stats fails
   */
  HOCDBStats getStats(int64_t start_ts, int64_t end_ts, size_t field_index,
                      bool compute_percentiles = false) {
    HOCDBStats stats;
    uint32_t flags = compute_percentiles ? HOCDB_STATS_PERCENTILES : 0;
    if (hocdb_get_stats(handle_, start_ts, end_ts, field_index, flags,
                        &stats) != 0) {
      throw std::runtime_error("getStats failed");
    }
    return stats;
  }

  /**
   * @brief Get the latest value and timestamp for a specific field.
   * @param field_index Index of the field to get the latest value for
   * @return std::pair containing the latest value (double) and its timestamp
   * (int64_t)
   * @throws std::runtime_error if getting the latest value fails
   */
  std::pair<double, int64_t> getLatest(size_t field_index) {
    double val;
    int64_t ts;
    if (hocdb_get_latest(handle_, field_index, &val, &ts) != 0) {
      throw std::runtime_error("getLatest failed");
    }
    return {val, ts};
  }

  /**
   * @brief Get statistics for a specific field by name within a time range.
   * @param start_ts Start timestamp
   * @param end_ts End timestamp
   * @param field_name Name of the field to get statistics for
   * @return HOCDBStats struct containing min, max, sum, count, and avg
   * @throws Exception if field not found or getting stats fails
   */
  HOCDBStats getStats(int64_t start_ts, int64_t end_ts,
                      const std::string &field_name,
                      bool compute_percentiles = false) {
    auto it = field_map_.find(field_name);
    if (it == field_map_.end()) {
      throw Exception("Unknown field: " + field_name);
    }
    return getStats(start_ts, end_ts, it->second, compute_percentiles);
  }

  /**
   * @brief Get the latest value and timestamp for a specific field by name.
   * @param field_name Name of the field to get the latest value for
   * @return std::pair containing the latest value (double) and its timestamp
   * (int64_t)
   * @throws Exception if field not found or getting the latest value fails
   */
  std::pair<double, int64_t> getLatest(const std::string &field_name) {
    auto it = field_map_.find(field_name);
    if (it == field_map_.end()) {
      throw Exception("Unknown field: " + field_name);
    }
    return getLatest(it->second);
  }

  /**
   * @brief Free memory allocated by load()
   * @param ptr Pointer returned by load()
   */
  void free_data(void *ptr) { hocdb_free(ptr); }

  /**
   * @brief Closes the database handle (explicit close)
   */
  void close() {
    if (handle_) {
      hocdb_close(handle_);
      handle_ = nullptr;
    }
  }

  /**
   * @brief Close the database and delete all data files
   */
  void drop() {
    if (handle_) {
      hocdb_drop(handle_);
      handle_ = nullptr;
    }
  }

  /**
   * @brief Check if the database handle is valid
   */
  bool is_valid() const { return handle_ != nullptr; }

  /**
   * @brief Get the underlying handle (for advanced usage)
   */
  HOCDBHandle get_handle() const { return handle_; }

  size_t get_record_size() const { return record_size_; }

  /* ----------------------------------------------------------------------- */
  /* Durability, readers, maintenance and metrics                             */
  /* ----------------------------------------------------------------------- */

  /**
   * @brief Bytes reserved by the file header (64 for the current format).
   * Ring-buffer capacity: max_file_size = headerSize() + N * record_size.
   */
  static size_t headerSize() { return hocdb_header_size(); }

  /**
   * @brief Error name of the last failed open on this thread
   * ("DatabaseLocked", "SchemaMismatch", ...), as reported by
   * hocdb_last_error(). The constructors already include it in their
   * exception message.
   */
  static std::string lastError() {
    const char *err = hocdb_last_error();
    return err ? err : "";
  }

  /** @brief File format version: 2 = current (64-byte header), 1 = legacy. */
  int formatVersion() const { return hocdb_format_version(handle_); }

  /** @brief True for handles opened with openReader(). */
  bool isReadOnly() const { return hocdb_is_read_only(handle_) == 1; }

  /**
   * @brief Flush and fsync now, whatever the configured fsync policy.
   * @throws Exception on failure, or when the handle is a reader
   */
  void sync() { checkStorageRc(hocdb_sync(handle_), "sync"); }

  /**
   * @brief Readers: pick up the writer's latest commit. Every read method
   * does this implicitly; call it to make the point explicit or to observe
   * the "refreshes" metric. Writers: no-op.
   * @throws Exception on failure
   */
  void refresh() { checkStorageRc(hocdb_refresh(handle_), "refresh"); }

  /**
   * @brief Recompute the CRC32C of the committed data and compare it with the
   * checksum stored in the header.
   * @return true when the data matches, false on a MISMATCH (the file is
   * corrupted but still readable)
   * @throws Exception "checksum unavailable" when the file has no checksum
   * (ring buffer, legacy format, or a file whose uncommitted tail was just
   * adopted by crash recovery: flush() first), or on any other error
   */
  bool verify() {
    const int rc = hocdb_verify(handle_);
    if (rc == 1) {
      return true;
    }
    if (rc == 0) {
      return false;
    }
    throwStorageError(rc, "verify");
  }

  /**
   * @brief Keep only the records with timestamp >= min_ts (the file is
   * rewritten atomically; readers follow).
   * @throws Exception on failure, or when the handle is a reader
   */
  void compact(int64_t min_ts) {
    checkStorageRc(hocdb_compact(handle_, min_ts), "compact");
  }

  /**
   * @brief Keep only the last n records (the file is rewritten atomically;
   * readers follow).
   * @throws Exception on failure, or when the handle is a reader
   */
  void retainLast(uint64_t n) {
    checkStorageRc(hocdb_retain_last(handle_, n), "retainLast");
  }

  /**
   * @brief Archive the current file as `<ticker>.<first_ts>-<last_ts>.bin`
   * in the same directory and continue with an empty one (timestamps stay
   * monotonic across files; readers follow). The archive is an ordinary
   * database: open it with ticker = the file name without ".bin".
   * @return Path of the archive file
   * @throws Exception on failure, or when the handle is a reader
   */
  std::string rollover() {
    std::vector<char> buf(4096, '\0');
    checkStorageRc(hocdb_rollover(handle_, buf.data(), buf.size()), "rollover");
    return std::string(buf.data());
  }

  /**
   * @brief Operational counters and state (see HOCDBMetrics in hocdb.h):
   * appends, bytes_written, flushes, commits, fsyncs (count / total / max
   * ns), reads (count / total / max / last / p50 / p99 ns, records_read),
   * refreshes, recovered_tail_records, dropped_tail_bytes, crc_failures,
   * compactions, rollovers, migrations, last_append_wall_ns,
   * last_commit_wall_ns, last_record_ts, ingest_lag_wall_ns,
   * ingest_lag_record_ns (needs Config::timestamp_unit_ns), committed_records,
   * file_size, format_version, read_only. Works for readers too.
   * @throws Exception when the HOCDBMetrics layout of hocdb.h differs from
   * the library's (use metricsMap() then), or on failure
   */
  HOCDBMetrics metrics() const {
    checkMetricsLayout();
    HOCDBMetrics out{};
    checkStorageRc(hocdb_metrics(handle_, &out), "metrics");
    return out;
  }

  /**
   * @brief metrics() decoded generically (via the hocdb_metrics_field_*
   * introspection functions) into a name -> value map, like summaryMap().
   * The int64 / uint64 counters are converted to double: exact up to 2^53
   * (9.007e15), which covers every counter and the nanosecond timestamps
   * for the foreseeable future; use metrics() when you need the raw
   * integers.
   * @throws Exception on failure
   */
  std::map<std::string, double> metricsMap() const {
    std::vector<unsigned char> buf(hocdb_metrics_size());
    checkStorageRc(
        hocdb_metrics(handle_, reinterpret_cast<HOCDBMetrics *>(buf.data())),
        "metrics");
    return decodeFields(buf.data(), buf.size(), hocdb_metrics_field_count(),
                        hocdb_metrics_field_name, hocdb_metrics_field_offset,
                        hocdb_metrics_field_type);
  }

  /**
   * @brief Zero the metrics counters (state fields such as last_record_ts,
   * committed_records, file_size, format_version and read_only are kept).
   */
  void metricsReset() { hocdb_metrics_reset(handle_); }

  /* ----------------------------------------------------------------------- */
  /* Trading calendar, backtester and universe (round 4)                      */
  /* ----------------------------------------------------------------------- */

  /**
   * @brief Attach a trading calendar (writers persist built-in ids in the file
   * header; readers keep it on the handle).
   * @throws Exception for an unknown id
   */
  void setCalendar(uint32_t id) {
    checkStorageRc(hocdb_set_calendar(handle_, id), "setCalendar");
  }

  /** @brief setCalendar by name ("nyse", "crypto", ... or a custom one). */
  void setCalendar(const std::string &name) {
    uint32_t id = calendarId(name);
    if (id == 0) {
      throw Exception("setCalendar failed: unknown calendar '" + name +
                      "' (UnknownCalendar); built-in names: crypto, fx, nyse, "
                      "nasdaq, lse, cme");
    }
    setCalendar(id);
  }

  /** @brief The handle's calendar id (0 = none). */
  uint32_t calendar() const { return hocdb_get_calendar(handle_); }

  /** @brief Nanoseconds per timestamp unit (1000 = microseconds). */
  void setTimestampUnit(uint64_t unit_ns) {
    checkStorageRc(hocdb_set_timestamp_unit(handle_, unit_ns),
                   "setTimestampUnit");
  }

  uint64_t timestampUnit() const { return hocdb_get_timestamp_unit(handle_); }

  /**
   * @brief Bars per year for `bucket` timestamp units, from the handle's
   * calendar and timestamp unit (0 when either is unknown).
   */
  double periodsPerYear(int64_t bucket) const {
    return hocdb_periods_per_year(handle_, bucket);
  }

  /**
   * @brief Backtest a target-position series over the rows of
   * indicators(start_ts, end_ts, bucket): bucket > 0 selects the bars whose
   * start lies in the window (equal to ohlcv for bucket-aligned bounds),
   * bucket 0 the raw records.
   * @param target one entry per row; NaN holds the previous signal
   * @throws Exception when the length differs from the row count (-7)
   */
  BacktestReport backtest(const std::vector<double> &target, int64_t start_ts,
                          int64_t end_ts, int64_t bucket = 0,
                          const HOCDBBacktestParams *params = nullptr,
                          const BacktestOutputs &outputs = {},
                          const IndicatorColumns *cols = nullptr) const {
    HOCDBIndicatorColumns c = resolveColumns(cols);
    HOCDBBacktestParams p = params ? *params : backtestDefaults();
    BacktestBuffers b(target.size(), outputs);
    checkBacktestRc(
        hocdb_backtest(handle_, &c, start_ts, end_ts, bucket, target.data(),
                       target.size(), &p, b.outputs(), b.tradesPtr(),
                       outputs.max_trades, &b.report.result),
        "backtest");
    return b.finish();
  }

  /** @brief backtest() over the last target.size() bars (bucket > 0) or records. */
  BacktestReport backtestTail(const std::vector<double> &target,
                              int64_t bucket = 0,
                              const HOCDBBacktestParams *params = nullptr,
                              const BacktestOutputs &outputs = {},
                              const IndicatorColumns *cols = nullptr) const {
    HOCDBIndicatorColumns c = resolveColumns(cols);
    HOCDBBacktestParams p = params ? *params : backtestDefaults();
    BacktestBuffers b(target.size(), outputs);
    checkBacktestRc(hocdb_backtest_tail(handle_, &c, bucket, target.data(),
                                        target.size(), &p, b.outputs(),
                                        b.tradesPtr(), outputs.max_trades,
                                        &b.report.result),
                    "backtestTail");
    return b.finish();
  }

  /** @brief The raw C handle (for universe() and other free functions). */
  HOCDBHandle raw() const { return handle_; }

  /** @brief Column roles resolved against this schema (auto-detected when null). */
  HOCDBIndicatorColumns
  indicatorColumns(const IndicatorColumns *cols = nullptr) const {
    return resolveColumns(cols);
  }

  /* ----------------------------------------------------------------------- */
  /* Technical indicators and quantitative analytics                          */
  /* ----------------------------------------------------------------------- */

  /**
   * @brief Compute a batch of indicators over [start_ts, end_ts) in one pass.
   * @param specs Indicators to compute (see IndicatorSpec)
   * @param start_ts Start timestamp (inclusive)
   * @param end_ts End timestamp (exclusive)
   * @param options Column roles, lookback and bucket (see IndicatorOptions)
   * @return One named series per indicator output. A spec's label is
   * spec.label if given, else the kind name plus "_<period>" when a period
   * is given ("sma_20", "rsi", "macd"). Single-output kinds use the label as
   * the column name; multi-output kinds append "_<output>" ("macd_signal",
   * "bbands_20_upper"), except that the output named like the kind itself
   * is just the label ("macd", "adx_14").
   * @throws Exception on unknown kind or field, duplicate output column
   * name, missing column, field override with bucket > 0, or any other C API
   * error
   */
  IndicatorResult indicators(const std::vector<IndicatorSpec> &specs,
                             int64_t start_ts, int64_t end_ts,
                             const IndicatorOptions &options = {}) {
    return runIndicators(specs, options, false, 0, start_ts, end_ts);
  }

  /**
   * @brief Same as indicators() for the last n records (or bars when
   * options.bucket > 0).
   */
  IndicatorResult indicatorsTail(size_t n,
                                 const std::vector<IndicatorSpec> &specs,
                                 const IndicatorOptions &options = {}) {
    return runIndicators(specs, options, true, n, 0, 0);
  }

  /**
   * @brief Indicators over this database aligned with `other`, whose close
   * column becomes the second series: with options.bucket > 0 both are
   * resampled and inner-joined on bar timestamps; on ticks `other` is as-of
   * joined onto this database's rows (latest `other` row at or before each
   * row). Single-series kinds run on this database; series2, ratio,
   * ratio_zscore, rel_strength, correl and beta use both.
   * @param other Another open database (may have a different schema)
   * @param options As IndicatorOptions for this database, plus
   * `other_columns` for `other` (see PairOptions)
   * @return As indicators(); timestamps are this database's rows (or the
   * joined bars)
   * @throws Exception as indicators(), and when a spec sets `field2` (the
   * second series is always `other`)
   */
  IndicatorResult pairIndicators(Database &other,
                                 const std::vector<IndicatorSpec> &specs,
                                 int64_t start_ts, int64_t end_ts,
                                 const PairOptions &options = {}) {
    return runPairIndicators(other, specs, options, false, 0, start_ts, end_ts);
  }

  /**
   * @brief Same as pairIndicators() for the last n rows (or bars when
   * options.bucket > 0) of this database.
   */
  IndicatorResult pairIndicatorsTail(Database &other, size_t n,
                                     const std::vector<IndicatorSpec> &specs,
                                     const PairOptions &options = {}) {
    return runPairIndicators(other, specs, options, true, n, 0, 0);
  }

  /**
   * @brief Aggregate records in [start_ts, end_ts) into OHLCV bars.
   * @param bucket Bar width in timestamp units
   * @param price_field Field used for open/high/low/close
   * @param volume_field Field summed into volume ("" = none; volume is then
   * the record count)
   * @param side_field Field holding the trade side (1 = buy, 0 = sell); when
   * given, Bars::buy_volume holds the per-bar volume of the buy records
   * ("" = none; buy_volume stays empty)
   * @throws Exception on unknown field or C API error
   */
  Bars ohlcv(int64_t start_ts, int64_t end_ts, int64_t bucket,
             const std::string &price_field,
             const std::string &volume_field = "",
             const std::string &side_field = "") {
    const size_t price = fieldIndex(price_field);
    const int64_t volume = optionalFieldIndex(volume_field);
    const int64_t side = optionalFieldIndex(side_field);
    BarsGuard guard;
    checkIndicatorRc(hocdb_ohlcv_ex(handle_, start_ts, end_ts, price, volume,
                                    side, bucket, &guard.bars),
                     "ohlcv");
    const HOCDBBarsEx &b = guard.bars;
    const size_t n = b.n_bars;
    auto copy = [n](const double *src, std::vector<double> &dst) {
      if (n > 0 && src) {
        dst.assign(src, src + n);
      }
    };
    Bars out;
    if (n > 0 && b.timestamps) {
      out.timestamps.assign(b.timestamps, b.timestamps + n);
    }
    copy(b.open, out.open);
    copy(b.high, out.high);
    copy(b.low, out.low);
    copy(b.close, out.close);
    copy(b.volume, out.volume);
    copy(b.count, out.count);
    copy(b.buy_volume, out.buy_volume); // NULL without a side field
    return out;
  }

  /**
   * @brief Scalar performance / risk summary of a field over
   * [start_ts, end_ts) (29 fields: Sharpe, Sortino, max drawdown, VaR, Hurst,
   * ...).
   * @param periods_per_year Annualisation for ann_return / ann_vol / Sharpe /
   * Sortino (0 = none)
   * @return HOCDBSummary struct (see hocdb.h); summaryMap() returns the same
   * values as name -> value
   * @throws Exception on unknown field or C API error
   */
  HOCDBSummary summary(int64_t start_ts, int64_t end_ts,
                       const std::string &field, double periods_per_year = 0) {
    if (hocdb_summary_size() != sizeof(HOCDBSummary)) {
      throw Exception("HOCDBSummary layout differs between hocdb.h and the "
                      "library; use summaryMap()");
    }
    HOCDBSummary out{};
    checkIndicatorRc(hocdb_summary(handle_, start_ts, end_ts, fieldIndex(field),
                                   periods_per_year, &out),
                     "summary");
    return out;
  }

  /**
   * @brief summary() decoded generically (via the hocdb_summary_field_*
   * introspection functions) into a name -> value map. Integer fields such
   * as "count" are converted to double.
   */
  std::map<std::string, double> summaryMap(int64_t start_ts, int64_t end_ts,
                                           const std::string &field,
                                           double periods_per_year = 0) {
    std::vector<unsigned char> buf(hocdb_summary_size());
    checkIndicatorRc(
        hocdb_summary(handle_, start_ts, end_ts, fieldIndex(field),
                      periods_per_year,
                      reinterpret_cast<HOCDBSummary *>(buf.data())),
        "summary");
    return decodeFields(buf.data(), buf.size(), hocdb_summary_field_count(),
                        hocdb_summary_field_name, hocdb_summary_field_offset,
                        hocdb_summary_field_type);
  }

  /**
   * @brief Data-quality statistics of [start_ts, end_ts): record count,
   * first/last timestamp, gap statistics, non-positive / NaN prices, outlier
   * returns, zero / negative volume (16 fields, see HOCDBHealth in hocdb.h).
   * @param price_field Price field
   * @param volume_field Volume field ("" = none: the volume counters stay 0)
   * @param gap_threshold Gaps (timestamp units) above this are counted in
   * n_gaps
   * @param outlier_threshold |log return| above this is counted as an outlier
   * @return HOCDBHealth struct; healthMap() returns the same values as
   * name -> value
   * @throws Exception on unknown field or C API error
   */
  HOCDBHealth health(int64_t start_ts, int64_t end_ts,
                     const std::string &price_field,
                     const std::string &volume_field = "",
                     int64_t gap_threshold = 0, double outlier_threshold = 0) {
    if (hocdb_health_size() != sizeof(HOCDBHealth)) {
      throw Exception("HOCDBHealth layout differs between hocdb.h and the "
                      "library; use healthMap()");
    }
    HOCDBHealth out{};
    checkIndicatorRc(hocdb_health(handle_, start_ts, end_ts,
                                  fieldIndex(price_field),
                                  optionalFieldIndex(volume_field),
                                  gap_threshold, outlier_threshold, &out),
                     "health");
    return out;
  }

  /**
   * @brief health() decoded generically (via the hocdb_health_field_*
   * introspection functions) into a name -> value map. Integer fields are
   * converted to double.
   */
  std::map<std::string, double>
  healthMap(int64_t start_ts, int64_t end_ts, const std::string &price_field,
            const std::string &volume_field = "", int64_t gap_threshold = 0,
            double outlier_threshold = 0) {
    std::vector<unsigned char> buf(hocdb_health_size());
    checkIndicatorRc(
        hocdb_health(handle_, start_ts, end_ts, fieldIndex(price_field),
                     optionalFieldIndex(volume_field), gap_threshold,
                     outlier_threshold,
                     reinterpret_cast<HOCDBHealth *>(buf.data())),
        "health");
    return decodeFields(buf.data(), buf.size(), hocdb_health_field_count(),
                        hocdb_health_field_name, hocdb_health_field_offset,
                        hocdb_health_field_type);
  }

  /**
   * @brief Evaluate trading decisions against the recorded prices: each
   * decision enters at the first price at or after its timestamp and exits
   * at the first price at or after timestamp + horizon; `cost_bps` is charged
   * per side. Decisions with direction 0 or without an exit price are counted
   * in n_decisions but not evaluated (NaN in the per-decision arrays).
   * @param decisions Decisions in any order
   * @param price_field Price field
   * @param default_horizon Horizon (timestamp units) for decisions whose
   * horizon is 0
   * @param cost_bps Transaction cost in basis points per side
   * @return Aggregate statistics plus per-decision entry / exit / net_return
   * (each decisions.size() long); evaluationMap() decodes the statistics
   * @throws Exception on unknown field or C API error
   */
  EvaluationResult evaluate(const std::vector<Decision> &decisions,
                            const std::string &price_field,
                            int64_t default_horizon = 0, double cost_bps = 0) {
    checkEvaluationLayout();
    const size_t price = fieldIndex(price_field);
    std::vector<HOCDBDecision> c_decisions;
    c_decisions.reserve(decisions.size());
    for (const auto &d : decisions) {
      HOCDBDecision c;
      c.timestamp = d.timestamp;
      c.direction = d.direction;
      c.size = d.size;
      c.horizon = d.horizon;
      c_decisions.push_back(c);
    }
    const size_t n = c_decisions.size();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    EvaluationResult out;
    out.entry.assign(n, nan);
    out.exit.assign(n, nan);
    out.net_return.assign(n, nan);
    checkIndicatorRc(
        hocdb_evaluate(handle_, price, n > 0 ? c_decisions.data() : nullptr, n,
                       default_horizon, cost_bps, &out.stats,
                       n > 0 ? out.entry.data() : nullptr,
                       n > 0 ? out.exit.data() : nullptr,
                       n > 0 ? out.net_return.data() : nullptr),
        "evaluate");
    return out;
  }

  /**
   * @brief An evaluation decoded generically (via the
   * hocdb_evaluation_field_* introspection functions) into a name -> value
   * map (20 fields: n_decisions, n_evaluated, hit_rate, avg_net_return,
   * total_pnl, sharpe, profit_factor, max_drawdown, ...). Integer fields are
   * converted to double.
   */
  static std::map<std::string, double>
  evaluationMap(const HOCDBEvaluation &evaluation) {
    checkEvaluationLayout();
    return decodeFields(reinterpret_cast<const unsigned char *>(&evaluation),
                        sizeof evaluation, hocdb_evaluation_field_count(),
                        hocdb_evaluation_field_name,
                        hocdb_evaluation_field_offset,
                        hocdb_evaluation_field_type);
  }

  /**
   * @brief One-shot snapshot of ~100 indicators for the latest bar.
   * @param cols Column roles (nullptr = auto-detect, see IndicatorOptions)
   * @param bars Records (or bars when bucket > 0) to use; 0 = recommended
   * (2500, enough for every field to converge)
   * @param bucket 0 = raw records; > 0 = aggregate into bars first
   * @param periods_per_year Annualisation for volatility / Sharpe / Sortino
   * @return HOCDBSnapshot struct (see hocdb.h); snapshotMap() returns the
   * same values as name -> value
   * @throws Exception on missing close column or C API error
   */
  HOCDBSnapshot snapshot(const IndicatorColumns *cols = nullptr,
                         size_t bars = 0, int64_t bucket = 0,
                         double periods_per_year = 0) {
    checkSnapshotLayout();
    HOCDBIndicatorColumns c = resolveColumns(cols);
    HOCDBSnapshot out{};
    checkIndicatorRc(hocdb_snapshot(handle_, &c, bars, bucket,
                                    periods_per_year, &out),
                     "snapshot");
    return out;
  }

  /**
   * @brief snapshot() decoded generically (via the hocdb_snapshot_field_*
   * introspection functions) into a name -> value map. "timestamp" and
   * "bars" are converted to double; everything else is already a double.
   */
  std::map<std::string, double> snapshotMap(const IndicatorColumns *cols = nullptr,
                                            size_t bars = 0, int64_t bucket = 0,
                                            double periods_per_year = 0) {
    HOCDBIndicatorColumns c = resolveColumns(cols);
    std::vector<unsigned char> buf(hocdb_snapshot_size());
    checkIndicatorRc(
        hocdb_snapshot(handle_, &c, bars, bucket, periods_per_year,
                       reinterpret_cast<HOCDBSnapshot *>(buf.data())),
        "snapshot");
    return decodeFields(buf.data(), buf.size(), hocdb_snapshot_field_count(),
                        hocdb_snapshot_field_name, hocdb_snapshot_field_offset,
                        hocdb_snapshot_field_type);
  }

  /**
   * @brief Snapshots for several bar sizes from one read: result[k] is the
   * snapshot of the last `bars` bars of buckets[k], annualised with
   * periods_per_year[k] (same as snapshot(cols, bars, buckets[k],
   * periods_per_year[k])).
   * @param buckets Bar widths in timestamp units, in result order
   * @param periods_per_year One entry per bucket, or empty for 0 everywhere
   * @param bars Bars to use per bucket; 0 = recommended (2500)
   * @param cols Column roles (nullptr = auto-detect)
   * @return One HOCDBSnapshot per bucket, in bucket order; snapshotMultiMap()
   * returns the same values as name -> value maps
   * @throws Exception when periods_per_year has the wrong length, on missing
   * close column or C API error
   */
  std::vector<HOCDBSnapshot>
  snapshotMulti(const std::vector<int64_t> &buckets,
                const std::vector<double> &periods_per_year = {},
                size_t bars = 0, const IndicatorColumns *cols = nullptr) {
    checkSnapshotLayout();
    const std::vector<double> ppy = multiPeriods(buckets, periods_per_year);
    HOCDBIndicatorColumns c = resolveColumns(cols);
    std::vector<HOCDBSnapshot> out(buckets.size());
    if (!buckets.empty()) {
      checkIndicatorRc(hocdb_snapshot_multi(handle_, &c, bars, buckets.data(),
                                            buckets.size(), ppy.data(),
                                            out.data()),
                       "snapshotMulti");
    }
    return out;
  }

  /**
   * @brief snapshotMulti() decoded generically: one name -> value map per
   * bucket, in bucket order (see snapshotMap()).
   */
  std::vector<std::map<std::string, double>>
  snapshotMultiMap(const std::vector<int64_t> &buckets,
                   const std::vector<double> &periods_per_year = {},
                   size_t bars = 0, const IndicatorColumns *cols = nullptr) {
    const std::vector<double> ppy = multiPeriods(buckets, periods_per_year);
    HOCDBIndicatorColumns c = resolveColumns(cols);
    const size_t size = hocdb_snapshot_size();
    std::vector<unsigned char> buf(size * buckets.size());
    if (!buckets.empty()) {
      checkIndicatorRc(
          hocdb_snapshot_multi(handle_, &c, bars, buckets.data(),
                               buckets.size(), ppy.data(),
                               reinterpret_cast<HOCDBSnapshot *>(buf.data())),
          "snapshotMulti");
    }
    std::vector<std::map<std::string, double>> out;
    out.reserve(buckets.size());
    for (size_t k = 0; k < buckets.size(); ++k) {
      out.push_back(decodeFields(buf.data() + k * size, size,
                                 hocdb_snapshot_field_count(),
                                 hocdb_snapshot_field_name,
                                 hocdb_snapshot_field_offset,
                                 hocdb_snapshot_field_type));
    }
    return out;
  }

  /**
   * @brief Names of every indicator kind the library knows ("sma", "rsi", ...)
   */
  static std::vector<std::string> indicatorKinds() {
    const size_t total = hocdb_indicator_kinds(nullptr, 0);
    std::vector<uint32_t> ids(total);
    if (total > 0) {
      hocdb_indicator_kinds(ids.data(), ids.size());
    }
    std::vector<std::string> names;
    names.reserve(total);
    for (uint32_t id : ids) {
      const char *name = hocdb_indicator_name(id);
      if (name) {
        names.emplace_back(name);
      }
    }
    return names;
  }

  /**
   * @brief Output names of a kind, e.g. {"macd", "signal", "hist"} for "macd".
   * Single-output kinds report {"value"}.
   * @throws Exception on unknown kind
   */
  static std::vector<std::string> indicatorOutputs(const std::string &kind) {
    const uint32_t id = indicatorKind(kind);
    const size_t n = hocdb_indicator_output_count(id);
    std::vector<std::string> names;
    names.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      const char *name = hocdb_indicator_output_name(id, i);
      names.emplace_back(name ? name : "");
    }
    return names;
  }

  /**
   * @brief Whether a kind's outputs depend on FUTURE rows (labels such as
   * "forward_return" and "triple_barrier"). Such columns must not be used as
   * features for the same row in a live strategy.
   * @throws Exception on unknown kind
   */
  static bool indicatorIsLookahead(const std::string &kind) {
    return hocdb_indicator_is_lookahead(indicatorKind(kind)) == 1;
  }

  /**
   * @brief Recommended warm-up rows for a spec (after applying defaults).
   * The spec's field names do not influence the warm-up and are ignored.
   * @throws Exception on unknown kind
   */
  static size_t indicatorWarmup(const IndicatorSpec &spec) {
    HOCDBIndicatorSpec c = toCSpec(spec, -1, -1);
    return hocdb_indicator_warmup(&c);
  }

private:
  // --- open / storage helpers -----------------------------------------------

  struct ReaderTag {};

  /** Reader constructor (see openReader()). */
  Database(ReaderTag, const std::string &ticker, const std::string &path,
           const std::vector<Field> &schema)
      : handle_(nullptr), record_size_(0) {
    open(ticker, path, schema, nullptr);
  }

  /** Common open path: builds the C schema and record size, then calls
   * hocdb_init_ex (config != nullptr, writer) or hocdb_open_reader
   * (config == nullptr, reader). */
  void open(const std::string &ticker, const std::string &path,
            const std::vector<Field> &schema, const Config *config) {
    std::vector<CField> c_schema;
    c_schema.reserve(schema.size());

    record_size_ = 0;
    for (size_t i = 0; i < schema.size(); ++i) {
      const auto &field = schema[i];
      field_map_[field.name] = i;
      c_schema.push_back({field.name.c_str(), field.type});
      switch (field.type) {
      case HOCDB_TYPE_I64:
        record_size_ += 8;
        break;
      case HOCDB_TYPE_F64:
        record_size_ += 8;
        break;
      case HOCDB_TYPE_U64:
        record_size_ += 8;
        break;
      case HOCDB_TYPE_BOOL:
        record_size_ += 1;
        break;
      default:
        throw Exception("Unsupported field type");
      }
    }

    if (config) {
      const HOCDBConfig c = toCConfig(*config);
      handle_ = hocdb_init_ex(ticker.c_str(), path.c_str(), c_schema.data(),
                              c_schema.size(), &c);
    } else {
      handle_ = hocdb_open_reader(ticker.c_str(), path.c_str(),
                                  c_schema.data(), c_schema.size());
    }
    if (!handle_) {
      std::string err = lastError();
      if (err.empty()) {
        err = "unknown error";
      }
      throw Exception(std::string("Failed to open HOCDB ") +
                      (config ? "writer" : "reader") + " \"" + ticker +
                      "\" in " + path + ": " + err);
    }
  }

  static HOCDBConfig toCConfig(const Config &config) {
    const int policy = static_cast<int>(config.fsync);
    if (policy < HOCDB_FSYNC_NONE || policy > HOCDB_FSYNC_INTERVAL) {
      throw Exception("Invalid Config::fsync policy " + std::to_string(policy) +
                      " (use hocdb::FsyncPolicy::None/OnClose/OnFlush/"
                      "Interval)");
    }
    HOCDBConfig c{};
    c.max_file_size = config.max_file_size;
    c.overwrite_on_full = config.overwrite_on_full ? 1 : 0;
    c.flush_on_write = config.flush_on_write ? 1 : 0;
    c.auto_increment = config.auto_increment ? 1 : 0;
    c.fsync_policy = policy;
    c.fsync_interval_ms = config.fsync_interval_ms;
    c.verify_on_open = config.verify_on_open ? 1 : 0;
    c.retention_span = config.retention_span;
    c.rollover_size = config.rollover_size;
    c.auto_migrate = config.auto_migrate ? 1 : 0;
    c.timestamp_unit_ns = config.timestamp_unit_ns;
    c.index_stride = config.index_stride;
    c.calendar = config.calendar;
    return c;
  }

  static Exception readOnlyError(const char *what) {
    return Exception(std::string(what) +
                     " failed: this handle is a read-only reader (opened with "
                     "Database::openReader); only the writer can modify the "
                     "database");
  }

  /** Map the return codes of the storage entry points (sync, refresh,
   * verify, compact, retainLast, rollover, metrics) to exceptions. */
  [[noreturn]] static void throwStorageError(int rc, const char *what) {
    switch (rc) {
    case -10:
      throw readOnlyError(what);
    case -11:
      throw Exception(std::string(what) +
                      " failed: DatabaseLocked (another writer holds the file)");
    case -12:
      throw Exception(std::string(what) +
                      " failed: ChecksumMismatch (the committed data does not "
                      "match the stored checksum)");
    case -20:
      throw Exception(std::string(what) +
                      " failed: checksum unavailable (ring buffers, legacy "
                      "HOC1 files and files whose uncommitted tail was just "
                      "adopted by crash recovery have no checksum; flush() "
                      "first in the last case)");
    case -21:
      throw Exception(std::string(what) + " failed: the database is empty");
    case -30:
      throw Exception(std::string(what) +
                      " failed: CalendarRequired (a session kind with param 0 "
                      "needs a handle with a trading calendar and a timestamp "
                      "unit: Config::calendar / setCalendar() and "
                      "setTimestampUnit(); otherwise pass param = the session "
                      "length in timestamp units)");
    case -31:
      throw Exception(std::string(what) +
                      " failed: UnknownCalendar (no calendar with that id; "
                      "built-in ids 1-6, custom ones come from "
                      "calendarDefine())");
    case -1:
      throw Exception(std::string(what) + " failed: I/O error or out of memory");
    case -2:
      throw Exception(std::string(what) + " failed: invalid parameter");
    default:
      throw Exception(std::string(what) + " failed: error code " +
                      std::to_string(rc));
    }
  }

  /** Per-bar output buffers of one backtest call (freed with the object). */
  struct BacktestBuffers {
    BacktestReport report;
    HOCDBBacktestOutputs outs{};
    bool any = false;

    BacktestBuffers(size_t n, const BacktestOutputs &want) {
      auto pick = [&](bool on, std::vector<double> &v) -> double * {
        if (!on || n == 0) {
          return nullptr;
        }
        v.assign(n, 0.0);
        any = true;
        return v.data();
      };
      outs.equity = pick(want.equity, report.equity);
      outs.position = pick(want.position, report.position);
      outs.cash = pick(want.cash, report.cash);
      outs.pnl = pick(want.pnl, report.pnl);
      outs.drawdown = pick(want.drawdown, report.drawdown);
      if (want.max_trades > 0) {
        report.trades.assign(want.max_trades, HOCDBTrade{});
      }
    }

    const HOCDBBacktestOutputs *outputs() const { return any ? &outs : nullptr; }
    HOCDBTrade *tradesPtr() {
      return report.trades.empty() ? nullptr : report.trades.data();
    }

    /** Trim the trade list to the number actually written. */
    BacktestReport finish() {
      size_t n = static_cast<size_t>(report.result.n_trades);
      if (report.trades.size() > n) {
        report.trades.resize(n);
      }
      return std::move(report);
    }
  };

  static void checkBacktestRc(int rc, const char *what) {
    if (rc == -7) {
      throw Exception(std::string(what) +
                      " failed: target length mismatch (len(target) must equal "
                      "the number of rows the window holds)");
    }
    checkStorageRc(rc, what);
  }

  static void checkStorageRc(int rc, const char *what) {
    if (rc != 0) {
      throwStorageError(rc, what);
    }
  }

  static void checkMetricsLayout() {
    if (hocdb_metrics_size() != sizeof(HOCDBMetrics)) {
      throw Exception("HOCDBMetrics layout differs between hocdb.h and the "
                      "library; use metricsMap()");
    }
  }

  // --- indicator helpers ----------------------------------------------------

  /** RAII guards: the C results are freed even if copying them out throws.
   * The C structs are value-initialised (all NULL / 0), and the free functions
   * accept that, so a guard is safe whether or not the C call succeeded. */
  struct IndicatorResultGuard {
    HOCDBIndicatorResult result{};
    IndicatorResultGuard() = default;
    IndicatorResultGuard(const IndicatorResultGuard &) = delete;
    IndicatorResultGuard &operator=(const IndicatorResultGuard &) = delete;
    ~IndicatorResultGuard() { hocdb_indicators_free(&result); }
  };
  struct BarsGuard {
    HOCDBBarsEx bars{};
    BarsGuard() = default;
    BarsGuard(const BarsGuard &) = delete;
    BarsGuard &operator=(const BarsGuard &) = delete;
    ~BarsGuard() { hocdb_ohlcv_ex_free(&bars); }
  };

  static void checkIndicatorRc(int rc, const char *what) {
    if (rc == 0) {
      return;
    }
    std::string msg = std::string(what) + " failed: ";
    switch (rc) {
    case -1:
      msg += "out of memory";
      break;
    case -2:
      msg += "invalid indicator spec (unknown kind, bad period or parameter; "
             "session_vwap/session_range/opening_range/pivots need param = "
             "session length, or 0 with a trading calendar)";
      break;
    case -3:
      msg += "missing column (a spec needs an open/high/low/close/volume/"
             "bid/ask/side column that is not available)";
      break;
    case -4:
      msg += "invalid field index";
      break;
    case -5:
      msg += "per-spec field overrides are not supported with bucket > 0";
      break;
    case -6:
      msg += "too many columns";
      break;
    case -7:
      msg += "length mismatch";
      break;
    case -30:
      msg += "CalendarRequired (a session kind with param 0 needs a handle "
             "with a trading calendar and a timestamp unit: Config::calendar / "
             "setCalendar() and setTimestampUnit(); otherwise pass param = the "
             "session length in timestamp units)";
      break;
    case -31:
      msg += "UnknownCalendar (no calendar with that id; built-in ids 1-6, "
             "custom ones come from calendarDefine())";
      break;
    default:
      msg += "error code " + std::to_string(rc);
      break;
    }
    throw Exception(msg);
  }

  static void checkSnapshotLayout() {
    if (hocdb_snapshot_size() != sizeof(HOCDBSnapshot)) {
      throw Exception("HOCDBSnapshot layout differs between hocdb.h and the "
                      "library; use snapshotMap() / snapshotMultiMap()");
    }
  }

  static void checkEvaluationLayout() {
    if (hocdb_evaluation_size() != sizeof(HOCDBEvaluation) ||
        hocdb_decision_size() != sizeof(HOCDBDecision)) {
      throw Exception("HOCDBEvaluation / HOCDBDecision layout differs "
                      "between hocdb.h and the library");
    }
  }

  static std::vector<double> multiPeriods(const std::vector<int64_t> &buckets,
                                          const std::vector<double> &ppy) {
    if (ppy.empty()) {
      return std::vector<double>(buckets.size(), 0.0);
    }
    if (ppy.size() != buckets.size()) {
      throw Exception("snapshotMulti: periods_per_year must be empty or have "
                      "one entry per bucket");
    }
    return ppy;
  }

  static uint32_t indicatorKind(const std::string &name) {
    const uint32_t kind = hocdb_indicator_kind_from_name(name.c_str());
    if (kind == 0) {
      throw Exception("Unknown indicator kind: " + name);
    }
    return kind;
  }

  size_t fieldIndex(const std::string &name) const {
    auto it = field_map_.find(name);
    if (it == field_map_.end()) {
      throw Exception("Unknown field: " + name);
    }
    return it->second;
  }

  int64_t optionalFieldIndex(const std::string &name) const {
    return name.empty() ? -1 : static_cast<int64_t>(fieldIndex(name));
  }

  HOCDBIndicatorColumns resolveColumns(const IndicatorColumns *cols) const {
    HOCDBIndicatorColumns c;
    if (cols) {
      if (cols->close.empty()) {
        throw Exception("IndicatorColumns::close is required");
      }
      c.open = optionalFieldIndex(cols->open);
      c.high = optionalFieldIndex(cols->high);
      c.low = optionalFieldIndex(cols->low);
      c.close = optionalFieldIndex(cols->close);
      c.volume = optionalFieldIndex(cols->volume);
      c.bid = optionalFieldIndex(cols->bid);
      c.ask = optionalFieldIndex(cols->ask);
      c.side = optionalFieldIndex(cols->side);
      return c;
    }
    auto find = [this](const char *name) -> int64_t {
      auto it = field_map_.find(name);
      return it == field_map_.end() ? -1 : static_cast<int64_t>(it->second);
    };
    c.open = find("open");
    c.high = find("high");
    c.low = find("low");
    c.close = find("close");
    c.volume = find("volume");
    c.bid = find("bid");
    c.ask = find("ask");
    c.side = find("side");
    if (c.close < 0) {
      c.close = find("price");
    }
    if (c.volume < 0) {
      c.volume = find("size");
    }
    if (c.volume < 0) {
      c.volume = find("qty");
    }
    if (c.close < 0) {
      throw Exception("No close column: the schema has no field named "
                      "\"close\" or \"price\"; set IndicatorOptions::columns");
    }
    return c;
  }

  static HOCDBIndicatorSpec toCSpec(const IndicatorSpec &spec,
                                    int64_t field_index, int64_t field_index2) {
    HOCDBIndicatorSpec c;
    c.kind = indicatorKind(spec.kind);
    c.period = spec.period;
    c.period2 = spec.period2;
    c.period3 = spec.period3;
    c.period4 = spec.period4;
    c.param = spec.param;
    c.param2 = spec.param2;
    c.field_index = field_index;
    c.field_index2 = field_index2;
    return c;
  }

  static std::string specLabel(const IndicatorSpec &spec, uint32_t kind) {
    if (!spec.label.empty()) {
      return spec.label;
    }
    const char *name = hocdb_indicator_name(kind);
    std::string label = name ? name : spec.kind;
    if (spec.period > 0) {
      label += "_" + std::to_string(spec.period);
    }
    return label;
  }

  /** Resolve the specs to C specs and compute their output column names.
   * `pair` = the second series comes from another database, so `field2` is
   * rejected instead of being silently ignored. */
  std::vector<HOCDBIndicatorSpec>
  buildSpecs(const std::vector<IndicatorSpec> &specs, bool pair,
             std::vector<std::string> &names) const {
    std::vector<HOCDBIndicatorSpec> c_specs;
    c_specs.reserve(specs.size());
    for (const auto &spec : specs) {
      if (pair && !spec.field2.empty()) {
        throw Exception("IndicatorSpec::field2 is not used by pairIndicators: "
                        "the second series is the other database's close "
                        "column");
      }
      HOCDBIndicatorSpec c =
          toCSpec(spec, optionalFieldIndex(spec.field),
                  pair ? -1 : optionalFieldIndex(spec.field2));
      const std::string label = specLabel(spec, c.kind);
      const char *kind_name = hocdb_indicator_name(c.kind);
      const size_t n_out = hocdb_indicator_output_count(c.kind);
      for (size_t k = 0; k < n_out; ++k) {
        const char *out_name = hocdb_indicator_output_name(c.kind, k);
        std::string column = label;
        // Single-output kinds use the label as-is; multi-output kinds append
        // the output name, except for the output named like the kind itself
        // (macd, ppo, adx, tsi), which is just the label.
        if (n_out > 1 && !(out_name && kind_name &&
                           std::strcmp(out_name, kind_name) == 0)) {
          column += "_" + (out_name ? std::string(out_name) : std::to_string(k));
        }
        for (const auto &existing : names) {
          if (existing == column) {
            throw Exception("Duplicate indicator output column \"" + column +
                            "\"; set IndicatorSpec::label to disambiguate");
          }
        }
        names.push_back(column);
      }
      c_specs.push_back(c);
    }
    return c_specs;
  }

  static IndicatorResult copyResult(const HOCDBIndicatorResult &r,
                                    std::vector<std::string> names,
                                    const char *what) {
    if (r.n_outputs != names.size()) {
      throw Exception(std::string(what) + ": output count differs between "
                                          "hocdb.h and the library");
    }
    IndicatorResult out;
    out.n_rows = r.n_rows;
    out.names = std::move(names);
    if (r.n_rows > 0 && r.timestamps) {
      out.timestamps.assign(r.timestamps, r.timestamps + r.n_rows);
    }
    out.outputs.resize(r.n_outputs);
    if (r.n_rows > 0 && r.values) {
      for (size_t k = 0; k < r.n_outputs; ++k) {
        const double *begin = r.values + k * r.n_rows;
        out.outputs[k].assign(begin, begin + r.n_rows);
      }
    }
    return out;
  }

  IndicatorResult runIndicators(const std::vector<IndicatorSpec> &specs,
                                const IndicatorOptions &options, bool tail,
                                size_t n_last, int64_t start_ts,
                                int64_t end_ts) {
    HOCDBIndicatorColumns cols =
        resolveColumns(options.columns ? &*options.columns : nullptr);
    std::vector<std::string> names;
    std::vector<HOCDBIndicatorSpec> c_specs = buildSpecs(specs, false, names);
    const size_t lookback =
        options.lookback ? *options.lookback : HOCDB_LOOKBACK_AUTO;
    const char *what = tail ? "indicatorsTail" : "indicators";
    IndicatorResultGuard guard;
    const int rc =
        tail ? hocdb_indicators_tail(handle_, n_last, &cols, c_specs.data(),
                                     c_specs.size(), lookback, options.bucket,
                                     &guard.result)
             : hocdb_indicators(handle_, start_ts, end_ts, &cols,
                                c_specs.data(), c_specs.size(), lookback,
                                options.bucket, &guard.result);
    checkIndicatorRc(rc, what);
    return copyResult(guard.result, std::move(names), what);
  }

  IndicatorResult runPairIndicators(Database &other,
                                    const std::vector<IndicatorSpec> &specs,
                                    const PairOptions &options, bool tail,
                                    size_t n_last, int64_t start_ts,
                                    int64_t end_ts) {
    HOCDBIndicatorColumns cols_a =
        resolveColumns(options.columns ? &*options.columns : nullptr);
    HOCDBIndicatorColumns cols_b = other.resolveColumns(
        options.other_columns ? &*options.other_columns : nullptr);
    std::vector<std::string> names;
    std::vector<HOCDBIndicatorSpec> c_specs = buildSpecs(specs, true, names);
    const size_t lookback =
        options.lookback ? *options.lookback : HOCDB_LOOKBACK_AUTO;
    const char *what = tail ? "pairIndicatorsTail" : "pairIndicators";
    IndicatorResultGuard guard;
    const int rc =
        tail ? hocdb_pair_indicators_tail(handle_, &cols_a, other.handle_,
                                          &cols_b, n_last, c_specs.data(),
                                          c_specs.size(), lookback,
                                          options.bucket, &guard.result)
             : hocdb_pair_indicators(handle_, &cols_a, other.handle_, &cols_b,
                                     start_ts, end_ts, c_specs.data(),
                                     c_specs.size(), lookback, options.bucket,
                                     &guard.result);
    checkIndicatorRc(rc, what);
    return copyResult(guard.result, std::move(names), what);
  }

  /** Decode a C struct into name -> value using its introspection functions
   * (field count / name / byte offset / type: 1 = int64, 2 = double,
   * 3 = uint64). Fields outside [buf, buf + size) are skipped. */
  template <typename NameFn, typename OffsetFn, typename TypeFn>
  static std::map<std::string, double>
  decodeFields(const unsigned char *buf, size_t size, size_t count,
               NameFn name_of, OffsetFn offset_of, TypeFn type_of) {
    std::map<std::string, double> out;
    for (size_t i = 0; i < count; ++i) {
      const char *name = name_of(i);
      const size_t offset = offset_of(i);
      if (!name || offset + sizeof(double) > size) {
        continue;
      }
      const unsigned char *p = buf + offset;
      double value = 0;
      switch (type_of(i)) {
      case HOCDB_TYPE_I64: {
        int64_t v;
        std::memcpy(&v, p, sizeof v);
        value = static_cast<double>(v);
        break;
      }
      case HOCDB_TYPE_F64:
        std::memcpy(&value, p, sizeof value);
        break;
      case HOCDB_TYPE_U64: {
        uint64_t v;
        std::memcpy(&v, p, sizeof v);
        value = static_cast<double>(v);
        break;
      }
      default:
        continue;
      }
      out[name] = value;
    }
    return out;
  }
};

/* ------------------------------------------------------------------------- */
/* Backtester and universe on caller-provided arrays                          */
/* ------------------------------------------------------------------------- */

namespace detail {
/** Decode a C struct into a name -> value map with its introspection
 * functions (types 1 = int64, 2 = double, 3 = uint64). Integer fields are
 * converted to double: exact up to 2^53. */
template <typename NameFn, typename OffsetFn, typename TypeFn>
inline std::map<std::string, double>
decodeStructFields(const unsigned char *buf, size_t size, size_t count,
                   NameFn name_of, OffsetFn offset_of, TypeFn type_of) {
  std::map<std::string, double> out;
  for (size_t i = 0; i < count; ++i) {
    const char *name = name_of(i);
    const size_t offset = offset_of(i);
    if (!name || offset + sizeof(double) > size) {
      continue;
    }
    const unsigned char *p = buf + offset;
    double value = 0;
    switch (type_of(i)) {
    case HOCDB_TYPE_I64: {
      int64_t v;
      std::memcpy(&v, p, sizeof v);
      value = static_cast<double>(v);
      break;
    }
    case HOCDB_TYPE_F64:
      std::memcpy(&value, p, sizeof value);
      break;
    case HOCDB_TYPE_U64: {
      uint64_t v;
      std::memcpy(&v, p, sizeof v);
      value = static_cast<double>(v);
      break;
    }
    default:
      continue;
    }
    out[name] = value;
  }
  return out;
}

/** Shared buffer setup for the array entry points (mirrors Database's). */
struct ArrayBacktestBuffers {
  BacktestReport report;
  HOCDBBacktestOutputs outs{};
  bool any = false;

  ArrayBacktestBuffers(size_t n, const BacktestOutputs &want) {
    auto pick = [&](bool on, std::vector<double> &v) -> double * {
      if (!on || n == 0) {
        return nullptr;
      }
      v.assign(n, 0.0);
      any = true;
      return v.data();
    };
    outs.equity = pick(want.equity, report.equity);
    outs.position = pick(want.position, report.position);
    outs.cash = pick(want.cash, report.cash);
    outs.pnl = pick(want.pnl, report.pnl);
    outs.drawdown = pick(want.drawdown, report.drawdown);
    if (want.max_trades > 0) {
      report.trades.assign(want.max_trades, HOCDBTrade{});
    }
  }

  const HOCDBBacktestOutputs *outputs() const { return any ? &outs : nullptr; }
  HOCDBTrade *tradesPtr() {
    return report.trades.empty() ? nullptr : report.trades.data();
  }
  BacktestReport finish() {
    size_t n = static_cast<size_t>(report.result.n_trades);
    if (report.trades.size() > n) {
      report.trades.resize(n);
    }
    return std::move(report);
  }
};

[[noreturn]] inline void throwArrayError(int rc, const char *what) {
  if (rc == -7) {
    throw Exception(std::string(what) +
                    " failed: series length mismatch (ts, close and target "
                    "must have the same length)");
  }
  if (rc == -2) {
    throw Exception(std::string(what) + " failed: invalid parameters");
  }
  throw Exception(std::string(what) + " failed: error code " +
                  std::to_string(rc));
}

inline const double *seriesData(const std::vector<double> *v, size_t n,
                                const char *name, const char *what) {
  if (v == nullptr) {
    return nullptr;
  }
  if (v->size() != n) {
    throw Exception(std::string(what) + ": " + name + " has " +
                    std::to_string(v->size()) + " entries, expected " +
                    std::to_string(n));
  }
  return v->data();
}
} // namespace detail

/**
 * @brief Backtest on caller-provided bars. `open`, `high` and `low` may be
 * null: fills then happen at the close and stops trigger on the close.
 * @throws Exception on a length mismatch or invalid parameters
 */
inline BacktestReport
backtestArrays(const std::vector<int64_t> &ts, const std::vector<double> *open,
               const std::vector<double> *high, const std::vector<double> *low,
               const std::vector<double> &close,
               const std::vector<double> &target,
               const HOCDBBacktestParams *params = nullptr,
               const BacktestOutputs &outputs = {}) {
  size_t n = ts.size();
  if (close.size() != n || target.size() != n) {
    throw Exception("backtestArrays: ts, close and target must have the same "
                    "length");
  }
  HOCDBBacktestParams p = params ? *params : backtestDefaults();
  detail::ArrayBacktestBuffers b(n, outputs);
  int rc = hocdb_backtest_arrays(
      ts.data(), detail::seriesData(open, n, "open", "backtestArrays"),
      detail::seriesData(high, n, "high", "backtestArrays"),
      detail::seriesData(low, n, "low", "backtestArrays"), close.data(), n,
      target.data(), &p, b.outputs(), b.tradesPtr(), outputs.max_trades,
      &b.report.result);
  if (rc != 0) {
    detail::throwArrayError(rc, "backtestArrays");
  }
  return b.finish();
}

/**
 * @brief Walk-forward windows over n bars: the first train window is
 * floor(train_frac * n) bars and the test windows tile the rest in n_splits
 * pieces; anchored grows the train window from 0, otherwise it rolls.
 */
inline std::vector<HOCDBSplit> walkForwardSplits(size_t n, size_t n_splits,
                                                 double train_frac,
                                                 bool anchored = true) {
  if (n == 0 || n_splits == 0) {
    return {};
  }
  std::vector<HOCDBSplit> out(n_splits);
  size_t k = hocdb_walk_forward_splits(n, n_splits, train_frac, anchored ? 1 : 0,
                                       out.data(), out.size());
  out.resize(k);
  return out;
}

/** @brief Run the backtester on every test window of `splits` (fresh equity). */
inline std::vector<HOCDBBacktestResult>
backtestSplits(const std::vector<int64_t> &ts, const std::vector<double> *open,
               const std::vector<double> *high, const std::vector<double> *low,
               const std::vector<double> &close,
               const std::vector<double> &target,
               const std::vector<HOCDBSplit> &splits,
               const HOCDBBacktestParams *params = nullptr) {
  size_t n = ts.size();
  if (close.size() != n || target.size() != n) {
    throw Exception("backtestSplits: ts, close and target must have the same "
                    "length");
  }
  if (splits.empty()) {
    return {};
  }
  HOCDBBacktestParams p = params ? *params : backtestDefaults();
  std::vector<HOCDBBacktestResult> results(splits.size());
  int rc = hocdb_backtest_splits_arrays(
      ts.data(), detail::seriesData(open, n, "open", "backtestSplits"),
      detail::seriesData(high, n, "high", "backtestSplits"),
      detail::seriesData(low, n, "low", "backtestSplits"), close.data(), n,
      target.data(), &p, splits.data(), splits.size(), results.data());
  if (rc < 0) {
    detail::throwArrayError(rc, "backtestSplits");
  }
  results.resize(static_cast<size_t>(rc));
  return results;
}

/**
 * @brief Cross-sectional features over a watch-list of databases (joined on
 * timestamps): momentum / volatility ranks, correlations, market factor,
 * betas, dispersion and breadth for the last bar.
 * @param n_bars last n bars (bucket > 0) or records of every database;
 *   0 = enough for the longest period
 * @throws Exception when a database has no close column or on bad parameters
 */
inline UniverseReport universe(const std::vector<const Database *> &dbs,
                               const IndicatorColumns *cols = nullptr,
                               size_t n_bars = 0, int64_t bucket = 0,
                               const HOCDBUniverseParams *params = nullptr,
                               bool with_corr = true) {
  if (dbs.empty()) {
    throw Exception("universe: dbs must not be empty");
  }
  std::vector<HOCDBHandle> handles;
  handles.reserve(dbs.size());
  for (size_t i = 0; i < dbs.size(); ++i) {
    if (dbs[i] == nullptr) {
      throw Exception("universe: dbs[" + std::to_string(i) + "] is null");
    }
    handles.push_back(dbs[i]->raw());
  }
  HOCDBIndicatorColumns c = dbs[0]->indicatorColumns(cols);
  HOCDBUniverseParams p = params ? *params : universeDefaults();
  UniverseReport rep;
  rep.rows.assign(dbs.size(), HOCDBUniverseRow{});
  if (with_corr) {
    rep.corr.assign(dbs.size() * dbs.size(), 0.0);
  }
  int rc = hocdb_universe(handles.data(), handles.size(), &c, n_bars, bucket, &p,
                          rep.rows.data(), with_corr ? rep.corr.data() : nullptr,
                          &rep.summary);
  if (rc != 0) {
    if (rc == -3) {
      throw Exception("universe failed: every database needs a close column");
    }
    detail::throwArrayError(rc, "universe");
  }
  return rep;
}

/** @brief universe() on caller-provided aligned series (one per ticker). */
inline UniverseReport
universeArrays(const std::vector<std::vector<double>> &closes,
               const std::vector<std::vector<double>> *volumes = nullptr,
               const std::vector<int64_t> *ts = nullptr,
               const HOCDBUniverseParams *params = nullptr,
               bool with_corr = true) {
  if (closes.empty()) {
    throw Exception("universeArrays: closes must not be empty");
  }
  size_t n = closes[0].size();
  std::vector<const double *> cl;
  cl.reserve(closes.size());
  for (size_t i = 0; i < closes.size(); ++i) {
    if (closes[i].size() != n) {
      throw Exception("universeArrays: every close series must have the same "
                      "length");
    }
    cl.push_back(closes[i].data());
  }
  std::vector<const double *> vl;
  if (volumes != nullptr) {
    if (volumes->size() != closes.size()) {
      throw Exception("universeArrays: one volume series per ticker");
    }
    vl.reserve(volumes->size());
    for (const auto &v : *volumes) {
      if (v.size() != n) {
        throw Exception("universeArrays: volume series must match the closes");
      }
      vl.push_back(v.data());
    }
  }
  if (ts != nullptr && ts->size() != n) {
    throw Exception("universeArrays: ts must have one entry per bar");
  }
  HOCDBUniverseParams p = params ? *params : universeDefaults();
  UniverseReport rep;
  rep.rows.assign(closes.size(), HOCDBUniverseRow{});
  if (with_corr) {
    rep.corr.assign(closes.size() * closes.size(), 0.0);
  }
  int rc = hocdb_universe_arrays(
      cl.data(), vl.empty() ? nullptr : vl.data(), closes.size(), n,
      ts ? ts->data() : nullptr, &p, rep.rows.data(),
      with_corr ? rep.corr.data() : nullptr, &rep.summary);
  if (rc != 0) {
    detail::throwArrayError(rc, "universeArrays");
  }
  return rep;
}

/** @brief A backtest result decoded generically into a name -> value map. */
inline std::map<std::string, double>
backtestResultMap(const HOCDBBacktestResult &result) {
  return detail::decodeStructFields(
      reinterpret_cast<const unsigned char *>(&result), sizeof result,
      hocdb_backtest_result_field_count(), hocdb_backtest_result_field_name,
      hocdb_backtest_result_field_offset, hocdb_backtest_result_field_type);
}

/** @brief One trade decoded generically into a name -> value map. */
inline std::map<std::string, double> tradeMap(const HOCDBTrade &trade) {
  return detail::decodeStructFields(
      reinterpret_cast<const unsigned char *>(&trade), sizeof trade,
      hocdb_trade_field_count(), hocdb_trade_field_name,
      hocdb_trade_field_offset, hocdb_trade_field_type);
}

/** @brief One universe row decoded generically into a name -> value map. */
inline std::map<std::string, double>
universeRowMap(const HOCDBUniverseRow &row) {
  return detail::decodeStructFields(
      reinterpret_cast<const unsigned char *>(&row), sizeof row,
      hocdb_universe_row_field_count(), hocdb_universe_row_field_name,
      hocdb_universe_row_field_offset, hocdb_universe_row_field_type);
}

/** @brief The universe summary decoded generically into a name -> value map. */
inline std::map<std::string, double>
universeSummaryMap(const HOCDBUniverseSummary &summary) {
  return detail::decodeStructFields(
      reinterpret_cast<const unsigned char *>(&summary), sizeof summary,
      hocdb_universe_summary_field_count(), hocdb_universe_summary_field_name,
      hocdb_universe_summary_field_offset, hocdb_universe_summary_field_type);
}

} // namespace hocdb

#endif // HOCDB_CPP_H
