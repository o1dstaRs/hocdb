#ifndef HOCDB_H
#define HOCDB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Field type constants
#define HOCDB_TYPE_I64 1
#define HOCDB_TYPE_F64 2
#define HOCDB_TYPE_U64 3
#define HOCDB_TYPE_STRING 5
#define HOCDB_TYPE_BOOL 6

// Structure for schema field definition
typedef struct {
  const char *name;
  int type;
} CField;

// Database handle
typedef void *HOCDBHandle;

/**
 * Initialize the database with dynamic schema and config
 * @param ticker Ticker symbol as null-terminated string
 * @param path Directory path for data as null-terminated string
 * @param schema Array of CField structs defining the schema
 * @param schema_len Number of fields in the schema
 * @param max_file_size Maximum file size (0 for default)
 * @param overwrite_on_full Whether to overwrite when full (1 for true, 0 for
 * false)
 * @param flush_on_write Whether to flush on every write (1 for true, 0 for
 * false)
 * @param auto_increment Whether to auto-increment timestamp (1 for true, 0 for
 * false)
 * @return Database handle or NULL on failure
 */
HOCDBHandle hocdb_init(const char *ticker, const char *path,
                       const CField *schema, size_t schema_len,
                       int64_t max_file_size, int overwrite_on_full,
                       int flush_on_write, int auto_increment);

/**
 * Append a raw record to the database
 * @param handle Database handle
 * @param data Pointer to raw data bytes
 * @param len Length of data in bytes
 * @return 0 on success, non-zero on failure
 */
int hocdb_append(HOCDBHandle handle, const void *data, size_t len);

/**
 * Flush the database (force write to disk)
 * @param handle Database handle
 * @return 0 on success, non-zero on failure
 */
int hocdb_flush(HOCDBHandle handle);

/**
 * Load all records into memory with zero-copy
 * @param handle Database handle
 * @param out_len Output parameter to store the number of bytes loaded
 * @return Pointer to raw data bytes (allocated with c_allocator, caller must
 * free with hocdb_free) Returns NULL on failure
 *
 * IMPORTANT: The returned pointer is valid only until the next operation on the
 * database or until the database is closed. The caller is responsible for
 * calling hocdb_free() to free the memory.
 */
void *hocdb_load(HOCDBHandle handle, size_t *out_len);

/**
 * Query records in a time range
 * @param handle Database handle
 * @param start_ts Start timestamp (inclusive)
 * @param end_ts End timestamp (exclusive)
 * @param out_len Output parameter to store the number of bytes loaded
 * @return Pointer to raw data bytes (allocated with c_allocator, caller must
 * free with hocdb_free) Returns NULL on failure
 */
typedef struct {
  size_t field_index;
  int type;
  int64_t val_i64;
  double val_f64;
  uint64_t val_u64;
  char val_string[128];
  bool val_bool;
} HOCDBFilter;

/**
 * Query records in a time range with optional filtering
 * @param handle Database handle
 * @param start_ts Start timestamp (inclusive)
 * @param end_ts End timestamp (exclusive)
 * @param filters Array of HOCDBFilter structs (can be NULL)
 * @param filters_len Number of filters
 * @param out_len Output parameter to store the number of bytes loaded
 * @return Pointer to raw data bytes (allocated with c_allocator, caller must
 * free with hocdb_free) Returns NULL on failure
 */
void *hocdb_query(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                  const HOCDBFilter *filters, size_t filters_len,
                  size_t *out_len);

typedef struct {
  double min;
  double max;
  double sum;
  uint64_t count;
  double mean;
  double p50;
  double p90;
  double p95;
  double p99;
} HOCDBStats;

#define HOCDB_STATS_PERCENTILES 1

int hocdb_get_stats(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                    size_t field_index, uint32_t flags, HOCDBStats *out_stats);
int hocdb_get_latest(HOCDBHandle handle, size_t field_index, double *out_val,
                     int64_t *out_ts);


/* ------------------------------------------------------------------------- */
/* Technical indicators and quantitative analytics                            */
/* ------------------------------------------------------------------------- */

/* Indicator kinds (stable ids). Names are also resolvable at runtime with
 * hocdb_indicator_kind_from_name("rsi"). */
enum {
  /* moving averages */
  HOCDB_IND_SMA = 1, HOCDB_IND_EMA = 2, HOCDB_IND_WMA = 3, HOCDB_IND_DEMA = 4,
  HOCDB_IND_TEMA = 5, HOCDB_IND_TRIMA = 6, HOCDB_IND_KAMA = 7, HOCDB_IND_HMA = 8,
  HOCDB_IND_ZLEMA = 9, HOCDB_IND_VWMA = 10, HOCDB_IND_RMA = 11,
  /* momentum */
  HOCDB_IND_RSI = 20, HOCDB_IND_MACD = 21, HOCDB_IND_PPO = 22, HOCDB_IND_STOCH = 23,
  HOCDB_IND_STOCH_RSI = 24, HOCDB_IND_CCI = 25, HOCDB_IND_WILLR = 26, HOCDB_IND_MOM = 27,
  HOCDB_IND_ROC = 28, HOCDB_IND_CMO = 29, HOCDB_IND_TRIX = 30, HOCDB_IND_ULTOSC = 31,
  HOCDB_IND_AO = 32, HOCDB_IND_TSI = 33, HOCDB_IND_BOP = 34, HOCDB_IND_DPO = 35,
  /* trend */
  HOCDB_IND_ADX = 40, HOCDB_IND_AROON = 41, HOCDB_IND_PSAR = 42, HOCDB_IND_SUPERTREND = 43,
  HOCDB_IND_VORTEX = 44, HOCDB_IND_ICHIMOKU = 45, HOCDB_IND_LINREG = 46,
  /* volatility */
  HOCDB_IND_ATR = 60, HOCDB_IND_NATR = 61, HOCDB_IND_TRUE_RANGE = 62, HOCDB_IND_BBANDS = 63,
  HOCDB_IND_KELTNER = 64, HOCDB_IND_DONCHIAN = 65, HOCDB_IND_STDDEV = 66, HOCDB_IND_VARIANCE = 67,
  HOCDB_IND_HIST_VOL = 68,
  /* volume */
  HOCDB_IND_OBV = 80, HOCDB_IND_VWAP = 81, HOCDB_IND_MFI = 82, HOCDB_IND_CMF = 83,
  HOCDB_IND_AD = 84, HOCDB_IND_ADOSC = 85, HOCDB_IND_EFI = 86,
  /* statistics / risk */
  HOCDB_IND_RETURNS = 100, HOCDB_IND_LOG_RETURNS = 101, HOCDB_IND_ZSCORE = 102,
  HOCDB_IND_PERCENT_RANK = 103, HOCDB_IND_ROLLING_MIN = 104, HOCDB_IND_ROLLING_MAX = 105,
  HOCDB_IND_DRAWDOWN = 106, HOCDB_IND_SHARPE = 107, HOCDB_IND_SORTINO = 108, HOCDB_IND_CORREL = 109,
  HOCDB_IND_BETA = 110, HOCDB_IND_SKEW = 111, HOCDB_IND_KURTOSIS = 112,
  /* price transforms */
  HOCDB_IND_TYPICAL_PRICE = 120, HOCDB_IND_MEDIAN_PRICE = 121, HOCDB_IND_HEIKIN_ASHI = 122,
  /* microstructure (ticks with bid / ask / side) */
  HOCDB_IND_SPREAD = 130, HOCDB_IND_ORDER_FLOW = 131, HOCDB_IND_TICK_PRESSURE = 132,
  HOCDB_IND_TRADE_INTENSITY = 133, HOCDB_IND_AMIHUD = 134, HOCDB_IND_REALIZED_VOL = 135,
  /* pairs / passthrough (second series = field_index2 or database B) */
  HOCDB_IND_SERIES = 140, HOCDB_IND_SERIES2 = 141, HOCDB_IND_RATIO = 142, HOCDB_IND_RATIO_ZSCORE = 143,
  HOCDB_IND_REL_STRENGTH = 144,
  /* labels: look-ahead by design (NaN at the end of every window) */
  HOCDB_IND_FORWARD_RETURN = 150, HOCDB_IND_TRIPLE_BARRIER = 151,
  /* session-anchored: param = session length, param2 = session offset (timestamp units) */
  HOCDB_IND_SESSION_VWAP = 160, HOCDB_IND_SESSION_RANGE = 161, HOCDB_IND_OPENING_RANGE = 162,
  HOCDB_IND_PIVOTS = 163
};

/* Field indices of the OHLCV roles (-1 = not available). `close` is required.
 * bid / ask / side (1 = buy, 0 = sell) are tick-level quotes used by the
 * microstructure kinds; side also yields per-bar buy volume when bucketing. */
typedef struct {
  int64_t open;
  int64_t high;
  int64_t low;
  int64_t close;
  int64_t volume;
  int64_t bid;
  int64_t ask;
  int64_t side;
} HOCDBIndicatorColumns;

/* One indicator request. Zero periods/params select the documented defaults
 * (e.g. RSI 14, MACD 12/26/9, BBANDS 20 x 2.0). field_index -1 uses the close
 * column; >= 0 runs a single-series indicator on that field. field_index2 is
 * the benchmark/second series for CORREL and BETA. */
typedef struct {
  uint32_t kind;
  uint32_t period;
  uint32_t period2;
  uint32_t period3;
  uint32_t period4;
  double param;   /* BBANDS k, KELTNER/SUPERTREND multiplier, PSAR accel,
                     periods-per-year for HIST_VOL/SHARPE/SORTINO/REALIZED_VOL,
                     timestamp units per second for TRADE_INTENSITY (1e6),
                     up-barrier fraction for TRIPLE_BARRIER (0.02),
                     session length for SESSION_* / PIVOTS (mandatory) */
  double param2;  /* PSAR max acceleration, down-barrier fraction, session offset */
  int64_t field_index;
  int64_t field_index2;
} HOCDBIndicatorSpec;

/* Batch result. `values` is planar: output k occupies
 * values[k*n_rows .. (k+1)*n_rows). Outputs are concatenated in spec order
 * (hocdb_indicator_output_count gives each spec's count). NaN marks warm-up. */
typedef struct {
  int64_t *timestamps;
  double *values;
  size_t n_rows;
  size_t n_outputs;
} HOCDBIndicatorResult;

/* Pass as `lookback` to use the recommended per-spec warm-up. */
#define HOCDB_LOOKBACK_AUTO SIZE_MAX

/**
 * Compute a batch of indicators over [start_ts, end_ts) in one pass.
 * @param lookback Extra records (bars when bucket > 0) read before the window
 *        so that the first in-window values are converged; HOCDB_LOOKBACK_AUTO
 *        picks the recommended amount. Warm-up rows are not returned.
 * @param bucket   0 = one row per record; > 0 = aggregate records into OHLCV
 *        bars of that many timestamp units first (tick -> bar). The window
 *        then selects the bars whose timestamp lies in [start_ts, end_ts);
 *        every returned bar is complete and warm-up counts existing bars.
 * @return 0 on success; -2 bad spec, -3 missing column, -4 bad field index,
 *         -5 field override with bucket, -1 out of memory. Free with
 *         hocdb_indicators_free.
 */
int hocdb_indicators(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                     const HOCDBIndicatorColumns *cols,
                     const HOCDBIndicatorSpec *specs, size_t n_specs,
                     size_t lookback, int64_t bucket, HOCDBIndicatorResult *out);

/** Same as hocdb_indicators for the last n_last records (or, with bucket > 0,
 * the last n_last existing bars: gaps between sessions do not count). */
int hocdb_indicators_tail(HOCDBHandle handle, size_t n_last,
                          const HOCDBIndicatorColumns *cols,
                          const HOCDBIndicatorSpec *specs, size_t n_specs,
                          size_t lookback, int64_t bucket, HOCDBIndicatorResult *out);

void hocdb_indicators_free(HOCDBIndicatorResult *result);

/* Registry helpers */
int hocdb_indicator_is_lookahead(uint32_t kind); /* 1 = uses future rows (labels) */
size_t hocdb_indicator_output_count(uint32_t kind);
const char *hocdb_indicator_output_name(uint32_t kind, size_t idx);
const char *hocdb_indicator_name(uint32_t kind);
uint32_t hocdb_indicator_kind_from_name(const char *name);
size_t hocdb_indicator_kinds(uint32_t *out, size_t cap);
size_t hocdb_indicator_warmup(const HOCDBIndicatorSpec *spec);

/* OHLCV bars produced by hocdb_ohlcv (free with hocdb_ohlcv_free). */
typedef struct {
  int64_t *timestamps;
  double *open;
  double *high;
  double *low;
  double *close;
  double *volume; /* record count when no volume field is given */
  double *count;
  size_t n_bars;
} HOCDBBars;

/**
 * Aggregate records in [start_ts, end_ts) into OHLCV bars of `bucket`
 * timestamp units using `price_field` (and `volume_field`, or -1).
 */
int hocdb_ohlcv(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                size_t price_field, int64_t volume_field, int64_t bucket,
                HOCDBBars *out);
void hocdb_ohlcv_free(HOCDBBars *bars);

/* Bars with per-bar buy volume (from a side field, 1 = buy). buy_volume is
 * NULL when side_field < 0. Free with hocdb_ohlcv_ex_free. */
typedef struct {
  int64_t *timestamps;
  double *open;
  double *high;
  double *low;
  double *close;
  double *volume;
  double *count;
  size_t n_bars;
  double *buy_volume;
} HOCDBBarsEx;

int hocdb_ohlcv_ex(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                   size_t price_field, int64_t volume_field, int64_t side_field,
                   int64_t bucket, HOCDBBarsEx *out);
void hocdb_ohlcv_ex_free(HOCDBBarsEx *bars);

/**
 * Indicators over database A (its `cols_a`) aligned with database B (whose
 * `cols_b->close` becomes the second input): with bucket > 0 both are
 * resampled and inner-joined on bar timestamps; on ticks B is as-of joined
 * onto A's rows (latest B row at or before each A row). Single-series kinds
 * run on A; SERIES2, RATIO, RATIO_ZSCORE, REL_STRENGTH, CORREL and BETA use
 * both. Free with hocdb_indicators_free.
 */
int hocdb_pair_indicators(HOCDBHandle a, const HOCDBIndicatorColumns *cols_a,
                          HOCDBHandle b, const HOCDBIndicatorColumns *cols_b,
                          int64_t start_ts, int64_t end_ts,
                          const HOCDBIndicatorSpec *specs, size_t n_specs,
                          size_t lookback, int64_t bucket, HOCDBIndicatorResult *out);
int hocdb_pair_indicators_tail(HOCDBHandle a, const HOCDBIndicatorColumns *cols_a,
                               HOCDBHandle b, const HOCDBIndicatorColumns *cols_b,
                               size_t n_last, const HOCDBIndicatorSpec *specs,
                               size_t n_specs, size_t lookback, int64_t bucket,
                               HOCDBIndicatorResult *out);

/* Data-quality statistics (decode generically with hocdb_health_field_*). */
typedef struct {
  uint64_t count;
  int64_t first_ts, last_ts, span;
  double mean_gap, median_gap;
  int64_t max_gap, max_gap_at;
  uint64_t n_gaps, n_nonpositive_price, n_nan_price, n_outlier_returns;
  int64_t first_outlier_at;
  double max_abs_return;
  uint64_t n_zero_volume, n_negative_volume;
  /* with a trading calendar: closed time inside [first, last] (timestamp units),
     gaps spanning a session boundary, sessions with no rows at all */
  int64_t closed_span;
  uint64_t n_session_breaks, n_missing_sessions;
} HOCDBHealth;

/**
 * @param gap_threshold gaps (timestamp units) above this are counted in n_gaps
 * @param outlier_threshold |log return| above this is counted as an outlier
 */
int hocdb_health(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                 size_t price_field, int64_t volume_field, int64_t gap_threshold,
                 double outlier_threshold, HOCDBHealth *out);
size_t hocdb_health_size(void);
size_t hocdb_health_field_count(void);
const char *hocdb_health_field_name(size_t idx);
size_t hocdb_health_field_offset(size_t idx);
int hocdb_health_field_type(size_t idx);

/* Decision evaluation: entry at the first price at or after `timestamp`,
 * exit at the first price at or after timestamp + horizon, cost_bps per side. */
typedef struct {
  int64_t timestamp;
  double direction; /* +1 long, -1 short, 0 flat (ignored) */
  double size;      /* position size in currency units */
  int64_t horizon;  /* timestamp units; 0 = default_horizon */
} HOCDBDecision;

typedef struct {
  uint64_t n_decisions, n_evaluated, n_long, n_short;
  double hit_rate, avg_return, avg_net_return, total_pnl, total_cost;
  double sharpe, profit_factor, max_drawdown, avg_win, avg_loss, best, worst;
  double long_hit_rate, short_hit_rate, long_avg_return, short_avg_return;
} HOCDBEvaluation;

/**
 * @param out_entry / out_exit / out_net optional arrays of n doubles receiving
 *        the entry price, exit price and net return per decision (NaN when
 *        the decision could not be evaluated).
 */
int hocdb_evaluate(HOCDBHandle handle, size_t price_field,
                   const HOCDBDecision *decisions, size_t n, int64_t default_horizon,
                   double cost_bps, HOCDBEvaluation *out, double *out_entry,
                   double *out_exit, double *out_net);
size_t hocdb_evaluation_size(void);
size_t hocdb_evaluation_field_count(void);
const char *hocdb_evaluation_field_name(size_t idx);
size_t hocdb_evaluation_field_offset(size_t idx);
int hocdb_evaluation_field_type(size_t idx);
size_t hocdb_decision_size(void);

/* Scalar performance / risk summary of a series. Fields are also
 * discoverable at runtime via hocdb_summary_field_* (count/name/offset/type). */
typedef struct {
  uint64_t count;
  double first, last, min, max, mean, std;
  double total_return, log_return, ann_return, ann_vol;
  double sharpe, sortino;
  double max_drawdown, max_drawdown_bars, calmar;
  double skew, kurtosis;
  double var_95, cvar_95;
  double win_rate, avg_gain, avg_loss, profit_factor, best, worst;
  double autocorr_1, hurst, half_life;
} HOCDBSummary;

int hocdb_summary(HOCDBHandle handle, int64_t start_ts, int64_t end_ts,
                  size_t field_index, double periods_per_year, HOCDBSummary *out);
size_t hocdb_summary_size(void);
size_t hocdb_summary_field_count(void);
const char *hocdb_summary_field_name(size_t idx);
size_t hocdb_summary_field_offset(size_t idx);
int hocdb_summary_field_type(size_t idx); /* 1 = int64, 2 = double, 3 = uint64 */

/* One-shot snapshot of ~100 indicators for the latest bar. Use the
 * hocdb_snapshot_field_* introspection functions to decode generically; the
 * struct layout is: int64_t timestamp; uint64_t bars; then doubles in the
 * order reported by hocdb_snapshot_field_name. */
typedef struct {
  int64_t timestamp;
  uint64_t bars;
  double open, high, low, close, volume;
  double sma_5, sma_10, sma_20, sma_50, sma_100, sma_200;
  double ema_9, ema_12, ema_21, ema_26, ema_50, ema_200;
  double wma_20, hma_20, vwma_20, kama_10, tema_20;
  double rsi_14, stoch_k, stoch_d, stochrsi_k, stochrsi_d;
  double macd, macd_signal, macd_hist, ppo, cci_20, williams_r_14, roc_10, mom_10,
      cmo_14, trix_15, ultosc, ao, tsi, tsi_signal;
  double adx_14, plus_di_14, minus_di_14, aroon_up_25, aroon_down_25, aroon_osc_25;
  double psar, psar_dir, supertrend, supertrend_dir, vortex_plus_14, vortex_minus_14;
  double ichimoku_tenkan, ichimoku_kijun, ichimoku_senkou_a, ichimoku_senkou_b;
  double linreg_value_20, linreg_slope_20, linreg_r2_20;
  double atr_14, natr_14, true_range;
  double bb_upper, bb_middle, bb_lower, bb_percent_b, bb_bandwidth;
  double keltner_upper, keltner_middle, keltner_lower;
  double donchian_upper_20, donchian_middle_20, donchian_lower_20;
  double stddev_20, hist_vol_20;
  double obv, vwap, mfi_14, cmf_20, ad, adosc, efi_13;
  double return_1, return_5, return_10, return_20, log_return_1;
  double zscore_20, percent_rank_20, high_20, low_20, high_250, low_250;
  double drawdown, sharpe_20, sortino_20, skew_20, kurtosis_20;
} HOCDBSnapshot;

/**
 * @param n_bars Records (or bars when bucket > 0) to use; 0 = recommended
 *        (2500, enough for every field to converge).
 * @param periods_per_year annualisation for volatility / Sharpe / Sortino.
 */
int hocdb_snapshot(HOCDBHandle handle, const HOCDBIndicatorColumns *cols,
                   size_t n_bars, int64_t bucket, double periods_per_year,
                   HOCDBSnapshot *out);
/**
 * Snapshots for several bar sizes from one read: out[k] covers the last
 * n_bars bars of buckets[k], annualised with periods_per_year[k].
 */
int hocdb_snapshot_multi(HOCDBHandle handle, const HOCDBIndicatorColumns *cols,
                         size_t n_bars, const int64_t *buckets, size_t n_buckets,
                         const double *periods_per_year, HOCDBSnapshot *out);
size_t hocdb_snapshot_size(void);
size_t hocdb_snapshot_field_count(void);
const char *hocdb_snapshot_field_name(size_t idx);
size_t hocdb_snapshot_field_offset(size_t idx);
int hocdb_snapshot_field_type(size_t idx);


/* ------------------------------------------------------------------------- */
/* Durability, readers, maintenance and metrics                              */
/* ------------------------------------------------------------------------- */

/* fsync policy for HOCDBConfig.fsync_policy */
#define HOCDB_FSYNC_NONE 0     /* never (the OS decides) */
#define HOCDB_FSYNC_ON_CLOSE 1 /* once on close (default) */
#define HOCDB_FSYNC_ON_FLUSH 2 /* after every flush */
#define HOCDB_FSYNC_INTERVAL 3 /* at most every fsync_interval_ms, and on close */

typedef struct {
  int64_t max_file_size;   /* 0 = default (2 GiB) */
  int overwrite_on_full;   /* ring buffer when full */
  int flush_on_write;
  int auto_increment;
  int fsync_policy;        /* HOCDB_FSYNC_* */
  uint32_t fsync_interval_ms; /* 0 = default 1000 */
  int verify_on_open;      /* recompute the checksum when opening */
  int64_t retention_span;  /* drop records older than last - span (timestamp units); 0 = off */
  uint64_t rollover_size;  /* archive the file above this many bytes; 0 = off */
  int auto_migrate;        /* rewrite legacy HOC1 files on open (recommended: 1) */
  uint64_t timestamp_unit_ns; /* ns per timestamp unit (1000 = microseconds); 0 = unknown. Persisted. */
  uint64_t index_stride;   /* 0 = default 1024 */
  uint32_t calendar;       /* trading calendar id (hocdb_calendar_id), 0 = none. Built-in ids are persisted. */
  uint32_t reserved0;
} HOCDBConfig;             /* 80 bytes */

/**
 * Open or create a database with the full configuration. Returns NULL on
 * failure; hocdb_last_error() names the error ("DatabaseLocked",
 * "SchemaMismatch", "ChecksumMismatch", ...). A writer holds an exclusive
 * lock; a second writer fails immediately instead of blocking.
 */
HOCDBHandle hocdb_init_ex(const char *ticker, const char *path, const CField *schema,
                          size_t schema_len, const HOCDBConfig *config);

/**
 * Attach as a lock-free reader to a database another process writes. Every
 * read re-reads the writer's committed cursor; appends, sync, compaction and
 * rollover fail with -10. Requires the current file format (legacy files are
 * migrated the first time a writer opens them).
 */
HOCDBHandle hocdb_open_reader(const char *ticker, const char *path, const CField *schema,
                              size_t schema_len);

const char *hocdb_last_error(void); /* error name of the last failed open on this thread */
size_t hocdb_header_size(void);     /* bytes reserved by the file header (ring capacity math) */
int hocdb_format_version(HOCDBHandle handle); /* 1 legacy, 2 current */
int hocdb_is_read_only(HOCDBHandle handle);

int hocdb_sync(HOCDBHandle handle);    /* flush + fsync now (-10 for readers) */
int hocdb_refresh(HOCDBHandle handle); /* readers: pick up the latest commit */
int hocdb_verify(HOCDBHandle handle);  /* 1 checksum ok, 0 MISMATCH, -20 unavailable */
int hocdb_compact(HOCDBHandle handle, int64_t min_ts); /* keep timestamp >= min_ts */
int hocdb_retain_last(HOCDBHandle handle, uint64_t n); /* keep the last n records */
int hocdb_rollover(HOCDBHandle handle, char *out_archive_path, size_t cap);

/* Operational counters; decode generically with hocdb_metrics_field_*. */
typedef struct {
  uint64_t appends, bytes_written, flushes, commits, fsyncs, fsync_ns_total, fsync_ns_max;
  uint64_t reads, read_ns_total, read_ns_max, read_ns_last, read_ns_p50, read_ns_p99, records_read;
  uint64_t refreshes, recovered_tail_records, dropped_tail_bytes, crc_failures, compactions,
      rollovers, migrations;
  int64_t last_append_wall_ns, last_commit_wall_ns, last_record_ts;
  int64_t ingest_lag_wall_ns;   /* now - last commit (readers) / last append (writers) */
  int64_t ingest_lag_record_ns; /* now - last record time, when timestamp_unit_ns is set */
  uint64_t committed_records, file_size, format_version, read_only;
} HOCDBMetrics;

int hocdb_metrics(HOCDBHandle handle, HOCDBMetrics *out);
void hocdb_metrics_reset(HOCDBHandle handle);
size_t hocdb_metrics_size(void);
size_t hocdb_metrics_field_count(void);
const char *hocdb_metrics_field_name(size_t idx);
size_t hocdb_metrics_field_offset(size_t idx);
int hocdb_metrics_field_type(size_t idx);

/**
 * Free memory allocated by hocdb_load
 * @param ptr Pointer returned by hocdb_load
 */
void hocdb_free(void *ptr);

/**
 * Get the index of a field by name
 * @param handle Database handle
 * @param field_name Field name as null-terminated string
 * @return Field index or -1 if not found
 */
int64_t hocdb_get_field_index(HOCDBHandle handle, const char *field_name);

/**
 * Close the database and delete the data file
 * @param handle Database handle
 */
void hocdb_drop(HOCDBHandle handle);

/**
 * Close and free the database handle
 * @param handle Database handle to close
 */
void hocdb_close(HOCDBHandle handle);

/* ------------------------------------------------------------------------- */
/* Trading calendars                                                         */
/*                                                                           */
/* Exchange sessions, holidays, early closes and daylight-saving rules. All  */
/* times are UTC seconds; database timestamps are converted with the         */
/* handle's timestamp unit. Built-in ids: 1 crypto (24/7), 2 fx (Sun 17:00 - */
/* Fri 17:00 New York), 3 nyse, 4 nasdaq, 5 lse, 6 cme (Globex equity index, */
/* approximation). A handle with a calendar and a timestamp unit gets:       */
/*   - session kinds (session_vwap, session_range, opening_range, pivots)    */
/*     with param = 0 use calendar sessions (pivots: previous trading day),  */
/*   - hocdb_health measures gaps in trading time (closed_span,              */
/*     n_session_breaks, n_missing_sessions),                                */
/*   - summary / snapshot / snapshot_multi with periods_per_year = 0 derive  */
/*     it from the calendar (hocdb_periods_per_year).                        */
/* Error codes: -30 CalendarRequired (param 0 without a calendar / unit),    */
/* -31 UnknownCalendar.                                                      */
/* ------------------------------------------------------------------------- */

#define HOCDB_CALENDAR_NONE 0
#define HOCDB_CALENDAR_CRYPTO 1
#define HOCDB_CALENDAR_FX 2
#define HOCDB_CALENDAR_NYSE 3
#define HOCDB_CALENDAR_NASDAQ 4
#define HOCDB_CALENDAR_LSE 5
#define HOCDB_CALENDAR_CME 6

#define HOCDB_DST_NONE 0
#define HOCDB_DST_US 1
#define HOCDB_DST_EU 2

typedef struct {
  int64_t open;        /* UTC seconds, inclusive */
  int64_t close;       /* UTC seconds, exclusive */
  int64_t trade_day;   /* days since 1970-01-01 (local trade date) */
  uint64_t early_close; /* 1 when the session closes early */
} HOCDBSession;

typedef struct {
  int32_t open_sec;  /* local seconds relative to the trade date's midnight (may be negative) */
  int32_t close_sec; /* close <= open means no session on that weekday */
} HOCDBDaySession;

typedef struct {
  int32_t day;       /* days since 1970-01-01 (local) */
  int32_t close_sec; /* close on that day */
} HOCDBEarlyClose;

uint32_t hocdb_calendar_id(const char *name);                       /* 0 = unknown */
int hocdb_calendar_name(uint32_t id, char *buf, size_t cap);         /* length, 0 unknown, -1 buffer too small */
/* which: 0 = session containing utc_sec, 1 = that or the previous, 2 = that or the next.
   Returns 1 (written), 0 (none), -31 (unknown id). */
int hocdb_calendar_session(uint32_t id, int64_t utc_sec, int which, HOCDBSession *out);
int hocdb_calendar_session_for_day(uint32_t id, int64_t day, HOCDBSession *out); /* 1 / 0 closed / -31 */
int hocdb_calendar_is_open(uint32_t id, int64_t utc_sec);            /* 1 / 0 / -31 */
int64_t hocdb_calendar_open_seconds(uint32_t id, int64_t a, int64_t b); /* trading seconds in [a, b) */
int64_t hocdb_calendar_sessions_between(uint32_t id, int64_t a, int64_t b); /* sessions opening in [a, b) */
double hocdb_calendar_periods_per_year(uint32_t id, double bucket_sec); /* bars per year for a bar length */
int64_t hocdb_calendar_to_local(uint32_t id, int64_t utc_sec);       /* local wall-clock seconds */
int64_t hocdb_days_from_civil(int64_t year, uint32_t month, uint32_t day);
void hocdb_civil_from_days(int64_t days, int64_t *year, uint32_t *month, uint32_t *day);
/* Register a custom calendar (process-local): weekly[7] Monday first. Returns the id (> 0),
   0 on invalid input, -1 when the registry (32 entries) is full. Redefining a name reuses its id. */
int64_t hocdb_calendar_define(const char *name, const HOCDBDaySession weekly[7], int32_t utc_offset_sec,
                              int dst_rule, const int32_t *holidays, size_t n_holidays,
                              const HOCDBEarlyClose *early_closes, size_t n_early, double sessions_per_year);

/* Per-handle calendar and timestamp unit (readers: local to the handle; writers persist
   built-in calendar ids and the unit in the file header). */
int hocdb_set_calendar(HOCDBHandle handle, uint32_t id);             /* 0 ok, -31 unknown */
uint32_t hocdb_get_calendar(HOCDBHandle handle);
int hocdb_set_timestamp_unit(HOCDBHandle handle, uint64_t unit_ns);
uint64_t hocdb_get_timestamp_unit(HOCDBHandle handle);
double hocdb_periods_per_year(HOCDBHandle handle, int64_t bucket);   /* 0 when calendar or unit unknown */

/* ------------------------------------------------------------------------- */
/* Universe (cross-sectional) features                                       */
/*                                                                           */
/* One call over a watch-list of databases (same column roles): the last     */
/* n_bars bars (bucket > 0; n_bars 0 = enough for the longest period) or     */
/* records of every database are inner-joined on timestamps, then per-ticker */
/* momentum / volatility / relative-strength percentile ranks, betas and     */
/* correlations to an (equal- or volume-weighted) market factor, and         */
/* universe-level dispersion / breadth / average pair correlation are        */
/* computed for the last bar. Decode rows and the summary generically with   */
/* the hocdb_universe_row_field_* / hocdb_universe_summary_field_*           */
/* introspection (types 1 int64, 2 double, 3 uint64). Semantics: see         */
/* src/universe.zig (strict rolling windows, pairwise-complete correlations,  */
/* NaN for tickers with too few bars, ranks in [0, 1] with 1 = highest).     */
/* ------------------------------------------------------------------------- */

typedef struct {
  uint64_t mom_short;   /* default 5 bars */
  uint64_t mom_mid;     /* 20 */
  uint64_t mom_long;    /* 60 */
  uint64_t vol_period;  /* 20 */
  uint64_t corr_period; /* 60 */
  uint64_t sma_period;  /* 50 */
  uint64_t beta_period; /* 60 */
  double periods_per_year; /* 0 = no annualisation of vol */
  uint64_t weights_mode;   /* 0 equal-weight market, 1 volume-weighted (needs volume columns) */
} HOCDBUniverseParams;

typedef struct {
  double last_close, ret_1, mom_short, mom_mid, mom_long, vol, sma_distance, beta, corr_market, rel_strength;
  double rank_mom_short, rank_mom_mid, rank_mom_long, rank_vol, rank_rel_strength, z_mom_mid, avg_corr, max_corr;
  uint64_t max_corr_index; /* index of the most correlated other ticker */
  double idio_vol, volume_ratio;
} HOCDBUniverseRow;

typedef struct {
  uint64_t n_tickers, n_bars; /* n_bars = joined bars actually used */
  double market_ret_1, market_mom_short, market_mom_mid, market_mom_long, market_vol;
  double dispersion, dispersion_mid, breadth_sma, breadth_up, avg_pair_corr, max_pair_corr, min_pair_corr;
  int64_t first_ts, last_ts;
} HOCDBUniverseSummary;

void hocdb_universe_params_default(HOCDBUniverseParams *out);
/* handles[n] databases, rows[n] out, corr n*n row-major or NULL. Errors: -3 no close column,
   -7 length mismatch, -2 bad params, -1 out of memory. */
int hocdb_universe(const HOCDBHandle *handles, size_t n, const HOCDBIndicatorColumns *cols, size_t n_bars,
                   int64_t bucket, const HOCDBUniverseParams *params, HOCDBUniverseRow *rows, double *corr,
                   HOCDBUniverseSummary *out);
/* The same on caller-provided aligned series: closes[n_tickers][n_bars], volumes may be NULL, ts may be NULL. */
int hocdb_universe_arrays(const double *const *closes, const double *const *volumes, size_t n_tickers, size_t n_bars,
                          const int64_t *ts, const HOCDBUniverseParams *params, HOCDBUniverseRow *rows, double *corr,
                          HOCDBUniverseSummary *out);
size_t hocdb_universe_params_size(void);
size_t hocdb_universe_row_size(void);
size_t hocdb_universe_row_field_count(void);
const char *hocdb_universe_row_field_name(size_t idx);
size_t hocdb_universe_row_field_offset(size_t idx);
int hocdb_universe_row_field_type(size_t idx);
size_t hocdb_universe_summary_size(void);
size_t hocdb_universe_summary_field_count(void);
const char *hocdb_universe_summary_field_name(size_t idx);
size_t hocdb_universe_summary_field_offset(size_t idx);
int hocdb_universe_summary_field_type(size_t idx);

/* ------------------------------------------------------------------------- */
/* Signal backtester                                                         */
/*                                                                           */
/* A target-position series over bars -> equity curve with costs, slippage,  */
/* stop-loss / take-profit / trailing exits, position limits, trade list and */
/* performance statistics; plus walk-forward split helpers. target[i] is the */
/* desired position at the END of bar i (units, fraction of equity or        */
/* notional per position_mode); fills happen at the next bar's open          */
/* (fill_mode 0, no look-ahead) or the same close (1); stopped positions are */
/* not re-entered on the same signal. Full semantics: src/backtest.zig.      */
/* Decode results / trades generically with hocdb_backtest_result_field_*   */
/* and hocdb_trade_field_* (types 1 int64, 2 double, 3 uint64).              */
/* ------------------------------------------------------------------------- */

typedef struct {
  double initial_equity;   /* <= 0 -> 1.0 */
  double cost_bps;         /* per side, on traded notional */
  double slippage_bps;     /* adverse price move per side */
  double stop_loss;        /* fraction of entry price, 0 = none */
  double take_profit;      /* fraction, 0 = none */
  double trailing_stop;    /* fraction from the best price since entry, 0 = none */
  double max_position;     /* cap on |units|, 0 = none */
  uint64_t position_mode;  /* 0 units, 1 fraction of equity, 2 notional */
  uint64_t fill_mode;      /* 0 next open, 1 same close */
  double periods_per_year; /* 0 = none (a database handle fills it from its calendar) */
  uint64_t allow_short;    /* 0 clamps negative targets to 0 */
  double risk_free_rate;   /* annual, for sharpe / sortino */
} HOCDBBacktestParams;

typedef struct {
  uint64_t n_bars, n_trades, n_long_trades, n_short_trades;
  double final_equity, total_return, ann_return, ann_vol, sharpe, sortino, calmar, max_drawdown;
  uint64_t max_drawdown_bars;
  double avg_drawdown, win_rate, profit_factor, avg_trade_return, avg_win, avg_loss, best_trade, worst_trade;
  double avg_holding_bars, exposure, long_share, turnover, total_cost, total_slippage;
  uint64_t n_stop_exits, n_take_profit_exits, n_trailing_exits;
  double gross_pnl, net_pnl;
} HOCDBBacktestResult;

typedef struct {
  int64_t entry_ts, exit_ts; /* exit_ts 0 = still open at the end */
  int64_t direction;         /* +1 long, -1 short */
  double entry_price, exit_price, size, pnl, ret;
  uint64_t bars;
  uint64_t exit_reason;      /* 0 signal, 1 stop_loss, 2 take_profit, 3 trailing, 4 end of data */
} HOCDBTrade;

typedef struct { double *equity, *position, *cash, *pnl, *drawdown; } HOCDBBacktestOutputs; /* each NULL or n */

typedef struct { uint64_t train_start, train_end, test_start, test_end; } HOCDBSplit; /* end exclusive */

void hocdb_backtest_params_default(HOCDBBacktestParams *out);
/* Exactly the rows hocdb_indicators(start_ts, end_ts, bucket) returns (bucket > 0: bars whose start
   lies in the window; equal to hocdb_ohlcv for bucket-aligned bounds; 0: raw records): compute the
   signals with hocdb_indicators over the same window and pass one target per row. n = target length
   must equal the row count (-7 otherwise).
   trades: up to trades_cap written, out->n_trades counts all. outputs may be NULL. */
int hocdb_backtest(HOCDBHandle handle, const HOCDBIndicatorColumns *cols, int64_t start_ts, int64_t end_ts,
                   int64_t bucket, const double *target, size_t n, const HOCDBBacktestParams *params,
                   const HOCDBBacktestOutputs *outputs, HOCDBTrade *trades, size_t trades_cap, HOCDBBacktestResult *out);
/* The last n bars (bucket > 0) or records. */
int hocdb_backtest_tail(HOCDBHandle handle, const HOCDBIndicatorColumns *cols, int64_t bucket, const double *target,
                        size_t n, const HOCDBBacktestParams *params, const HOCDBBacktestOutputs *outputs,
                        HOCDBTrade *trades, size_t trades_cap, HOCDBBacktestResult *out);
/* Caller-provided arrays (open / high / low may be NULL: fills at the close, no intrabar stops). */
int hocdb_backtest_arrays(const int64_t *ts, const double *open, const double *high, const double *low,
                          const double *close, size_t n, const double *target, const HOCDBBacktestParams *params,
                          const HOCDBBacktestOutputs *outputs, HOCDBTrade *trades, size_t trades_cap,
                          HOCDBBacktestResult *out);
/* Walk-forward ranges: the first train window is floor(train_frac * n) bars, the test windows tile the
   rest in n_splits pieces; anchored expands the train window from 0, else it rolls. Returns the count. */
size_t hocdb_walk_forward_splits(size_t n, size_t n_splits, double train_frac, int anchored, HOCDBSplit *out, size_t cap);
/* Run every test window independently (fresh equity each); returns the number run or a negative error. */
int hocdb_backtest_splits_arrays(const int64_t *ts, const double *open, const double *high, const double *low,
                                 const double *close, size_t n, const double *target, const HOCDBBacktestParams *params,
                                 const HOCDBSplit *splits, size_t n_splits, HOCDBBacktestResult *results);
size_t hocdb_backtest_params_size(void);
size_t hocdb_backtest_result_size(void);
size_t hocdb_backtest_result_field_count(void);
const char *hocdb_backtest_result_field_name(size_t idx);
size_t hocdb_backtest_result_field_offset(size_t idx);
int hocdb_backtest_result_field_type(size_t idx);
size_t hocdb_trade_size(void);
size_t hocdb_trade_field_count(void);
const char *hocdb_trade_field_name(size_t idx);
size_t hocdb_trade_field_offset(size_t idx);
int hocdb_trade_field_type(size_t idx);

#ifdef __cplusplus
}
#endif

#endif // HOCDB_H