import { dlopen, FFIType, suffix, ptr, toArrayBuffer } from "bun:ffi";
import { join } from "path";
import { unlinkSync, existsSync } from "node:fs";

// Locate the shared library
const libPath = join(import.meta.dir, "..", "..", "zig-out", "lib", `libhocdb_c.${suffix}`);

const { symbols } = dlopen(libPath, {
    hocdb_init: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.i64, FFIType.i32, FFIType.i32, FFIType.i32],
        returns: FFIType.ptr,
    },
    hocdb_append: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.u64],
        returns: FFIType.i32,
    },
    hocdb_close: { // This is the correct hocdb_close, returning void
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
    hocdb_flush: {
        args: [FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_load: {
        args: [FFIType.ptr, FFIType.ptr],
        returns: FFIType.ptr,
    },
    hocdb_query: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.ptr, FFIType.u64, FFIType.ptr],
        returns: FFIType.ptr,
    },
    hocdb_query_into: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64],
        returns: FFIType.i64, // Returns bytes written or error code
    },
    hocdb_get_stats: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.u64, FFIType.u32, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_get_latest: {
        args: [FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_free: {
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
    hocdb_drop: {
        args: [FFIType.ptr],
        returns: FFIType.i32,
    },
    // --- Technical indicators and quantitative analytics (see bindings/c/hocdb.h) ---
    hocdb_indicators: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.i64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_indicators_tail: {
        args: [FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.i64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_indicators_free: {
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
    hocdb_indicator_output_count: {
        args: [FFIType.u32],
        returns: FFIType.u64,
    },
    hocdb_indicator_output_name: {
        args: [FFIType.u32, FFIType.u64],
        returns: FFIType.cstring, // null when idx is out of range
    },
    hocdb_indicator_name: {
        args: [FFIType.u32],
        returns: FFIType.cstring, // null for an unknown kind
    },
    hocdb_indicator_kind_from_name: {
        args: [FFIType.ptr],
        returns: FFIType.u32, // 0 = unknown
    },
    hocdb_indicator_kinds: {
        args: [FFIType.ptr, FFIType.u64],
        returns: FFIType.u64,
    },
    hocdb_indicator_warmup: {
        args: [FFIType.ptr],
        returns: FFIType.u64,
    },
    hocdb_indicator_is_lookahead: {
        args: [FFIType.u32],
        returns: FFIType.i32, // 1 = uses future rows (labels)
    },
    // bars with optional per-bar buy volume (HOCDBBarsEx); used for every ohlcv() call
    hocdb_ohlcv_ex: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.u64, FFIType.i64, FFIType.i64, FFIType.i64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_ohlcv_ex_free: {
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
    // pairs: database A (its columns) aligned with database B (its columns)
    hocdb_pair_indicators: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.i64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_pair_indicators_tail: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.i64, FFIType.ptr],
        returns: FFIType.i32,
    },
    // data-quality statistics (HOCDBHealth, decoded through introspection)
    hocdb_health: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.u64, FFIType.i64, FFIType.i64, FFIType.f64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_health_size: { args: [], returns: FFIType.u64 },
    hocdb_health_field_count: { args: [], returns: FFIType.u64 },
    hocdb_health_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_health_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_health_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    // decision evaluation (HOCDBDecision[] in, HOCDBEvaluation + per-decision arrays out)
    hocdb_evaluate: {
        args: [FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.i64, FFIType.f64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_evaluation_size: { args: [], returns: FFIType.u64 },
    hocdb_evaluation_field_count: { args: [], returns: FFIType.u64 },
    hocdb_evaluation_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_evaluation_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_evaluation_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    hocdb_decision_size: { args: [], returns: FFIType.u64 },
    hocdb_summary: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.u64, FFIType.f64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_summary_size: { args: [], returns: FFIType.u64 },
    hocdb_summary_field_count: { args: [], returns: FFIType.u64 },
    hocdb_summary_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_summary_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_summary_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    hocdb_snapshot: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.i64, FFIType.f64, FFIType.ptr],
        returns: FFIType.i32,
    },
    // snapshots for several bar sizes from one read (out = n_buckets x HOCDBSnapshot)
    hocdb_snapshot_multi: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_snapshot_size: { args: [], returns: FFIType.u64 },
    hocdb_snapshot_field_count: { args: [], returns: FFIType.u64 },
    hocdb_snapshot_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_snapshot_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_snapshot_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    // --- Durability, lock-free readers, maintenance and metrics (see bindings/c/hocdb.h) ---
    hocdb_init_ex: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr], // ticker, path, schema, n, HOCDBConfig*
        returns: FFIType.ptr, // NULL on failure: hocdb_last_error() names the error
    },
    hocdb_open_reader: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64],
        returns: FFIType.ptr,
    },
    hocdb_last_error: { args: [], returns: FFIType.cstring }, // error name of the last failed open on this thread
    hocdb_header_size: { args: [], returns: FFIType.u64 },
    hocdb_format_version: { args: [FFIType.ptr], returns: FFIType.i32 }, // 1 legacy, 2 current
    hocdb_is_read_only: { args: [FFIType.ptr], returns: FFIType.i32 },
    hocdb_sync: { args: [FFIType.ptr], returns: FFIType.i32 },
    hocdb_refresh: { args: [FFIType.ptr], returns: FFIType.i32 },
    hocdb_verify: { args: [FFIType.ptr], returns: FFIType.i32 }, // 1 ok, 0 mismatch, -20 unavailable
    hocdb_compact: { args: [FFIType.ptr, FFIType.i64], returns: FFIType.i32 },
    hocdb_retain_last: { args: [FFIType.ptr, FFIType.u64], returns: FFIType.i32 },
    hocdb_rollover: { args: [FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.i32 }, // out_path, cap
    hocdb_metrics: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    hocdb_metrics_reset: { args: [FFIType.ptr], returns: FFIType.void },
    hocdb_metrics_size: { args: [], returns: FFIType.u64 },
    hocdb_metrics_field_count: { args: [], returns: FFIType.u64 },
    hocdb_metrics_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_metrics_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_metrics_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    // --- Trading calendars (see bindings/c/hocdb.h): all times are UTC seconds ---
    hocdb_calendar_id: { args: [FFIType.ptr], returns: FFIType.u32 }, // 0 = unknown
    hocdb_calendar_name: { args: [FFIType.u32, FFIType.ptr, FFIType.u64], returns: FFIType.i32 }, // length, 0 unknown, -1 buffer too small
    hocdb_calendar_session: { args: [FFIType.u32, FFIType.i64, FFIType.i32, FFIType.ptr], returns: FFIType.i32 }, // 1 written, 0 none, -31 unknown id
    hocdb_calendar_session_for_day: { args: [FFIType.u32, FFIType.i64, FFIType.ptr], returns: FFIType.i32 },
    hocdb_calendar_is_open: { args: [FFIType.u32, FFIType.i64], returns: FFIType.i32 },
    hocdb_calendar_open_seconds: { args: [FFIType.u32, FFIType.i64, FFIType.i64], returns: FFIType.i64 },
    hocdb_calendar_sessions_between: { args: [FFIType.u32, FFIType.i64, FFIType.i64], returns: FFIType.i64 },
    hocdb_calendar_periods_per_year: { args: [FFIType.u32, FFIType.f64], returns: FFIType.f64 },
    hocdb_calendar_to_local: { args: [FFIType.u32, FFIType.i64], returns: FFIType.i64 },
    hocdb_days_from_civil: { args: [FFIType.i64, FFIType.u32, FFIType.u32], returns: FFIType.i64 },
    hocdb_civil_from_days: { args: [FFIType.i64, FFIType.ptr, FFIType.ptr, FFIType.ptr], returns: FFIType.void },
    // name, HOCDBDaySession[7], utc_offset_sec, dst_rule, holidays, n_holidays, early_closes, n_early, sessions_per_year
    hocdb_calendar_define: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.i32, FFIType.i32, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.f64],
        returns: FFIType.i64, // id > 0, 0 invalid input, -1 registry full
    },
    hocdb_set_calendar: { args: [FFIType.ptr, FFIType.u32], returns: FFIType.i32 }, // 0 ok, -31 unknown
    hocdb_get_calendar: { args: [FFIType.ptr], returns: FFIType.u32 },
    hocdb_set_timestamp_unit: { args: [FFIType.ptr, FFIType.u64], returns: FFIType.i32 },
    hocdb_get_timestamp_unit: { args: [FFIType.ptr], returns: FFIType.u64 },
    hocdb_periods_per_year: { args: [FFIType.ptr, FFIType.i64], returns: FFIType.f64 }, // 0 when calendar or unit unknown
    // --- Universe (cross-sectional) features ---
    hocdb_universe_params_default: { args: [FFIType.ptr], returns: FFIType.void },
    // handles[n], n, HOCDBIndicatorColumns*, n_bars, bucket, HOCDBUniverseParams*, rows[n], corr[n*n] | NULL, HOCDBUniverseSummary*
    hocdb_universe: {
        args: [FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.i64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr],
        returns: FFIType.i32,
    },
    // closes[n_tickers], volumes[n_tickers] | NULL, n_tickers, n_bars, ts | NULL, params, rows, corr | NULL, summary
    hocdb_universe_arrays: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_universe_params_size: { args: [], returns: FFIType.u64 },
    hocdb_universe_row_size: { args: [], returns: FFIType.u64 },
    hocdb_universe_row_field_count: { args: [], returns: FFIType.u64 },
    hocdb_universe_row_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_universe_row_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_universe_row_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    hocdb_universe_summary_size: { args: [], returns: FFIType.u64 },
    hocdb_universe_summary_field_count: { args: [], returns: FFIType.u64 },
    hocdb_universe_summary_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_universe_summary_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_universe_summary_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    // --- Signal backtester ---
    hocdb_backtest_params_default: { args: [FFIType.ptr], returns: FFIType.void },
    // handle, cols, start, end, bucket, target, n, params, outputs | NULL, trades | NULL, trades_cap, result
    hocdb_backtest: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.i64, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr],
        returns: FFIType.i32,
    },
    // handle, cols, bucket, target, n, params, outputs | NULL, trades | NULL, trades_cap, result
    hocdb_backtest_tail: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.i64, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr],
        returns: FFIType.i32,
    },
    // ts, open | NULL, high | NULL, low | NULL, close, n, target, params, outputs | NULL, trades | NULL, trades_cap, result
    hocdb_backtest_arrays: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr],
        returns: FFIType.i32,
    },
    hocdb_walk_forward_splits: { args: [FFIType.u64, FFIType.u64, FFIType.f64, FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.u64 },
    // ts, open | NULL, high | NULL, low | NULL, close, n, target, params, splits, n_splits, results[n_splits]
    hocdb_backtest_splits_arrays: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr],
        returns: FFIType.i32, // number of splits run, or a negative error
    },
    hocdb_backtest_params_size: { args: [], returns: FFIType.u64 },
    hocdb_backtest_result_size: { args: [], returns: FFIType.u64 },
    hocdb_backtest_result_field_count: { args: [], returns: FFIType.u64 },
    hocdb_backtest_result_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_backtest_result_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_backtest_result_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
    hocdb_trade_size: { args: [], returns: FFIType.u64 },
    hocdb_trade_field_count: { args: [], returns: FFIType.u64 },
    hocdb_trade_field_name: { args: [FFIType.u64], returns: FFIType.cstring },
    hocdb_trade_field_offset: { args: [FFIType.u64], returns: FFIType.u64 },
    hocdb_trade_field_type: { args: [FFIType.u64], returns: FFIType.i32 },
});

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** When the data file is fsync'ed (HOCDB_FSYNC_* in hocdb.h): "none" 0, "on_close" 1, "on_flush" 2, "interval" 3. */
export type FsyncPolicy = "none" | "on_close" | "on_flush" | "interval" | 0 | 1 | 2 | 3;

/**
 * Database options (HOCDBConfig). Every option is also accepted under its
 * camelCase name (`maxFileSize`, `fsyncIntervalMs`, `verifyOnOpen`, ...).
 */
export interface DBConfig {
    max_file_size?: number | bigint;    // 0 / omitted = default (2 GiB); ring capacity = (max_file_size - 64) / record_size
    overwrite_on_full?: boolean;        // ring buffer when full (default true)
    flush_on_write?: boolean;
    auto_increment?: boolean;
    fsync?: FsyncPolicy;                // default "on_close"
    fsync_interval_ms?: number;         // for fsync: "interval"; 0 = 1000
    verify_on_open?: boolean;           // recompute the checksum when opening; a mismatch fails the open with ChecksumMismatch
    retention_span?: number | bigint;   // drop records older than last - span (timestamp units); 0 = off
    rollover_size?: number | bigint;    // archive the file above this many bytes; 0 = off
    auto_migrate?: boolean;             // rewrite legacy HOC1 files on open (default true)
    timestamp_unit_ns?: number | bigint; // ns per timestamp unit for ingest-lag metrics; 0 = unknown
    index_stride?: number | bigint;     // 0 = default 1024
    calendar?: number | string;         // trading calendar: id (1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme, custom ids) or name; 0 / omitted = none. Persisted with timestamp_unit_ns
    read_only?: boolean;                // attach as a lock-free reader (what HOCDB.openReader does)
    // camelCase aliases
    maxFileSize?: number | bigint;
    overwriteOnFull?: boolean;
    flushOnWrite?: boolean;
    autoIncrement?: boolean;
    fsync_policy?: FsyncPolicy;
    fsyncPolicy?: FsyncPolicy;
    fsyncIntervalMs?: number;
    verifyOnOpen?: boolean;
    retentionSpan?: number | bigint;
    rolloverSize?: number | bigint;
    autoMigrate?: boolean;
    timestampUnitNs?: number | bigint;
    indexStride?: number | bigint;
    readOnly?: boolean;
}

/**
 * The 30 HOCDBMetrics counters decoded by name. Same convention as `summary()`:
 * uint64 counters are `number`s, the int64 wall-clock / record timestamps and
 * lags are `bigint`s.
 */
export interface MetricsResult {
    appends: number;
    bytes_written: number;
    flushes: number;
    commits: number;
    fsyncs: number;
    fsync_ns_total: number;
    fsync_ns_max: number;
    reads: number;
    read_ns_total: number;
    read_ns_max: number;
    read_ns_last: number;
    read_ns_p50: number;
    read_ns_p99: number;
    records_read: number;
    refreshes: number;
    recovered_tail_records: number;
    dropped_tail_bytes: number;
    crc_failures: number;
    compactions: number;
    rollovers: number;
    migrations: number;
    last_append_wall_ns: bigint;
    last_commit_wall_ns: bigint;
    last_record_ts: bigint;
    ingest_lag_wall_ns: bigint;     // now - last commit (readers) / last append (writers)
    ingest_lag_record_ns: bigint;   // now - last record time, when timestamp_unit_ns is set
    committed_records: number;
    file_size: number;
    format_version: number;         // 1 legacy, 2 current
    read_only: number;              // 1 for readers
    [field: string]: number | bigint;
}

export interface FieldDef {
    name: string;
    type: 'i64' | 'f64' | 'u64' | 'bool';
}

export interface Filter {
    field_index: number;
    value: number | bigint | string;
}

interface SchemaInfo {
    recordSize: number;
    fieldOffsets: Record<string, { offset: number, type: string, index: number }>;
}

// ---------------------------------------------------------------------------
// Technical indicators and quantitative analytics
// ---------------------------------------------------------------------------

/** A schema field reference: field name or index. `null` / -1 = explicitly absent. */
export type FieldRef = string | number | null;

/**
 * Which schema fields play the OHLCV / quote roles. Roles that are not given
 * are auto-detected by name: open/high/low/close/volume (a field named `price`
 * is used as close when there is no `close`; `size` or `qty` as volume when
 * there is no `volume`) and the tick-level bid/ask/side (1 = buy, 0 = sell)
 * used by the microstructure kinds. Pass `null` (or -1) to mark a role as
 * absent and skip auto-detection for it.
 */
export interface IndicatorColumns {
    open?: FieldRef;
    high?: FieldRef;
    low?: FieldRef;
    close?: FieldRef;
    volume?: FieldRef;
    bid?: FieldRef;
    ask?: FieldRef;
    side?: FieldRef;
}

/**
 * One indicator request. Omitted / zero periods and params select the
 * documented defaults (RSI 14, MACD 12/26/9, BBANDS 20 x 2.0, ...).
 */
export interface IndicatorSpec {
    kind: string | number;      // kind name ("rsi", case-insensitive) or stable id (20)
    period?: number;
    period2?: number;
    period3?: number;
    period4?: number;
    param?: number;             // BBANDS k, KELTNER/SUPERTREND multiplier, PSAR accel, periods/year for HIST_VOL/SHARPE/SORTINO/REALIZED_VOL,
                                // timestamp units per second for TRADE_INTENSITY (default 1e6), up-barrier fraction for TRIPLE_BARRIER (0.02),
                                // session length in timestamp units for SESSION_VWAP/SESSION_RANGE/OPENING_RANGE/PIVOTS (mandatory)
    param2?: number;            // PSAR max acceleration, TRIPLE_BARRIER down-barrier fraction (= param), session offset in timestamp units
    field?: string | number;    // run on this field instead of the close column
    field2?: string | number;   // second series for CORREL / BETA / SERIES2 / RATIO / RATIO_ZSCORE / REL_STRENGTH (single-database calls)
    label?: string;             // output column name; default: kind name plus "_period" when a period is given
}

export interface IndicatorOptions {
    start?: number | bigint;    // window start (inclusive); default: everything
    end?: number | bigint;      // window end (exclusive)
    tail?: number;              // last n rows (or bars when bucket > 0) instead of start/end
    columns?: IndicatorColumns;
    lookback?: "auto" | number; // extra rows read before the window for warm-up; default "auto"
    bucket?: number | bigint;   // 0 = one row per record; > 0 = aggregate records into OHLCV bars of this size first
}

/** Options of `pairIndicators`: the same as `indicators` plus the column roles of the other database. */
export interface PairIndicatorOptions extends IndicatorOptions {
    columns2?: IndicatorColumns;      // roles of the other database (its close is the second series); auto-detected by default
    otherColumns?: IndicatorColumns;  // alias of columns2
}

export interface IndicatorResult {
    timestamps: BigInt64Array;
    n_rows: number;
    n_outputs: number;
    names: string[];                        // column names in output order
    columns: Record<string, Float64Array>;  // one array per output; NaN marks the warm-up region
    values: Float64Array;                   // raw planar buffer: output k = values[k*n_rows .. (k+1)*n_rows)
}

export interface OhlcvOptions {
    price?: FieldRef;   // default: field named "close", else "price"
    volume?: FieldRef;  // default: field named "volume" (else "size" / "qty") if present; null = none (volume = record count)
    side?: FieldRef;    // trade side field (1 = buy); when given the result also carries per-bar `buy_volume`
}

export interface OhlcvResult {
    timestamps: BigInt64Array;
    n_bars: number;
    open: Float64Array;
    high: Float64Array;
    low: Float64Array;
    close: Float64Array;
    volume: Float64Array;
    count: Float64Array;
    buy_volume?: Float64Array;  // only when `side` was given
}

/** The 29 HOCDBSummary fields, decoded by name (count, first, last, ..., sharpe, max_drawdown, ...). */
export interface SummaryResult {
    count: number;
    [field: string]: number;
}

export interface SnapshotOptions {
    columns?: IndicatorColumns;
    bars?: number;              // records (or bars when bucket > 0) to use; 0 = recommended (2500)
    bucket?: number | bigint;
    periodsPerYear?: number;    // annualisation for volatility / Sharpe / Sortino
}

/** All HOCDBSnapshot fields decoded by name: `timestamp` (bigint), `bars` (number), everything else double. */
export interface SnapshotResult {
    timestamp: bigint;
    bars: number;
    [field: string]: number | bigint;
}

export interface SnapshotMultiOptions {
    buckets: (number | bigint)[];  // one snapshot per bar size, in this order
    periodsPerYear?: number[];     // annualisation per bucket (same length); default 0 for every bucket
    bars?: number;                 // bars per snapshot; 0 = recommended (2500)
    columns?: IndicatorColumns;
}

/**
 * The HOCDBHealth data-quality statistics decoded by name. Timestamps and
 * gaps in timestamp units are `bigint` (int64); counts and doubles are `number`.
 */
export interface HealthResult {
    count: number;
    first_ts: bigint;
    last_ts: bigint;
    span: bigint;
    mean_gap: number;
    median_gap: number;
    max_gap: bigint;
    max_gap_at: bigint;
    n_gaps: number;
    n_nonpositive_price: number;
    n_nan_price: number;
    n_outlier_returns: number;
    first_outlier_at: bigint;
    max_abs_return: number;
    n_zero_volume: number;
    n_negative_volume: number;
    closed_span: bigint;
    n_session_breaks: number;
    n_missing_sessions: number;
    [field: string]: number | bigint;
}

/** One trading decision for `evaluate`: entry at the first price at or after `timestamp`, exit `horizon` later. */
export interface Decision {
    timestamp: number | bigint;
    direction: number;          // +1 long, -1 short, 0 flat (ignored)
    size?: number;              // position size in currency units; default 1
    horizon?: number | bigint;  // timestamp units; 0 / omitted = options.defaultHorizon
}

export interface EvaluateOptions {
    priceField?: FieldRef;          // default: field named "close", else "price"
    defaultHorizon?: number | bigint;  // horizon for decisions without one (timestamp units)
    costBps?: number;               // transaction cost per side in basis points
}

/** HOCDBEvaluation decoded by name plus the per-decision arrays (NaN where a decision could not be evaluated). */
export interface EvaluationResult {
    n_decisions: number;
    n_evaluated: number;
    n_long: number;
    n_short: number;
    hit_rate: number;
    avg_return: number;
    avg_net_return: number;
    total_pnl: number;
    total_cost: number;
    sharpe: number;
    profit_factor: number;
    max_drawdown: number;
    avg_win: number;
    avg_loss: number;
    best: number;
    worst: number;
    long_hit_rate: number;
    short_hit_rate: number;
    long_avg_return: number;
    short_avg_return: number;
    entry: Float64Array;        // entry price per decision
    exit: Float64Array;         // exit price per decision
    net_return: Float64Array;   // net return per decision (after costs)
    [field: string]: number | Float64Array;
}

// ---------------------------------------------------------------------------
// Trading calendars, signal backtester and universe features (round 4)
// ---------------------------------------------------------------------------

/** A calendar reference: the numeric id (1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme, custom ids >= 32) or its name. */
export type CalendarRef = number | string;

/** One resolved trading session. All times are UTC seconds; `trade_day` is days since 1970-01-01 (local trade date). */
export interface CalendarSession {
    open: number;         // UTC seconds, inclusive
    close: number;        // UTC seconds, exclusive
    trade_day: number;    // days since 1970-01-01 (the local date the session belongs to)
    early_close: boolean; // true when the session closes early
}

/** Which session `calendarSession` looks up: the one containing the instant (0), that or the previous (1), that or the next (2). */
export type SessionWhich = 0 | 1 | 2 | "at" | "prev" | "next";

/** One weekday's trading window in local seconds relative to the trade date's midnight (open may be negative, close may exceed 86400). */
export interface DaySession {
    open_sec: number;
    close_sec: number;   // close <= open means no session on that weekday
    openSec?: number;    // camelCase aliases
    closeSec?: number;
}

/** An early close: `day` (days since 1970-01-01, local) closes at `close_sec` local seconds. */
export interface EarlyClose {
    day: number;
    close_sec: number;
    closeSec?: number;
}

/** Daylight-saving rule of a custom calendar: "none" (0), "us" (1: second Sunday of March - first Sunday of November), "eu" (2: last Sunday of March - last Sunday of October). */
export type DstRule = "none" | "us" | "eu" | 0 | 1 | 2;

/** Definition of a custom calendar for `HOCDB.calendarDefine(name, definition)`. */
export interface CalendarDefinition {
    weekly: (DaySession | null)[];  // 7 entries, Monday first; null = no session on that weekday
    utc_offset_sec?: number;        // standard (non-DST) offset of the local time zone, seconds east of UTC (default 0)
    dst_rule?: DstRule;             // default "none"
    holidays?: number[];            // full closures, days since 1970-01-01 (HOCDB.daysFromCivil)
    early_closes?: EarlyClose[];
    sessions_per_year: number;      // sessions per year (annualisation), > 0
    // camelCase aliases
    utcOffsetSec?: number;
    dstRule?: DstRule;
    earlyCloses?: EarlyClose[];
    sessionsPerYear?: number;
}

export interface CivilDate {
    year: number;
    month: number;  // 1-12
    day: number;    // 1-31
}

/** How `target` values are interpreted: 0 units, 1 fraction of current equity, 2 notional in currency. */
export type PositionMode = 0 | 1 | 2 | "units" | "fraction" | "notional";
/** When a change of target is filled: 0 at the next bar's open (no look-ahead), 1 at the same bar's close. */
export type FillMode = 0 | 1 | "next_open" | "same_close";

/**
 * Backtester parameters (HOCDBBacktestParams). Omitted entries take the
 * library defaults (`HOCDB.backtestDefaults()`: initial_equity 1, no costs /
 * stops, units, next-open fills, periods_per_year 0, shorts allowed, rf 0).
 * Every entry is also accepted under its camelCase name.
 */
export interface BacktestParams {
    initial_equity?: number;    // <= 0 -> 1.0
    cost_bps?: number;          // per side, on traded notional
    slippage_bps?: number;      // adverse price move per side
    stop_loss?: number;         // fraction of the entry price, 0 = none
    take_profit?: number;       // fraction, 0 = none
    trailing_stop?: number;     // fraction from the best price since entry, 0 = none
    max_position?: number;      // cap on |units|, 0 = none
    position_mode?: PositionMode;
    fill_mode?: FillMode;
    periods_per_year?: number;  // 0 = none (db.backtest / backtestTail derive it from the handle's calendar)
    allow_short?: boolean;      // false clamps negative targets to 0
    risk_free_rate?: number;    // annual, for sharpe / sortino
    // camelCase aliases
    initialEquity?: number;
    costBps?: number;
    slippageBps?: number;
    stopLoss?: number;
    takeProfit?: number;
    trailingStop?: number;
    maxPosition?: number;
    positionMode?: PositionMode;
    fillMode?: FillMode;
    periodsPerYear?: number;
    allowShort?: boolean;
    riskFreeRate?: number;
}

/** The 32 HOCDBBacktestResult fields decoded by name (uint64 counts and doubles are all `number`s). */
export interface BacktestResult {
    n_bars: number;
    n_trades: number;           // counts every trade, even those beyond `maxTrades`
    n_long_trades: number;
    n_short_trades: number;
    final_equity: number;
    total_return: number;
    ann_return: number;
    ann_vol: number;
    sharpe: number;
    sortino: number;
    calmar: number;
    max_drawdown: number;       // positive fraction below the running peak
    max_drawdown_bars: number;
    avg_drawdown: number;
    win_rate: number;
    profit_factor: number;
    avg_trade_return: number;
    avg_win: number;
    avg_loss: number;
    best_trade: number;
    worst_trade: number;
    avg_holding_bars: number;
    exposure: number;
    long_share: number;
    turnover: number;
    total_cost: number;
    total_slippage: number;
    n_stop_exits: number;
    n_take_profit_exits: number;
    n_trailing_exits: number;
    gross_pnl: number;
    net_pnl: number;
    [field: string]: number;
}

/**
 * One trade (HOCDBTrade) decoded by name. Same convention as `summary()` /
 * `metrics()`: the int64 fields `entry_ts`, `exit_ts` and `direction` are
 * `bigint`s (`direction` is `1n` long / `-1n` short; `exit_ts` is `0n` while
 * the trade is still open at the end), everything else is a `number`.
 */
export interface Trade {
    entry_ts: bigint;
    exit_ts: bigint;
    direction: bigint;
    entry_price: number;
    exit_price: number;
    size: number;
    pnl: number;
    ret: number;
    bars: number;
    exit_reason: number;        // 0 signal, 1 stop_loss, 2 take_profit, 3 trailing, 4 end of data
    [field: string]: number | bigint;
}

/** Per-bar series the backtester can fill in: pass the names you want in `outputs`. */
export type BacktestOutput = "equity" | "position" | "cash" | "pnl" | "drawdown";

/** Options shared by every backtest entry point. */
export interface BacktestArraysOptions {
    params?: BacktestParams;
    outputs?: BacktestOutput[];     // per-bar arrays to return (each one Float64Array of n entries)
    maxTrades?: number;             // return up to this many trades in `trades` (result.n_trades counts all); omitted = no trade list
    max_trades?: number;
}

/** Options of `db.backtest` / `db.backtestTail`: the array options plus the window / column roles. */
export interface BacktestOptions extends BacktestArraysOptions {
    bucket?: number | bigint;       // 0 = one row per record; > 0 = bars of this many timestamp units (as `indicators`)
    columns?: IndicatorColumns;     // OHLCV roles (auto-detected by default)
}

/** What a backtest returns: the statistics, the trade list when `maxTrades` was given and the requested per-bar outputs. */
export interface BacktestRun {
    result: BacktestResult;
    trades?: Trade[];
    equity?: Float64Array;
    position?: Float64Array;
    cash?: Float64Array;
    pnl?: Float64Array;
    drawdown?: Float64Array;
}

/** A walk-forward split (bar indices, ends exclusive). */
export interface Split {
    train_start: number;
    train_end: number;
    test_start: number;
    test_end: number;
}

/** 0 / "equal": equal-weight market factor; 1 / "volume": volume-weighted (needs volumes). */
export type WeightsMode = 0 | 1 | "equal" | "volume";

/**
 * Universe parameters (HOCDBUniverseParams); omitted entries take the library
 * defaults (`HOCDB.universeDefaults()`: 5 / 20 / 60 momentum, vol 20, corr 60,
 * sma 50, beta 60, no annualisation, equal weights). camelCase names are accepted.
 */
export interface UniverseParams {
    mom_short?: number;
    mom_mid?: number;
    mom_long?: number;
    vol_period?: number;
    corr_period?: number;
    sma_period?: number;
    beta_period?: number;
    periods_per_year?: number;  // 0 = no annualisation of vol
    weights_mode?: WeightsMode;
    // camelCase aliases
    momShort?: number;
    momMid?: number;
    momLong?: number;
    volPeriod?: number;
    corrPeriod?: number;
    smaPeriod?: number;
    betaPeriod?: number;
    periodsPerYear?: number;
    weightsMode?: WeightsMode;
}

/** The 21 HOCDBUniverseRow fields of one ticker, decoded by name (all `number`s; NaN = not enough bars). */
export interface UniverseRow {
    last_close: number;
    ret_1: number;
    mom_short: number;
    mom_mid: number;
    mom_long: number;
    vol: number;
    sma_distance: number;
    beta: number;
    corr_market: number;
    rel_strength: number;
    rank_mom_short: number;     // percentile ranks in [0, 1], 1 = highest
    rank_mom_mid: number;
    rank_mom_long: number;
    rank_vol: number;
    rank_rel_strength: number;
    z_mom_mid: number;
    avg_corr: number;
    max_corr: number;
    max_corr_index: number;     // index (in the input order) of the most correlated other ticker
    idio_vol: number;
    volume_ratio: number;       // NaN without volumes
    [field: string]: number;
}

/** The 16 HOCDBUniverseSummary fields decoded by name; the int64 `first_ts` / `last_ts` are `bigint`s (0n without timestamps). */
export interface UniverseSummary {
    n_tickers: number;
    n_bars: number;             // joined bars actually used
    market_ret_1: number;
    market_mom_short: number;
    market_mom_mid: number;
    market_mom_long: number;
    market_vol: number;
    dispersion: number;
    dispersion_mid: number;
    breadth_sma: number;
    breadth_up: number;
    avg_pair_corr: number;
    max_pair_corr: number;
    min_pair_corr: number;
    first_ts: bigint;
    last_ts: bigint;
    [field: string]: number | bigint;
}

export interface UniverseArraysOptions {
    volumes?: (Float64Array | number[])[];      // one series per ticker, same length as the closes
    ts?: BigInt64Array | (number | bigint)[];   // bar timestamps (only feed summary.first_ts / last_ts)
    params?: UniverseParams;
    corr?: boolean;                             // compute the n x n correlation matrix (default true)
}

/** Options of `HOCDB.universe(dbs, options)`. */
export interface UniverseOptions {
    columns?: IndicatorColumns;     // column roles, applied to every database (auto-detected by default; close is required)
    bars?: number;                  // last n bars (bucket > 0) or records of every database; 0 = enough for the longest period
    nBars?: number;                 // alias of bars
    n_bars?: number;
    bucket?: number | bigint;       // 0 = raw records; > 0 = bars of this many timestamp units
    params?: UniverseParams;
    corr?: boolean;                 // default true
}

export interface UniverseResult {
    summary: UniverseSummary;
    rows: UniverseRow[];            // one per ticker, in input order
    corr?: number[][];              // n x n pairwise correlations (row-major), when requested
}

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const LOOKBACK_AUTO = 0xFFFFFFFFFFFFFFFFn; // HOCDB_LOOKBACK_AUTO (SIZE_MAX)

// sizeof() of the C structs in hocdb.h (natural alignment, little-endian)
const INDICATOR_COLUMNS_SIZE = 64; // HOCDBIndicatorColumns: 8 x int64 (open, high, low, close, volume, bid, ask, side)
const INDICATOR_COLUMN_ROLES = ["open", "high", "low", "close", "volume", "bid", "ask", "side"] as const;
const INDICATOR_SPEC_SIZE = 56;    // HOCDBIndicatorSpec: 5 x uint32, 4 pad, 2 x double, 2 x int64
const INDICATOR_RESULT_SIZE = 32;  // HOCDBIndicatorResult: 2 pointers, 2 x size_t
const BARS_EX_SIZE = 72;           // HOCDBBarsEx: 7 pointers, size_t n_bars (offset 56), double* buy_volume (offset 64)
const DECISION_SIZE = 32;          // HOCDBDecision: int64 timestamp, double direction, double size, int64 horizon

const INDICATOR_ERRORS: Record<number, string> = {
    [-1]: "out of memory",
    [-2]: "invalid indicator spec (unknown kind or bad period/param)",
    [-3]: "required OHLCV column missing (the indicator needs a column that was not given and could not be auto-detected)",
    [-4]: "invalid field index",
    [-5]: "per-spec field override is not supported when bucket > 0",
    [-6]: "too many columns",
    [-7]: "series length mismatch",
    [-30]: "CalendarRequired: a session kind (session_vwap, session_range, opening_range, pivots) with param = 0 uses the database's trading calendar, but this handle has no calendar or timestamp unit (open with { calendar, timestamp_unit_ns } or call setCalendar() / setTimestampUnit(), or pass param = session length)",
    [-31]: "UnknownCalendar: no calendar with this id (built-in ids 1-6, custom ids from HOCDB.calendarDefine)",
};

// The backtester and universe entry points share the indicator codes; a few mean something more specific there.
const BACKTEST_ERRORS: Record<number, string> = {
    ...INDICATOR_ERRORS,
    [-2]: "invalid backtest params (position_mode must be 0-2, fill_mode 0-1, costs / fractions finite and non-negative)",
    [-7]: "target length mismatch: target must have exactly one entry per row of indicators() over the same window (bucket > 0: bars whose start lies in [start, end); bucket 0: raw records)",
};
const UNIVERSE_ERRORS: Record<number, string> = {
    ...INDICATOR_ERRORS,
    [-2]: "invalid universe params (periods must be >= 1, weights_mode 0 or 1)",
    [-3]: "no close column: every database needs a close role (columns.close, or a field named 'close' / 'price')",
    [-7]: "series length mismatch: every closes / volumes series must have the same number of bars",
};

function codedError(fn: string, rc: number, table: Record<number, string>): Error {
    return new Error(`${fn} failed with error code ${rc}: ${table[rc] ?? STORAGE_ERRORS[rc] ?? "unknown error"}`);
}

function indicatorError(fn: string, rc: number): Error {
    return new Error(`${fn} failed with error code ${rc}: ${INDICATOR_ERRORS[rc] ?? "unknown error"}`);
}

function cstr(s: string): Uint8Array {
    return encoder.encode(s + "\0");
}

/** Kind name or id -> kind id; throws on unknown kinds. */
function resolveIndicatorKind(kind: string | number): number {
    if (typeof kind === "string") {
        const id = symbols.hocdb_indicator_kind_from_name(ptr(cstr(kind)));
        if (id === 0) throw new Error(`Unknown indicator kind '${kind}' (see HOCDB.indicatorKinds())`);
        return id;
    }
    const id = Math.floor(kind);
    if (symbols.hocdb_indicator_name(id) === null) throw new Error(`Unknown indicator kind id ${kind}`);
    return id;
}

function indicatorKindName(kind: number): string {
    return String(symbols.hocdb_indicator_name(kind) ?? kind);
}

function indicatorOutputNames(kind: number): string[] {
    const n = Number(symbols.hocdb_indicator_output_count(kind));
    const names: string[] = [];
    for (let i = 0; i < n; i++) names.push(String(symbols.hocdb_indicator_output_name(kind, BigInt(i))));
    return names;
}

/** Encode specs into a contiguous HOCDBIndicatorSpec[] buffer (kinds already resolved). */
function encodeIndicatorSpecs(specs: IndicatorSpec[], kinds: number[], resolveField: (f: string | number | undefined) => bigint): Uint8Array {
    const buffer = new Uint8Array(Math.max(specs.length, 1) * INDICATOR_SPEC_SIZE);
    const view = new DataView(buffer.buffer);
    for (let i = 0; i < specs.length; i++) {
        const s = specs[i]!;
        const o = i * INDICATOR_SPEC_SIZE;
        view.setUint32(o + 0, kinds[i]!, true);
        view.setUint32(o + 4, s.period ?? 0, true);
        view.setUint32(o + 8, s.period2 ?? 0, true);
        view.setUint32(o + 12, s.period3 ?? 0, true);
        view.setUint32(o + 16, s.period4 ?? 0, true);
        // 4 bytes of padding at o + 20
        view.setFloat64(o + 24, s.param ?? 0, true);
        view.setFloat64(o + 32, s.param2 ?? 0, true);
        view.setBigInt64(o + 40, resolveField(s.field), true);
        view.setBigInt64(o + 48, resolveField(s.field2), true);
    }
    return buffer;
}

/**
 * Output column names for a list of specs. label = spec.label, else the kind
 * name plus "_period" when a period is given. Single-output kinds use the label
 * as-is; multi-output kinds use `${label}_${outputName}`, except that the
 * output named like the kind itself (macd, ppo, adx, tsi) is just the label
 * (e.g. macd, macd_signal, macd_hist).
 */
function indicatorColumnNames(specs: IndicatorSpec[], kinds: number[]): string[] {
    const names: string[] = [];
    const seen = new Set<string>();
    for (let i = 0; i < specs.length; i++) {
        const s = specs[i]!;
        const kind = kinds[i]!;
        const kindName = indicatorKindName(kind);
        const label = s.label ?? (kindName + (s.period ? `_${s.period}` : ""));
        const outputs = indicatorOutputNames(kind);
        const cols = outputs.length === 1 ? [label] : outputs.map((o) => o === kindName ? label : `${label}_${o}`);
        for (const c of cols) {
            if (seen.has(c)) throw new Error(`Duplicate output column '${c}'; set 'label' on the spec to disambiguate`);
            seen.add(c);
            names.push(c);
        }
    }
    return names;
}

/** Copy `n` native little-endian doubles / int64s starting at `p` into JS-owned arrays. */
function copyF64(p: bigint, n: number): Float64Array {
    if (n === 0 || p === 0n) return new Float64Array(0);
    return new Float64Array(toArrayBuffer(Number(p), 0, n * 8)).slice();
}

function copyI64(p: bigint, n: number): BigInt64Array {
    if (n === 0 || p === 0n) return new BigInt64Array(0);
    return new BigInt64Array(toArrayBuffer(Number(p), 0, n * 8)).slice();
}

// Struct layouts of HOCDBSummary / HOCDBSnapshot discovered at runtime through
// the hocdb_*_field_* introspection functions (types: 1 = int64, 2 = double, 3 = uint64).
interface StructField { name: string; offset: number; type: number }
interface StructLayout { size: number; fields: StructField[] }

function introspectStruct(
    what: string,
    size: () => bigint, count: () => bigint,
    name: (i: bigint) => unknown, offset: (i: bigint) => bigint, type: (i: bigint) => number,
): StructLayout {
    const n = Number(count());
    const total = Number(size());
    const fields: StructField[] = [];
    for (let i = 0; i < n; i++) {
        const idx = BigInt(i);
        const f = { name: String(name(idx)), offset: Number(offset(idx)), type: type(idx) };
        // every field is 8 bytes wide (int64 / double / uint64) and must lie inside the struct
        if (f.offset < 0 || f.offset + 8 > total || (f.type !== 1 && f.type !== 2 && f.type !== 3)) {
            throw new Error(`libhocdb_c ABI mismatch: ${what} field '${f.name}' (offset ${f.offset}, type ${f.type}) does not fit hocdb_${what}_size() = ${total}`);
        }
        fields.push(f);
    }
    if (n === 0 || total === 0) throw new Error(`libhocdb_c ABI mismatch: ${what} introspection reports ${n} fields in ${total} bytes`);
    return { size: total, fields };
}

let summaryLayoutCache: StructLayout | null = null;
let snapshotLayoutCache: StructLayout | null = null;

function summaryLayout(): StructLayout {
    if (!summaryLayoutCache) {
        summaryLayoutCache = introspectStruct("summary",
            symbols.hocdb_summary_size, symbols.hocdb_summary_field_count, symbols.hocdb_summary_field_name,
            symbols.hocdb_summary_field_offset, symbols.hocdb_summary_field_type);
    }
    return summaryLayoutCache;
}

function snapshotLayout(): StructLayout {
    if (!snapshotLayoutCache) {
        snapshotLayoutCache = introspectStruct("snapshot",
            symbols.hocdb_snapshot_size, symbols.hocdb_snapshot_field_count, symbols.hocdb_snapshot_field_name,
            symbols.hocdb_snapshot_field_offset, symbols.hocdb_snapshot_field_type);
    }
    return snapshotLayoutCache;
}

// HOCDBHealth / HOCDBEvaluation layouts are discovered (and validated against
// hocdb_health_size / hocdb_evaluation_size) when the library is loaded, together
// with the fixed HOCDBDecision size this binding encodes by hand.
const HEALTH_LAYOUT: StructLayout = introspectStruct("health",
    symbols.hocdb_health_size, symbols.hocdb_health_field_count, symbols.hocdb_health_field_name,
    symbols.hocdb_health_field_offset, symbols.hocdb_health_field_type);
const EVALUATION_LAYOUT: StructLayout = introspectStruct("evaluation",
    symbols.hocdb_evaluation_size, symbols.hocdb_evaluation_field_count, symbols.hocdb_evaluation_field_name,
    symbols.hocdb_evaluation_field_offset, symbols.hocdb_evaluation_field_type);
{
    const nativeDecisionSize = Number(symbols.hocdb_decision_size());
    if (nativeDecisionSize !== DECISION_SIZE) {
        throw new Error(`libhocdb_c ABI mismatch: hocdb_decision_size() = ${nativeDecisionSize}, this binding expects ${DECISION_SIZE}`);
    }
}

/** Decode a C struct into a plain object: int64 -> bigint, uint64 -> number, double -> number. */
function decodeStruct(buffer: Uint8Array, layout: StructLayout): Record<string, number | bigint> {
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const out: Record<string, number | bigint> = {};
    for (const f of layout.fields) {
        switch (f.type) {
            case 1: out[f.name] = view.getBigInt64(f.offset, true); break;
            case 2: out[f.name] = view.getFloat64(f.offset, true); break;
            case 3: out[f.name] = Number(view.getBigUint64(f.offset, true)); break;
            default: throw new Error(`Unknown struct field type ${f.type} for '${f.name}'`);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Trading calendars, signal backtester and universe features
// ---------------------------------------------------------------------------

// Fixed layouts this binding encodes by hand (natural alignment, little-endian); the
// result structs are discovered through introspection below.
const SESSION_SIZE = 32;           // HOCDBSession: int64 open, close, trade_day; uint64 early_close
const BACKTEST_PARAMS_SIZE = 96;   // HOCDBBacktestParams: 7 doubles, u64 position_mode, u64 fill_mode, double periods_per_year, u64 allow_short, double risk_free_rate
const UNIVERSE_PARAMS_SIZE = 72;   // HOCDBUniverseParams: 7 x uint64 periods, double periods_per_year, uint64 weights_mode
const CALENDAR_NAME_CAP = 1024;
const BACKTEST_OUTPUT_NAMES: readonly BacktestOutput[] = ["equity", "position", "cash", "pnl", "drawdown"]; // HOCDBBacktestOutputs pointer order

const BACKTEST_RESULT_LAYOUT: StructLayout = introspectStruct("backtest_result",
    symbols.hocdb_backtest_result_size, symbols.hocdb_backtest_result_field_count, symbols.hocdb_backtest_result_field_name,
    symbols.hocdb_backtest_result_field_offset, symbols.hocdb_backtest_result_field_type);
const TRADE_LAYOUT: StructLayout = introspectStruct("trade",
    symbols.hocdb_trade_size, symbols.hocdb_trade_field_count, symbols.hocdb_trade_field_name,
    symbols.hocdb_trade_field_offset, symbols.hocdb_trade_field_type);
const UNIVERSE_ROW_LAYOUT: StructLayout = introspectStruct("universe_row",
    symbols.hocdb_universe_row_size, symbols.hocdb_universe_row_field_count, symbols.hocdb_universe_row_field_name,
    symbols.hocdb_universe_row_field_offset, symbols.hocdb_universe_row_field_type);
const UNIVERSE_SUMMARY_LAYOUT: StructLayout = introspectStruct("universe_summary",
    symbols.hocdb_universe_summary_size, symbols.hocdb_universe_summary_field_count, symbols.hocdb_universe_summary_field_name,
    symbols.hocdb_universe_summary_field_offset, symbols.hocdb_universe_summary_field_type);
{
    const bp = Number(symbols.hocdb_backtest_params_size());
    const up = Number(symbols.hocdb_universe_params_size());
    if (bp !== BACKTEST_PARAMS_SIZE || up !== UNIVERSE_PARAMS_SIZE) {
        throw new Error(`libhocdb_c ABI mismatch: hocdb_backtest_params_size() = ${bp} (this binding expects ${BACKTEST_PARAMS_SIZE}), hocdb_universe_params_size() = ${up} (expects ${UNIVERSE_PARAMS_SIZE})`);
    }
}

const DST_RULES: Record<string, number> = { none: 0, us: 1, eu: 2 };
const POSITION_MODES: Record<string, number> = { units: 0, fraction: 1, notional: 2 };
const FILL_MODES: Record<string, number> = { next_open: 0, same_close: 1 };
const WEIGHTS_MODES: Record<string, number> = { equal: 0, volume: 1 };
const SESSION_WHICH: Record<string, number> = { at: 0, prev: 1, previous: 1, next: 2 };

/** Name or small integer -> enum value; `dflt` when undefined / null. */
function resolveEnum(what: string, v: unknown, names: Record<string, number>, max: number, dflt: number): number {
    if (v === undefined || v === null) return dflt;
    if (typeof v === "number" && Number.isInteger(v) && v >= 0 && v <= max) return v;
    if (typeof v === "string") {
        const n = names[v.toLowerCase().replace(/-/g, "_")];
        if (n !== undefined) return n;
    }
    throw new Error(`Invalid ${what} ${JSON.stringify(v)}: expected ${Object.keys(names).map((k) => `"${k}"`).join(", ")} or 0-${max}`);
}

function toI64(v: number | bigint, what: string): bigint {
    if (typeof v === "bigint") return v;
    if (typeof v === "number" && Number.isFinite(v)) return BigInt(Math.trunc(v));
    throw new Error(`${what} must be a number or bigint, got ${JSON.stringify(v)}`);
}

function requireInt(v: unknown, what: string, min = 0): number {
    if (typeof v !== "number" || !Number.isInteger(v) || v < min) throw new Error(`${what} must be an integer >= ${min}, got ${JSON.stringify(v)}`);
    return v;
}

/** Name of a calendar id, or null for an unknown id. */
function calendarNameOf(id: number): string | null {
    const buf = new Uint8Array(CALENDAR_NAME_CAP);
    const rc = symbols.hocdb_calendar_name(id, ptr(buf), BigInt(buf.length));
    if (rc === 0) return null;
    if (rc < 0) throw new Error(`hocdb_calendar_name failed with error code ${rc}: name buffer too small`);
    return decoder.decode(buf.subarray(0, rc));
}

/** Calendar id or name -> id. Names are resolved with hocdb_calendar_id (unknown name -> error); ids are range-checked only. */
function resolveCalendarRef(ref: CalendarRef, what: string): number {
    if (typeof ref === "string") {
        const id = symbols.hocdb_calendar_id(ptr(cstr(ref)));
        if (id === 0) throw new Error(`${what}: UnknownCalendar: no calendar named '${ref}' (built-in: crypto, fx, nyse, nasdaq, lse, cme; custom calendars: HOCDB.calendarDefine)`);
        return id;
    }
    if (typeof ref === "number" && Number.isInteger(ref) && ref >= 0 && ref <= 0xFFFFFFFF) return ref;
    throw new Error(`${what}: calendar must be an id (uint32) or a name, got ${JSON.stringify(ref)}`);
}

/** Like resolveCalendarRef, but also rejects ids no calendar is registered under. */
function requireCalendar(ref: CalendarRef, what: string): number {
    const id = resolveCalendarRef(ref, what);
    if (id === 0 || calendarNameOf(id) === null) throw new Error(`${what}: UnknownCalendar: no calendar with id ${id} (built-in ids 1-6, custom ids from HOCDB.calendarDefine)`);
    return id;
}

function decodeSession(buf: Uint8Array): CalendarSession {
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    return {
        open: Number(v.getBigInt64(0, true)),
        close: Number(v.getBigInt64(8, true)),
        trade_day: Number(v.getBigInt64(16, true)),
        early_close: v.getBigUint64(24, true) !== 0n,
    };
}

/** A double series as a Float64Array the FFI can point at (copied when it is a plain array or an offset view). */
function f64Series(x: unknown, what: string, n?: number): Float64Array {
    let a: Float64Array;
    if (x instanceof Float64Array) a = x.byteOffset === 0 ? x : x.slice();
    else if (Array.isArray(x)) a = Float64Array.from(x, Number);
    else throw new Error(`${what} must be a Float64Array or an array of numbers`);
    if (n !== undefined && a.length !== n) throw new Error(`${what} must have ${n} entries (got ${a.length})`);
    return a;
}

function i64Series(x: unknown, what: string, n?: number): BigInt64Array {
    let a: BigInt64Array;
    if (x instanceof BigInt64Array) a = x.byteOffset === 0 ? x : x.slice();
    else if (Array.isArray(x)) a = BigInt64Array.from(x, (v) => toI64(v as number | bigint, what));
    else throw new Error(`${what} must be a BigInt64Array or an array of numbers / bigints`);
    if (n !== undefined && a.length !== n) throw new Error(`${what} must have ${n} entries (got ${a.length})`);
    return a;
}

/** HOCDBBacktestParams: the library defaults overlaid with the given entries (snake_case or camelCase). */
function encodeBacktestParams(params: BacktestParams | undefined, what: string): Uint8Array {
    const p = (params ?? {}) as Record<string, unknown>;
    if (typeof p !== "object" || Array.isArray(p)) throw new Error(`${what}: 'params' must be an object`);
    const buf = new Uint8Array(BACKTEST_PARAMS_SIZE);
    symbols.hocdb_backtest_params_default(ptr(buf));
    const v = new DataView(buf.buffer);
    const setF64 = (off: number, name: string, alias: string) => {
        const x = opt<unknown>(p, name, alias);
        if (x === undefined) return;
        if (typeof x !== "number" || !Number.isFinite(x)) throw new Error(`${what}: param ${name} must be a finite number, got ${JSON.stringify(x)}`);
        v.setFloat64(off, x, true);
    };
    setF64(0, "initial_equity", "initialEquity");
    setF64(8, "cost_bps", "costBps");
    setF64(16, "slippage_bps", "slippageBps");
    setF64(24, "stop_loss", "stopLoss");
    setF64(32, "take_profit", "takeProfit");
    setF64(40, "trailing_stop", "trailingStop");
    setF64(48, "max_position", "maxPosition");
    const pm = opt<unknown>(p, "position_mode", "positionMode");
    if (pm !== undefined) v.setBigUint64(56, BigInt(resolveEnum("position_mode", pm, POSITION_MODES, 2, 0)), true);
    const fm = opt<unknown>(p, "fill_mode", "fillMode");
    if (fm !== undefined) v.setBigUint64(64, BigInt(resolveEnum("fill_mode", fm, FILL_MODES, 1, 0)), true);
    setF64(72, "periods_per_year", "periodsPerYear");
    const as = opt<unknown>(p, "allow_short", "allowShort");
    if (as !== undefined) v.setBigUint64(80, as ? 1n : 0n, true);
    setF64(88, "risk_free_rate", "riskFreeRate");
    return buf;
}

function decodeBacktestParams(buf: Uint8Array): Required<Pick<BacktestParams, "initial_equity" | "cost_bps" | "slippage_bps" | "stop_loss" | "take_profit" | "trailing_stop" | "max_position" | "periods_per_year" | "allow_short" | "risk_free_rate">> & { position_mode: number; fill_mode: number } {
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    return {
        initial_equity: v.getFloat64(0, true), cost_bps: v.getFloat64(8, true), slippage_bps: v.getFloat64(16, true),
        stop_loss: v.getFloat64(24, true), take_profit: v.getFloat64(32, true), trailing_stop: v.getFloat64(40, true),
        max_position: v.getFloat64(48, true), position_mode: Number(v.getBigUint64(56, true)), fill_mode: Number(v.getBigUint64(64, true)),
        periods_per_year: v.getFloat64(72, true), allow_short: v.getBigUint64(80, true) !== 0n, risk_free_rate: v.getFloat64(88, true),
    };
}

/** HOCDBUniverseParams: the library defaults overlaid with the given entries. */
function encodeUniverseParams(params: UniverseParams | undefined, what: string): Uint8Array {
    const p = (params ?? {}) as Record<string, unknown>;
    if (typeof p !== "object" || Array.isArray(p)) throw new Error(`${what}: 'params' must be an object`);
    const buf = new Uint8Array(UNIVERSE_PARAMS_SIZE);
    symbols.hocdb_universe_params_default(ptr(buf));
    const v = new DataView(buf.buffer);
    const periods: [number, string, string][] = [
        [0, "mom_short", "momShort"], [8, "mom_mid", "momMid"], [16, "mom_long", "momLong"], [24, "vol_period", "volPeriod"],
        [32, "corr_period", "corrPeriod"], [40, "sma_period", "smaPeriod"], [48, "beta_period", "betaPeriod"],
    ];
    for (const [off, name, alias] of periods) {
        const x = opt<unknown>(p, name, alias);
        if (x !== undefined) v.setBigUint64(off, BigInt(requireInt(x, `${what}: param ${name}`)), true);
    }
    const ppy = opt<unknown>(p, "periods_per_year", "periodsPerYear");
    if (ppy !== undefined) {
        if (typeof ppy !== "number" || !Number.isFinite(ppy)) throw new Error(`${what}: param periods_per_year must be a finite number`);
        v.setFloat64(56, ppy, true);
    }
    const wm = opt<unknown>(p, "weights_mode", "weightsMode");
    if (wm !== undefined) v.setBigUint64(64, BigInt(resolveEnum("weights_mode", wm, WEIGHTS_MODES, 1, 0)), true);
    return buf;
}

function decodeUniverseParams(buf: Uint8Array): Required<Pick<UniverseParams, "mom_short" | "mom_mid" | "mom_long" | "vol_period" | "corr_period" | "sma_period" | "beta_period" | "periods_per_year">> & { weights_mode: number } {
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const u = (off: number) => Number(v.getBigUint64(off, true));
    return {
        mom_short: u(0), mom_mid: u(8), mom_long: u(16), vol_period: u(24), corr_period: u(32), sma_period: u(40), beta_period: u(48),
        periods_per_year: v.getFloat64(56, true), weights_mode: u(64),
    };
}

// Buffers of one backtest call: built before the native call, decoded after it.
interface BacktestBuffers {
    n: number;
    target: Float64Array;
    params: Uint8Array;
    outputsBuf: BigUint64Array | null;                         // HOCDBBacktestOutputs (5 pointers) or NULL
    outputArrays: Partial<Record<BacktestOutput, Float64Array>>;
    trades: Uint8Array | null;                                 // HOCDBTrade[tradesCap] or NULL
    tradesCap: number;
    wantTrades: boolean;
    result: Uint8Array;                                        // HOCDBBacktestResult
}

function prepareBacktest(target: unknown, options: BacktestArraysOptions, what: string, n?: number): BacktestBuffers {
    const tgt = f64Series(target, `${what}: target`, n);
    if (tgt.length === 0) throw new Error(`${what}: target must not be empty (one entry per bar)`);
    const o = (options ?? {}) as Record<string, unknown>;
    const params = encodeBacktestParams(options?.params, what);
    const outputs = options?.outputs ?? [];
    if (!Array.isArray(outputs)) throw new Error(`${what}: 'outputs' must be an array of ${BACKTEST_OUTPUT_NAMES.join(" / ")}`);
    const outputArrays: Partial<Record<BacktestOutput, Float64Array>> = {};
    let outputsBuf: BigUint64Array | null = null;
    if (outputs.length > 0) {
        outputsBuf = new BigUint64Array(BACKTEST_OUTPUT_NAMES.length);
        for (const name of outputs) {
            const k = BACKTEST_OUTPUT_NAMES.indexOf(name);
            if (k < 0) throw new Error(`${what}: unknown output '${name}' (expected ${BACKTEST_OUTPUT_NAMES.join(", ")})`);
            if (outputArrays[name] === undefined) {
                const arr = new Float64Array(tgt.length);
                outputArrays[name] = arr;
                outputsBuf[k] = BigInt(ptr(arr));
            }
        }
    }
    const maxTrades = opt<unknown>(o, "maxTrades", "max_trades");
    let tradesCap = 0;
    let trades: Uint8Array | null = null;
    if (maxTrades !== undefined) {
        tradesCap = requireInt(maxTrades, `${what}: maxTrades`);
        if (tradesCap > 0) trades = new Uint8Array(tradesCap * TRADE_LAYOUT.size);
    }
    return {
        n: tgt.length, target: tgt, params, outputsBuf, outputArrays, trades, tradesCap, wantTrades: maxTrades !== undefined,
        result: new Uint8Array(BACKTEST_RESULT_LAYOUT.size),
    };
}

function finishBacktest(b: BacktestBuffers): BacktestRun {
    const result = decodeStruct(b.result, BACKTEST_RESULT_LAYOUT) as BacktestResult;
    const run: BacktestRun = { result };
    if (b.wantTrades) {
        const count = Math.min(result.n_trades, b.tradesCap);
        const trades: Trade[] = [];
        for (let i = 0; i < count; i++) {
            trades.push(decodeStruct(b.trades!.subarray(i * TRADE_LAYOUT.size, (i + 1) * TRADE_LAYOUT.size), TRADE_LAYOUT) as Trade);
        }
        run.trades = trades;
    }
    for (const name of BACKTEST_OUTPUT_NAMES) {
        const arr = b.outputArrays[name];
        if (arr !== undefined) run[name] = arr;
    }
    return run;
}

/** The OHLC inputs of the array entry points: ts and close are required, open / high / low may be null. */
interface BarArrays {
    n: number;
    ts: BigInt64Array;
    open: Float64Array | null;
    high: Float64Array | null;
    low: Float64Array | null;
    close: Float64Array;
}

function barArrays(ts: unknown, open: unknown, high: unknown, low: unknown, close: unknown, what: string): BarArrays {
    const t = i64Series(ts, `${what}: ts`);
    const n = t.length;
    if (n === 0) throw new Error(`${what}: ts must not be empty`);
    const optional = (x: unknown, name: string) => x === null || x === undefined ? null : f64Series(x, `${what}: ${name}`, n);
    return { n, ts: t, open: optional(open, "open"), high: optional(high, "high"), low: optional(low, "low"), close: f64Series(close, `${what}: close`, n) };
}

function encodeSplits(splits: Split[], what: string): BigUint64Array {
    if (!Array.isArray(splits) || splits.length === 0) throw new Error(`${what}: 'splits' must be a non-empty array of { train_start, train_end, test_start, test_end }`);
    const buf = new BigUint64Array(4 * splits.length);
    for (let i = 0; i < splits.length; i++) {
        const s = splits[i] as unknown as Record<string, unknown>;
        if (s === null || typeof s !== "object") throw new Error(`${what}: splits[${i}] must be an object`);
        const names: [string, string][] = [["train_start", "trainStart"], ["train_end", "trainEnd"], ["test_start", "testStart"], ["test_end", "testEnd"]];
        for (let k = 0; k < 4; k++) {
            const [name, alias] = names[k]!;
            buf[4 * i + k] = BigInt(requireInt(opt<unknown>(s, name, alias), `${what}: splits[${i}].${name}`));
        }
    }
    return buf;
}

function decodeUniverse(n: number, rows: Uint8Array, corr: Float64Array | null, summary: Uint8Array): UniverseResult {
    const out: UniverseResult = {
        summary: decodeStruct(summary, UNIVERSE_SUMMARY_LAYOUT) as UniverseSummary,
        rows: [],
    };
    for (let i = 0; i < n; i++) {
        out.rows.push(decodeStruct(rows.subarray(i * UNIVERSE_ROW_LAYOUT.size, (i + 1) * UNIVERSE_ROW_LAYOUT.size), UNIVERSE_ROW_LAYOUT) as UniverseRow);
    }
    if (corr !== null) {
        out.corr = [];
        for (let i = 0; i < n; i++) out.corr.push(Array.from(corr.subarray(i * n, (i + 1) * n)));
    }
    return out;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Durability, lock-free readers, maintenance and metrics
// ---------------------------------------------------------------------------

// HOCDBMetrics layout, discovered and validated against hocdb_metrics_size() when the library is loaded.
const METRICS_LAYOUT: StructLayout = introspectStruct("metrics",
    symbols.hocdb_metrics_size, symbols.hocdb_metrics_field_count, symbols.hocdb_metrics_field_name,
    symbols.hocdb_metrics_field_offset, symbols.hocdb_metrics_field_type);

const CONFIG_SIZE = 80; // sizeof(HOCDBConfig): natural alignment, little-endian (calendar id at 72)
const FSYNC_POLICIES: Record<string, number> = { none: 0, on_close: 1, on_flush: 2, interval: 3 };

const READ_ONLY_MESSAGE = "read-only: this handle is a reader (opened with HOCDB.openReader / readOnly: true), writes and maintenance need a writer";

// Return codes of the storage entry points (errCode in src/c_bindings.zig).
const STORAGE_ERRORS: Record<number, string> = {
    [-1]: "I/O error or out of memory",
    [-10]: READ_ONLY_MESSAGE,
    [-11]: "DatabaseLocked: another writer holds this database",
    [-12]: "ChecksumMismatch: the stored CRC32C does not match the data",
    [-20]: "checksum unavailable (ring buffer, legacy HOC1 file, or an adopted crash tail that is not committed yet)",
    [-21]: "EmptyDatabase: nothing has been committed yet",
};

function storageError(fn: string, rc: number): Error {
    return new Error(`${fn} failed with error code ${rc}: ${STORAGE_ERRORS[rc] ?? "unknown error"}`);
}

/** Read a config option by its snake_case name or any camelCase alias. */
function opt<T>(config: Record<string, unknown>, ...names: string[]): T | undefined {
    for (const n of names) {
        if (config[n] !== undefined && config[n] !== null) return config[n] as T;
    }
    return undefined;
}

function resolveFsyncPolicy(v: unknown): number {
    if (v === undefined) return FSYNC_POLICIES.on_close!;
    if (typeof v === "number" && Number.isInteger(v) && v >= 0 && v <= 3) return v;
    if (typeof v === "string") {
        const p = FSYNC_POLICIES[v.toLowerCase().replace(/-/g, "_")];
        if (p !== undefined) return p;
    }
    throw new Error(`Invalid fsync policy ${JSON.stringify(v)}: expected "none", "on_close", "on_flush", "interval" or 0-3`);
}

function configInt(what: string, v: unknown, min: bigint, max: bigint): bigint {
    if (v === undefined) return 0n;
    let n: bigint;
    if (typeof v === "bigint") n = v;
    else if (typeof v === "number" && Number.isFinite(v)) n = BigInt(Math.floor(v));
    else throw new Error(`Invalid value for ${what}: ${JSON.stringify(v)} (expected a number or bigint)`);
    if (n < min || n > max) throw new Error(`Invalid value for ${what}: ${n} is out of range [${min}, ${max}]`);
    return n;
}

const U32_MAX = 0xFFFFFFFFn;
const U64_MAX = 0xFFFFFFFFFFFFFFFFn;

/**
 * Pack a DBConfig into a HOCDBConfig struct (80 bytes):
 *   0 int64 max_file_size     8 int overwrite_on_full   12 int flush_on_write    16 int auto_increment
 *  20 int fsync_policy       24 uint32 fsync_interval_ms  28 int verify_on_open  32 int64 retention_span
 *  40 uint64 rollover_size   48 int auto_migrate (+4 pad)  56 uint64 timestamp_unit_ns  64 uint64 index_stride
 */
function encodeConfig(config: DBConfig): Uint8Array {
    const c = config as Record<string, unknown>;
    const buffer = new Uint8Array(CONFIG_SIZE);
    const view = new DataView(buffer.buffer);
    view.setBigInt64(0, configInt("max_file_size", opt(c, "max_file_size", "maxFileSize"), 0n, INT64_MAX), true);
    view.setInt32(8, opt<boolean>(c, "overwrite_on_full", "overwriteOnFull") === false ? 0 : 1, true); // ring buffer unless disabled (as before)
    view.setInt32(12, opt<boolean>(c, "flush_on_write", "flushOnWrite") === true ? 1 : 0, true);
    view.setInt32(16, opt<boolean>(c, "auto_increment", "autoIncrement") === true ? 1 : 0, true);
    view.setInt32(20, resolveFsyncPolicy(opt(c, "fsync", "fsync_policy", "fsyncPolicy")), true);
    view.setUint32(24, Number(configInt("fsync_interval_ms", opt(c, "fsync_interval_ms", "fsyncIntervalMs"), 0n, U32_MAX)), true);
    view.setInt32(28, opt<boolean>(c, "verify_on_open", "verifyOnOpen") === true ? 1 : 0, true);
    view.setBigInt64(32, configInt("retention_span", opt(c, "retention_span", "retentionSpan"), 0n, INT64_MAX), true);
    view.setBigUint64(40, configInt("rollover_size", opt(c, "rollover_size", "rolloverSize"), 0n, U64_MAX), true);
    view.setInt32(48, opt<boolean>(c, "auto_migrate", "autoMigrate") === false ? 0 : 1, true); // migrate legacy files unless disabled
    // 4 bytes of padding at 52
    view.setBigUint64(56, configInt("timestamp_unit_ns", opt(c, "timestamp_unit_ns", "timestampUnitNs"), 0n, U64_MAX), true);
    view.setBigUint64(64, configInt("index_stride", opt(c, "index_stride", "indexStride"), 0n, U64_MAX), true);
    // trading calendar: an id or a name (resolved here, so an unknown name fails before the open); 0 = none
    const calendar = opt<number | string>(c, "calendar");
    view.setUint32(72, calendar === undefined ? 0 : resolveCalendarRef(calendar, "config.calendar"), true);
    return buffer;
}

function processSchema(schema: FieldDef[]): SchemaInfo & { nameBuffers: Uint8Array[], schemaBuffer: Uint8Array } {
    let recordSize = 0;
    const fieldOffsets: Record<string, { offset: number, type: string, index: number }> = {};
    const nameBuffers: Uint8Array[] = [];
    const schemaBuffer = new Uint8Array(schema.length * 16);
    const schemaView = new DataView(schemaBuffer.buffer);

    for (let i = 0; i < schema.length; i++) {
        const field = schema[i];
        if (!field) continue;
        fieldOffsets[field.name] = { offset: recordSize, type: field.type, index: i };

        let typeCode;
        let size;
        switch (field.type) {
            case "i64": typeCode = 1; size = 8; break;
            case "f64": typeCode = 2; size = 8; break;
            case "u64": typeCode = 3; size = 8; break;
            case "bool": typeCode = 6; size = 1; break;
            default: throw new Error(`Unsupported field type: ${field.type}`);
        }
        recordSize += size;

        const nameBytes = encoder.encode(field.name + "\0");
        nameBuffers.push(nameBytes);

        schemaView.setBigUint64(i * 16, BigInt(ptr(nameBytes)), true);
        schemaView.setInt32(i * 16 + 8, typeCode, true);
    }
    return { recordSize, fieldOffsets, nameBuffers, schemaBuffer };
}

function parseBuffer(buffer: ArrayBuffer, recordSize: number, fieldOffsets: Record<string, { offset: number, type: string, index: number }>): Record<string, number | bigint>[] {
    const view = new DataView(buffer);
    const count = buffer.byteLength / recordSize;
    const result = new Array(count);

    for (let i = 0; i < count; i++) {
        const record: Record<string, number | bigint> = {};
        const base = i * recordSize;
        for (const [name, info] of Object.entries(fieldOffsets)) {
            switch (info.type) {
                case 'i64': record[name] = view.getBigInt64(base + info.offset, true); break;
                case 'f64': record[name] = view.getFloat64(base + info.offset, true); break;
                case 'u64': record[name] = view.getBigUint64(base + info.offset, true); break;
                case 'bool': record[name] = view.getUint8(base + info.offset); break;
            }
        }
        result[i] = record;
    }
    return result;
}

export class HOCDB {
    db: any;
    schema: FieldDef[];
    recordSize: number;
    fieldOffsets: Record<string, { offset: number, type: string, index: number }>;
    nameBuffers: Uint8Array[];
    ticker: string;
    path: string;
    /** true for lock-free readers (HOCDB.openReader / readOnly: true); see isReadOnly(). */
    readonly readOnly: boolean;

    /**
     * Open (or create) a database as its single writer, or attach as a
     * lock-free reader with `readOnly: true` (see `HOCDB.openReader`). A
     * failed open throws with the engine's error name in the message
     * (DatabaseLocked, SchemaMismatch, ChecksumMismatch, EmptyDatabase, ...).
     */
    constructor(ticker: string, path: string, schema: FieldDef[], config: DBConfig = {}) {
        this.ticker = ticker;
        this.path = path;
        const tickerBytes = encoder.encode(ticker + "\0");
        const pathBytes = encoder.encode(path + "\0");

        // Process schema
        this.schema = schema;
        const { recordSize, fieldOffsets, nameBuffers, schemaBuffer } = processSchema(schema);
        this.recordSize = recordSize;
        this.fieldOffsets = fieldOffsets;
        this.nameBuffers = nameBuffers;

        const readOnly = opt<boolean>((config ?? {}) as Record<string, unknown>, "read_only", "readOnly") === true;
        if (readOnly) {
            // no lock; every read re-reads the writer's committed cursor
            this.db = symbols.hocdb_open_reader(ptr(tickerBytes), ptr(pathBytes), ptr(schemaBuffer), BigInt(schema.length));
        } else {
            const configBuffer = encodeConfig(config ?? {}); // HOCDBConfig
            this.db = symbols.hocdb_init_ex(ptr(tickerBytes), ptr(pathBytes), ptr(schemaBuffer), BigInt(schema.length), ptr(configBuffer));
        }

        if (!this.db) {
            const err = symbols.hocdb_last_error();
            const reason = err ? String(err) : "";
            throw new Error(`Failed to open HOCDB '${ticker}' in '${path}' as ${readOnly ? "reader" : "writer"}: ${reason || "unknown error"}`);
        }
        this.readOnly = symbols.hocdb_is_read_only(this.db) === 1;
    }

    private assertOpen(): void {
        if (!this.db) throw new Error("Database not initialized (closed or dropped?)");
    }

    append(data: Record<string, number | bigint>) {
        const buffer = new Uint8Array(this.recordSize);
        const view = new DataView(buffer.buffer);

        for (const [key, value] of Object.entries(data)) {
            const info = this.fieldOffsets[key];
            if (!info) continue;

            switch (info.type) {
                case 'i64': view.setBigInt64(info.offset, BigInt(value), true); break;
                case 'f64': view.setFloat64(info.offset, Number(value), true); break;
                case 'u64': view.setBigUint64(info.offset, BigInt(value), true); break;
                case 'bool': view.setUint8(info.offset, value ? 1 : 0); break;
            }
        }

        const res = symbols.hocdb_append(this.db, ptr(buffer), BigInt(this.recordSize));
        if (res !== 0) {
            let msg = `Append failed with error code: ${res}`;
            if (res === -2) msg += " (Invalid Record Size)";
            if (res === -3) msg += " (Timestamp Not Monotonic - timestamps must be strictly increasing)";
            if (res === -10) msg += ` (${READ_ONLY_MESSAGE})`;
            throw new Error(msg);
        }
    }

    /** Commit buffered records (writers): after this they are visible to readers. Readers: same as refresh(). */
    flush() {
        this.assertOpen();
        const res = symbols.hocdb_flush(this.db);
        if (res !== 0) {
            throw new Error(`Failed to flush DB (error code ${res})`);
        }
    }

    load(): Record<string, number | bigint>[] {
        if (!this.db) throw new Error("DB not initialized");
        const lenPtr = new BigUint64Array(1);
        const dataPtr = symbols.hocdb_load(this.db, ptr(lenPtr));

        if (dataPtr === 0) return [];


        const len = Number(lenPtr[0]);
        // toArrayBuffer provides a copy from native memory
        const data = new Uint8Array(toArrayBuffer(dataPtr, 0, len));

        // Parse the buffer into objects
        const records = parseBuffer(data.buffer, this.recordSize, this.fieldOffsets);

        symbols.hocdb_free(dataPtr);

        return records;
    }

    queryRaw(startTs: number | bigint, endTs: number | bigint, filters: Filter[] | Record<string, number | bigint | string> = []): ArrayBuffer {
        if (!this.db) throw new Error("Database not initialized");
        // ... filter logic same as query ...
        let filterArray: Filter[] = [];

        if (Array.isArray(filters)) {
            filterArray = filters;
        } else {
            for (const [key, value] of Object.entries(filters)) {
                const info = this.fieldOffsets[key];
                if (!info) throw new Error(`Unknown field in filter: ${key}`);
                filterArray.push({
                    field_index: info.index,
                    value: value
                });
            }
        }

        const lenPtr = new BigUint64Array(1);
        let filtersPtr = null;
        let filtersBuf = null;

        if (filterArray.length > 0) {
            const structSize = 176;
            filtersBuf = new Uint8Array(filterArray.length * structSize);
            const view = new DataView(filtersBuf.buffer);
            for (let i = 0; i < filterArray.length; i++) {
                const offset = i * structSize;
                const f = filterArray[i];
                if (!f) continue;
                view.setBigUint64(offset, BigInt(f.field_index), true);
                if (typeof f.value === 'bigint') {
                    view.setInt32(offset + 8, 1, true);
                    view.setBigInt64(offset + 16, f.value, true);
                } else if (typeof f.value === 'number') {
                    view.setInt32(offset + 8, 2, true);
                    view.setFloat64(offset + 24, f.value, true);
                } else if (typeof f.value === 'string') {
                    view.setInt32(offset + 8, 5, true);
                    const strBytes = encoder.encode(f.value);
                    for (let j = 0; j < Math.min(strBytes.length, 128); j++) {
                        filtersBuf[offset + 40 + j] = strBytes[j]!;
                    }
                } else if (typeof f.value === 'boolean') {
                    view.setInt32(offset + 8, 6, true);
                    view.setUint8(offset + 168, f.value ? 1 : 0);
                }
            }
            filtersPtr = ptr(filtersBuf);
        }

        const dataPtr = symbols.hocdb_query(
            this.db,
            BigInt(startTs),
            BigInt(endTs),
            filtersPtr ?? 0,
            BigInt(filterArray.length),
            ptr(lenPtr)
        );

        if (!dataPtr && lenPtr[0]! > 0n) {
            throw new Error("Query failed");
        }

        if (lenPtr[0] === 0n) return new ArrayBuffer(0);

        const totalBytes = Number(lenPtr[0]!);
        // View into native memory
        // We verified dataPtr is not null above (if len > 0)
        const viewBuffer = toArrayBuffer(dataPtr!, 0, totalBytes);

        // CRITICAL: We MUST copy the data because we are about to free the native pointer.
        // .slice() on an ArrayBuffer creates a copy.
        const copy = viewBuffer.slice(0); // Make a copy

        symbols.hocdb_free(dataPtr);
        return copy;
    }

    queryInto(startTs: number | bigint, endTs: number | bigint, filters: Filter[] | Record<string, number | bigint | string> = [], buffer: Uint8Array): number {
        if (!this.db) throw new Error("Database not initialized");

        let filterArray: Filter[] = [];
        if (Array.isArray(filters)) {
            filterArray = filters;
        } else {
            for (const [key, value] of Object.entries(filters)) {
                const info = this.fieldOffsets[key];
                if (!info) throw new Error(`Unknown field in filter: ${key}`);
                filterArray.push({
                    field_index: info.index,
                    value: value
                });
            }
        }

        let filtersPtr = null;
        let filtersBuf = null;

        if (filterArray.length > 0) {
            const structSize = 176;
            filtersBuf = new Uint8Array(filterArray.length * structSize);
            const view = new DataView(filtersBuf.buffer);
            for (let i = 0; i < filterArray.length; i++) {
                const offset = i * structSize;
                const f = filterArray[i];
                if (!f) continue;
                view.setBigUint64(offset, BigInt(f.field_index), true);
                if (typeof f.value === 'bigint') {
                    view.setInt32(offset + 8, 1, true);
                    view.setBigInt64(offset + 16, f.value, true);
                } else if (typeof f.value === 'number') {
                    view.setInt32(offset + 8, 2, true);
                    view.setFloat64(offset + 24, f.value, true);
                } else if (typeof f.value === 'string') {
                    view.setInt32(offset + 8, 5, true);
                    const strBytes = encoder.encode(f.value);
                    for (let j = 0; j < Math.min(strBytes.length, 128); j++) {
                        filtersBuf[offset + 40 + j] = strBytes[j]!;
                    }
                } else if (typeof f.value === 'boolean') {
                    view.setInt32(offset + 8, 6, true);
                    view.setUint8(offset + 168, f.value ? 1 : 0);
                }
            }
            filtersPtr = ptr(filtersBuf);
        }

        const bytesWritten = symbols.hocdb_query_into(
            this.db,
            BigInt(startTs),
            BigInt(endTs),
            filtersPtr ?? 0,
            BigInt(filterArray.length),
            ptr(buffer),
            BigInt(buffer.byteLength)
        );

        if (bytesWritten === -2n) {
            throw new Error("BufferTooSmall");
        }
        if (bytesWritten === -1n) {
            throw new Error("QueryInto failed");
        }

        return Number(bytesWritten);
    }

    query(startTs: number | bigint, endTs: number | bigint, filters: Filter[] | Record<string, number | bigint | string> = []): Record<string, number | bigint>[] {
        const buffer = this.queryRaw(startTs, endTs, filters);
        return parseBuffer(buffer, this.recordSize, this.fieldOffsets);
    }

    getStats(start: bigint, end: bigint, fieldIndex: number | string, options?: { percentiles?: boolean }): { min: number, max: number, sum: number, count: bigint, mean: number, p50: number, p90: number, p95: number, p99: number } {
        let idx: bigint;
        if (typeof fieldIndex === 'string') {
            const field = this.fieldOffsets[fieldIndex];
            if (!field) {
                throw new Error(`Field '${fieldIndex}' not found in schema`);
            }
            idx = BigInt(field.index);
        } else {
            idx = BigInt(Math.floor(fieldIndex));
        }

        const statsBuffer = new Uint8Array(72); // Size of HOCDBStats
        const flags = options?.percentiles ? 1 : 0;
        const res = symbols.hocdb_get_stats(this.db, start, end, idx, flags, ptr(statsBuffer));

        if (res !== 0) {
            throw new Error("getStats failed");
        }

        const view = new DataView(statsBuffer.buffer);
        return {
            min: view.getFloat64(0, true),
            max: view.getFloat64(8, true),
            sum: view.getFloat64(16, true),
            count: view.getBigUint64(24, true),
            mean: view.getFloat64(32, true),
            p50: view.getFloat64(40, true),
            p90: view.getFloat64(48, true),
            p95: view.getFloat64(56, true),
            p99: view.getFloat64(64, true)
        };
    }

    getLatest(fieldIndex: number | string): { value: number, timestamp: bigint } {
        const valPtr = new Float64Array(1);
        const tsPtr = new BigInt64Array(1);

        let idx: bigint;
        if (typeof fieldIndex === 'string') {
            const field = this.fieldOffsets[fieldIndex];
            if (!field) {
                throw new Error(`Field '${fieldIndex}' not found in schema`);
            }
            idx = BigInt(field.index);
        } else {
            idx = BigInt(Math.floor(fieldIndex));
        }

        const res = symbols.hocdb_get_latest(this.db, idx, ptr(valPtr), ptr(tsPtr));

        if (res !== 0) {
            throw new Error("getLatest failed");
        }

        return {
            value: valPtr[0]!,
            timestamp: tsPtr[0]!
        };
    }


    // -----------------------------------------------------------------------
    // Technical indicators and quantitative analytics
    // -----------------------------------------------------------------------

    /** Field name or index -> index; throws on unknown names / out-of-range indices. */
    private fieldIndex(ref: string | number, what: string): number {
        if (typeof ref === "string") {
            const info = this.fieldOffsets[ref];
            if (!info) {
                throw new Error(`Unknown field '${ref}' for ${what} (schema fields: ${Object.keys(this.fieldOffsets).join(", ")})`);
            }
            return info.index;
        }
        const idx = Math.floor(ref);
        if (idx < 0 || idx >= this.schema.length) {
            throw new Error(`Field index ${ref} out of range for ${what} (schema has ${this.schema.length} fields)`);
        }
        return idx;
    }

    /**
     * Index of the field literally named `role`, or -1. `close` falls back to
     * `price`; `volume` falls back to `size`, then `qty`.
     */
    private autoColumn(role: string): number {
        const info = this.fieldOffsets[role];
        if (info) return info.index;
        const fallbacks = role === "close" ? ["price"] : role === "volume" ? ["size", "qty"] : [];
        for (const alt of fallbacks) {
            const f = this.fieldOffsets[alt];
            if (f) return f.index;
        }
        return -1;
    }

    /** Resolve an optional field reference: undefined -> `auto`, null / -1 -> -1, else the field index. */
    private optionalField(ref: FieldRef | undefined, what: string, auto: number): number {
        if (ref === undefined) return auto;
        if (ref === null || ref === -1) return -1;
        return this.fieldIndex(ref, what);
    }

    /** Build the HOCDBIndicatorColumns struct (8 x int64: open, high, low, close, volume, bid, ask, side; -1 = absent). */
    private encodeColumns(columns: IndicatorColumns = {}): Uint8Array {
        const buffer = new Uint8Array(INDICATOR_COLUMNS_SIZE);
        const view = new DataView(buffer.buffer);
        for (let i = 0; i < INDICATOR_COLUMN_ROLES.length; i++) {
            const role = INDICATOR_COLUMN_ROLES[i]!;
            const idx = this.optionalField(columns[role], `${role} column`, this.autoColumn(role));
            view.setBigInt64(i * 8, BigInt(idx), true);
        }
        if (view.getBigInt64(3 * 8, true) < 0n) {
            throw new Error("No close column found: pass columns.close (field name or index); no schema field is named 'close' or 'price'");
        }
        return buffer;
    }

    /**
     * Compute a batch of indicators in one pass over [start, end) (or the last
     * `tail` rows). Returns one Float64Array per output column plus the row
     * timestamps; NaN marks values still in warm-up.
     */
    indicators(specs: IndicatorSpec[], options: IndicatorOptions = {}): IndicatorResult {
        return this.runIndicators(specs, options);
    }

    /** Same as `indicators` for the last `n` rows (or bars when `bucket` > 0). */
    indicatorsTail(n: number, specs: IndicatorSpec[], options: Omit<IndicatorOptions, "tail" | "start" | "end"> = {}): IndicatorResult {
        return this.indicators(specs, { ...options, tail: n });
    }

    /**
     * Indicators over this database aligned with another open database, whose
     * close column becomes the second series (`series2`, `ratio`, `ratio_zscore`,
     * `rel_strength`, `correl`, `beta`); single-series kinds run on this
     * database. With `bucket > 0` both sides are resampled and inner-joined on
     * bar timestamps; on ticks the other database is as-of joined onto this
     * database's rows. Options are those of `indicators` plus `columns2`
     * (alias `otherColumns`) for the other database's roles.
     */
    pairIndicators(other: HOCDB, specs: IndicatorSpec[], options: PairIndicatorOptions = {}): IndicatorResult {
        return this.runIndicators(specs, options, other);
    }

    /** Same as `pairIndicators` for the last `n` rows (or bars when `bucket` > 0). */
    pairIndicatorsTail(other: HOCDB, n: number, specs: IndicatorSpec[], options: Omit<PairIndicatorOptions, "tail" | "start" | "end"> = {}): IndicatorResult {
        return this.pairIndicators(other, specs, { ...options, tail: n });
    }

    /** Shared implementation of `indicators` (other = undefined) and `pairIndicators`. */
    private runIndicators(specs: IndicatorSpec[], options: PairIndicatorOptions, other?: HOCDB): IndicatorResult {
        const what = other === undefined ? "indicators" : "pairIndicators";
        if (!this.db) throw new Error("Database not initialized");
        if (!Array.isArray(specs) || specs.length === 0) throw new Error(`${what}: at least one spec is required`);
        if (other !== undefined) {
            if (!(other instanceof HOCDB)) throw new Error("pairIndicators: 'other' must be an open HOCDB instance");
            if (!other.db) throw new Error("pairIndicators: the other database is not initialized (closed or dropped?)");
        }

        const kinds = specs.map((s) => resolveIndicatorKind(s.kind));
        const names = indicatorColumnNames(specs, kinds); // validates labels before touching native memory
        const colsBuf = this.encodeColumns(options.columns);
        const colsBuf2 = other === undefined ? null : other.encodeColumns(options.columns2 ?? options.otherColumns);
        const specsBuf = encodeIndicatorSpecs(specs, kinds, (f) => f === undefined ? -1n : BigInt(this.fieldIndex(f, "spec field")));
        const lookback = options.lookback === undefined || options.lookback === "auto"
            ? LOOKBACK_AUTO
            : BigInt(Math.max(0, Math.floor(options.lookback)));
        const bucket = BigInt(options.bucket ?? 0);
        const nSpecs = BigInt(specs.length);
        const out = new Uint8Array(INDICATOR_RESULT_SIZE); // HOCDBIndicatorResult

        let rc: number;
        let fn: string;
        if (options.tail !== undefined) {
            const nLast = BigInt(Math.max(0, Math.floor(options.tail)));
            if (colsBuf2 === null) {
                fn = "hocdb_indicators_tail";
                rc = symbols.hocdb_indicators_tail(this.db, nLast, ptr(colsBuf), ptr(specsBuf), nSpecs, lookback, bucket, ptr(out));
            } else {
                fn = "hocdb_pair_indicators_tail";
                rc = symbols.hocdb_pair_indicators_tail(this.db, ptr(colsBuf), other!.db, ptr(colsBuf2), nLast,
                    ptr(specsBuf), nSpecs, lookback, bucket, ptr(out));
            }
        } else {
            const start = options.start === undefined ? INT64_MIN : BigInt(options.start);
            const end = options.end === undefined ? INT64_MAX : BigInt(options.end);
            if (colsBuf2 === null) {
                fn = "hocdb_indicators";
                rc = symbols.hocdb_indicators(this.db, start, end, ptr(colsBuf), ptr(specsBuf), nSpecs, lookback, bucket, ptr(out));
            } else {
                fn = "hocdb_pair_indicators";
                rc = symbols.hocdb_pair_indicators(this.db, ptr(colsBuf), other!.db, ptr(colsBuf2), start, end,
                    ptr(specsBuf), nSpecs, lookback, bucket, ptr(out));
            }
        }
        if (rc !== 0) throw indicatorError(fn, rc);

        // Copy everything out of native memory, then free it.
        const view = new DataView(out.buffer);
        const nRows = Number(view.getBigUint64(16, true));
        const nOutputs = Number(view.getBigUint64(24, true));
        const timestamps = copyI64(view.getBigUint64(0, true), nRows);
        const values = copyF64(view.getBigUint64(8, true), nRows * nOutputs);
        symbols.hocdb_indicators_free(ptr(out));

        if (nOutputs !== names.length) {
            throw new Error(`${what}: expected ${names.length} outputs but the native library returned ${nOutputs}`);
        }
        const columns: Record<string, Float64Array> = {};
        for (let k = 0; k < nOutputs; k++) {
            columns[names[k]!] = values.subarray(k * nRows, (k + 1) * nRows);
        }
        return { timestamps, n_rows: nRows, n_outputs: nOutputs, names, columns, values };
    }

    /**
     * Aggregate records in [start, end) into OHLCV bars of `bucket` timestamp
     * units. With `options.side` (a 1 = buy / 0 = sell field) every bar also
     * carries `buy_volume`.
     */
    ohlcv(start: number | bigint, end: number | bigint, bucket: number | bigint, options: OhlcvOptions = {}): OhlcvResult {
        if (!this.db) throw new Error("Database not initialized");
        const price = this.optionalField(options.price, "price field", this.autoColumn("close"));
        if (price < 0) throw new Error("ohlcv: no price field found: pass options.price (field name or index)");
        const volume = this.optionalField(options.volume, "volume field", this.autoColumn("volume"));
        const side = this.optionalField(options.side, "side field", -1); // never auto-detected: buy_volume only on request

        const out = new Uint8Array(BARS_EX_SIZE); // HOCDBBarsEx
        const rc = symbols.hocdb_ohlcv_ex(this.db, BigInt(start), BigInt(end), BigInt(price), BigInt(volume), BigInt(side), BigInt(bucket), ptr(out));
        if (rc !== 0) throw indicatorError("hocdb_ohlcv_ex", rc);

        const view = new DataView(out.buffer);
        const nBars = Number(view.getBigUint64(56, true));
        const result: OhlcvResult = {
            timestamps: copyI64(view.getBigUint64(0, true), nBars),
            n_bars: nBars,
            open: copyF64(view.getBigUint64(8, true), nBars),
            high: copyF64(view.getBigUint64(16, true), nBars),
            low: copyF64(view.getBigUint64(24, true), nBars),
            close: copyF64(view.getBigUint64(32, true), nBars),
            volume: copyF64(view.getBigUint64(40, true), nBars),
            count: copyF64(view.getBigUint64(48, true), nBars),
        };
        const buyVolumePtr = view.getBigUint64(64, true); // NULL when no side field was given
        if (side >= 0 && buyVolumePtr !== 0n) result.buy_volume = copyF64(buyVolumePtr, nBars);
        symbols.hocdb_ohlcv_ex_free(ptr(out));
        return result;
    }

    /** Scalar performance / risk summary of a field over [start, end). */
    summary(start: number | bigint, end: number | bigint, field: string | number, periodsPerYear: number = 0): SummaryResult {
        if (!this.db) throw new Error("Database not initialized");
        const idx = this.fieldIndex(field, "summary field");
        const layout = summaryLayout();
        const buffer = new Uint8Array(layout.size); // HOCDBSummary
        const rc = symbols.hocdb_summary(this.db, BigInt(start), BigInt(end), BigInt(idx), periodsPerYear, ptr(buffer));
        if (rc !== 0) throw indicatorError("hocdb_summary", rc);
        return decodeStruct(buffer, layout) as SummaryResult;
    }

    /** One-shot snapshot of ~100 indicators for the latest bar. */
    snapshot(options: SnapshotOptions = {}): SnapshotResult {
        if (!this.db) throw new Error("Database not initialized");
        const colsBuf = this.encodeColumns(options.columns);
        const layout = snapshotLayout();
        const buffer = new Uint8Array(layout.size); // HOCDBSnapshot
        const rc = symbols.hocdb_snapshot(this.db, ptr(colsBuf), BigInt(Math.max(0, Math.floor(options.bars ?? 0))),
            BigInt(options.bucket ?? 0), options.periodsPerYear ?? 0, ptr(buffer));
        if (rc !== 0) throw indicatorError("hocdb_snapshot", rc);
        return decodeStruct(buffer, layout) as SnapshotResult;
    }

    /**
     * Snapshots for several bar sizes from one read of the data: one
     * `SnapshotResult` per entry of `buckets`, in the same order, each built
     * from the last `bars` bars of that size and annualised with the matching
     * `periodsPerYear` entry.
     */
    snapshotMulti(options: SnapshotMultiOptions): SnapshotResult[] {
        if (!this.db) throw new Error("Database not initialized");
        const buckets = options?.buckets;
        if (!Array.isArray(buckets) || buckets.length === 0) throw new Error("snapshotMulti: 'buckets' must be a non-empty array of bar sizes");
        const ppy = options.periodsPerYear ?? new Array<number>(buckets.length).fill(0);
        if (!Array.isArray(ppy) || ppy.length !== buckets.length) {
            throw new Error(`snapshotMulti: 'periodsPerYear' must have one entry per bucket (${buckets.length}), got ${Array.isArray(ppy) ? ppy.length : typeof ppy}`);
        }
        const colsBuf = this.encodeColumns(options.columns);
        const layout = snapshotLayout();
        const bucketsBuf = new BigInt64Array(buckets.length);
        const ppyBuf = new Float64Array(buckets.length);
        for (let i = 0; i < buckets.length; i++) {
            bucketsBuf[i] = BigInt(buckets[i]!);
            ppyBuf[i] = Number(ppy[i]);
        }
        const buffer = new Uint8Array(layout.size * buckets.length); // HOCDBSnapshot[n_buckets]
        const rc = symbols.hocdb_snapshot_multi(this.db, ptr(colsBuf), BigInt(Math.max(0, Math.floor(options.bars ?? 0))),
            ptr(bucketsBuf), BigInt(buckets.length), ptr(ppyBuf), ptr(buffer));
        if (rc !== 0) throw indicatorError("hocdb_snapshot_multi", rc);
        const snapshots: SnapshotResult[] = [];
        for (let i = 0; i < buckets.length; i++) {
            snapshots.push(decodeStruct(buffer.subarray(i * layout.size, (i + 1) * layout.size), layout) as SnapshotResult);
        }
        return snapshots;
    }

    /**
     * Data-quality statistics of the records in [start, end): timestamp gaps
     * (those above `gapThreshold` timestamp units are counted in `n_gaps`),
     * non-positive / NaN prices, outlier returns (|log return| above
     * `outlierThreshold`) and, when a volume field is given, zero / negative
     * volumes. `price` defaults to the close / price field; `volume` is off by default.
     */
    health(start: number | bigint, end: number | bigint, price?: FieldRef, volume: FieldRef = null,
           gapThreshold: number | bigint = 0, outlierThreshold: number = 0): HealthResult {
        if (!this.db) throw new Error("Database not initialized");
        const priceIdx = this.optionalField(price, "price field", this.autoColumn("close"));
        if (priceIdx < 0) throw new Error("health: no price field found: pass a price field name or index");
        const volumeIdx = this.optionalField(volume, "volume field", -1);
        const buffer = new Uint8Array(HEALTH_LAYOUT.size); // HOCDBHealth
        const rc = symbols.hocdb_health(this.db, BigInt(start), BigInt(end), BigInt(priceIdx), BigInt(volumeIdx),
            BigInt(gapThreshold), Number(outlierThreshold), ptr(buffer));
        if (rc !== 0) throw indicatorError("hocdb_health", rc);
        return decodeStruct(buffer, HEALTH_LAYOUT) as HealthResult;
    }

    /**
     * Evaluate trading decisions against the stored prices: each decision
     * enters at the first price at or after its timestamp and exits at the
     * first price at or after timestamp + horizon (`defaultHorizon` when the
     * decision has none), paying `costBps` per side. Returns the aggregate
     * statistics plus per-decision `entry`, `exit` and `net_return` arrays
     * (NaN where a decision could not be evaluated, e.g. flat or beyond the data).
     */
    evaluate(decisions: Decision[], options: EvaluateOptions = {}): EvaluationResult {
        if (!this.db) throw new Error("Database not initialized");
        if (!Array.isArray(decisions)) throw new Error("evaluate: 'decisions' must be an array");
        const priceIdx = this.optionalField(options.priceField, "price field", this.autoColumn("close"));
        if (priceIdx < 0) throw new Error("evaluate: no price field found: pass options.priceField (field name or index)");

        const n = decisions.length;
        const decBuf = new Uint8Array(Math.max(n, 1) * DECISION_SIZE); // HOCDBDecision[n]
        const view = new DataView(decBuf.buffer);
        for (let i = 0; i < n; i++) {
            const d = decisions[i]!;
            if (d === null || typeof d !== "object" || d.timestamp === undefined) {
                throw new Error(`evaluate: decision ${i} must be an object with at least 'timestamp' and 'direction'`);
            }
            const o = i * DECISION_SIZE;
            view.setBigInt64(o + 0, BigInt(d.timestamp), true);
            view.setFloat64(o + 8, Number(d.direction ?? 0), true);
            view.setFloat64(o + 16, Number(d.size ?? 1), true);
            view.setBigInt64(o + 24, BigInt(d.horizon ?? 0), true);
        }
        const entry = new Float64Array(n);
        const exit = new Float64Array(n);
        const net = new Float64Array(n);
        const out = new Uint8Array(EVALUATION_LAYOUT.size); // HOCDBEvaluation
        const rc = symbols.hocdb_evaluate(this.db, BigInt(priceIdx),
            n === 0 ? null : ptr(decBuf), BigInt(n),
            BigInt(options.defaultHorizon ?? 0), Number(options.costBps ?? 0), ptr(out),
            n === 0 ? null : ptr(entry), n === 0 ? null : ptr(exit), n === 0 ? null : ptr(net));
        if (rc !== 0) throw indicatorError("hocdb_evaluate", rc);
        const result = decodeStruct(out, EVALUATION_LAYOUT) as unknown as EvaluationResult;
        result.entry = entry;
        result.exit = exit;
        result.net_return = net;
        return result;
    }

    /** Names of every supported indicator kind. */
    static indicatorKinds(): string[] {
        const total = Number(symbols.hocdb_indicator_kinds(null, 0n));
        const ids = new Uint32Array(Math.max(total, 1));
        symbols.hocdb_indicator_kinds(ptr(ids), BigInt(total));
        return Array.from(ids.subarray(0, total), (id) => indicatorKindName(id));
    }

    /** Output names of a kind (e.g. "macd" -> ["macd", "signal", "hist"]). */
    static indicatorOutputs(kind: string | number): string[] {
        return indicatorOutputNames(resolveIndicatorKind(kind));
    }

    /** Recommended warm-up rows for a spec (after applying defaults). */
    static indicatorWarmup(spec: IndicatorSpec): number {
        const kinds = [resolveIndicatorKind(spec.kind)];
        // warm-up does not depend on the field; names cannot be resolved without a schema
        const buffer = encodeIndicatorSpecs([spec], kinds, (f) => typeof f === "number" ? BigInt(Math.floor(f)) : -1n);
        return Number(symbols.hocdb_indicator_warmup(ptr(buffer)));
    }

    /**
     * True for kinds that use FUTURE rows (the labels `forward_return` and
     * `triple_barrier`): their values are NaN at the end of every window and
     * must never be used as live features.
     */
    static indicatorIsLookahead(kind: string | number): boolean {
        return symbols.hocdb_indicator_is_lookahead(resolveIndicatorKind(kind)) === 1;
    }

    // -----------------------------------------------------------------------
    // Trading calendars (module-level helpers; all times are UTC seconds)
    // -----------------------------------------------------------------------

    /** Id of a built-in or custom calendar by name (case-insensitive), 0 when unknown. */
    static calendarId(name: string): number {
        if (typeof name !== "string") throw new Error("calendarId: 'name' must be a string");
        return symbols.hocdb_calendar_id(ptr(cstr(name)));
    }

    /** Name of a calendar id, or null for an unknown id. */
    static calendarName(id: number): string | null {
        return calendarNameOf(requireInt(id, "calendarName: id"));
    }

    /**
     * The session containing `utcSec` (`which` 0 / "at"), that or the previous
     * one (1 / "prev"), or that or the next one (2 / "next"); null when there is
     * none. Throws UnknownCalendar for an unknown id or name.
     */
    static calendarSession(calendar: CalendarRef, utcSec: number | bigint, which: SessionWhich = 0): CalendarSession | null {
        const id = requireCalendar(calendar, "calendarSession");
        const w = resolveEnum("session lookup 'which'", which, SESSION_WHICH, 2, 0);
        const out = new Uint8Array(SESSION_SIZE); // HOCDBSession
        const rc = symbols.hocdb_calendar_session(id, toI64(utcSec, "calendarSession: utcSec"), w, ptr(out));
        if (rc === 1) return decodeSession(out);
        if (rc === 0) return null;
        throw codedError("hocdb_calendar_session", rc, INDICATOR_ERRORS);
    }

    /** The session of a trade date (days since 1970-01-01, see `daysFromCivil`), or null when the calendar is closed that day. */
    static calendarSessionForDay(calendar: CalendarRef, day: number | bigint): CalendarSession | null {
        const id = requireCalendar(calendar, "calendarSessionForDay");
        const out = new Uint8Array(SESSION_SIZE);
        const rc = symbols.hocdb_calendar_session_for_day(id, toI64(day, "calendarSessionForDay: day"), ptr(out));
        if (rc === 1) return decodeSession(out);
        if (rc === 0) return null;
        throw codedError("hocdb_calendar_session_for_day", rc, INDICATOR_ERRORS);
    }

    /** true when the calendar is trading at `utcSec`. */
    static calendarIsOpen(calendar: CalendarRef, utcSec: number | bigint): boolean {
        const id = requireCalendar(calendar, "calendarIsOpen");
        const rc = symbols.hocdb_calendar_is_open(id, toI64(utcSec, "calendarIsOpen: utcSec"));
        if (rc < 0) throw codedError("hocdb_calendar_is_open", rc, INDICATOR_ERRORS);
        return rc === 1;
    }

    /** Seconds of trading time inside [a, b). */
    static calendarOpenSeconds(calendar: CalendarRef, a: number | bigint, b: number | bigint): number {
        const id = requireCalendar(calendar, "calendarOpenSeconds");
        return Number(symbols.hocdb_calendar_open_seconds(id, toI64(a, "calendarOpenSeconds: a"), toI64(b, "calendarOpenSeconds: b")));
    }

    /** Number of sessions opening inside [a, b). */
    static calendarSessionsBetween(calendar: CalendarRef, a: number | bigint, b: number | bigint): number {
        const id = requireCalendar(calendar, "calendarSessionsBetween");
        return Number(symbols.hocdb_calendar_sessions_between(id, toI64(a, "calendarSessionsBetween: a"), toI64(b, "calendarSessionsBetween: b")));
    }

    /** Bars per year for bars of `bucketSec` seconds on this calendar (252 x 390 for one-minute NYSE bars, 365 for daily crypto). */
    static calendarPeriodsPerYear(calendar: CalendarRef, bucketSec: number): number {
        const id = requireCalendar(calendar, "calendarPeriodsPerYear");
        if (typeof bucketSec !== "number" || !(bucketSec > 0)) throw new Error("calendarPeriodsPerYear: bucketSec must be a positive number of seconds");
        return symbols.hocdb_calendar_periods_per_year(id, bucketSec);
    }

    /** Local wall-clock seconds of a UTC instant in the calendar's time zone (DST applied). */
    static calendarToLocal(calendar: CalendarRef, utcSec: number | bigint): number {
        const id = requireCalendar(calendar, "calendarToLocal");
        return Number(symbols.hocdb_calendar_to_local(id, toI64(utcSec, "calendarToLocal: utcSec")));
    }

    /** Days since 1970-01-01 of a civil date (proleptic Gregorian). */
    static daysFromCivil(year: number, month: number, day: number): number {
        if (!Number.isInteger(year)) throw new Error("daysFromCivil: year must be an integer");
        if (!Number.isInteger(month) || month < 1 || month > 12) throw new Error("daysFromCivil: month must be 1-12");
        if (!Number.isInteger(day) || day < 1 || day > 31) throw new Error("daysFromCivil: day must be 1-31");
        return Number(symbols.hocdb_days_from_civil(BigInt(year), month, day));
    }

    /** Civil date of a day number (days since 1970-01-01). */
    static civilFromDays(days: number | bigint): CivilDate {
        const year = new BigInt64Array(1);
        const month = new Uint32Array(1);
        const day = new Uint32Array(1);
        symbols.hocdb_civil_from_days(toI64(days, "civilFromDays: days"), ptr(year), ptr(month), ptr(day));
        return { year: Number(year[0]!), month: month[0]!, day: day[0]! };
    }

    /**
     * Register a custom calendar (process-local) and return its id (>= 32).
     * Either `calendarDefine(name, { weekly, utc_offset_sec, dst_rule, holidays,
     * early_closes, sessions_per_year })` or the positional form
     * `calendarDefine(name, weekly, utcOffsetSec, dstRule, holidays, earlyCloses, sessionsPerYear)`.
     * `weekly` has 7 entries, Monday first, each `{ open_sec, close_sec }` in local
     * seconds relative to the trade date's midnight or null for no session;
     * `holidays` are day numbers (`daysFromCivil`), `early_closes` are
     * `{ day, close_sec }`. Redefining a name reuses its id.
     */
    static calendarDefine(name: string, definition: CalendarDefinition): number;
    static calendarDefine(name: string, weekly: (DaySession | null)[], utcOffsetSec?: number, dstRule?: DstRule, holidays?: number[], earlyCloses?: EarlyClose[], sessionsPerYear?: number): number;
    static calendarDefine(name: string, def: CalendarDefinition | (DaySession | null)[], utcOffsetSec: number = 0, dstRule: DstRule = "none",
                          holidays: number[] = [], earlyCloses: EarlyClose[] = [], sessionsPerYear: number = 0): number {
        if (typeof name !== "string" || name.length === 0) throw new Error("calendarDefine: 'name' must be a non-empty string");
        let d: CalendarDefinition;
        if (Array.isArray(def)) {
            d = { weekly: def, utc_offset_sec: utcOffsetSec, dst_rule: dstRule, holidays, early_closes: earlyCloses, sessions_per_year: sessionsPerYear };
        } else if (def !== null && typeof def === "object") {
            d = def;
        } else {
            throw new Error("calendarDefine: expected a definition object or the 7-entry weekly template as the second argument");
        }
        const dd = d as unknown as Record<string, unknown>;
        const weekly = d.weekly;
        if (!Array.isArray(weekly) || weekly.length !== 7) throw new Error("calendarDefine: 'weekly' must have 7 entries (Monday first), each { open_sec, close_sec } or null");
        const wk = new Int32Array(14); // HOCDBDaySession[7]; 0 / 0 = no session
        for (let i = 0; i < 7; i++) {
            const w = weekly[i];
            if (w === null || w === undefined) continue;
            const o = opt<unknown>(w as unknown as Record<string, unknown>, "open_sec", "openSec");
            const c = opt<unknown>(w as unknown as Record<string, unknown>, "close_sec", "closeSec");
            if (typeof o !== "number" || typeof c !== "number" || !Number.isInteger(o) || !Number.isInteger(c)) {
                throw new Error(`calendarDefine: weekly[${i}] must be { open_sec, close_sec } (integer local seconds) or null`);
            }
            if (c <= o) throw new Error(`calendarDefine: weekly[${i}]: close_sec must be greater than open_sec (use null for no session)`);
            wk[2 * i] = o;
            wk[2 * i + 1] = c;
        }
        const offset = opt<unknown>(dd, "utc_offset_sec", "utcOffsetSec") ?? 0;
        if (typeof offset !== "number" || !Number.isInteger(offset)) throw new Error("calendarDefine: utc_offset_sec must be an integer number of seconds");
        const dst = resolveEnum("dst_rule", opt(dd, "dst_rule", "dstRule"), DST_RULES, 2, 0);
        const hol = opt<unknown>(dd, "holidays") ?? [];
        if (!Array.isArray(hol)) throw new Error("calendarDefine: 'holidays' must be an array of day numbers");
        const hArr = Int32Array.from(hol, (h, i) => requireInt(h, `calendarDefine: holidays[${i}]`, -2147483648));
        const early = opt<unknown>(dd, "early_closes", "earlyCloses") ?? [];
        if (!Array.isArray(early)) throw new Error("calendarDefine: 'early_closes' must be an array of { day, close_sec }");
        const eArr = new Int32Array(2 * early.length); // HOCDBEarlyClose[]
        for (let i = 0; i < early.length; i++) {
            const e = early[i] as Record<string, unknown>;
            if (e === null || typeof e !== "object") throw new Error(`calendarDefine: early_closes[${i}] must be { day, close_sec }`);
            eArr[2 * i] = requireInt(opt<unknown>(e, "day"), `calendarDefine: early_closes[${i}].day`, -2147483648);
            eArr[2 * i + 1] = requireInt(opt<unknown>(e, "close_sec", "closeSec"), `calendarDefine: early_closes[${i}].close_sec`, -2147483648);
        }
        const spy = opt<unknown>(dd, "sessions_per_year", "sessionsPerYear");
        if (typeof spy !== "number" || !(spy > 0)) throw new Error("calendarDefine: 'sessions_per_year' must be > 0 (e.g. 252 for a five-day exchange calendar, 365 for 24/7)");
        const id = symbols.hocdb_calendar_define(ptr(cstr(name)), ptr(wk), offset, dst,
            hArr.length > 0 ? ptr(hArr) : null, BigInt(hArr.length), early.length > 0 ? ptr(eArr) : null, BigInt(early.length), spy);
        if (id === -1n) throw new Error("calendarDefine: the custom calendar registry is full (32 calendars per process; redefining an existing name reuses its id)");
        if (id <= 0n) throw new Error(`calendarDefine: invalid calendar definition for '${name}'`);
        return Number(id);
    }

    // per-handle calendar and timestamp unit

    /** Set this handle's trading calendar (id or name). Writers persist built-in ids in the file header; readers keep it local. Throws UnknownCalendar. */
    setCalendar(calendar: CalendarRef): void {
        this.assertOpen();
        const id = resolveCalendarRef(calendar, "setCalendar");
        const rc = symbols.hocdb_set_calendar(this.db, id);
        if (rc !== 0) throw codedError("hocdb_set_calendar", rc, INDICATOR_ERRORS);
    }

    /** The handle's calendar id (0 = none). */
    getCalendar(): number {
        this.assertOpen();
        return symbols.hocdb_get_calendar(this.db);
    }

    /** The handle's calendar name, or null when it has none. */
    getCalendarName(): string | null {
        const id = this.getCalendar();
        return id === 0 ? null : calendarNameOf(id);
    }

    /** Set the nanoseconds per timestamp unit (1000 = microseconds, 1e9 = seconds); writers persist it. */
    setTimestampUnit(unitNs: number | bigint): void {
        this.assertOpen();
        const rc = symbols.hocdb_set_timestamp_unit(this.db, configInt("timestamp unit", unitNs, 0n, U64_MAX));
        if (rc !== 0) throw codedError("hocdb_set_timestamp_unit", rc, INDICATOR_ERRORS);
    }

    /** Nanoseconds per timestamp unit (0 = unknown). */
    getTimestampUnit(): number {
        this.assertOpen();
        return Number(symbols.hocdb_get_timestamp_unit(this.db));
    }

    /** Bars per year for bars of `bucket` timestamp units, from the handle's calendar and unit (0 when either is unknown). */
    periodsPerYear(bucket: number | bigint): number {
        this.assertOpen();
        return symbols.hocdb_periods_per_year(this.db, toI64(bucket, "periodsPerYear: bucket"));
    }

    // -----------------------------------------------------------------------
    // Signal backtester
    // -----------------------------------------------------------------------

    /**
     * Backtest a target-position series over the bars of [start, end):
     * `target[i]` is the desired position at the END of row i of
     * `indicators(specs, { start, end, bucket })` over the same window (bucket > 0:
     * the bars whose start lies in [start, end), equal to `ohlcv()` for
     * bucket-aligned bounds; bucket 0: raw records), so compute the signals with
     * `indicators` and pass one target per row (a length mismatch throws). Fills
     * happen at the next bar's open by default (`fill_mode` 1: the same close);
     * stopped positions are not re-entered on the same signal. With
     * `params.periods_per_year` 0 the handle's calendar supplies it. `options`
     * may also be a bare bucket (`backtest(target, start, end, 300, { params })`).
     */
    backtest(target: Float64Array | number[], start: number | bigint, end: number | bigint,
             options: BacktestOptions | number | bigint = {}, moreOptions: BacktestOptions = {}): BacktestRun {
        if (!this.db) throw new Error("Database not initialized");
        const opts: BacktestOptions = typeof options === "number" || typeof options === "bigint" ? { ...moreOptions, bucket: options } : (options ?? {});
        const colsBuf = this.encodeColumns(opts.columns);
        const b = prepareBacktest(target, opts, "backtest");
        const rc = symbols.hocdb_backtest(this.db, ptr(colsBuf), toI64(start, "backtest: start"), toI64(end, "backtest: end"), toI64(opts.bucket ?? 0, "backtest: bucket"),
            ptr(b.target), BigInt(b.n), ptr(b.params), b.outputsBuf === null ? null : ptr(b.outputsBuf),
            b.trades === null ? null : ptr(b.trades), BigInt(b.tradesCap), ptr(b.result));
        if (rc !== 0) throw codedError("hocdb_backtest", rc, BACKTEST_ERRORS);
        return finishBacktest(b);
    }

    /** Same as `backtest` over the last `target.length` bars (bucket > 0) or records. */
    backtestTail(target: Float64Array | number[], options: BacktestOptions = {}): BacktestRun {
        if (!this.db) throw new Error("Database not initialized");
        const opts = options ?? {};
        const colsBuf = this.encodeColumns(opts.columns);
        const b = prepareBacktest(target, opts, "backtestTail");
        const rc = symbols.hocdb_backtest_tail(this.db, ptr(colsBuf), toI64(opts.bucket ?? 0, "backtestTail: bucket"),
            ptr(b.target), BigInt(b.n), ptr(b.params), b.outputsBuf === null ? null : ptr(b.outputsBuf),
            b.trades === null ? null : ptr(b.trades), BigInt(b.tradesCap), ptr(b.result));
        if (rc !== 0) throw codedError("hocdb_backtest_tail", rc, BACKTEST_ERRORS);
        return finishBacktest(b);
    }

    /** The library's default backtest parameters (what omitted `params` entries take). */
    static backtestDefaults(): ReturnType<typeof decodeBacktestParams> {
        return decodeBacktestParams(encodeBacktestParams({}, "backtestDefaults"));
    }

    /**
     * Backtest on caller-provided bars: `ts` and `close` of n entries, `target`
     * of n entries; `open` / `high` / `low` may be null (fills at the close, no
     * intrabar stops). Options: `params`, `outputs`, `maxTrades`.
     */
    static backtestArrays(ts: BigInt64Array | (number | bigint)[], open: Float64Array | number[] | null | undefined,
                          high: Float64Array | number[] | null | undefined, low: Float64Array | number[] | null | undefined,
                          close: Float64Array | number[], target: Float64Array | number[], options: BacktestArraysOptions = {}): BacktestRun {
        const bars = barArrays(ts, open, high, low, close, "backtestArrays");
        const b = prepareBacktest(target, options ?? {}, "backtestArrays", bars.n);
        const rc = symbols.hocdb_backtest_arrays(ptr(bars.ts), bars.open === null ? null : ptr(bars.open), bars.high === null ? null : ptr(bars.high),
            bars.low === null ? null : ptr(bars.low), ptr(bars.close), BigInt(bars.n), ptr(b.target), ptr(b.params),
            b.outputsBuf === null ? null : ptr(b.outputsBuf), b.trades === null ? null : ptr(b.trades), BigInt(b.tradesCap), ptr(b.result));
        if (rc !== 0) throw codedError("hocdb_backtest_arrays", rc, BACKTEST_ERRORS);
        return finishBacktest(b);
    }

    /**
     * Walk-forward index ranges over n bars: the first train window is
     * floor(trainFrac * n) bars and the test windows tile the rest in nSplits
     * pieces; `anchored` grows the train window from 0, otherwise it rolls.
     * Ends are exclusive.
     */
    static walkForwardSplits(n: number, nSplits: number, trainFrac: number, anchored: boolean = false): Split[] {
        requireInt(n, "walkForwardSplits: n");
        requireInt(nSplits, "walkForwardSplits: nSplits", 1);
        if (typeof trainFrac !== "number" || !(trainFrac >= 0 && trainFrac <= 1)) throw new Error("walkForwardSplits: trainFrac must be in [0, 1]");
        const out = new BigUint64Array(4 * nSplits); // HOCDBSplit[nSplits]
        const k = Number(symbols.hocdb_walk_forward_splits(BigInt(n), BigInt(nSplits), trainFrac, anchored ? 1 : 0, ptr(out), BigInt(nSplits)));
        const splits: Split[] = [];
        for (let i = 0; i < k; i++) {
            splits.push({
                train_start: Number(out[4 * i]!), train_end: Number(out[4 * i + 1]!),
                test_start: Number(out[4 * i + 2]!), test_end: Number(out[4 * i + 3]!),
            });
        }
        return splits;
    }

    /** Run `backtestArrays` on every test window of `splits` independently (fresh equity each); one result per split. */
    static backtestSplits(ts: BigInt64Array | (number | bigint)[], open: Float64Array | number[] | null | undefined,
                          high: Float64Array | number[] | null | undefined, low: Float64Array | number[] | null | undefined,
                          close: Float64Array | number[], target: Float64Array | number[], splits: Split[], params?: BacktestParams): BacktestResult[] {
        const bars = barArrays(ts, open, high, low, close, "backtestSplits");
        const tgt = f64Series(target, "backtestSplits: target", bars.n);
        const p = encodeBacktestParams(params, "backtestSplits");
        const sp = encodeSplits(splits, "backtestSplits");
        const nSplits = splits.length;
        const results = new Uint8Array(nSplits * BACKTEST_RESULT_LAYOUT.size); // HOCDBBacktestResult[nSplits]
        const rc = symbols.hocdb_backtest_splits_arrays(ptr(bars.ts), bars.open === null ? null : ptr(bars.open), bars.high === null ? null : ptr(bars.high),
            bars.low === null ? null : ptr(bars.low), ptr(bars.close), BigInt(bars.n), ptr(tgt), ptr(p), ptr(sp), BigInt(nSplits), ptr(results));
        if (rc < 0) throw codedError("hocdb_backtest_splits_arrays", rc, BACKTEST_ERRORS);
        const out: BacktestResult[] = [];
        for (let i = 0; i < rc; i++) {
            out.push(decodeStruct(results.subarray(i * BACKTEST_RESULT_LAYOUT.size, (i + 1) * BACKTEST_RESULT_LAYOUT.size), BACKTEST_RESULT_LAYOUT) as BacktestResult);
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Universe (cross-sectional) features
    // -----------------------------------------------------------------------

    /** The library's default universe parameters. */
    static universeDefaults(): ReturnType<typeof decodeUniverseParams> {
        return decodeUniverseParams(encodeUniverseParams({}, "universeDefaults"));
    }

    /**
     * Cross-sectional features over a watch-list of open databases with the
     * same column roles: the last `bars` bars (`bucket` > 0; 0 = enough for the
     * longest period) or records of every database are inner-joined on
     * timestamps, then per-ticker momentum / volatility / relative-strength
     * ranks, betas and correlations to the market factor and the universe-level
     * dispersion / breadth are computed for the last bar. Returns `{ summary,
     * rows, corr }` (`corr` is n x n unless `corr: false`).
     */
    static universe(dbs: HOCDB[], options: UniverseOptions = {}): UniverseResult {
        if (!Array.isArray(dbs) || dbs.length === 0) throw new Error("universe: 'dbs' must be a non-empty array of open HOCDB instances");
        const opts = options ?? {};
        const n = dbs.length;
        const handles = new BigUint64Array(n); // HOCDBHandle[n]
        let colsBuf: Uint8Array | null = null;
        for (let i = 0; i < n; i++) {
            const d = dbs[i]!;
            if (!(d instanceof HOCDB)) throw new Error(`universe: dbs[${i}] is not a HOCDB instance`);
            if (!d.db) throw new Error(`universe: dbs[${i}] is not initialized (closed or dropped?)`);
            const cb = d.encodeColumns(opts.columns);
            if (colsBuf === null) colsBuf = cb;
            else if (!bytesEqual(colsBuf, cb)) {
                throw new Error(`universe: dbs[${i}] ('${d.ticker}') resolves the column roles to different field indices than dbs[0]; every database must share the same roles (pass explicit 'columns')`);
            }
            handles[i] = BigInt(d.db);
        }
        const params = encodeUniverseParams(opts.params, "universe");
        const nBars = opt<unknown>(opts as unknown as Record<string, unknown>, "bars", "nBars", "n_bars") ?? 0;
        requireInt(nBars, "universe: bars");
        const wantCorr = opts.corr !== false;
        const rows = new Uint8Array(n * UNIVERSE_ROW_LAYOUT.size);            // HOCDBUniverseRow[n]
        const corr = wantCorr ? new Float64Array(n * n) : null;
        const summary = new Uint8Array(UNIVERSE_SUMMARY_LAYOUT.size);        // HOCDBUniverseSummary
        const rc = symbols.hocdb_universe(ptr(handles), BigInt(n), ptr(colsBuf!), BigInt(nBars as number), toI64(opts.bucket ?? 0, "universe: bucket"),
            ptr(params), ptr(rows), corr === null ? null : ptr(corr), ptr(summary));
        if (rc !== 0) throw codedError("hocdb_universe", rc, UNIVERSE_ERRORS);
        return decodeUniverse(n, rows, corr, summary);
    }

    /**
     * The same features on caller-provided aligned series: `closes` is one
     * series per ticker (all of the same length); `options.volumes` (same shape)
     * and `options.ts` (bar timestamps, only for summary.first_ts / last_ts) are optional.
     */
    static universeArrays(closes: (Float64Array | number[])[], options: UniverseArraysOptions = {}): UniverseResult {
        if (!Array.isArray(closes) || closes.length === 0) throw new Error("universeArrays: 'closes' must be a non-empty array of series (one per ticker)");
        const opts = options ?? {};
        const n = closes.length;
        const cl = closes.map((c, i) => f64Series(c, `universeArrays: closes[${i}]`));
        const nBars = cl[0]!.length;
        if (nBars === 0) throw new Error("universeArrays: series must not be empty");
        for (let i = 1; i < n; i++) {
            if (cl[i]!.length !== nBars) throw new Error(`universeArrays: series length mismatch: closes[${i}] has ${cl[i]!.length} bars, closes[0] has ${nBars}`);
        }
        const closePtrs = new BigUint64Array(n);
        for (let i = 0; i < n; i++) closePtrs[i] = BigInt(ptr(cl[i]!));
        let vl: Float64Array[] | null = null;
        let volPtrs: BigUint64Array | null = null;
        if (opts.volumes !== undefined && opts.volumes !== null) {
            if (!Array.isArray(opts.volumes) || opts.volumes.length !== n) throw new Error(`universeArrays: 'volumes' must have one series per ticker (${n})`);
            vl = opts.volumes.map((v, i) => f64Series(v, `universeArrays: volumes[${i}]`, nBars));
            volPtrs = new BigUint64Array(n);
            for (let i = 0; i < n; i++) volPtrs[i] = BigInt(ptr(vl[i]!));
        }
        const ts = opts.ts === undefined || opts.ts === null ? null : i64Series(opts.ts, "universeArrays: ts", nBars);
        const params = encodeUniverseParams(opts.params, "universeArrays");
        const wantCorr = opts.corr !== false;
        const rows = new Uint8Array(n * UNIVERSE_ROW_LAYOUT.size);
        const corr = wantCorr ? new Float64Array(n * n) : null;
        const summary = new Uint8Array(UNIVERSE_SUMMARY_LAYOUT.size);
        const rc = symbols.hocdb_universe_arrays(ptr(closePtrs), volPtrs === null ? null : ptr(volPtrs), BigInt(n), BigInt(nBars),
            ts === null ? null : ptr(ts), ptr(params), ptr(rows), corr === null ? null : ptr(corr), ptr(summary));
        if (rc !== 0) throw codedError("hocdb_universe_arrays", rc, UNIVERSE_ERRORS);
        return decodeUniverse(n, rows, corr, summary);
    }

    // -----------------------------------------------------------------------
    // Durability, lock-free readers, maintenance and metrics
    // -----------------------------------------------------------------------

    /**
     * Attach to a database another process writes, without taking any lock.
     * Every read entry point (and `refresh()`) picks up the writer's latest
     * commit, so only flushed data is visible; the reader follows compaction
     * and rollover automatically. Writes and maintenance throw a read-only
     * error. Requires a committed current-format file (a database that has
     * never been flushed fails with EmptyDatabase; a legacy HOC1 file needs
     * one writer open first to migrate it).
     */
    static openReader(ticker: string, path: string, schema: FieldDef[]): HOCDB {
        return new HOCDB(ticker, path, schema, { readOnly: true });
    }

    /** Bytes reserved by the file header of new files (64): ring capacity = (max_file_size - headerSize()) / record_size. */
    static headerSize(): number {
        return Number(symbols.hocdb_header_size());
    }

    /** true when this handle is a lock-free reader (HOCDB.openReader / readOnly: true). */
    isReadOnly(): boolean {
        if (this.db) return symbols.hocdb_is_read_only(this.db) === 1;
        return this.readOnly;
    }

    /** File format version of the open database: 1 legacy (HOC1), 2 current (HOC2). */
    formatVersion(): number {
        this.assertOpen();
        return symbols.hocdb_format_version(this.db);
    }

    /** Readers: pick up the writer's latest commit now (every read does this as well). Writers: no-op. */
    refresh(): void {
        this.assertOpen();
        const rc = symbols.hocdb_refresh(this.db);
        if (rc !== 0) throw storageError("hocdb_refresh", rc);
    }

    /** Flush and fsync now, whatever the fsync policy (writers only). */
    sync(): void {
        this.assertOpen();
        const rc = symbols.hocdb_sync(this.db);
        if (rc !== 0) throw storageError("hocdb_sync", rc);
    }

    /**
     * Recompute the CRC32C of the committed data: true = matches, false =
     * MISMATCH. Throws "checksum unavailable" for ring buffers, legacy HOC1
     * files and files whose recovered crash tail has not been committed yet.
     */
    verify(): boolean {
        this.assertOpen();
        const rc = symbols.hocdb_verify(this.db);
        if (rc === 1) return true;
        if (rc === 0) return false;
        throw storageError("hocdb_verify", rc);
    }

    /** Keep only the records with timestamp >= minTs (atomic rewrite of the file; readers follow). Writers only. */
    compact(minTs: number | bigint): void {
        this.assertOpen();
        const rc = symbols.hocdb_compact(this.db, BigInt(minTs));
        if (rc !== 0) throw storageError("hocdb_compact", rc);
    }

    /** Keep only the last n records (atomic rewrite of the file; readers follow). Writers only. */
    retainLast(n: number | bigint): void {
        this.assertOpen();
        const rc = symbols.hocdb_retain_last(this.db, BigInt(n));
        if (rc !== 0) throw storageError("hocdb_retain_last", rc);
    }

    /**
     * Archive the current file as `<ticker>.<first_ts>-<last_ts>.bin` in the
     * same directory and continue with an empty file (timestamps stay
     * monotonic across files). Returns the archive path; the archive opens as
     * a normal database whose ticker is the file name without ".bin". Writers only.
     */
    rollover(): string {
        this.assertOpen();
        const out = new Uint8Array(4096);
        const rc = symbols.hocdb_rollover(this.db, ptr(out), BigInt(out.length));
        if (rc !== 0) throw storageError("hocdb_rollover", rc);
        const end = out.indexOf(0);
        return decoder.decode(out.subarray(0, end < 0 ? out.length : end));
    }

    /** Operational counters: the 30 HOCDBMetrics fields decoded by name (see MetricsResult). */
    metrics(): MetricsResult {
        this.assertOpen();
        const buffer = new Uint8Array(METRICS_LAYOUT.size); // HOCDBMetrics
        const rc = symbols.hocdb_metrics(this.db, ptr(buffer));
        if (rc !== 0) throw storageError("hocdb_metrics", rc);
        return decodeStruct(buffer, METRICS_LAYOUT) as MetricsResult;
    }

    /** Reset the counters; state fields (last_record_ts, committed_records, file_size, ...) are kept. */
    metricsReset(): void {
        this.assertOpen();
        symbols.hocdb_metrics_reset(this.db);
    }

    close() {
        if (this.db) {
            symbols.hocdb_close(this.db);
            this.db = null;
        }
    }

    /** Close the database and delete its data file. Writers only: a reader must not delete the writer's file. */
    drop() {
        if (this.db) {
            if (this.readOnly) throw new Error(`drop failed: ${READ_ONLY_MESSAGE}`);
            symbols.hocdb_drop(this.db);
            this.db = null;
        }
    }

    static async initAsync(ticker: string, path: string, schema: FieldDef[], config: DBConfig = {}) {
        console.warn("HOCDB.initAsync is deprecated. Use new HOCDBAsync() instead.");
        return new HOCDBAsync(ticker, path, schema, config);
    }
}

export class HOCDBAsync {
    private worker: Worker;
    private msgId: number = 0;
    private pending: Map<number, { resolve: (value: any) => void, reject: (reason?: any) => void }>;

    // Schema info for parsing raw buffers
    recordSize: number;
    fieldOffsets: Record<string, { offset: number, type: string, index: number }>;

    constructor(ticker: string, path: string, schema: FieldDef[], config: DBConfig = {}) {
        const workerURL = new URL("worker.ts", import.meta.url).href;
        this.worker = new Worker(workerURL);
        this.pending = new Map();

        // Process schema locally so we can parse raw buffers
        const { recordSize, fieldOffsets } = processSchema(schema);
        this.recordSize = recordSize;
        this.fieldOffsets = fieldOffsets;

        this.worker.onmessage = (event) => {
            const { id, result, error } = event.data;
            if (this.pending.has(id)) {
                const { resolve, reject } = this.pending.get(id)!;
                this.pending.delete(id);
                if (error) reject(new Error(error));
                else resolve(result);
            }
        };

        this.worker.onerror = (err) => {
            console.error("Worker error:", err);
        };

        this.callWorker('init', { ticker, path, schema, config }).catch(err => {
            console.error("Failed to initialize HOCDBAsync:", err);
        });
    }

    private callWorker(type: string, payload: any): Promise<any> {
        return new Promise((resolve, reject) => {
            const id = this.msgId++;
            this.pending.set(id, { resolve, reject });
            this.worker.postMessage({ id, type, payload });
        });
    }

    async append(data: any): Promise<void> {
        await this.callWorker('append', data);
    }

    async appendBatch(data: any[]): Promise<void> {
        await this.callWorker('appendBatch', data);
    }

    async flush(): Promise<void> {
        await this.callWorker('flush', {});
    }

    async query(start: bigint | number, end: bigint | number, filters: any): Promise<any[]> {
        // Request RAW buffer from worker
        const buffer = await this.callWorker('queryRaw', { start, end, filters });
        if (!buffer || buffer.byteLength === 0) return [];

        // Parse on main thread
        return parseBuffer(buffer, this.recordSize, this.fieldOffsets);
    }

    async load(): Promise<any[]> {
        return this.callWorker('load', {});
    }

    async getStats(start: bigint | number, end: bigint | number, field_index: number): Promise<any> {
        return this.callWorker('getStats', { start, end, field_index });
    }

    async getLatest(field_index: number): Promise<any> {
        return this.callWorker('getLatest', { field_index });
    }

    async close(): Promise<void> {
        await this.callWorker('close', {});
        this.worker.terminate();
    }

    async drop(): Promise<void> {
        await this.callWorker('drop', {});
        this.worker.terminate();
    }
}
