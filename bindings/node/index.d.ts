export interface TradeData {
    timestamp: number; // i64 (passed as number, precision loss possible > 2^53)
    usd: number;       // f64
    volume: number;    // f64
}

export interface HOCDB {
    /**
     * Appends a record to the database.
     * @param timestamp Timestamp (i64)
     * @param usd USD Value (f64)
     * @param volume Volume (f64)
     */
    dbAppend(timestamp: number, usd: number, volume: number): void;

    /**
     * Loads all records into a zero-copy ArrayBuffer.
     * The returned buffer is backed by Zig memory.
     * @returns Float64Array view of the data (Note: struct layout matters)
     */
    dbLoad(): ArrayBuffer;

    /**
     * Closes the database and frees resources.
     */
    dbClose(): void;
}

/**
 * Initializes the database.
 * @param ticker Ticker symbol (e.g., "BTC_USD")
 * @param path Directory path for data
 * @returns Database instance
 */
/**
 * Durability policy for flushed data: "none" (the OS decides), "on_close" (default: once when
 * closing), "on_flush" (after every flush), "interval" (at most every fsync_interval_ms, and on
 * close). The numbers 0-3 (hocdb.FSYNC) are accepted too.
 */
export type FsyncPolicy = 'none' | 'on_close' | 'on_flush' | 'interval' | 0 | 1 | 2 | 3;

/** Options of dbInit / dbInitAsync; every option is accepted in snake_case or camelCase. */
export interface DBConfig {
    /** File size cap in bytes; 0 = default (2 GiB). A ring buffer of N records needs headerSize() + N * recordSize. */
    max_file_size?: number | bigint; maxFileSize?: number | bigint;
    /** Ring buffer: overwrite the oldest records when the file is full (default true). */
    overwrite_on_full?: boolean; overwriteOnFull?: boolean;
    /** Flush (commit) after every append. */
    flush_on_write?: boolean; flushOnWrite?: boolean;
    /** Timestamps are assigned by the database. */
    auto_increment?: boolean; autoIncrement?: boolean;
    /** fsync policy (default "on_close"). */
    fsync?: FsyncPolicy; fsync_policy?: FsyncPolicy; fsyncPolicy?: FsyncPolicy;
    /** Interval for fsync: "interval" (default 1000). */
    fsync_interval_ms?: number; fsyncIntervalMs?: number;
    /** Recompute the checksum when opening; the open fails with ChecksumMismatch when it differs. */
    verify_on_open?: boolean; verifyOnOpen?: boolean;
    /** Drop records older than lastTimestamp - span (timestamp units) by compacting once the excess is > 25 %; 0 = off. */
    retention_span?: number | bigint; retentionSpan?: number | bigint;
    /** Archive the file and continue with an empty one once it exceeds this many bytes; 0 = off. */
    rollover_size?: number | bigint; rolloverSize?: number | bigint;
    /** Rewrite legacy HOC1 files into the current format the first time a writer opens them (default true). */
    auto_migrate?: boolean; autoMigrate?: boolean;
    /** Nanoseconds per timestamp unit (1e9 for seconds); enables metrics().ingest_lag_record_ns. 0 = unknown. */
    timestamp_unit_ns?: number | bigint; timestampUnitNs?: number | bigint;
    /** Records between sparse index entries (default 1024). */
    index_stride?: number; indexStride?: number;
}

// ---------------------------------------------------------------------------
// Durability, readers, maintenance and metrics
// ---------------------------------------------------------------------------

/** Operational counters; every field is a BigInt (latencies in nanoseconds). */
export interface Metrics {
    appends: bigint; bytes_written: bigint; flushes: bigint; commits: bigint;
    fsyncs: bigint; fsync_ns_total: bigint; fsync_ns_max: bigint;
    reads: bigint; read_ns_total: bigint; read_ns_max: bigint; read_ns_last: bigint; read_ns_p50: bigint; read_ns_p99: bigint;
    records_read: bigint; refreshes: bigint;
    /** Records adopted from an uncommitted tail on open (crash recovery) and torn bytes truncated. */
    recovered_tail_records: bigint; dropped_tail_bytes: bigint;
    crc_failures: bigint; compactions: bigint; rollovers: bigint; migrations: bigint;
    last_append_wall_ns: bigint; last_commit_wall_ns: bigint; last_record_ts: bigint;
    /** now - last commit (readers) / last append (writers), wall-clock nanoseconds. */
    ingest_lag_wall_ns: bigint;
    /** now - last record time in nanoseconds when timestamp_unit_ns is set, else 0. */
    ingest_lag_record_ns: bigint;
    committed_records: bigint; file_size: bigint;
    /** 1 legacy, 2 current. */
    format_version: bigint;
    /** 1 for readers. */
    read_only: bigint;
}

/**
 * Storage operations. Writers support all of them; on a reader (openReader) sync / compact /
 * retainLast / rollover throw an Error with code "ReadOnly" whose message says the handle is a reader.
 */
export interface StorageMethods {
    /** Flush and fsync now, whatever the fsync policy. */
    sync(): void;
    /** Readers: pick up the writer's latest commit (every read does this on its own). Writers: no-op. */
    refresh(): void;
    /** true when the CRC32C of the committed data matches, false on a mismatch; throws code "ChecksumUnavailable" for ring buffers / legacy files. */
    verify(): boolean;
    /** Keep only records with timestamp >= minTs (rewrites the file atomically; readers follow). */
    compact(minTs: Timestamp): void;
    /** Keep only the last n records. */
    retainLast(n: number): void;
    /** Archive the file as <ticker>.<first_ts>-<last_ts>.bin next to it and continue with an empty one. Returns the archive path. */
    rollover(): string;
    metrics(): Metrics;
    metricsReset(): void;
    /** 1 = legacy HOC1 file, 2 = current format. */
    formatVersion(): number;
    /** true for handles opened with openReader / openReaderAsync. */
    isReadOnly(): boolean;
}

export interface AsyncStorageMethods {
    sync(): Promise<{ success: boolean }>;
    refresh(): Promise<{ success: boolean }>;
    verify(): Promise<boolean>;
    compact(minTs: Timestamp): Promise<{ success: boolean }>;
    retainLast(n: number): Promise<{ success: boolean }>;
    rollover(): Promise<string>;
    metrics(): Promise<Metrics>;
    metricsReset(): Promise<{ success: boolean }>;
    formatVersion(): Promise<number>;
    isReadOnly(): Promise<boolean>;
}

export interface FieldDef {
    name: string;
    type: 'i64' | 'f64' | 'u64' | 'bool';
}

// ---------------------------------------------------------------------------
// Indicators & analytics
// ---------------------------------------------------------------------------

/** A field, given by schema name or index. */
export type FieldRef = string | number | bigint;

/** Timestamp argument: BigInt preferred, numbers are converted with BigInt(). */
export type Timestamp = bigint | number;

/** One indicator request. Zero / omitted periods and params select the documented defaults. */
export interface IndicatorSpec {
    /** Kind name (case-insensitive, e.g. "rsi") or numeric kind id. */
    kind: string | number;
    /** Main period (horizon for forward_return / triple_barrier, rows for opening_range). */
    period?: number;
    period2?: number;
    period3?: number;
    period4?: number;
    /**
     * BBANDS k, KELTNER/SUPERTREND multiplier, PSAR acceleration, periods-per-year for
     * HIST_VOL/SHARPE/SORTINO/REALIZED_VOL, timestamp units per second for TRADE_INTENSITY (1e6),
     * up-barrier fraction for TRIPLE_BARRIER (0.02), session length (timestamp units) for the
     * session kinds session_vwap / session_range / opening_range / pivots (mandatory there).
     */
    param?: number;
    /** PSAR max acceleration, TRIPLE_BARRIER down-barrier fraction, session offset for the session kinds. */
    param2?: number;
    /** Run a single-series indicator on this field instead of the close column. */
    field?: FieldRef;
    /** Second series for correl / beta / series2 / ratio / ratio_zscore / rel_strength (single-database calls). */
    field2?: FieldRef;
    /** Column name for the result (default: kind name plus "_<period>" when a period is given). */
    label?: string;
}

/**
 * Which schema fields play the open/high/low/close/volume/bid/ask/side roles.
 * bid / ask / side (1 = buy) are tick-level quotes used by the microstructure kinds;
 * side also yields per-bar buy volume when bucketing.
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

export interface IndicatorOptions {
    /** Window start (inclusive). Default: beginning of the DB. */
    start?: Timestamp;
    /** Window end (exclusive). Default: end of the DB. */
    end?: Timestamp;
    /** Last n rows (or bars when bucket > 0) instead of start/end. */
    tail?: number;
    /**
     * Column roles. Default: auto-detect fields named open/high/low/close/volume/bid/ask/side
     * ("price" stands in for close, "size" / "qty" for volume).
     */
    columns?: IndicatorColumns;
    /** Extra rows read before the window for warm-up. Default "auto" (recommended per-spec warm-up). */
    lookback?: 'auto' | number;
    /** 0 = one row per record; > 0 = aggregate records into OHLCV bars of this many timestamp units first. */
    bucket?: Timestamp;
}

export interface PairIndicatorOptions extends IndicatorOptions {
    /** Column roles of the other database (default: auto-detected from its schema). */
    columns2?: IndicatorColumns;
    /** Alias of columns2. */
    otherColumns?: IndicatorColumns;
}

/**
 * Batch result: `timestamps`, `n_rows` and one Float64Array per output column,
 * named by the naming rule (see README). NaN marks the warm-up region.
 */
export interface IndicatorResult {
    timestamps: BigInt64Array;
    n_rows: number;
    n_outputs: number;
    /** Column names in output order. */
    names: string[];
    /** Raw planar buffer: output k occupies values[k * n_rows, (k + 1) * n_rows). */
    values: Float64Array;
    [column: string]: Float64Array | BigInt64Array | number | string[];
}

export interface OhlcvOptions {
    /** Price field (default: "close", else "price"). */
    price?: FieldRef;
    /** Volume field (default: "volume", else "size" / "qty"; bar volume is the record count when none exists). */
    volume?: FieldRef;
    /** Aggressor side field (1 = buy). When given, the result also has `buy_volume`. */
    side?: FieldRef;
}

export interface Bars {
    timestamps: BigInt64Array;
    open: Float64Array;
    high: Float64Array;
    low: Float64Array;
    close: Float64Array;
    volume: Float64Array;
    count: Float64Array;
    /** Volume traded on the buy side per bar; only present when `side` was given. */
    buy_volume?: Float64Array;
    n_bars: number;
}

/** Scalar performance / risk summary of a series. */
export interface Summary {
    count: bigint;
    first: number; last: number; min: number; max: number; mean: number; std: number;
    total_return: number; log_return: number; ann_return: number; ann_vol: number;
    sharpe: number; sortino: number;
    max_drawdown: number; max_drawdown_bars: number; calmar: number;
    skew: number; kurtosis: number;
    var_95: number; cvar_95: number;
    win_rate: number; avg_gain: number; avg_loss: number; profit_factor: number; best: number; worst: number;
    autocorr_1: number; hurst: number; half_life: number;
}

export interface SnapshotOptions {
    columns?: IndicatorColumns;
    /** Records (or bars when bucket > 0) to use; 0 = recommended (2500). */
    bars?: number;
    bucket?: Timestamp;
    /** Annualisation for volatility / Sharpe / Sortino (0 = none). */
    periodsPerYear?: number;
}

export interface SnapshotMultiOptions {
    /** Bar sizes (timestamp units, each > 0); one snapshot is returned per bucket, in this order. */
    buckets: Timestamp[];
    /** Annualisation per bucket (same length as `buckets`), or one number for all; default 0. */
    periodsPerYear?: number[] | number;
    /** Bars per snapshot; 0 = recommended (2500). */
    bars?: number;
    columns?: IndicatorColumns;
}

/** ~100 indicator values for the latest bar; `timestamp` and `bars` are BigInt, everything else a number. */
export interface Snapshot {
    timestamp: bigint;
    bars: bigint;
    open: number; high: number; low: number; close: number; volume: number;
    sma_5: number; sma_10: number; sma_20: number; sma_50: number; sma_100: number; sma_200: number;
    ema_9: number; ema_12: number; ema_21: number; ema_26: number; ema_50: number; ema_200: number;
    rsi_14: number; macd: number; macd_signal: number; macd_hist: number;
    adx_14: number; atr_14: number; bb_upper: number; bb_middle: number; bb_lower: number;
    supertrend: number; supertrend_dir: number; obv: number; vwap: number; mfi_14: number;
    drawdown: number; sharpe_20: number; sortino_20: number;
    [field: string]: number | bigint;
}

export interface HealthOptions {
    /** Price field (default: "close", else "price"). */
    price?: FieldRef;
    /** Volume field (default: "volume" / "size" / "qty" when present). */
    volume?: FieldRef;
    /** Gaps (timestamp units) above this are counted in n_gaps. */
    gapThreshold?: Timestamp;
    /** |log return| above this is counted as an outlier. */
    outlierThreshold?: number;
}

/** Data-quality statistics; counters and timestamps are BigInt, gaps / returns numbers. */
export interface Health {
    count: bigint;
    first_ts: bigint; last_ts: bigint; span: bigint;
    mean_gap: number; median_gap: number;
    max_gap: bigint; max_gap_at: bigint;
    n_gaps: bigint; n_nonpositive_price: bigint; n_nan_price: bigint; n_outlier_returns: bigint;
    first_outlier_at: bigint;
    max_abs_return: number;
    n_zero_volume: bigint; n_negative_volume: bigint;
}

/** One trading decision: entry at the first price at or after `timestamp`, exit after `horizon`. */
export interface Decision {
    timestamp: Timestamp;
    /** +1 long, -1 short, 0 flat (ignored). */
    direction: number;
    /** Position size in currency units (default 1). */
    size?: number;
    /** Timestamp units until exit; 0 (default) = options.defaultHorizon. */
    horizon?: Timestamp;
}

export interface EvaluateOptions {
    /** Price field (default: "close", else "price"). */
    priceField?: FieldRef;
    /** Horizon for decisions with horizon 0. */
    defaultHorizon?: Timestamp;
    /** Transaction cost per side, in basis points. */
    costBps?: number;
}

/** Evaluation of a batch of decisions plus the per-decision arrays (NaN where not evaluated). */
export interface Evaluation {
    n_decisions: bigint; n_evaluated: bigint; n_long: bigint; n_short: bigint;
    hit_rate: number; avg_return: number; avg_net_return: number; total_pnl: number; total_cost: number;
    sharpe: number; profit_factor: number; max_drawdown: number; avg_win: number; avg_loss: number; best: number; worst: number;
    long_hit_rate: number; short_hit_rate: number; long_avg_return: number; short_avg_return: number;
    /** Entry price per decision. */
    entry: Float64Array;
    /** Exit price per decision. */
    exit: Float64Array;
    /** Net (after-cost) directional return per decision. */
    net_return: Float64Array;
}

export interface IndicatorMethods {
    /** Compute a batch of indicators over a window ([start, end) or the last `tail` rows). */
    indicators(specs: (IndicatorSpec | string | number)[], options?: IndicatorOptions): IndicatorResult;
    /** Same as indicators() for the last n rows (or bars when bucket > 0). */
    indicatorsTail(n: number, specs: (IndicatorSpec | string | number)[], options?: Omit<IndicatorOptions, 'tail' | 'start' | 'end'>): IndicatorResult;
    /** Aggregate records in [start, end) into OHLCV bars of `bucket` timestamp units. null/undefined = open-ended. */
    ohlcv(start: Timestamp | null | undefined, end: Timestamp | null | undefined, bucket: Timestamp, options?: OhlcvOptions): Bars;
    /** Scalar summary of a field over [start, end). */
    summary(start: Timestamp | null | undefined, end: Timestamp | null | undefined, field: FieldRef, periodsPerYear?: number): Summary;
    /** One-shot snapshot of ~100 indicators for the latest bar. */
    snapshot(options?: SnapshotOptions): Snapshot;
    /** Snapshots for several bar sizes from one read, in `buckets` order. */
    snapshotMulti(options: SnapshotMultiOptions): Snapshot[];
    /** Data-quality statistics of a price (and optional volume) field over [start, end). */
    health(start: Timestamp | null | undefined, end: Timestamp | null | undefined, price?: FieldRef, volume?: FieldRef, gapThreshold?: Timestamp, outlierThreshold?: number): Health;
    health(start: Timestamp | null | undefined, end: Timestamp | null | undefined, options: HealthOptions): Health;
    /** Evaluate directional decisions against a price field. */
    evaluate(decisions: Decision[], options?: EvaluateOptions): Evaluation;
}

export interface DBInstance extends IndicatorMethods, StorageMethods {
    append(data: Record<string, number | bigint | boolean>): void;
    flush(): void;
    load(): Record<string, number | bigint | boolean>[];
    query(start: bigint, end: bigint, filters?: Record<string, number | bigint | boolean> | any[]): Record<string, number | bigint | boolean>[];
    getStats(start: bigint, end: bigint, field_index: number | string, compute_percentiles?: boolean): { min: number, max: number, sum: number, count: bigint, mean: number, p50?: number, p90?: number, p95?: number, p99?: number };
    getLatest(field_index: number | string): { value: number, timestamp: bigint };
    /**
     * Indicators over this database (series A) aligned with `other` (series B, whose close is
     * the second input of series2 / ratio / ratio_zscore / rel_strength / correl / beta).
     * `other` must be another database opened with dbInit.
     */
    pairIndicators(other: DBInstance, specs: (IndicatorSpec | string | number)[], options?: PairIndicatorOptions): IndicatorResult;
    close(): void;
    drop(): void;
}

type Promisify<T> = {
    [K in keyof T]: T[K] extends (...args: infer A) => infer R ? (...args: A) => Promise<R> : T[K];
};

export interface AsyncDBInstance extends Promisify<IndicatorMethods>, AsyncStorageMethods {
    append(data: Record<string, number | bigint | boolean>): Promise<{ success: boolean }>;
    appendBatch(data: Record<string, number | bigint | boolean>[]): Promise<{ success: boolean }>;
    flush(): Promise<{ success: boolean }>;
    load(): Promise<Record<string, number | bigint | boolean>[]>;
    query(start: bigint, end: bigint, filters?: Record<string, number | bigint | boolean> | any[]): Promise<Record<string, number | bigint | boolean>[]>;
    getStats(start: bigint, end: bigint, field_index: number | string): Promise<{ min: number, max: number, sum: number, count: bigint, mean: number }>;
    getLatest(field_index: number | string): Promise<{ value: number, timestamp: bigint }>;
    /** Pair indicators; `other` must be hosted by the same worker (see openAsync). */
    pairIndicators(other: AsyncDBInstance, specs: (IndicatorSpec | string | number)[], options?: PairIndicatorOptions): Promise<IndicatorResult>;
    /**
     * Open another database on this instance's worker thread, e.g. the second leg of a pair.
     * The worker stays alive until every database opened on it is closed or dropped.
     */
    openAsync(ticker: string, path: string, schema: FieldDef[], config?: DBConfig): Promise<AsyncDBInstance>;
    /** Open a lock-free reader on this instance's worker thread (see openReaderAsync). */
    openReaderAsync(ticker: string, path: string, schema: FieldDef[]): Promise<AsyncDBInstance>;
    close(): Promise<void>;
    drop(): Promise<void>;
}

/**
 * Open (or create) a database for writing. Writers hold an exclusive lock: a second writer fails
 * immediately with an Error whose code / message name the engine error ("DatabaseLocked",
 * "SchemaMismatch", "ChecksumMismatch", "LegacyFormatNeedsMigration", ...).
 */
export function dbInit(ticker: string, path: string, schema: FieldDef[], config?: DBConfig): DBInstance;
export function dbInitAsync(ticker: string, path: string, schema: FieldDef[], config?: DBConfig): Promise<AsyncDBInstance>;
/**
 * Attach as a lock-free reader to a database another process writes. Every read sees the
 * writer's committed data (after its flush) and follows compaction / rollover; append, drop
 * and the maintenance methods throw an Error with code "ReadOnly".
 */
export function openReader(ticker: string, path: string, schema: FieldDef[]): DBInstance;
export function openReaderAsync(ticker: string, path: string, schema: FieldDef[]): Promise<AsyncDBInstance>;
/** Bytes reserved by the file header (64). A ring buffer of N records needs max_file_size = headerSize() + N * recordSize. */
export function headerSize(): number;
/** fsync policy numbers: { none: 0, on_close: 1, on_flush: 2, interval: 3 }. */
export const FSYNC: Readonly<{ none: 0, on_close: 1, on_flush: 2, interval: 3 }>;

// --- Indicator registry (no database needed) ---

/** Kind name -> numeric id (e.g. INDICATOR_KINDS.rsi === 20). */
export const INDICATOR_KINDS: Readonly<Record<string, number>>;
/** All indicator kind names (83). */
export function indicatorKinds(): string[];
/** Numeric id of a kind (name is case-insensitive). Throws for unknown kinds. */
export function indicatorKindId(kind: string | number): number;
/** Output names of a kind, e.g. ["macd", "signal", "hist"]. */
export function indicatorOutputs(kind: string | number): string[];
/** true for label kinds whose outputs use future rows (forward_return, triple_barrier). */
export function indicatorIsLookahead(kind: string | number): boolean;
/** Recommended warm-up rows for a spec (after applying defaults). */
export function indicatorWarmup(spec: IndicatorSpec | string | number): number;
