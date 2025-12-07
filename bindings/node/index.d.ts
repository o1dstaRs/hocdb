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
export interface DBConfig {
    max_file_size?: number; // Default: 2GB
    overwrite_on_full?: boolean; // Default: true
    flush_on_write?: boolean;
    auto_increment?: boolean;
}

export interface FieldDef {
    name: string;
    type: 'i64' | 'f64' | 'u64';
}

export interface DBInstance {
    append(data: Record<string, number | bigint>): void;
    load(): Record<string, number | bigint>[];
    query(start: bigint, end: bigint, filters?: Record<string, number | bigint> | any[]): Record<string, number | bigint>[];
    getStats(start: bigint, end: bigint, field_index: number): { min: number, max: number, sum: number, count: bigint, mean: number };
    getLatest(field_index: number): { value: number, timestamp: bigint };
    close(): void;
}

export function dbInit(ticker: string, path: string, schema: FieldDef[], config?: DBConfig): DBInstance;
