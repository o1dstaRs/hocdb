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
export function dbInit(ticker: string, path: string): HOCDB;
