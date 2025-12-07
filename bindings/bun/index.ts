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
    hocdb_get_stats: {
        args: [FFIType.ptr, FFIType.i64, FFIType.i64, FFIType.u64, FFIType.ptr],
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
    hocdb_close: {
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
});

const encoder = new TextEncoder();

export interface DBConfig {
    max_file_size?: number;
    overwrite_on_full?: boolean;
    flush_on_write?: boolean;
    auto_increment?: boolean;
}

export interface FieldDef {
    name: string;
    type: 'i64' | 'f64' | 'u64' | 'bool';
}

export interface Filter {
    field_index: number;
    value: number | bigint | string;
}

export class HOCDB {
    db: any;
    schema: FieldDef[];
    recordSize: number;
    fieldOffsets: Record<string, { offset: number, type: string, index: number }>;
    nameBuffers: Uint8Array[];
    ticker: string;
    path: string;

    constructor(ticker: string, path: string, schema: FieldDef[], config: any = {}) {
        this.ticker = ticker;
        this.path = path;
        const tickerBytes = encoder.encode(ticker + "\0");
        const pathBytes = encoder.encode(path + "\0");

        // Process schema
        this.schema = schema;
        this.recordSize = 0;
        this.fieldOffsets = {};

        // Prepare schema for C-ABI
        const schemaBuffer = new Uint8Array(schema.length * 16);
        const schemaView = new DataView(schemaBuffer.buffer);

        this.nameBuffers = [];

        for (let i = 0; i < schema.length; i++) {
            const field = schema[i];
            if (!field) continue;
            this.fieldOffsets[field.name] = { offset: this.recordSize, type: field.type, index: i };

            let typeCode;
            let size;
            switch (field.type) {
                case "i64": typeCode = 1; size = 8; break;
                case "f64": typeCode = 2; size = 8; break;
                case "u64": typeCode = 3; size = 8; break;
                case "bool": typeCode = 6; size = 1; break;
                default: throw new Error(`Unsupported field type: ${field.type}`);
            }
            this.recordSize += size;

            const nameBytes = encoder.encode(field.name + "\0");
            this.nameBuffers.push(nameBytes);

            schemaView.setBigUint64(i * 16, BigInt(ptr(nameBytes)), true);
            schemaView.setInt32(i * 16 + 8, typeCode, true);
        }

        const maxSize = config.max_file_size ? BigInt(config.max_file_size) : 0n;
        const overwrite = config.overwrite_on_full === false ? 0 : 1;
        const flush = config.flush_on_write === true ? 1 : 0;
        const autoInc = config.auto_increment === true ? 1 : 0;

        this.db = symbols.hocdb_init(
            ptr(tickerBytes),
            ptr(pathBytes),
            ptr(schemaBuffer),
            BigInt(schema.length),
            maxSize,
            overwrite,
            flush,
            autoInc
        );

        if (!this.db) {
            throw new Error("Failed to initialize HOCDB");
        }
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
            throw new Error(msg);
        }
    }

    flush() {
        const res = symbols.hocdb_flush(this.db);
        if (res !== 0) {
            throw new Error("Failed to flush DB");
        }
    }

    load(): Record<string, number | bigint>[] {
        const lenPtr = new BigUint64Array(1);
        const dataPtr = symbols.hocdb_load(this.db, ptr(lenPtr));

        if (!dataPtr) {
            throw new Error("Load failed");
        }

        const totalBytes = Number(lenPtr[0]);
        const buffer = toArrayBuffer(dataPtr, 0, totalBytes);
        const view = new DataView(buffer);

        const count = totalBytes / this.recordSize;
        const result = new Array(count);

        for (let i = 0; i < count; i++) {
            const record: Record<string, number | bigint> = {};
            const base = i * this.recordSize;
            for (const [name, info] of Object.entries(this.fieldOffsets)) {
                switch (info.type) {
                    case 'i64': record[name] = view.getBigInt64(base + info.offset, true); break;
                    case 'f64': record[name] = view.getFloat64(base + info.offset, true); break;
                    case 'u64': record[name] = view.getBigUint64(base + info.offset, true); break;
                }
            }
            result[i] = record;
        }

        return result;
    }

    query(startTs: number | bigint, endTs: number | bigint, filters: Filter[] | Record<string, number | bigint | string> = []): Record<string, number | bigint>[] {
        if (!this.db) throw new Error("Database not initialized");

        let filterArray: Filter[] = [];

        if (Array.isArray(filters)) {
            filterArray = filters;
        } else {
            // Convert object to array
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
            // Construct C filter array
            // Struct size: 168 + 8 (bool + padding) = 176 bytes
            const structSize = 176;
            filtersBuf = new Uint8Array(filterArray.length * structSize);
            const view = new DataView(filtersBuf.buffer);

            for (let i = 0; i < filterArray.length; i++) {
                const offset = i * structSize;
                const f = filterArray[i];
                if (!f) continue;

                view.setBigUint64(offset, BigInt(f.field_index), true); // Little endian

                if (typeof f.value === 'bigint') {
                    view.setInt32(offset + 8, 1, true); // Type I64
                    view.setBigInt64(offset + 16, f.value, true);
                } else if (typeof f.value === 'number') {
                    // Could be f64 or i64 (if small int). Assume f64 for number.
                    view.setInt32(offset + 8, 2, true); // Type F64
                    view.setFloat64(offset + 24, f.value, true);
                } else if (typeof f.value === 'string') {
                    view.setInt32(offset + 8, 5, true); // Type String
                    const strBytes = encoder.encode(f.value);
                    // Copy to offset 40
                    for (let j = 0; j < Math.min(strBytes.length, 128); j++) {
                        filtersBuf[offset + 40 + j] = strBytes[j];
                    }
                } else if (typeof f.value === 'boolean') {
                    view.setInt32(offset + 8, 6, true); // Type Bool
                    view.setUint8(offset + 168, f.value ? 1 : 0); // val_bool is at end of struct, check offset!
                }
            }
            filtersPtr = ptr(filtersBuf);
        }

        const dataPtr = symbols.hocdb_query(
            this.db,
            BigInt(startTs),
            BigInt(endTs),
            filtersPtr ?? 0, // Pass 0 (null pointer) if filtersPtr is null
            BigInt(filterArray.length),
            ptr(lenPtr)
        );

        if (!dataPtr && lenPtr[0] > 0n) {
            throw new Error("Query failed");
        }

        if (lenPtr[0] === 0n) return [];

        const totalBytes = Number(lenPtr[0]);
        const buffer = toArrayBuffer(dataPtr, 0, totalBytes);
        const view = new DataView(buffer);

        const count = totalBytes / this.recordSize;
        const result = new Array(count);

        for (let i = 0; i < count; i++) {
            const record: Record<string, number | bigint> = {};
            const base = i * this.recordSize;
            for (const [name, info] of Object.entries(this.fieldOffsets)) {
                switch (info.type) {
                    case 'i64': record[name] = view.getBigInt64(base + info.offset, true); break;
                    case 'f64': record[name] = view.getFloat64(base + info.offset, true); break;
                    case 'u64': record[name] = view.getBigUint64(base + info.offset, true); break;
                }
            }
            result[i] = record;
        }

        symbols.hocdb_free(dataPtr);

        return result;
    }

    getStats(start: bigint, end: bigint, fieldIndex: number): { min: number, max: number, sum: number, count: bigint, mean: number } {
        // Struct layout: min(f64), max(f64), sum(f64), count(u64), mean(f64)
        // Size: 8 + 8 + 8 + 8 + 8 = 40 bytes
        const statsBuffer = new Uint8Array(40);
        const res = symbols.hocdb_get_stats(this.db, start, end, BigInt(fieldIndex), ptr(statsBuffer));

        if (res !== 0) {
            throw new Error("getStats failed");
        }

        const view = new DataView(statsBuffer.buffer);
        return {
            min: view.getFloat64(0, true),
            max: view.getFloat64(8, true),
            sum: view.getFloat64(16, true),
            count: view.getBigUint64(24, true),
            mean: view.getFloat64(32, true)
        };
    }

    getLatest(fieldIndex: number): { value: number, timestamp: bigint } {
        const valPtr = new Float64Array(1);
        const tsPtr = new BigInt64Array(1);

        const res = symbols.hocdb_get_latest(this.db, BigInt(fieldIndex), ptr(valPtr), ptr(tsPtr));

        if (res !== 0) {
            throw new Error("getLatest failed");
        }

        return {
            value: valPtr[0]!,
            timestamp: tsPtr[0]!
        };
    }

    close() {
        if (this.db) {
            symbols.hocdb_close(this.db);
            this.db = null;
        }
    }

    drop() {
        if (this.db) {
            symbols.hocdb_drop(this.db);
            this.db = null;
        }
    }

    static async initAsync(ticker: string, path: string, schema: FieldDef[], config: any = {}) {
        const workerURL = new URL("worker.ts", import.meta.url).href;
        const worker = new Worker(workerURL);

        let msgId = 0;
        const pending = new Map();

        worker.onmessage = (event) => {
            const { id, result, error } = event.data;
            if (pending.has(id)) {
                const { resolve, reject } = pending.get(id);
                pending.delete(id);
                if (error) reject(new Error(error));
                else resolve(result);
            }
        };

        worker.onerror = (err) => {
            console.error("Worker error:", err);
        };

        const callWorker = (type: string, payload: any) => {
            return new Promise((resolve, reject) => {
                const id = msgId++;
                pending.set(id, { resolve, reject });
                worker.postMessage({ id, type, payload });
            });
        };

        // Initialize DB in worker
        await callWorker('init', { ticker, path, schema, config });

        return {
            append: (data: any) => callWorker('append', data),
            query: (start: bigint | number, end: bigint | number, filters: any) => callWorker('query', { start, end, filters }),
            load: () => callWorker('load', {}),
            getStats: (start: bigint | number, end: bigint | number, field_index: number) => callWorker('getStats', { start, end, field_index }),
            getLatest: (field_index: number) => callWorker('getLatest', { field_index }),
            close: async () => {
                await callWorker('close', {});
                worker.terminate();
            },
            drop: async () => {
                await callWorker('drop', {});
                worker.terminate();
            }
        };
    }
}
