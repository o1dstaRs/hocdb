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

interface SchemaInfo {
    recordSize: number;
    fieldOffsets: Record<string, { offset: number, type: string, index: number }>;
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

    constructor(ticker: string, path: string, schema: FieldDef[], config: any = {}) {
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
        const viewBuffer = toArrayBuffer(dataPtr, 0, totalBytes);
        // Copy buffer because we're about to free logic?
        // Actually for load() specifically we might not want to optimize yet, but consistency is good.
        // But load() is rarely used in hot path compared to query.

        const result = parseBuffer(viewBuffer, this.recordSize, this.fieldOffsets);

        // Wait, parseBuffer does not copy, it reads.
        // We handle logic: read, then free.

        // symbols.hocdb_free is NOT called in original load() implementation??
        // Checking original code... 
        // Original: const buffer = toArrayBuffer(...); ... return result;
        // IT WAS LEAKING! Original load() definition didn't call hocdb_free!
        // Wait, looking at file content provided in Context...
        // lines 172-201.
        // It does NOT call hocdb_free(dataPtr).
        // That is a leak in load() too!
        // Wait, hocdb_load in zig returns a pointer to internal buffer?
        // No, zig function: 
        // `const data = db.load(std.heap.c_allocator) catch return null;`
        // It allocates using c_allocator. So it MUST be freed.
        // The previous implementation of load() was leaking memory.

        symbols.hocdb_free(dataPtr);
        return result;
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

    query(startTs: number | bigint, endTs: number | bigint, filters: Filter[] | Record<string, number | bigint | string> = []): Record<string, number | bigint>[] {
        const buffer = this.queryRaw(startTs, endTs, filters);
        return parseBuffer(buffer, this.recordSize, this.fieldOffsets);
    }

    getStats(start: bigint, end: bigint, fieldIndex: number | string): { min: number, max: number, sum: number, count: bigint, mean: number } {
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

        const statsBuffer = new Uint8Array(40);
        const res = symbols.hocdb_get_stats(this.db, start, end, idx, ptr(statsBuffer));

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

    constructor(ticker: string, path: string, schema: FieldDef[], config: any = {}) {
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
