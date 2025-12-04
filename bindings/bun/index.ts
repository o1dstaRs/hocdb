import { dlopen, FFIType, suffix, ptr, toArrayBuffer } from "bun:ffi";
import { join } from "path";

// Locate the shared library
const libPath = join(import.meta.dir, "..", "..", "zig-out", "lib", `libhocdb.${suffix}`);

const { symbols } = dlopen(libPath, {
    hocdb_init: {
        args: [FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.i64, FFIType.i32, FFIType.i32],
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
    hocdb_close: {
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
    hocdb_free: {
        args: [FFIType.ptr],
        returns: FFIType.void,
    },
});

const encoder = new TextEncoder();

export interface DBConfig {
    max_file_size?: number;
    overwrite_on_full?: boolean;
    flush_on_write?: boolean;
}

export interface FieldDef {
    name: string;
    type: 'i64' | 'f64' | 'u64';
}

export class HOCDB {
    db: any;
    schema: FieldDef[];
    recordSize: number;
    fieldOffsets: Record<string, { offset: number, type: string }>;
    nameBuffers: Uint8Array[];
    // Actually we need to keep the name strings alive during init call.
    // But hocdb_init duplicates them. So we don't need to keep them after init.

    constructor(ticker: string, path: string, schema: FieldDef[], config: any = {}) {
        const tickerBytes = encoder.encode(ticker);
        const pathBytes = encoder.encode(path);

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
            this.fieldOffsets[field.name] = { offset: this.recordSize, type: field.type };

            let typeCode;
            let size;
            switch (field.type) {
                case "i64": typeCode = 1; size = 8; break;
                case "f64": typeCode = 2; size = 8; break;
                case "u64": typeCode = 3; size = 8; break;
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

        this.db = symbols.hocdb_init(
            ptr(tickerBytes),
            tickerBytes.length,
            ptr(pathBytes),
            pathBytes.length,
            ptr(schemaBuffer),
            schema.length,
            maxSize,
            overwrite,
            flush
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
            }
        }

        const res = symbols.hocdb_append(this.db, ptr(buffer), BigInt(this.recordSize));
        if (res !== 0) {
            throw new Error("Append failed");
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

    close() {
        if (this.db) {
            symbols.hocdb_close(this.db);
            this.db = null;
        }
    }
}
