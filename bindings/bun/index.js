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

export class HOCDB {
    constructor(ticker, path, schema, config = {}) {
        const tickerBytes = encoder.encode(ticker);
        const pathBytes = encoder.encode(path);

        // Process schema
        this.schema = schema;
        this.recordSize = 0;
        this.fieldOffsets = {};

        // Prepare schema for C-ABI
        // We need to create an array of CField structs: { name: char*, type: int }
        // CField size is 16 bytes (8 bytes ptr + 4 bytes int + 4 bytes padding) on 64-bit
        const schemaBuffer = new Uint8Array(schema.length * 16);
        const schemaView = new DataView(schemaBuffer.buffer);

        // Keep references to name buffers to prevent GC
        this.nameBuffers = [];

        for (let i = 0; i < schema.length; i++) {
            const field = schema[i];
            this.fieldOffsets[field.name] = this.recordSize;

            let typeCode;
            let size;
            switch (field.type) {
                case "int64": typeCode = 1; size = 8; break;
                case "float64": typeCode = 2; size = 8; break;
                case "uint64": typeCode = 3; size = 8; break;
                default: throw new Error(`Unsupported field type: ${field.type}`);
            }
            this.recordSize += size;

            const nameBytes = encoder.encode(field.name + "\0"); // Null-terminated
            this.nameBuffers.push(nameBytes);

            // Write name pointer (u64)
            schemaView.setBigUint64(i * 16, BigInt(ptr(nameBytes)), true);
            // Write type (i32)
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

    append(data) {
        const buffer = new Uint8Array(this.recordSize);
        const view = new DataView(buffer.buffer);

        for (const field of this.schema) {
            const offset = this.fieldOffsets[field.name];
            const value = data[field.name];

            if (value === undefined) {
                throw new Error(`Missing value for field: ${field.name}`);
            }

            switch (field.type) {
                case "int64":
                    view.setBigInt64(offset, BigInt(value), true);
                    break;
                case "float64":
                    view.setFloat64(offset, Number(value), true);
                    break;
                case "uint64":
                    view.setBigUint64(offset, BigInt(value), true);
                    break;
            }
        }

        const res = symbols.hocdb_append(this.db, ptr(buffer), buffer.length);
        if (res !== 0) {
            throw new Error("Failed to append record");
        }
    }

    flush() {
        const res = symbols.hocdb_flush(this.db);
        if (res !== 0) {
            throw new Error("Failed to flush DB");
        }
    }

    load() {
        // We need to pass a pointer to a usize to get the length
        const lenPtr = new BigUint64Array(1);
        const dataPtr = symbols.hocdb_load(this.db, ptr(lenPtr));

        if (!dataPtr && lenPtr[0] > 0n) {
            throw new Error("Load failed");
        }

        const totalBytes = Number(lenPtr[0]);
        const buffer = toArrayBuffer(dataPtr, 0, totalBytes);
        const view = new DataView(buffer);

        const count = totalBytes / this.recordSize;
        const result = [];

        for (let i = 0; i < count; i++) {
            const recordOffset = i * this.recordSize;
            const record = {};

            for (const field of this.schema) {
                const fieldOffset = recordOffset + this.fieldOffsets[field.name];

                switch (field.type) {
                    case "int64":
                        record[field.name] = view.getBigInt64(fieldOffset, true);
                        break;
                    case "float64":
                        record[field.name] = view.getFloat64(fieldOffset, true);
                        break;
                    case "uint64":
                        record[field.name] = view.getBigUint64(fieldOffset, true);
                        break;
                }
            }
            result.push(record);
        }

        // Free the memory allocated by Zig
        symbols.hocdb_free(dataPtr);

        return result;
    }

    close() {
        if (this.db) {
            symbols.hocdb_close(this.db);
            this.db = null;
        }
    }
}
