import { dlopen, FFIType, suffix, ptr, toArrayBuffer } from "bun:ffi";
import { join } from "path";

// Locate the shared library
const libPath = join(import.meta.dir, "..", "..", "zig-out", "lib", `libhocdb.${suffix}`);

const { symbols } = dlopen(libPath, {
    hocdb_init: {
        args: [FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64],
        returns: FFIType.ptr,
    },
    hocdb_append: {
        args: [FFIType.ptr, FFIType.i64, FFIType.f64, FFIType.f64],
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
});

const encoder = new TextEncoder();

export class HOCDB {
    constructor(ticker, path) {
        const tickerBytes = encoder.encode(ticker);
        const pathBytes = encoder.encode(path);

        this.db = symbols.hocdb_init(
            ptr(tickerBytes),
            tickerBytes.length,
            ptr(pathBytes),
            pathBytes.length
        );

        if (!this.db) {
            throw new Error("Failed to initialize HOCDB");
        }
    }

    append(timestamp, usd, volume) {
        const res = symbols.hocdb_append(this.db, BigInt(timestamp), usd, volume);
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

        if (!dataPtr) {
            throw new Error("Failed to load data");
        }

        const len = Number(lenPtr[0]);
        const byteLen = len * 24; // 24 bytes per record

        return new Float64Array(toArrayBuffer(dataPtr, 0, byteLen));
    }

    close() {
        symbols.hocdb_close(this.db);
        this.db = null;
    }
}
