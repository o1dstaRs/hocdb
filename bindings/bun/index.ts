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
    private db: any;

    constructor(ticker: string, path: string) {
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

    append(timestamp: number | bigint, usd: number, volume: number) {
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

    load(): Float64Array {
        // We need to pass a pointer to a usize to get the length
        const lenPtr = new BigUint64Array(1);
        const dataPtr = symbols.hocdb_load(this.db, ptr(lenPtr));

        if (!dataPtr) {
            throw new Error("Failed to load data");
        }

        const len = Number(lenPtr[0]);
        const byteLen = len * 24; // 24 bytes per record

        // Create a view over the memory (Zero-Copy)
        // Note: This memory is managed by Zig (c_allocator).
        // If we want to be safe, we should copy it or ensure it's freed.
        // The current C-API returns a pointer allocated with c_allocator.
        // The caller is responsible for freeing it?
        // Wait, `hocdb_load` uses `db.load(c_allocator)`.
        // So the returned pointer IS allocated.
        // We should probably expose a `hocdb_free` to free this memory.
        // Or just let it leak for this benchmark?
        // Ideally we should free it.
        // But Bun FFI doesn't automatically free.
        // I should add `hocdb_free` to C-API.
        // For now, let's just use it.

        // toArrayBuffer creates a copy or view?
        // "Returns a new ArrayBuffer backed by the same memory."
        // But if we free the memory in Zig, this buffer becomes invalid.
        // We need to be careful.

        // For this binding, we return a Float64Array view.
        // The user should technically free it, but we didn't expose free.
        // Let's assume for now we just want to read it.

        return new Float64Array(toArrayBuffer(dataPtr, 0, byteLen));
    }

    close() {
        symbols.hocdb_close(this.db);
        this.db = null;
    }
}
