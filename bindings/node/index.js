const path = require('path');
const fs = require('fs');

// Try to find the built binary
const buildPath = path.join(__dirname, '..', '..', 'zig-out', 'lib', 'libhocdb.dylib'); // macOS
// Note: On Linux it would be .so, on Windows .dll.
// For a real package, we'd rename it to .node or use node-gyp/cmake-js.
// But for this setup, we'll just load the dylib if Node allows it, or rename it.

// Node.js requires .node extension for native modules usually.
// Let's try to load it. If it fails, we might need to copy/rename.

let bindingPath = buildPath;
if (!fs.existsSync(bindingPath)) {
    // Fallback to local build dir if running from source (Linux)
    bindingPath = path.join(__dirname, '..', '..', 'zig-out', 'lib', 'libhocdb.so');
}

if (!fs.existsSync(bindingPath)) {
    console.error("Could not find hocdb native binding at", bindingPath);
    process.exit(1);
}

// We can use 'process.dlopen' or just require if it has .node extension.
// Since it's .dylib/.so, we might need to symlink it to .node
const nodePath = path.join(__dirname, 'hocdb.node');
try {
    if (fs.existsSync(nodePath)) fs.unlinkSync(nodePath);
    fs.copyFileSync(bindingPath, nodePath);
} catch (e) {
    // Ignore if we can't copy (maybe permission or already exists)
}

const addon = require(nodePath);

module.exports = {
    dbInit: (ticker, path, schema, config) => {
        if (!schema || !Array.isArray(schema)) {
            throw new Error("Schema must be an array of field definitions");
        }

        // Validate schema and calculate record size
        let recordSize = 0;
        const fieldOffsets = {};

        for (const field of schema) {
            if (!field.name || !field.type) throw new Error("Invalid field definition");
            fieldOffsets[field.name] = { offset: recordSize, type: field.type };

            switch (field.type) {
                case 'i64': recordSize += 8; break;
                case 'f64': recordSize += 8; break;
                case 'u64': recordSize += 8; break;
                default: throw new Error(`Unsupported field type: ${field.type}`);
            }
        }

        const db = addon.dbInit(ticker, path, schema, config);

        // Return a wrapper object that handles data packing
        return {
            _db: db,
            _recordSize: recordSize,
            _fieldOffsets: fieldOffsets,

            append: (data) => {
                const buffer = Buffer.allocUnsafe(recordSize);
                for (const [key, value] of Object.entries(data)) {
                    const info = fieldOffsets[key];
                    if (!info) continue; // Ignore extra fields? Or throw?

                    switch (info.type) {
                        case 'i64': buffer.writeBigInt64LE(BigInt(value), info.offset); break;
                        case 'f64': buffer.writeDoubleLE(Number(value), info.offset); break;
                        case 'u64': buffer.writeBigUInt64LE(BigInt(value), info.offset); break;
                    }
                }
                addon.dbAppend(db, buffer);
            },

            load: () => {
                const buffer = addon.dbLoad(db);
                // Parse buffer into array of objects
                const count = buffer.byteLength / recordSize;
                const result = new Array(count);
                const view = new DataView(buffer);

                for (let i = 0; i < count; i++) {
                    const record = {};
                    const base = i * recordSize;
                    for (const [name, info] of Object.entries(fieldOffsets)) {
                        switch (info.type) {
                            case 'i64': record[name] = view.getBigInt64(base + info.offset, true); break;
                            case 'f64': record[name] = view.getFloat64(base + info.offset, true); break;
                            case 'u64': record[name] = view.getBigUint64(base + info.offset, true); break;
                        }
                    }
                    result[i] = record;
                }
                return result;
            },

            query: (start, end) => {
                const buffer = addon.dbQuery(db, BigInt(start), BigInt(end));
                // Parse buffer into array of objects
                const count = buffer.byteLength / recordSize;
                const result = new Array(count);
                const view = new DataView(buffer);

                for (let i = 0; i < count; i++) {
                    const record = {};
                    const base = i * recordSize;
                    for (const [name, info] of Object.entries(fieldOffsets)) {
                        switch (info.type) {
                            case 'i64': record[name] = view.getBigInt64(base + info.offset, true); break;
                            case 'f64': record[name] = view.getFloat64(base + info.offset, true); break;
                            case 'u64': record[name] = view.getBigUint64(base + info.offset, true); break;
                        }
                    }
                    result[i] = record;
                }
                return result;
            },

            close: () => {
                addon.dbClose(db);
            }
        };
    },
};
