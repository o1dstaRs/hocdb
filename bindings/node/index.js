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
    dbInit: (ticker, dirPath, schema, config) => {
        if (!schema || !Array.isArray(schema)) {
            throw new Error("Schema must be an array of field definitions");
        }

        // Validate schema and calculate record size
        let recordSize = 0;
        const fieldOffsets = {};

        for (const field of schema) {
            if (!field.name || !field.type) throw new Error("Invalid field definition");
            fieldOffsets[field.name] = { offset: recordSize, type: field.type, index: schema.indexOf(field) };

            switch (field.type) {
                case 'i64': recordSize += 8; break;
                case 'f64': recordSize += 8; break;
                case 'u64': recordSize += 8; break;
                case 'bool': recordSize += 1; break;
                default: throw new Error(`Unsupported field type: ${field.type}`);
            }
        }

        const db = addon.dbInit(ticker, dirPath, schema, config);

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
                        case 'bool': buffer.writeUInt8(value ? 1 : 0, info.offset); break;
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
                            case 'bool': record[name] = view.getUint8(base + info.offset) !== 0; break;
                        }
                    }
                    result[i] = record;
                }
                return result;
            },

            query: (start, end, filters = []) => {
                let filterArray = [];
                if (!Array.isArray(filters) && typeof filters === 'object') {
                    // Convert object { key: val } to array
                    for (const [key, value] of Object.entries(filters)) {
                        const info = fieldOffsets[key];
                        if (!info) throw new Error(`Unknown field in filter: ${key}`);
                        filterArray.push({
                            field_index: info.index,
                            value: value,
                            type: info.type
                        });
                    }
                } else if (Array.isArray(filters)) {
                    // Assume already in correct format, but ensure type is present
                    filterArray = filters.map(f => {
                        if (f.type) return f;
                        // If type missing, try to look up by index? Hard if we don't have reverse map.
                        // But if user passes field_index, they should pass type or we need reverse map.
                        // For now assume user passes type if using raw array.
                        return f;
                    });
                }

                const buffer = addon.dbQuery(db, BigInt(start), BigInt(end), filterArray);
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
                            case 'bool': record[name] = view.getUint8(base + info.offset) !== 0; break;
                        }
                    }
                    result[i] = record;
                }
                return result;
            },

            getStats: (start, end, field_index) => {
                return addon.dbGetStats(db, BigInt(start), BigInt(end), Number(field_index));
            },

            getLatest: (field_index) => {
                return addon.dbGetLatest(db, Number(field_index));
            },

            close: () => {
                addon.dbClose(db);
            },

            drop: () => {
                addon.dbDrop(db);
            }
        };
    },

    dbInitAsync: (ticker, dirPath, schema, config) => {
        const { Worker } = require('worker_threads');
        const worker = new Worker(path.join(__dirname, 'worker.js'));

        let msgId = 0;
        const pending = new Map();

        worker.on('message', (msg) => {
            const { id, result, error } = msg;
            if (pending.has(id)) {
                const { resolve, reject } = pending.get(id);
                pending.delete(id);
                if (error) reject(new Error(error));
                else resolve(result);
            }
        });

        worker.on('error', (err) => {
            console.error("Worker error:", err);
        });

        const callWorker = (type, payload) => {
            return new Promise((resolve, reject) => {
                const id = msgId++;
                pending.set(id, { resolve, reject });
                worker.postMessage({ id, type, payload });
            });
        };

        // Initialize DB in worker
        return callWorker('init', { ticker, path: dirPath, schema, config }).then(() => {
            return {
                append: (data) => callWorker('append', data),
                query: (start, end, filters) => callWorker('query', { start, end, filters }),
                load: () => callWorker('load', {}),
                getStats: (start, end, field_index) => callWorker('getStats', { start, end, field_index }),
                getLatest: (field_index) => callWorker('getLatest', { field_index }),
                close: () => callWorker('close', {}).then(() => worker.terminate()),
                drop: () => callWorker('drop', {}).then(() => worker.terminate())
            };
        });
    },
};
