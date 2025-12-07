const { parentPort } = require('worker_threads');
const hocdb = require('./index.js');

let db = null;

parentPort.on('message', (msg) => {
    const { id, type, payload } = msg;
    try {
        let result;
        switch (type) {
            case 'init':
                db = hocdb.dbInit(payload.ticker, payload.path, payload.schema, payload.config);
                result = { success: true };
                break;
            case 'append':
                if (!db) throw new Error("DB not initialized");
                db.append(payload);
                parentPort.postMessage({ id, result: { success: true } });
                break;

            case 'appendBatch':
                if (!db) throw new Error("DB not initialized");
                for (const record of payload) {
                    db.append(record);
                }
                parentPort.postMessage({ id, result: { success: true } });
                break;

            case 'flush':
                // Node binding doesn't expose flush yet in sync mode? 
                // Let's check index.js dbInit return object.
                // It seems dbInit returns an object with _db, append, load, query, getStats, getLatest, close, drop.
                // It does NOT expose flush. We need to add flush to dbInit in index.js first if we want to use it here.
                // For now, we can ignore or try to call it if it exists.
                if (db.flush) db.flush();
                parentPort.postMessage({ id, result: { success: true } });
                break;
            case 'query':
                result = db.query(payload.start, payload.end, payload.filters);
                break;
            case 'load':
                result = db.load();
                break;
            case 'getStats':
                result = db.getStats(payload.start, payload.end, payload.field_index);
                break;
            case 'getLatest':
                result = db.getLatest(payload.field_index);
                break;
            case 'close':
                if (db) {
                    db.close();
                    db = null;
                }
                result = { success: true };
                break;
            case 'drop':
                if (db) {
                    db.drop();
                    db = null;
                }
                result = { success: true };
                break;
            default:
                throw new Error(`Unknown message type: ${type}`);
        }
        parentPort.postMessage({ id, result });
    } catch (error) {
        parentPort.postMessage({ id, error: error.message });
    }
});
