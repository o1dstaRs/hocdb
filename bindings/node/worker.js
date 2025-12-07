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
                db.append(payload);
                result = { success: true };
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
