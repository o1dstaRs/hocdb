const { parentPort } = require('worker_threads');
const hocdb = require('./index.js');

// Databases hosted by this worker, keyed by the id the main thread assigned:
// dbInitAsync() opens the first one, adb.openAsync() adds more so that
// pairIndicators() can see both handles on one thread.
const dbs = new Map();

function get(dbId) {
    const db = dbs.get(dbId);
    if (!db) throw new Error("DB not initialized");
    return db;
}

parentPort.on('message', (msg) => {
    const { id, type, payload } = msg;
    const dbId = msg.dbId === undefined ? 0 : msg.dbId;
    try {
        let result;
        switch (type) {
            case 'init':
                if (dbs.has(dbId)) throw new Error(`DB ${dbId} already initialized`);
                dbs.set(dbId, payload.readOnly
                    ? hocdb.openReader(payload.ticker, payload.path, payload.schema)
                    : hocdb.dbInit(payload.ticker, payload.path, payload.schema, payload.config));
                result = { success: true };
                break;
            case 'append':
                get(dbId).append(payload);
                result = { success: true };
                break;
            case 'appendBatch': {
                const db = get(dbId);
                for (const record of payload) {
                    db.append(record);
                }
                result = { success: true };
                break;
            }
            case 'flush':
                get(dbId).flush();
                result = { success: true };
                break;
            case 'query':
                result = get(dbId).query(payload.start, payload.end, payload.filters);
                break;
            case 'load':
                result = get(dbId).load();
                break;
            case 'getStats':
                result = get(dbId).getStats(payload.start, payload.end, payload.field_index);
                break;
            case 'getLatest':
                result = get(dbId).getLatest(payload.field_index);
                break;
            case 'indicators':
                result = get(dbId).indicators(payload.specs, payload.options);
                break;
            case 'pairIndicators':
                result = get(dbId).pairIndicators(get(payload.otherId), payload.specs, payload.options);
                break;
            case 'ohlcv':
                result = get(dbId).ohlcv(payload.start, payload.end, payload.bucket, payload.opts);
                break;
            case 'summary':
                result = get(dbId).summary(payload.start, payload.end, payload.field, payload.periodsPerYear);
                break;
            case 'snapshot':
                result = get(dbId).snapshot(payload.options);
                break;
            case 'snapshotMulti':
                result = get(dbId).snapshotMulti(payload.options);
                break;
            case 'health':
                result = get(dbId).health(payload.start, payload.end, payload.price, payload.volume, payload.gapThreshold, payload.outlierThreshold);
                break;
            case 'evaluate':
                result = get(dbId).evaluate(payload.decisions, payload.options);
                break;
            // --- Durability, readers, maintenance and metrics ---
            case 'sync':
                get(dbId).sync();
                result = { success: true };
                break;
            case 'refresh':
                get(dbId).refresh();
                result = { success: true };
                break;
            case 'verify':
                result = get(dbId).verify();
                break;
            case 'compact':
                get(dbId).compact(payload.minTs);
                result = { success: true };
                break;
            case 'retainLast':
                get(dbId).retainLast(payload.n);
                result = { success: true };
                break;
            case 'rollover':
                result = get(dbId).rollover();
                break;
            case 'metrics':
                result = get(dbId).metrics();
                break;
            case 'metricsReset':
                get(dbId).metricsReset();
                result = { success: true };
                break;
            case 'formatVersion':
                result = get(dbId).formatVersion();
                break;
            case 'isReadOnly':
                result = get(dbId).isReadOnly();
                break;
            case 'close': {
                const db = dbs.get(dbId);
                if (db) {
                    db.close();
                    dbs.delete(dbId);
                }
                result = { success: true };
                break;
            }
            case 'drop': {
                const db = dbs.get(dbId);
                if (db) {
                    db.drop();
                    dbs.delete(dbId);
                }
                result = { success: true };
                break;
            }
            default:
                throw new Error(`Unknown message type: ${type}`);
        }
        parentPort.postMessage({ id, result });
    } catch (error) {
        parentPort.postMessage({ id, error: error.message, code: error.code });
    }
});
