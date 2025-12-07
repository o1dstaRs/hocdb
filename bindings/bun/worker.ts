import { HOCDB } from "./index";

declare var self: Worker;

let db: HOCDB | null = null;

self.onmessage = (event: MessageEvent) => {
    const { id, type, payload } = event.data;
    try {
        let result;
        switch (type) {
            case 'init':
                db = new HOCDB(payload.ticker, payload.path, payload.schema, payload.config);
                result = { success: true };
                break;
            case 'append':
                if (!db) throw new Error("DB not initialized");
                db.append(payload);
                result = { success: true };
                break;
            case 'query':
                if (!db) throw new Error("DB not initialized");
                result = db.query(payload.start, payload.end, payload.filters);
                break;
            case 'load':
                if (!db) throw new Error("DB not initialized");
                result = db.load();
                break;
            case 'getStats':
                if (!db) throw new Error("DB not initialized");
                result = db.getStats(payload.start, payload.end, payload.field_index);
                break;
            case 'getLatest':
                if (!db) throw new Error("DB not initialized");
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
        self.postMessage({ id, result });
    } catch (error) {
        self.postMessage({ id, error: error instanceof Error ? error.message : String(error) });
    }
};
