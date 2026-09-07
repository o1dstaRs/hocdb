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

// ---------------------------------------------------------------------------
// Indicators / analytics helpers (shared by the sync wrapper and the worker)
// ---------------------------------------------------------------------------

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

// [{ id, name, outputs: [...], lookahead }] straight from the Zig registry.
const INDICATOR_REGISTRY = addon.indicatorRegistry();
const kindByName = new Map();
const kindById = new Map();
for (const k of INDICATOR_REGISTRY) {
    kindByName.set(k.name, k);
    kindById.set(k.id, k);
}
// Convenience constants: { sma: 1, ema: 2, ..., pivots: 163 }
const INDICATOR_KINDS = Object.freeze(Object.fromEntries(INDICATOR_REGISTRY.map(k => [k.name, k.id])));

// Kind name (case-insensitive) or numeric id -> registry entry.
function resolveKind(kind) {
    if (typeof kind === 'string') {
        const k = kindByName.get(kind.toLowerCase());
        if (!k) throw new Error(`Unknown indicator kind: '${kind}' (see hocdb.indicatorKinds())`);
        return k;
    }
    if (typeof kind === 'number' || typeof kind === 'bigint') {
        const k = kindById.get(Number(kind));
        if (!k) throw new Error(`Unknown indicator kind id: ${kind}`);
        return k;
    }
    throw new Error("Indicator spec needs a 'kind' (name such as 'rsi', or numeric id)");
}

// Field name or index -> index. fieldOffsets is the sync wrapper's {name: {index,...}} map
// (null when no schema is available, in which case only numeric indices are accepted).
function fieldIndex(fieldOffsets, field, what) {
    if (typeof field === 'string') {
        const info = fieldOffsets ? fieldOffsets[field] : undefined;
        if (!info) throw new Error(`Unknown field for ${what}: '${field}'`);
        return info.index;
    }
    if (typeof field === 'number' || typeof field === 'bigint') {
        const idx = Number(field);
        const n = fieldOffsets ? Object.keys(fieldOffsets).length : Infinity;
        if (!Number.isInteger(idx) || idx < 0 || idx >= n) throw new Error(`Invalid field index for ${what}: ${field}`);
        return idx;
    }
    throw new Error(`Invalid field for ${what}: ${field} (expected a field name or index)`);
}

const COLUMN_ROLES = ['open', 'high', 'low', 'close', 'volume', 'bid', 'ask', 'side'];
// Field names that play the volume role when auto-detecting (first match wins).
const VOLUME_NAMES = ['volume', 'size', 'qty'];

function emptyColumns() {
    return { open: -1, high: -1, low: -1, close: -1, volume: -1, bid: -1, ask: -1, side: -1 };
}

// Auto-detected volume field index ("volume", else "size" / "qty" for tick schemas), -1 when none.
function detectVolume(fieldOffsets) {
    for (const name of VOLUME_NAMES) if (fieldOffsets[name]) return fieldOffsets[name].index;
    return -1;
}

// options.columns -> { open, high, low, close, volume, bid, ask, side } field indices (-1 = absent).
// Without `columns` the roles are auto-detected from the schema (fields literally named
// open/high/low/close/volume/bid/ask/side; `price` stands in for a missing close and
// `size` / `qty` for a missing volume). With `columns`, only the roles given are used.
function resolveColumns(fieldOffsets, columns, what = 'options.columns') {
    const cols = emptyColumns();
    if (columns === undefined || columns === null) {
        for (const role of COLUMN_ROLES) {
            if (fieldOffsets[role]) cols[role] = fieldOffsets[role].index;
        }
        if (cols.close < 0 && fieldOffsets.price) cols.close = fieldOffsets.price.index;
        if (cols.volume < 0) cols.volume = detectVolume(fieldOffsets);
        if (cols.close < 0) {
            throw new Error(`No close column: the schema has no field named 'close' or 'price'. Pass ${what} = { close: '<field>' }`);
        }
        return cols;
    }
    if (typeof columns !== 'object') throw new Error(`${what} must be an object { open, high, low, close, volume, bid, ask, side }`);
    for (const role of COLUMN_ROLES) {
        const f = columns[role];
        if (f === undefined || f === null) continue;
        cols[role] = fieldIndex(fieldOffsets, f, `${what}.${role}`);
    }
    if (cols.close < 0) throw new Error(`No close column: ${what} must include 'close'`);
    return cols;
}

// Explicit price field, else "close", else "price".
function resolvePriceField(fieldOffsets, price, what) {
    if (price !== undefined && price !== null) return fieldIndex(fieldOffsets, price, what);
    if (fieldOffsets.close) return fieldOffsets.close.index;
    if (fieldOffsets.price) return fieldOffsets.price.index;
    throw new Error(`No price column: the schema has no field named 'close' or 'price'. Pass { ${what}: '<field>' }`);
}

// Explicit volume field, else auto-detected (volume / size / qty), else -1.
function resolveVolumeField(fieldOffsets, volume) {
    if (volume !== undefined && volume !== null) return fieldIndex(fieldOffsets, volume, 'volume');
    return detectVolume(fieldOffsets);
}

function intParam(value, name) {
    if (value === undefined || value === null) return 0;
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0) throw new Error(`Indicator spec '${name}' must be a non-negative integer, got ${value}`);
    return n;
}

function numParam(value, name) {
    if (value === undefined || value === null) return 0;
    const x = Number(value);
    if (Number.isNaN(x)) throw new Error(`Indicator spec '${name}' must be a number, got ${value}`);
    return x;
}

// {kind, period, ..., field, field2, label} -> {native spec for the addon, resolved kind}
function normalizeSpec(fieldOffsets, spec) {
    const s = (typeof spec === 'string' || typeof spec === 'number') ? { kind: spec } : spec;
    if (!s || typeof s !== 'object') throw new Error(`Invalid indicator spec: ${spec}`);
    const kind = resolveKind(s.kind);
    const native = {
        kind: kind.id,
        period: intParam(s.period, 'period'),
        period2: intParam(s.period2, 'period2'),
        period3: intParam(s.period3, 'period3'),
        period4: intParam(s.period4, 'period4'),
        param: numParam(s.param, 'param'),
        param2: numParam(s.param2, 'param2'),
        field_index: (s.field === undefined || s.field === null) ? -1 : fieldIndex(fieldOffsets, s.field, 'field'),
        field_index2: (s.field2 === undefined || s.field2 === null) ? -1 : fieldIndex(fieldOffsets, s.field2, 'field2'),
    };
    return { native, kind, label: s.label };
}

// Names that the result object uses for its own bookkeeping.
const RESULT_RESERVED = new Set(['timestamps', 'values', 'n_rows', 'n_outputs', 'names']);

// Naming rule: label = spec.label, else kind name plus "_<period>" when a period is given.
// Single-output kinds use the label as the column name, multi-output kinds use `${label}_${output}`
// except for the output named like the kind itself, which keeps the bare label (macd, macd_signal, macd_hist).
function buildSpecs(fieldOffsets, specs) {
    if (!Array.isArray(specs) || specs.length === 0) throw new Error("specs must be a non-empty array of indicator specs");
    const native = [];
    const names = [];
    for (const spec of specs) {
        const { native: n, kind, label } = normalizeSpec(fieldOffsets, spec);
        native.push(n);
        const base = (label !== undefined && label !== null) ? String(label) : (n.period > 0 ? `${kind.name}_${n.period}` : kind.name);
        if (kind.outputs.length === 1) names.push(base);
        else for (const out of kind.outputs) names.push(out === kind.name ? base : `${base}_${out}`);
    }
    const seen = new Set();
    for (const name of names) {
        if (RESULT_RESERVED.has(name)) throw new Error(`Indicator column name '${name}' is reserved; use a different label`);
        if (seen.has(name)) throw new Error(`Duplicate indicator column name '${name}'; use 'label' to disambiguate`);
        seen.add(name);
    }
    return { native, names };
}

function parseLookback(lookback) {
    if (lookback === undefined || lookback === null || lookback === 'auto') return -1;
    const n = Number(lookback);
    if (!Number.isInteger(n) || n < 0) throw new Error(`options.lookback must be 'auto' or a non-negative integer, got ${lookback}`);
    return n;
}

function parseBucket(bucket) {
    if (bucket === undefined || bucket === null) return 0n;
    let b;
    try { b = BigInt(bucket); } catch (e) { throw new Error(`bucket must be an integer, got ${bucket}`); }
    if (b < 0n) throw new Error(`bucket must be >= 0, got ${bucket}`);
    return b;
}

function toTimestamp(value, fallback, name) {
    if (value === undefined || value === null) return fallback;
    try { return BigInt(value); } catch (e) { throw new Error(`${name} must be an integer timestamp, got ${value}`); }
}

// options.tail, or options.start/end -> { tail } | { start, end }
function parseWindow(options) {
    if (options.tail !== undefined && options.tail !== null) {
        if (options.start !== undefined || options.end !== undefined) throw new Error("Use either options.tail or options.start/end, not both");
        const n = Number(options.tail);
        if (!Number.isInteger(n) || n < 0) throw new Error(`options.tail must be a non-negative integer, got ${options.tail}`);
        return { tail: n };
    }
    return {
        start: toTimestamp(options.start, INT64_MIN, 'options.start'),
        end: toTimestamp(options.end, INT64_MAX, 'options.end'),
    };
}

// Split the planar values buffer into one named Float64Array view per output.
function shapeIndicatorResult(raw, names) {
    const n = raw.n_rows;
    if (raw.n_outputs !== names.length) throw new Error(`Internal error: expected ${names.length} outputs, got ${raw.n_outputs}`);
    const result = { timestamps: raw.timestamps, n_rows: n, n_outputs: raw.n_outputs, names, values: raw.values };
    for (let k = 0; k < names.length; k++) {
        result[names[k]] = raw.values.subarray(k * n, (k + 1) * n);
    }
    return result;
}

function runIndicators(dbHandle, fieldOffsets, specs, options = {}) {
    if (options === null || typeof options !== 'object') throw new Error("options must be an object");
    const cols = resolveColumns(fieldOffsets, options.columns);
    const { native, names } = buildSpecs(fieldOffsets, specs);
    const lookback = parseLookback(options.lookback);
    const bucket = parseBucket(options.bucket);
    const w = parseWindow(options);
    const raw = (w.tail !== undefined)
        ? addon.dbIndicatorsTail(dbHandle, w.tail, cols, native, lookback, bucket)
        : addon.dbIndicators(dbHandle, w.start, w.end, cols, native, lookback, bucket);
    return shapeIndicatorResult(raw, names);
}

// Indicators over this database (series A) aligned with `other` (series B). `columns` selects
// the roles of this database, `columns2` (alias `otherColumns`) those of the other one; both
// are auto-detected from the respective schema when omitted.
function runPairIndicators(dbHandle, fieldOffsets, otherHandle, otherFieldOffsets, specs, options = {}) {
    if (options === null || typeof options !== 'object') throw new Error("options must be an object");
    const cols = resolveColumns(fieldOffsets, options.columns);
    const columns2 = (options.columns2 !== undefined && options.columns2 !== null) ? options.columns2 : options.otherColumns;
    const cols2 = resolveColumns(otherFieldOffsets, columns2, 'options.columns2');
    const { native, names } = buildSpecs(fieldOffsets, specs);
    const lookback = parseLookback(options.lookback);
    const bucket = parseBucket(options.bucket);
    const w = parseWindow(options);
    const raw = (w.tail !== undefined)
        ? addon.dbPairIndicatorsTail(dbHandle, otherHandle, w.tail, cols, cols2, native, lookback, bucket)
        : addon.dbPairIndicators(dbHandle, otherHandle, w.start, w.end, cols, cols2, native, lookback, bucket);
    return shapeIndicatorResult(raw, names);
}

// A calendar given as an id or a name ("nyse"); 0 / "" / null means "no calendar".
function resolveCalendar(calendar, what) {
    if (calendar === null || calendar === undefined) return 0;
    if (typeof calendar === 'number' || typeof calendar === 'bigint') return Number(calendar);
    if (typeof calendar === 'string') {
        const id = addon.calendarId(calendar);
        if (id === 0) throw new Error(`${what}: unknown calendar '${calendar}' (UnknownCalendar); built-in names: crypto, fx, nyse, nasdaq, lse, cme, custom ones come from calendarDefine()`);
        return id;
    }
    throw new Error(`${what}: calendar must be an id (number) or a name (string)`);
}

// Split the backtest options object into the pieces the addon expects.
function backtestOptions(options, what) {
    if (options === null || typeof options !== 'object') {
        throw new Error(`${what} options must be an object { bucket, columns, params, equity, position, cash, pnl, drawdown, maxTrades }`);
    }
    const { bucket = 0, columns = null, params = null, ...rest } = options;
    const outputs = {};
    for (const k of ['equity', 'position', 'cash', 'pnl', 'drawdown', 'maxTrades', 'max_trades']) {
        if (rest[k] !== undefined) outputs[k] = rest[k];
    }
    return { bucket, columns, params: params ?? {}, outputs };
}

// Every entry of `dbs` must be a database opened on this thread (dbInit / openReader).
// Returns the raw handles and the column roles resolved against the first database's schema.
function universeHandles(dbs, columns, what) {
    if (!Array.isArray(dbs) || dbs.length === 0) throw new Error(`${what}: dbs must be a non-empty array of databases opened with dbInit / openReader`);
    const handles = dbs.map((d, i) => {
        if (!d || typeof d !== 'object' || !d._db || !d._fieldOffsets) throw new Error(`${what}: dbs[${i}] is not a database opened with dbInit / openReader`);
        return d._db;
    });
    return { handles, cols: resolveColumns(dbs[0]._fieldOffsets, columns) };
}

// `other` of the sync pairIndicators(): a database opened with dbInit on this thread.
function syncOther(other) {
    if (!other || typeof other !== 'object' || !other._db || !other._fieldOffsets) {
        throw new Error("pairIndicators: `other` must be a database opened with dbInit (for the async API open it with adb.openAsync(...) so that both share one worker)");
    }
    return other;
}

function runOhlcv(dbHandle, fieldOffsets, start, end, bucket, opts = {}) {
    if (opts === null || typeof opts !== 'object') throw new Error("ohlcv options must be an object { price, volume, side }");
    const price = resolvePriceField(fieldOffsets, opts.price, 'price');
    const volume = resolveVolumeField(fieldOffsets, opts.volume);
    const side = (opts.side === undefined || opts.side === null) ? -1 : fieldIndex(fieldOffsets, opts.side, 'side');
    const b = parseBucket(bucket);
    if (b <= 0n) throw new Error(`ohlcv bucket must be > 0, got ${bucket}`);
    return addon.dbOhlcv(dbHandle, toTimestamp(start, INT64_MIN, 'start'), toTimestamp(end, INT64_MAX, 'end'), price, volume, side, b);
}

function runSummary(dbHandle, fieldOffsets, start, end, field, periodsPerYear = 0) {
    const idx = fieldIndex(fieldOffsets, field, 'field');
    return addon.dbSummary(dbHandle, toTimestamp(start, INT64_MIN, 'start'), toTimestamp(end, INT64_MAX, 'end'), idx, Number(periodsPerYear));
}

function runSnapshot(dbHandle, fieldOffsets, options = {}) {
    if (options === null || typeof options !== 'object') throw new Error("snapshot options must be an object");
    const cols = resolveColumns(fieldOffsets, options.columns);
    const bars = intParam(options.bars, 'bars');
    const bucket = parseBucket(options.bucket);
    const ppy = numParam(options.periodsPerYear, 'periodsPerYear');
    return addon.dbSnapshot(dbHandle, cols, bars, bucket, ppy);
}

// snapshotMulti({ buckets, periodsPerYear, bars, columns }) -> [snapshot per bucket, in bucket order]
function runSnapshotMulti(dbHandle, fieldOffsets, options = {}) {
    if (options === null || typeof options !== 'object') throw new Error("snapshotMulti options must be an object { buckets, periodsPerYear, bars, columns }");
    if (!Array.isArray(options.buckets) || options.buckets.length === 0) throw new Error("snapshotMulti: options.buckets must be a non-empty array of bar sizes");
    const buckets = options.buckets.map((b, i) => {
        const x = parseBucket(b);
        if (x <= 0n) throw new Error(`snapshotMulti: buckets[${i}] must be > 0, got ${b}`);
        return x;
    });
    const p = options.periodsPerYear;
    let ppy;
    if (p === undefined || p === null) ppy = buckets.map(() => 0);
    else if (Array.isArray(p)) {
        if (p.length !== buckets.length) throw new Error(`snapshotMulti: periodsPerYear must have one entry per bucket (${buckets.length}), got ${p.length}`);
        ppy = p.map((x, i) => numParam(x, `periodsPerYear[${i}]`));
    } else ppy = buckets.map(() => numParam(p, 'periodsPerYear')); // one value for every bucket
    const cols = resolveColumns(fieldOffsets, options.columns);
    const bars = intParam(options.bars, 'bars');
    return addon.dbSnapshotMulti(dbHandle, cols, bars, buckets, ppy);
}

// health(start, end, price?, volume?, gapThreshold = 0, outlierThreshold = 0), or
// health(start, end, { price, volume, gapThreshold, outlierThreshold })
function runHealth(dbHandle, fieldOffsets, start, end, price, volume, gapThreshold, outlierThreshold) {
    if (price !== null && typeof price === 'object') {
        const o = price;
        return runHealth(dbHandle, fieldOffsets, start, end, o.price, o.volume, o.gapThreshold, o.outlierThreshold);
    }
    const p = resolvePriceField(fieldOffsets, price, 'price');
    const v = resolveVolumeField(fieldOffsets, volume);
    const gap = toTimestamp(gapThreshold, 0n, 'gapThreshold');
    if (gap < 0n) throw new Error(`gapThreshold must be >= 0, got ${gapThreshold}`);
    const outlier = numParam(outlierThreshold, 'outlierThreshold');
    if (outlier < 0) throw new Error(`outlierThreshold must be >= 0, got ${outlierThreshold}`);
    return addon.dbHealth(dbHandle, toTimestamp(start, INT64_MIN, 'start'), toTimestamp(end, INT64_MAX, 'end'), p, v, gap, outlier);
}

// [{ timestamp, direction, size = 1, horizon = 0 }] -> validated decisions for the addon
function normalizeDecisions(decisions) {
    if (!Array.isArray(decisions)) throw new Error("evaluate: decisions must be an array of { timestamp, direction, size, horizon } objects");
    return decisions.map((d, i) => {
        if (!d || typeof d !== 'object') throw new Error(`evaluate: decisions[${i}] must be an object { timestamp, direction, size, horizon }`);
        if (d.timestamp === undefined || d.timestamp === null) throw new Error(`evaluate: decisions[${i}] needs a timestamp`);
        if (d.direction === undefined || d.direction === null) throw new Error(`evaluate: decisions[${i}] needs a direction (+1 long, -1 short, 0 flat)`);
        return {
            timestamp: toTimestamp(d.timestamp, 0n, `decisions[${i}].timestamp`),
            direction: numParam(d.direction, `decisions[${i}].direction`),
            size: (d.size === undefined || d.size === null) ? 1 : numParam(d.size, `decisions[${i}].size`),
            horizon: toTimestamp(d.horizon, 0n, `decisions[${i}].horizon`),
        };
    });
}

// evaluate(decisions, { priceField, defaultHorizon, costBps }) -> Evaluation + entry / exit / net_return arrays
function runEvaluate(dbHandle, fieldOffsets, decisions, options = {}) {
    if (options === null || typeof options !== 'object') throw new Error("evaluate options must be an object { priceField, defaultHorizon, costBps }");
    const priceField = (options.priceField !== undefined && options.priceField !== null) ? options.priceField : options.price;
    const price = resolvePriceField(fieldOffsets, priceField, 'priceField');
    const defaultHorizon = toTimestamp(options.defaultHorizon, 0n, 'options.defaultHorizon');
    if (defaultHorizon < 0n) throw new Error(`options.defaultHorizon must be >= 0, got ${options.defaultHorizon}`);
    const costBps = numParam(options.costBps, 'costBps');
    return addon.dbEvaluate(dbHandle, price, normalizeDecisions(decisions), defaultHorizon, costBps);
}

// ---------------------------------------------------------------------------
// Database instances
// ---------------------------------------------------------------------------

// fsync policies for config.fsync (the names are accepted as well).
const FSYNC = Object.freeze({ none: 0, on_close: 1, on_flush: 2, interval: 3 });

// Built-in trading calendar ids (calendarId(name) also resolves custom ones).
const CALENDAR = Object.freeze({ NONE: 0, CRYPTO: 1, FX: 2, NYSE: 3, NASDAQ: 4, LSE: 5, CME: 6 });

// Validate a schema and compute the record layout used to pack / unpack records.
function schemaLayout(schema) {
    if (!schema || !Array.isArray(schema)) {
        throw new Error("Schema must be an array of field definitions");
    }
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
    return { recordSize, fieldOffsets };
}

function toCount(value, what) {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0) throw new Error(`${what} must be a non-negative integer, got ${value}`);
    return n;
}

// Wrapper around a native handle: a writer (dbInit) or a lock-free reader (openReader).
// Readers support every read method; writes and maintenance throw a ReadOnly error.
function makeSyncInstance(db, recordSize, fieldOffsets) {
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

        flush: () => {
            addon.dbFlush(db);
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
            let index = field_index;
            if (typeof field_index === 'string') {
                const info = fieldOffsets[field_index];
                if (!info) throw new Error(`Unknown field: ${field_index}`);
                index = info.index;
            }
            return addon.dbGetStats(db, BigInt(start), BigInt(end), Number(index));
        },

        getLatest: (field_index) => {
            let index = field_index;
            if (typeof field_index === 'string') {
                const info = fieldOffsets[field_index];
                if (!info) throw new Error(`Unknown field: ${field_index}`);
                index = info.index;
            }
            return addon.dbGetLatest(db, Number(index));
        },

        // --- Indicators / analytics ---
        indicators: (specs, options) => runIndicators(db, fieldOffsets, specs, options),
        indicatorsTail: (n, specs, options) => runIndicators(db, fieldOffsets, specs, { ...(options || {}), tail: n }),
        pairIndicators: (other, specs, options) => {
            const o = syncOther(other);
            return runPairIndicators(db, fieldOffsets, o._db, o._fieldOffsets, specs, options);
        },
        ohlcv: (start, end, bucket, opts) => runOhlcv(db, fieldOffsets, start, end, bucket, opts),
        summary: (start, end, field, periodsPerYear) => runSummary(db, fieldOffsets, start, end, field, periodsPerYear),
        snapshot: (options) => runSnapshot(db, fieldOffsets, options),
        snapshotMulti: (options) => runSnapshotMulti(db, fieldOffsets, options),
        health: (start, end, price, volume, gapThreshold, outlierThreshold) => runHealth(db, fieldOffsets, start, end, price, volume, gapThreshold, outlierThreshold),
        evaluate: (decisions, options) => runEvaluate(db, fieldOffsets, decisions, options),

        // --- Durability, readers, maintenance and metrics ---
        // Flush and fsync now, whatever the fsync policy (writers only).
        sync: () => { addon.dbSync(db); },
        // Readers: pick up the writer's latest commit (every read does this on its own); writers: no-op.
        refresh: () => { addon.dbRefresh(db); },
        // true when the CRC32C of the committed data matches; throws ChecksumUnavailable for ring buffers / legacy files.
        verify: () => addon.dbVerify(db),
        // Keep only records with timestamp >= minTs (rewrites the file; readers follow automatically).
        compact: (minTs) => {
            if (minTs === undefined || minTs === null) throw new Error("compact: minTs is required");
            addon.dbCompact(db, toTimestamp(minTs, 0n, 'minTs'));
        },
        // Keep only the last n records.
        retainLast: (n) => { addon.dbRetainLast(db, toCount(n, 'retainLast: n')); },
        // Archive the file as <ticker>.<first_ts>-<last_ts>.bin next to it and continue empty; returns the archive path.
        rollover: () => addon.dbRollover(db),
        // Operational counters: 30 BigInt fields (see Metrics in index.d.ts).
        metrics: () => addon.dbMetrics(db),
        metricsReset: () => { addon.dbMetricsReset(db); },
        // 1 = legacy HOC1 file, 2 = current format.
        formatVersion: () => addon.dbFormatVersion(db),
        // true for handles opened with openReader.
        isReadOnly: () => addon.dbIsReadOnly(db),

        // --- Trading calendar (round 4) ---
        // Attach a trading calendar: an id (see calendarId) or a name ("nyse", "crypto", ...).
        // A writer persists built-in ids in the file header; readers keep it on the handle.
        setCalendar: (calendar) => { addon.dbSetCalendar(db, resolveCalendar(calendar, 'setCalendar')); },
        getCalendar: () => addon.dbGetCalendar(db),
        // Nanoseconds per timestamp unit (1000 = microseconds); persisted by writers.
        setTimestampUnit: (unitNs) => { addon.dbSetTimestampUnit(db, BigInt(unitNs)); },
        getTimestampUnit: () => addon.dbGetTimestampUnit(db),
        // Bars per year for `bucket` timestamp units, from the calendar (0 when unknown).
        periodsPerYear: (bucket) => addon.dbPeriodsPerYear(db, BigInt(bucket)),

        // --- Signal backtester (round 4) ---
        // backtest(target, start, end, { bucket, columns, params, equity, position, cash, pnl, drawdown, maxTrades })
        // target has one entry per row of indicators(start, end, bucket) over the same window.
        backtest: (target, start, end, options = {}) => {
            const o = backtestOptions(options, 'backtest');
            return addon.dbBacktest(db, resolveColumns(fieldOffsets, o.columns), target, toTimestamp(start, INT64_MIN, 'start'),
                toTimestamp(end, INT64_MAX, 'end'), BigInt(o.bucket), o.params, o.outputs);
        },
        // The same over the last target.length bars (bucket > 0) or records.
        backtestTail: (target, options = {}) => {
            const o = backtestOptions(options, 'backtestTail');
            return addon.dbBacktestTail(db, resolveColumns(fieldOffsets, o.columns), target, BigInt(o.bucket), o.params, o.outputs);
        },

        close: () => {
            addon.dbClose(db);
        },

        drop: () => {
            addon.dbDrop(db);
        }
    };
}

// One worker thread hosting one or more databases (dbInitAsync / openReaderAsync open the
// first one, adb.openAsync / adb.openReaderAsync add more, e.g. for pairIndicators).
function createWorkerHost() {
    const { Worker } = require('worker_threads');
    const worker = new Worker(path.join(__dirname, 'worker.js'));

    let msgId = 0;
    const pending = new Map();

    worker.on('message', (msg) => {
        const { id, result, error, code } = msg;
        if (pending.has(id)) {
            const { resolve, reject } = pending.get(id);
            pending.delete(id);
            if (error) {
                const e = new Error(error);
                if (code) e.code = code; // engine error name, e.g. 'DatabaseLocked', 'ReadOnly'
                reject(e);
            }
            else resolve(result);
        }
    });

    worker.on('error', (err) => {
        console.error("Worker error:", err);
    });

    // One worker can host several databases (see openAsync); `dbId` selects one.
    const callWorker = (type, payload, dbId) => {
        return new Promise((resolve, reject) => {
            const id = msgId++;
            pending.set(id, { resolve, reject });
            worker.postMessage({ id, type, dbId, payload });
        });
    };

    let nextDbId = 0;
    const hosted = new Set(); // ids of the databases currently open on this worker
    // The worker is terminated when the last database hosted by it is closed / dropped.
    const release = (dbId) => {
        hosted.delete(dbId);
        if (hosted.size === 0) return worker.terminate();
    };

    const makeInstance = (dbId, dbSchema) => {
        const call = (type, payload) => callWorker(type, payload, dbId);

        // Local field offsets for async resolution
        const fieldOffsets = {};
        for (let i = 0; i < dbSchema.length; i++) {
            fieldOffsets[dbSchema[i].name] = i;
        }

        return {
            _worker: worker,
            _dbId: dbId,
            append: (data) => call('append', data),
            appendBatch: (data) => call('appendBatch', data),
            flush: () => call('flush', {}),
            query: (start, end, filters) => call('query', { start, end, filters }),
            load: () => call('load', {}),
            getStats: (start, end, field_index) => {
                let index = field_index;
                if (typeof field_index === 'string') {
                    index = fieldOffsets[field_index];
                    if (index === undefined) throw new Error(`Unknown field: ${field_index}`);
                }
                return call('getStats', { start, end, field_index: index });
            },
            getLatest: (field_index) => {
                let index = field_index;
                if (typeof field_index === 'string') {
                    index = fieldOffsets[field_index];
                    if (index === undefined) throw new Error(`Unknown field: ${field_index}`);
                }
                return call('getLatest', { field_index: index });
            },
            // Indicators / analytics: field names, kind names and column
            // detection are resolved inside the worker (same code as the sync API).
            indicators: (specs, options) => call('indicators', { specs, options }),
            indicatorsTail: (n, specs, options) => call('indicators', { specs, options: { ...(options || {}), tail: n } }),
            // `other` must live on the same worker thread: open it with openAsync() below.
            pairIndicators: (other, specs, options) => {
                if (!other || typeof other !== 'object' || other._worker !== worker || other._dbId === undefined) {
                    return Promise.reject(new Error("pairIndicators: `other` must be a database hosted by the same worker; open it with adb.openAsync(ticker, path, schema, config)"));
                }
                return call('pairIndicators', { otherId: other._dbId, specs, options });
            },
            ohlcv: (start, end, bucket, opts) => call('ohlcv', { start, end, bucket, opts }),
            summary: (start, end, field, periodsPerYear) => call('summary', { start, end, field, periodsPerYear }),
            snapshot: (options) => call('snapshot', { options }),
            snapshotMulti: (options) => call('snapshotMulti', { options }),
            health: (start, end, price, volume, gapThreshold, outlierThreshold) => call('health', { start, end, price, volume, gapThreshold, outlierThreshold }),
            evaluate: (decisions, options) => call('evaluate', { decisions, options }),
            // --- Durability, readers, maintenance and metrics (same semantics as the sync API) ---
            sync: () => call('sync', {}),
            refresh: () => call('refresh', {}),
            verify: () => call('verify', {}),
            compact: (minTs) => call('compact', { minTs }),
            retainLast: (n) => call('retainLast', { n }),
            rollover: () => call('rollover', {}),
            metrics: () => call('metrics', {}),
            metricsReset: () => call('metricsReset', {}),
            formatVersion: () => call('formatVersion', {}),
            isReadOnly: () => call('isReadOnly', {}),
            // Open another database on this worker (e.g. the second leg of a pair).
            openAsync: (ticker2, dirPath2, schema2, config2) => openDb(ticker2, dirPath2, schema2, config2, false),
            // Open a lock-free reader on this worker.
            openReaderAsync: (ticker2, dirPath2, schema2) => openDb(ticker2, dirPath2, schema2, undefined, true),
            close: () => call('close', {}).then(() => release(dbId)),
            drop: () => call('drop', {}).then(() => release(dbId))
        };
    };

    const openDb = (t, p, s, c, readOnly) => {
        if (!s || !Array.isArray(s)) return Promise.reject(new Error("Schema must be an array of field definitions"));
        const dbId = nextDbId++;
        return callWorker('init', { ticker: t, path: p, schema: s, config: c, readOnly: !!readOnly }, dbId).then(() => {
            hosted.add(dbId);
            return makeInstance(dbId, s);
        }, (err) => {
            if (hosted.size === 0) worker.terminate(); // nothing else lives on this worker
            throw err;
        });
    };

    return { openDb };
}

module.exports = {
    // --- Indicator registry (no DB needed) ---
    INDICATOR_KINDS,
    indicatorKinds: () => INDICATOR_REGISTRY.map(k => k.name),
    indicatorKindId: (kind) => resolveKind(kind).id,
    indicatorOutputs: (kind) => resolveKind(kind).outputs.slice(),
    indicatorIsLookahead: (kind) => resolveKind(kind).lookahead,
    indicatorWarmup: (spec) => addon.indicatorWarmup(normalizeSpec(null, spec).native),

    dbInit: (ticker, dirPath, schema, config) => {
        const { recordSize, fieldOffsets } = schemaLayout(schema);
        return makeSyncInstance(addon.dbInit(ticker, dirPath, schema, config), recordSize, fieldOffsets);
    },

    // Lock-free reader on a database another process (or handle) writes: sees committed
    // data only, follows compaction / rollover; append & co. throw a ReadOnly error.
    openReader: (ticker, dirPath, schema) => {
        const { recordSize, fieldOffsets } = schemaLayout(schema);
        return makeSyncInstance(addon.dbOpenReader(ticker, dirPath, schema), recordSize, fieldOffsets);
    },

    // Bytes reserved by the file header (64): a ring buffer of N records needs max_file_size = headerSize() + N * recordSize.
    headerSize: () => addon.dbHeaderSize(),
    FSYNC,

    // --- Trading calendars (round 4; all times are UTC seconds) ---
    // Built-in ids: 1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme; 0 = unknown name.
    CALENDAR,
    calendarId: (name) => addon.calendarId(name),
    calendarName: (id) => addon.calendarName(Number(id)),
    // which: 0 = the session containing utcSec, 1 = that or the previous, 2 = that or the next.
    // Returns { open, close, tradeDay, earlyClose } or null when there is none.
    calendarSession: (calendar, utcSec, which = 0) => addon.calendarSession(resolveCalendar(calendar, 'calendarSession'), BigInt(utcSec), Number(which)),
    calendarSessionForDay: (calendar, day) => addon.calendarSessionForDay(resolveCalendar(calendar, 'calendarSessionForDay'), BigInt(day)),
    calendarIsOpen: (calendar, utcSec) => addon.calendarIsOpen(resolveCalendar(calendar, 'calendarIsOpen'), BigInt(utcSec)),
    calendarOpenSeconds: (calendar, a, b) => addon.calendarOpenSeconds(resolveCalendar(calendar, 'calendarOpenSeconds'), BigInt(a), BigInt(b)),
    calendarSessionsBetween: (calendar, a, b) => addon.calendarSessionsBetween(resolveCalendar(calendar, 'calendarSessionsBetween'), BigInt(a), BigInt(b)),
    calendarPeriodsPerYear: (calendar, bucketSec) => addon.calendarPeriodsPerYear(resolveCalendar(calendar, 'calendarPeriodsPerYear'), Number(bucketSec)),
    calendarToLocal: (calendar, utcSec) => addon.calendarToLocal(resolveCalendar(calendar, 'calendarToLocal'), BigInt(utcSec)),
    daysFromCivil: (year, month, day) => addon.daysFromCivil(Number(year), Number(month), Number(day)),
    civilFromDays: (days) => addon.civilFromDays(BigInt(days)),
    // calendarDefine(name, weekly, utcOffsetSec, dstRule, holidays, earlyCloses, sessionsPerYear)
    // weekly: 7 entries (Monday first) of { openSec, closeSec } or null (no session that day);
    // dstRule: "none" | "us" | "eu" (or 0-2); holidays: day numbers; earlyCloses: { day, closeSec }.
    calendarDefine: (name, weekly, utcOffsetSec = 0, dstRule = 'none', holidays = [], earlyCloses = [], sessionsPerYear = 252) => {
        if (!Array.isArray(weekly) || weekly.length !== 7) throw new Error("calendarDefine: weekly must have 7 entries (Monday first), each { openSec, closeSec } or null");
        const flat = [];
        for (const d of weekly) {
            if (d === null || d === undefined) { flat.push(0, 0); continue; }
            const open = d.openSec ?? d.open_sec ?? d.open;
            const close = d.closeSec ?? d.close_sec ?? d.close;
            if (typeof open !== 'number' || typeof close !== 'number') throw new Error("calendarDefine: every weekly entry must be null or { openSec, closeSec }");
            flat.push(open, close);
        }
        const early = [];
        for (const e of earlyCloses ?? []) {
            const day = e.day ?? e.date;
            const close = e.closeSec ?? e.close_sec ?? e.close;
            if (typeof day !== 'number' || typeof close !== 'number') throw new Error("calendarDefine: every early close must be { day, closeSec }");
            early.push(day, close);
        }
        const dst = typeof dstRule === 'number' ? dstRule : ({ none: 0, us: 1, eu: 2 })[String(dstRule).toLowerCase()];
        if (dst === undefined) throw new Error(`calendarDefine: dstRule must be "none", "us", "eu" or 0-2, got ${dstRule}`);
        return addon.calendarDefine(String(name), flat, Number(utcOffsetSec), Number(dst), (holidays ?? []).map(Number), early, Number(sessionsPerYear));
    },

    // --- Signal backtester and universe features on caller-provided arrays (round 4) ---
    backtestArrays: (ts, open, high, low, close, target, params = {}, options = {}) =>
        addon.backtestArrays(ts, open, high, low, close, target, params ?? {}, options ?? {}),
    walkForwardSplits: (n, nSplits, trainFrac, anchored = true) =>
        addon.walkForwardSplits(Number(n), Number(nSplits), Number(trainFrac), Boolean(anchored)),
    backtestSplits: (ts, open, high, low, close, target, splits, params = {}) =>
        addon.backtestSplits(ts, open, high, low, close, target, splits, params ?? {}),
    // universe(dbs, { columns, bars, bucket, params, corr }) over databases opened on this thread.
    universe: (dbs, options = {}) => {
        if (options === null || typeof options !== 'object') throw new Error("universe options must be an object { columns, bars, bucket, params, corr }");
        const { handles, cols } = universeHandles(dbs, options.columns, 'universe');
        return addon.universe(handles, cols, Number(options.bars ?? 0), BigInt(options.bucket ?? 0),
            options.params ?? {}, options.corr === undefined ? true : Boolean(options.corr));
    },
    universeArrays: (closes, options = {}) => {
        if (options === null || typeof options !== 'object') throw new Error("universeArrays options must be an object { volumes, ts, params, corr }");
        return addon.universeArrays(closes, options.volumes ?? null, options.ts ?? null, options.params ?? {},
            options.corr === undefined ? true : Boolean(options.corr));
    },

    dbInitAsync: (ticker, dirPath, schema, config) => createWorkerHost().openDb(ticker, dirPath, schema, config, false),
    // Async lock-free reader (see openReader); every method returns a Promise.
    openReaderAsync: (ticker, dirPath, schema) => createWorkerHost().openDb(ticker, dirPath, schema, undefined, true),
};
