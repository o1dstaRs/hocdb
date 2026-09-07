// Node.js cross-binding consistency program (see README.md).
const fs = require('fs');
const hocdb = require('../../../bindings/node/index.js');
const req = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const schema = [
    { name: 'timestamp', type: 'i64' }, { name: 'price', type: 'f64' }, { name: 'size', type: 'f64' },
    { name: 'bid', type: 'f64' }, { name: 'ask', type: 'f64' }, { name: 'side', type: 'bool' },
];
const db = hocdb.dbInit(req.ticker, req.dir, schema, {});
const cols = { close: 'price', volume: 'size' };
const enc = (x) => (Number.isNaN(x) ? null : x === Infinity ? 'inf' : x === -Infinity ? '-inf' : x);
const encArr = (a) => Array.from(a, enc);
const out = { binding: 'node' };
const res = db.indicatorsTail(req.tail, req.specs, { columns: cols, bucket: req.bucket });
out.timestamps = Array.from(res.timestamps, (t) => Number(t));
out.columns = {};
for (const name of res.names) out.columns[name] = encArr(res[name]);
const snap = db.snapshot({ columns: cols, bars: req.snapshot.bars, bucket: req.snapshot.bucket, periodsPerYear: req.snapshot.periods_per_year });
out.snapshot = {};
for (const [k, v] of Object.entries(snap)) out.snapshot[k] = typeof v === 'bigint' ? Number(v) : enc(v);
const sum = db.summary(req.summary.start, req.summary.end, req.summary.field, req.summary.periods_per_year);
out.summary = {};
for (const [k, v] of Object.entries(sum)) out.summary[k] = typeof v === 'bigint' ? Number(v) : enc(v);
const bars = db.ohlcv(req.ohlcv.start, req.ohlcv.end, req.ohlcv.bucket, { price: 'price', volume: 'size' });
out.ohlcv = { timestamps: Array.from(bars.timestamps, (t) => Number(t)) };
for (const k of ['open', 'high', 'low', 'close', 'volume', 'count']) out.ohlcv[k] = encArr(bars[k]);
db.close();
process.stdout.write(JSON.stringify(out));
