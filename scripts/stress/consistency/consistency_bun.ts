// Bun cross-binding consistency program (see README.md).
import { HOCDB } from "../../../bindings/bun/index.ts";
import { readFileSync } from "node:fs";

const req = JSON.parse(readFileSync(process.argv[2]!, "utf8"));
const schema = [
    { name: "timestamp", type: "i64" }, { name: "price", type: "f64" }, { name: "size", type: "f64" },
    { name: "bid", type: "f64" }, { name: "ask", type: "f64" }, { name: "side", type: "bool" },
] as const;
const db = new HOCDB(req.ticker, req.dir, schema as any, { max_file_size: 0 });
const cols = { close: "price", volume: "size" };
const enc = (x: number) => (Number.isNaN(x) ? null : x === Infinity ? "inf" : x === -Infinity ? "-inf" : x);
const encArr = (a: ArrayLike<number>) => Array.from(a, enc);
const out: any = { binding: "bun" };
const res = db.indicatorsTail(req.tail, req.specs, { columns: cols, bucket: BigInt(req.bucket) });
out.timestamps = Array.from(res.timestamps, (t: bigint) => Number(t));
out.columns = {};
for (const [k, v] of Object.entries(res.columns)) out.columns[k] = encArr(v as Float64Array);
const snap: any = db.snapshot({ columns: cols, bars: req.snapshot.bars, bucket: BigInt(req.snapshot.bucket), periodsPerYear: req.snapshot.periods_per_year });
out.snapshot = {};
for (const [k, v] of Object.entries(snap)) out.snapshot[k] = typeof v === "bigint" ? Number(v) : enc(v as number);
const sum: any = db.summary(BigInt(req.summary.start), BigInt(req.summary.end), req.summary.field, req.summary.periods_per_year);
out.summary = {};
for (const [k, v] of Object.entries(sum)) out.summary[k] = typeof v === "bigint" ? Number(v) : enc(v as number);
const bars: any = db.ohlcv(BigInt(req.ohlcv.start), BigInt(req.ohlcv.end), BigInt(req.ohlcv.bucket), { price: "price", volume: "size" });
out.ohlcv = { timestamps: Array.from(bars.timestamps, (t: bigint) => Number(t)) };
for (const k of ["open", "high", "low", "close", "volume", "count"]) out.ohlcv[k] = encArr(bars[k]);
db.close();
console.log(JSON.stringify(out));
