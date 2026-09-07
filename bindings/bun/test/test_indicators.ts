// Test for the indicator / analytics API of the Bun binding.
import { HOCDB, FieldDef, IndicatorSpec } from "../index";
import { join } from "path";
import { rmSync, existsSync } from "node:fs";

const TICKER = "TEST_BUN_IND";
const DATA_DIR = join(import.meta.dir, "..", "..", "..", "b_bun_test_indicators");

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

function check(cond: boolean, msg: string) {
    if (!cond) throw new Error(`FAIL: ${msg}`);
}

function expectThrows(fn: () => unknown, what: string): string {
    try {
        fn();
    } catch (e: any) {
        console.log(`  ${what} -> threw: ${e.message}`);
        return String(e.message);
    }
    throw new Error(`FAIL: ${what} did not throw`);
}

function cleanup() {
    if (existsSync(DATA_DIR)) {
        rmSync(DATA_DIR, { recursive: true, force: true });
    }
}

// Deterministic pseudo-random walk (same generator as the C test).
let seed = 7n;
function lcg(): number {
    seed = (seed * 6364136223846793005n + 1442695040888963407n) & 0xFFFFFFFFFFFFFFFFn;
    return Number(seed >> 11n) / 9007199254740992;
}

const schema: FieldDef[] = [
    { name: "timestamp", type: "i64" },
    { name: "open", type: "f64" },
    { name: "high", type: "f64" },
    { name: "low", type: "f64" },
    { name: "close", type: "f64" },
    { name: "volume", type: "f64" },
];

const bigintJson = (_: string, v: unknown) => typeof v === "bigint" ? v.toString() : v;

cleanup();
let db: HOCDB | undefined;
let ta: HOCDB | undefined; // tick database A (round 2)
let tb: HOCDB | undefined; // tick database B (round 2)
try {
    console.log("Initializing DB...");
    db = new HOCDB(TICKER, DATA_DIR, schema);

    const N = 3000;
    console.log(`Appending ${N} bars...`);
    const closes: number[] = [];
    const volumes: number[] = [];
    let p = 100.0;
    for (let i = 0; i < N; i++) {
        const o = p;
        p *= Math.exp((lcg() - 0.5) * 0.02);
        const volume = 1000 + (i % 50);
        db.append({
            timestamp: BigInt(1000 + i * 60),
            open: o,
            high: Math.max(o, p) * 1.003,
            low: Math.min(o, p) * 0.997,
            close: p,
            volume,
        });
        closes.push(p);
        volumes.push(volume);
    }
    db.flush();
    const lastTs = BigInt(1000 + (N - 1) * 60);

    // --- registry ---
    console.log("Testing registry helpers...");
    const kinds = HOCDB.indicatorKinds();
    check(kinds.length === 83, `83 kinds (got ${kinds.length})`);
    for (const k of ["sma", "rsi", "macd", "bbands", "heikin_ashi",
                     "spread", "order_flow", "series", "series2", "ratio", "forward_return", "triple_barrier", "session_vwap", "pivots"]) {
        check(kinds.includes(k), `kinds include ${k}`);
    }
    check(JSON.stringify(HOCDB.indicatorOutputs("macd")) === JSON.stringify(["macd", "signal", "hist"]), "macd outputs");
    check(JSON.stringify(HOCDB.indicatorOutputs(21)) === JSON.stringify(["macd", "signal", "hist"]), "macd outputs by id");
    check(HOCDB.indicatorOutputs("rsi").length === 1 && JSON.stringify(HOCDB.indicatorOutputs("RSI")) === JSON.stringify(HOCDB.indicatorOutputs("rsi")), "kind names are case-insensitive");
    check(HOCDB.indicatorWarmup({ kind: "ema", period: 200 }) > 200, "warmup > period for EMA");
    check(HOCDB.indicatorWarmup({ kind: "sma", period: 20 }) >= 19, "warmup for SMA 20");
    expectThrows(() => HOCDB.indicatorOutputs("nope"), "indicatorOutputs('nope')");

    // --- batch over a range ---
    console.log("Testing batch indicators over rows 1000..1500...");
    const specs: IndicatorSpec[] = [
        { kind: "sma", period: 20 },
        { kind: "macd" },
        { kind: "rsi", period: 14 },
        { kind: "bbands" },
        { kind: "atr", period: 14 },
        { kind: "obv" },
        { kind: "sma", period: 10, field: "volume" },
    ];
    const start = 1000 + 1000 * 60;
    const end = 1000 + 1500 * 60;
    const res = db.indicators(specs, { start, end });
    check(res.n_rows === 500, `500 rows (got ${res.n_rows})`);
    check(res.n_outputs === 13, `13 outputs (got ${res.n_outputs})`);
    const expectedNames = [
        "sma_20", "macd", "macd_signal", "macd_hist", "rsi_14",
        "bbands_upper", "bbands_middle", "bbands_lower", "bbands_percent_b", "bbands_bandwidth",
        "atr_14", "obv", "sma_10",
    ];
    check(JSON.stringify(res.names) === JSON.stringify(expectedNames), `column names: ${res.names.join(",")}`);
    for (const name of expectedNames) {
        check(res.columns[name] instanceof Float64Array && res.columns[name]!.length === 500, `column ${name} present`);
    }
    check(res.timestamps.length === 500 && res.values.length === 13 * 500, "array sizes");
    check(res.timestamps[0] === BigInt(start), "first timestamp == start");
    check(res.timestamps[499] === BigInt(end - 60), "last timestamp == end - 60");
    const sma20 = res.columns["sma_20"]!;
    const rsi = res.columns["rsi_14"]!;
    const upper = res.columns["bbands_upper"]!, middle = res.columns["bbands_middle"]!, lower = res.columns["bbands_lower"]!;
    const atr = res.columns["atr_14"]!;
    for (let i = 0; i < res.n_rows; i++) {
        check(!Number.isNaN(sma20[i]!), `sma_20 converged with lookback auto (row ${i})`);
        check(rsi[i]! >= 0 && rsi[i]! <= 100, `rsi in [0,100] (row ${i})`);
        check(upper[i]! >= middle[i]! && middle[i]! >= lower[i]!, `bbands ordered (row ${i})`);
        check(atr[i]! > 0, `atr positive (row ${i})`);
    }
    // planar layout: output k == values[k*n_rows ..]
    check(res.values[4 * 500 + 7] === rsi[7], "columns are views of the planar buffer");

    // sma_20 against a local reference (mean of the previous 20 closes)
    for (const row of [0, 1, 123, 250, 499]) {
        const idx = 1000 + row;
        let sum = 0;
        for (let j = idx - 19; j <= idx; j++) sum += closes[j]!;
        const ref = sum / 20;
        check(Math.abs(sma20[row]! - ref) <= 1e-9 * Math.abs(ref), `sma_20 matches reference at row ${row}: ${sma20[row]} vs ${ref}`);
    }
    // sma_10 of the volume field against a local reference
    const volSma = res.columns["sma_10"]!;
    for (const row of [0, 77, 499]) {
        const idx = 1000 + row;
        let sum = 0;
        for (let j = idx - 9; j <= idx; j++) sum += volumes[j]!;
        check(Math.abs(volSma[row]! - sum / 10) <= 1e-9, `volume sma_10 matches reference at row ${row}`);
    }

    // explicit lookback 0 -> NaN warm-up inside the window
    console.log("Testing lookback 0...");
    const res0 = db.indicators([{ kind: "sma", period: 20 }], { start, end, lookback: 0 });
    check(res0.n_rows === 500, "lookback 0 rows");
    const s0 = res0.columns["sma_20"]!;
    for (let i = 0; i < 19; i++) check(Number.isNaN(s0[i]!), `sma_20 row ${i} is NaN with lookback 0`);
    check(!Number.isNaN(s0[19]!), "sma_20 row 19 is defined with lookback 0");

    // --- tail ---
    console.log("Testing tail...");
    const tail = db.indicatorsTail(5, [{ kind: "sma", period: 20 }, { kind: "macd" }, { kind: "ema", period: 9, label: "fast" }]);
    check(tail.n_rows === 5 && tail.n_outputs === 5, "tail rows/outputs");
    check(tail.timestamps[4] === lastTs, "tail ends at the last timestamp");
    check(JSON.stringify(tail.names) === JSON.stringify(["sma_20", "macd", "macd_signal", "macd_hist", "fast"]), "tail names incl. label");
    check(!Number.isNaN(tail.columns["fast"]![4]!), "labelled column populated");
    const tailOpt = db.indicators([{ kind: "rsi" }], { tail: 3 });
    check(tailOpt.n_rows === 3 && tailOpt.names[0] === "rsi" && tailOpt.timestamps[2] === lastTs, "tail option + default label");

    // --- bucket (tick -> 5-minute bars) ---
    console.log("Testing tail with bucket 300...");
    const bars = db.indicatorsTail(10, [{ kind: "sma", period: 20 }], { bucket: 300 });
    check(bars.n_rows === 10, `10 bars (got ${bars.n_rows})`);
    for (let i = 1; i < 10; i++) check(bars.timestamps[i]! - bars.timestamps[i - 1]! === 300n, `bar spacing at ${i}`);
    check(bars.timestamps[0]! % 300n === 0n, "bar alignment");

    // --- errors ---
    console.log("Testing errors...");
    expectThrows(() => db!.indicators([{ kind: "nope" }], { tail: 10 }), "unknown kind name");
    expectThrows(() => db!.indicators([{ kind: 9999 }], { tail: 10 }), "unknown kind id");
    expectThrows(() => db!.indicators([{ kind: "sma" }], { tail: 10, columns: { close: null } }), "missing close column");
    expectThrows(() => db!.indicators([{ kind: "sma" }], { tail: 10, columns: { close: "nonexistent" } }), "invalid close column name");
    expectThrows(() => db!.indicators([{ kind: "atr", period: 14 }], { tail: 10, columns: { open: null, high: null, low: null, volume: null } }), "atr with only close");
    expectThrows(() => db!.indicators([{ kind: "sma", period: 5, field: "nonexistent" }], { tail: 10 }), "invalid field name");
    expectThrows(() => db!.indicators([{ kind: "sma", period: 5, field: 42 }], { tail: 10 }), "invalid field index");
    expectThrows(() => db!.indicators([{ kind: "sma", period: 5, field: "volume" }], { tail: 10, bucket: 300 }), "field override with bucket");
    expectThrows(() => db!.indicators([{ kind: "sma", period: 5 }, { kind: "sma", period: 5, field: "volume" }], { tail: 10 }), "duplicate column names");
    expectThrows(() => db!.indicators([], { tail: 10 }), "empty spec list");

    // --- ohlcv ---
    console.log("Testing ohlcv...");
    const ohlcv = db.ohlcv(INT64_MIN, INT64_MAX, 300);
    check(ohlcv.n_bars > 500, `ohlcv bars > 500 (got ${ohlcv.n_bars})`);
    check(ohlcv.timestamps.length === ohlcv.n_bars && ohlcv.close.length === ohlcv.n_bars, "ohlcv array sizes");
    let totalCount = 0, totalVolume = 0;
    for (let i = 0; i < ohlcv.n_bars; i++) {
        check(ohlcv.high[i]! >= ohlcv.low[i]!, `bar high >= low (${i})`);
        check(ohlcv.close[i]! <= ohlcv.high[i]! && ohlcv.close[i]! >= ohlcv.low[i]!, `close within bar (${i})`);
        check(ohlcv.count[i]! >= 1, `bar count (${i})`);
        totalCount += ohlcv.count[i]!;
        totalVolume += ohlcv.volume[i]!;
    }
    check(totalCount === N, `bar counts sum to ${N}`);
    check(Math.abs(totalVolume - volumes.reduce((a, b) => a + b, 0)) < 1e-6, "bar volumes sum to total volume");
    const ohlcvExplicit = db.ohlcv(INT64_MIN, INT64_MAX, 300, { price: "close", volume: null });
    check(ohlcvExplicit.n_bars === ohlcv.n_bars && ohlcvExplicit.volume[0] === ohlcvExplicit.count[0], "ohlcv without volume field uses record count");
    expectThrows(() => db!.ohlcv(INT64_MIN, INT64_MAX, 300, { price: "nonexistent" }), "ohlcv invalid price field");

    // --- summary ---
    console.log("Testing summary...");
    const sum = db.summary(INT64_MIN, INT64_MAX, "close", 252);
    console.log("  summary:", JSON.stringify(sum));
    check(Object.keys(sum).length === 29, `29 summary fields (got ${Object.keys(sum).length})`);
    check(sum.count === N, `summary count == ${N} (got ${sum.count})`);
    check(sum.max_drawdown! <= 0 && sum.max_drawdown! >= -1, "max drawdown range");
    check(sum.win_rate! >= 0 && sum.win_rate! <= 1, "win rate range");
    check(Number.isFinite(sum.sharpe!) && Number.isFinite(sum.hurst!), "sharpe / hurst finite");
    check(Math.abs(sum.last! - closes[N - 1]!) < 1e-9 && Math.abs(sum.first! - closes[0]!) < 1e-9, "summary first / last");
    expectThrows(() => db!.summary(INT64_MIN, INT64_MAX, "nonexistent"), "summary invalid field");

    // --- snapshot ---
    console.log("Testing snapshot...");
    const snap = db.snapshot({ periodsPerYear: 252 });
    check(Object.keys(snap).length >= 90, `>= 90 snapshot fields (got ${Object.keys(snap).length})`);
    check(typeof snap.timestamp === "bigint" && typeof snap.bars === "number", "snapshot timestamp is bigint, bars is number");
    check(snap.bars === 2500, `snapshot bars == 2500 (got ${snap.bars})`);
    check(snap.timestamp === lastTs, "snapshot timestamp == last ts");
    check((snap.rsi_14 as number) >= 0 && (snap.rsi_14 as number) <= 100, "snapshot rsi_14 in [0,100]");
    for (const f of ["ema_200", "adx_14", "mfi_14", "supertrend"]) {
        check(Number.isFinite(snap[f] as number), `snapshot ${f} finite`);
    }
    check(Math.abs((snap.close as number) - closes[N - 1]!) < 1e-9, "snapshot close is the latest close");
    console.log(`  snapshot: close=${snap.close} rsi_14=${snap.rsi_14} ema_200=${snap.ema_200} adx_14=${snap.adx_14} supertrend=${snap.supertrend}`);

    const snapBars = db.snapshot({ bars: 50, bucket: 300, periodsPerYear: 252 });
    check(snapBars.bars === 50, `snapshot with bucket: bars == 50 (got ${snapBars.bars})`);
    check(Number.isNaN(snapBars.sma_200 as number), "snapshot with 50 bars: sma_200 is NaN");
    check(Number.isFinite(snapBars.sma_20 as number), "snapshot with 50 bars: sma_20 finite");
    expectThrows(() => db!.snapshot({ columns: { close: null } }), "snapshot without close");

    // ------------------------------------------------------------------
    // Round 2: ticks with quotes / sides, pairs, labels, sessions, health,
    // decision evaluation, multi-timeframe snapshots
    // ------------------------------------------------------------------
    console.log("Testing registry round 2 (look-ahead flag, new kinds)...");
    check(HOCDB.indicatorIsLookahead("forward_return") === true, "forward_return is look-ahead");
    check(HOCDB.indicatorIsLookahead("triple_barrier") === true && HOCDB.indicatorIsLookahead(150) === true, "triple_barrier / id 150 are look-ahead");
    check(HOCDB.indicatorIsLookahead("sma") === false && HOCDB.indicatorIsLookahead("session_vwap") === false, "sma / session_vwap are not look-ahead");
    expectThrows(() => HOCDB.indicatorIsLookahead("nope"), "indicatorIsLookahead('nope')");
    check(HOCDB.indicatorOutputs("pivots").length === 5, "pivots has 5 outputs");
    check(JSON.stringify(HOCDB.indicatorOutputs("spread")) === JSON.stringify(["abs", "bps"]), "spread outputs");
    check(JSON.stringify(HOCDB.indicatorOutputs(131)) === JSON.stringify(["net", "imbalance"]), "order_flow outputs by id");

    console.log("Creating tick databases A and B (6000 s, one tick per second / every 2 s)...");
    const tickSchema: FieldDef[] = [
        { name: "timestamp", type: "i64" },
        { name: "price", type: "f64" },
        { name: "size", type: "f64" },
        { name: "bid", type: "f64" },
        { name: "ask", type: "f64" },
        { name: "side", type: "bool" },
    ];
    ta = new HOCDB("PAIR_A", DATA_DIR, tickSchema);
    tb = new HOCDB("PAIR_B", DATA_DIR, tickSchema);
    const NT = 6000;
    const US = 1_000_000; // microsecond timestamps, one tick per second
    const tickPrices: number[] = [];
    let pa = 100.0, pb = 50.0;
    for (let i = 0; i < NT; i++) {
        pa *= Math.exp((lcg() - 0.5) * 0.004);
        pb *= Math.exp((lcg() - 0.5) * 0.004);
        ta.append({ timestamp: BigInt(US * i), price: pa, size: 1 + (i % 4), bid: pa * 0.999, ask: pa * 1.001, side: i % 3 !== 0 ? 1 : 0 });
        if (i % 2 === 0) { // B trades every 2 seconds, 300 ms after A
            tb.append({ timestamp: BigInt(US * i + 300_000), price: pb, size: 2, bid: pb * 0.999, ask: pb * 1.001, side: i % 2 });
        }
        tickPrices.push(pa);
    }
    ta.flush();
    tb.flush();
    const lastTickTs = BigInt(US * (NT - 1));

    // --- microstructure / session / label kinds on raw ticks (columns auto-detected: price, size, bid, ask, side) ---
    console.log("Testing microstructure / session / label indicators on ticks (tail 100)...");
    const micro = ta.indicatorsTail(100, [
        { kind: "spread" },
        { kind: "order_flow", period: 10 },
        { kind: "trade_intensity", period: 10, param: 1e6 },
        { kind: "tick_pressure", period: 20 },
        { kind: "session_vwap", param: 600e6 }, // 10-minute sessions
        { kind: "forward_return", period: 5 },
    ]);
    check(micro.n_rows === 100, `micro: 100 rows (got ${micro.n_rows})`);
    check(micro.n_outputs === 11, `micro: 11 outputs (got ${micro.n_outputs})`);
    const microNames = [
        "spread_abs", "spread_bps", "order_flow_10_net", "order_flow_10_imbalance",
        "trade_intensity_10_trades_per_sec", "trade_intensity_10_volume_per_sec", "tick_pressure_20", "session_vwap",
        "forward_return_5_ret", "forward_return_5_max", "forward_return_5_min",
    ];
    check(JSON.stringify(micro.names) === JSON.stringify(microNames), `micro names: ${micro.names.join(",")}`);
    check(micro.timestamps[99] === lastTickTs, "micro ends at the last tick");
    const spreadBps = micro.columns["spread_bps"]!, tps = micro.columns["trade_intensity_10_trades_per_sec"]!;
    const imbalance = micro.columns["order_flow_10_imbalance"]!, fwd = micro.columns["forward_return_5_ret"]!;
    const svwap = micro.columns["session_vwap"]!;
    for (let i = 0; i < 100; i++) {
        check(Math.abs(spreadBps[i]! - 20) < 1e-9, `spread_bps == 20 (row ${i}: ${spreadBps[i]})`);
        check(Math.abs(tps[i]! - 1) < 1e-9, `trades_per_sec == 1 (row ${i}: ${tps[i]})`);
        check(imbalance[i]! >= -1 && imbalance[i]! <= 1, `imbalance in [-1, 1] (row ${i}: ${imbalance[i]})`);
        check(svwap[i]! > 0 && Number.isFinite(svwap[i]!), `session_vwap positive (row ${i})`);
        if (i >= 95) check(Number.isNaN(fwd[i]!), `forward_return_5_ret NaN in the last 5 rows (row ${i})`);
        else check(Number.isFinite(fwd[i]!), `forward_return_5_ret finite before the last 5 rows (row ${i})`);
    }
    console.log(`  spread_bps=${spreadBps[99]} imbalance=${imbalance[99]} session_vwap=${svwap[99]} forward_return_5_ret[90]=${fwd[90]}`);

    // column roles: explicit by index, absent roles rejected
    const spreadIdx = ta.indicatorsTail(10, [{ kind: "spread" }], { columns: { close: "price", bid: 3, ask: 4, side: null } });
    check(spreadIdx.n_rows === 10 && Math.abs(spreadIdx.columns["spread_bps"]![9]! - 20) < 1e-9, "bid / ask roles by index");
    expectThrows(() => ta!.indicatorsTail(10, [{ kind: "spread" }], { columns: { bid: null } }), "spread without a bid column");
    expectThrows(() => ta!.indicatorsTail(10, [{ kind: "spread" }], { columns: { ask: "nonexistent" } }), "invalid ask column name");
    // session kinds need param = session length
    expectThrows(() => ta!.indicatorsTail(10, [{ kind: "session_vwap" }]), "session_vwap without param");
    expectThrows(() => ta!.indicatorsTail(10, [{ kind: "pivots" }]), "pivots without param");
    // passthrough kinds with field2 in a single-database call
    const single2 = ta.indicatorsTail(5, [{ kind: "series" }, { kind: "series2", field2: "ask" }, { kind: "ratio", field2: "bid", label: "px_over_bid" }]);
    check(JSON.stringify(single2.names) === JSON.stringify(["series", "series2", "px_over_bid"]), "passthrough names");
    for (let i = 0; i < 5; i++) {
        const p = tickPrices[NT - 5 + i]!;
        check(Math.abs(single2.columns["series"]![i]! - p) < 1e-12, `series == price (row ${i})`);
        check(Math.abs(single2.columns["series2"]![i]! - p * 1.001) < 1e-9, `series2 == ask via field2 (row ${i})`);
        check(Math.abs(single2.columns["px_over_bid"]![i]! - 1 / 0.999) < 1e-9, `ratio == price / bid via field2 (row ${i})`);
    }

    // --- pairs ---
    console.log("Testing pairIndicatorsTail (as-of join on ticks)...");
    const pairSpecs: IndicatorSpec[] = [
        { kind: "series" }, { kind: "series2" }, { kind: "ratio" }, { kind: "correl", period: 30 }, { kind: "rel_strength", period: 10 },
    ];
    const pt = ta.pairIndicatorsTail(tb, 50, pairSpecs);
    check(pt.n_rows === 50 && pt.n_outputs === 5, `pair tail: 50 rows x 5 outputs (got ${pt.n_rows} x ${pt.n_outputs})`);
    check(JSON.stringify(pt.names) === JSON.stringify(["series", "series2", "ratio", "correl_30", "rel_strength_10"]), `pair names: ${pt.names.join(",")}`);
    check(pt.timestamps[49] === lastTickTs, "pair tail ends at A's last tick");
    for (let i = 0; i < 50; i++) {
        const a = pt.columns["series"]![i]!, b = pt.columns["series2"]![i]!, r = pt.columns["ratio"]![i]!;
        check(Math.abs(a / b - r) < 1e-12, `ratio == series / series2 (row ${i})`);
        check(Math.abs(a - tickPrices[NT - 50 + i]!) < 1e-12, `series is A's price (row ${i})`);
        check(Number.isFinite(pt.columns["correl_30"]![i]!), `correl finite (row ${i})`);
        check(Number.isFinite(pt.columns["rel_strength_10"]![i]!), `rel_strength finite (row ${i})`);
    }
    console.log(`  ratio=${pt.columns["ratio"]![49]} correl_30=${pt.columns["correl_30"]![49]} rel_strength_10=${pt.columns["rel_strength_10"]![49]}`);

    console.log("Testing pairIndicators over [1000 s, 2000 s) on 10-second bars...");
    const pr = ta.pairIndicators(tb, pairSpecs, { start: 1_000_000_000, end: 2_000_000_000, bucket: 10_000_000, lookback: 0 });
    check(pr.n_rows === 100, `pair range: 100 bars (got ${pr.n_rows})`);
    check(pr.timestamps[0] === 1_000_000_000n, `pair range starts at 1_000_000_000 (got ${pr.timestamps[0]})`);
    for (let i = 0; i < 100; i++) {
        check(pr.timestamps[i]! % 10_000_000n === 0n, `pair bar ${i} aligned`);
        if (i > 0) check(pr.timestamps[i]! - pr.timestamps[i - 1]! === 10_000_000n, `pair bar spacing at ${i}`);
    }
    check(Number.isFinite(pr.columns["ratio"]![99]!) && Number.isFinite(pr.columns["correl_30"]![99]!), "pair range values on bars");
    const pr2 = ta.pairIndicators(tb, [{ kind: "ratio" }], { tail: 5, columns: { close: "price" }, columns2: { close: "price", volume: null } });
    const pr3 = ta.pairIndicators(tb, [{ kind: "ratio" }], { tail: 5, otherColumns: { close: 1 } });
    check(pr2.n_rows === 5 && pr3.n_rows === 5 && pr2.columns["ratio"]![4] === pr3.columns["ratio"]![4], "columns2 / otherColumns");
    expectThrows(() => ta!.pairIndicators({} as unknown as HOCDB, pairSpecs, { tail: 5 }), "pairIndicators with a non-HOCDB other");
    expectThrows(() => ta!.pairIndicators(tb!, pairSpecs, { tail: 5, columns2: { close: null } }), "pairIndicators without the other close");
    expectThrows(() => ta!.pairIndicators(tb!, [], { tail: 5 }), "pairIndicators with no specs");

    // --- ohlcv with buy volume ---
    console.log("Testing ohlcv with a side field (buy volume)...");
    const bx = ta.ohlcv(INT64_MIN, INT64_MAX, 60_000_000, { side: "side" });
    check(bx.n_bars === 100, `ohlcv_ex: 100 bars (got ${bx.n_bars})`);
    check(bx.buy_volume instanceof Float64Array && bx.buy_volume.length === 100, "buy_volume present");
    let buyTotal = 0, volTotal = 0;
    for (let i = 0; i < bx.n_bars; i++) {
        check(bx.buy_volume![i]! >= 0 && bx.buy_volume![i]! <= bx.volume[i]!, `0 <= buy_volume <= volume (bar ${i})`);
        buyTotal += bx.buy_volume![i]!;
        volTotal += bx.volume[i]!;
    }
    check(Math.abs(volTotal - 15000) < 1e-6, `volume auto-detected from 'size' (total ${volTotal})`); // sum of 1 + i % 4
    check(buyTotal > 0 && buyTotal < volTotal, `buy volume is a strict share of the volume (${buyTotal} / ${volTotal})`);
    const bx0 = ta.ohlcv(INT64_MIN, INT64_MAX, 60_000_000);
    check(bx0.n_bars === 100 && bx0.buy_volume === undefined && !("buy_volume" in bx0), "without side: no buy_volume");
    check(bx0.volume[0] === bx.volume[0] && bx0.close[99] === bx.close[99], "ohlcv with / without side agree on the bars");
    expectThrows(() => ta!.ohlcv(INT64_MIN, INT64_MAX, 60_000_000, { side: "nonexistent" }), "ohlcv invalid side field");

    // --- health ---
    console.log("Testing health...");
    const hl = ta.health(INT64_MIN, INT64_MAX, "price", "size", 5_000_000, 0.05);
    console.log("  health:", JSON.stringify(hl, bigintJson));
    check(Object.keys(hl).length === 19, `19 health fields (got ${Object.keys(hl).length})`);
    check(hl.count === 6000, `health count == 6000 (got ${hl.count})`);
    check(hl.n_gaps === 0, `n_gaps == 0 (got ${hl.n_gaps})`);
    check(hl.median_gap === 1_000_000 && hl.mean_gap === 1_000_000, `median / mean gap == 1_000_000 (got ${hl.median_gap} / ${hl.mean_gap})`);
    check(hl.n_outlier_returns === 0, `n_outlier_returns == 0 (got ${hl.n_outlier_returns})`);
    check(hl.first_ts === 0n && hl.last_ts === 5_999_000_000n, `first_ts 0, last_ts 5_999_000_000 (got ${hl.first_ts}, ${hl.last_ts})`);
    check(typeof hl.first_ts === "bigint" && typeof hl.max_gap === "bigint" && typeof hl.count === "number", "health field types");
    check(hl.span === 5_999_000_000n && hl.max_gap === 1_000_000n, "health span / max_gap");
    check(hl.n_nonpositive_price === 0 && hl.n_nan_price === 0 && hl.n_zero_volume === 0 && hl.n_negative_volume === 0, "health counters are zero");
    const hl2 = ta.health(INT64_MIN, INT64_MAX, undefined, null, 500_000, 0.05); // price auto-detected, every 1 s gap exceeds 0.5 s
    check(hl2.count === 6000 && hl2.n_gaps === 5999, `gap threshold 500_000 -> 5999 gaps (got ${hl2.n_gaps})`);
    expectThrows(() => ta!.health(INT64_MIN, INT64_MAX, "nonexistent"), "health invalid price field");

    // --- evaluate ---
    console.log("Testing evaluate...");
    const ev = ta.evaluate([
        { timestamp: US * 100, direction: 1, size: 1000, horizon: 60 * US },
        { timestamp: BigInt(US * 200), direction: -1, size: 500 },            // horizon 0 -> defaultHorizon
        { timestamp: US * 5990, direction: 1, size: 100, horizon: 60 * US },  // exit beyond the data
        { timestamp: US * 300, direction: 0 },                                 // flat: ignored
    ], { defaultHorizon: 120 * US, costBps: 5 });
    console.log("  evaluation:", JSON.stringify(ev, (k, v) => v instanceof Float64Array ? Array.from(v) : v));
    check(Object.keys(ev).length === 20 + 3, `20 evaluation fields + 3 arrays (got ${Object.keys(ev).length})`);
    check(ev.n_decisions === 4 && ev.n_evaluated === 2 && ev.n_long === 2 && ev.n_short === 1,
        `evaluate counts: ${ev.n_decisions} / ${ev.n_evaluated} / ${ev.n_long} / ${ev.n_short}`);
    check(ev.entry.length === 4 && ev.exit.length === 4 && ev.net_return.length === 4, "per-decision arrays have 4 entries");
    check(Math.abs(ev.net_return[0]! - (ev.exit[0]! / ev.entry[0]! - 1 - 0.001)) < 1e-12, "net[0] == exit / entry - 1 - 2 x 5 bps");
    check(Math.abs(ev.entry[0]! - tickPrices[100]!) < 1e-12 && Math.abs(ev.exit[0]! - tickPrices[160]!) < 1e-12, "entry / exit prices of decision 0");
    check(Number.isFinite(ev.net_return[1]!) && Number.isNaN(ev.net_return[2]!) && Number.isNaN(ev.net_return[3]!), "net[1] finite, net[2] and net[3] NaN");
    check(ev.hit_rate >= 0 && ev.hit_rate <= 1 && ev.total_cost > 0, "hit rate / cost");
    const ev0 = ta.evaluate([]);
    check(ev0.n_decisions === 0 && ev0.n_evaluated === 0 && Number.isNaN(ev0.hit_rate), "empty evaluation");
    check(ev0.entry.length === 0 && ev0.exit.length === 0 && ev0.net_return.length === 0, "empty evaluation arrays");
    expectThrows(() => ta!.evaluate([{ timestamp: 0, direction: 1 }], { priceField: "nonexistent" }), "evaluate invalid price field");
    expectThrows(() => ta!.evaluate([{ direction: 1 } as unknown as { timestamp: number, direction: number }]), "decision without timestamp");

    // --- snapshotMulti ---
    console.log("Testing snapshotMulti...");
    const multi = ta.snapshotMulti({ buckets: [60_000_000, 300_000_000], periodsPerYear: [525600, 105120], bars: 50 });
    check(multi.length === 2, `two snapshots (got ${multi.length})`);
    const single = ta.snapshot({ bars: 50, bucket: 60_000_000, periodsPerYear: 525600 });
    check(Object.keys(single).length === Object.keys(multi[0]!).length, "multi[0] has every snapshot field");
    for (const key of Object.keys(single)) {
        check(Object.is(single[key], multi[0]![key]), `multi[0].${key} == snapshot (${multi[0]![key]} vs ${single[key]})`);
    }
    check(multi[0]!.bars === 50, `multi[0].bars == 50 (got ${multi[0]!.bars})`);
    check(multi[1]!.bars === 20, `multi[1].bars == 20: only 100 minutes of data (got ${multi[1]!.bars})`);
    check(multi[0]!.timestamp % 60_000_000n === 0n && multi[1]!.timestamp % 300_000_000n === 0n, "multi snapshots aligned to their buckets");
    check(Number.isFinite(multi[1]!.sma_10 as number) && Number.isNaN(multi[1]!.sma_50 as number), "20 five-minute bars: sma_10 defined, sma_50 NaN");
    const multi0 = ta.snapshotMulti({ buckets: [60_000_000n], bars: 50 }); // periodsPerYear defaults to 0
    check(multi0.length === 1 && multi0[0]!.bars === 50 && multi0[0]!.close === single.close, "snapshotMulti with default periodsPerYear");
    expectThrows(() => ta!.snapshotMulti({ buckets: [60_000_000, 300_000_000], periodsPerYear: [1] }), "snapshotMulti periodsPerYear length mismatch");
    expectThrows(() => ta!.snapshotMulti({ buckets: [] }), "snapshotMulti with no buckets");

    console.log("Closing tick DBs...");
    tb.close();
    expectThrows(() => ta!.pairIndicatorsTail(tb!, 5, [{ kind: "ratio" }]), "pairIndicators with a closed other");
    ta.close();
    ta = tb = undefined;

    console.log("Closing DB...");
    db.close();
    db = undefined;
    console.log("Bun Indicator Test Passed!");
} catch (e) {
    console.error(e);
    if (db) db.close();
    if (ta) ta.close();
    if (tb) tb.close();
    cleanup();
    process.exit(1);
} finally {
    cleanup();
}
