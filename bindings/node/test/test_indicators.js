// Test for the indicator / analytics API of the Node.js binding
// (indicators, indicatorsTail, pairIndicators, ohlcv, summary, snapshot, snapshotMulti,
// health, evaluate and the kind registry), mirroring bindings/c/test/test_indicators.c.
const hocdb = require('../index.js');
const path = require('path');
const fs = require('fs');

const DATA_DIR = path.join(__dirname, '..', '..', '..', 'b_node_test_indicators');
const TICKER = 'TEST_NODE_IND';

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

function rmrf(dir) {
    if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
}

function check(cond, msg) {
    if (!cond) throw new Error(`FAIL: ${msg}`);
}

function expectThrow(fn, msg) {
    let threw = false;
    try { fn(); } catch (e) { threw = true; }
    check(threw, msg);
}

async function expectReject(promise, msg) {
    let rejected = false;
    try { await promise; } catch (e) { rejected = true; }
    check(rejected, msg);
}

// Same generator as the C test: 64-bit LCG, top 53 bits as a double in [0, 1).
let seed = 7n;
function lcg() {
    seed = (seed * 6364136223846793005n + 1442695040888963407n) & 0xFFFFFFFFFFFFFFFFn;
    return Number(seed >> 11n) / 9007199254740992.0;
}

const schema = [
    { name: 'timestamp', type: 'i64' },
    { name: 'open', type: 'f64' },
    { name: 'high', type: 'f64' },
    { name: 'low', type: 'f64' },
    { name: 'close', type: 'f64' },
    { name: 'volume', type: 'f64' }
];

const N = 3000;
const closes = new Array(N);
let lastClose = 0;

function tsAt(i) { return 1000n + BigInt(i) * 60n; }

// --- round 2: tick databases with quotes and sides (A: 1 tick/s, B: 1 tick/2 s) ---
const tickSchema = [
    { name: 'timestamp', type: 'i64' },
    { name: 'price', type: 'f64' },
    { name: 'size', type: 'f64' },
    { name: 'bid', type: 'f64' },
    { name: 'ask', type: 'f64' },
    { name: 'side', type: 'bool' }
];
const TICKER_A = 'PAIR_A', TICKER_B = 'PAIR_B';
const NT = 6000;
const SEC = 1_000_000n; // microsecond timestamps, one tick per second

const microSpecs = [
    { kind: 'spread' },
    { kind: 'order_flow', period: 10 },
    { kind: 'trade_intensity', period: 10, param: 1e6 },
    { kind: 'tick_pressure', period: 20 },
    { kind: 'session_vwap', param: 600e6 },      // 10-minute sessions
    { kind: 'forward_return', period: 5 },
];
const microNames = ['spread_abs', 'spread_bps', 'order_flow_10_net', 'order_flow_10_imbalance',
    'trade_intensity_10_trades_per_sec', 'trade_intensity_10_volume_per_sec', 'tick_pressure_20',
    'session_vwap', 'forward_return_5_ret', 'forward_return_5_max', 'forward_return_5_min'];
const pairSpecs = ['series', 'series2', 'ratio', { kind: 'correl', period: 30 }, { kind: 'rel_strength', period: 10 }];
const decisions = [
    { timestamp: 100n * SEC, direction: 1, size: 1000, horizon: 60n * SEC },
    { timestamp: 200n * SEC, direction: -1, size: 500, horizon: 0 },
    { timestamp: 5990n * SEC, direction: 1, size: 100, horizon: 60n * SEC },
    { timestamp: 300n * SEC, direction: 0 },
];
const evalOpts = { priceField: 'price', defaultHorizon: 120n * SEC, costBps: 5 };

function checkMicro(res, label) {
    check(res.n_rows === 100 && res.n_outputs === 11, `${label}: 100 rows x 11 outputs, got ${res.n_rows} x ${res.n_outputs}`);
    check(JSON.stringify(res.names) === JSON.stringify(microNames), `${label}: column names: ${res.names}`);
    for (let i = 0; i < 100; i++) {
        check(Math.abs(res.spread_bps[i] - 20) < 1e-9, `${label}: spread 20 bps (row ${i}): ${res.spread_bps[i]}`);
        check(Math.abs(res.trade_intensity_10_trades_per_sec[i] - 1) < 1e-9, `${label}: 1 trade per second (row ${i})`);
        check(res.order_flow_10_imbalance[i] >= -1 && res.order_flow_10_imbalance[i] <= 1, `${label}: imbalance in [-1, 1] (row ${i})`);
        check(Number.isFinite(res.session_vwap[i]) && Number.isFinite(res.tick_pressure_20[i]), `${label}: session_vwap / tick_pressure finite (row ${i})`);
    }
    for (let i = 95; i < 100; i++) check(Number.isNaN(res.forward_return_5_ret[i]), `${label}: forward return NaN at row ${i}`);
    for (let i = 0; i < 95; i++) check(Number.isFinite(res.forward_return_5_ret[i]), `${label}: forward return finite at row ${i}`);
}

function checkPairTail(res, label) {
    check(res.n_rows === 50 && res.n_outputs === 5, `${label}: 50 rows x 5 outputs`);
    check(JSON.stringify(res.names) === '["series","series2","ratio","correl_30","rel_strength_10"]', `${label}: names ${res.names}`);
    for (let i = 0; i < 50; i++) {
        check(Math.abs(res.series[i] / res.series2[i] - res.ratio[i]) < 1e-12, `${label}: ratio == series / series2 (row ${i})`);
        check(Number.isFinite(res.correl_30[i]), `${label}: correl finite (row ${i})`);
        check(Number.isFinite(res.rel_strength_10[i]), `${label}: rel_strength finite (row ${i})`);
    }
}

function checkPairRange(res, label) {
    check(res.n_rows === 100, `${label}: 100 bars, got ${res.n_rows}`);
    check(res.timestamps[0] === 1_000_000_000n, `${label}: first bar at 1_000_000_000, got ${res.timestamps[0]}`);
    for (let i = 1; i < 100; i++) check(res.timestamps[i] - res.timestamps[i - 1] === 10_000_000n, `${label}: bar spacing (row ${i})`);
    for (let i = 0; i < 100; i++) check(res.timestamps[i] % 10_000_000n === 0n, `${label}: bar alignment (row ${i})`);
}

function checkBarsWithSide(bars, label) {
    check(bars.n_bars === 100, `${label}: 100 bars, got ${bars.n_bars}`);
    check(bars.buy_volume instanceof Float64Array && bars.buy_volume.length === 100, `${label}: buy_volume present`);
    for (let i = 0; i < bars.n_bars; i++) {
        check(bars.buy_volume[i] >= 0 && bars.buy_volume[i] <= bars.volume[i], `${label}: 0 <= buy_volume <= volume (bar ${i})`);
    }
    // 2 of every 3 ticks are buys, sizes cycle 1..4: buys are a large but not complete share
    check(bars.buy_volume[0] > 0.5 * bars.volume[0] && bars.buy_volume[0] < bars.volume[0], `${label}: buy share plausible`);
}

function checkHealth(h, label) {
    check(h.count === 6000n, `${label}: count ${h.count}`);
    check(h.n_gaps === 0n, `${label}: n_gaps ${h.n_gaps}`);
    check(h.median_gap === 1_000_000 && h.mean_gap === 1_000_000, `${label}: median/mean gap ${h.median_gap} / ${h.mean_gap}`);
    check(h.max_gap === 1_000_000n, `${label}: max gap ${h.max_gap}`);
    check(h.n_outlier_returns === 0n, `${label}: n_outlier_returns ${h.n_outlier_returns}`);
    check(h.first_ts === 0n && h.last_ts === 5_999_000_000n && h.span === 5_999_000_000n, `${label}: first/last/span ${h.first_ts} ${h.last_ts} ${h.span}`);
    check(h.n_nonpositive_price === 0n && h.n_nan_price === 0n && h.n_zero_volume === 0n && h.n_negative_volume === 0n, `${label}: price / volume counters`);
    check(Number.isFinite(h.max_abs_return) && h.max_abs_return > 0 && h.max_abs_return < 0.05, `${label}: max_abs_return ${h.max_abs_return}`);
    check(Object.keys(h).length === 19, `${label}: 19 health fields, got ${Object.keys(h).length}`);
}

function checkEvaluation(ev, label) {
    check(ev.n_decisions === 4n && ev.n_evaluated === 2n && ev.n_long === 2n && ev.n_short === 1n, `${label}: counts ${ev.n_decisions} ${ev.n_evaluated} ${ev.n_long} ${ev.n_short}`);
    for (const k of ['entry', 'exit', 'net_return']) check(ev[k] instanceof Float64Array && ev[k].length === 4, `${label}: ${k} is a Float64Array of 4`);
    check(Number.isFinite(ev.net_return[0]) && Number.isFinite(ev.net_return[1]), `${label}: decisions 0 and 1 evaluated`);
    check(Number.isNaN(ev.net_return[2]) && Number.isNaN(ev.net_return[3]), `${label}: decisions 2 (beyond the data) and 3 (flat) not evaluated`);
    check(Number.isNaN(ev.entry[3]) && Number.isNaN(ev.exit[2]), `${label}: entry / exit NaN where not evaluated`);
    check(Math.abs(ev.net_return[0] - (ev.exit[0] / ev.entry[0] - 1 - 0.001)) < 1e-12, `${label}: net return = gross - 2 x 5 bps`);
    check(Math.abs(ev.net_return[1] - (1 - ev.exit[1] / ev.entry[1] - 0.001)) < 1e-12, `${label}: short net return`);
    check(Number.isFinite(ev.hit_rate) && ev.hit_rate >= 0 && ev.hit_rate <= 1, `${label}: hit rate ${ev.hit_rate}`);
    check(Math.abs(ev.total_cost - (1000 + 500) * 0.001) < 1e-9, `${label}: total cost ${ev.total_cost}`);
    check(Object.keys(ev).length === 20 + 3, `${label}: 20 evaluation fields + 3 arrays, got ${Object.keys(ev).length}`);
}

function sameSnapshot(a, b, label) {
    const ka = Object.keys(a), kb = Object.keys(b);
    check(ka.length === kb.length && ka.length >= 90, `${label}: same field count (${ka.length} vs ${kb.length})`);
    for (const k of ka) {
        const va = a[k], vb = b[k];
        check(Object.is(va, vb) || va === vb, `${label}: field ${k} differs (${va} vs ${vb})`);
    }
}

async function main() {
    rmrf(DATA_DIR);
    const db = hocdb.dbInit(TICKER, DATA_DIR, schema);

    console.log("Appending 3000 bars...");
    let p = 100.0;
    for (let i = 0; i < N; i++) {
        const o = p;
        p *= Math.exp((lcg() - 0.5) * 0.02);
        db.append({
            timestamp: tsAt(i),
            open: o,
            high: Math.max(o, p) * 1.003,
            low: Math.min(o, p) * 0.997,
            close: p,
            volume: 1000.0 + (i % 50)
        });
        closes[i] = p;
    }
    lastClose = p;
    db.flush();

    // --- registry ---
    console.log("Testing registry...");
    const kinds = hocdb.indicatorKinds();
    check(kinds.length === 83, `83 kinds, got ${kinds.length}`);
    check(kinds.includes('rsi') && kinds.includes('heikin_ashi'), "kind names present");
    check(kinds.includes('order_flow') && kinds.includes('pivots') && kinds.includes('series2'), "round-2 kind names present");
    check(hocdb.INDICATOR_KINDS.rsi === 20 && hocdb.INDICATOR_KINDS.macd === 21, "INDICATOR_KINDS constants");
    check(hocdb.INDICATOR_KINDS.spread === 130 && hocdb.INDICATOR_KINDS.realized_vol === 135 && hocdb.INDICATOR_KINDS.series === 140
        && hocdb.INDICATOR_KINDS.rel_strength === 144 && hocdb.INDICATOR_KINDS.forward_return === 150 && hocdb.INDICATOR_KINDS.triple_barrier === 151
        && hocdb.INDICATOR_KINDS.session_vwap === 160 && hocdb.INDICATOR_KINDS.pivots === 163, "round-2 kind ids");
    check(hocdb.indicatorKindId('MACD') === 21, "kind name is case-insensitive");
    check(JSON.stringify(hocdb.indicatorOutputs('macd')) === '["macd","signal","hist"]', "macd output names");
    check(JSON.stringify(hocdb.indicatorOutputs(63)) === '["upper","middle","lower","percent_b","bandwidth"]', "bbands output names by id");
    check(JSON.stringify(hocdb.indicatorOutputs('pivots')) === '["pp","r1","s1","r2","s2"]', "pivots output names");
    check(hocdb.indicatorIsLookahead('forward_return') === true && hocdb.indicatorIsLookahead('triple_barrier') === true, "labels are look-ahead");
    check(hocdb.indicatorIsLookahead('sma') === false && hocdb.indicatorIsLookahead(160) === false, "sma / session_vwap are not look-ahead");
    check(hocdb.indicatorWarmup({ kind: 'ema', period: 200 }) > 200, "warmup > period for EMA");
    check(hocdb.indicatorWarmup({ kind: 'sma', period: 20 }) === 19, "warmup for SMA 20 is 19");
    expectThrow(() => hocdb.indicatorKindId('nope'), "unknown kind name throws");
    expectThrow(() => hocdb.indicatorIsLookahead('nope'), "unknown kind name throws (lookahead)");
    expectThrow(() => hocdb.indicatorWarmup({ kind: 'sma', period: -1 }), "negative period throws");

    // --- batch over a range ---
    console.log("Testing indicators() over a range...");
    const specs = [
        { kind: 'sma', period: 20 },
        { kind: 'macd' },
        { kind: 'rsi', period: 14 },
        { kind: 'bbands', period: 20, param: 2.0 },
        { kind: 'atr', period: 14 },
        { kind: 'obv' },
        { kind: 'sma', period: 10, field: 'volume', label: 'vol_sma' },
    ];
    const start = tsAt(1000), end = tsAt(1500);
    const res = db.indicators(specs, { start, end });
    check(res.n_rows === 500, `500 rows, got ${res.n_rows}`);
    check(res.n_outputs === 13, `13 outputs, got ${res.n_outputs}`);
    const expectedNames = ['sma_20', 'macd', 'macd_signal', 'macd_hist', 'rsi_14',
        'bbands_20_upper', 'bbands_20_middle', 'bbands_20_lower', 'bbands_20_percent_b', 'bbands_20_bandwidth',
        'atr_14', 'obv', 'vol_sma'];
    check(JSON.stringify(res.names) === JSON.stringify(expectedNames), `column names: ${res.names}`);
    for (const name of expectedNames) {
        check(res[name] instanceof Float64Array && res[name].length === 500, `column ${name} is a Float64Array of 500`);
    }
    check(res.timestamps instanceof BigInt64Array && res.timestamps.length === 500, "timestamps is a BigInt64Array");
    check(res.timestamps[0] === start, "first timestamp == start");
    check(res.timestamps[499] === end - 60n, "last timestamp == end - 60");
    check(res.values instanceof Float64Array && res.values.length === 13 * 500, "planar values buffer");
    for (let i = 0; i < 500; i++) {
        check(!Number.isNaN(res.sma_20[i]), `sma_20 converged with lookback auto (row ${i})`);
        check(res.rsi_14[i] >= 0 && res.rsi_14[i] <= 100, `rsi in [0,100] (row ${i})`);
        check(res.bbands_20_upper[i] >= res.bbands_20_middle[i] && res.bbands_20_middle[i] >= res.bbands_20_lower[i], `bbands ordered (row ${i})`);
        check(res.atr_14[i] > 0, `atr positive (row ${i})`);
        check(!Number.isNaN(res.obv[i]), `obv defined (row ${i})`);
    }
    // vol_sma is an SMA(10) of the volume field: volume is 1000 + i % 50
    for (const r of [0, 250, 499]) {
        const g = 1000 + r;
        let sum = 0;
        for (let j = g - 9; j <= g; j++) sum += 1000 + (j % 50);
        check(Math.abs(res.vol_sma[r] - sum / 10) < 1e-9, `vol_sma matches reference at row ${r}`);
    }

    // sma_20 against a local reference (mean of the 20 closes ending at the row)
    for (const r of [0, 1, 100, 250, 499]) {
        const g = 1000 + r;
        let sum = 0;
        for (let j = g - 19; j <= g; j++) sum += closes[j];
        const ref = sum / 20;
        check(Math.abs(res.sma_20[r] - ref) <= 1e-9 * Math.abs(ref), `sma_20 matches reference at row ${r}: ${res.sma_20[r]} vs ${ref}`);
    }

    // explicit lookback 0 -> NaN warm-up inside the window
    const res0 = db.indicators([{ kind: 'sma', period: 20 }], { start, end, lookback: 0 });
    check(res0.n_rows === 500, "lookback 0 rows");
    for (let i = 0; i < 19; i++) check(Number.isNaN(res0.sma_20[i]), `sma_20 NaN in warm-up (row ${i})`);
    check(!Number.isNaN(res0.sma_20[19]), "sma_20 defined at row 19");

    // no window at all -> everything; string shorthand for specs
    const all = db.indicators(['rsi']);
    check(all.n_rows === N && all.rsi.length === N, "indicators() without a window covers everything");

    // --- tail ---
    console.log("Testing indicatorsTail()...");
    const tail = db.indicatorsTail(5, specs.slice(0, 3));
    check(tail.n_rows === 5 && tail.n_outputs === 5, "tail rows/outputs");
    check(tail.timestamps[4] === tsAt(N - 1), "tail ends at the last timestamp");
    const tailOpt = db.indicators(specs.slice(0, 3), { tail: 5 });
    check(tailOpt.n_rows === 5 && tailOpt.timestamps[4] === tsAt(N - 1), "options.tail works too");

    // --- bucket (tick -> 5-minute bars) ---
    const bars = db.indicatorsTail(10, [{ kind: 'sma', period: 20 }], { bucket: 300 });
    check(bars.n_rows === 10, `10 bars, got ${bars.n_rows}`);
    check(bars.timestamps[1] - bars.timestamps[0] === 300n, "bar spacing");
    check(bars.timestamps[0] % 300n === 0n, "bar alignment");

    // --- errors ---
    console.log("Testing errors...");
    expectThrow(() => db.indicators([{ kind: 'nope' }], { tail: 10 }), "unknown kind name");
    expectThrow(() => db.indicators([{ kind: 9999 }], { tail: 10 }), "unknown kind id");
    expectThrow(() => db.indicators([{ kind: 'sma' }], { tail: 10, columns: { open: 'open' } }), "missing close column");
    expectThrow(() => db.indicators([{ kind: 'atr' }], { tail: 10, columns: { close: 'close' } }), "atr with only close");
    expectThrow(() => db.indicators([{ kind: 'sma', field: 'nope' }], { tail: 10 }), "invalid field name");
    expectThrow(() => db.indicators([{ kind: 'sma', field: 42 }], { tail: 10 }), "invalid field index");
    expectThrow(() => db.indicators([{ kind: 'sma', field: 'volume' }], { tail: 10, bucket: 300 }), "field override with bucket");
    expectThrow(() => db.indicators([], { tail: 10 }), "empty specs");
    expectThrow(() => db.indicators([{ kind: 'sma', period: 20 }, { kind: 'sma', period: 20 }], { tail: 10 }), "duplicate column names");
    expectThrow(() => db.indicators([{ kind: 'sma' }], { tail: 10, lookback: 'lots' }), "bad lookback");
    expectThrow(() => db.indicators(['spread'], { tail: 10 }), "spread needs bid / ask columns");

    // --- ohlcv ---
    console.log("Testing ohlcv()...");
    const ohlcv = db.ohlcv(INT64_MIN, INT64_MAX, 300);
    check(ohlcv.n_bars > 500, `> 500 bars, got ${ohlcv.n_bars}`);
    check(ohlcv.timestamps.length === ohlcv.n_bars && ohlcv.close.length === ohlcv.n_bars, "ohlcv array lengths");
    for (let i = 0; i < ohlcv.n_bars; i++) {
        check(ohlcv.high[i] >= ohlcv.low[i], `bar high >= low (${i})`);
        check(ohlcv.close[i] <= ohlcv.high[i] && ohlcv.close[i] >= ohlcv.low[i], `close within bar (${i})`);
        check(ohlcv.count[i] >= 1, `bar count (${i})`);
        check(ohlcv.volume[i] >= 1000 * ohlcv.count[i], `bar volume is summed (${i})`);
    }
    check(ohlcv.buy_volume === undefined && !('buy_volume' in ohlcv), "no buy_volume without a side field");
    const ohlcvNamed = db.ohlcv(null, null, 300n, { price: 'close', volume: 'volume' });
    check(ohlcvNamed.n_bars === ohlcv.n_bars, "ohlcv with explicit field names");
    expectThrow(() => db.ohlcv(INT64_MIN, INT64_MAX, 0), "ohlcv needs bucket > 0");
    expectThrow(() => db.ohlcv(INT64_MIN, INT64_MAX, 300, { price: 'nope' }), "ohlcv invalid price field");
    expectThrow(() => db.ohlcv(INT64_MIN, INT64_MAX, 300, { side: 'nope' }), "ohlcv invalid side field");

    // --- summary ---
    console.log("Testing summary()...");
    const sum = db.summary(INT64_MIN, INT64_MAX, 'close', 252);
    check(sum.count === BigInt(N), `summary count ${sum.count}`);
    check(sum.max_drawdown <= 0 && sum.max_drawdown >= -1, "max drawdown range");
    check(sum.win_rate >= 0 && sum.win_rate <= 1, "win rate range");
    check(Number.isFinite(sum.sharpe) && Number.isFinite(sum.hurst), "sharpe / hurst computed");
    check(Object.keys(sum).length === 29, `29 summary fields, got ${Object.keys(sum).length}`);
    check(Math.abs(sum.last - lastClose) < 1e-9, "summary last == last close");
    expectThrow(() => db.summary(INT64_MIN, INT64_MAX, 'nope'), "summary invalid field");

    // --- snapshot ---
    console.log("Testing snapshot()...");
    const snap = db.snapshot({ periodsPerYear: 252 });
    check(Object.keys(snap).length >= 90, `>= 90 snapshot fields, got ${Object.keys(snap).length}`);
    check(snap.bars === 2500n, `snapshot bars ${snap.bars}`);
    check(snap.timestamp === tsAt(N - 1), "snapshot timestamp is the last one");
    check(snap.rsi_14 >= 0 && snap.rsi_14 <= 100, "snapshot rsi");
    check(Number.isFinite(snap.ema_200) && Number.isFinite(snap.adx_14) && Number.isFinite(snap.mfi_14) && Number.isFinite(snap.supertrend), "snapshot fields finite");
    check(Math.abs(snap.close - lastClose) < 1e-9, "snapshot close is the latest close");
    const snapBucket = db.snapshot({ bars: 50, bucket: 300, periodsPerYear: 252 });
    check(snapBucket.bars === 50n, "snapshot with bucket: bars");
    check(Number.isNaN(snapBucket.sma_200) && Number.isFinite(snapBucket.sma_20), "snapshot with bucket: sma_200 NaN, sma_20 finite");
    expectThrow(() => db.snapshot({ columns: { open: 'open' } }), "snapshot without close");

    // --- health / evaluate / snapshotMulti also work on the bar database ---
    const hb = db.health(null, null, 'close', 'volume', 120n, 0.05);
    check(hb.count === BigInt(N) && hb.n_gaps === 0n && hb.median_gap === 60, `bar db health: ${hb.count} ${hb.n_gaps} ${hb.median_gap}`);
    const evb = db.evaluate([{ timestamp: tsAt(10), direction: 1, horizon: 600 }], { costBps: 1 });
    check(evb.n_evaluated === 1n && Number.isFinite(evb.net_return[0]), "bar db evaluate (priceField defaults to close)");
    const smb = db.snapshotMulti({ buckets: [300n, 900n], periodsPerYear: 252, bars: 50 });
    check(smb.length === 2 && smb[0].bars === 50n && smb[1].bars === 50n, "bar db snapshotMulti");
    sameSnapshot(smb[0], snapBucket, "bar db snapshotMulti[0] == snapshot(bars 50, bucket 300)");

    db.close();

    // ------------------------------------------------------------------
    // Round 2: microstructure, pairs, labels, sessions, health, evaluation
    // ------------------------------------------------------------------
    console.log("Appending 6000 ticks to A and 3000 to B...");
    const ta = hocdb.dbInit(TICKER_A, DATA_DIR, tickSchema);
    const tb = hocdb.dbInit(TICKER_B, DATA_DIR, tickSchema);
    let pa = 100.0, pb = 50.0;
    const aPrices = new Array(NT);
    for (let i = 0; i < NT; i++) {
        pa *= Math.exp((lcg() - 0.5) * 0.004);
        pb *= Math.exp((lcg() - 0.5) * 0.004);
        aPrices[i] = pa;
        ta.append({ timestamp: SEC * BigInt(i), price: pa, size: 1 + (i % 4), bid: pa * 0.999, ask: pa * 1.001, side: (i % 3) !== 0 });
        if (i % 2 === 0) { // B trades every 2 seconds, offset by 300 ms
            tb.append({ timestamp: SEC * BigInt(i) + 300_000n, price: pb, size: 2, bid: pb * 0.999, ask: pb * 1.001, side: (i % 2) !== 0 });
        }
    }
    ta.flush();
    tb.flush();

    // --- microstructure / session / label kinds on ticks (columns auto-detected: price, size, bid, ask, side) ---
    console.log("Testing microstructure indicators on ticks...");
    const micro = ta.indicatorsTail(100, microSpecs);
    checkMicro(micro, "tick tail");
    const microExplicit = ta.indicatorsTail(100, microSpecs, { columns: { close: 'price', volume: 'size', bid: 3, ask: 4n, side: 'side' } });
    checkMicro(microExplicit, "tick tail with explicit columns");
    check(micro.spread_abs[0] === microExplicit.spread_abs[0] && micro.session_vwap[50] === microExplicit.session_vwap[50], "auto-detected columns == explicit columns");
    expectThrow(() => ta.indicatorsTail(10, ['session_vwap']), "session_vwap without param (session length) is a validation error");
    expectThrow(() => ta.indicatorsTail(10, [{ kind: 'pivots', param: 0 }]), "pivots without a session length");
    expectThrow(() => ta.indicatorsTail(10, ['spread'], { columns: { close: 'price' } }), "spread without bid / ask");
    // order flow on 10-second bars uses the side role (bucket > 0)
    const flowBars = ta.indicatorsTail(20, [{ kind: 'order_flow', period: 5 }], { bucket: 10n * SEC });
    check(flowBars.n_rows === 20 && Number.isFinite(flowBars.order_flow_5_net[19]), "order_flow on bars via the side role");
    // triple barrier labels: -1 / 0 / +1
    const tbl = ta.indicatorsTail(200, [{ kind: 'triple_barrier', period: 20, param: 0.01 }]);
    check(tbl.n_rows === 200 && [-1, 0, 1].includes(tbl.triple_barrier_20_label[0]) && Number.isNaN(tbl.triple_barrier_20_label[199]), "triple_barrier labels");

    // --- pairs: as-of join on ticks, inner join on 10-second bars ---
    console.log("Testing pairIndicators()...");
    const pairTail = ta.pairIndicators(tb, pairSpecs, { tail: 50 });
    checkPairTail(pairTail, "pair tail");
    check(pairTail.timestamps[49] === SEC * BigInt(NT - 1), "pair tail ends at A's last tick");
    // series2 is the latest B price at or before each A row
    const bLast = tb.load().slice(-1)[0];
    check(Math.abs(pairTail.series2[49] - bLast.price) < 1e-12, "series2 is B's last price (as-of join)");
    const pairRange = ta.pairIndicators(tb, pairSpecs, { start: 1_000_000_000n, end: 2_000_000_000n, bucket: 10n * SEC });
    checkPairRange(pairRange, "pair range on 10 s bars");
    const pairRange0 = ta.pairIndicators(tb, pairSpecs, { start: 1_000_000_000n, end: 2_000_000_000n, bucket: 10n * SEC, lookback: 0 });
    checkPairRange(pairRange0, "pair range on 10 s bars, lookback 0");
    check(Math.abs(pairRange.ratio[50] - pairRange0.ratio[50]) < 1e-12, "ratio does not depend on lookback");
    const pairCols = ta.pairIndicators(tb, ['ratio'], { tail: 5, columns: { close: 'price' }, columns2: { close: 'bid' } });
    check(pairCols.n_rows === 5 && Math.abs(pairCols.ratio[4] - pairTail.series[49] / (bLast.bid)) < 1e-9, "columns2 selects the other db's close (bid)");
    const pairCols2 = ta.pairIndicators(tb, ['ratio'], { tail: 5, otherColumns: { close: 'bid' } });
    check(pairCols2.ratio[4] === pairCols.ratio[4], "otherColumns is an alias of columns2");
    const selfPair = ta.pairIndicators(ta, ['ratio'], { tail: 3 });
    check(Math.abs(selfPair.ratio[2] - 1) < 1e-12, "pairing a database with itself gives ratio 1");
    expectThrow(() => ta.pairIndicators(null, pairSpecs, { tail: 5 }), "pairIndicators without other");
    expectThrow(() => ta.pairIndicators({ foo: 1 }, pairSpecs, { tail: 5 }), "pairIndicators with a non-database other");
    expectThrow(() => ta.pairIndicators(tb, pairSpecs, { tail: 5, columns2: { open: 'price' } }), "columns2 without close");
    expectThrow(() => ta.pairIndicators(tb, [], { tail: 5 }), "pair with empty specs");

    // --- ohlcv with side -> buy_volume ---
    console.log("Testing ohlcv() with side...");
    const bx = ta.ohlcv(null, null, 60n * SEC, { price: 'price', volume: 'size', side: 'side' });
    checkBarsWithSide(bx, "ohlcv with side");
    const bxAuto = ta.ohlcv(INT64_MIN, INT64_MAX, 60n * SEC, { side: 5 });
    check(bxAuto.n_bars === 100 && bxAuto.volume[3] === bx.volume[3] && bxAuto.buy_volume[3] === bx.buy_volume[3], "price / volume (size) auto-detected on the tick schema");
    const bNo = ta.ohlcv(null, null, 60n * SEC);
    check(bNo.n_bars === 100 && bNo.buy_volume === undefined && !('buy_volume' in bNo), "no buy_volume without side");
    check(bNo.volume[0] === bx.volume[0] && bNo.volume[0] > bNo.count[0], "volume is the summed size, not the count");

    // --- health ---
    console.log("Testing health()...");
    const hl = ta.health(INT64_MIN, INT64_MAX, 'price', 'size', 5n * SEC, 0.05);
    checkHealth(hl, "health");
    const hlOpts = ta.health(null, null, { gapThreshold: 5_000_000, outlierThreshold: 0.05 });
    checkHealth(hlOpts, "health with an options object (price / volume auto-detected)");
    const hlGaps = ta.health(null, null, 'price', null, 500_000n, 0.001);
    check(hlGaps.n_gaps === 5999n && hlGaps.n_outlier_returns > 0n && hlGaps.first_outlier_at > 0n, `tight thresholds count gaps / outliers: ${hlGaps.n_gaps} ${hlGaps.n_outlier_returns}`);
    const hlWin = ta.health(1000n * SEC, 2000n * SEC, 'price');
    check(hlWin.count === 1000n && hlWin.first_ts === 1000n * SEC && hlWin.last_ts === 1999n * SEC, "health over a window");
    expectThrow(() => ta.health(null, null, 'nope'), "health invalid price field");
    expectThrow(() => ta.health(null, null, 'price', 'size', -1), "health negative gap threshold");

    // --- evaluate ---
    console.log("Testing evaluate()...");
    const ev = ta.evaluate(decisions, evalOpts);
    checkEvaluation(ev, "evaluate");
    const evNum = ta.evaluate(decisions.map(d => ({ ...d, timestamp: Number(d.timestamp), horizon: d.horizon === undefined ? undefined : Number(d.horizon) })), { ...evalOpts, defaultHorizon: 120e6 });
    check(evNum.net_return[0] === ev.net_return[0] && evNum.total_pnl === ev.total_pnl, "number timestamps / horizons give the same evaluation");
    const evEmpty = ta.evaluate([], { priceField: 'price', defaultHorizon: 1, costBps: 0 });
    check(evEmpty.n_decisions === 0n && evEmpty.n_evaluated === 0n && Number.isNaN(evEmpty.hit_rate), "empty evaluation");
    check(evEmpty.entry.length === 0 && evEmpty.net_return.length === 0, "empty evaluation arrays");
    const evNoCost = ta.evaluate(decisions.slice(0, 1), { priceField: 'price' });
    check(Math.abs(evNoCost.net_return[0] - (evNoCost.exit[0] / evNoCost.entry[0] - 1)) < 1e-12 && evNoCost.total_cost === 0, "no costs by default");
    expectThrow(() => ta.evaluate([{ direction: 1 }], evalOpts), "decision without timestamp");
    expectThrow(() => ta.evaluate([{ timestamp: 1n }], evalOpts), "decision without direction");
    expectThrow(() => ta.evaluate(decisions, { priceField: 'nope' }), "evaluate invalid price field");
    expectThrow(() => ta.evaluate({}, evalOpts), "decisions must be an array");

    // --- snapshotMulti ---
    console.log("Testing snapshotMulti()...");
    const multi = ta.snapshotMulti({ buckets: [60n * SEC, 300n * SEC], periodsPerYear: [525600, 105120], bars: 50 });
    check(Array.isArray(multi) && multi.length === 2, "two snapshots");
    const single = ta.snapshot({ bars: 50, bucket: 60n * SEC, periodsPerYear: 525600 });
    sameSnapshot(multi[0], single, "snapshotMulti[0] == snapshot(bars 50, bucket 60 s)");
    check(multi[0].bars === 50n && multi[1].bars === 20n, `bars 50 / 20 (only 100 minutes of data), got ${multi[0].bars} / ${multi[1].bars}`);
    check(multi[1].timestamp === 5700n * SEC && multi[0].timestamp === 5940n * SEC, "snapshot timestamps are the last bar starts");
    const multiOne = ta.snapshotMulti({ buckets: [60n * SEC], bars: 50 });
    check(multiOne.length === 1 && Number.isNaN(multiOne[0].sharpe_20) === Number.isNaN(single.sharpe_20), "single bucket, default periodsPerYear");
    expectThrow(() => ta.snapshotMulti({ buckets: [] }), "snapshotMulti needs buckets");
    expectThrow(() => ta.snapshotMulti({ buckets: [0] }), "snapshotMulti bucket must be > 0");
    expectThrow(() => ta.snapshotMulti({ buckets: [60n * SEC, 300n * SEC], periodsPerYear: [1] }), "periodsPerYear length mismatch");
    expectThrow(() => ta.snapshotMulti({ buckets: [60n * SEC], columns: { open: 'price' } }), "snapshotMulti without close");

    ta.close();
    tb.close();

    // --- async API (worker thread) on the same data ---
    console.log("Testing async API...");
    const adb = await hocdb.dbInitAsync(TICKER, DATA_DIR, schema);
    try {
        const atail = await adb.indicatorsTail(5, [{ kind: 'rsi' }, { kind: 'macd', label: 'm' }]);
        check(atail.n_rows === 5 && atail.rsi instanceof Float64Array && atail.m_signal instanceof Float64Array, "async indicatorsTail");
        check(atail.timestamps[4] === tsAt(N - 1), "async tail last timestamp");
        const arange = await adb.indicators([{ kind: 'sma', period: 20 }], { start, end });
        check(arange.n_rows === 500 && Math.abs(arange.sma_20[0] - res.sma_20[0]) < 1e-12, "async indicators matches sync");
        const asnap = await adb.snapshot();
        check(asnap.bars === 2500n && Math.abs(asnap.close - lastClose) < 1e-9, "async snapshot");
        const asum = await adb.summary(INT64_MIN, INT64_MAX, 'close', 252);
        check(asum.count === BigInt(N), "async summary");
        const abars = await adb.ohlcv(INT64_MIN, INT64_MAX, 300);
        check(abars.n_bars === ohlcv.n_bars, "async ohlcv");
        await expectReject(adb.indicators([{ kind: 'nope' }], { tail: 5 }), "async unknown kind rejects");
        await expectReject(adb.summary(INT64_MIN, INT64_MAX, 'nope'), "async invalid field rejects");
    } finally {
        await adb.close();
    }

    // async round 2: both legs of a pair must live on one worker (openAsync)
    console.log("Testing async round-2 API...");
    const aa = await hocdb.dbInitAsync(TICKER_A, DATA_DIR, tickSchema);
    let ab = null;
    try {
        ab = await aa.openAsync(TICKER_B, DATA_DIR, tickSchema);
        check(ab._worker === aa._worker && ab._dbId !== aa._dbId, "openAsync shares the worker");
        checkMicro(await aa.indicatorsTail(100, microSpecs), "async tick tail");
        checkPairTail(await aa.pairIndicators(ab, pairSpecs, { tail: 50 }), "async pair tail");
        checkPairRange(await aa.pairIndicators(ab, pairSpecs, { start: 1_000_000_000n, end: 2_000_000_000n, bucket: 10n * SEC }), "async pair range");
        // B / A: B's last row (5998.3 s) is as-of joined with A's tick at 5998 s
        const abSelf = await ab.pairIndicators(aa, ['ratio'], { tail: 3 });
        check(abSelf.n_rows === 3 && abSelf.timestamps[2] === 5998n * SEC + 300_000n, "pair in the other direction: B's rows");
        check(Math.abs(abSelf.ratio[2] - bLast.price / aPrices[5998]) < 1e-12, "pair in the other direction: B / A as-of ratio");
        checkBarsWithSide(await aa.ohlcv(null, null, 60n * SEC, { side: 'side' }), "async ohlcv with side");
        const abNo = await aa.ohlcv(null, null, 60n * SEC);
        check(abNo.buy_volume === undefined, "async ohlcv without side has no buy_volume");
        checkHealth(await aa.health(null, null, 'price', 'size', 5n * SEC, 0.05), "async health");
        checkHealth(await aa.health(null, null, { gapThreshold: 5n * SEC, outlierThreshold: 0.05 }), "async health (options object)");
        checkEvaluation(await aa.evaluate(decisions, evalOpts), "async evaluate");
        const amulti = await aa.snapshotMulti({ buckets: [60n * SEC, 300n * SEC], periodsPerYear: [525600, 105120], bars: 50 });
        check(amulti.length === 2 && amulti[1].bars === 20n, "async snapshotMulti");
        sameSnapshot(amulti[0], multi[0], "async snapshotMulti == sync");
        await expectReject(aa.pairIndicators({ _db: 1, _fieldOffsets: {} }, pairSpecs, { tail: 5 }), "async pair with a sync database rejects");
        await expectReject(aa.pairIndicators(ab, ['session_vwap'], { tail: 5 }), "async session kind without param rejects");
        await expectReject(aa.evaluate([{ timestamp: 1n }], evalOpts), "async decision without direction rejects");
        await expectReject(aa.snapshotMulti({ buckets: [] }), "async snapshotMulti without buckets rejects");
        // closing one leg keeps the worker (and the other leg) alive
        await ab.close();
        ab = null;
        const after = await aa.snapshot({ bars: 50, bucket: 60n * SEC, periodsPerYear: 525600 });
        sameSnapshot(after, single, "A still works after closing B");
        await expectReject(aa.pairIndicators({ _worker: aa._worker, _dbId: 12345 }, pairSpecs, { tail: 5 }), "pair with a closed / unknown database rejects");
    } finally {
        if (ab) await ab.close();
        await aa.close();
    }
}

main().then(() => {
    rmrf(DATA_DIR);
    console.log("Node.js Indicators Test Passed!");
}).catch((err) => {
    console.error(err);
    rmrf(DATA_DIR);
    process.exit(1);
});
