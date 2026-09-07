// Round 4: trading calendars, signal backtester and universe features.
//   node bindings/node/test/test_round4.js
const fs = require('fs');
const path = require('path');
const hocdb = require('../index.js');

const DATA_DIR = 'b_node_test_round4';
const US = 1_000_000;
let failures = 0;

function check(cond, msg) {
    if (cond) console.log(`  ok: ${msg}`);
    else { console.log(`  FAIL: ${msg}`); failures++; }
}

function near(a, b, tol = 1e-9) {
    const x = Number(a), y = Number(b);
    if (Number.isNaN(x) || Number.isNaN(y)) return Number.isNaN(x) && Number.isNaN(y);
    return Math.abs(x - y) <= tol + 1e-9 * Math.abs(y);
}

function expectError(fn, msg, needle) {
    try {
        fn();
    } catch (e) {
        if (needle && !String(e.message).includes(needle)) { console.log(`  FAIL: ${msg} -> wrong message: ${e.message}`); failures++; }
        else console.log(`  ok: ${msg} -> ${e.message.split('\n')[0]}`);
        return;
    }
    console.log(`  FAIL: ${msg} (no error thrown)`);
    failures++;
}

const utc = (y, m, d, hh = 0, mm = 0) => hocdb.daysFromCivil(y, m, d) * 86400 + hh * 3600 + mm * 60;

const SCHEMA = [
    { name: 'timestamp', type: 'i64' }, { name: 'open', type: 'f64' }, { name: 'high', type: 'f64' },
    { name: 'low', type: 'f64' }, { name: 'close', type: 'f64' }, { name: 'volume', type: 'f64' },
];
const COLUMNS = { open: 'open', high: 'high', low: 'low', close: 'close', volume: 'volume' };

function testCalendars() {
    console.log('Trading calendars...');
    check(hocdb.calendarId('nyse') === 3 && hocdb.calendarId('LSE') === 5 && hocdb.calendarId('nope') === 0, 'calendar ids by name');
    check(hocdb.calendarName(1) === 'crypto' && hocdb.calendarName(99) === null, 'calendar names');
    check(hocdb.CALENDAR.NYSE === 3 && hocdb.CALENDAR.CME === 6, 'CALENDAR constants');
    const fri = utc(2025, 9, 5, 15, 0);
    const s = hocdb.calendarSession('nyse', fri);
    check(s && Number(s.open) === utc(2025, 9, 5, 13, 30) && Number(s.close) === utc(2025, 9, 5, 20, 0)
        && s.earlyClose === false && Number(s.tradeDay) === hocdb.daysFromCivil(2025, 9, 5), 'NYSE Friday session 13:30-20:00 UTC');
    check(hocdb.calendarIsOpen('nyse', fri) && !hocdb.calendarIsOpen('nyse', utc(2025, 9, 6, 12, 0)), 'isOpen');
    const sat = utc(2025, 9, 6, 12, 0);
    check(hocdb.calendarSession('nyse', sat) === null, 'Saturday has no session');
    check(Number(hocdb.calendarSession('nyse', sat, 1).tradeDay) === hocdb.daysFromCivil(2025, 9, 5), 'previous session is Friday');
    check(Number(hocdb.calendarSession('nyse', sat, 2).tradeDay) === hocdb.daysFromCivil(2025, 9, 8), 'next session is Monday');
    check(hocdb.calendarSessionForDay('nyse', hocdb.daysFromCivil(2025, 7, 4)) === null, 'Independence Day is closed');
    const bf = hocdb.calendarSessionForDay('nyse', hocdb.daysFromCivil(2025, 11, 28));
    check(bf.earlyClose === true && Number(bf.close) === utc(2025, 11, 28, 18, 0), 'Black Friday early close');
    check(Number(hocdb.calendarOpenSeconds('nyse', utc(2025, 8, 29, 15, 0), utc(2025, 9, 2, 15, 0))) === 5 * 3600 + 5400,
        'trading seconds across the Labor Day weekend');
    check(Number(hocdb.calendarSessionsBetween('nyse', utc(2025, 1, 1), utc(2026, 1, 1))) === 250, '250 NYSE sessions in 2025');
    check(near(hocdb.calendarPeriodsPerYear('nyse', 60), 252 * 390) && near(hocdb.calendarPeriodsPerYear('crypto', 86400), 365),
        'periods per year');
    check(Number(hocdb.calendarToLocal('nyse', fri)) === utc(2025, 9, 5, 11, 0), 'UTC -> New York local');
    const c = hocdb.civilFromDays(hocdb.daysFromCivil(2024, 2, 29));
    check(Number(c.year) === 2024 && c.month === 2 && c.day === 29, 'civil date round trip');
    check(Number(hocdb.calendarSessionForDay('fx', hocdb.daysFromCivil(2025, 9, 8)).open) === utc(2025, 9, 7, 21, 0),
        'FX Monday opens Sunday 17:00 New York');
    expectError(() => hocdb.calendarSession('not_a_calendar', fri), 'unknown calendar name', 'UnknownCalendar');

    const weekly = [0, 1, 2, 3].map(() => ({ openSec: 10 * 3600, closeSec: 15 * 3600 })).concat([null, null, null]);
    const cid = hocdb.calendarDefine('node_custom', weekly, 9 * 3600, 'none', [hocdb.daysFromCivil(2025, 9, 9)],
        [{ day: hocdb.daysFromCivil(2025, 9, 10), closeSec: 12 * 3600 }], 200);
    check(cid >= 32 && hocdb.calendarId('node_custom') === cid, 'custom calendar registered');
    check(Number(hocdb.calendarSessionForDay(cid, hocdb.daysFromCivil(2025, 9, 8)).open) === utc(2025, 9, 8, 1, 0), 'custom Monday 10:00 UTC+9');
    check(hocdb.calendarSessionForDay(cid, hocdb.daysFromCivil(2025, 9, 9)) === null, 'custom holiday');
    check(Number(hocdb.calendarSessionForDay(cid, hocdb.daysFromCivil(2025, 9, 10)).close) === utc(2025, 9, 10, 3, 0), 'custom early close');
    check(hocdb.calendarSessionForDay(cid, hocdb.daysFromCivil(2025, 9, 12)) === null, 'custom week has no Friday');
    expectError(() => hocdb.calendarDefine('bad', [null], 0), 'calendarDefine with a short weekly template');
}

function fillSessions(db, days, seed = 1.0) {
    let p = 100.0 * seed;
    const firstOpen = {}, perDay = {};
    for (const day of days) {
        const s = hocdb.calendarSessionForDay('nyse', day);
        let hi = -1e18, lo = 1e18, last = 0;
        for (let t = Number(s.open); t < Number(s.close); t += 60) {
            const o = p;
            p *= 1 + 0.0007 * Math.sin(t / 613) + 0.0003 * Math.cos(t / 97);
            const bh = Math.max(o, p) * 1.0005, bl = Math.min(o, p) * 0.9995;
            db.append({ timestamp: t * US, open: o, high: bh, low: bl, close: p, volume: 500 + (t % 97) });
            if (t === Number(s.open)) firstOpen[day] = o;
            hi = Math.max(hi, bh); lo = Math.min(lo, bl); last = p;
        }
        perDay[day] = [hi, lo, last];
    }
    db.flush();
    return { firstOpen, perDay };
}

function testCalendarDatabase() {
    console.log('Database with a trading calendar...');
    expectError(() => hocdb.dbInit('BAD', DATA_DIR, SCHEMA, { calendar: 999 }), 'unknown calendar id at open', 'UnknownCalendar');
    expectError(() => hocdb.dbInit('BAD', DATA_DIR, SCHEMA, { calendar: 'not_a_calendar' }), 'unknown calendar name at open', 'UnknownCalendar');
    const db = hocdb.dbInit('CAL', DATA_DIR, SCHEMA, { calendar: 'nyse', timestampUnitNs: 1000 });
    check(db.getCalendar() === 3 && Number(db.getTimestampUnit()) === 1000, 'handle reports the calendar and unit');
    check(near(db.periodsPerYear(60 * US), 252 * 390) && near(db.periodsPerYear(86400 * US), 252), 'handle periodsPerYear');
    const thu = hocdb.daysFromCivil(2025, 9, 4), fri = hocdb.daysFromCivil(2025, 9, 5), tue = hocdb.daysFromCivil(2025, 9, 9);
    const { firstOpen, perDay } = fillSessions(db, [thu, fri, tue]);
    check(db.load().length === 3 * 390, `3 sessions x 390 one-minute bars, got ${db.load().length}`);

    const sFri = hocdb.calendarSessionForDay('nyse', fri);
    const res = db.indicators([{ kind: 'session_range', param: 0 }, { kind: 'pivots', param: 0 },
        { kind: 'session_vwap', param: 0 }, { kind: 'opening_range', period: 5, param: 0 }],
        { start: (Number(sFri.open) + 100 * 60) * US, end: Number(sFri.close) * US, columns: COLUMNS, lookback: 0 });
    check(res.timestamps.length === 290, `window has 290 rows, got ${res.timestamps.length}`);
    check(res.session_range_open.every(v => near(v, firstOpen[fri], 1e-12)),
        "session_range open == Friday's first bar even though the window starts later");
    const [ph, pl, pc] = perDay[thu];
    check(res.pivots_pp.every(v => near(v, (ph + pl + pc) / 3)), 'pivots come from Thursday (previous trading day)');
    check(res.session_vwap.every(v => Number.isFinite(v) && v > 0), 'session vwap defined over the window');
    check(Array.from(res.opening_range_5_breakout).every(v => Number.isFinite(v)), 'opening range formed before the window');

    const sTue = hocdb.calendarSessionForDay('nyse', tue);
    const res2 = db.indicators([{ kind: 'pivots', param: 0 }],
        { start: Number(sTue.open) * US, end: Number(sTue.close) * US, columns: COLUMNS, lookback: 0 });
    const [fh, fl, fc] = perDay[fri];
    check(near(res2.pivots_pp[0], (fh + fl + fc) / 3), "Tuesday's pivots skip the missing Monday");

    const h = db.health(0, Number.MAX_SAFE_INTEGER, 'close', 'volume', 5 * 60 * US, 0.2);
    check(Number(h.n_session_breaks) === 2 && Number(h.n_missing_sessions) === 1, 'health: 2 session breaks, 1 missing session');
    check(Number(h.n_gaps) === 1 && Number(h.max_gap) === (60 + 390 * 60) * US, 'health: the missing session is the only real gap');
    check(Number(h.closed_span) > 0 && Object.keys(h).length === 19, 'health: closed_span and 19 fields');

    const auto = db.summary(0, Number.MAX_SAFE_INTEGER, 'close', 0);
    const explicit = db.summary(0, Number.MAX_SAFE_INTEGER, 'close', 252 * 390);
    check(near(auto.ann_vol, explicit.ann_vol, 1e-12) && auto.ann_vol > 0, 'summary annualises from the calendar');

    const plain = hocdb.dbInit('PLAIN', DATA_DIR, SCHEMA);
    fillSessions(plain, [thu], 1.1);
    expectError(() => plain.indicators([{ kind: 'session_vwap', param: 0 }], { tail: 10, columns: COLUMNS, lookback: 0 }),
        'session kind with param 0 and no calendar', 'CalendarRequired');
    expectError(() => plain.setCalendar(999), 'setCalendar with an unknown id', 'UnknownCalendar');
    plain.setCalendar('crypto');
    plain.setTimestampUnit(1000);
    check(plain.getCalendar() === 1 && near(plain.periodsPerYear(60 * US), 365 * 1440), 'setCalendar / setTimestampUnit');
    plain.close();
    db.close();

    const reopened = hocdb.dbInit('CAL', DATA_DIR, SCHEMA);
    check(reopened.getCalendar() === 3 && Number(reopened.getTimestampUnit()) === 1000, 'calendar and unit persisted in the header');
    const reader = hocdb.openReader('CAL', DATA_DIR, SCHEMA);
    check(reader.getCalendar() === 3 && near(reader.periodsPerYear(60 * US), 252 * 390), "a reader sees the writer's calendar");
    reader.close();
    reopened.close();
}

const TIED = {
    ts: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    open: [100, 101, 102, 98, 95, 97, 99, 103, 104, 102, 100, 99],
    high: [101, 103, 103, 99, 97, 99, 104, 105, 105, 103, 101, 100],
    low: [99, 100, 96, 93, 94, 96, 98, 102, 101, 99, 98, 97],
    close: [100.5, 102.5, 97, 94, 96.5, 98.5, 103.5, 104.5, 102, 100, 98.5, 99.5],
    target: [1, 1, 1, 1, 1, 2, 2, 2, -1, -1, -1, -1],
    params: { initial_equity: 1000, cost_bps: 10, slippage_bps: 5, stop_loss: 0.05, periods_per_year: 252 },
    equity: [1000, 1001.3484495, 995.8484495, 994.7024755365, 994.7024755365, 994.7024755365,
        1003.4053765365, 1005.4053765365, 1000.4053765365, 1001.9465295365, 1003.4465295365, 1002.4465295365],
};

function testBacktest() {
    console.log('Signal backtester...');
    const r = hocdb.backtestArrays(TIED.ts, TIED.open, TIED.high, TIED.low, TIED.close, TIED.target, TIED.params,
        { equity: true, position: true, cash: true, pnl: true, drawdown: true, maxTrades: 8 });
    const res = r.result;
    check(Number(res.n_bars) === 12 && Number(res.n_trades) === 3 && Number(res.n_long_trades) === 2
        && Number(res.n_short_trades) === 1 && Number(res.n_stop_exits) === 1, 'tied example trade counts');
    check(near(res.final_equity, 1002.4465295364876) && near(res.max_drawdown, 0.006637024271452185)
        && Number(res.max_drawdown_bars) === 4 && near(res.sharpe, 0.961805823133203)
        && near(res.turnover, 0.7010422825639875) && near(res.total_cost, 0.7009464760125), 'tied example statistics');
    check(TIED.equity.every((v, i) => near(r.equity[i], v)), 'equity curve');
    check(r.position[6] === 2 && r.position[9] === -1 && r.drawdown.length === 12 && r.cash.length === 12, 'per-bar outputs');
    const t0 = r.trades[0];
    check(r.trades.length === 3 && Number(t0.entry_ts) === 2 && Number(t0.exit_ts) === 4 && Number(t0.exit_reason) === 1
        && near(t0.exit_price, 95.9499760125), 'first trade stopped out');
    check(Number(r.trades[2].exit_ts) === 0 && Number(r.trades[2].exit_reason) === 4 && Number(r.trades[2].direction) === -1,
        'last trade is still open at the end');
    expectError(() => hocdb.backtestArrays(TIED.ts, TIED.open, TIED.high, TIED.low, TIED.close, TIED.target, { position_mode: 9 }),
        'invalid position_mode');

    const sp = hocdb.walkForwardSplits(100, 4, 0.5, true);
    check(sp.length === 4 && Number(sp[0].train_start) === 0 && Number(sp[0].train_end) === 50
        && Number(sp[0].test_start) === 50 && Number(sp[0].test_end) === 62 && Number(sp[3].test_end) === 100,
        'anchored walk-forward splits');
    const rolling = hocdb.walkForwardSplits(100, 4, 0.5, false);
    check(Number(rolling[3].train_start) === 36 && Number(rolling[3].train_end) === 86, 'rolling walk-forward splits');
    const rampTs = Array.from({ length: 100 }, (_, i) => i + 1);
    const ramp = Array.from({ length: 100 }, (_, i) => 100 + 0.25 * i);
    const ones = new Array(100).fill(1);
    const results = hocdb.backtestSplits(rampTs, null, null, null, ramp, ones, sp, { fill_mode: 1, position_mode: 1 });
    check(results.length === 4 && results.every((x, i) => near(x.total_return, ramp[Number(sp[i].test_end) - 1] / ramp[Number(sp[i].test_start)] - 1)),
        'backtestSplits runs every test window independently');

    const db = hocdb.dbInit('BT', DATA_DIR, SCHEMA, { calendar: 'crypto', timestampUnitNs: 1_000_000_000 });
    let price = 100;
    for (let i = 0; i < 6000; i++) {
        price *= 1 + 0.0006 * Math.sin(i * 0.37) + 0.0002 * Math.cos(i * 0.11);
        const t = 1_700_000_000 + i * 7;
        db.append({ timestamp: t, open: price, high: price * 1.001, low: price * 0.999, close: price, volume: 10 + (i % 5) });
    }
    db.flush();
    const start = 1_700_000_100, end = start + 6 * 3600;
    const bars = db.ohlcv(start, end, 300, { price: 'close', volume: 'volume' });
    const n = bars.timestamps.length;
    check(n === 72, `6 hours of 5-minute bars, got ${n}`);
    const target = Array.from({ length: n }, (_, i) => (i >= 5 ? (bars.close[i] > bars.close[i - 5] ? 1 : -1) : 0));
    const params = { initial_equity: 10000, cost_bps: 5, position_mode: 1 };
    const dbres = db.backtest(target, start, end, { bucket: 300, columns: COLUMNS, params, equity: true, maxTrades: n });
    const kernel = hocdb.backtestArrays(bars.timestamps, bars.open, bars.high, bars.low, bars.close, target,
        { ...params, periods_per_year: 365 * 288 }, { maxTrades: n });
    check(Number(dbres.result.n_bars) === n && near(dbres.result.final_equity, kernel.result.final_equity, 0)
        && near(dbres.result.sharpe, kernel.result.sharpe, 0),
        "db backtest == kernel on ohlcv bars with the calendar's periods_per_year");
    check(dbres.equity.length === n && dbres.result.ann_vol > 0, 'db backtest outputs');
    const tail = db.backtestTail(target.slice(-20), { bucket: 300, columns: COLUMNS, params });
    check(Number(tail.result.n_bars) === 20, 'backtestTail window');
    expectError(() => db.backtest(target.slice(0, -1), start, end, { bucket: 300, columns: COLUMNS, params }), 'target length mismatch');
    db.close();
}

function testUniverse() {
    console.log('Universe features...');
    const n = 120;
    const c0 = Array.from({ length: n }, (_, i) => 100 + 5 * Math.sin(i * 0.2) + 0.1 * i);
    const c1 = Array.from({ length: n }, (_, i) => 50 + 3 * Math.cos(i * 0.15) - 0.05 * i);
    const v0 = Array.from({ length: n }, (_, i) => 1000 + (i % 7) * 10);
    const ts = Array.from({ length: n }, (_, i) => 1000 + i * 60);
    const params = { mom_long: 30, corr_period: 30, beta_period: 30, sma_period: 20 };
    const u = hocdb.universeArrays([c0, c1, c0], { volumes: [v0, new Array(n).fill(2000), new Array(n).fill(500)], ts, params });
    check(Number(u.summary.n_tickers) === 3 && Number(u.summary.n_bars) === n && Number(u.summary.first_ts) === 1000
        && Number(u.summary.last_ts) === ts[n - 1], 'summary basics');
    check(near(u.corr[0][2], 1) && near(u.corr[2][0], 1) && u.corr[0][0] === 1 && near(u.corr[0][1], u.corr[1][0]),
        'correlation matrix: identical tickers, symmetry, unit diagonal');
    check(Number(u.rows[0].max_corr_index) === 2 && Number(u.rows[2].max_corr_index) === 0, 'most correlated partner');
    check(u.rows[0].rank_mom_mid === u.rows[2].rank_mom_mid && Number.isFinite(u.rows[1].beta), 'ranks tie; beta defined');
    check(Object.keys(u.rows[0]).length === 21 && Object.keys(u.summary).length === 16, '21 row fields, 16 summary fields');
    const noVol = hocdb.universeArrays([c0, c1, c0], { params, corr: false });
    check(Number.isNaN(noVol.rows[0].volume_ratio) && Number(noVol.summary.first_ts) === 0 && !noVol.corr,
        'without volumes / timestamps / correlation matrix');

    const dbs = [c0, c1, c0].map((series, k) => {
        const db = hocdb.dbInit(`U${k}`, DATA_DIR, SCHEMA);
        for (let i = 0; i < n; i++) {
            if (k === 2 && i % 7 === 6) continue; // this ticker is missing every 7th bar
            db.append({ timestamp: ts[i], open: series[i], high: series[i], low: series[i], close: series[i], volume: v0[i] });
        }
        db.flush();
        return db;
    });
    const got = hocdb.universe(dbs, { columns: COLUMNS, bars: n, bucket: 0, params, corr: true });
    const joined = Array.from({ length: n }, (_, i) => i).filter(i => i % 7 !== 6);
    check(Number(got.summary.n_bars) === joined.length && Number(got.summary.last_ts) === ts[joined[joined.length - 1]],
        `inner join over 3 databases: ${got.summary.n_bars} bars`);
    const hand = hocdb.universeArrays([joined.map(i => c0[i]), joined.map(i => c1[i]), joined.map(i => c0[i])],
        { volumes: [joined.map(i => v0[i]), joined.map(i => v0[i]), joined.map(i => v0[i])], ts: joined.map(i => ts[i]), params });
    const same = [0, 1, 2].every(i => Object.keys(hand.rows[0]).every(k => near(got.rows[i][k], hand.rows[i][k])));
    check(same, 'db join == hand-joined arrays (all row fields)');
    check(Object.keys(hand.summary).every(k => near(got.summary[k], hand.summary[k])), 'summary matches the hand join');
    const auto = hocdb.universe(dbs, { columns: COLUMNS, bars: 0, params, corr: false });
    check(Number(auto.summary.n_bars) >= 31, 'bars 0 reads enough bars for the longest period');
    expectError(() => hocdb.universe(dbs, { columns: {}, bars: 10 }), 'universe without a close column');
    expectError(() => hocdb.universe([], {}), 'universe with no databases');
    for (const db of dbs) db.close();
}

function main() {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
    fs.mkdirSync(DATA_DIR, { recursive: true });
    try {
        testCalendars();
        testCalendarDatabase();
        testBacktest();
        testUniverse();
    } finally {
        fs.rmSync(DATA_DIR, { recursive: true, force: true });
    }
    if (failures) { console.error(`Node.js round-4 test FAILED (${failures} checks)`); process.exit(1); }
    console.log('Node.js Round 4 Test Passed!');
}

main();
