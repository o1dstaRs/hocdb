// Round 4: trading calendars, signal backtester and universe features.
//   bun run bindings/bun/test/test_round4.ts
import { rmSync, mkdirSync } from "fs";
import { HOCDB } from "../index";

const DATA_DIR = "b_bun_test_round4";
const US = 1_000_000;
let failures = 0;

function check(cond: boolean, msg: string) {
    if (cond) console.log(`  ok: ${msg}`);
    else { console.log(`  FAIL: ${msg}`); failures++; }
}

function near(a: number | bigint, b: number, tol = 1e-9): boolean {
    const x = Number(a);
    if (Number.isNaN(x) || Number.isNaN(b)) return Number.isNaN(x) && Number.isNaN(b);
    return Math.abs(x - b) <= tol + 1e-9 * Math.abs(b);
}

function expectError(fn: () => unknown, msg: string, needle?: string) {
    try {
        fn();
    } catch (e) {
        const text = e instanceof Error ? e.message : String(e);
        if (needle && !text.includes(needle)) { console.log(`  FAIL: ${msg} -> wrong message: ${text}`); failures++; }
        else console.log(`  ok: ${msg} -> ${text.split("\n")[0]}`);
        return;
    }
    console.log(`  FAIL: ${msg} (no error thrown)`);
    failures++;
}

const utc = (y: number, m: number, d: number, hh = 0, mm = 0) => HOCDB.daysFromCivil(y, m, d) * 86400 + hh * 3600 + mm * 60;

const SCHEMA = [
    { name: "timestamp", type: "i64" as const }, { name: "open", type: "f64" as const }, { name: "high", type: "f64" as const },
    { name: "low", type: "f64" as const }, { name: "close", type: "f64" as const }, { name: "volume", type: "f64" as const },
];
const COLUMNS = { open: "open", high: "high", low: "low", close: "close", volume: "volume" };

function testCalendars() {
    console.log("Trading calendars...");
    check(HOCDB.calendarId("nyse") === 3 && HOCDB.calendarId("LSE") === 5 && HOCDB.calendarId("nope") === 0, "calendar ids by name");
    check(HOCDB.calendarName(1) === "crypto" && HOCDB.calendarName(99) === null, "calendar names");
    const fri = utc(2025, 9, 5, 15, 0);
    const s = HOCDB.calendarSession("nyse", fri)!;
    check(s !== null && s.open === utc(2025, 9, 5, 13, 30) && s.close === utc(2025, 9, 5, 20, 0)
        && s.early_close === false && s.trade_day === HOCDB.daysFromCivil(2025, 9, 5), "NYSE Friday session 13:30-20:00 UTC");
    check(HOCDB.calendarIsOpen("nyse", fri) && !HOCDB.calendarIsOpen("nyse", utc(2025, 9, 6, 12, 0)), "isOpen");
    const sat = utc(2025, 9, 6, 12, 0);
    check(HOCDB.calendarSession("nyse", sat) === null, "Saturday has no session");
    check(HOCDB.calendarSession("nyse", sat, 1)!.trade_day === HOCDB.daysFromCivil(2025, 9, 5), "previous session is Friday");
    check(HOCDB.calendarSession("nyse", sat, "next")!.trade_day === HOCDB.daysFromCivil(2025, 9, 8), "next session is Monday");
    check(HOCDB.calendarSessionForDay("nyse", HOCDB.daysFromCivil(2025, 7, 4)) === null, "Independence Day is closed");
    const bf = HOCDB.calendarSessionForDay("nyse", HOCDB.daysFromCivil(2025, 11, 28))!;
    check(bf.early_close === true && bf.close === utc(2025, 11, 28, 18, 0), "Black Friday early close");
    check(HOCDB.calendarOpenSeconds("nyse", utc(2025, 8, 29, 15, 0), utc(2025, 9, 2, 15, 0)) === 5 * 3600 + 5400,
        "trading seconds across the Labor Day weekend");
    check(HOCDB.calendarSessionsBetween("nyse", utc(2025, 1, 1), utc(2026, 1, 1)) === 250, "250 NYSE sessions in 2025");
    check(near(HOCDB.calendarPeriodsPerYear("nyse", 60), 252 * 390) && near(HOCDB.calendarPeriodsPerYear("crypto", 86400), 365),
        "periods per year");
    check(HOCDB.calendarToLocal("nyse", fri) === utc(2025, 9, 5, 11, 0), "UTC -> New York local");
    const c = HOCDB.civilFromDays(HOCDB.daysFromCivil(2024, 2, 29));
    check(c.year === 2024 && c.month === 2 && c.day === 29, "civil date round trip");
    check(HOCDB.calendarSessionForDay("fx", HOCDB.daysFromCivil(2025, 9, 8))!.open === utc(2025, 9, 7, 21, 0),
        "FX Monday opens Sunday 17:00 New York");
    expectError(() => HOCDB.calendarSession("not_a_calendar", fri), "unknown calendar name");

    const weekly = [0, 1, 2, 3].map(() => ({ open_sec: 10 * 3600, close_sec: 15 * 3600 })).concat([null, null, null] as any);
    const cid = HOCDB.calendarDefine("bun_custom", {
        weekly, utc_offset_sec: 9 * 3600, dst_rule: "none", holidays: [HOCDB.daysFromCivil(2025, 9, 9)],
        early_closes: [{ day: HOCDB.daysFromCivil(2025, 9, 10), close_sec: 12 * 3600 }], sessions_per_year: 200,
    });
    check(cid >= 32 && HOCDB.calendarId("bun_custom") === cid, "custom calendar registered");
    check(HOCDB.calendarSessionForDay(cid, HOCDB.daysFromCivil(2025, 9, 8))!.open === utc(2025, 9, 8, 1, 0), "custom Monday 10:00 UTC+9");
    check(HOCDB.calendarSessionForDay(cid, HOCDB.daysFromCivil(2025, 9, 9)) === null, "custom holiday");
    check(HOCDB.calendarSessionForDay(cid, HOCDB.daysFromCivil(2025, 9, 10))!.close === utc(2025, 9, 10, 3, 0), "custom early close");
    check(HOCDB.calendarSessionForDay(cid, HOCDB.daysFromCivil(2025, 9, 12)) === null, "custom week has no Friday");
}

function fillSessions(db: HOCDB, days: number[], seed = 1.0) {
    let p = 100.0 * seed;
    const firstOpen: Record<number, number> = {};
    const perDay: Record<number, [number, number, number]> = {};
    for (const day of days) {
        const s = HOCDB.calendarSessionForDay("nyse", day)!;
        let hi = -1e18, lo = 1e18, last = 0;
        for (let t = s.open; t < s.close; t += 60) {
            const o = p;
            p *= 1 + 0.0007 * Math.sin(t / 613) + 0.0003 * Math.cos(t / 97);
            const bh = Math.max(o, p) * 1.0005, bl = Math.min(o, p) * 0.9995;
            db.append({ timestamp: t * US, open: o, high: bh, low: bl, close: p, volume: 500 + (t % 97) });
            if (t === s.open) firstOpen[day] = o;
            hi = Math.max(hi, bh); lo = Math.min(lo, bl); last = p;
        }
        perDay[day] = [hi, lo, last];
    }
    db.flush();
    return { firstOpen, perDay };
}

function testCalendarDatabase() {
    console.log("Database with a trading calendar...");
    expectError(() => new HOCDB("BAD", DATA_DIR, SCHEMA, { calendar: 999 }), "unknown calendar id at open", "UnknownCalendar");
    const db = new HOCDB("CAL", DATA_DIR, SCHEMA, { calendar: HOCDB.calendarId("nyse"), timestamp_unit_ns: 1000, overwrite_on_full: false });
    check(db.getCalendar() === 3 && db.getTimestampUnit() === 1000, "handle reports the calendar and unit");
    check(near(db.periodsPerYear(60 * US), 252 * 390) && near(db.periodsPerYear(86400 * US), 252), "handle periodsPerYear");
    const thu = HOCDB.daysFromCivil(2025, 9, 4), fri = HOCDB.daysFromCivil(2025, 9, 5), tue = HOCDB.daysFromCivil(2025, 9, 9);
    const { firstOpen, perDay } = fillSessions(db, [thu, fri, tue]);

    const sFri = HOCDB.calendarSessionForDay("nyse", fri)!;
    const res = db.indicators([{ kind: "session_range", param: 0 }, { kind: "pivots", param: 0 },
        { kind: "session_vwap", param: 0 }, { kind: "opening_range", period: 5, param: 0 }],
        { start: (sFri.open + 100 * 60) * US, end: sFri.close * US, columns: COLUMNS, lookback: 0 });
    check(res.timestamps.length === 290, `window has 290 rows, got ${res.timestamps.length}`);
    check(Array.from(res.columns.session_range_open!).every(v => near(v, firstOpen[fri]!, 1e-12)),
        "session_range open == Friday's first bar even though the window starts later");
    const [ph, pl, pc] = perDay[thu]!;
    check(Array.from(res.columns.pivots_pp!).every(v => near(v, (ph + pl + pc) / 3)), "pivots come from Thursday");
    check(Array.from(res.columns.session_vwap!).every(v => Number.isFinite(v) && v > 0), "session vwap defined over the window");

    const sTue = HOCDB.calendarSessionForDay("nyse", tue)!;
    const res2 = db.indicators([{ kind: "pivots", param: 0 }], { start: sTue.open * US, end: sTue.close * US, columns: COLUMNS, lookback: 0 });
    const [fh, fl, fc] = perDay[fri]!;
    check(near(res2.columns.pivots_pp![0]!, (fh + fl + fc) / 3), "Tuesday's pivots skip the missing Monday");

    const h = db.health(0, Number.MAX_SAFE_INTEGER, "close", "volume", 5 * 60 * US, 0.2);
    check(Number(h.n_session_breaks) === 2 && Number(h.n_missing_sessions) === 1, "health: 2 session breaks, 1 missing session");
    check(Number(h.n_gaps) === 1 && Number(h.max_gap) === (60 + 390 * 60) * US, "health: the missing session is the only real gap");
    check(Number(h.closed_span) > 0 && Object.keys(h).length === 19, "health: closed_span and 19 fields");

    const auto = db.summary(0, Number.MAX_SAFE_INTEGER, "close", 0);
    const explicit = db.summary(0, Number.MAX_SAFE_INTEGER, "close", 252 * 390);
    check(near(auto.ann_vol, explicit.ann_vol, 1e-12) && auto.ann_vol > 0, "summary annualises from the calendar");

    const plain = new HOCDB("PLAIN", DATA_DIR, SCHEMA, { overwrite_on_full: false });
    fillSessions(plain, [thu], 1.1);
    expectError(() => plain.indicators([{ kind: "session_vwap", param: 0 }], { tail: 10, columns: COLUMNS, lookback: 0 }),
        "session kind with param 0 and no calendar", "CalendarRequired");
    expectError(() => plain.setCalendar(999), "setCalendar with an unknown id");
    plain.setCalendar("crypto");
    plain.setTimestampUnit(1000);
    check(plain.getCalendar() === 1 && near(plain.periodsPerYear(60 * US), 365 * 1440), "setCalendar / setTimestampUnit");
    plain.close();
    db.close();

    const reopened = new HOCDB("CAL", DATA_DIR, SCHEMA, { overwrite_on_full: false });
    check(reopened.getCalendar() === 3 && reopened.getTimestampUnit() === 1000, "calendar and unit persisted in the header");
    const reader = HOCDB.openReader("CAL", DATA_DIR, SCHEMA);
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
    equity: [1000, 1001.3484495, 995.8484495, 994.7024755365, 994.7024755365, 994.7024755365,
        1003.4053765365, 1005.4053765365, 1000.4053765365, 1001.9465295365, 1003.4465295365, 1002.4465295365],
};

function testBacktest() {
    console.log("Signal backtester...");
    const d = HOCDB.backtestDefaults();
    check(Number(d.initial_equity) === 1 && Number(d.allow_short) === 1 && Number(d.fill_mode) === 0, "default params");
    const run = HOCDB.backtestArrays(TIED.ts, TIED.open, TIED.high, TIED.low, TIED.close, TIED.target, {
        params: { initial_equity: 1000, cost_bps: 10, slippage_bps: 5, stop_loss: 0.05, periods_per_year: 252 },
        outputs: ["equity", "position", "cash", "pnl", "drawdown"], maxTrades: 8,
    });
    const r = run.result;
    check(Number(r.n_bars) === 12 && Number(r.n_trades) === 3 && Number(r.n_long_trades) === 2
        && Number(r.n_short_trades) === 1 && Number(r.n_stop_exits) === 1, "tied example trade counts");
    check(near(r.final_equity, 1002.4465295364876) && near(r.max_drawdown, 0.006637024271452185)
        && Number(r.max_drawdown_bars) === 4 && near(r.sharpe, 0.961805823133203)
        && near(r.turnover, 0.7010422825639875) && near(r.total_cost, 0.7009464760125), "tied example statistics");
    check(TIED.equity.every((v, i) => near(run.equity![i]!, v)), "equity curve");
    check(run.position![6] === 2 && run.position![9] === -1 && run.drawdown!.length === 12, "per-bar outputs");
    const t0 = run.trades![0]!;
    check(run.trades!.length === 3 && Number(t0.entry_ts) === 2 && Number(t0.exit_ts) === 4 && Number(t0.exit_reason) === 1
        && near(t0.exit_price, 95.9499760125), "first trade stopped out");
    check(Number(run.trades![2]!.exit_ts) === 0 && Number(run.trades![2]!.exit_reason) === 4, "last trade is still open");
    expectError(() => HOCDB.backtestArrays(TIED.ts, TIED.open, TIED.high, TIED.low, TIED.close, TIED.target,
        { params: { position_mode: 9 as any } }), "invalid position_mode");

    const sp = HOCDB.walkForwardSplits(100, 4, 0.5, true);
    check(sp.length === 4 && sp[0]!.train_start === 0 && sp[0]!.train_end === 50 && sp[0]!.test_start === 50
        && sp[0]!.test_end === 62 && sp[3]!.test_end === 100, "anchored walk-forward splits");
    const rolling = HOCDB.walkForwardSplits(100, 4, 0.5, false);
    check(rolling[3]!.train_start === 36 && rolling[3]!.train_end === 86, "rolling walk-forward splits");
    const rampTs = Array.from({ length: 100 }, (_, i) => i + 1);
    const ramp = Array.from({ length: 100 }, (_, i) => 100 + 0.25 * i);
    const ones = new Array(100).fill(1);
    const results = HOCDB.backtestSplits(rampTs, null, null, null, ramp, ones, sp, { fill_mode: 1, position_mode: 1 });
    check(results.length === 4 && results.every((x, i) => near(x.total_return, ramp[sp[i]!.test_end - 1]! / ramp[sp[i]!.test_start]! - 1)),
        "backtestSplits runs every test window independently");

    const db = new HOCDB("BT", DATA_DIR, SCHEMA, { calendar: "crypto", timestamp_unit_ns: 1_000_000_000, overwrite_on_full: false });
    let price = 100;
    for (let i = 0; i < 6000; i++) {
        price *= 1 + 0.0006 * Math.sin(i * 0.37) + 0.0002 * Math.cos(i * 0.11);
        const t = 1_700_000_000 + i * 7;
        db.append({ timestamp: t, open: price, high: price * 1.001, low: price * 0.999, close: price, volume: 10 + (i % 5) });
    }
    db.flush();
    const start = 1_700_000_100, end = start + 6 * 3600;
    const bars = db.ohlcv(start, end, 300, { price: "close", volume: "volume" });
    const n = bars.timestamps.length;
    check(n === 72, `6 hours of 5-minute bars, got ${n}`);
    const target = Array.from({ length: n }, (_, i) => (i >= 5 ? (bars.close[i]! > bars.close[i - 5]! ? 1 : -1) : 0));
    const params = { initial_equity: 10000, cost_bps: 5, position_mode: 1 };
    const dbrun = db.backtest(target, start, end, { bucket: 300, columns: COLUMNS, params, outputs: ["equity"], maxTrades: n });
    const kernel = HOCDB.backtestArrays(bars.timestamps, bars.open, bars.high, bars.low, bars.close, target,
        { params: { ...params, periods_per_year: 365 * 288 }, maxTrades: n });
    check(Number(dbrun.result.n_bars) === n && dbrun.result.final_equity === kernel.result.final_equity
        && dbrun.result.sharpe === kernel.result.sharpe, "db backtest == kernel on ohlcv bars with the calendar's ppy");
    check(dbrun.equity!.length === n && dbrun.result.ann_vol > 0, "db backtest outputs");
    const tail = db.backtestTail(target.slice(-20), { bucket: 300, columns: COLUMNS, params });
    check(Number(tail.result.n_bars) === 20, "backtestTail window");
    expectError(() => db.backtest(target.slice(0, -1), start, end, { bucket: 300, columns: COLUMNS, params }), "target length mismatch");
    db.close();
}

function testUniverse() {
    console.log("Universe features...");
    const n = 120;
    const c0 = Array.from({ length: n }, (_, i) => 100 + 5 * Math.sin(i * 0.2) + 0.1 * i);
    const c1 = Array.from({ length: n }, (_, i) => 50 + 3 * Math.cos(i * 0.15) - 0.05 * i);
    const v0 = Array.from({ length: n }, (_, i) => 1000 + (i % 7) * 10);
    const ts = Array.from({ length: n }, (_, i) => 1000 + i * 60);
    const params = { mom_long: 30, corr_period: 30, beta_period: 30, sma_period: 20 };
    const u = HOCDB.universeArrays([c0, c1, c0], { volumes: [v0, new Array(n).fill(2000), new Array(n).fill(500)], ts, params });
    check(Number(u.summary.n_tickers) === 3 && Number(u.summary.n_bars) === n && Number(u.summary.first_ts) === 1000
        && Number(u.summary.last_ts) === ts[n - 1], "summary basics");
    check(near(u.corr![0]![2]!, 1) && near(u.corr![2]![0]!, 1) && u.corr![0]![0] === 1, "correlation matrix");
    check(Number(u.rows[0]!.max_corr_index) === 2 && Number(u.rows[2]!.max_corr_index) === 0, "most correlated partner");
    check(u.rows[0]!.rank_mom_mid === u.rows[2]!.rank_mom_mid && Number.isFinite(u.rows[1]!.beta), "ranks tie; beta defined");
    check(Object.keys(u.rows[0]!).length === 21 && Object.keys(u.summary).length === 16, "21 row fields, 16 summary fields");
    const noVol = HOCDB.universeArrays([c0, c1, c0], { params, corr: false });
    check(Number.isNaN(noVol.rows[0]!.volume_ratio) && Number(noVol.summary.first_ts) === 0 && !noVol.corr,
        "without volumes / timestamps / correlation matrix");

    const dbs = [c0, c1, c0].map((series, k) => {
        const db = new HOCDB(`U${k}`, DATA_DIR, SCHEMA, { overwrite_on_full: false });
        for (let i = 0; i < n; i++) {
            if (k === 2 && i % 7 === 6) continue; // this ticker is missing every 7th bar
            db.append({ timestamp: ts[i]!, open: series[i]!, high: series[i]!, low: series[i]!, close: series[i]!, volume: v0[i]! });
        }
        db.flush();
        return db;
    });
    const got = HOCDB.universe(dbs, { columns: COLUMNS, bars: n, bucket: 0, params, corr: true });
    const joined = Array.from({ length: n }, (_, i) => i).filter(i => i % 7 !== 6);
    check(Number(got.summary.n_bars) === joined.length && Number(got.summary.last_ts) === ts[joined[joined.length - 1]!],
        `inner join over 3 databases: ${got.summary.n_bars} bars`);
    const hand = HOCDB.universeArrays([joined.map(i => c0[i]!), joined.map(i => c1[i]!), joined.map(i => c0[i]!)], {
        volumes: [joined.map(i => v0[i]!), joined.map(i => v0[i]!), joined.map(i => v0[i]!)],
        ts: joined.map(i => ts[i]!), params,
    });
    const rowKeys = Object.keys(hand.rows[0]!) as (keyof typeof hand.rows[0])[];
    const same = [0, 1, 2].every(i => rowKeys.every(k => near(got.rows[i]![k] as number, hand.rows[i]![k] as number)));
    check(same, "db join == hand-joined arrays (all row fields)");
    const auto = HOCDB.universe(dbs, { columns: COLUMNS, bars: 0, params, corr: false });
    check(Number(auto.summary.n_bars) >= 31, "bars 0 reads enough bars for the longest period");
    expectError(() => HOCDB.universe([], {}), "universe with no databases");
    for (const db of dbs) db.close();
}

function main() {
    rmSync(DATA_DIR, { recursive: true, force: true });
    mkdirSync(DATA_DIR, { recursive: true });
    try {
        testCalendars();
        testCalendarDatabase();
        testBacktest();
        testUniverse();
    } finally {
        rmSync(DATA_DIR, { recursive: true, force: true });
    }
    if (failures) { console.error(`Bun round-4 test FAILED (${failures} checks)`); process.exit(1); }
    console.log("Bun Round 4 Test Passed!");
}

main();
