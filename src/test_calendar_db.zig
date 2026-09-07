//! DB-level tests for trading calendars: calendar sessions for session kinds
//! (param = 0), trading-time gaps in health(), automatic periods_per_year,
//! and persistence of the calendar id / timestamp unit in the file header.
const std = @import("std");
const hocdb = @import("root.zig");
const ind = hocdb.indicators;
const cal = hocdb.calendar;
const DB = hocdb.DynamicTimeSeriesDB;

const Bar = extern struct { timestamp: i64, open: f64, high: f64, low: f64, close: f64, volume: f64 };

const schema = hocdb.Schema{ .fields = &[_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "open", .type = .f64 },
    .{ .name = "high", .type = .f64 },
    .{ .name = "low", .type = .f64 },
    .{ .name = "close", .type = .f64 },
    .{ .name = "volume", .type = .f64 },
} };

const cols = DB.IndicatorColumns{ .open = 1, .high = 2, .low = 3, .close = 4, .volume = 5 };
const US: i64 = 1_000_000;
const MIN: i64 = 60 * US;

fn tmpDir(buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "test_calendar_db_{x}", .{std.crypto.random.int(u64)});
}

/// One-minute bars for every minute of the given NYSE trade dates (µs timestamps).
fn fillSessions(db: *DB, days: []const i64, seed: u64) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();
    var p: f64 = 100.0;
    for (days) |day| {
        const s = cal.nyse.sessionForDay(day).?;
        var t = s.open;
        while (t < s.close) : (t += 60) {
            const o = p;
            p *= @exp(r.floatNorm(f64) * 0.002);
            const bar = Bar{ .timestamp = t * US, .open = o, .high = @max(o, p) * 1.001, .low = @min(o, p) * 0.999, .close = p, .volume = 500 + @as(f64, @floatFromInt(@mod(t, 97))) };
            try db.append(std.mem.asBytes(&bar));
        }
    }
    try db.flush();
}

test "calendar sessions drive session kinds, health and annualisation" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    // Thu 2025-09-04, Fri 2025-09-05, (Mon 2025-09-08 missing), Tue 2025-09-09
    const thu = cal.daysFromCivil(2025, 9, 4);
    const fri = cal.daysFromCivil(2025, 9, 5);
    const tue = cal.daysFromCivil(2025, 9, 9);
    const s_thu = cal.nyse.sessionForDay(thu).?;
    const s_fri = cal.nyse.sessionForDay(fri).?;
    const s_tue = cal.nyse.sessionForDay(tue).?;
    var db = try DB.init("CAL", dir, a, schema, .{ .calendar = @intFromEnum(cal.Id.nyse), .timestamp_unit_ns = 1000 });
    try db.initWriter();
    defer db.deinit();
    try fillSessions(&db, &.{ thu, fri, tue }, 7);
    try std.testing.expectEqual(@as(u64, 3 * 390), db.count());
    try std.testing.expectEqualStrings("nyse", db.tradingCalendar().?.name);
    try std.testing.expectApproxEqAbs(@as(f64, 252 * 390), db.periodsPerYear(MIN), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 252), db.periodsPerYear(86400 * US), 1e-9);

    // session_range with param 0: Friday's window, starting 100 minutes after the open,
    // still sees the session open of Friday's first bar
    const fri_first = try db.query(s_fri.open * US, s_fri.open * US + MIN, &.{}, a);
    defer a.free(fri_first);
    const first_bar: *const Bar = @ptrCast(@alignCast(fri_first.ptr));
    const specs = [_]ind.Spec{
        .{ .kind = @intFromEnum(ind.Kind.session_range), .param = 0 },
        .{ .kind = @intFromEnum(ind.Kind.session_vwap), .param = 0 },
        .{ .kind = @intFromEnum(ind.Kind.pivots), .param = 0 },
        .{ .kind = @intFromEnum(ind.Kind.opening_range), .period = 5, .param = 0 },
    };
    const res = try db.indicatorsRange((s_fri.open + 100 * 60) * US, s_fri.close * US, cols, &specs, 0, 0, a);
    defer res.deinit();
    try std.testing.expectEqual(@as(usize, 290), res.n_rows);
    for (res.output(0)) |v| try std.testing.expectApproxEqAbs(first_bar.open, v, 1e-12); // session open
    try std.testing.expectApproxEqAbs(first_bar.close / first_bar.open - 1.0, res.output(3)[0], 0.5); // ret vs open sane
    for (res.output(4)) |v| try std.testing.expect(!ind.isNan(v)); // session vwap defined
    // pivots on Friday come from Thursday's high / low / close
    const thu_rows = try db.query(s_thu.open * US, s_thu.close * US, &.{}, a);
    defer a.free(thu_rows);
    const thu_bars: []const Bar = @as([*]const Bar, @ptrCast(@alignCast(thu_rows.ptr)))[0 .. thu_rows.len / @sizeOf(Bar)];
    var th: f64 = -1;
    var tl: f64 = 1e18;
    for (thu_bars) |b| {
        th = @max(th, b.high);
        tl = @min(tl, b.low);
    }
    const tc = thu_bars[thu_bars.len - 1].close;
    try std.testing.expectApproxEqAbs((th + tl + tc) / 3.0, res.output(5)[0], 1e-9);
    try std.testing.expectApproxEqAbs((th + tl + tc) / 3.0, res.output(5)[289], 1e-9);
    // opening range breakout is NaN only while forming (first 5 rows of the session, all before the window)
    for (res.output(12)) |v| try std.testing.expect(!ind.isNan(v));

    // Tuesday's pivots use Friday (the previous *existing* session), not the missing Monday
    const pv = [_]ind.Spec{.{ .kind = @intFromEnum(ind.Kind.pivots), .param = 0 }};
    const res2 = try db.indicatorsRange(s_tue.open * US, s_tue.close * US, cols, &pv, 0, 0, a);
    defer res2.deinit();
    const fri_rows = try db.query(s_fri.open * US, s_fri.close * US, &.{}, a);
    defer a.free(fri_rows);
    const fri_bars: []const Bar = @as([*]const Bar, @ptrCast(@alignCast(fri_rows.ptr)))[0 .. fri_rows.len / @sizeOf(Bar)];
    var fh: f64 = -1;
    var fl: f64 = 1e18;
    for (fri_bars) |b| {
        fh = @max(fh, b.high);
        fl = @min(fl, b.low);
    }
    const fc = fri_bars[fri_bars.len - 1].close;
    try std.testing.expectApproxEqAbs((fh + fl + fc) / 3.0, res2.output(0)[0], 1e-9);
    // tail mode with calendar sessions works too
    const tail = try db.indicatorsTail(10, cols, &pv, 0, 0, a);
    defer tail.deinit();
    try std.testing.expectApproxEqAbs((fh + fl + fc) / 3.0, tail.output(0)[9], 1e-9);

    // health: gaps are measured in trading time
    const h = try db.health(0, std.math.maxInt(i64), 4, 5, 5 * MIN, 0.2, a);
    try std.testing.expectEqual(@as(u64, 3 * 390), h.count);
    // the Thursday -> Friday boundary is one minute of trading time; the missing Monday is a
    // real gap of a whole session (60 s + 390 minutes) and also counts as a missing session
    try std.testing.expectEqual(@as(u64, 1), h.n_gaps);
    try std.testing.expectEqual(@as(i64, (60 + 390 * 60) * US), h.max_gap);
    try std.testing.expectEqual((s_fri.close - 60) * US, h.max_gap_at);
    try std.testing.expectEqual(@as(u64, 2), h.n_session_breaks);
    try std.testing.expectEqual(@as(u64, 1), h.n_missing_sessions);
    const expect_closed = ((s_fri.open - (s_thu.close - 60)) - 60 + (s_tue.open - (s_fri.close - 60)) - 60 - 390 * 60) * US;
    try std.testing.expectEqual(expect_closed, h.closed_span);
    const expect_mean = @as(f64, @floatFromInt((1167 * 60 + 60 + 60 + 390 * 60) * US)) / 1169.0;
    try std.testing.expectApproxEqAbs(expect_mean, h.mean_gap, 1e-3);
    try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(MIN)), h.median_gap, 1e-9);

    // automatic annualisation: summary / snapshot with ppy 0 equal explicit 252 * 390
    const auto_sum = try db.summary(0, std.math.maxInt(i64), 4, 0, a);
    const exp_sum = try db.summary(0, std.math.maxInt(i64), 4, 252 * 390, a);
    try std.testing.expectApproxEqAbs(exp_sum.ann_vol, auto_sum.ann_vol, 1e-12);
    try std.testing.expect(auto_sum.ann_vol > 0);
    const auto_snap = try db.snapshot(cols, 300, MIN, 0, a);
    const exp_snap = try db.snapshot(cols, 300, MIN, 252 * 390, a);
    try std.testing.expectApproxEqAbs(exp_snap.hist_vol_20, auto_snap.hist_vol_20, 1e-12);
    const tick_snap = try db.snapshot(cols, 300, 0, 0, a); // spacing estimated from the rows
    try std.testing.expectApproxEqAbs(exp_snap.hist_vol_20, tick_snap.hist_vol_20, 1e-12);
    var multi: [2]ind.Snapshot = undefined;
    try db.snapshotMulti(cols, 300, &.{ MIN, 5 * MIN }, &.{ 0, 0 }, &multi, a);
    try std.testing.expectApproxEqAbs(exp_snap.hist_vol_20, multi[0].hist_vol_20, 1e-12);
    try std.testing.expect(multi[1].hist_vol_20 > 0);
}

test "calendar id and timestamp unit persist in the header and reach readers" {
    const a = std.testing.allocator;
    var dir_buf: [64]u8 = undefined;
    const dir = try tmpDir(&dir_buf);
    defer std.fs.cwd().deleteTree(dir) catch {};
    try std.testing.expectError(error.UnknownCalendar, DB.init("P", dir, a, schema, .{ .calendar = 999 }));
    {
        var db = try DB.init("P", dir, a, schema, .{ .calendar = @intFromEnum(cal.Id.lse), .timestamp_unit_ns = 1_000_000 });
        try db.initWriter();
        defer db.deinit();
        try fillSessions(&db, &.{cal.daysFromCivil(2025, 9, 4)}, 1);
        try std.testing.expectEqual(@as(u32, 5), db.calendar_id);
    }
    {
        var db = try DB.init("P", dir, a, schema, .{});
        try db.initWriter();
        defer db.deinit();
        try std.testing.expectEqual(@as(u32, 5), db.calendar_id);
        try std.testing.expectEqual(@as(u64, 1_000_000), db.timestampUnitNs());
        var r = try DB.openReader("P", dir, a, schema);
        try r.initWriter();
        defer r.deinit();
        try std.testing.expectEqualStrings("lse", r.tradingCalendar().?.name);
        try std.testing.expectEqual(@as(u64, 1_000_000), r.timestampUnitNs());
        try std.testing.expectError(error.UnknownCalendar, db.setCalendar(77));
        try db.setCalendar(@intFromEnum(cal.Id.crypto));
        try db.setTimestampUnit(1000);
        try std.testing.expectApproxEqAbs(@as(f64, 365 * 1440), db.periodsPerYear(60 * US), 1e-9);
    }
    {
        var db = try DB.init("P", dir, a, schema, .{});
        try db.initWriter();
        defer db.deinit();
        try std.testing.expectEqualStrings("crypto", db.tradingCalendar().?.name);
        try std.testing.expectEqual(@as(u64, 1000), db.timestampUnitNs());
        // a config value overrides what the file records
        var db2 = try DB.openReader("P", dir, a, schema);
        try db2.initWriter();
        defer db2.deinit();
        try db2.setCalendar(@intFromEnum(cal.Id.fx)); // local to the reader, not persisted
        try std.testing.expectEqualStrings("fx", db2.tradingCalendar().?.name);
        // custom calendars are not persisted (process-local)
        var weekly: [7]?cal.DaySession = .{null} ** 7;
        weekly[0] = .{ .open_sec = 0, .close_sec = 3600 };
        const id = try cal.define("db_custom", weekly, 0, .none, &.{}, &.{}, 52);
        try db.setCalendar(id);
        try std.testing.expectEqualStrings("db_custom", db.tradingCalendar().?.name);
        try db.flush();
    }
    {
        var db = try DB.init("P", dir, a, schema, .{});
        try db.initWriter();
        defer db.deinit();
        try std.testing.expectEqual(@as(u32, 0), db.calendar_id);
        try std.testing.expectEqual(@as(f64, 0), db.periodsPerYear(60 * US));
    }
}
