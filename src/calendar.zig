//! Trading calendars: exchange sessions, holidays, early closes and
//! daylight-saving rules, so that session-anchored indicators, data-health
//! gap detection and annualisation work in exchange time instead of fixed
//! 24-hour cycles.
//!
//! All public functions work in **UTC seconds** (i64). The storage layer
//! converts database timestamps with its `timestamp_unit_ns`.
//!
//! A calendar is a weekly template (one optional [open, close) window per
//! weekday, in local seconds relative to the local midnight of the *trade
//! date*; the open may be negative for sessions that start the evening
//! before, e.g. FX and CME), a fixed standard UTC offset, a daylight-saving
//! rule, holidays (full closures) and early closes.
//!
//! Built-in calendars (ids are stable, see `Id`):
//!   crypto  24/7, UTC days, 365 sessions / year
//!   fx      Sunday 17:00 -> Friday 17:00 New York time, one session per
//!           trade date from 17:00 the evening before; closed Dec 25 / Jan 1
//!   nyse    09:30-16:00 America/New_York, NYSE holiday rules incl. observed
//!           weekend holidays, Good Friday, Juneteenth (2022+), 13:00 early
//!           closes, special closures (9/11, Sandy, presidential funerals)
//!   nasdaq  alias of nyse
//!   lse     08:00-16:30 Europe/London, UK bank holidays incl. substitute
//!           days and the 2011-2023 one-off closures, 12:30 early closes
//!   cme     Globex equity-index schedule (approximation): trade date
//!           sessions 17:00 (previous evening) -> 16:00 America/Chicago,
//!           Sunday evening through Friday, closed on New Year's Day, Good
//!           Friday and Christmas, 12:00 early close on other US holidays.
//!           Define a custom calendar for exact product rules.
//! Custom calendars can be registered at runtime with `define` (ids from
//! `first_custom_id`); they live in this process only.

const std = @import("std");

pub const DAY: i64 = 86400;

/// Daylight-saving rules. `us`: second Sunday of March 02:00 local standard
/// time to first Sunday of November 02:00 local daylight time (2007+ rule;
/// first Sunday of April to last Sunday of October before 2007).
/// `eu`: last Sunday of March 01:00 UTC to last Sunday of October 01:00 UTC.
pub const DstRule = enum(u8) { none = 0, us = 1, eu = 2 };

/// One weekday's trading window in local seconds relative to the local
/// midnight of the trade date (open may be negative, close may exceed 86400).
pub const DaySession = extern struct { open_sec: i32, close_sec: i32 };

pub const EarlyClose = extern struct { day: i32, close_sec: i32 };

/// A resolved session.
pub const Session = extern struct {
    /// UTC seconds of the open (inclusive).
    open: i64,
    /// UTC seconds of the close (exclusive).
    close: i64,
    /// Trade date as days since 1970-01-01 (local date the session belongs to).
    trade_day: i64,
    /// 1 when the session closes early.
    early_close: u64,
};

pub const Id = enum(u32) {
    none = 0,
    crypto = 1,
    fx = 2,
    nyse = 3,
    nasdaq = 4,
    lse = 5,
    cme = 6,
    _,
};

pub const first_custom_id: u32 = 32;
const max_custom: usize = 32;

const Rules = enum(u8) { custom, crypto, fx, nyse, lse, cme };

pub const Calendar = struct {
    name: []const u8,
    weekly: [7]?DaySession, // index 0 = Monday
    utc_offset_sec: i32,
    dst: DstRule,
    rules: Rules,
    holidays: []const i32 = &.{}, // sorted local day numbers (custom)
    early_closes: []const EarlyClose = &.{}, // sorted by day (custom)
    sessions_per_year: f64,

    /// Typical session length in seconds (the first defined weekday).
    pub fn sessionSeconds(self: *const Calendar) f64 {
        for (self.weekly) |w| if (w) |d| return @floatFromInt(d.close_sec - d.open_sec);
        return @floatFromInt(DAY);
    }

    /// Bars per year for bars of `bucket_sec` seconds: intraday buckets are
    /// scaled by the session length (252 * 390 one-minute bars for NYSE),
    /// buckets of a day or more count calendar buckets capped by the
    /// number of sessions (daily -> sessions_per_year, weekly -> 52.18).
    pub fn periodsPerYear(self: *const Calendar, bucket_sec: f64) f64 {
        if (!(bucket_sec > 0)) return 0;
        const day: f64 = @floatFromInt(DAY);
        if (bucket_sec < day) return self.sessions_per_year * self.sessionSeconds() / bucket_sec;
        return @min(self.sessions_per_year, 365.25 * day / bucket_sec);
    }

    // -- time zone -------------------------------------------------------------

    fn dstActive(self: *const Calendar, utc: i64) bool {
        return switch (self.dst) {
            .none => false,
            .us => blk: {
                const local_std = utc + self.utc_offset_sec;
                const y = civilFromDays(@divFloor(local_std, DAY)).year;
                // 2007+: second Sunday of March .. first Sunday of November;
                // 1987-2006: first Sunday of April .. last Sunday of October
                const start_day = if (y >= 2007) nthWeekdayOfMonth(y, 3, 6, 2) else nthWeekdayOfMonth(y, 4, 6, 1);
                const end_day = if (y >= 2007) nthWeekdayOfMonth(y, 11, 6, 1) else lastWeekdayOfMonth(y, 10, 6);
                const start_utc = start_day * DAY + 2 * 3600 - self.utc_offset_sec;
                const end_utc = end_day * DAY + 3600 - self.utc_offset_sec; // 02:00 daylight = 01:00 standard
                break :blk utc >= start_utc and utc < end_utc;
            },
            .eu => blk: {
                const y = civilFromDays(@divFloor(utc, DAY)).year;
                const start_utc = lastWeekdayOfMonth(y, 3, 6) * DAY + 3600;
                const end_utc = lastWeekdayOfMonth(y, 10, 6) * DAY + 3600;
                break :blk utc >= start_utc and utc < end_utc;
            },
        };
    }

    /// Local wall-clock seconds -> UTC seconds.
    pub fn localToUtc(self: *const Calendar, local: i64) i64 {
        const guess = local - self.utc_offset_sec;
        return if (self.dstActive(guess - 1800)) guess - 3600 else guess;
    }

    /// UTC seconds -> local wall-clock seconds.
    pub fn utcToLocal(self: *const Calendar, utc: i64) i64 {
        return utc + self.utc_offset_sec + @as(i64, if (self.dstActive(utc)) 3600 else 0);
    }

    // -- holidays ------------------------------------------------------------------

    const DayInfo = struct { closed: bool = false, early_close_sec: ?i32 = null };

    fn dayInfo(self: *const Calendar, day: i64) DayInfo {
        return switch (self.rules) {
            .custom => blk: {
                if (day < std.math.minInt(i32) or day > std.math.maxInt(i32)) break :blk .{};
                const d32: i32 = @intCast(day);
                if (std.sort.binarySearch(i32, self.holidays, d32, orderI32) != null) break :blk .{ .closed = true };
                for (self.early_closes) |e| if (e.day == d32) break :blk .{ .early_close_sec = e.close_sec };
                break :blk .{};
            },
            .crypto => .{},
            .fx => blk: {
                const c = civilFromDays(day);
                if ((c.month == 12 and c.day == 25) or (c.month == 1 and c.day == 1)) break :blk .{ .closed = true };
                break :blk .{};
            },
            .nyse => nyseDayInfo(day),
            .lse => lseDayInfo(day),
            .cme => cmeDayInfo(day),
        };
    }

    // -- sessions --------------------------------------------------------------------

    /// The session of trade date `day` (days since epoch, local), if any.
    pub fn sessionForDay(self: *const Calendar, day: i64) ?Session {
        const wd = weekday(day);
        const tpl = self.weekly[wd] orelse return null;
        const info = self.dayInfo(day);
        if (info.closed) return null;
        const close_sec: i64 = if (info.early_close_sec) |e| e else tpl.close_sec;
        const open = self.localToUtc(day * DAY + tpl.open_sec);
        const close = self.localToUtc(day * DAY + close_sec);
        if (close <= open) return null;
        return .{ .open = open, .close = close, .trade_day = day, .early_close = if (info.early_close_sec != null) 1 else 0 };
    }

    /// The session containing `utc` (open <= utc < close), or null when closed.
    pub fn sessionAt(self: *const Calendar, utc: i64) ?Session {
        const d = @divFloor(self.utcToLocal(utc), DAY);
        var k: i64 = -1;
        while (k <= 1) : (k += 1) {
            if (self.sessionForDay(d + k)) |s| {
                if (utc >= s.open and utc < s.close) return s;
            }
        }
        return null;
    }

    pub fn isOpen(self: *const Calendar, utc: i64) bool {
        return self.sessionAt(utc) != null;
    }

    /// The session containing `utc`, or the next one to open (null when the
    /// calendar has no session within 60 days, e.g. an empty template).
    pub fn nextSession(self: *const Calendar, utc: i64) ?Session {
        const d = @divFloor(self.utcToLocal(utc), DAY);
        var k: i64 = -1;
        while (k <= 60) : (k += 1) {
            if (self.sessionForDay(d + k)) |s| {
                if (s.close > utc) return s;
            }
        }
        return null;
    }

    /// The session containing `utc`, or the most recent one that closed.
    pub fn prevSession(self: *const Calendar, utc: i64) ?Session {
        const d = @divFloor(self.utcToLocal(utc), DAY);
        var k: i64 = 1;
        while (k >= -60) : (k -= 1) {
            if (self.sessionForDay(d + k)) |s| {
                if (s.open <= utc) return s;
            }
        }
        return null;
    }

    /// Session strictly before `s`.
    pub fn sessionBefore(self: *const Calendar, s: Session) ?Session {
        var d = s.trade_day - 1;
        var k: usize = 0;
        while (k < 60) : (k += 1) {
            if (self.sessionForDay(d)) |p| return p;
            d -= 1;
        }
        return null;
    }

    /// Session strictly after `s`.
    pub fn sessionAfter(self: *const Calendar, s: Session) ?Session {
        var d = s.trade_day + 1;
        var k: usize = 0;
        while (k < 60) : (k += 1) {
            if (self.sessionForDay(d)) |p| return p;
            d += 1;
        }
        return null;
    }

    /// Seconds of trading time inside [a, b).
    pub fn openSecondsBetween(self: *const Calendar, a: i64, b: i64) i64 {
        if (b <= a) return 0;
        var total: i64 = 0;
        var cur = self.nextSession(a);
        while (cur) |s| {
            if (s.open >= b) break;
            const lo = @max(s.open, a);
            const hi = @min(s.close, b);
            if (hi > lo) total += hi - lo;
            cur = self.sessionAfter(s);
        }
        return total;
    }

    /// Number of sessions opening inside [a, b).
    pub fn sessionsBetween(self: *const Calendar, a: i64, b: i64) u64 {
        var n: u64 = 0;
        var cur = self.nextSession(a);
        while (cur) |s| {
            if (s.open >= b) break;
            if (s.open >= a) n += 1;
            cur = self.sessionAfter(s);
        }
        return n;
    }
};

fn orderI32(ctx: i32, item: i32) std.math.Order {
    return std.math.order(ctx, item);
}

// ---------------------------------------------------------------------------
// Civil date helpers (proleptic Gregorian, days since 1970-01-01)
// ---------------------------------------------------------------------------

pub const Civil = struct { year: i64, month: u32, day: u32 };

pub fn daysFromCivil(y_in: i64, m: u32, d: u32) i64 {
    const y = if (m <= 2) y_in - 1 else y_in;
    const era = @divFloor(y, 400);
    const yoe: i64 = y - era * 400;
    const mp: i64 = if (m > 2) m - 3 else m + 9;
    const doy: i64 = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe: i64 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

pub fn civilFromDays(z_in: i64) Civil {
    const z = z_in + 719468;
    const era = @divFloor(z, 146097);
    const doe: i64 = z - era * 146097;
    const yoe: i64 = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const doy: i64 = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp: i64 = @divFloor(5 * doy + 2, 153);
    const d: i64 = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m: i64 = if (mp < 10) mp + 3 else mp - 9;
    const y = yoe + era * 400 + @as(i64, if (m <= 2) 1 else 0);
    return .{ .year = y, .month = @intCast(m), .day = @intCast(d) };
}

/// 0 = Monday ... 6 = Sunday.
pub fn weekday(day: i64) u32 {
    return @intCast(@mod(day + 3, 7));
}

/// Day number of the n-th (1-based) `wd` (0 = Monday) of month `m` in year `y`.
pub fn nthWeekdayOfMonth(y: i64, m: u32, wd: u32, n: u32) i64 {
    const first = daysFromCivil(y, m, 1);
    const delta: i64 = @mod(@as(i64, wd) - @as(i64, weekday(first)), 7);
    return first + delta + 7 * (@as(i64, n) - 1);
}

pub fn lastWeekdayOfMonth(y: i64, m: u32, wd: u32) i64 {
    const next_first = if (m == 12) daysFromCivil(y + 1, 1, 1) else daysFromCivil(y, m + 1, 1);
    const last = next_first - 1;
    const delta: i64 = @mod(@as(i64, weekday(last)) - @as(i64, wd), 7);
    return last - delta;
}

/// Western Easter Sunday (anonymous Gregorian algorithm).
pub fn easterSunday(y: i64) i64 {
    const a = @mod(y, 19);
    const b = @divFloor(y, 100);
    const c = @mod(y, 100);
    const d = @divFloor(b, 4);
    const e = @mod(b, 4);
    const f = @divFloor(b + 8, 25);
    const g = @divFloor(b - f + 1, 3);
    const h = @mod(19 * a + b - d - g + 15, 30);
    const i = @divFloor(c, 4);
    const k = @mod(c, 4);
    const l = @mod(32 + 2 * e + 2 * i - h - k, 7);
    const m = @divFloor(a + 11 * h + 22 * l, 451);
    const month: i64 = @divFloor(h + l - 7 * m + 114, 31);
    const day: i64 = @mod(h + l - 7 * m + 114, 31) + 1;
    return daysFromCivil(y, @intCast(month), @intCast(day));
}

/// US-style observed holiday: Saturday -> Friday, Sunday -> Monday.
fn observedUs(day: i64) i64 {
    return switch (weekday(day)) {
        5 => day - 1,
        6 => day + 1,
        else => day,
    };
}

// ---------------------------------------------------------------------------
// NYSE
// ---------------------------------------------------------------------------

const nyse_special_closures = [_][3]i64{
    .{ 2001, 9, 11 }, .{ 2001, 9, 12 }, .{ 2001, 9, 13 },  .{ 2001, 9, 14 },
    .{ 2004, 6, 11 }, .{ 2007, 1, 2 },  .{ 2012, 10, 29 }, .{ 2012, 10, 30 },
    .{ 2018, 12, 5 }, .{ 2025, 1, 9 },
};

fn nyseDayInfo(day: i64) Calendar.DayInfo {
    const c = civilFromDays(day);
    const y = c.year;
    const wd = weekday(day);
    if (wd >= 5) return .{ .closed = true };
    // fixed-date holidays with weekend observance (New Year's Day is not
    // observed on the preceding Friday when it falls on a Saturday)
    const new_year = daysFromCivil(y, 1, 1);
    if (day == new_year or (weekday(new_year) == 6 and day == new_year + 1)) return .{ .closed = true };
    const next_new_year = daysFromCivil(y + 1, 1, 1);
    _ = next_new_year;
    if (day == observedUs(daysFromCivil(y, 7, 4))) return .{ .closed = true };
    if (day == observedUs(daysFromCivil(y, 12, 25))) return .{ .closed = true };
    if (y >= 2022 and day == observedUs(daysFromCivil(y, 6, 19))) return .{ .closed = true };
    // floating holidays
    if (day == nthWeekdayOfMonth(y, 1, 0, 3)) return .{ .closed = true }; // MLK
    if (day == nthWeekdayOfMonth(y, 2, 0, 3)) return .{ .closed = true }; // Presidents
    if (day == easterSunday(y) - 2) return .{ .closed = true }; // Good Friday
    if (day == lastWeekdayOfMonth(y, 5, 0)) return .{ .closed = true }; // Memorial
    if (day == nthWeekdayOfMonth(y, 9, 0, 1)) return .{ .closed = true }; // Labor
    const thanksgiving = nthWeekdayOfMonth(y, 11, 3, 4);
    if (day == thanksgiving) return .{ .closed = true };
    for (nyse_special_closures) |s| if (day == daysFromCivil(s[0], @intCast(s[1]), @intCast(s[2]))) return .{ .closed = true };
    // early closes at 13:00 (2002-07-05 and 2003-12-26 were one-off post-holiday early closes,
    // 2002-07-03 a full day)
    if (day == thanksgiving + 1) return .{ .early_close_sec = 13 * 3600 };
    if (day == daysFromCivil(2002, 7, 5) or day == daysFromCivil(2003, 12, 26)) return .{ .early_close_sec = 13 * 3600 };
    if (c.month == 7 and c.day == 3 and y != 2002 and weekday(daysFromCivil(y, 7, 4)) < 5) return .{ .early_close_sec = 13 * 3600 };
    if (c.month == 12 and c.day == 24 and weekday(daysFromCivil(y, 12, 25)) < 5) return .{ .early_close_sec = 13 * 3600 };
    return .{};
}

// ---------------------------------------------------------------------------
// LSE
// ---------------------------------------------------------------------------

const lse_extra_closures = [_][3]i64{
    .{ 2002, 6, 3 }, .{ 2002, 6, 4 }, .{ 2011, 4, 29 }, .{ 2012, 6, 4 },  .{ 2012, 6, 5 },
    .{ 2020, 5, 8 }, .{ 2022, 6, 2 }, .{ 2022, 6, 3 },  .{ 2022, 9, 19 }, .{ 2023, 5, 8 },
};

fn lseDayInfo(day: i64) Calendar.DayInfo {
    const c = civilFromDays(day);
    const y = c.year;
    const wd = weekday(day);
    if (wd >= 5) return .{ .closed = true };
    // New Year's Day, substitute Monday when on a weekend
    const ny = daysFromCivil(y, 1, 1);
    const ny_obs = switch (weekday(ny)) {
        5 => ny + 2,
        6 => ny + 1,
        else => ny,
    };
    if (day == ny_obs) return .{ .closed = true };
    const easter = easterSunday(y);
    if (day == easter - 2 or day == easter + 1) return .{ .closed = true };
    // Early May bank holiday (moved to May 8 in 2020 for VE day)
    if (y == 2020) {
        if (day == daysFromCivil(2020, 5, 8)) return .{ .closed = true };
    } else if (day == nthWeekdayOfMonth(y, 5, 0, 1)) return .{ .closed = true };
    // Spring bank holiday (moved in 2002, 2012 and 2022 for the jubilees)
    if (y != 2002 and y != 2012 and y != 2022 and day == lastWeekdayOfMonth(y, 5, 0)) return .{ .closed = true };
    if (day == lastWeekdayOfMonth(y, 8, 0)) return .{ .closed = true }; // Summer bank holiday
    // Christmas and Boxing Day with substitute days
    const xmas = daysFromCivil(y, 12, 25);
    const boxing = daysFromCivil(y, 12, 26);
    var d1 = xmas;
    while (weekday(d1) >= 5) d1 += 1;
    var d2 = boxing;
    while (weekday(d2) >= 5 or d2 == d1) d2 += 1;
    if (day == d1 or day == d2) return .{ .closed = true };
    for (lse_extra_closures) |s| if (day == daysFromCivil(s[0], @intCast(s[1]), @intCast(s[2]))) return .{ .closed = true };
    // early closes at 12:30 on the last trading day before Christmas and before New Year
    if (c.month == 12) {
        var eve = daysFromCivil(y, 12, 24);
        while (weekday(eve) >= 5) eve -= 1;
        var nye = daysFromCivil(y, 12, 31);
        while (weekday(nye) >= 5) nye -= 1;
        if (day == eve or day == nye) return .{ .early_close_sec = 12 * 3600 + 1800 };
    }
    return .{};
}

// ---------------------------------------------------------------------------
// CME Globex equity index (approximation)
// ---------------------------------------------------------------------------

fn cmeDayInfo(day: i64) Calendar.DayInfo {
    const c = civilFromDays(day);
    const y = c.year;
    const wd = weekday(day);
    if (wd >= 5) return .{ .closed = true };
    const new_year = daysFromCivil(y, 1, 1);
    if (day == new_year or (weekday(new_year) == 6 and day == new_year + 1)) return .{ .closed = true };
    if (day == easterSunday(y) - 2) return .{ .closed = true };
    if (day == observedUs(daysFromCivil(y, 12, 25))) return .{ .closed = true };
    const funerals = [_][3]i64{ .{ 2004, 6, 11 }, .{ 2007, 1, 2 }, .{ 2018, 12, 5 }, .{ 2025, 1, 9 } };
    for (funerals) |s| if (day == daysFromCivil(s[0], @intCast(s[1]), @intCast(s[2]))) return .{ .closed = true };
    const noon = 12 * 3600;
    if (day == nthWeekdayOfMonth(y, 1, 0, 3)) return .{ .early_close_sec = noon };
    if (day == nthWeekdayOfMonth(y, 2, 0, 3)) return .{ .early_close_sec = noon };
    if (day == lastWeekdayOfMonth(y, 5, 0)) return .{ .early_close_sec = noon };
    if (y >= 2022 and day == observedUs(daysFromCivil(y, 6, 19))) return .{ .early_close_sec = noon };
    if (day == observedUs(daysFromCivil(y, 7, 4))) return .{ .early_close_sec = noon };
    if (day == nthWeekdayOfMonth(y, 9, 0, 1)) return .{ .early_close_sec = noon };
    const thanksgiving = nthWeekdayOfMonth(y, 11, 3, 4);
    if (day == thanksgiving) return .{ .early_close_sec = noon };
    if (day == thanksgiving + 1) return .{ .early_close_sec = 12 * 3600 + 15 * 60 };
    return .{};
}

// ---------------------------------------------------------------------------
// Built-in calendars and the registry
// ---------------------------------------------------------------------------

fn weekdays5(open_sec: i32, close_sec: i32) [7]?DaySession {
    const d = DaySession{ .open_sec = open_sec, .close_sec = close_sec };
    return .{ d, d, d, d, d, null, null };
}

fn weekdays7(open_sec: i32, close_sec: i32) [7]?DaySession {
    const d = DaySession{ .open_sec = open_sec, .close_sec = close_sec };
    return .{ d, d, d, d, d, d, d };
}

pub const crypto = Calendar{ .name = "crypto", .weekly = weekdays7(0, 86400), .utc_offset_sec = 0, .dst = .none, .rules = .crypto, .sessions_per_year = 365 };
pub const fx = Calendar{ .name = "fx", .weekly = weekdays5(-7 * 3600, 17 * 3600), .utc_offset_sec = -5 * 3600, .dst = .us, .rules = .fx, .sessions_per_year = 260 };
pub const nyse = Calendar{ .name = "nyse", .weekly = weekdays5(9 * 3600 + 1800, 16 * 3600), .utc_offset_sec = -5 * 3600, .dst = .us, .rules = .nyse, .sessions_per_year = 252 };
pub const nasdaq = Calendar{ .name = "nasdaq", .weekly = weekdays5(9 * 3600 + 1800, 16 * 3600), .utc_offset_sec = -5 * 3600, .dst = .us, .rules = .nyse, .sessions_per_year = 252 };
pub const lse = Calendar{ .name = "lse", .weekly = weekdays5(8 * 3600, 16 * 3600 + 1800), .utc_offset_sec = 0, .dst = .eu, .rules = .lse, .sessions_per_year = 253 };
pub const cme = Calendar{ .name = "cme", .weekly = weekdays5(-7 * 3600, 16 * 3600), .utc_offset_sec = -6 * 3600, .dst = .us, .rules = .cme, .sessions_per_year = 252 };

const builtin = [_]*const Calendar{ &crypto, &fx, &nyse, &nasdaq, &lse, &cme };

var custom_mutex: std.Thread.Mutex = .{};
var custom_slots: [max_custom]?Calendar = [_]?Calendar{null} ** max_custom;
var custom_count: usize = 0;

/// Calendar by id (built-in or custom), null for 0 / unknown ids.
pub fn get(id: u32) ?*const Calendar {
    if (id == 0) return null;
    if (id <= builtin.len) return builtin[id - 1];
    if (id < first_custom_id) return null;
    const slot = id - first_custom_id;
    if (slot >= max_custom) return null;
    custom_mutex.lock();
    defer custom_mutex.unlock();
    if (custom_slots[slot]) |*c| return c;
    return null;
}

/// Id of a calendar by name (case-insensitive), 0 when unknown.
pub fn idByName(name: []const u8) u32 {
    for (builtin, 0..) |c, i| if (std.ascii.eqlIgnoreCase(c.name, name)) return @intCast(i + 1);
    custom_mutex.lock();
    defer custom_mutex.unlock();
    for (custom_slots, 0..) |slot, i| if (slot) |c| if (std.ascii.eqlIgnoreCase(c.name, name)) return @intCast(first_custom_id + i);
    return 0;
}

pub const DefineError = error{ TooManyCalendars, InvalidParameter, OutOfMemory };

/// Register a custom calendar (copies every slice with the C allocator).
/// `holidays` are local day numbers, `early_closes` per-day close seconds.
pub fn define(name: []const u8, weekly: [7]?DaySession, utc_offset_sec: i32, dst: DstRule, holidays: []const i32, early_closes: []const EarlyClose, sessions_per_year: f64) DefineError!u32 {
    if (name.len == 0 or !(sessions_per_year > 0)) return DefineError.InvalidParameter;
    for (weekly) |w| if (w) |d| if (d.close_sec <= d.open_sec) return DefineError.InvalidParameter;
    const alloc = std.heap.c_allocator;
    const name_copy = try alloc.dupe(u8, name);
    errdefer alloc.free(name_copy);
    const hol = try alloc.dupe(i32, holidays);
    errdefer alloc.free(hol);
    std.mem.sort(i32, hol, {}, std.sort.asc(i32));
    const early = try alloc.dupe(EarlyClose, early_closes);
    errdefer alloc.free(early);
    custom_mutex.lock();
    defer custom_mutex.unlock();
    // replace an existing calendar with the same name
    var slot: ?usize = null;
    for (custom_slots, 0..) |s, i| if (s) |c| if (std.mem.eql(u8, c.name, name)) {
        slot = i;
    };
    if (slot == null) {
        if (custom_count >= max_custom) return DefineError.TooManyCalendars;
        slot = custom_count;
        custom_count += 1;
    }
    custom_slots[slot.?] = .{ .name = name_copy, .weekly = weekly, .utc_offset_sec = utc_offset_sec, .dst = dst, .rules = .custom, .holidays = hol, .early_closes = early, .sessions_per_year = sessions_per_year };
    return @intCast(first_custom_id + slot.?);
}

/// UTC seconds of a civil date/time helper for tests and bindings.
pub fn utcSeconds(y: i64, mo: u32, d: u32, h: u32, mi: u32, s: u32) i64 {
    return daysFromCivil(y, mo, d) * DAY + @as(i64, h) * 3600 + @as(i64, mi) * 60 + s;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "civil date round trip and weekdays" {
    try testing.expectEqual(@as(i64, 0), daysFromCivil(1970, 1, 1));
    try testing.expectEqual(@as(u32, 3), weekday(0)); // Thursday
    try testing.expectEqual(@as(i64, 19723), daysFromCivil(2024, 1, 1));
    var d: i64 = -800000;
    while (d < 800000) : (d += 997) {
        const c = civilFromDays(d);
        try testing.expectEqual(d, daysFromCivil(c.year, c.month, c.day));
    }
    try testing.expectEqual(daysFromCivil(2025, 9, 1), nthWeekdayOfMonth(2025, 9, 0, 1)); // Labor Day 2025
    try testing.expectEqual(daysFromCivil(2025, 5, 26), lastWeekdayOfMonth(2025, 5, 0));
    try testing.expectEqual(daysFromCivil(2025, 4, 20), easterSunday(2025));
    try testing.expectEqual(daysFromCivil(2024, 3, 31), easterSunday(2024));
    try testing.expectEqual(daysFromCivil(2026, 4, 5), easterSunday(2026));
}

test "US and EU daylight saving" {
    // 2025: US DST 2025-03-09 .. 2025-11-02; EU 2025-03-30 .. 2025-10-26
    try testing.expect(!nyse.dstActive(utcSeconds(2025, 3, 9, 6, 59, 0)));
    try testing.expect(nyse.dstActive(utcSeconds(2025, 3, 9, 7, 0, 0)));
    try testing.expect(nyse.dstActive(utcSeconds(2025, 11, 2, 5, 59, 0)));
    try testing.expect(!nyse.dstActive(utcSeconds(2025, 11, 2, 6, 0, 0)));
    try testing.expect(!lse.dstActive(utcSeconds(2025, 3, 30, 0, 59, 0)));
    try testing.expect(lse.dstActive(utcSeconds(2025, 3, 30, 1, 0, 0)));
    try testing.expect(!lse.dstActive(utcSeconds(2025, 10, 26, 1, 0, 0)));
    // pre-2007 rule: 2000-04-02 .. 2000-10-29
    try testing.expect(!nyse.dstActive(utcSeconds(2000, 3, 15, 12, 0, 0)));
    try testing.expect(nyse.dstActive(utcSeconds(2000, 4, 2, 7, 0, 0)));
    try testing.expect(nyse.dstActive(utcSeconds(2000, 10, 29, 5, 59, 0)));
    try testing.expect(!nyse.dstActive(utcSeconds(2000, 10, 29, 6, 0, 0)));
    // NYSE open in summer is 13:30 UTC, in winter 14:30 UTC
    const s_summer = nyse.sessionForDay(daysFromCivil(2025, 7, 7)).?;
    try testing.expectEqual(utcSeconds(2025, 7, 7, 13, 30, 0), s_summer.open);
    try testing.expectEqual(utcSeconds(2025, 7, 7, 20, 0, 0), s_summer.close);
    const s_winter = nyse.sessionForDay(daysFromCivil(2025, 1, 6)).?;
    try testing.expectEqual(utcSeconds(2025, 1, 6, 14, 30, 0), s_winter.open);
    try testing.expectEqual(utcSeconds(2025, 1, 6, 21, 0, 0), s_winter.close);
}

test "NYSE holidays and early closes" {
    const closed = [_][3]i64{
        .{ 2025, 1, 1 },   .{ 2025, 1, 9 },   .{ 2025, 1, 20 },  .{ 2025, 2, 17 },  .{ 2025, 4, 18 },  .{ 2025, 5, 26 },
        .{ 2025, 6, 19 },  .{ 2025, 7, 4 },   .{ 2025, 9, 1 },   .{ 2025, 11, 27 }, .{ 2025, 12, 25 }, .{ 2026, 1, 1 },
        .{ 2026, 1, 19 },  .{ 2026, 2, 16 },  .{ 2026, 4, 3 },   .{ 2026, 5, 25 },  .{ 2026, 6, 19 },  .{ 2026, 7, 3 },
        .{ 2026, 9, 7 },   .{ 2026, 11, 26 }, .{ 2026, 12, 25 }, .{ 2027, 6, 18 },  .{ 2027, 7, 5 },   .{ 2027, 12, 24 },
        .{ 2021, 12, 24 }, .{ 2023, 1, 2 },   .{ 2024, 3, 29 },  .{ 2012, 10, 29 }, .{ 2001, 9, 11 },
    };
    for (closed) |c| {
        const day = daysFromCivil(c[0], @intCast(c[1]), @intCast(c[2]));
        try testing.expect(nyse.sessionForDay(day) == null);
    }
    const open_days = [_][3]i64{ .{ 2021, 12, 31 }, .{ 2022, 1, 3 }, .{ 2025, 7, 3 }, .{ 2025, 11, 28 }, .{ 2025, 12, 24 }, .{ 2026, 7, 2 }, .{ 2025, 9, 2 } };
    for (open_days) |c| {
        const day = daysFromCivil(c[0], @intCast(c[1]), @intCast(c[2]));
        try testing.expect(nyse.sessionForDay(day) != null);
    }
    // early closes at 13:00 local (17:00 UTC in summer, 18:00 UTC in winter)
    const jul3 = nyse.sessionForDay(daysFromCivil(2025, 7, 3)).?;
    try testing.expectEqual(@as(u64, 1), jul3.early_close);
    try testing.expectEqual(utcSeconds(2025, 7, 3, 17, 0, 0), jul3.close);
    const black_friday = nyse.sessionForDay(daysFromCivil(2025, 11, 28)).?;
    try testing.expectEqual(utcSeconds(2025, 11, 28, 18, 0, 0), black_friday.close);
    const xmas_eve = nyse.sessionForDay(daysFromCivil(2025, 12, 24)).?;
    try testing.expectEqual(@as(u64, 1), xmas_eve.early_close);
    // 2026-07-02 is a full day (Jul 3 is the observed holiday)
    try testing.expectEqual(@as(u64, 0), nyse.sessionForDay(daysFromCivil(2026, 7, 2)).?.early_close);
    try testing.expectEqual(@as(u64, 0), nyse.sessionForDay(daysFromCivil(2025, 9, 2)).?.early_close);
    // sessions per year sanity: 2025 has 250 NYSE trading days
    const n2025 = nyse.sessionsBetween(utcSeconds(2025, 1, 1, 0, 0, 0), utcSeconds(2026, 1, 1, 0, 0, 0));
    try testing.expectEqual(@as(u64, 250), n2025);
    const n2024 = nyse.sessionsBetween(utcSeconds(2024, 1, 1, 0, 0, 0), utcSeconds(2025, 1, 1, 0, 0, 0));
    try testing.expectEqual(@as(u64, 252), n2024);
}

test "LSE holidays" {
    const closed = [_][3]i64{
        .{ 2025, 1, 1 },   .{ 2025, 4, 18 },  .{ 2025, 4, 21 }, .{ 2025, 5, 5 }, .{ 2025, 5, 26 },  .{ 2025, 8, 25 },  .{ 2025, 12, 25 }, .{ 2025, 12, 26 },
        .{ 2022, 6, 2 },   .{ 2022, 6, 3 },   .{ 2022, 9, 19 }, .{ 2023, 5, 8 }, .{ 2021, 12, 27 }, .{ 2021, 12, 28 }, .{ 2022, 1, 3 },   .{ 2020, 5, 8 },
        .{ 2026, 12, 25 }, .{ 2026, 12, 28 },
    };
    for (closed) |c| try testing.expect(lse.sessionForDay(daysFromCivil(c[0], @intCast(c[1]), @intCast(c[2]))) == null);
    const open_days = [_][3]i64{ .{ 2022, 5, 30 }, .{ 2020, 5, 4 }, .{ 2025, 12, 24 }, .{ 2025, 12, 31 }, .{ 2026, 12, 24 }, .{ 2002, 5, 27 } };
    try testing.expect(lse.sessionForDay(daysFromCivil(2002, 6, 3)) == null);
    // 2000-12-24 is a Sunday: the early close moves to Friday 2000-12-22
    try testing.expectEqual(utcSeconds(2000, 12, 22, 12, 30, 0), lse.sessionForDay(daysFromCivil(2000, 12, 22)).?.close);
    for (open_days) |c| try testing.expect(lse.sessionForDay(daysFromCivil(c[0], @intCast(c[1]), @intCast(c[2]))) != null);
    const xmas_eve = lse.sessionForDay(daysFromCivil(2025, 12, 24)).?;
    try testing.expectEqual(utcSeconds(2025, 12, 24, 12, 30, 0), xmas_eve.close);
    const summer = lse.sessionForDay(daysFromCivil(2025, 7, 7)).?;
    try testing.expectEqual(utcSeconds(2025, 7, 7, 7, 0, 0), summer.open); // 08:00 BST
    try testing.expectEqual(utcSeconds(2025, 7, 7, 15, 30, 0), summer.close);
    try testing.expectEqual(@as(u64, 253), lse.sessionsBetween(utcSeconds(2025, 1, 1, 0, 0, 0), utcSeconds(2026, 1, 1, 0, 0, 0)));
}

test "FX, crypto and CME sessions" {
    // FX: Monday 2025-09-08 trade date opens Sunday 21:00 UTC (17:00 EDT)
    const mon = fx.sessionForDay(daysFromCivil(2025, 9, 8)).?;
    try testing.expectEqual(utcSeconds(2025, 9, 7, 21, 0, 0), mon.open);
    try testing.expectEqual(utcSeconds(2025, 9, 8, 21, 0, 0), mon.close);
    try testing.expect(fx.sessionForDay(daysFromCivil(2025, 9, 6)) == null); // Saturday
    try testing.expect(fx.isOpen(utcSeconds(2025, 9, 7, 22, 0, 0)));
    try testing.expect(!fx.isOpen(utcSeconds(2025, 9, 6, 12, 0, 0)));
    try testing.expect(!fx.isOpen(utcSeconds(2025, 9, 5, 21, 30, 0))); // after Friday close
    // the Friday session belongs to trade day Friday; Sunday 20:59 UTC is still closed
    try testing.expect(fx.sessionAt(utcSeconds(2025, 9, 7, 20, 59, 0)) == null);
    try testing.expectEqual(daysFromCivil(2025, 9, 8), fx.nextSession(utcSeconds(2025, 9, 6, 12, 0, 0)).?.trade_day);
    try testing.expectEqual(daysFromCivil(2025, 9, 5), fx.prevSession(utcSeconds(2025, 9, 6, 12, 0, 0)).?.trade_day);
    // crypto: always open, UTC days
    const any = crypto.sessionAt(utcSeconds(2025, 9, 6, 12, 0, 0)).?;
    try testing.expectEqual(utcSeconds(2025, 9, 6, 0, 0, 0), any.open);
    try testing.expectEqual(utcSeconds(2025, 9, 7, 0, 0, 0), any.close);
    try testing.expectEqual(@as(i64, 7 * DAY), crypto.openSecondsBetween(utcSeconds(2025, 9, 1, 0, 0, 0), utcSeconds(2025, 9, 8, 0, 0, 0)));
    // CME: Monday session opens Sunday 17:00 CT (22:00 UTC in September), closes Monday 16:00 CT (21:00 UTC)
    const es = cme.sessionForDay(daysFromCivil(2025, 9, 8)).?;
    try testing.expectEqual(utcSeconds(2025, 9, 7, 22, 0, 0), es.open);
    try testing.expectEqual(utcSeconds(2025, 9, 8, 21, 0, 0), es.close);
    try testing.expect(cme.sessionForDay(daysFromCivil(2025, 12, 25)) == null);
    try testing.expectEqual(@as(u64, 1), cme.sessionForDay(daysFromCivil(2025, 9, 1)).?.early_close);
}

test "session navigation and open time" {
    // Friday 2025-09-05 15:00 UTC (11:00 EDT, open) -> prev/next/at
    const t = utcSeconds(2025, 9, 5, 15, 0, 0);
    try testing.expect(nyse.isOpen(t));
    try testing.expectEqual(daysFromCivil(2025, 9, 5), nyse.sessionAt(t).?.trade_day);
    try testing.expectEqual(daysFromCivil(2025, 9, 5), nyse.prevSession(t).?.trade_day);
    try testing.expectEqual(daysFromCivil(2025, 9, 5), nyse.nextSession(t).?.trade_day);
    // Saturday: prev = Friday, next = Monday (2025-09-08)
    const sat = utcSeconds(2025, 9, 6, 12, 0, 0);
    try testing.expect(!nyse.isOpen(sat));
    try testing.expectEqual(daysFromCivil(2025, 9, 5), nyse.prevSession(sat).?.trade_day);
    try testing.expectEqual(daysFromCivil(2025, 9, 8), nyse.nextSession(sat).?.trade_day);
    // Labor Day weekend 2025-08-29 (Fri) -> 2025-09-02 (Tue)
    const fri = nyse.sessionForDay(daysFromCivil(2025, 8, 29)).?;
    try testing.expectEqual(daysFromCivil(2025, 9, 2), nyse.sessionAfter(fri).?.trade_day);
    try testing.expectEqual(daysFromCivil(2025, 8, 28), nyse.sessionBefore(fri).?.trade_day);
    // trading seconds between Friday 15:00 UTC and Tuesday 15:00 UTC: 5h Friday + 1.5h Tuesday
    const open_secs = nyse.openSecondsBetween(utcSeconds(2025, 8, 29, 15, 0, 0), utcSeconds(2025, 9, 2, 15, 0, 0));
    try testing.expectEqual(@as(i64, 5 * 3600 + 5400), open_secs);
    try testing.expectEqual(@as(u64, 1), nyse.sessionsBetween(utcSeconds(2025, 8, 29, 15, 0, 0), utcSeconds(2025, 9, 2, 15, 0, 0)));
    // periods per year
    try testing.expectApproxEqAbs(@as(f64, 252 * 390), nyse.periodsPerYear(60), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, 252), nyse.periodsPerYear(86400), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, 365.25 / 7.0), nyse.periodsPerYear(7 * 86400), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, 365 * 1440), crypto.periodsPerYear(60), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, 365), crypto.periodsPerYear(86400), 1e-9);
}

test "custom calendar registry" {
    try testing.expectEqual(@as(u32, 3), idByName("NYSE"));
    try testing.expectEqual(@as(u32, 0), idByName("nope"));
    try testing.expect(get(0) == null);
    try testing.expectEqualStrings("lse", get(5).?.name);
    // a 4-day week with a lunch break-free 10:00-15:00 in UTC+9, one holiday and one early close
    var weekly: [7]?DaySession = .{null} ** 7;
    for (0..4) |i| weekly[i] = .{ .open_sec = 10 * 3600, .close_sec = 15 * 3600 };
    const hol = [_]i32{@intCast(daysFromCivil(2025, 9, 9))};
    const early = [_]EarlyClose{.{ .day = @intCast(daysFromCivil(2025, 9, 10)), .close_sec = 12 * 3600 }};
    const id = try define("test_custom", weekly, 9 * 3600, .none, &hol, &early, 200);
    try testing.expect(id >= first_custom_id);
    try testing.expectEqual(id, idByName("test_custom"));
    const cal = get(id).?;
    const mon = cal.sessionForDay(daysFromCivil(2025, 9, 8)).?;
    try testing.expectEqual(utcSeconds(2025, 9, 8, 1, 0, 0), mon.open); // 10:00 UTC+9
    try testing.expectEqual(utcSeconds(2025, 9, 8, 6, 0, 0), mon.close);
    try testing.expect(cal.sessionForDay(daysFromCivil(2025, 9, 9)) == null); // holiday
    try testing.expectEqual(utcSeconds(2025, 9, 10, 3, 0, 0), cal.sessionForDay(daysFromCivil(2025, 9, 10)).?.close);
    try testing.expect(cal.sessionForDay(daysFromCivil(2025, 9, 12)) == null); // Friday not in the template
    // redefining the same name reuses the id
    const id2 = try define("test_custom", weekly, 9 * 3600, .none, &.{}, &.{}, 200);
    try testing.expectEqual(id, id2);
    try testing.expect(get(id).?.sessionForDay(daysFromCivil(2025, 9, 9)) != null);
    try testing.expectError(DefineError.InvalidParameter, define("", weekly, 0, .none, &.{}, &.{}, 200));
}
