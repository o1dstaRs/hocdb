const std = @import("std");
const calendar = @import("hocdb").calendar;
const universe = @import("hocdb").universe_mod;
const backtest = @import("hocdb").backtest_mod;
const hocdb = @import("hocdb");

const DB = hocdb.DynamicTimeSeriesDB;

// --- C-ABI Exports (for C/C++ bindings) ---

pub const CField = extern struct {
    name: [*:0]const u8,
    type: c_int, // 1=i64, 2=f64, 3=u64, 5=string, 6=bool
};

pub const CFilter = extern struct {
    field_index: usize,
    type: c_int,
    val_i64: i64,
    val_f64: f64,
    val_u64: u64,
    val_string: [128]u8,
    val_bool: bool,
};

export fn hocdb_init(ticker_z: [*:0]const u8, path_z: [*:0]const u8, schema_ptr: [*]const CField, schema_len: usize, max_size: i64, overwrite: c_int, flush: c_int, auto_increment: c_int) ?*anyopaque {
    const ticker = std.mem.span(ticker_z);
    const path = std.mem.span(path_z);

    // Convert C schema to Zig schema
    const fields = std.heap.c_allocator.alloc(hocdb.FieldInfo, schema_len) catch return null;
    var i: usize = 0;
    while (i < schema_len) : (i += 1) {
        const c_field = schema_ptr[i];
        const name_len = std.mem.len(c_field.name);
        const name = std.heap.c_allocator.alloc(u8, name_len) catch {
            // Clean up previously allocated names if OOM
            var j: usize = 0;
            while (j < i) : (j += 1) {
                std.heap.c_allocator.free(fields[j].name);
            }
            std.heap.c_allocator.free(fields);
            return null;
        };
        @memcpy(name, c_field.name[0..name_len]);

        const f_type: hocdb.FieldType = switch (c_field.type) {
            1 => .i64,
            2 => .f64,
            3 => .u64,
            5 => .string,
            6 => .bool,
            else => {
                std.heap.c_allocator.free(name); // Free current name
                var j: usize = 0; // Free previous names
                while (j < i) : (j += 1) {
                    std.heap.c_allocator.free(fields[j].name);
                }
                std.heap.c_allocator.free(fields); // Free fields array
                return null;
            },
        };
        fields[i] = .{ .name = name, .type = f_type };
    }
    // We leak names here because we don't have a clean way to free them in this function after init?
    // Actually init doesn't take ownership. So we should free them.
    defer {
        for (fields) |f| std.heap.c_allocator.free(f.name);
        std.heap.c_allocator.free(fields);
    }

    var config = DB.Config{};
    if (max_size > 0) config.max_file_size = @intCast(max_size);
    config.overwrite_on_full = (overwrite != 0);
    config.flush_on_write = (flush != 0);
    config.auto_increment = (auto_increment != 0);

    const ticker_dupe = std.heap.c_allocator.dupe(u8, ticker) catch return null;
    const path_dupe = std.heap.c_allocator.dupe(u8, path) catch {
        std.heap.c_allocator.free(ticker_dupe);
        return null;
    };

    const schema = hocdb.Schema{ .fields = fields };

    const db_ptr = std.heap.c_allocator.create(DB) catch {
        std.heap.c_allocator.free(ticker_dupe);
        std.heap.c_allocator.free(path_dupe);
        return null;
    };
    db_ptr.* = DB.init(ticker_dupe, path_dupe, std.heap.c_allocator, schema, config) catch |err| {
        setLastError(@errorName(err));
        std.heap.c_allocator.free(ticker_dupe);
        std.heap.c_allocator.free(path_dupe);
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };
    db_ptr.initWriter() catch {
        std.heap.c_allocator.free(ticker_dupe);
        std.heap.c_allocator.free(path_dupe);
        db_ptr.deinit(); // Clean up file handle
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };

    std.heap.c_allocator.free(ticker_dupe);
    std.heap.c_allocator.free(path_dupe);

    return db_ptr;
}

export fn hocdb_append(db_ptr: *anyopaque, data_ptr: [*]const u8, data_len: usize) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.append(data_ptr[0..data_len]) catch |err| {
        if (err == error.InvalidRecordSize) return -2;
        if (err == error.TimestampNotMonotonic) return -3;
        if (err == error.ReadOnly) return -10;
        std.debug.print("HOCDB Append Error: {s}\n", .{@errorName(err)});
        return -1;
    };
    return 0;
}

export fn hocdb_flush(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.flush() catch return -1;
    return 0;
}

export fn hocdb_load(db_ptr: *anyopaque, out_len: *usize) ?[*]u8 {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.flush() catch return null;
    const data = db.load(std.heap.c_allocator) catch return null;
    out_len.* = data.len;
    if (data.len == 0) {
        // If data is empty, we must ensure we don't return a pointer that might be unsafe to free later
        // or that TS might misinterpret.
        // We free it now and return null.
        std.heap.c_allocator.free(data);
        return null;
    }
    return data.ptr;
}

export fn hocdb_query_into(
    handle: ?*anyopaque,
    start_ts: i64,
    end_ts: i64,
    filters: ?[*]const CFilter,
    filters_len: usize,
    buffer_ptr: [*]u8,
    buffer_len: usize,
) i64 {
    if (handle) |h| {
        var db = @as(*DB, @ptrCast(@alignCast(h)));
        const f_len = filters_len;
        var zig_filters: []hocdb.Filter = undefined;

        if (f_len > 0 and filters != null) {
            zig_filters = std.heap.c_allocator.alloc(hocdb.Filter, f_len) catch return -1;
            defer std.heap.c_allocator.free(zig_filters);

            var i: usize = 0;
            while (i < f_len) : (i += 1) {
                const cf = filters.?[i];
                zig_filters[i] = fromCFilter(cf);
            }
        } else {
            zig_filters = std.heap.c_allocator.alloc(hocdb.Filter, 0) catch return -1;
            defer std.heap.c_allocator.free(zig_filters);
        }

        const buffer = buffer_ptr[0..buffer_len];
        const bytes_written = db.queryInto(start_ts, end_ts, zig_filters, buffer) catch |err| {
            if (err == error.BufferTooSmall) return -2;
            return -1;
        };
        return @as(i64, @intCast(bytes_written));
    }
    return -1;
}

export fn hocdb_query(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, filters_ptr: [*]const CFilter, filters_len: usize, out_len: *usize) ?[*]u8 {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.flush() catch return null;

    // Convert C filters to Zig filters
    const filters = std.heap.c_allocator.alloc(hocdb.Filter, filters_len) catch return null;
    defer std.heap.c_allocator.free(filters);

    for (0..filters_len) |i| {
        const cf = filters_ptr[i];
        filters[i] = .{
            .field_index = cf.field_index,
            .value = switch (cf.type) {
                1 => .{ .i64 = cf.val_i64 },
                2 => .{ .f64 = cf.val_f64 },
                3 => .{ .u64 = cf.val_u64 },
                5 => .{ .string = cf.val_string },
                6 => .{ .bool = cf.val_bool },
                else => return null, // Invalid type
            },
        };
    }

    const data = db.query(start_ts, end_ts, filters, std.heap.c_allocator) catch return null;
    out_len.* = data.len;
    if (data.len == 0) {
        std.heap.c_allocator.free(data);
        return null;
    }
    return data.ptr;
}

export fn hocdb_get_stats(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, field_index: usize, flags: u32, out_stats: *hocdb.Stats) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const compute_percentiles = (flags & 1) != 0;
    const stats = db.getStats(start_ts, end_ts, field_index, compute_percentiles) catch return -1;
    out_stats.* = stats;
    return 0;
}

export fn hocdb_get_latest(db_ptr: *anyopaque, field_index: usize, out_val: *f64, out_ts: *i64) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const latest = db.getLatest(field_index) catch return -1;
    out_val.* = latest.value;
    out_ts.* = latest.timestamp;
    return 0;
}

export fn hocdb_close(db_ptr: *anyopaque) void {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.deinit();
    std.heap.c_allocator.destroy(db);
}

export fn hocdb_drop(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.drop() catch return -1;
    std.heap.c_allocator.destroy(db);
    return 0;
}

export fn hocdb_free(ptr: ?*anyopaque) void {
    if (ptr) |p| {
        std.c.free(p);
    }
}

export fn hocdb_get_field_index(db_ptr: *anyopaque, field_name_z: [*:0]const u8) isize {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const field_name = std.mem.span(field_name_z);

    for (db.fields, 0..) |field, i| {
        if (std.mem.eql(u8, field.name, field_name)) {
            return @intCast(i);
        }
    }
    return -1;
}

fn fromCFilter(cf: CFilter) hocdb.Filter {
    return .{
        .field_index = cf.field_index,
        .value = switch (cf.type) {
            1 => .{ .i64 = cf.val_i64 },
            2 => .{ .f64 = cf.val_f64 },
            3 => .{ .u64 = cf.val_u64 },
            5 => .{ .string = cf.val_string }, // This copies the array.
            6 => .{ .bool = cf.val_bool },
            else => .{ .i64 = 0 }, // Should not happen if pre-validated or we trust C caller
        },
    };
}

// ---------------------------------------------------------------------------
// Indicators / analytics
// ---------------------------------------------------------------------------

const ind = hocdb.indicators;

pub const CIndicatorResult = extern struct {
    timestamps: ?[*]i64,
    values: ?[*]f64,
    n_rows: usize,
    n_outputs: usize,
};

pub const CBars = extern struct {
    timestamps: ?[*]i64,
    open: ?[*]f64,
    high: ?[*]f64,
    low: ?[*]f64,
    close: ?[*]f64,
    volume: ?[*]f64,
    count: ?[*]f64,
    n_bars: usize,
};

fn errCode(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => -1,
        error.InvalidParameter, error.InvalidPeriod => -2,
        error.MissingColumn, error.MissingCloseColumn => -3,
        error.InvalidFieldIndex => -4,
        error.FieldOverrideNotSupportedWithBucket => -5,
        error.TooManyColumns => -6,
        error.LengthMismatch => -7,
        error.ReadOnly => -10,
        error.DatabaseLocked => -11,
        error.ChecksumMismatch => -12,
        error.ChecksumUnavailable => -20,
        error.EmptyDatabase => -21,
        error.CalendarRequired => -30,
        error.UnknownCalendar => -31,
        else => -100,
    };
}

fn fillResult(res: DB.IndicatorResult, out: *CIndicatorResult) void {
    out.n_rows = res.n_rows;
    out.n_outputs = res.n_outputs;
    out.timestamps = if (res.timestamps.len > 0) res.timestamps.ptr else null;
    out.values = if (res.values.len > 0) res.values.ptr else null;
    // ownership passes to the caller (freed with hocdb_indicators_free)
    if (res.timestamps.len == 0) std.heap.c_allocator.free(res.timestamps);
    if (res.values.len == 0) std.heap.c_allocator.free(res.values);
}

/// Batch indicator computation over [start_ts, end_ts). See hocdb.h.
export fn hocdb_indicators(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, cols: *const DB.IndicatorColumns, specs_ptr: ?[*]const ind.Spec, n_specs: usize, lookback: usize, bucket: i64, out: *CIndicatorResult) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = .{ .timestamps = null, .values = null, .n_rows = 0, .n_outputs = 0 };
    const specs: []const ind.Spec = if (specs_ptr) |p| p[0..n_specs] else &[_]ind.Spec{};
    const res = db.indicatorsRange(start_ts, end_ts, cols.*, specs, lookback, bucket, std.heap.c_allocator) catch |err| return errCode(err);
    fillResult(res, out);
    return 0;
}

/// Batch indicator computation for the last `n_last` records / bars.
export fn hocdb_indicators_tail(db_ptr: *anyopaque, n_last: usize, cols: *const DB.IndicatorColumns, specs_ptr: ?[*]const ind.Spec, n_specs: usize, lookback: usize, bucket: i64, out: *CIndicatorResult) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = .{ .timestamps = null, .values = null, .n_rows = 0, .n_outputs = 0 };
    const specs: []const ind.Spec = if (specs_ptr) |p| p[0..n_specs] else &[_]ind.Spec{};
    const res = db.indicatorsTail(n_last, cols.*, specs, lookback, bucket, std.heap.c_allocator) catch |err| return errCode(err);
    fillResult(res, out);
    return 0;
}

export fn hocdb_indicators_free(res: *CIndicatorResult) void {
    if (res.timestamps) |p| std.heap.c_allocator.free(p[0..res.n_rows]);
    if (res.values) |p| std.heap.c_allocator.free(p[0 .. res.n_rows * res.n_outputs]);
    res.* = .{ .timestamps = null, .values = null, .n_rows = 0, .n_outputs = 0 };
}

/// Number of output series of a kind (0 for an unknown kind).
export fn hocdb_indicator_output_count(kind: u32) usize {
    const k = ind.Kind.fromInt(kind) orelse return 0;
    return ind.outputCount(k);
}

/// Name of output `idx` of a kind, or NULL.
export fn hocdb_indicator_output_name(kind: u32, idx: usize) ?[*:0]const u8 {
    const k = ind.Kind.fromInt(kind) orelse return null;
    const names = ind.outputNames(k);
    if (idx >= names.len) return null;
    return zName(names[idx]);
}

/// Name of a kind ("rsi", "macd", ...), or NULL.
export fn hocdb_indicator_name(kind: u32) ?[*:0]const u8 {
    const k = ind.Kind.fromInt(kind) orelse return null;
    return zName(ind.kindName(k));
}

/// Kind id for a name (case-insensitive), or 0 if unknown.
export fn hocdb_indicator_kind_from_name(name_z: [*:0]const u8) u32 {
    const name = std.mem.span(name_z);
    for (ind.all_kinds) |k| {
        if (std.ascii.eqlIgnoreCase(name, @tagName(k))) return @intFromEnum(k);
    }
    return 0;
}

/// Enumerate valid kind ids into `out` (up to `cap`); returns the total count.
export fn hocdb_indicator_kinds(out: ?[*]u32, cap: usize) usize {
    if (out) |o| {
        for (ind.all_kinds, 0..) |k, i| {
            if (i >= cap) break;
            o[i] = @intFromEnum(k);
        }
    }
    return ind.all_kinds.len;
}

/// Recommended warm-up rows for a spec (after applying defaults); 0 on error.
export fn hocdb_indicator_warmup(spec: *const ind.Spec) usize {
    const p = ind.resolve(spec.*) catch return 0;
    return ind.warmup(p);
}

/// Aggregate records into OHLCV bars. See hocdb.h.
export fn hocdb_ohlcv(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, price_field: usize, volume_field: i64, bucket: i64, out: *CBars) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = .{ .timestamps = null, .open = null, .high = null, .low = null, .close = null, .volume = null, .count = null, .n_bars = 0 };
    const bars = db.ohlcv(start_ts, end_ts, price_field, volume_field, bucket, std.heap.c_allocator) catch |err| return errCode(err);
    out.n_bars = bars.len();
    if (bars.len() == 0) {
        bars.deinit(std.heap.c_allocator);
        return 0;
    }
    out.timestamps = bars.ts.ptr;
    out.open = bars.open.ptr;
    out.high = bars.high.ptr;
    out.low = bars.low.ptr;
    out.close = bars.close.ptr;
    out.volume = bars.volume.ptr;
    out.count = bars.count.ptr;
    std.heap.c_allocator.free(bars.buy_volume); // not exposed by the legacy struct
    return 0;
}

export fn hocdb_ohlcv_free(bars: *CBars) void {
    const n = bars.n_bars;
    if (bars.timestamps) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.open) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.high) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.low) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.close) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.volume) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.count) |p| std.heap.c_allocator.free(p[0..n]);
    bars.* = .{ .timestamps = null, .open = null, .high = null, .low = null, .close = null, .volume = null, .count = null, .n_bars = 0 };
}

/// Scalar risk / performance summary of a field over [start_ts, end_ts).
export fn hocdb_summary(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, field_index: usize, periods_per_year: f64, out: *ind.Summary) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = db.summary(start_ts, end_ts, field_index, periods_per_year, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

/// One-shot indicator snapshot for the latest bar.
export fn hocdb_snapshot(db_ptr: *anyopaque, cols: *const DB.IndicatorColumns, n_bars: usize, bucket: i64, periods_per_year: f64, out: *ind.Snapshot) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = db.snapshot(cols.*, n_bars, bucket, periods_per_year, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

// Struct introspection so bindings can decode Summary / Snapshot by index
// without hard-coding field lists.
export fn hocdb_summary_size() usize {
    return @sizeOf(ind.Summary);
}
export fn hocdb_summary_field_count() usize {
    return @typeInfo(ind.Summary).@"struct".fields.len;
}
export fn hocdb_summary_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(ind.Summary, idx);
}
export fn hocdb_summary_field_offset(idx: usize) usize {
    return structFieldOffset(ind.Summary, idx);
}
export fn hocdb_snapshot_size() usize {
    return @sizeOf(ind.Snapshot);
}
export fn hocdb_snapshot_field_count() usize {
    return @typeInfo(ind.Snapshot).@"struct".fields.len;
}
export fn hocdb_snapshot_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(ind.Snapshot, idx);
}
export fn hocdb_snapshot_field_offset(idx: usize) usize {
    return structFieldOffset(ind.Snapshot, idx);
}
/// Field type code for summary/snapshot fields: 1 = i64, 2 = f64, 3 = u64.
export fn hocdb_summary_field_type(idx: usize) c_int {
    return structFieldType(ind.Summary, idx);
}
export fn hocdb_snapshot_field_type(idx: usize) c_int {
    return structFieldType(ind.Snapshot, idx);
}

fn structFieldName(comptime T: type, idx: usize) ?[*:0]const u8 {
    inline for (@typeInfo(T).@"struct".fields, 0..) |f, i| {
        if (i == idx) return f.name ++ "";
    }
    return null;
}

fn structFieldOffset(comptime T: type, idx: usize) usize {
    inline for (@typeInfo(T).@"struct".fields, 0..) |f, i| {
        if (i == idx) return @offsetOf(T, f.name);
    }
    return 0;
}

fn structFieldType(comptime T: type, idx: usize) c_int {
    inline for (@typeInfo(T).@"struct".fields, 0..) |f, i| {
        if (i == idx) return switch (f.type) {
            i64 => 1,
            f64 => 2,
            u64 => 3,
            else => 0,
        };
    }
    return 0;
}

// Static zero-terminated copies of enum/output names.
fn zName(name: []const u8) ?[*:0]const u8 {
    // Names come from comptime string literals; find the matching static.
    inline for (ind.all_kinds) |k| {
        if (std.mem.eql(u8, name, @tagName(k))) return @tagName(k);
    }
    inline for (.{ "value", "macd", "signal", "hist", "ppo", "adx", "plus_di", "minus_di", "up", "down", "osc", "upper", "middle", "lower", "k", "d", "sar", "dir", "line", "plus", "minus", "tsi", "tenkan", "kijun", "senkou_a", "senkou_b", "chikou", "percent_b", "bandwidth", "slope", "intercept", "r2", "open", "high", "low", "close", "abs", "bps", "net", "imbalance", "trades_per_sec", "volume_per_sec", "ret", "max", "min", "label", "bars", "breakout", "pp", "r1", "s1", "r2", "s2" }) |s| {
        if (std.mem.eql(u8, name, s)) return s;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Pairs, microstructure bars, health, evaluation, multi-timeframe snapshots
// ---------------------------------------------------------------------------

pub const CBarsEx = extern struct {
    timestamps: ?[*]i64,
    open: ?[*]f64,
    high: ?[*]f64,
    low: ?[*]f64,
    close: ?[*]f64,
    volume: ?[*]f64,
    count: ?[*]f64,
    n_bars: usize,
    buy_volume: ?[*]f64,
};

fn fillBars(bars: ind.Bars, with_side: bool, out: *CBarsEx) void {
    out.* = .{ .timestamps = null, .open = null, .high = null, .low = null, .close = null, .volume = null, .count = null, .n_bars = bars.len(), .buy_volume = null };
    if (bars.len() == 0) {
        bars.deinit(std.heap.c_allocator);
        return;
    }
    out.timestamps = bars.ts.ptr;
    out.open = bars.open.ptr;
    out.high = bars.high.ptr;
    out.low = bars.low.ptr;
    out.close = bars.close.ptr;
    out.volume = bars.volume.ptr;
    out.count = bars.count.ptr;
    if (with_side) {
        out.buy_volume = bars.buy_volume.ptr;
    } else {
        std.heap.c_allocator.free(bars.buy_volume);
    }
}

/// OHLCV bars with buy volume (side_field >= 0, 1 = buy). See hocdb.h.
export fn hocdb_ohlcv_ex(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, price_field: usize, volume_field: i64, side_field: i64, bucket: i64, out: *CBarsEx) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = .{ .timestamps = null, .open = null, .high = null, .low = null, .close = null, .volume = null, .count = null, .n_bars = 0, .buy_volume = null };
    const bars = db.ohlcvSide(start_ts, end_ts, price_field, volume_field, side_field, bucket, std.heap.c_allocator) catch |err| return errCode(err);
    fillBars(bars, side_field >= 0, out);
    return 0;
}

export fn hocdb_ohlcv_ex_free(bars: *CBarsEx) void {
    const n = bars.n_bars;
    if (bars.timestamps) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.open) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.high) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.low) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.close) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.volume) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.count) |p| std.heap.c_allocator.free(p[0..n]);
    if (bars.buy_volume) |p| std.heap.c_allocator.free(p[0..n]);
    bars.* = .{ .timestamps = null, .open = null, .high = null, .low = null, .close = null, .volume = null, .count = null, .n_bars = 0, .buy_volume = null };
}

/// Indicators over database A aligned with database B (see hocdb.h).
export fn hocdb_pair_indicators(db_a: *anyopaque, cols_a: *const DB.IndicatorColumns, db_b: *anyopaque, cols_b: *const DB.IndicatorColumns, start_ts: i64, end_ts: i64, specs_ptr: ?[*]const ind.Spec, n_specs: usize, lookback: usize, bucket: i64, out: *CIndicatorResult) c_int {
    const a = @as(*DB, @ptrCast(@alignCast(db_a)));
    const b = @as(*DB, @ptrCast(@alignCast(db_b)));
    out.* = .{ .timestamps = null, .values = null, .n_rows = 0, .n_outputs = 0 };
    const specs: []const ind.Spec = if (specs_ptr) |p| p[0..n_specs] else &[_]ind.Spec{};
    const res = a.pairRange(b, cols_a.*, cols_b.*, start_ts, end_ts, specs, lookback, bucket, std.heap.c_allocator) catch |err| return errCode(err);
    fillResult(res, out);
    return 0;
}

export fn hocdb_pair_indicators_tail(db_a: *anyopaque, cols_a: *const DB.IndicatorColumns, db_b: *anyopaque, cols_b: *const DB.IndicatorColumns, n_last: usize, specs_ptr: ?[*]const ind.Spec, n_specs: usize, lookback: usize, bucket: i64, out: *CIndicatorResult) c_int {
    const a = @as(*DB, @ptrCast(@alignCast(db_a)));
    const b = @as(*DB, @ptrCast(@alignCast(db_b)));
    out.* = .{ .timestamps = null, .values = null, .n_rows = 0, .n_outputs = 0 };
    const specs: []const ind.Spec = if (specs_ptr) |p| p[0..n_specs] else &[_]ind.Spec{};
    const res = a.pairTail(b, cols_a.*, cols_b.*, n_last, specs, lookback, bucket, std.heap.c_allocator) catch |err| return errCode(err);
    fillResult(res, out);
    return 0;
}

/// 1 when the kind's outputs depend on future rows (labels), 0 otherwise, -1 unknown.
export fn hocdb_indicator_is_lookahead(kind: u32) c_int {
    const k = ind.Kind.fromInt(kind) orelse return -1;
    return if (ind.isLookahead(k)) 1 else 0;
}

/// Data-quality statistics (see hocdb.h).
export fn hocdb_health(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, price_field: usize, volume_field: i64, gap_threshold: i64, outlier_threshold: f64, out: *ind.Health) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = db.health(start_ts, end_ts, price_field, volume_field, gap_threshold, outlier_threshold, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

export fn hocdb_health_size() usize {
    return @sizeOf(ind.Health);
}
export fn hocdb_health_field_count() usize {
    return @typeInfo(ind.Health).@"struct".fields.len;
}
export fn hocdb_health_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(ind.Health, idx);
}
export fn hocdb_health_field_offset(idx: usize) usize {
    return structFieldOffset(ind.Health, idx);
}
export fn hocdb_health_field_type(idx: usize) c_int {
    return structFieldType(ind.Health, idx);
}

/// Evaluate decisions (see hocdb.h). Optional per-decision arrays may be NULL.
export fn hocdb_evaluate(db_ptr: *anyopaque, price_field: usize, decisions_ptr: ?[*]const ind.Decision, n: usize, default_horizon: i64, cost_bps: f64, out: *ind.Evaluation, out_entry: ?[*]f64, out_exit: ?[*]f64, out_net: ?[*]f64) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const decisions: []const ind.Decision = if (decisions_ptr) |p| p[0..n] else &[_]ind.Decision{};
    out.* = db.evaluate(price_field, decisions, default_horizon, cost_bps, if (out_entry) |p| p[0..n] else null, if (out_exit) |p| p[0..n] else null, if (out_net) |p| p[0..n] else null, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

export fn hocdb_evaluation_size() usize {
    return @sizeOf(ind.Evaluation);
}
export fn hocdb_evaluation_field_count() usize {
    return @typeInfo(ind.Evaluation).@"struct".fields.len;
}
export fn hocdb_evaluation_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(ind.Evaluation, idx);
}
export fn hocdb_evaluation_field_offset(idx: usize) usize {
    return structFieldOffset(ind.Evaluation, idx);
}
export fn hocdb_evaluation_field_type(idx: usize) c_int {
    return structFieldType(ind.Evaluation, idx);
}
export fn hocdb_decision_size() usize {
    return @sizeOf(ind.Decision);
}

/// Snapshots for several bar sizes from one read (see hocdb.h).
export fn hocdb_snapshot_multi(db_ptr: *anyopaque, cols: *const DB.IndicatorColumns, n_bars: usize, buckets: [*]const i64, n_buckets: usize, ppy: [*]const f64, out: [*]ind.Snapshot) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.snapshotMulti(cols.*, n_bars, buckets[0..n_buckets], ppy[0..n_buckets], out[0..n_buckets], std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

// ---------------------------------------------------------------------------
// Durability, readers, maintenance and metrics
// ---------------------------------------------------------------------------

/// Extended configuration (see hocdb.h HOCDBConfig).
pub const CConfig = extern struct {
    max_file_size: i64,
    overwrite_on_full: c_int,
    flush_on_write: c_int,
    auto_increment: c_int,
    fsync_policy: c_int, // 0 none, 1 on_close, 2 on_flush, 3 interval
    fsync_interval_ms: u32,
    verify_on_open: c_int,
    retention_span: i64,
    rollover_size: u64,
    auto_migrate: c_int,
    timestamp_unit_ns: u64,
    index_stride: u64,
    calendar: u32, // trading calendar id (hocdb_calendar_id), 0 = none
    reserved0: u32,
};

comptime {
    std.debug.assert(@sizeOf(CConfig) == 80);
}

threadlocal var last_error: [64]u8 = [_]u8{0} ** 64;
threadlocal var last_error_len: usize = 0;

fn setLastError(name: []const u8) void {
    const n = @min(name.len, last_error.len - 1);
    @memcpy(last_error[0..n], name[0..n]);
    last_error[n] = 0;
    last_error_len = n;
}

/// Name of the error of the last failed init/open on this thread ("" if none).
export fn hocdb_last_error() [*:0]const u8 {
    return @ptrCast(&last_error);
}

fn configFromC(c: *const CConfig) DB.Config {
    var config = DB.Config{};
    if (c.max_file_size > 0) config.max_file_size = @intCast(c.max_file_size);
    config.overwrite_on_full = c.overwrite_on_full != 0;
    config.flush_on_write = c.flush_on_write != 0;
    config.auto_increment = c.auto_increment != 0;
    config.fsync = switch (c.fsync_policy) {
        0 => .none,
        2 => .on_flush,
        3 => .interval,
        else => .on_close,
    };
    if (c.fsync_interval_ms > 0) config.fsync_interval_ms = c.fsync_interval_ms;
    config.verify_on_open = c.verify_on_open != 0;
    config.retention_span = c.retention_span;
    config.rollover_size = c.rollover_size;
    config.auto_migrate = c.auto_migrate != 0;
    config.timestamp_unit_ns = c.timestamp_unit_ns;
    if (c.index_stride > 0) config.index_stride = c.index_stride;
    config.calendar = c.calendar;
    return config;
}

fn schemaFromC(schema_ptr: [*]const CField, schema_len: usize) ?[]hocdb.FieldInfo {
    const fields = std.heap.c_allocator.alloc(hocdb.FieldInfo, schema_len) catch return null;
    var i: usize = 0;
    while (i < schema_len) : (i += 1) {
        const c_field = schema_ptr[i];
        const name = std.heap.c_allocator.dupe(u8, std.mem.span(c_field.name)) catch {
            for (fields[0..i]) |f| std.heap.c_allocator.free(f.name);
            std.heap.c_allocator.free(fields);
            return null;
        };
        const f_type: hocdb.FieldType = switch (c_field.type) {
            1 => .i64,
            2 => .f64,
            3 => .u64,
            5 => .string,
            6 => .bool,
            else => {
                std.heap.c_allocator.free(name);
                for (fields[0..i]) |f| std.heap.c_allocator.free(f.name);
                std.heap.c_allocator.free(fields);
                setLastError("InvalidFieldType");
                return null;
            },
        };
        fields[i] = .{ .name = name, .type = f_type };
    }
    return fields;
}

fn freeSchema(fields: []hocdb.FieldInfo) void {
    for (fields) |f| std.heap.c_allocator.free(f.name);
    std.heap.c_allocator.free(fields);
}

fn openCommon(ticker_z: [*:0]const u8, path_z: [*:0]const u8, schema_ptr: [*]const CField, schema_len: usize, config: DB.Config, read_only: bool) ?*anyopaque {
    setLastError("");
    const fields = schemaFromC(schema_ptr, schema_len) orelse return null;
    defer freeSchema(fields);
    const schema = hocdb.Schema{ .fields = fields };
    const db_ptr = std.heap.c_allocator.create(DB) catch {
        setLastError("OutOfMemory");
        return null;
    };
    const opened = if (read_only)
        DB.openReader(std.mem.span(ticker_z), std.mem.span(path_z), std.heap.c_allocator, schema)
    else
        DB.init(std.mem.span(ticker_z), std.mem.span(path_z), std.heap.c_allocator, schema, config);
    db_ptr.* = opened catch |err| {
        setLastError(@errorName(err));
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };
    db_ptr.initWriter() catch |err| {
        setLastError(@errorName(err));
        db_ptr.deinit();
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };
    return db_ptr;
}

/// Open or create a database with the full configuration.
export fn hocdb_init_ex(ticker_z: [*:0]const u8, path_z: [*:0]const u8, schema_ptr: [*]const CField, schema_len: usize, config: *const CConfig) ?*anyopaque {
    return openCommon(ticker_z, path_z, schema_ptr, schema_len, configFromC(config), false);
}

/// Attach as a lock-free reader to a database written by another process.
export fn hocdb_open_reader(ticker_z: [*:0]const u8, path_z: [*:0]const u8, schema_ptr: [*]const CField, schema_len: usize) ?*anyopaque {
    return openCommon(ticker_z, path_z, schema_ptr, schema_len, .{}, true);
}

/// Flush and fsync now (writers). -10 read-only.
export fn hocdb_sync(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.sync() catch |err| return errCode(err);
    return 0;
}

/// Readers: pick up the writer's latest commit. Writers: no-op.
export fn hocdb_refresh(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.refresh() catch |err| return errCode(err);
    return 0;
}

/// Recompute the data checksum: 1 = matches, 0 = MISMATCH, -20 unavailable
/// (ring buffer, legacy or adopted-tail file), other negatives = error.
export fn hocdb_verify(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const ok = db.verify() catch |err| return errCode(err);
    return if (ok) 1 else 0;
}

/// Keep only records with timestamp >= min_ts (rewrites the file atomically).
export fn hocdb_compact(db_ptr: *anyopaque, min_ts: i64) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.compact(min_ts) catch |err| return errCode(err);
    return 0;
}

/// Keep only the last n records.
export fn hocdb_retain_last(db_ptr: *anyopaque, n: u64) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.retainLast(n) catch |err| return errCode(err);
    return 0;
}

/// Archive the current file and continue with an empty one. The archive path
/// is copied into `out_path` (cap bytes, NUL terminated) when given.
export fn hocdb_rollover(db_ptr: *anyopaque, out_path: ?[*]u8, cap: usize) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const p = db.rollover(std.heap.c_allocator) catch |err| return errCode(err);
    defer std.heap.c_allocator.free(p);
    if (out_path) |o| {
        if (cap > 0) {
            const n = @min(p.len, cap - 1);
            @memcpy(o[0..n], p[0..n]);
            o[n] = 0;
        }
    }
    return 0;
}

export fn hocdb_metrics(db_ptr: *anyopaque, out: *DB.Metrics) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    out.* = db.getMetrics();
    return 0;
}

export fn hocdb_metrics_reset(db_ptr: *anyopaque) void {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.resetMetrics();
}

export fn hocdb_metrics_size() usize {
    return @sizeOf(DB.Metrics);
}
export fn hocdb_metrics_field_count() usize {
    return @typeInfo(DB.Metrics).@"struct".fields.len;
}
export fn hocdb_metrics_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(DB.Metrics, idx);
}
export fn hocdb_metrics_field_offset(idx: usize) usize {
    return structFieldOffset(DB.Metrics, idx);
}
export fn hocdb_metrics_field_type(idx: usize) c_int {
    return structFieldType(DB.Metrics, idx);
}

/// Bytes reserved by the file header of newly created files.
export fn hocdb_header_size() usize {
    return @intCast(DB.HEADER_SIZE);
}

/// File format version of an open database (1 legacy, 2 current).
export fn hocdb_format_version(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    return db.format_version;
}

/// 1 when the handle is a read-only reader.
export fn hocdb_is_read_only(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    return if (db.read_only) 1 else 0;
}

// ---------------------------------------------------------------------------
// Trading calendars
// ---------------------------------------------------------------------------

/// Id of a built-in or custom calendar by name (0 when unknown).
export fn hocdb_calendar_id(name: ?[*:0]const u8) u32 {
    const n = name orelse return 0;
    return calendar.idByName(std.mem.span(n));
}

/// Copy the calendar name into `buf`; returns its length, 0 for an unknown id, -1 when the buffer is too small.
export fn hocdb_calendar_name(id: u32, buf: ?[*]u8, cap: usize) c_int {
    const c = calendar.get(id) orelse return 0;
    if (buf == null or cap < c.name.len + 1) return -1;
    @memcpy(buf.?[0..c.name.len], c.name);
    buf.?[c.name.len] = 0;
    return @intCast(c.name.len);
}

/// Session lookup in UTC seconds: `which` 0 = the session containing `utc_sec`,
/// 1 = that or the previous one, 2 = that or the next one. Returns 1 when a
/// session was written to `out`, 0 when there is none, -31 for an unknown id.
export fn hocdb_calendar_session(id: u32, utc_sec: i64, which: c_int, out: ?*calendar.Session) c_int {
    const c = calendar.get(id) orelse return -31;
    const s = switch (which) {
        0 => c.sessionAt(utc_sec),
        1 => c.prevSession(utc_sec),
        else => c.nextSession(utc_sec),
    } orelse return 0;
    if (out) |o| o.* = s;
    return 1;
}

/// Session of a trade date (days since 1970-01-01, local): 1 found / 0 closed / -31 unknown id.
export fn hocdb_calendar_session_for_day(id: u32, day: i64, out: ?*calendar.Session) c_int {
    const c = calendar.get(id) orelse return -31;
    const s = c.sessionForDay(day) orelse return 0;
    if (out) |o| o.* = s;
    return 1;
}

export fn hocdb_calendar_is_open(id: u32, utc_sec: i64) c_int {
    const c = calendar.get(id) orelse return -31;
    return if (c.isOpen(utc_sec)) 1 else 0;
}

/// Seconds of trading time inside [a, b) (-1 for an unknown id).
export fn hocdb_calendar_open_seconds(id: u32, a: i64, b: i64) i64 {
    const c = calendar.get(id) orelse return -1;
    return c.openSecondsBetween(a, b);
}

/// Number of sessions opening inside [a, b) (-1 for an unknown id).
export fn hocdb_calendar_sessions_between(id: u32, a: i64, b: i64) i64 {
    const c = calendar.get(id) orelse return -1;
    return @intCast(c.sessionsBetween(a, b));
}

/// Bars per year for bars of `bucket_sec` seconds (0 for an unknown id).
export fn hocdb_calendar_periods_per_year(id: u32, bucket_sec: f64) f64 {
    const c = calendar.get(id) orelse return 0;
    return c.periodsPerYear(bucket_sec);
}

/// Local wall-clock seconds of a UTC instant in the calendar's time zone (with DST).
export fn hocdb_calendar_to_local(id: u32, utc_sec: i64) i64 {
    const c = calendar.get(id) orelse return utc_sec;
    return c.utcToLocal(utc_sec);
}

/// Days since 1970-01-01 of a civil date, and back (helpers for holiday lists).
export fn hocdb_days_from_civil(year: i64, month: u32, day: u32) i64 {
    if (month < 1 or month > 12 or day < 1 or day > 31) return 0;
    return calendar.daysFromCivil(year, month, day);
}

export fn hocdb_civil_from_days(days: i64, year: ?*i64, month: ?*u32, day: ?*u32) void {
    const c = calendar.civilFromDays(days);
    if (year) |y| y.* = c.year;
    if (month) |m| m.* = c.month;
    if (day) |d| d.* = c.day;
}

/// Register a custom calendar. `weekly` has 7 entries (Monday first), local
/// seconds relative to the trade date's midnight; an entry with close <= open
/// means no session on that weekday. `dst_rule`: 0 none, 1 US, 2 EU.
/// Returns the new id, 0 on invalid input, -1 when the registry is full.
export fn hocdb_calendar_define(name: ?[*:0]const u8, weekly: ?[*]const calendar.DaySession, utc_offset_sec: i32, dst_rule: c_int, holidays: ?[*]const i32, n_holidays: usize, early_closes: ?[*]const calendar.EarlyClose, n_early: usize, sessions_per_year: f64) i64 {
    const n = name orelse return 0;
    const w = weekly orelse return 0;
    var tpl: [7]?calendar.DaySession = .{null} ** 7;
    for (0..7) |i| {
        if (w[i].close_sec > w[i].open_sec) tpl[i] = w[i];
    }
    const dst: calendar.DstRule = switch (dst_rule) {
        1 => .us,
        2 => .eu,
        else => .none,
    };
    const hol: []const i32 = if (holidays) |h| h[0..n_holidays] else &[_]i32{};
    const early: []const calendar.EarlyClose = if (early_closes) |e| e[0..n_early] else &[_]calendar.EarlyClose{};
    const id = calendar.define(std.mem.span(n), tpl, utc_offset_sec, dst, hol, early, sessions_per_year) catch |err| return switch (err) {
        error.TooManyCalendars => -1,
        else => 0,
    };
    return id;
}

/// Per-handle calendar and timestamp unit (writers persist built-in ids and the unit).
export fn hocdb_set_calendar(db_ptr: *anyopaque, id: u32) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.setCalendar(id) catch |err| return errCode(err);
    return 0;
}

export fn hocdb_get_calendar(db_ptr: *anyopaque) u32 {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    return db.calendar_id;
}

export fn hocdb_set_timestamp_unit(db_ptr: *anyopaque, unit_ns: u64) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.setTimestampUnit(unit_ns) catch |err| return errCode(err);
    return 0;
}

export fn hocdb_get_timestamp_unit(db_ptr: *anyopaque) u64 {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    return db.timestampUnitNs();
}

/// Bars per year for `bucket` timestamp units from the handle's calendar and unit (0 = unknown).
export fn hocdb_periods_per_year(db_ptr: *anyopaque, bucket: i64) f64 {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    return db.periodsPerYear(bucket);
}

// ---------------------------------------------------------------------------
// Universe (cross-sectional) features
// ---------------------------------------------------------------------------

export fn hocdb_universe_params_default(out: *universe.Params) void {
    out.* = .{};
}

/// Cross-sectional features over `n` databases with the same column roles
/// (see DynamicTimeSeriesDB.universe). `rows` has n entries, `corr` n*n or NULL.
export fn hocdb_universe(handles: [*]const *anyopaque, n: usize, cols: *const DB.IndicatorColumns, n_bars: usize, bucket: i64, params: ?*const universe.Params, rows: [*]universe.Row, corr: ?[*]f64, out: *universe.Summary) c_int {
    const dbs = std.heap.c_allocator.alloc(*DB, n) catch return -1;
    defer std.heap.c_allocator.free(dbs);
    for (0..n) |i| dbs[i] = @as(*DB, @ptrCast(@alignCast(handles[i])));
    const p: universe.Params = if (params) |pp| pp.* else .{};
    out.* = DB.universe(dbs, cols.*, n_bars, bucket, p, rows[0..n], if (corr) |c| c[0 .. n * n] else null, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

/// Same on caller-provided aligned close (and optional volume) series of `n_bars` bars each.
export fn hocdb_universe_arrays(closes: [*]const [*]const f64, volumes: ?[*]const [*]const f64, n_tickers: usize, n_bars: usize, ts: ?[*]const i64, params: ?*const universe.Params, rows: [*]universe.Row, corr: ?[*]f64, out: *universe.Summary) c_int {
    const a = std.heap.c_allocator;
    const cl = a.alloc([]const f64, n_tickers) catch return -1;
    defer a.free(cl);
    for (0..n_tickers) |i| cl[i] = closes[i][0..n_bars];
    var vl: ?[][]const f64 = null;
    defer if (vl) |v| a.free(v);
    if (volumes) |vv| {
        const tmp = a.alloc([]const f64, n_tickers) catch return -1;
        for (0..n_tickers) |i| tmp[i] = vv[i][0..n_bars];
        vl = tmp;
    }
    const p: universe.Params = if (params) |pp| pp.* else .{};
    out.* = universe.compute(cl, vl, if (ts) |t| t[0..n_bars] else null, p, rows[0..n_tickers], if (corr) |c| c[0 .. n_tickers * n_tickers] else null, a) catch |err| return errCode(err);
    return 0;
}

export fn hocdb_universe_row_size() usize {
    return @sizeOf(universe.Row);
}
export fn hocdb_universe_row_field_count() usize {
    return @typeInfo(universe.Row).@"struct".fields.len;
}
export fn hocdb_universe_row_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(universe.Row, idx);
}
export fn hocdb_universe_row_field_offset(idx: usize) usize {
    return structFieldOffset(universe.Row, idx);
}
export fn hocdb_universe_row_field_type(idx: usize) c_int {
    return structFieldType(universe.Row, idx);
}
export fn hocdb_universe_summary_size() usize {
    return @sizeOf(universe.Summary);
}
export fn hocdb_universe_summary_field_count() usize {
    return @typeInfo(universe.Summary).@"struct".fields.len;
}
export fn hocdb_universe_summary_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(universe.Summary, idx);
}
export fn hocdb_universe_summary_field_offset(idx: usize) usize {
    return structFieldOffset(universe.Summary, idx);
}
export fn hocdb_universe_summary_field_type(idx: usize) c_int {
    return structFieldType(universe.Summary, idx);
}
export fn hocdb_universe_params_size() usize {
    return @sizeOf(universe.Params);
}

// ---------------------------------------------------------------------------
// Signal backtester
// ---------------------------------------------------------------------------

/// Optional per-bar output columns (each NULL or n entries).
pub const CBacktestOutputs = extern struct {
    equity: ?[*]f64,
    position: ?[*]f64,
    cash: ?[*]f64,
    pnl: ?[*]f64,
    drawdown: ?[*]f64,
};

fn outputsFromC(o: ?*const CBacktestOutputs, n: usize) backtest.Outputs {
    const c = o orelse return .{};
    return .{
        .equity = if (c.equity) |p| p[0..n] else null,
        .position = if (c.position) |p| p[0..n] else null,
        .cash = if (c.cash) |p| p[0..n] else null,
        .pnl = if (c.pnl) |p| p[0..n] else null,
        .drawdown = if (c.drawdown) |p| p[0..n] else null,
    };
}

export fn hocdb_backtest_params_default(out: *backtest.Params) void {
    out.* = .{};
}

/// Backtest `target[n]` over the bars of [start_ts, end_ts) (see DynamicTimeSeriesDB.backtest);
/// n must equal the number of bars in the window (-7 otherwise).
export fn hocdb_backtest(db_ptr: *anyopaque, cols: *const DB.IndicatorColumns, start_ts: i64, end_ts: i64, bucket: i64, target: [*]const f64, n: usize, params: ?*const backtest.Params, outputs: ?*const CBacktestOutputs, trades: ?[*]backtest.Trade, trades_cap: usize, out: *backtest.Result) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const p: backtest.Params = if (params) |pp| pp.* else .{};
    out.* = db.backtest(cols.*, start_ts, end_ts, bucket, target[0..n], p, outputsFromC(outputs, n), if (trades) |t| t[0..trades_cap] else null, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

/// Backtest over the last n bars (bucket > 0) or records; n = target length.
export fn hocdb_backtest_tail(db_ptr: *anyopaque, cols: *const DB.IndicatorColumns, bucket: i64, target: [*]const f64, n: usize, params: ?*const backtest.Params, outputs: ?*const CBacktestOutputs, trades: ?[*]backtest.Trade, trades_cap: usize, out: *backtest.Result) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const p: backtest.Params = if (params) |pp| pp.* else .{};
    out.* = db.backtestTail(cols.*, n, bucket, target[0..n], p, outputsFromC(outputs, n), if (trades) |t| t[0..trades_cap] else null, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

/// Backtest on caller-provided arrays (open/high/low may be NULL).
export fn hocdb_backtest_arrays(ts: [*]const i64, open: ?[*]const f64, high: ?[*]const f64, low: ?[*]const f64, close: [*]const f64, n: usize, target: [*]const f64, params: ?*const backtest.Params, outputs: ?*const CBacktestOutputs, trades: ?[*]backtest.Trade, trades_cap: usize, out: *backtest.Result) c_int {
    const p: backtest.Params = if (params) |pp| pp.* else .{};
    out.* = backtest.run(ts[0..n], if (open) |o| o[0..n] else null, if (high) |h| h[0..n] else null, if (low) |l| l[0..n] else null, close[0..n], target[0..n], p, outputsFromC(outputs, n), if (trades) |t| t[0..trades_cap] else null, std.heap.c_allocator) catch |err| return errCode(err);
    return 0;
}

/// Walk-forward index ranges (see backtest.walkForwardSplits); returns the number written.
export fn hocdb_walk_forward_splits(n: usize, n_splits: usize, train_frac: f64, anchored: c_int, out: [*]backtest.Split, cap: usize) usize {
    return backtest.walkForwardSplits(n, n_splits, train_frac, anchored != 0, out[0..cap]);
}

/// Run the backtest on every test window of `splits` (fresh equity each); results[n_splits].
export fn hocdb_backtest_splits_arrays(ts: [*]const i64, open: ?[*]const f64, high: ?[*]const f64, low: ?[*]const f64, close: [*]const f64, n: usize, target: [*]const f64, params: ?*const backtest.Params, splits: [*]const backtest.Split, n_splits: usize, results: [*]backtest.Result) c_int {
    const p: backtest.Params = if (params) |pp| pp.* else .{};
    const k = backtest.runSplits(ts[0..n], if (open) |o| o[0..n] else null, if (high) |h| h[0..n] else null, if (low) |l| l[0..n] else null, close[0..n], target[0..n], p, splits[0..n_splits], results[0..n_splits], std.heap.c_allocator) catch |err| return errCode(err);
    return @intCast(k);
}

export fn hocdb_backtest_params_size() usize {
    return @sizeOf(backtest.Params);
}
export fn hocdb_backtest_result_size() usize {
    return @sizeOf(backtest.Result);
}
export fn hocdb_backtest_result_field_count() usize {
    return @typeInfo(backtest.Result).@"struct".fields.len;
}
export fn hocdb_backtest_result_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(backtest.Result, idx);
}
export fn hocdb_backtest_result_field_offset(idx: usize) usize {
    return structFieldOffset(backtest.Result, idx);
}
export fn hocdb_backtest_result_field_type(idx: usize) c_int {
    return structFieldType(backtest.Result, idx);
}
export fn hocdb_trade_size() usize {
    return @sizeOf(backtest.Trade);
}
export fn hocdb_trade_field_count() usize {
    return @typeInfo(backtest.Trade).@"struct".fields.len;
}
export fn hocdb_trade_field_name(idx: usize) ?[*:0]const u8 {
    return structFieldName(backtest.Trade, idx);
}
export fn hocdb_trade_field_offset(idx: usize) usize {
    return structFieldOffset(backtest.Trade, idx);
}
export fn hocdb_trade_field_type(idx: usize) c_int {
    return structFieldType(backtest.Trade, idx);
}
