const std = @import("std");
const hocdb = @import("hocdb");

// We no longer use a fixed TradeData struct.
// Instead we use DynamicTimeSeriesDB directly.

const DB = hocdb.DynamicTimeSeriesDB;

// --- N-API Definitions (Manual) ---
const napi_env = *anyopaque;
const napi_value = ?*anyopaque;
const napi_callback_info = *anyopaque;
const napi_ref = *anyopaque;
const napi_deferred = *anyopaque;
const napi_handle_scope = *anyopaque;
const napi_escapable_handle_scope = *anyopaque;
const napi_callback = *const fn (env: napi_env, info: napi_callback_info) callconv(.c) napi_value;
const napi_finalize = *const fn (env: napi_env, finalize_data: *anyopaque, finalize_hint: *anyopaque) callconv(.c) void;

const napi_property_attributes = enum(c_int) {
    default = 0,
};

const napi_property_descriptor = extern struct {
    utf8name: ?[*:0]const u8,
    name: napi_value,
    method: ?napi_callback,
    getter: ?napi_callback,
    setter: ?napi_callback,
    value: napi_value,
    attributes: napi_property_attributes,
    data: ?*anyopaque,
};

const napi_status = enum(c_int) {
    ok = 0,
    invalid_arg = 1,
    object_expected = 2,
    string_expected = 3,
    name_expected = 4,
    function_expected = 5,
    number_expected = 6,
    boolean_expected = 7,
    array_expected = 8,
    generic_failure = 9,
    pending_exception = 10,
    cancelled = 11,
    escape_called_twice = 12,
    handle_scope_mismatch = 13,
    callback_scope_mismatch = 14,
    queue_full = 15,
    closing = 16,
    bigint_expected = 17,
    date_expected = 18,
    arraybuffer_expected = 19,
    detachable_arraybuffer_expected = 20,
    would_deadlock = 21,
};

const napi_valuetype = enum(c_int) {
    undefined = 0,
    null = 1,
    boolean = 2,
    number = 3,
    string = 4,
    symbol = 5,
    object = 6,
    function = 7,
    external = 8,
    bigint = 9,
};

extern "C" fn napi_define_properties(env: napi_env, object: napi_value, property_count: usize, properties: [*]const napi_property_descriptor) napi_status;
extern "C" fn napi_get_cb_info(env: napi_env, cbinfo: napi_callback_info, argc: *usize, argv: [*]napi_value, this_arg: ?*napi_value, data: ?*?*anyopaque) napi_status;
extern "C" fn napi_create_string_utf8(env: napi_env, str: [*]const u8, length: usize, result: *napi_value) napi_status;
extern "C" fn napi_create_double(env: napi_env, value: f64, result: *napi_value) napi_status;
extern "C" fn napi_create_int64(env: napi_env, value: i64, result: *napi_value) napi_status;
extern "C" fn napi_get_value_double(env: napi_env, value: napi_value, result: *f64) napi_status;
extern "C" fn napi_get_value_int64(env: napi_env, value: napi_value, result: *i64) napi_status;
extern "C" fn napi_get_value_bigint_int64(env: napi_env, value: napi_value, result: *i64, lossless: *bool) napi_status;
extern "C" fn napi_get_value_bigint_uint64(env: napi_env, value: napi_value, result: *u64, lossless: *bool) napi_status;
extern "C" fn napi_get_value_string_utf8(env: napi_env, value: napi_value, buf: ?[*]u8, bufsize: usize, result: ?*usize) napi_status;
extern "C" fn napi_create_external(env: napi_env, data: *anyopaque, finalize_cb: ?napi_finalize, finalize_hint: ?*anyopaque, result: *napi_value) napi_status;
extern "C" fn napi_get_value_external(env: napi_env, value: napi_value, result: *?*anyopaque) napi_status;
extern "C" fn napi_throw_error(env: napi_env, code: ?[*:0]const u8, msg: [*:0]const u8) napi_status;
extern "C" fn napi_create_external_arraybuffer(env: napi_env, external_data: *anyopaque, byte_length: usize, finalize_cb: ?napi_finalize, finalize_hint: ?*anyopaque, result: *napi_value) napi_status;
extern "C" fn napi_typeof(env: napi_env, value: napi_value, result: *napi_valuetype) napi_status;
extern "C" fn napi_get_named_property(env: napi_env, object: napi_value, utf8name: [*]const u8, result: *napi_value) napi_status;
extern "C" fn napi_get_value_bool(env: napi_env, value: napi_value, result: *bool) napi_status;
extern "C" fn napi_get_element(env: napi_env, object: napi_value, index: u32, result: *napi_value) napi_status;
extern "C" fn napi_get_array_length(env: napi_env, value: napi_value, result: *u32) napi_status;
extern "C" fn napi_get_buffer_info(env: napi_env, value: napi_value, data: *?*anyopaque, length: *usize) napi_status;
extern "C" fn napi_create_object(env: napi_env, result: *napi_value) napi_status;
extern "C" fn napi_set_named_property(env: napi_env, object: napi_value, utf8name: [*]const u8, value: napi_value) napi_status;
extern "C" fn napi_create_bigint_int64(env: napi_env, value: i64, result: *napi_value) napi_status;
extern "C" fn napi_create_bigint_uint64(env: napi_env, value: u64, result: *napi_value) napi_status;
extern "C" fn napi_create_arraybuffer(env: napi_env, byte_length: usize, data: *?*anyopaque, result: *napi_value) napi_status;
extern "C" fn napi_create_typedarray(env: napi_env, typedarray_type: c_int, length: usize, arraybuffer: napi_value, byte_offset: usize, result: *napi_value) napi_status;
extern "C" fn napi_has_named_property(env: napi_env, object: napi_value, utf8name: [*]const u8, result: *bool) napi_status;
extern "C" fn napi_set_property(env: napi_env, object: napi_value, key: napi_value, value: napi_value) napi_status;
extern "C" fn napi_create_array_with_length(env: napi_env, length: usize, result: *napi_value) napi_status;
extern "C" fn napi_set_element(env: napi_env, object: napi_value, index: u32, value: napi_value) napi_status;
extern "C" fn napi_get_boolean(env: napi_env, value: bool, result: *napi_value) napi_status;
extern "C" fn napi_get_null(env: napi_env, result: *napi_value) napi_status;
extern "C" fn napi_is_array(env: napi_env, value: napi_value, result: *bool) napi_status;
extern "C" fn napi_is_typedarray(env: napi_env, value: napi_value, result: *bool) napi_status;
extern "C" fn napi_get_typedarray_info(env: napi_env, typedarray: napi_value, ta_type: *c_int, length: *usize, data: *?*anyopaque, arraybuffer: *napi_value, byte_offset: *usize) napi_status;

// napi_typedarray_type values used below
const napi_float64_array: c_int = 8;
const napi_bigint64_array: c_int = 9;

// --- Helper Functions ---

fn throwError(env: napi_env, msg: []const u8) napi_value {
    const msg_z = std.heap.c_allocator.dupeZ(u8, msg) catch return null;
    defer std.heap.c_allocator.free(msg_z);
    _ = napi_throw_error(env, null, msg_z);
    return null;
}

fn getArgs(env: napi_env, info: napi_callback_info, comptime N: usize) ![N]napi_value {
    var argc: usize = N;
    var argv: [N]napi_value = undefined;
    _ = napi_get_cb_info(env, info, &argc, &argv, null, null);
    if (argc < N) return error.NotEnoughArguments;
    return argv;
}

// --- Implementation ---

// --- Errors ---

/// Throw an Error with `code` (the Zig error name) and the given message.
fn throwCoded(env: napi_env, code: []const u8, msg: []const u8) napi_value {
    const code_z = std.heap.c_allocator.dupeZ(u8, code) catch return null;
    defer std.heap.c_allocator.free(code_z);
    const msg_z = std.heap.c_allocator.dupeZ(u8, msg) catch return null;
    defer std.heap.c_allocator.free(msg_z);
    _ = napi_throw_error(env, code_z, msg_z);
    return null;
}

/// Explanation of the storage errors a user can run into ("" when there is none).
fn dbErrorDetail(err: anyerror) []const u8 {
    return switch (err) {
        error.ReadOnly => "this handle is a reader (opened with openReader); append, sync, compact, retainLast, rollover and drop need a writer",
        error.DatabaseLocked => "another writer holds this database (writers take an exclusive lock; use openReader for concurrent readers)",
        error.ChecksumUnavailable => "no checksum is available for ring-buffer (overwrite_on_full) or legacy files",
        error.ChecksumMismatch => "the stored checksum does not match the data (corrupted file)",
        error.EmptyDatabase => "the database has no records",
        error.SchemaMismatch => "the on-disk schema differs from the schema given",
        error.LegacyFormatNeedsMigration => "legacy HOC1 file; open it with a writer first so that it is migrated (auto_migrate)",
        error.MaxFileSizeTooSmall => "max_file_size must be at least headerSize() + one record",
        error.FileNotFound => "no such database file (a reader needs a file the writer has already created)",
        error.TimestampNotMonotonic => "timestamps must be strictly increasing",
        error.InvalidRecordSize => "the record does not match the schema's record size",
        error.UnknownCalendar => "no such calendar (built-in ids / names: 1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme; custom ids come from calendarDefine)",
        error.CalendarRequired => "session kinds with param 0 use the database's calendar sessions: open it with { calendar, timestamp_unit_ns } or call setCalendar() and setTimestampUnit()",
        else => "",
    };
}

/// Throw "<ErrorName>: <detail>" (err.code = ErrorName) for a failed database operation.
fn throwDbError(env: napi_env, err: anyerror) napi_value {
    const name = @errorName(err);
    const detail = dbErrorDetail(err);
    if (detail.len == 0) return throwCoded(env, name, name);
    const msg = std.fmt.allocPrint(std.heap.c_allocator, "{s}: {s}", .{ name, detail }) catch return throwCoded(env, name, name);
    defer std.heap.c_allocator.free(msg);
    return throwCoded(env, name, msg);
}

/// Throw for a failed open; the message names the error ("DatabaseLocked", "SchemaMismatch", ...).
fn throwOpenError(env: napi_env, err: anyerror, ticker: []const u8, path: []const u8, read_only: bool) napi_value {
    const name = @errorName(err);
    const detail = dbErrorDetail(err);
    const msg = std.fmt.allocPrint(std.heap.c_allocator, "{s}: cannot open {s} '{s}' in '{s}'{s}{s}", .{
        name, if (read_only) "a reader for" else "database", ticker, path, if (detail.len > 0) ": " else "", detail,
    }) catch return throwCoded(env, name, name);
    defer std.heap.c_allocator.free(msg);
    return throwCoded(env, name, msg);
}

// --- Strings, schema and config parsing ---

/// JS string -> NUL-terminated copy (c_allocator); the slice excludes the terminator. Free with freeString.
fn readString(env: napi_env, value: napi_value) error{ NotAString, OutOfMemory }![]u8 {
    var len: usize = 0;
    if (napi_get_value_string_utf8(env, value, null, 0, &len) != .ok) return error.NotAString;
    const buf = try std.heap.c_allocator.alloc(u8, len + 1);
    errdefer std.heap.c_allocator.free(buf);
    if (napi_get_value_string_utf8(env, value, buf.ptr, len + 1, null) != .ok) return error.NotAString;
    return buf[0..len];
}

fn freeString(s: []const u8) void {
    std.heap.c_allocator.free(s.ptr[0 .. s.len + 1]);
}

const SchemaParseError = error{ NotAnArray, BadElement, MissingName, InvalidName, MissingType, InvalidType, UnsupportedType, OutOfMemory };

/// [{name, type}] -> []FieldInfo (free with freeSchema).
fn parseSchemaInner(env: napi_env, arr: napi_value) SchemaParseError![]hocdb.FieldInfo {
    const allocator = std.heap.c_allocator;
    var schema_len: u32 = 0;
    if (napi_get_array_length(env, arr, &schema_len) != .ok) return error.NotAnArray;
    const fields = try allocator.alloc(hocdb.FieldInfo, schema_len);
    var n: usize = 0;
    errdefer {
        for (fields[0..n]) |f| freeString(f.name);
        allocator.free(fields);
    }
    while (n < schema_len) {
        var element: napi_value = undefined;
        if (napi_get_element(env, arr, @intCast(n), &element) != .ok) return error.BadElement;
        var name_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "name", &name_val) != .ok) return error.MissingName;
        const name = readString(env, name_val) catch |e| return if (e == error.OutOfMemory) error.OutOfMemory else error.InvalidName;
        errdefer freeString(name);
        var type_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "type", &type_val) != .ok) return error.MissingType;
        const type_str = readString(env, type_val) catch |e| return if (e == error.OutOfMemory) error.OutOfMemory else error.InvalidType;
        defer freeString(type_str);
        const f_type: hocdb.FieldType = if (std.mem.eql(u8, type_str, "i64")) .i64 else if (std.mem.eql(u8, type_str, "f64")) .f64 else if (std.mem.eql(u8, type_str, "u64")) .u64 else if (std.mem.eql(u8, type_str, "bool")) .bool else return error.UnsupportedType;
        fields[n] = .{ .name = name, .type = f_type };
        n += 1;
    }
    return fields;
}

/// parseSchemaInner with the error already thrown (null = exception pending).
fn parseSchema(env: napi_env, arr: napi_value) ?[]hocdb.FieldInfo {
    return parseSchemaInner(env, arr) catch |err| {
        _ = throwError(env, switch (err) {
            error.NotAnArray => "Invalid schema array",
            error.BadElement => "Failed to get schema element",
            error.MissingName => "Missing name in schema",
            error.InvalidName => "Invalid name",
            error.MissingType => "Missing type in schema",
            error.InvalidType => "Invalid type",
            error.UnsupportedType => "Unsupported type",
            error.OutOfMemory => "OOM",
        });
        return null;
    };
}

fn freeSchema(fields: []hocdb.FieldInfo) void {
    for (fields) |f| freeString(f.name);
    std.heap.c_allocator.free(fields);
}

/// First property of `obj` among `keys` (snake_case / camelCase aliases) that is set, or null.
fn propAny(env: napi_env, obj: napi_value, keys: []const [*:0]const u8) ?napi_value {
    for (keys) |key| {
        var has: bool = false;
        if (napi_has_named_property(env, obj, key, &has) != .ok or !has) continue;
        var v: napi_value = undefined;
        if (napi_get_named_property(env, obj, key, &v) != .ok) continue;
        var t: napi_valuetype = undefined;
        if (napi_typeof(env, v, &t) != .ok or t == .undefined or t == .null) continue;
        return v;
    }
    return null;
}

/// JS BigInt or number -> u64 (negative values are rejected).
fn readU64(env: napi_env, value: napi_value, out: *u64) bool {
    var lossless: bool = true;
    if (napi_get_value_bigint_uint64(env, value, out, &lossless) == .ok) return lossless;
    var i: i64 = 0;
    if (napi_get_value_int64(env, value, &i) != .ok or i < 0) return false;
    out.* = @intCast(i);
    return true;
}

/// JS boolean (or number, 0 = false) -> bool.
fn readBoolish(env: napi_env, value: napi_value, out: *bool) bool {
    if (napi_get_value_bool(env, value, out) == .ok) return true;
    var x: f64 = 0;
    if (napi_get_value_double(env, value, &x) != .ok) return false;
    out.* = x != 0;
    return true;
}

/// Throw "Invalid config.<key>: <what>" and signal the caller to bail out.
fn cfgFail(env: napi_env, key: [*:0]const u8, what: []const u8) error{Thrown} {
    const msg = std.fmt.allocPrint(std.heap.c_allocator, "Invalid config.{s}: {s}", .{ key, what }) catch {
        _ = throwError(env, "Invalid config");
        return error.Thrown;
    };
    defer std.heap.c_allocator.free(msg);
    _ = throwError(env, msg);
    return error.Thrown;
}

fn cfgBool(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *bool) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    if (!readBoolish(env, v, out)) return cfgFail(env, keys[0], "expected a boolean");
}

/// Unsigned option; 0 keeps the engine default when `zero_is_default`.
fn cfgU64(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *u64, zero_is_default: bool) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    var x: u64 = 0;
    if (!readU64(env, v, &x)) return cfgFail(env, keys[0], "expected a non-negative integer (number or BigInt)");
    if (x == 0 and zero_is_default) return;
    out.* = x;
}

fn cfgI64(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *i64) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    var x: i64 = 0;
    if (!readI64(env, v, &x)) return cfgFail(env, keys[0], "expected an integer (number or BigInt)");
    out.* = x;
}

/// "none" | "on_close" | "on_flush" | "interval" (case-insensitive, '_' / '-' optional, camelCase ok).
fn fsyncPolicyFromName(s: []const u8) ?DB.FsyncPolicy {
    var buf: [16]u8 = undefined;
    var n: usize = 0;
    for (s) |c| {
        if (c == '_' or c == '-' or c == ' ') continue;
        if (n >= buf.len) return null;
        buf[n] = std.ascii.toLower(c);
        n += 1;
    }
    const k = buf[0..n];
    if (std.mem.eql(u8, k, "none")) return .none;
    if (std.mem.eql(u8, k, "onclose")) return .on_close;
    if (std.mem.eql(u8, k, "onflush")) return .on_flush;
    if (std.mem.eql(u8, k, "interval")) return .interval;
    return null;
}

fn cfgFsync(env: napi_env, obj: napi_value, out: *DB.FsyncPolicy) error{Thrown}!void {
    const keys = [_][*:0]const u8{ "fsync", "fsync_policy", "fsyncPolicy" };
    const v = propAny(env, obj, &keys) orelse return;
    const what = "expected 'none', 'on_close', 'on_flush', 'interval' or 0-3";
    if (readString(env, v)) |s| {
        defer freeString(s);
        out.* = fsyncPolicyFromName(s) orelse return cfgFail(env, "fsync", what);
        return;
    } else |_| {}
    var x: i64 = -1;
    if (!readI64(env, v, &x) or x < 0 or x > 3) return cfgFail(env, "fsync", what);
    out.* = @enumFromInt(@as(u8, @intCast(x)));
}

/// Fill `config` from a JS options object (snake_case or camelCase keys); throws on bad values.
fn parseConfig(env: napi_env, obj: napi_value, config: *DB.Config) error{Thrown}!void {
    var t: napi_valuetype = undefined;
    if (napi_typeof(env, obj, &t) != .ok or t == .undefined or t == .null) return;
    if (t != .object) return cfgFail(env, "config", "expected an object");
    try cfgU64(env, obj, &.{ "max_file_size", "maxFileSize" }, &config.max_file_size, true);
    try cfgBool(env, obj, &.{ "overwrite_on_full", "overwriteOnFull" }, &config.overwrite_on_full);
    try cfgBool(env, obj, &.{ "flush_on_write", "flushOnWrite" }, &config.flush_on_write);
    try cfgBool(env, obj, &.{ "auto_increment", "autoIncrement" }, &config.auto_increment);
    try cfgFsync(env, obj, &config.fsync);
    var interval: u64 = 0;
    try cfgU64(env, obj, &.{ "fsync_interval_ms", "fsyncIntervalMs" }, &interval, true);
    if (interval > std.math.maxInt(u32)) return cfgFail(env, "fsync_interval_ms", "must fit in 32 bits");
    if (interval > 0) config.fsync_interval_ms = @intCast(interval);
    try cfgBool(env, obj, &.{ "verify_on_open", "verifyOnOpen" }, &config.verify_on_open);
    try cfgI64(env, obj, &.{ "retention_span", "retentionSpan" }, &config.retention_span);
    if (config.retention_span < 0) return cfgFail(env, "retention_span", "must be >= 0 (timestamp units; 0 = off)");
    try cfgU64(env, obj, &.{ "rollover_size", "rolloverSize" }, &config.rollover_size, false);
    try cfgBool(env, obj, &.{ "auto_migrate", "autoMigrate" }, &config.auto_migrate);
    try cfgU64(env, obj, &.{ "timestamp_unit_ns", "timestampUnitNs" }, &config.timestamp_unit_ns, false);
    try cfgU64(env, obj, &.{ "index_stride", "indexStride" }, &config.index_stride, true);
    try cfgCalendar(env, obj, &config.calendar);
}

/// `calendar`: a calendar id (1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme, custom ids) or a name; unknown -> UnknownCalendar.
fn cfgCalendar(env: napi_env, obj: napi_value, out: *u32) error{Thrown}!void {
    const keys = [_][*:0]const u8{ "calendar", "calendar_id", "calendarId" };
    const v = propAny(env, obj, &keys) orelse return;
    if (readString(env, v)) |name| {
        defer freeString(name);
        const id = hocdb.calendar.idByName(name);
        if (id == 0) {
            _ = throwCodedFmt(env, "UnknownCalendar", "UnknownCalendar: no calendar named '{s}' (built-in: crypto, fx, nyse, nasdaq, lse, cme; custom names come from calendarDefine)", .{name});
            return error.Thrown;
        }
        out.* = id;
        return;
    } else |_| {}
    var x: u64 = 0;
    if (!readU64(env, v, &x) or x > std.math.maxInt(u32)) return cfgFail(env, "calendar", "expected a calendar id (number) or name (string)");
    if (x != 0 and hocdb.calendar.get(@intCast(x)) == null) {
        _ = throwCodedFmt(env, "UnknownCalendar", "UnknownCalendar: no calendar with id {d} (built-in: 1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme; custom ids come from calendarDefine)", .{x});
        return error.Thrown;
    }
    out.* = @intCast(x);
}

// --- Opening ---

/// Shared by dbInit (writer: exclusive lock, config honoured) and dbOpenReader (lock-free reader).
fn openCommon(env: napi_env, info: napi_callback_info, comptime read_only: bool) napi_value {
    const max_args = 4;
    var argc: usize = max_args;
    var args: [max_args]napi_value = undefined;
    if (napi_get_cb_info(env, info, &argc, &args, null, null) != .ok) {
        return throwError(env, "Failed to parse arguments");
    }
    if (argc < 3) {
        return throwError(env, "Expected at least 3 arguments: ticker, path, schema");
    }

    const ticker = readString(env, args[0]) catch return throwError(env, "Invalid ticker");
    defer freeString(ticker);
    const path = readString(env, args[1]) catch return throwError(env, "Invalid path");
    defer freeString(path);
    const fields = parseSchema(env, args[2]) orelse return null;
    defer freeSchema(fields);

    var config = DB.Config{};
    if (argc >= 4) parseConfig(env, args[3], &config) catch return null;

    const schema = hocdb.Schema{ .fields = fields };
    const allocator = std.heap.c_allocator;
    const db_ptr = allocator.create(DB) catch return throwError(env, "Allocation failed");
    const opened = if (read_only) DB.openReader(ticker, path, allocator, schema) else DB.init(ticker, path, allocator, schema, config);
    db_ptr.* = opened catch |err| {
        allocator.destroy(db_ptr);
        return throwOpenError(env, err, ticker, path, read_only);
    };
    db_ptr.initWriter() catch |err| {
        db_ptr.deinit();
        allocator.destroy(db_ptr);
        return throwOpenError(env, err, ticker, path, read_only);
    };

    var result: napi_value = undefined;
    _ = napi_create_external(env, db_ptr, null, null, &result);
    return result;
}

// dbInit(ticker: string, path: string, schema: object[], config?: object): external
fn dbInit(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    return openCommon(env, info, false);
}

// dbOpenReader(ticker: string, path: string, schema: object[]): external
// Lock-free reader: sees committed data only, follows compaction / rollover; writes fail with ReadOnly.
fn dbOpenReader(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    return openCommon(env, info, true);
}

// dbAppend(db: external, buffer: ArrayBuffer): void
fn dbAppend(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var data_ptr: ?*anyopaque = null;
    var data_len: usize = 0;
    if (napi_get_buffer_info(env, args[1], &data_ptr, &data_len) != .ok) {
        return throwError(env, "Invalid data buffer");
    }

    const data = @as([*]const u8, @ptrCast(data_ptr.?))[0..data_len];

    db.append(data) catch |err| {
        if (err == error.InvalidRecordSize) return throwError(env, "Append failed: Invalid Record Size");
        if (err == error.TimestampNotMonotonic) return throwError(env, "Append failed: Timestamp Not Monotonic - timestamps must be strictly increasing");
        return throwDbError(env, err);
    };

    return null;
}

// dbFlush(db: external): void
fn dbFlush(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    db.flush() catch |err| {
        return throwDbError(env, err);
    };

    return null;
}

// Finalizer for ArrayBuffer
fn freeData(env: napi_env, data: *anyopaque, hint: *anyopaque) callconv(.c) void {
    _ = env;
    _ = hint;
    std.c.free(data);
}

// dbLoad(db: external): ArrayBuffer
fn dbLoad(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    const allocator = std.heap.c_allocator;

    db.flush() catch |err| {
        return throwDbError(env, err);
    };

    const data = db.load(allocator) catch |err| {
        return throwDbError(env, err);
    };

    var result: napi_value = undefined;
    const byte_length = data.len;

    _ = napi_create_external_arraybuffer(env, data.ptr, byte_length, freeData, null, &result);
    return result;
}

// dbQuery(db: external, start: i64, end: i64, filters: object[]): ArrayBuffer
fn dbQuery(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 4) catch return throwError(env, "Expected 4 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var start: i64 = 0;
    var lossless: bool = true;
    if (napi_get_value_bigint_int64(env, args[1], &start, &lossless) != .ok) {
        if (napi_get_value_int64(env, args[1], &start) != .ok) return throwError(env, "Invalid start timestamp");
    }

    var end: i64 = 0;
    if (napi_get_value_bigint_int64(env, args[2], &end, &lossless) != .ok) {
        if (napi_get_value_int64(env, args[2], &end) != .ok) return throwError(env, "Invalid end timestamp");
    }

    var filters_len: u32 = 0;
    if (napi_get_array_length(env, args[3], &filters_len) != .ok) return throwError(env, "Invalid filters array");

    const allocator = std.heap.c_allocator;
    const filters = allocator.alloc(hocdb.Filter, filters_len) catch return throwError(env, "OOM");
    defer allocator.free(filters);

    var i: u32 = 0;
    while (i < filters_len) : (i += 1) {
        var element: napi_value = undefined;
        if (napi_get_element(env, args[3], i, &element) != .ok) return throwError(env, "Failed to get filter element");

        var index_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "field_index", &index_val) != .ok) return throwError(env, "Missing field_index");
        var field_index: i64 = 0;
        if (napi_get_value_int64(env, index_val, &field_index) != .ok) return throwError(env, "Invalid field_index");

        var type_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "type", &type_val) != .ok) return throwError(env, "Missing type");
        var type_len: usize = 0;
        if (napi_get_value_string_utf8(env, type_val, null, 0, &type_len) != .ok) return throwError(env, "Invalid type");
        const type_str = allocator.alloc(u8, type_len + 1) catch return throwError(env, "OOM");
        defer allocator.free(type_str);
        if (napi_get_value_string_utf8(env, type_val, type_str.ptr, type_len + 1, null) != .ok) return throwError(env, "Failed to get type");

        var value_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "value", &value_val) != .ok) return throwError(env, "Missing value");

        if (std.mem.eql(u8, type_str[0..type_len], "i64")) {
            var val: i64 = 0;
            var l: bool = true;
            if (napi_get_value_bigint_int64(env, value_val, &val, &l) != .ok) {
                if (napi_get_value_int64(env, value_val, &val) != .ok) return throwError(env, "Invalid i64 value");
            }
            filters[i] = .{ .field_index = @intCast(field_index), .value = .{ .i64 = val } };
        } else if (std.mem.eql(u8, type_str[0..type_len], "f64")) {
            var val: f64 = 0;
            if (napi_get_value_double(env, value_val, &val) != .ok) return throwError(env, "Invalid f64 value");
            filters[i] = .{ .field_index = @intCast(field_index), .value = .{ .f64 = val } };
        } else if (std.mem.eql(u8, type_str[0..type_len], "u64")) {
            var val: u64 = 0;
            var l: bool = true;
            if (napi_get_value_bigint_uint64(env, value_val, &val, &l) != .ok) return throwError(env, "Invalid u64 value");
            filters[i] = .{ .field_index = @intCast(field_index), .value = .{ .u64 = val } };
        } else if (std.mem.eql(u8, type_str[0..type_len], "bool")) {
            var val: bool = false;
            if (napi_get_value_bool(env, value_val, &val) != .ok) return throwError(env, "Invalid bool value");
            filters[i] = .{ .field_index = @intCast(field_index), .value = .{ .bool = val } };
        } else {
            return throwError(env, "Unsupported filter type");
        }
    }

    db.flush() catch |err| {
        return throwDbError(env, err);
    };

    const data = db.query(start, end, filters, allocator) catch |err| {
        return throwDbError(env, err);
    };

    var result: napi_value = undefined;
    const byte_length = data.len;

    _ = napi_create_external_arraybuffer(env, data.ptr, byte_length, freeData, null, &result);
    return result;
}

// dbGetStats(db: external, start: i64, end: i64, field_index: u32, compute_percentiles?: boolean): Object
fn dbGetStats(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    var argc: usize = 5;
    var argv: [5]napi_value = undefined;
    _ = napi_get_cb_info(env, info, &argc, &argv, null, null);
    if (argc < 4) return throwError(env, "Expected at least 4 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, argv[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var start: i64 = 0;
    var lossless: bool = true;
    if (napi_get_value_bigint_int64(env, argv[1], &start, &lossless) != .ok) {
        if (napi_get_value_int64(env, argv[1], &start) != .ok) return throwError(env, "Invalid start timestamp");
    }

    var end: i64 = 0;
    if (napi_get_value_bigint_int64(env, argv[2], &end, &lossless) != .ok) {
        if (napi_get_value_int64(env, argv[2], &end) != .ok) return throwError(env, "Invalid end timestamp");
    }

    var field_index: i64 = 0;
    if (napi_get_value_int64(env, argv[3], &field_index) != .ok) return throwError(env, "Invalid field index");

    var compute_percentiles: bool = false;
    if (argc >= 5) {
        _ = napi_get_value_bool(env, argv[4], &compute_percentiles);
    }

    const stats = db.getStats(start, end, @intCast(field_index), compute_percentiles) catch |err| {
        return throwDbError(env, err);
    };

    var result: napi_value = undefined;
    _ = napi_create_object(env, &result);

    var val: napi_value = undefined;
    _ = napi_create_double(env, stats.min, &val);
    _ = napi_set_named_property(env, result, "min", val);
    _ = napi_create_double(env, stats.max, &val);
    _ = napi_set_named_property(env, result, "max", val);
    _ = napi_create_double(env, stats.sum, &val);
    _ = napi_set_named_property(env, result, "sum", val);
    _ = napi_create_double(env, stats.mean, &val);
    _ = napi_set_named_property(env, result, "mean", val);
    _ = napi_create_bigint_uint64(env, @intCast(stats.count), &val);
    _ = napi_set_named_property(env, result, "count", val);

    if (compute_percentiles) {
        _ = napi_create_double(env, stats.p50, &val);
        _ = napi_set_named_property(env, result, "p50", val);
        _ = napi_create_double(env, stats.p90, &val);
        _ = napi_set_named_property(env, result, "p90", val);
        _ = napi_create_double(env, stats.p95, &val);
        _ = napi_set_named_property(env, result, "p95", val);
        _ = napi_create_double(env, stats.p99, &val);
        _ = napi_set_named_property(env, result, "p99", val);
    }

    return result;
}

// ... skipped dbGetLatest which is unchanged ...

export fn hocdb_get_stats(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, field_index: usize, flags: u32, out_stats: *hocdb.Stats) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const compute_percentiles = (flags & 1) != 0;
    const stats = db.getStats(start_ts, end_ts, field_index, compute_percentiles) catch return -1;
    out_stats.* = stats;
    return 0;
}

// dbGetLatest(db: external, field_index: u32): Object
fn dbGetLatest(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var field_index: i64 = 0;
    if (napi_get_value_int64(env, args[1], &field_index) != .ok) return throwError(env, "Invalid field index");

    const latest = db.getLatest(@intCast(field_index)) catch |err| {
        return throwDbError(env, err);
    };

    var result: napi_value = undefined;
    _ = napi_create_object(env, &result);

    var val: napi_value = undefined;
    _ = napi_create_double(env, latest.value, &val);
    _ = napi_set_named_property(env, result, "value", val);
    _ = napi_create_bigint_int64(env, latest.timestamp, &val);
    _ = napi_set_named_property(env, result, "timestamp", val);

    return result;
}

// dbClose(db: external): void
fn dbClose(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    db.deinit();
    std.heap.c_allocator.destroy(db);

    return null;
}

// dbDrop(db: external): void
fn dbDrop(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    db.drop() catch |err| {
        return throwDbError(env, err);
    };
    std.heap.c_allocator.destroy(db);

    return null;
}

// --- Indicators / analytics ---

const ind = hocdb.indicators;

/// Read a JS number or BigInt as i64 (BigInt first, number fallback).
fn readI64(env: napi_env, value: napi_value, out: *i64) bool {
    var lossless: bool = true;
    if (napi_get_value_bigint_int64(env, value, out, &lossless) == .ok) return true;
    return napi_get_value_int64(env, value, out) == .ok;
}

/// obj[name] as i64, or `default` when missing / not numeric.
fn propI64(env: napi_env, obj: napi_value, name: [*:0]const u8, default: i64) i64 {
    var has: bool = false;
    if (napi_has_named_property(env, obj, name, &has) != .ok or !has) return default;
    var v: napi_value = undefined;
    if (napi_get_named_property(env, obj, name, &v) != .ok) return default;
    var out: i64 = 0;
    if (!readI64(env, v, &out)) return default;
    return out;
}

/// obj[name] as f64, or `default` when missing / not numeric.
fn propF64(env: napi_env, obj: napi_value, name: [*:0]const u8, default: f64) f64 {
    var has: bool = false;
    if (napi_has_named_property(env, obj, name, &has) != .ok or !has) return default;
    var v: napi_value = undefined;
    if (napi_get_named_property(env, obj, name, &v) != .ok) return default;
    var out: f64 = 0;
    if (napi_get_value_double(env, v, &out) != .ok) return default;
    return out;
}

fn makeNumber(env: napi_env, x: f64) napi_value {
    var v: napi_value = undefined;
    if (napi_create_double(env, x, &v) != .ok) return null;
    return v;
}

fn makeString(env: napi_env, s: []const u8) napi_value {
    var v: napi_value = undefined;
    if (napi_create_string_utf8(env, s.ptr, s.len, &v) != .ok) return null;
    return v;
}

/// obj[name] = value for a (not necessarily NUL-terminated) name slice.
fn setProp(env: napi_env, obj: napi_value, name: []const u8, value: napi_value) void {
    const key = makeString(env, name);
    if (key == null) return;
    _ = napi_set_property(env, obj, key, value);
}

fn makeBool(env: napi_env, b: bool) napi_value {
    var v: napi_value = undefined;
    if (napi_get_boolean(env, b, &v) != .ok) return null;
    return v;
}

/// Unwrap a database handle created by dbInit (an `external` value); null when it is not one.
fn unwrapDb(env: napi_env, value: napi_value) ?*DB {
    var t: napi_valuetype = undefined;
    if (napi_typeof(env, value, &t) != .ok or t != .external) return null;
    var p: ?*anyopaque = null;
    if (napi_get_value_external(env, value, &p) != .ok) return null;
    return @ptrCast(@alignCast(p orelse return null));
}

/// JS array of numbers / BigInts -> []i64 (caller frees).
fn readI64Array(env: napi_env, arr: napi_value, allocator: std.mem.Allocator) ![]i64 {
    var len: u32 = 0;
    if (napi_get_array_length(env, arr, &len) != .ok) return error.NotAnArray;
    const out = try allocator.alloc(i64, len);
    errdefer allocator.free(out);
    var i: u32 = 0;
    while (i < len) : (i += 1) {
        var el: napi_value = undefined;
        if (napi_get_element(env, arr, i, &el) != .ok) return error.NotAnArray;
        if (!readI64(env, el, &out[i])) return error.InvalidElement;
    }
    return out;
}

/// JS array of numbers -> []f64 (caller frees).
fn readF64Array(env: napi_env, arr: napi_value, allocator: std.mem.Allocator) ![]f64 {
    var len: u32 = 0;
    if (napi_get_array_length(env, arr, &len) != .ok) return error.NotAnArray;
    const out = try allocator.alloc(f64, len);
    errdefer allocator.free(out);
    var i: u32 = 0;
    while (i < len) : (i += 1) {
        var el: napi_value = undefined;
        if (napi_get_element(env, arr, i, &el) != .ok) return error.NotAnArray;
        if (napi_get_value_double(env, el, &out[i]) != .ok) return error.InvalidElement;
    }
    return out;
}

fn arrayParseMessage(err: anyerror, what: []const u8) []const u8 {
    return switch (err) {
        error.NotAnArray => if (std.mem.eql(u8, what, "buckets")) "buckets must be an array" else "periodsPerYear must be an array",
        error.InvalidElement => if (std.mem.eql(u8, what, "buckets")) "buckets must contain integers" else "periodsPerYear must contain numbers",
        error.OutOfMemory => "OOM",
        else => @errorName(err),
    };
}

/// Copy a Zig slice into a fresh Float64Array / BigInt64Array owned by JS.
fn makeTypedArray(env: napi_env, comptime T: type, data: []const T) !napi_value {
    const ta_type: c_int = switch (T) {
        f64 => napi_float64_array,
        i64 => napi_bigint64_array,
        else => @compileError("unsupported typed array element type"),
    };
    const byte_len = data.len * @sizeOf(T);
    var buf_ptr: ?*anyopaque = null;
    var array_buffer: napi_value = undefined;
    if (napi_create_arraybuffer(env, byte_len, &buf_ptr, &array_buffer) != .ok) return error.NapiFailure;
    if (byte_len > 0) {
        const p = buf_ptr orelse return error.NapiFailure;
        @memcpy(@as([*]u8, @ptrCast(p))[0..byte_len], std.mem.sliceAsBytes(data));
    }
    var typed: napi_value = undefined;
    if (napi_create_typedarray(env, ta_type, data.len, array_buffer, 0, &typed) != .ok) return error.NapiFailure;
    return typed;
}

/// Convert an extern struct of i64/u64/f64 fields (Summary, Snapshot) into a JS object.
fn structToObject(env: napi_env, comptime T: type, value: T) napi_value {
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create result object");
    inline for (@typeInfo(T).@"struct".fields) |f| {
        var v: napi_value = undefined;
        const x = @field(value, f.name);
        switch (f.type) {
            i64 => _ = napi_create_bigint_int64(env, x, &v),
            u64 => _ = napi_create_bigint_uint64(env, x, &v),
            f64 => _ = napi_create_double(env, x, &v),
            else => @compileError("unsupported field type in " ++ @typeName(T)),
        }
        setProp(env, obj, f.name, v);
    }
    return obj;
}

fn indicatorErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.OutOfMemory => "Out of memory",
        error.InvalidParameter, error.InvalidPeriod => "Invalid indicator spec: unknown kind or bad period/parameter (session kinds need param = session length, or 0 to use the database's calendar sessions; buckets must be > 0)",
        error.CalendarRequired => "CalendarRequired: session kinds with param 0 use the database's calendar sessions: open it with { calendar, timestamp_unit_ns } or call setCalendar() and setTimestampUnit()",
        error.UnknownCalendar => "UnknownCalendar: no such calendar (built-in ids / names: 1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme; custom ids come from calendarDefine)",
        error.MissingColumn => "Indicator needs a column (open/high/low/volume/bid/ask/side) that was not provided in columns",
        error.MissingCloseColumn => "Missing close column",
        error.InvalidFieldIndex => "Invalid field index",
        error.FieldOverrideNotSupportedWithBucket => "Per-spec field overrides are not supported when bucket > 0",
        error.TooManyColumns => "Too many distinct columns requested",
        error.LengthMismatch => "Series length mismatch",
        else => @errorName(err),
    };
}

/// Throw an Error whose code is the Zig error name and whose message explains it.
fn throwIndicatorError(env: napi_env, err: anyerror) napi_value {
    return throwCoded(env, @errorName(err), indicatorErrorMessage(err));
}

fn parseColumns(env: napi_env, obj: napi_value) DB.IndicatorColumns {
    return .{
        .open = propI64(env, obj, "open", -1),
        .high = propI64(env, obj, "high", -1),
        .low = propI64(env, obj, "low", -1),
        .close = propI64(env, obj, "close", -1),
        .volume = propI64(env, obj, "volume", -1),
        .bid = propI64(env, obj, "bid", -1),
        .ask = propI64(env, obj, "ask", -1),
        .side = propI64(env, obj, "side", -1),
    };
}

fn propU32(env: napi_env, obj: napi_value, name: [*:0]const u8) !u32 {
    const v = propI64(env, obj, name, 0);
    if (v < 0 or v > std.math.maxInt(u32)) return error.InvalidSpec;
    return @intCast(v);
}

/// {kind, period, period2, period3, period4, param, param2, field_index, field_index2}
fn parseSpec(env: napi_env, obj: napi_value) !ind.Spec {
    var type_result: napi_valuetype = undefined;
    if (napi_typeof(env, obj, &type_result) != .ok or type_result != .object) return error.InvalidSpec;
    return .{
        .kind = try propU32(env, obj, "kind"),
        .period = try propU32(env, obj, "period"),
        .period2 = try propU32(env, obj, "period2"),
        .period3 = try propU32(env, obj, "period3"),
        .period4 = try propU32(env, obj, "period4"),
        .param = propF64(env, obj, "param", 0),
        .param2 = propF64(env, obj, "param2", 0),
        .field_index = propI64(env, obj, "field_index", -1),
        .field_index2 = propI64(env, obj, "field_index2", -1),
    };
}

fn parseSpecs(env: napi_env, arr: napi_value, allocator: std.mem.Allocator) ![]ind.Spec {
    var len: u32 = 0;
    if (napi_get_array_length(env, arr, &len) != .ok) return error.NotAnArray;
    const specs = try allocator.alloc(ind.Spec, len);
    errdefer allocator.free(specs);
    var i: u32 = 0;
    while (i < len) : (i += 1) {
        var element: napi_value = undefined;
        if (napi_get_element(env, arr, i, &element) != .ok) return error.NotAnArray;
        specs[i] = try parseSpec(env, element);
    }
    return specs;
}

fn specParseMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.NotAnArray => "specs must be an array of indicator spec objects",
        error.InvalidSpec => "Invalid indicator spec object",
        error.OutOfMemory => "OOM",
        else => @errorName(err),
    };
}

/// lookback: negative = auto (recommended warm-up), otherwise the row count.
fn readLookback(env: napi_env, value: napi_value) usize {
    var v: i64 = -1;
    if (!readI64(env, value, &v) or v < 0) return DB.lookback_auto;
    return @intCast(v);
}

/// {timestamps: BigInt64Array, values: Float64Array (planar), n_rows, n_outputs}
fn buildIndicatorResult(env: napi_env, res: DB.IndicatorResult) napi_value {
    defer res.deinit();
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create result object");
    const ts = makeTypedArray(env, i64, res.timestamps) catch return throwError(env, "Failed to allocate timestamps array");
    setProp(env, obj, "timestamps", ts);
    const values = makeTypedArray(env, f64, res.values) catch return throwError(env, "Failed to allocate values array");
    setProp(env, obj, "values", values);
    setProp(env, obj, "n_rows", makeNumber(env, @floatFromInt(res.n_rows)));
    setProp(env, obj, "n_outputs", makeNumber(env, @floatFromInt(res.n_outputs)));
    return obj;
}

// dbIndicators(db: external, start: i64, end: i64, columns: object, specs: object[], lookback: number, bucket: i64): Object
fn dbIndicators(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 7) catch return throwError(env, "Expected 7 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var start: i64 = 0;
    if (!readI64(env, args[1], &start)) return throwError(env, "Invalid start timestamp");
    var end: i64 = 0;
    if (!readI64(env, args[2], &end)) return throwError(env, "Invalid end timestamp");

    const cols = parseColumns(env, args[3]);
    const allocator = std.heap.c_allocator;
    const specs = parseSpecs(env, args[4], allocator) catch |err| return throwError(env, specParseMessage(err));
    defer allocator.free(specs);
    const lookback = readLookback(env, args[5]);
    var bucket: i64 = 0;
    if (!readI64(env, args[6], &bucket)) return throwError(env, "Invalid bucket");

    const res = db.indicatorsRange(start, end, cols, specs, lookback, bucket, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return buildIndicatorResult(env, res);
}

// dbIndicatorsTail(db: external, n_last: number, columns: object, specs: object[], lookback: number, bucket: i64): Object
fn dbIndicatorsTail(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 6) catch return throwError(env, "Expected 6 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var n_last: i64 = 0;
    if (!readI64(env, args[1], &n_last) or n_last < 0) return throwError(env, "Invalid tail count");

    const cols = parseColumns(env, args[2]);
    const allocator = std.heap.c_allocator;
    const specs = parseSpecs(env, args[3], allocator) catch |err| return throwError(env, specParseMessage(err));
    defer allocator.free(specs);
    const lookback = readLookback(env, args[4]);
    var bucket: i64 = 0;
    if (!readI64(env, args[5], &bucket)) return throwError(env, "Invalid bucket");

    const res = db.indicatorsTail(@intCast(n_last), cols, specs, lookback, bucket, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return buildIndicatorResult(env, res);
}

// dbPairIndicators(db: external, other: external, start: i64, end: i64, columns: object, columns2: object, specs: object[], lookback: number, bucket: i64): Object
// Indicators over `db` (series A) aligned with `other` (series B, whose close is the second input).
fn dbPairIndicators(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 9) catch return throwError(env, "Expected 9 arguments");

    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const other = unwrapDb(env, args[1]) orelse return throwError(env, "Invalid database handle for `other`");

    var start: i64 = 0;
    if (!readI64(env, args[2], &start)) return throwError(env, "Invalid start timestamp");
    var end: i64 = 0;
    if (!readI64(env, args[3], &end)) return throwError(env, "Invalid end timestamp");

    const cols = parseColumns(env, args[4]);
    const cols_b = parseColumns(env, args[5]);
    const allocator = std.heap.c_allocator;
    const specs = parseSpecs(env, args[6], allocator) catch |err| return throwError(env, specParseMessage(err));
    defer allocator.free(specs);
    const lookback = readLookback(env, args[7]);
    var bucket: i64 = 0;
    if (!readI64(env, args[8], &bucket)) return throwError(env, "Invalid bucket");

    const res = db.pairRange(other, cols, cols_b, start, end, specs, lookback, bucket, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return buildIndicatorResult(env, res);
}

// dbPairIndicatorsTail(db: external, other: external, n_last: number, columns: object, columns2: object, specs: object[], lookback: number, bucket: i64): Object
fn dbPairIndicatorsTail(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 8) catch return throwError(env, "Expected 8 arguments");

    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const other = unwrapDb(env, args[1]) orelse return throwError(env, "Invalid database handle for `other`");

    var n_last: i64 = 0;
    if (!readI64(env, args[2], &n_last) or n_last < 0) return throwError(env, "Invalid tail count");

    const cols = parseColumns(env, args[3]);
    const cols_b = parseColumns(env, args[4]);
    const allocator = std.heap.c_allocator;
    const specs = parseSpecs(env, args[5], allocator) catch |err| return throwError(env, specParseMessage(err));
    defer allocator.free(specs);
    const lookback = readLookback(env, args[6]);
    var bucket: i64 = 0;
    if (!readI64(env, args[7], &bucket)) return throwError(env, "Invalid bucket");

    const res = db.pairTail(other, cols, cols_b, @intCast(n_last), specs, lookback, bucket, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return buildIndicatorResult(env, res);
}

// dbOhlcv(db: external, start: i64, end: i64, price_field: number, volume_field: number (-1 = none), side_field: number (-1 = none), bucket: i64): Object
// The result has a `buy_volume` array only when a side field is given.
fn dbOhlcv(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 7) catch return throwError(env, "Expected 7 arguments");

    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");

    var start: i64 = 0;
    if (!readI64(env, args[1], &start)) return throwError(env, "Invalid start timestamp");
    var end: i64 = 0;
    if (!readI64(env, args[2], &end)) return throwError(env, "Invalid end timestamp");
    var price_field: i64 = 0;
    if (!readI64(env, args[3], &price_field) or price_field < 0) return throwError(env, "Invalid price field index");
    var volume_field: i64 = -1;
    if (!readI64(env, args[4], &volume_field)) return throwError(env, "Invalid volume field index");
    var side_field: i64 = -1;
    if (!readI64(env, args[5], &side_field)) return throwError(env, "Invalid side field index");
    var bucket: i64 = 0;
    if (!readI64(env, args[6], &bucket)) return throwError(env, "Invalid bucket");
    if (bucket <= 0) return throwError(env, "bucket must be > 0");

    const allocator = std.heap.c_allocator;
    const bars = db.ohlcvSide(start, end, @intCast(price_field), volume_field, side_field, bucket, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    defer bars.deinit(allocator);

    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create result object");
    const ts = makeTypedArray(env, i64, bars.ts) catch return throwError(env, "Failed to allocate timestamps array");
    setProp(env, obj, "timestamps", ts);
    const series = [_]struct { name: []const u8, data: []const f64 }{
        .{ .name = "open", .data = bars.open },
        .{ .name = "high", .data = bars.high },
        .{ .name = "low", .data = bars.low },
        .{ .name = "close", .data = bars.close },
        .{ .name = "volume", .data = bars.volume },
        .{ .name = "count", .data = bars.count },
    };
    for (series) |s| {
        const arr = makeTypedArray(env, f64, s.data) catch return throwError(env, "Failed to allocate bar array");
        setProp(env, obj, s.name, arr);
    }
    if (side_field >= 0) {
        const bv = makeTypedArray(env, f64, bars.buy_volume) catch return throwError(env, "Failed to allocate bar array");
        setProp(env, obj, "buy_volume", bv);
    }
    setProp(env, obj, "n_bars", makeNumber(env, @floatFromInt(bars.len())));
    return obj;
}

// dbSummary(db: external, start: i64, end: i64, field_index: number, periods_per_year: number): Object
fn dbSummary(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 5) catch return throwError(env, "Expected 5 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var start: i64 = 0;
    if (!readI64(env, args[1], &start)) return throwError(env, "Invalid start timestamp");
    var end: i64 = 0;
    if (!readI64(env, args[2], &end)) return throwError(env, "Invalid end timestamp");
    var field_index: i64 = 0;
    if (!readI64(env, args[3], &field_index) or field_index < 0) return throwError(env, "Invalid field index");
    var periods_per_year: f64 = 0;
    if (napi_get_value_double(env, args[4], &periods_per_year) != .ok) return throwError(env, "Invalid periods_per_year");

    const s = db.summary(start, end, @intCast(field_index), periods_per_year, std.heap.c_allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return structToObject(env, ind.Summary, s);
}

// dbSnapshot(db: external, columns: object, n_bars: number, bucket: i64, periods_per_year: number): Object
fn dbSnapshot(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 5) catch return throwError(env, "Expected 5 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    const cols = parseColumns(env, args[1]);
    var n_bars: i64 = 0;
    if (!readI64(env, args[2], &n_bars) or n_bars < 0) return throwError(env, "Invalid bars count");
    var bucket: i64 = 0;
    if (!readI64(env, args[3], &bucket)) return throwError(env, "Invalid bucket");
    var periods_per_year: f64 = 0;
    if (napi_get_value_double(env, args[4], &periods_per_year) != .ok) return throwError(env, "Invalid periods_per_year");

    const snap = db.snapshot(cols, @intCast(n_bars), bucket, periods_per_year, std.heap.c_allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return structToObject(env, ind.Snapshot, snap);
}

// dbHealth(db: external, start: i64, end: i64, price_field: number, volume_field: number (-1 = none), gap_threshold: i64, outlier_threshold: number): Object
fn dbHealth(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 7) catch return throwError(env, "Expected 7 arguments");

    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");

    var start: i64 = 0;
    if (!readI64(env, args[1], &start)) return throwError(env, "Invalid start timestamp");
    var end: i64 = 0;
    if (!readI64(env, args[2], &end)) return throwError(env, "Invalid end timestamp");
    var price_field: i64 = 0;
    if (!readI64(env, args[3], &price_field) or price_field < 0) return throwError(env, "Invalid price field index");
    var volume_field: i64 = -1;
    if (!readI64(env, args[4], &volume_field)) return throwError(env, "Invalid volume field index");
    var gap_threshold: i64 = 0;
    if (!readI64(env, args[5], &gap_threshold)) return throwError(env, "Invalid gap threshold");
    var outlier_threshold: f64 = 0;
    if (napi_get_value_double(env, args[6], &outlier_threshold) != .ok) return throwError(env, "Invalid outlier threshold");

    const h = db.health(start, end, @intCast(price_field), volume_field, gap_threshold, outlier_threshold, std.heap.c_allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    return structToObject(env, ind.Health, h);
}

/// [{timestamp, direction, size, horizon}] -> []Decision (direction / horizon default to 0, size to 1).
fn parseDecisions(env: napi_env, arr: napi_value, allocator: std.mem.Allocator) ![]ind.Decision {
    var len: u32 = 0;
    if (napi_get_array_length(env, arr, &len) != .ok) return error.NotAnArray;
    const out = try allocator.alloc(ind.Decision, len);
    errdefer allocator.free(out);
    var i: u32 = 0;
    while (i < len) : (i += 1) {
        var el: napi_value = undefined;
        if (napi_get_element(env, arr, i, &el) != .ok) return error.NotAnArray;
        var t: napi_valuetype = undefined;
        if (napi_typeof(env, el, &t) != .ok or t != .object) return error.InvalidDecision;
        var has_ts: bool = false;
        if (napi_has_named_property(env, el, "timestamp", &has_ts) != .ok or !has_ts) return error.InvalidDecision;
        out[i] = .{
            .timestamp = propI64(env, el, "timestamp", 0),
            .direction = propF64(env, el, "direction", 0),
            .size = propF64(env, el, "size", 1),
            .horizon = propI64(env, el, "horizon", 0),
        };
    }
    return out;
}

// dbEvaluate(db: external, price_field: number, decisions: object[], default_horizon: i64, cost_bps: number): Object
// Returns the Evaluation fields plus per-decision `entry`, `exit` and `net_return` Float64Arrays.
fn dbEvaluate(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 5) catch return throwError(env, "Expected 5 arguments");

    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");

    var price_field: i64 = 0;
    if (!readI64(env, args[1], &price_field) or price_field < 0) return throwError(env, "Invalid price field index");
    const allocator = std.heap.c_allocator;
    const decisions = parseDecisions(env, args[2], allocator) catch |err| return throwError(env, switch (err) {
        error.NotAnArray => "decisions must be an array of {timestamp, direction, size, horizon} objects",
        error.InvalidDecision => "Invalid decision: expected an object with a timestamp",
        error.OutOfMemory => "OOM",
    });
    defer allocator.free(decisions);
    var default_horizon: i64 = 0;
    if (!readI64(env, args[3], &default_horizon)) return throwError(env, "Invalid default horizon");
    var cost_bps: f64 = 0;
    if (napi_get_value_double(env, args[4], &cost_bps) != .ok) return throwError(env, "Invalid cost_bps");

    const n = decisions.len;
    const entry = allocator.alloc(f64, n) catch return throwError(env, "OOM");
    defer allocator.free(entry);
    const exit = allocator.alloc(f64, n) catch return throwError(env, "OOM");
    defer allocator.free(exit);
    const net = allocator.alloc(f64, n) catch return throwError(env, "OOM");
    defer allocator.free(net);

    const ev = db.evaluate(@intCast(price_field), decisions, default_horizon, cost_bps, entry, exit, net, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };
    const obj = structToObject(env, ind.Evaluation, ev);
    if (obj == null) return null;
    const per_decision = [_]struct { name: []const u8, data: []const f64 }{
        .{ .name = "entry", .data = entry },
        .{ .name = "exit", .data = exit },
        .{ .name = "net_return", .data = net },
    };
    for (per_decision) |s| {
        const arr = makeTypedArray(env, f64, s.data) catch return throwError(env, "Failed to allocate result array");
        setProp(env, obj, s.name, arr);
    }
    return obj;
}

// dbSnapshotMulti(db: external, columns: object, n_bars: number, buckets: (number|bigint)[], periods_per_year: number[]): Object[]
// One snapshot object per bucket, in bucket order.
fn dbSnapshotMulti(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 5) catch return throwError(env, "Expected 5 arguments");

    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");

    const cols = parseColumns(env, args[1]);
    var n_bars: i64 = 0;
    if (!readI64(env, args[2], &n_bars) or n_bars < 0) return throwError(env, "Invalid bars count");
    const allocator = std.heap.c_allocator;
    const buckets = readI64Array(env, args[3], allocator) catch |err| return throwError(env, arrayParseMessage(err, "buckets"));
    defer allocator.free(buckets);
    const ppy = readF64Array(env, args[4], allocator) catch |err| return throwError(env, arrayParseMessage(err, "periodsPerYear"));
    defer allocator.free(ppy);
    if (ppy.len != buckets.len) return throwError(env, "periodsPerYear must have one entry per bucket");

    const out = allocator.alloc(ind.Snapshot, buckets.len) catch return throwError(env, "OOM");
    defer allocator.free(out);
    db.snapshotMulti(cols, @intCast(n_bars), buckets, ppy, out, allocator) catch |err| {
        return throwIndicatorError(env, err);
    };

    var arr: napi_value = undefined;
    if (napi_create_array_with_length(env, out.len, &arr) != .ok) return throwError(env, "Failed to create array");
    for (out, 0..) |snap, i| {
        const obj = structToObject(env, ind.Snapshot, snap);
        if (obj == null) return null;
        _ = napi_set_element(env, arr, @intCast(i), obj);
    }
    return arr;
}

// indicatorRegistry(): [{id: number, name: string, outputs: string[], lookahead: boolean}]
fn indicatorRegistry(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    _ = info;
    var arr: napi_value = undefined;
    if (napi_create_array_with_length(env, ind.all_kinds.len, &arr) != .ok) return throwError(env, "Failed to create array");
    for (ind.all_kinds, 0..) |kind, i| {
        var entry: napi_value = undefined;
        if (napi_create_object(env, &entry) != .ok) return throwError(env, "Failed to create object");
        setProp(env, entry, "id", makeNumber(env, @floatFromInt(@intFromEnum(kind))));
        setProp(env, entry, "name", makeString(env, ind.kindName(kind)));
        const names = ind.outputNames(kind);
        var outputs: napi_value = undefined;
        if (napi_create_array_with_length(env, names.len, &outputs) != .ok) return throwError(env, "Failed to create array");
        for (names, 0..) |name, j| _ = napi_set_element(env, outputs, @intCast(j), makeString(env, name));
        setProp(env, entry, "outputs", outputs);
        setProp(env, entry, "lookahead", makeBool(env, ind.isLookahead(kind)));
        _ = napi_set_element(env, arr, @intCast(i), entry);
    }
    return arr;
}

// indicatorWarmup(spec: object): number
fn indicatorWarmup(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const spec = parseSpec(env, args[0]) catch |err| return throwError(env, specParseMessage(err));
    const params = ind.resolve(spec) catch |err| return throwIndicatorError(env, err);
    return makeNumber(env, @floatFromInt(ind.warmup(params)));
}

// --- Durability, readers, maintenance and metrics ---

// dbSync(db: external): void -- flush + fsync now (writers only)
fn dbSync(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    db.sync() catch |err| return throwDbError(env, err);
    return null;
}

// dbRefresh(db: external): void -- readers pick up the writer's latest commit; no-op for writers
fn dbRefresh(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    db.refresh() catch |err| return throwDbError(env, err);
    return null;
}

// dbVerify(db: external): boolean -- CRC32C of the committed data matches; throws ChecksumUnavailable
fn dbVerify(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const ok = db.verify() catch |err| return throwDbError(env, err);
    return makeBool(env, ok);
}

// dbCompact(db: external, min_ts: i64): void -- keep records with timestamp >= min_ts
fn dbCompact(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    var min_ts: i64 = 0;
    if (!readI64(env, args[1], &min_ts)) return throwError(env, "Invalid min timestamp");
    db.compact(min_ts) catch |err| return throwDbError(env, err);
    return null;
}

// dbRetainLast(db: external, n: number): void -- keep the last n records
fn dbRetainLast(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    var n: u64 = 0;
    if (!readU64(env, args[1], &n)) return throwError(env, "retainLast: n must be a non-negative integer");
    db.retainLast(n) catch |err| return throwDbError(env, err);
    return null;
}

// dbRollover(db: external): string -- archive the file as <ticker>.<first_ts>-<last_ts>.bin, continue empty
fn dbRollover(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const archive = db.rollover(std.heap.c_allocator) catch |err| return throwDbError(env, err);
    defer std.heap.c_allocator.free(archive);
    return makeString(env, archive);
}

// dbMetrics(db: external): Object -- the 30 operational counters (BigInt fields)
fn dbMetrics(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    return structToObject(env, DB.Metrics, db.getMetrics());
}

// dbMetricsReset(db: external): void
fn dbMetricsReset(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    db.resetMetrics();
    return null;
}

// dbHeaderSize(): number -- bytes reserved by the file header (64)
fn dbHeaderSize(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    _ = info;
    return makeNumber(env, @floatFromInt(DB.HEADER_SIZE));
}

// dbFormatVersion(db: external): number -- 1 legacy, 2 current
fn dbFormatVersion(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    return makeNumber(env, @floatFromInt(db.format_version));
}

// dbIsReadOnly(db: external): boolean
fn dbIsReadOnly(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    return makeBool(env, db.read_only);
}

// --- Trading calendars, signal backtester and universe features ---

const cal = hocdb.calendar;
const bt = hocdb.backtest_mod;
const uni = hocdb.universe_mod;

fn isNullish(env: napi_env, value: napi_value) bool {
    var t: napi_valuetype = undefined;
    return napi_typeof(env, value, &t) != .ok or t == .undefined or t == .null;
}

fn makeNull(env: napi_env) napi_value {
    var v: napi_value = undefined;
    if (napi_get_null(env, &v) != .ok) return null;
    return v;
}

/// Throw a plain Error with a formatted message.
fn throwFmt(env: napi_env, comptime fmt: []const u8, args: anytype) napi_value {
    const msg = std.fmt.allocPrint(std.heap.c_allocator, fmt, args) catch return throwError(env, "Invalid argument");
    defer std.heap.c_allocator.free(msg);
    return throwError(env, msg);
}

/// Throw an Error with `code` and a formatted message.
fn throwCodedFmt(env: napi_env, code: []const u8, comptime fmt: []const u8, args: anytype) napi_value {
    const msg = std.fmt.allocPrint(std.heap.c_allocator, fmt, args) catch return throwCoded(env, code, code);
    defer std.heap.c_allocator.free(msg);
    return throwCoded(env, code, msg);
}

// -- series arguments (Float64Array / BigInt64Array / plain arrays) --

const SeriesError = error{ NotASeries, InvalidElement, OutOfMemory };

const TypedInfo = struct { ta_type: c_int, len: usize, data: ?*anyopaque };

fn typedArrayInfo(env: napi_env, value: napi_value) ?TypedInfo {
    var is_ta = false;
    if (napi_is_typedarray(env, value, &is_ta) != .ok or !is_ta) return null;
    var ti: TypedInfo = .{ .ta_type = 0, .len = 0, .data = null };
    var ab: napi_value = undefined;
    var off: usize = 0;
    if (napi_get_typedarray_info(env, value, &ti.ta_type, &ti.len, &ti.data, &ab, &off) != .ok) return null;
    return ti;
}

/// Element count of a JS Array or typed array, null for anything else.
fn seriesLength(env: napi_env, value: napi_value) ?usize {
    if (typedArrayInfo(env, value)) |ti| return ti.len;
    var is_arr = false;
    if (napi_is_array(env, value, &is_arr) != .ok or !is_arr) return null;
    var n: u32 = 0;
    if (napi_get_array_length(env, value, &n) != .ok) return null;
    return n;
}

/// Float64Array (memcpy), any other numeric typed array or an array of numbers -> []f64 (caller frees).
fn readF64Series(env: napi_env, value: napi_value, allocator: std.mem.Allocator) SeriesError![]f64 {
    const ti = typedArrayInfo(env, value);
    const n = seriesLength(env, value) orelse return error.NotASeries;
    const out = try allocator.alloc(f64, n);
    errdefer allocator.free(out);
    if (ti != null and ti.?.ta_type == napi_float64_array) {
        if (n > 0) @memcpy(std.mem.sliceAsBytes(out), @as([*]const u8, @ptrCast(ti.?.data.?))[0 .. n * @sizeOf(f64)]);
        return out;
    }
    for (out, 0..) |*x, i| {
        var el: napi_value = undefined;
        if (napi_get_element(env, value, @intCast(i), &el) != .ok) return error.NotASeries;
        if (napi_get_value_double(env, el, x) != .ok) return error.InvalidElement;
    }
    return out;
}

/// BigInt64Array (memcpy), any other typed array or an array of numbers / BigInts -> []i64 (caller frees).
fn readI64Series(env: napi_env, value: napi_value, allocator: std.mem.Allocator) SeriesError![]i64 {
    const ti = typedArrayInfo(env, value);
    const n = seriesLength(env, value) orelse return error.NotASeries;
    const out = try allocator.alloc(i64, n);
    errdefer allocator.free(out);
    if (ti != null and ti.?.ta_type == napi_bigint64_array) {
        if (n > 0) @memcpy(std.mem.sliceAsBytes(out), @as([*]const u8, @ptrCast(ti.?.data.?))[0 .. n * @sizeOf(i64)]);
        return out;
    }
    for (out, 0..) |*x, i| {
        var el: napi_value = undefined;
        if (napi_get_element(env, value, @intCast(i), &el) != .ok) return error.NotASeries;
        if (!readI64(env, el, x)) return error.InvalidElement;
    }
    return out;
}

/// null / undefined -> null, else readF64Series.
fn readOptF64Series(env: napi_env, value: napi_value, allocator: std.mem.Allocator) SeriesError!?[]f64 {
    if (isNullish(env, value)) return null;
    return try readF64Series(env, value, allocator);
}

fn freeOpt(comptime T: type, s: ?[]T, allocator: std.mem.Allocator) void {
    if (s) |x| allocator.free(x);
}

/// Array of series -> slices (free with freeSeriesList).
fn readSeriesList(env: napi_env, value: napi_value, allocator: std.mem.Allocator) SeriesError![][]const f64 {
    var is_arr = false;
    if (napi_is_array(env, value, &is_arr) != .ok or !is_arr) return error.NotASeries;
    var n: u32 = 0;
    if (napi_get_array_length(env, value, &n) != .ok) return error.NotASeries;
    const out = try allocator.alloc([]const f64, n);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |s| allocator.free(s);
        allocator.free(out);
    }
    for (0..n) |i| {
        var el: napi_value = undefined;
        if (napi_get_element(env, value, @intCast(i), &el) != .ok) return error.NotASeries;
        out[i] = try readF64Series(env, el, allocator);
        filled += 1;
    }
    return out;
}

fn freeSeriesList(list: [][]const f64, allocator: std.mem.Allocator) void {
    for (list) |s| allocator.free(s);
    allocator.free(list);
}

fn throwSeriesError(env: napi_env, err: SeriesError, what: []const u8) napi_value {
    return switch (err) {
        error.NotASeries => throwFmt(env, "{s} must be a Float64Array (BigInt64Array for timestamps) or an array of numbers", .{what}),
        error.InvalidElement => throwFmt(env, "{s} must contain numbers only", .{what}),
        error.OutOfMemory => throwError(env, "Out of memory"),
    };
}

/// JS array of database handles (externals) -> []*DB (caller frees).
fn readDbArray(env: napi_env, value: napi_value, allocator: std.mem.Allocator) ![]*DB {
    var is_arr = false;
    if (napi_is_array(env, value, &is_arr) != .ok or !is_arr) return error.NotAnArray;
    var n: u32 = 0;
    if (napi_get_array_length(env, value, &n) != .ok) return error.NotAnArray;
    const out = try allocator.alloc(*DB, n);
    errdefer allocator.free(out);
    for (0..n) |i| {
        var el: napi_value = undefined;
        if (napi_get_element(env, value, @intCast(i), &el) != .ok) return error.NotAnArray;
        out[i] = unwrapDb(env, el) orelse return error.InvalidElement;
    }
    return out;
}

// -- calendars --

/// Calendar by id argument; throws an Error with code UnknownCalendar when there is none.
fn calendarArg(env: napi_env, value: napi_value) ?*const cal.Calendar {
    var id: i64 = -1;
    if (!readI64(env, value, &id) or id < 0 or id > std.math.maxInt(u32)) {
        _ = throwCoded(env, "UnknownCalendar", "UnknownCalendar: the calendar id must be a non-negative integer (use calendarId(name) to resolve names)");
        return null;
    }
    return cal.get(@intCast(id)) orelse {
        _ = throwCodedFmt(env, "UnknownCalendar", "UnknownCalendar: no calendar with id {d} (built-in: 1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme; custom ids come from calendarDefine)", .{id});
        return null;
    };
}

/// {open, close, tradeDay, earlyClose} (UTC seconds / days since 1970-01-01 as numbers).
fn makeSession(env: napi_env, s: cal.Session) napi_value {
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create session object");
    setProp(env, obj, "open", makeNumber(env, @floatFromInt(s.open)));
    setProp(env, obj, "close", makeNumber(env, @floatFromInt(s.close)));
    setProp(env, obj, "tradeDay", makeNumber(env, @floatFromInt(s.trade_day)));
    setProp(env, obj, "earlyClose", makeBool(env, s.early_close != 0));
    return obj;
}

fn sessionOrNull(env: napi_env, s: ?cal.Session) napi_value {
    return if (s) |x| makeSession(env, x) else makeNull(env);
}

fn readI64Arg(env: napi_env, value: napi_value, what: []const u8) ?i64 {
    var x: i64 = 0;
    if (!readI64(env, value, &x)) {
        _ = throwFmt(env, "{s} must be an integer (number or BigInt)", .{what});
        return null;
    }
    return x;
}

// calendarId(name: string): number -- 0 when unknown
fn calendarId(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const name = readString(env, args[0]) catch return throwError(env, "calendarId: name must be a string");
    defer freeString(name);
    return makeNumber(env, @floatFromInt(cal.idByName(name)));
}

// calendarName(id: number): string | null
fn calendarName(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    var id: i64 = -1;
    if (!readI64(env, args[0], &id) or id < 0 or id > std.math.maxInt(u32)) return makeNull(env);
    const c = cal.get(@intCast(id)) orelse return makeNull(env);
    return makeString(env, c.name);
}

// calendarSession(id, utcSec, which): Session | null -- which 0 = containing, 1 = that or the previous, 2 = that or the next
fn calendarSession(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 3) catch return throwError(env, "Expected 3 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    const utc = readI64Arg(env, args[1], "utcSec") orelse return null;
    const which = readI64Arg(env, args[2], "which") orelse return null;
    const s = switch (which) {
        0 => c.sessionAt(utc),
        1 => c.prevSession(utc),
        2 => c.nextSession(utc),
        else => return throwError(env, "calendarSession: which must be 0 (containing), 1 (previous) or 2 (next)"),
    };
    return sessionOrNull(env, s);
}

// calendarSessionForDay(id, day): Session | null -- day = days since 1970-01-01 (local trade date)
fn calendarSessionForDay(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    const day = readI64Arg(env, args[1], "day") orelse return null;
    return sessionOrNull(env, c.sessionForDay(day));
}

// calendarIsOpen(id, utcSec): boolean
fn calendarIsOpen(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    const utc = readI64Arg(env, args[1], "utcSec") orelse return null;
    return makeBool(env, c.isOpen(utc));
}

// calendarOpenSeconds(id, a, b): number -- trading seconds inside [a, b)
fn calendarOpenSeconds(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 3) catch return throwError(env, "Expected 3 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    const a = readI64Arg(env, args[1], "a") orelse return null;
    const b = readI64Arg(env, args[2], "b") orelse return null;
    return makeNumber(env, @floatFromInt(c.openSecondsBetween(a, b)));
}

// calendarSessionsBetween(id, a, b): number -- sessions opening inside [a, b)
fn calendarSessionsBetween(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 3) catch return throwError(env, "Expected 3 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    const a = readI64Arg(env, args[1], "a") orelse return null;
    const b = readI64Arg(env, args[2], "b") orelse return null;
    return makeNumber(env, @floatFromInt(c.sessionsBetween(a, b)));
}

// calendarPeriodsPerYear(id, bucketSec: number): number
fn calendarPeriodsPerYear(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    var bucket_sec: f64 = 0;
    if (napi_get_value_double(env, args[1], &bucket_sec) != .ok) return throwError(env, "bucketSec must be a number");
    return makeNumber(env, c.periodsPerYear(bucket_sec));
}

// calendarToLocal(id, utcSec): number -- local wall-clock seconds
fn calendarToLocal(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const c = calendarArg(env, args[0]) orelse return null;
    const utc = readI64Arg(env, args[1], "utcSec") orelse return null;
    return makeNumber(env, @floatFromInt(c.utcToLocal(utc)));
}

// daysFromCivil(year, month, day): number -- days since 1970-01-01
fn daysFromCivil(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 3) catch return throwError(env, "Expected 3 arguments");
    const y = readI64Arg(env, args[0], "year") orelse return null;
    const m = readI64Arg(env, args[1], "month") orelse return null;
    const d = readI64Arg(env, args[2], "day") orelse return null;
    if (m < 1 or m > 12 or d < 1 or d > 31) return throwError(env, "daysFromCivil: month must be 1-12 and day 1-31");
    return makeNumber(env, @floatFromInt(cal.daysFromCivil(y, @intCast(m), @intCast(d))));
}

// civilFromDays(days): {year, month, day}
fn civilFromDays(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const days = readI64Arg(env, args[0], "days") orelse return null;
    const c = cal.civilFromDays(days);
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create result object");
    setProp(env, obj, "year", makeNumber(env, @floatFromInt(c.year)));
    setProp(env, obj, "month", makeNumber(env, @floatFromInt(c.month)));
    setProp(env, obj, "day", makeNumber(env, @floatFromInt(c.day)));
    return obj;
}

fn toI32(x: i64) ?i32 {
    if (x < std.math.minInt(i32) or x > std.math.maxInt(i32)) return null;
    return @intCast(x);
}

// calendarDefine(name: string, weekly: number[14] (open, close seconds per weekday, Monday first; close <= open = no session),
//                utcOffsetSec: number, dst: 0 none / 1 us / 2 eu, holidays: number[] (day numbers),
//                earlyCloses: number[] (day, closeSec pairs), sessionsPerYear: number): number (the id)
fn calendarDefine(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 7) catch return throwError(env, "Expected 7 arguments");
    const allocator = std.heap.c_allocator;
    const name = readString(env, args[0]) catch return throwError(env, "calendarDefine: name must be a string");
    defer freeString(name);
    const weekly = readI64Array(env, args[1], allocator) catch return throwError(env, "calendarDefine: weekly must be an array of 14 integers (open, close per weekday)");
    defer allocator.free(weekly);
    if (weekly.len != 14) return throwError(env, "calendarDefine: weekly must have 7 entries (Monday first)");
    var tpl: [7]?cal.DaySession = .{null} ** 7;
    for (0..7) |i| {
        const o = toI32(weekly[2 * i]) orelse return throwError(env, "calendarDefine: session seconds out of range");
        const c = toI32(weekly[2 * i + 1]) orelse return throwError(env, "calendarDefine: session seconds out of range");
        if (c > o) tpl[i] = .{ .open_sec = o, .close_sec = c };
    }
    const off = readI64Arg(env, args[2], "utcOffsetSec") orelse return null;
    const off32 = toI32(off) orelse return throwError(env, "calendarDefine: utcOffsetSec out of range");
    const dst_i = readI64Arg(env, args[3], "dstRule") orelse return null;
    const dst: cal.DstRule = switch (dst_i) {
        0 => .none,
        1 => .us,
        2 => .eu,
        else => return throwError(env, "calendarDefine: dstRule must be 'none' (0), 'us' (1) or 'eu' (2)"),
    };
    const hol64 = readI64Array(env, args[4], allocator) catch return throwError(env, "calendarDefine: holidays must be an array of day numbers");
    defer allocator.free(hol64);
    const hol = allocator.alloc(i32, hol64.len) catch return throwError(env, "Out of memory");
    defer allocator.free(hol);
    for (hol64, 0..) |d, i| hol[i] = toI32(d) orelse return throwError(env, "calendarDefine: holiday day number out of range");
    const early64 = readI64Array(env, args[5], allocator) catch return throwError(env, "calendarDefine: earlyCloses must be an array of { day, close_sec } entries");
    defer allocator.free(early64);
    if (early64.len % 2 != 0) return throwError(env, "calendarDefine: earlyCloses must be (day, closeSec) pairs");
    const early = allocator.alloc(cal.EarlyClose, early64.len / 2) catch return throwError(env, "Out of memory");
    defer allocator.free(early);
    for (early, 0..) |*e, i| {
        e.* = .{
            .day = toI32(early64[2 * i]) orelse return throwError(env, "calendarDefine: early close day out of range"),
            .close_sec = toI32(early64[2 * i + 1]) orelse return throwError(env, "calendarDefine: early close seconds out of range"),
        };
    }
    var spy: f64 = 0;
    if (napi_get_value_double(env, args[6], &spy) != .ok) return throwError(env, "calendarDefine: sessionsPerYear must be a number");
    const id = cal.define(name, tpl, off32, dst, hol, early, spy) catch |err| return switch (err) {
        error.TooManyCalendars => throwCoded(env, "TooManyCalendars", "TooManyCalendars: the custom calendar registry holds 32 calendars (redefining an existing name reuses its id)"),
        error.InvalidParameter => throwCoded(env, "InvalidParameter", "InvalidParameter: calendarDefine needs a non-empty name, sessionsPerYear > 0 and close > open for every session"),
        error.OutOfMemory => throwError(env, "Out of memory"),
    };
    return makeNumber(env, @floatFromInt(id));
}

// dbSetCalendar(db, id: number): void -- throws code UnknownCalendar
fn dbSetCalendar(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    var id: i64 = -1;
    if (!readI64(env, args[1], &id) or id < 0 or id > std.math.maxInt(u32)) return throwDbError(env, error.UnknownCalendar);
    db.setCalendar(@intCast(id)) catch |err| return throwDbError(env, err);
    return null;
}

// dbGetCalendar(db): number -- 0 = none
fn dbGetCalendar(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    return makeNumber(env, @floatFromInt(db.calendar_id));
}

// dbSetTimestampUnit(db, unitNs: number | bigint): void
fn dbSetTimestampUnit(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    var unit: u64 = 0;
    if (!readU64(env, args[1], &unit)) return throwError(env, "setTimestampUnit: unitNs must be a non-negative integer (nanoseconds per timestamp unit)");
    db.setTimestampUnit(unit) catch |err| return throwDbError(env, err);
    return null;
}

// dbGetTimestampUnit(db): number -- nanoseconds per timestamp unit, 0 = unknown
fn dbGetTimestampUnit(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    return makeNumber(env, @floatFromInt(db.timestampUnitNs()));
}

// dbPeriodsPerYear(db, bucket: i64): number -- 0 when the calendar or the unit is unknown
fn dbPeriodsPerYear(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const bucket = readI64Arg(env, args[1], "bucket") orelse return null;
    return makeNumber(env, db.periodsPerYear(bucket));
}

// -- params objects (snake_case or camelCase keys, defaults from the Zig structs) --

fn paramFail(env: napi_env, key: [*:0]const u8, what: []const u8) error{Thrown} {
    _ = throwFmt(env, "Invalid params.{s}: {s}", .{ key, what });
    return error.Thrown;
}

fn paramF64(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *f64) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    if (napi_get_value_double(env, v, out) != .ok) return paramFail(env, keys[0], "expected a number");
}

fn paramU64(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *u64) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    if (!readU64(env, v, out)) return paramFail(env, keys[0], "expected a non-negative integer");
}

fn paramFlag(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *u64) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    var b = false;
    if (!readBoolish(env, v, &b)) return paramFail(env, keys[0], "expected a boolean");
    out.* = if (b) 1 else 0;
}

/// Case-insensitive comparison ignoring '_', '-' and ' ' ("next_open" == "nextOpen" == "NEXT OPEN").
fn eqlLoose(a: []const u8, b: []const u8) bool {
    var i: usize = 0;
    var j: usize = 0;
    while (true) {
        while (i < a.len and (a[i] == '_' or a[i] == '-' or a[i] == ' ')) i += 1;
        while (j < b.len and (b[j] == '_' or b[j] == '-' or b[j] == ' ')) j += 1;
        if (i == a.len or j == b.len) return i == a.len and j == b.len;
        if (std.ascii.toLower(a[i]) != std.ascii.toLower(b[j])) return false;
        i += 1;
        j += 1;
    }
}

/// Enumerated option: an index or one of `names` (case-insensitive, separators ignored).
fn paramEnum(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, names: []const []const u8, what: []const u8, out: *u64) error{Thrown}!void {
    const v = propAny(env, obj, keys) orelse return;
    if (readString(env, v)) |s| {
        defer freeString(s);
        for (names, 0..) |name, i| {
            if (eqlLoose(s, name)) {
                out.* = i;
                return;
            }
        }
        return paramFail(env, keys[0], what);
    } else |_| {}
    var x: u64 = 0;
    if (!readU64(env, v, &x) or x >= names.len) return paramFail(env, keys[0], what);
    out.* = x;
}

fn checkParamsObject(env: napi_env, obj: napi_value) error{Thrown}!bool {
    if (isNullish(env, obj)) return false;
    var t: napi_valuetype = undefined;
    if (napi_typeof(env, obj, &t) != .ok or t != .object) {
        _ = throwError(env, "params must be an object");
        return error.Thrown;
    }
    return true;
}

fn parseBacktestParams(env: napi_env, obj: napi_value) error{Thrown}!bt.Params {
    var p = bt.Params{};
    if (!try checkParamsObject(env, obj)) return p;
    try paramF64(env, obj, &.{ "initial_equity", "initialEquity" }, &p.initial_equity);
    try paramF64(env, obj, &.{ "cost_bps", "costBps" }, &p.cost_bps);
    try paramF64(env, obj, &.{ "slippage_bps", "slippageBps" }, &p.slippage_bps);
    try paramF64(env, obj, &.{ "stop_loss", "stopLoss" }, &p.stop_loss);
    try paramF64(env, obj, &.{ "take_profit", "takeProfit" }, &p.take_profit);
    try paramF64(env, obj, &.{ "trailing_stop", "trailingStop" }, &p.trailing_stop);
    try paramF64(env, obj, &.{ "max_position", "maxPosition" }, &p.max_position);
    try paramEnum(env, obj, &.{ "position_mode", "positionMode" }, &.{ "units", "fraction", "notional" }, "expected 0 / 'units', 1 / 'fraction' or 2 / 'notional'", &p.position_mode);
    try paramEnum(env, obj, &.{ "fill_mode", "fillMode" }, &.{ "next_open", "same_close" }, "expected 0 / 'next_open' or 1 / 'same_close'", &p.fill_mode);
    try paramF64(env, obj, &.{ "periods_per_year", "periodsPerYear" }, &p.periods_per_year);
    try paramFlag(env, obj, &.{ "allow_short", "allowShort" }, &p.allow_short);
    try paramF64(env, obj, &.{ "risk_free_rate", "riskFreeRate" }, &p.risk_free_rate);
    return p;
}

fn parseUniverseParams(env: napi_env, obj: napi_value) error{Thrown}!uni.Params {
    var p = uni.Params{};
    if (!try checkParamsObject(env, obj)) return p;
    try paramU64(env, obj, &.{ "mom_short", "momShort" }, &p.mom_short);
    try paramU64(env, obj, &.{ "mom_mid", "momMid" }, &p.mom_mid);
    try paramU64(env, obj, &.{ "mom_long", "momLong" }, &p.mom_long);
    try paramU64(env, obj, &.{ "vol_period", "volPeriod" }, &p.vol_period);
    try paramU64(env, obj, &.{ "corr_period", "corrPeriod" }, &p.corr_period);
    try paramU64(env, obj, &.{ "sma_period", "smaPeriod" }, &p.sma_period);
    try paramU64(env, obj, &.{ "beta_period", "betaPeriod" }, &p.beta_period);
    try paramF64(env, obj, &.{ "periods_per_year", "periodsPerYear" }, &p.periods_per_year);
    try paramEnum(env, obj, &.{ "weights_mode", "weightsMode" }, &.{ "equal", "volume" }, "expected 0 / 'equal' or 1 / 'volume'", &p.weights_mode);
    return p;
}

// -- backtester --

fn throwBacktestError(env: napi_env, err: anyerror, target_len: usize) napi_value {
    return switch (err) {
        error.LengthMismatch => throwCodedFmt(env, "LengthMismatch", "LengthMismatch: target length {d} does not match the number of rows (pass one target per row of indicators() / ohlcv() over the same window; ts, open, high, low, close and target must all have the same length)", .{target_len}),
        error.InvalidParameter => throwCoded(env, "InvalidParameter", "InvalidParameter: backtest params out of range (position_mode 0-2, fill_mode 0-1; cost_bps, slippage_bps, stop_loss, take_profit, trailing_stop, max_position and periods_per_year >= 0; finite initial_equity / risk_free_rate; splits must lie inside the data)"),
        error.MissingCloseColumn => throwCoded(env, "MissingCloseColumn", "Missing close column"),
        error.InvalidFieldIndex => throwCoded(env, "InvalidFieldIndex", "Invalid field index"),
        error.OutOfMemory => throwError(env, "Out of memory"),
        else => throwDbError(env, err),
    };
}

fn throwUniverseError(env: napi_env, err: anyerror) napi_value {
    return switch (err) {
        error.InvalidPeriod => throwCoded(env, "InvalidPeriod", "InvalidPeriod: universe periods must be >= 1 (mom_short, mom_mid, mom_long, sma_period), >= 2 (vol_period, beta_period) and >= 3 (corr_period)"),
        error.InvalidParameter => throwCoded(env, "InvalidParameter", "InvalidParameter: weights_mode must be 0 ('equal') or 1 ('volume')"),
        error.LengthMismatch => throwCoded(env, "LengthMismatch", "LengthMismatch: every close (and volume) series must have the same length, and ts too"),
        error.MissingCloseColumn => throwCoded(env, "MissingCloseColumn", "Missing close column"),
        error.InvalidFieldIndex => throwCoded(env, "InvalidFieldIndex", "Invalid field index"),
        error.OutOfMemory => throwError(env, "Out of memory"),
        else => throwDbError(env, err),
    };
}

/// {equity, position, cash, pnl, drawdown: bool, maxTrades: number} (maxTrades present -> a trade list is returned).
const BacktestOptions = struct {
    equity: bool = false,
    position: bool = false,
    cash: bool = false,
    pnl: bool = false,
    drawdown: bool = false,
    want_trades: bool = false,
    max_trades: usize = 0,
};

fn optBool(env: napi_env, obj: napi_value, key: [*:0]const u8, out: *bool) error{Thrown}!void {
    const v = propAny(env, obj, &.{key}) orelse return;
    if (!readBoolish(env, v, out)) {
        _ = throwFmt(env, "Invalid options.{s}: expected a boolean", .{key});
        return error.Thrown;
    }
}

fn parseBacktestOptions(env: napi_env, obj: napi_value) error{Thrown}!BacktestOptions {
    var o = BacktestOptions{};
    if (isNullish(env, obj)) return o;
    var t: napi_valuetype = undefined;
    if (napi_typeof(env, obj, &t) != .ok or t != .object) {
        _ = throwError(env, "options must be an object");
        return error.Thrown;
    }
    try optBool(env, obj, "equity", &o.equity);
    try optBool(env, obj, "position", &o.position);
    try optBool(env, obj, "cash", &o.cash);
    try optBool(env, obj, "pnl", &o.pnl);
    try optBool(env, obj, "drawdown", &o.drawdown);
    if (propAny(env, obj, &.{ "maxTrades", "max_trades" })) |v| {
        var x: u64 = 0;
        if (!readU64(env, v, &x)) {
            _ = throwError(env, "Invalid options.maxTrades: expected a non-negative integer");
            return error.Thrown;
        }
        o.want_trades = true;
        o.max_trades = @intCast(@min(x, @as(u64, 1 << 26)));
    }
    return o;
}

/// Per-bar output buffers and the trade list, sized from the options.
const BacktestBuffers = struct {
    equity: ?[]f64 = null,
    position: ?[]f64 = null,
    cash: ?[]f64 = null,
    pnl: ?[]f64 = null,
    drawdown: ?[]f64 = null,
    trades: ?[]bt.Trade = null,

    fn init(opts: BacktestOptions, n: usize, allocator: std.mem.Allocator) !BacktestBuffers {
        var b = BacktestBuffers{};
        errdefer b.deinit(allocator);
        if (opts.equity) b.equity = try allocator.alloc(f64, n);
        if (opts.position) b.position = try allocator.alloc(f64, n);
        if (opts.cash) b.cash = try allocator.alloc(f64, n);
        if (opts.pnl) b.pnl = try allocator.alloc(f64, n);
        if (opts.drawdown) b.drawdown = try allocator.alloc(f64, n);
        if (opts.want_trades) b.trades = try allocator.alloc(bt.Trade, opts.max_trades);
        return b;
    }

    fn deinit(self: *BacktestBuffers, allocator: std.mem.Allocator) void {
        freeOpt(f64, self.equity, allocator);
        freeOpt(f64, self.position, allocator);
        freeOpt(f64, self.cash, allocator);
        freeOpt(f64, self.pnl, allocator);
        freeOpt(f64, self.drawdown, allocator);
        freeOpt(bt.Trade, self.trades, allocator);
        self.* = .{};
    }

    fn outputs(self: BacktestBuffers) bt.Outputs {
        return .{ .equity = self.equity, .position = self.position, .cash = self.cash, .pnl = self.pnl, .drawdown = self.drawdown };
    }
};

/// {result, trades?, equity?, position?, cash?, pnl?, drawdown?}
fn buildBacktestResult(env: napi_env, res: bt.Result, bufs: BacktestBuffers) napi_value {
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create result object");
    const r = structToObject(env, bt.Result, res);
    if (r == null) return null;
    setProp(env, obj, "result", r);
    if (bufs.trades) |t| {
        const k: usize = @intCast(@min(res.n_trades, @as(u64, t.len)));
        var arr: napi_value = undefined;
        if (napi_create_array_with_length(env, k, &arr) != .ok) return throwError(env, "Failed to create trades array");
        for (t[0..k], 0..) |trade, i| {
            const o = structToObject(env, bt.Trade, trade);
            if (o == null) return null;
            _ = napi_set_element(env, arr, @intCast(i), o);
        }
        setProp(env, obj, "trades", arr);
    }
    inline for (.{ "equity", "position", "cash", "pnl", "drawdown" }) |name| {
        if (@field(bufs, name)) |data| {
            const arr = makeTypedArray(env, f64, data) catch return throwError(env, "Failed to allocate output array");
            setProp(env, obj, name, arr);
        }
    }
    return obj;
}

// dbBacktest(db, columns, target: Float64Array | number[], start: i64, end: i64, bucket: i64, params: object, options: object): Object
// target[i] is the desired position at the end of row i of indicators(start, end, bucket); fills at the next open by default.
fn dbBacktest(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 8) catch return throwError(env, "Expected 8 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const cols = parseColumns(env, args[1]);
    const allocator = std.heap.c_allocator;
    const target = readF64Series(env, args[2], allocator) catch |err| return throwSeriesError(env, err, "target");
    defer allocator.free(target);
    const start = readI64Arg(env, args[3], "start") orelse return null;
    const end = readI64Arg(env, args[4], "end") orelse return null;
    const bucket = readI64Arg(env, args[5], "bucket") orelse return null;
    const params = parseBacktestParams(env, args[6]) catch return null;
    const opts = parseBacktestOptions(env, args[7]) catch return null;
    var bufs = BacktestBuffers.init(opts, target.len, allocator) catch return throwError(env, "Out of memory");
    defer bufs.deinit(allocator);
    const res = db.backtest(cols, start, end, bucket, target, params, bufs.outputs(), bufs.trades, allocator) catch |err| {
        return throwBacktestError(env, err, target.len);
    };
    return buildBacktestResult(env, res, bufs);
}

// dbBacktestTail(db, columns, target, bucket: i64, params, options): Object -- the last target.length bars / records
fn dbBacktestTail(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 6) catch return throwError(env, "Expected 6 arguments");
    const db = unwrapDb(env, args[0]) orelse return throwError(env, "Invalid database handle");
    const cols = parseColumns(env, args[1]);
    const allocator = std.heap.c_allocator;
    const target = readF64Series(env, args[2], allocator) catch |err| return throwSeriesError(env, err, "target");
    defer allocator.free(target);
    const bucket = readI64Arg(env, args[3], "bucket") orelse return null;
    const params = parseBacktestParams(env, args[4]) catch return null;
    const opts = parseBacktestOptions(env, args[5]) catch return null;
    var bufs = BacktestBuffers.init(opts, target.len, allocator) catch return throwError(env, "Out of memory");
    defer bufs.deinit(allocator);
    const res = db.backtestTail(cols, target.len, bucket, target, params, bufs.outputs(), bufs.trades, allocator) catch |err| {
        return throwBacktestError(env, err, target.len);
    };
    return buildBacktestResult(env, res, bufs);
}

/// ts / open / high / low / close / target arguments of the array entry points (args[base..base+6]).
const BarArrays = struct {
    ts: []i64,
    open: ?[]f64,
    high: ?[]f64,
    low: ?[]f64,
    close: []f64,
    target: []f64,

    fn read(env: napi_env, args: []const napi_value, allocator: std.mem.Allocator) ?BarArrays {
        var b: BarArrays = undefined;
        b.ts = readI64Series(env, args[0], allocator) catch |err| {
            _ = throwSeriesError(env, err, "ts");
            return null;
        };
        errdefer allocator.free(b.ts);
        b.open = readOptF64Series(env, args[1], allocator) catch |err| {
            _ = throwSeriesError(env, err, "open");
            return null;
        };
        errdefer freeOpt(f64, b.open, allocator);
        b.high = readOptF64Series(env, args[2], allocator) catch |err| {
            _ = throwSeriesError(env, err, "high");
            return null;
        };
        errdefer freeOpt(f64, b.high, allocator);
        b.low = readOptF64Series(env, args[3], allocator) catch |err| {
            _ = throwSeriesError(env, err, "low");
            return null;
        };
        errdefer freeOpt(f64, b.low, allocator);
        b.close = readF64Series(env, args[4], allocator) catch |err| {
            _ = throwSeriesError(env, err, "close");
            return null;
        };
        errdefer allocator.free(b.close);
        b.target = readF64Series(env, args[5], allocator) catch |err| {
            _ = throwSeriesError(env, err, "target");
            return null;
        };
        return b;
    }

    fn deinit(self: BarArrays, allocator: std.mem.Allocator) void {
        allocator.free(self.ts);
        freeOpt(f64, self.open, allocator);
        freeOpt(f64, self.high, allocator);
        freeOpt(f64, self.low, allocator);
        allocator.free(self.close);
        allocator.free(self.target);
    }
};

// backtestArrays(ts, open | null, high | null, low | null, close, target, params, options): Object
fn backtestArrays(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 8) catch return throwError(env, "Expected 8 arguments");
    const allocator = std.heap.c_allocator;
    const bars = BarArrays.read(env, args[0..6], allocator) orelse return null;
    defer bars.deinit(allocator);
    const params = parseBacktestParams(env, args[6]) catch return null;
    const opts = parseBacktestOptions(env, args[7]) catch return null;
    var bufs = BacktestBuffers.init(opts, bars.close.len, allocator) catch return throwError(env, "Out of memory");
    defer bufs.deinit(allocator);
    const res = bt.run(bars.ts, bars.open, bars.high, bars.low, bars.close, bars.target, params, bufs.outputs(), bufs.trades, allocator) catch |err| {
        return throwBacktestError(env, err, bars.target.len);
    };
    return buildBacktestResult(env, res, bufs);
}

fn makeSplit(env: napi_env, s: bt.Split) napi_value {
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create split object");
    setProp(env, obj, "train_start", makeNumber(env, @floatFromInt(s.train_start)));
    setProp(env, obj, "train_end", makeNumber(env, @floatFromInt(s.train_end)));
    setProp(env, obj, "test_start", makeNumber(env, @floatFromInt(s.test_start)));
    setProp(env, obj, "test_end", makeNumber(env, @floatFromInt(s.test_end)));
    return obj;
}

// walkForwardSplits(n, nSplits, trainFrac, anchored: boolean): [{train_start, train_end, test_start, test_end}]
fn walkForwardSplits(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 4) catch return throwError(env, "Expected 4 arguments");
    var n: u64 = 0;
    if (!readU64(env, args[0], &n)) return throwError(env, "walkForwardSplits: n must be a non-negative integer");
    var n_splits: u64 = 0;
    if (!readU64(env, args[1], &n_splits)) return throwError(env, "walkForwardSplits: nSplits must be a non-negative integer");
    var train_frac: f64 = 0;
    if (napi_get_value_double(env, args[2], &train_frac) != .ok) return throwError(env, "walkForwardSplits: trainFrac must be a number");
    var anchored = true;
    if (!readBoolish(env, args[3], &anchored)) return throwError(env, "walkForwardSplits: anchored must be a boolean");
    const allocator = std.heap.c_allocator;
    const cap: usize = @intCast(@min(n_splits, n));
    const out = allocator.alloc(bt.Split, cap) catch return throwError(env, "Out of memory");
    defer allocator.free(out);
    const count = bt.walkForwardSplits(@intCast(n), @intCast(n_splits), train_frac, anchored, out);
    var arr: napi_value = undefined;
    if (napi_create_array_with_length(env, count, &arr) != .ok) return throwError(env, "Failed to create array");
    for (out[0..count], 0..) |s, i| {
        const o = makeSplit(env, s);
        if (o == null) return null;
        _ = napi_set_element(env, arr, @intCast(i), o);
    }
    return arr;
}

fn splitField(env: napi_env, obj: napi_value, keys: []const [*:0]const u8, out: *u64) bool {
    const v = propAny(env, obj, keys) orelse return false;
    return readU64(env, v, out);
}

/// [{train_start, train_end, test_start, test_end}] (camelCase accepted) -> []Split (caller frees).
fn readSplits(env: napi_env, value: napi_value, allocator: std.mem.Allocator) ![]bt.Split {
    var is_arr = false;
    if (napi_is_array(env, value, &is_arr) != .ok or !is_arr) return error.NotAnArray;
    var n: u32 = 0;
    if (napi_get_array_length(env, value, &n) != .ok) return error.NotAnArray;
    const out = try allocator.alloc(bt.Split, n);
    errdefer allocator.free(out);
    for (out, 0..) |*s, i| {
        var el: napi_value = undefined;
        if (napi_get_element(env, value, @intCast(i), &el) != .ok) return error.NotAnArray;
        var t: napi_valuetype = undefined;
        if (napi_typeof(env, el, &t) != .ok or t != .object) return error.InvalidElement;
        s.* = .{ .train_start = 0, .train_end = 0, .test_start = 0, .test_end = 0 };
        _ = splitField(env, el, &.{ "train_start", "trainStart" }, &s.train_start);
        _ = splitField(env, el, &.{ "train_end", "trainEnd" }, &s.train_end);
        if (!splitField(env, el, &.{ "test_start", "testStart" }, &s.test_start)) return error.InvalidElement;
        if (!splitField(env, el, &.{ "test_end", "testEnd" }, &s.test_end)) return error.InvalidElement;
    }
    return out;
}

// backtestSplits(ts, open | null, high | null, low | null, close, target, splits: object[], params): Object[] -- one result per test window
fn backtestSplits(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 8) catch return throwError(env, "Expected 8 arguments");
    const allocator = std.heap.c_allocator;
    const bars = BarArrays.read(env, args[0..6], allocator) orelse return null;
    defer bars.deinit(allocator);
    const splits = readSplits(env, args[6], allocator) catch |err| return throwError(env, switch (err) {
        error.NotAnArray => "splits must be an array of { train_start, train_end, test_start, test_end } objects (see walkForwardSplits)",
        error.InvalidElement => "Invalid split: expected an object with test_start and test_end (non-negative integers)",
        error.OutOfMemory => "Out of memory",
    });
    defer allocator.free(splits);
    const params = parseBacktestParams(env, args[7]) catch return null;
    const results = allocator.alloc(bt.Result, splits.len) catch return throwError(env, "Out of memory");
    defer allocator.free(results);
    const count = bt.runSplits(bars.ts, bars.open, bars.high, bars.low, bars.close, bars.target, params, splits, results, allocator) catch |err| {
        return throwBacktestError(env, err, bars.target.len);
    };
    var arr: napi_value = undefined;
    if (napi_create_array_with_length(env, count, &arr) != .ok) return throwError(env, "Failed to create array");
    for (results[0..count], 0..) |r, i| {
        const o = structToObject(env, bt.Result, r);
        if (o == null) return null;
        _ = napi_set_element(env, arr, @intCast(i), o);
    }
    return arr;
}

// -- universe --

/// {summary, rows: [...], corr: number[][] | null}
fn buildUniverseResult(env: napi_env, summary: uni.Summary, rows: []const uni.Row, corr: ?[]const f64) napi_value {
    var obj: napi_value = undefined;
    if (napi_create_object(env, &obj) != .ok) return throwError(env, "Failed to create result object");
    const s = structToObject(env, uni.Summary, summary);
    if (s == null) return null;
    setProp(env, obj, "summary", s);
    const m = rows.len;
    var arr: napi_value = undefined;
    if (napi_create_array_with_length(env, m, &arr) != .ok) return throwError(env, "Failed to create rows array");
    for (rows, 0..) |row, i| {
        const o = structToObject(env, uni.Row, row);
        if (o == null) return null;
        _ = napi_set_element(env, arr, @intCast(i), o);
    }
    setProp(env, obj, "rows", arr);
    if (corr) |c| {
        var outer: napi_value = undefined;
        if (napi_create_array_with_length(env, m, &outer) != .ok) return throwError(env, "Failed to create corr array");
        for (0..m) |i| {
            var inner: napi_value = undefined;
            if (napi_create_array_with_length(env, m, &inner) != .ok) return throwError(env, "Failed to create corr array");
            for (0..m) |j| _ = napi_set_element(env, inner, @intCast(j), makeNumber(env, c[i * m + j]));
            _ = napi_set_element(env, outer, @intCast(i), inner);
        }
        setProp(env, obj, "corr", outer);
    } else {
        setProp(env, obj, "corr", makeNull(env));
    }
    return obj;
}

// universe(dbs: external[], columns, nBars: number, bucket: i64, params: object, corr: boolean): Object
fn universe(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 6) catch return throwError(env, "Expected 6 arguments");
    const allocator = std.heap.c_allocator;
    const dbs = readDbArray(env, args[0], allocator) catch |err| return throwError(env, switch (err) {
        error.NotAnArray => "universe: dbs must be an array of databases opened with dbInit / openReader",
        error.InvalidElement => "universe: every entry of dbs must be a database handle opened with dbInit / openReader",
        error.OutOfMemory => "Out of memory",
    });
    defer allocator.free(dbs);
    const cols = parseColumns(env, args[1]);
    var n_bars: u64 = 0;
    if (!readU64(env, args[2], &n_bars)) return throwError(env, "universe: bars must be a non-negative integer");
    const bucket = readI64Arg(env, args[3], "bucket") orelse return null;
    const params = parseUniverseParams(env, args[4]) catch return null;
    var want_corr = true;
    if (!readBoolish(env, args[5], &want_corr)) return throwError(env, "universe: corr must be a boolean");
    const m = dbs.len;
    const rows = allocator.alloc(uni.Row, m) catch return throwError(env, "Out of memory");
    defer allocator.free(rows);
    const corr: ?[]f64 = if (want_corr) allocator.alloc(f64, m * m) catch return throwError(env, "Out of memory") else null;
    defer freeOpt(f64, corr, allocator);
    const summary = DB.universe(dbs, cols, @intCast(n_bars), bucket, params, rows, corr, allocator) catch |err| {
        return throwUniverseError(env, err);
    };
    return buildUniverseResult(env, summary, rows, corr);
}

// universeArrays(closes: series[], volumes: series[] | null, ts: i64 series | null, params: object, corr: boolean): Object
fn universeArrays(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 5) catch return throwError(env, "Expected 5 arguments");
    const allocator = std.heap.c_allocator;
    const closes = readSeriesList(env, args[0], allocator) catch |err| return throwSeriesError(env, err, "closes (an array of close series, one per ticker)");
    defer freeSeriesList(closes, allocator);
    var volumes: ?[][]const f64 = null;
    if (!isNullish(env, args[1])) {
        volumes = readSeriesList(env, args[1], allocator) catch |err| return throwSeriesError(env, err, "volumes (an array of volume series, one per ticker)");
    }
    defer if (volumes) |v| freeSeriesList(v, allocator);
    if (volumes != null and volumes.?.len != closes.len) return throwCoded(env, "LengthMismatch", "LengthMismatch: volumes must have one series per close series");
    var ts: ?[]i64 = null;
    if (!isNullish(env, args[2])) {
        ts = readI64Series(env, args[2], allocator) catch |err| return throwSeriesError(env, err, "ts");
    }
    defer freeOpt(i64, ts, allocator);
    const params = parseUniverseParams(env, args[3]) catch return null;
    var want_corr = true;
    if (!readBoolish(env, args[4], &want_corr)) return throwError(env, "universeArrays: corr must be a boolean");
    const m = closes.len;
    const rows = allocator.alloc(uni.Row, m) catch return throwError(env, "Out of memory");
    defer allocator.free(rows);
    const corr: ?[]f64 = if (want_corr) allocator.alloc(f64, m * m) catch return throwError(env, "Out of memory") else null;
    defer freeOpt(f64, corr, allocator);
    const summary = uni.compute(closes, volumes, ts, params, rows, corr, allocator) catch |err| {
        return throwUniverseError(env, err);
    };
    return buildUniverseResult(env, summary, rows, corr);
}

// --- Module Registration ---

export fn napi_register_module_v1(env: napi_env, exports: napi_value) napi_value {
    const descriptors = [_]napi_property_descriptor{
        .{ .utf8name = "dbInit", .name = null, .method = dbInit, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbAppend", .name = null, .method = dbAppend, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbFlush", .name = null, .method = dbFlush, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbLoad", .name = null, .method = dbLoad, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbQuery", .name = null, .method = dbQuery, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbGetStats", .name = null, .method = dbGetStats, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbGetLatest", .name = null, .method = dbGetLatest, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbClose", .name = null, .method = dbClose, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbDrop", .name = null, .method = dbDrop, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbIndicators", .name = null, .method = dbIndicators, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbIndicatorsTail", .name = null, .method = dbIndicatorsTail, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbOhlcv", .name = null, .method = dbOhlcv, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbSummary", .name = null, .method = dbSummary, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbSnapshot", .name = null, .method = dbSnapshot, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbPairIndicators", .name = null, .method = dbPairIndicators, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbPairIndicatorsTail", .name = null, .method = dbPairIndicatorsTail, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbHealth", .name = null, .method = dbHealth, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbEvaluate", .name = null, .method = dbEvaluate, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbSnapshotMulti", .name = null, .method = dbSnapshotMulti, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "indicatorRegistry", .name = null, .method = indicatorRegistry, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "indicatorWarmup", .name = null, .method = indicatorWarmup, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbOpenReader", .name = null, .method = dbOpenReader, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbSync", .name = null, .method = dbSync, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbRefresh", .name = null, .method = dbRefresh, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbVerify", .name = null, .method = dbVerify, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbCompact", .name = null, .method = dbCompact, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbRetainLast", .name = null, .method = dbRetainLast, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbRollover", .name = null, .method = dbRollover, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbMetrics", .name = null, .method = dbMetrics, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbMetricsReset", .name = null, .method = dbMetricsReset, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbHeaderSize", .name = null, .method = dbHeaderSize, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbFormatVersion", .name = null, .method = dbFormatVersion, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbIsReadOnly", .name = null, .method = dbIsReadOnly, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarId", .name = null, .method = calendarId, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarName", .name = null, .method = calendarName, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarSession", .name = null, .method = calendarSession, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarSessionForDay", .name = null, .method = calendarSessionForDay, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarIsOpen", .name = null, .method = calendarIsOpen, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarOpenSeconds", .name = null, .method = calendarOpenSeconds, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarSessionsBetween", .name = null, .method = calendarSessionsBetween, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarPeriodsPerYear", .name = null, .method = calendarPeriodsPerYear, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarToLocal", .name = null, .method = calendarToLocal, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "daysFromCivil", .name = null, .method = daysFromCivil, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "civilFromDays", .name = null, .method = civilFromDays, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "calendarDefine", .name = null, .method = calendarDefine, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbSetCalendar", .name = null, .method = dbSetCalendar, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbGetCalendar", .name = null, .method = dbGetCalendar, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbSetTimestampUnit", .name = null, .method = dbSetTimestampUnit, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbGetTimestampUnit", .name = null, .method = dbGetTimestampUnit, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbPeriodsPerYear", .name = null, .method = dbPeriodsPerYear, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbBacktest", .name = null, .method = dbBacktest, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbBacktestTail", .name = null, .method = dbBacktestTail, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "backtestArrays", .name = null, .method = backtestArrays, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "walkForwardSplits", .name = null, .method = walkForwardSplits, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "backtestSplits", .name = null, .method = backtestSplits, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "universe", .name = null, .method = universe, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "universeArrays", .name = null, .method = universeArrays, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
    };

    _ = napi_define_properties(env, exports, descriptors.len, &descriptors);
    return exports;
}

// --- C-ABI Exports (for Bun FFI / Python / Rust) ---

pub const CField = extern struct {
    name: [*:0]const u8,
    type: c_int, // 1=i64, 2=f64, 3=u64
};

export fn hocdb_init(ticker_ptr: [*]const u8, ticker_len: usize, path_ptr: [*]const u8, path_len: usize, schema_ptr: [*]const CField, schema_len: usize, max_size: i64, overwrite: c_int, flush: c_int, auto_increment: c_int) ?*anyopaque {
    const ticker = ticker_ptr[0..ticker_len];
    const path = path_ptr[0..path_len];

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
    db_ptr.* = DB.init(ticker_dupe, path_dupe, std.heap.c_allocator, schema, config) catch {
        std.heap.c_allocator.free(ticker_dupe);
        std.heap.c_allocator.free(path_dupe);
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };
    db_ptr.initWriter() catch {
        std.heap.c_allocator.free(ticker_dupe);
        std.heap.c_allocator.free(path_dupe);
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };

    std.heap.c_allocator.free(ticker_dupe);
    std.heap.c_allocator.free(path_dupe);

    return db_ptr;
}

export fn hocdb_append(db_ptr: *anyopaque, data_ptr: [*]const u8, data_len: usize) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.append(data_ptr[0..data_len]) catch return -1;
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
    return data.ptr;
}

export fn hocdb_query(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, out_len: *usize) ?[*]u8 {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.flush() catch return null;
    const data = db.query(start_ts, end_ts, &[_]hocdb.Filter{}, std.heap.c_allocator) catch return null;
    out_len.* = data.len;
    return data.ptr;
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

export fn hocdb_free(ptr: ?*anyopaque) void {
    if (ptr) |p| {
        std.c.free(p);
    }
}
