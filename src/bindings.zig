const std = @import("std");
const hocdb = @import("hocdb");

// Define the data structure we are exposing
const TradeData = struct {
    timestamp: i64,
    usd: f64,
    volume: f64,
};

const DB = hocdb.TimeSeriesDB(TradeData);

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

extern "c" fn napi_define_properties(env: napi_env, object: napi_value, property_count: usize, properties: [*]const napi_property_descriptor) napi_status;
extern "c" fn napi_get_cb_info(env: napi_env, cbinfo: napi_callback_info, argc: *usize, argv: [*]napi_value, this_arg: ?*napi_value, data: ?*?*anyopaque) napi_status;
extern "c" fn napi_create_string_utf8(env: napi_env, str: [*]const u8, length: usize, result: *napi_value) napi_status;
extern "c" fn napi_create_double(env: napi_env, value: f64, result: *napi_value) napi_status;
extern "c" fn napi_create_int64(env: napi_env, value: i64, result: *napi_value) napi_status;
extern "c" fn napi_get_value_double(env: napi_env, value: napi_value, result: *f64) napi_status;
extern "c" fn napi_get_value_int64(env: napi_env, value: napi_value, result: *i64) napi_status;
extern "c" fn napi_get_value_string_utf8(env: napi_env, value: napi_value, buf: ?[*]u8, bufsize: usize, result: ?*usize) napi_status;
extern "c" fn napi_create_external(env: napi_env, data: *anyopaque, finalize_cb: ?napi_finalize, finalize_hint: ?*anyopaque, result: *napi_value) napi_status;
extern "c" fn napi_get_value_external(env: napi_env, value: napi_value, result: *?*anyopaque) napi_status;
extern "c" fn napi_throw_error(env: napi_env, code: ?[*:0]const u8, msg: [*:0]const u8) napi_status;
extern "c" fn napi_create_external_arraybuffer(env: napi_env, external_data: *anyopaque, byte_length: usize, finalize_cb: ?napi_finalize, finalize_hint: ?*anyopaque, result: *napi_value) napi_status;

// --- Helper Functions ---

fn throwError(env: napi_env, msg: []const u8) napi_value {
    const msg_z = std.heap.c_allocator.dupeZ(u8, msg) catch return null;
    defer std.heap.c_allocator.free(msg_z);
    _ = napi_throw_error(env, null, msg_z);
    return null;
}

fn getArgCount(env: napi_env, info: napi_callback_info) usize {
    var argc: usize = 0;
    _ = napi_get_cb_info(env, info, &argc, null, null, null);
    return argc;
}

fn getArgs(env: napi_env, info: napi_callback_info, comptime N: usize) ![N]napi_value {
    var argc: usize = N;
    var argv: [N]napi_value = undefined;
    _ = napi_get_cb_info(env, info, &argc, &argv, null, null);
    if (argc < N) return error.NotEnoughArguments;
    return argv;
}

// --- Implementation ---

// dbInit(ticker: string, path: string): external
fn dbInit(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 2) catch return throwError(env, "Expected 2 arguments: ticker, path");

    // Get Ticker
    var ticker_len: usize = 0;
    _ = napi_get_value_string_utf8(env, args[0], null, 0, &ticker_len);
    const ticker = std.heap.c_allocator.alloc(u8, ticker_len + 1) catch return throwError(env, "OOM");
    defer std.heap.c_allocator.free(ticker);
    _ = napi_get_value_string_utf8(env, args[0], ticker.ptr, ticker_len + 1, null);

    // Get Path
    var path_len: usize = 0;
    _ = napi_get_value_string_utf8(env, args[1], null, 0, &path_len);
    const path = std.heap.c_allocator.alloc(u8, path_len + 1) catch return throwError(env, "OOM");
    defer std.heap.c_allocator.free(path);
    _ = napi_get_value_string_utf8(env, args[1], path.ptr, path_len + 1, null);

    // Init DB
    const db_ptr = std.heap.c_allocator.create(DB) catch return throwError(env, "OOM");
    db_ptr.* = DB.init(ticker[0..ticker_len], path[0..path_len]) catch |err| {
        std.heap.c_allocator.destroy(db_ptr);
        return throwError(env, @errorName(err));
    };

    // Wrap in External
    var result: napi_value = undefined;
    _ = napi_create_external(env, db_ptr, null, null, &result);
    return result;
}

// dbAppend(db: external, timestamp: number, usd: number, volume: number): void
fn dbAppend(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 4) catch return throwError(env, "Expected 4 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var timestamp: i64 = 0;
    _ = napi_get_value_int64(env, args[1], &timestamp);

    var usd: f64 = 0;
    _ = napi_get_value_double(env, args[2], &usd);

    var volume: f64 = 0;
    _ = napi_get_value_double(env, args[3], &volume);

    db.append(.{ .timestamp = timestamp, .usd = usd, .volume = volume }) catch |err| {
        return throwError(env, @errorName(err));
    };

    return null;
}

// Finalizer for ArrayBuffer
fn freeData(env: napi_env, data: *anyopaque, hint: *anyopaque) callconv(.c) void {
    _ = env;
    _ = hint;
    // We don't need to cast to slice_ptr if we just free data.
    // const slice_ptr = @as([*]TradeData, @ptrCast(@alignCast(data)));
    // We don't know the length here to pass to free, but we used c_allocator.
    // Wait, DB.load uses 'allocator' passed to it.
    // If we use c_allocator in dbLoad, we can use free.
    // But Zig allocator interface requires length for free.
    // We should probably use a struct wrapper or just use standard malloc/free if we want to be safe with void*.
    // Or we can store the slice metadata in the hint?
    // Let's use the hint to store the allocator? No, we need the slice.

    // Better approach: Use std.heap.c_allocator for load in dbLoad.
    // Then we can use std.c.free(data).
    std.c.free(data);
}

// dbLoad(db: external): ArrayBuffer
fn dbLoad(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 1) catch return throwError(env, "Expected 1 argument");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    // Use C allocator so we can free it with std.c.free in finalizer
    // We need to adapt C allocator to Zig Allocator interface
    const allocator = std.heap.c_allocator;

    // Flush any buffered data before loading
    db.flush() catch |err| {
        return throwError(env, @errorName(err));
    };

    const data = db.load(allocator) catch |err| {
        return throwError(env, @errorName(err));
    };

    // Create External ArrayBuffer
    var result: napi_value = undefined;
    const byte_length = data.len * @sizeOf(TradeData);

    // We pass 'data.ptr' as the data.
    // We pass 'freeData' as finalizer.
    // We don't need a hint if we use std.c.free.

    _ = napi_create_external_arraybuffer(env, data.ptr, byte_length, freeData, null, &result);
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

// --- Module Registration ---

export fn napi_register_module_v1(env: napi_env, exports: napi_value) napi_value {
    const descriptors = [_]napi_property_descriptor{
        .{ .utf8name = "dbInit", .name = null, .method = dbInit, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbAppend", .name = null, .method = dbAppend, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbLoad", .name = null, .method = dbLoad, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbClose", .name = null, .method = dbClose, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
    };

    _ = napi_define_properties(env, exports, descriptors.len, &descriptors);
    return exports;
}

// --- C-ABI Exports (for Bun FFI / Python / Rust) ---

export fn hocdb_init(ticker_ptr: [*]const u8, ticker_len: usize, path_ptr: [*]const u8, path_len: usize) ?*anyopaque {
    const ticker = ticker_ptr[0..ticker_len];
    const path = path_ptr[0..path_len];

    // We must copy the strings because the caller might free them
    // But DB.init copies them internally? No, DB.init expects slices.
    // DB.init stores the paths? Let's check root.zig.
    // DB.init(ticker, path) calls fs.cwd().makePath(path) and stores ticker/path in struct?
    // TimeSeriesDB struct has: ticker: []const u8, root_dir: []const u8.
    // And init does: return Self { .ticker = ticker, .root_dir = root_dir ... }
    // So it stores the SLICES. It does NOT copy the string data.
    // This is dangerous if passed from C/FFI where strings are temporary.
    // We MUST duplicate them here.

    const ticker_dupe = std.heap.c_allocator.dupe(u8, ticker) catch return null;
    const path_dupe = std.heap.c_allocator.dupe(u8, path) catch return null;

    const db_ptr = std.heap.c_allocator.create(DB) catch return null;
    db_ptr.* = DB.init(ticker_dupe, path_dupe) catch {
        std.heap.c_allocator.free(ticker_dupe);
        std.heap.c_allocator.free(path_dupe);
        std.heap.c_allocator.destroy(db_ptr);
        return null;
    };

    // DB.init doesn't store the strings, so we can free them now
    std.heap.c_allocator.free(ticker_dupe);
    std.heap.c_allocator.free(path_dupe);

    return db_ptr;
}

export fn hocdb_append(db_ptr: *anyopaque, timestamp: i64, usd: f64, volume: f64) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.append(.{ .timestamp = timestamp, .usd = usd, .volume = volume }) catch return -1;
    return 0;
}

export fn hocdb_flush(db_ptr: *anyopaque) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.flush() catch return -1;
    return 0;
}

export fn hocdb_load(db_ptr: *anyopaque, out_len: *usize) ?[*]TradeData {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    // Use C allocator so caller can free with free()
    const data = db.load(std.heap.c_allocator) catch return null;
    out_len.* = data.len;
    return data.ptr;
}

export fn hocdb_close(db_ptr: *anyopaque) void {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    db.deinit();
    std.heap.c_allocator.destroy(db);
}
