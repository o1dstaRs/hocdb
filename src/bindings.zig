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

extern "c" fn napi_define_properties(env: napi_env, object: napi_value, property_count: usize, properties: [*]const napi_property_descriptor) napi_status;
extern "c" fn napi_get_cb_info(env: napi_env, cbinfo: napi_callback_info, argc: *usize, argv: [*]napi_value, this_arg: ?*napi_value, data: ?*?*anyopaque) napi_status;
extern "c" fn napi_create_string_utf8(env: napi_env, str: [*]const u8, length: usize, result: *napi_value) napi_status;
extern "c" fn napi_create_double(env: napi_env, value: f64, result: *napi_value) napi_status;
extern "c" fn napi_create_int64(env: napi_env, value: i64, result: *napi_value) napi_status;
extern "c" fn napi_get_value_double(env: napi_env, value: napi_value, result: *f64) napi_status;
extern "c" fn napi_get_value_int64(env: napi_env, value: napi_value, result: *i64) napi_status;
extern "c" fn napi_get_value_bigint_int64(env: napi_env, value: napi_value, result: *i64, lossless: *bool) napi_status;
extern "c" fn napi_get_value_string_utf8(env: napi_env, value: napi_value, buf: ?[*]u8, bufsize: usize, result: ?*usize) napi_status;
extern "c" fn napi_create_external(env: napi_env, data: *anyopaque, finalize_cb: ?napi_finalize, finalize_hint: ?*anyopaque, result: *napi_value) napi_status;
extern "c" fn napi_get_value_external(env: napi_env, value: napi_value, result: *?*anyopaque) napi_status;
extern "c" fn napi_throw_error(env: napi_env, code: ?[*:0]const u8, msg: [*:0]const u8) napi_status;
extern "c" fn napi_create_external_arraybuffer(env: napi_env, external_data: *anyopaque, byte_length: usize, finalize_cb: ?napi_finalize, finalize_hint: ?*anyopaque, result: *napi_value) napi_status;
extern "c" fn napi_typeof(env: napi_env, value: napi_value, result: *napi_valuetype) napi_status;
extern "c" fn napi_get_named_property(env: napi_env, object: napi_value, utf8name: [*]const u8, result: *napi_value) napi_status;
extern "c" fn napi_get_value_bool(env: napi_env, value: napi_value, result: *bool) napi_status;
extern "c" fn napi_get_element(env: napi_env, object: napi_value, index: u32, result: *napi_value) napi_status;
extern "c" fn napi_get_array_length(env: napi_env, value: napi_value, result: *u32) napi_status;
extern "c" fn napi_get_buffer_info(env: napi_env, value: napi_value, data: *?*anyopaque, length: *usize) napi_status;
extern "c" fn napi_create_object(env: napi_env, result: *napi_value) napi_status;
extern "c" fn napi_set_named_property(env: napi_env, object: napi_value, utf8name: [*]const u8, value: napi_value) napi_status;
extern "c" fn napi_create_bigint_int64(env: napi_env, value: i64, result: *napi_value) napi_status;
extern "c" fn napi_create_bigint_uint64(env: napi_env, value: u64, result: *napi_value) napi_status;

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

// dbInit(ticker: string, path: string, schema: object[], config?: object): external
fn dbInit(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const max_args = 4;
    var argc: usize = max_args;
    var args: [max_args]napi_value = undefined;
    if (napi_get_cb_info(env, info, &argc, &args, null, null) != .ok) {
        return throwError(env, "Failed to parse arguments");
    }

    if (argc < 3) {
        return throwError(env, "Expected at least 3 arguments: ticker, path, schema");
    }

    // Parse Ticker
    var ticker_len: usize = 0;
    if (napi_get_value_string_utf8(env, args[0], null, 0, &ticker_len) != .ok) return throwError(env, "Invalid ticker");
    const ticker = std.heap.c_allocator.alloc(u8, ticker_len + 1) catch return throwError(env, "OOM");
    defer std.heap.c_allocator.free(ticker);
    if (napi_get_value_string_utf8(env, args[0], ticker.ptr, ticker_len + 1, null) != .ok) return throwError(env, "Failed to get ticker");

    // Parse Path
    var path_len: usize = 0;
    if (napi_get_value_string_utf8(env, args[1], null, 0, &path_len) != .ok) return throwError(env, "Invalid path");
    const path = std.heap.c_allocator.alloc(u8, path_len + 1) catch return throwError(env, "OOM");
    defer std.heap.c_allocator.free(path);
    if (napi_get_value_string_utf8(env, args[1], path.ptr, path_len + 1, null) != .ok) return throwError(env, "Failed to get path");

    // Parse Schema
    // Schema is an array of objects: [{name: "ts", type: "i64"}, ...]
    var schema_len: u32 = 0;
    if (napi_get_array_length(env, args[2], &schema_len) != .ok) return throwError(env, "Invalid schema array");

    const fields = std.heap.c_allocator.alloc(hocdb.FieldInfo, schema_len) catch return throwError(env, "OOM");
    // We need to keep the field names alive? No, DynamicTimeSeriesDB copies them?
    // Wait, Schema struct has `[]const FieldInfo`, and FieldInfo has `[]const u8`.
    // DynamicTimeSeriesDB computes hash from them but does NOT copy them for storage.
    // It only uses them during init to compute hash and offsets.
    // So we can free them after init.

    var i: u32 = 0;
    while (i < schema_len) : (i += 1) {
        var element: napi_value = undefined;
        if (napi_get_element(env, args[2], i, &element) != .ok) return throwError(env, "Failed to get schema element");

        // Get name
        var name_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "name", &name_val) != .ok) return throwError(env, "Missing name in schema");
        var name_len: usize = 0;
        if (napi_get_value_string_utf8(env, name_val, null, 0, &name_len) != .ok) return throwError(env, "Invalid name");
        const name = std.heap.c_allocator.alloc(u8, name_len + 1) catch return throwError(env, "OOM");
        if (napi_get_value_string_utf8(env, name_val, name.ptr, name_len + 1, null) != .ok) return throwError(env, "Failed to get name");

        // Get type
        var type_val: napi_value = undefined;
        if (napi_get_named_property(env, element, "type", &type_val) != .ok) return throwError(env, "Missing type in schema");
        var type_str_len: usize = 0;
        if (napi_get_value_string_utf8(env, type_val, null, 0, &type_str_len) != .ok) return throwError(env, "Invalid type");
        const type_str = std.heap.c_allocator.alloc(u8, type_str_len + 1) catch return throwError(env, "OOM");
        defer std.heap.c_allocator.free(type_str);
        if (napi_get_value_string_utf8(env, type_val, type_str.ptr, type_str_len + 1, null) != .ok) return throwError(env, "Failed to get type");

        const f_type: hocdb.FieldType = if (std.mem.eql(u8, type_str[0..type_str_len], "i64")) .i64 else if (std.mem.eql(u8, type_str[0..type_str_len], "f64")) .f64 else if (std.mem.eql(u8, type_str[0..type_str_len], "u64")) .u64 else return throwError(env, "Unsupported type");

        fields[i] = .{ .name = name[0..name_len], .type = f_type };
    }
    defer {
        for (fields) |f| std.heap.c_allocator.free(f.name.ptr[0 .. f.name.len + 1]); // +1 for null terminator we allocated
        std.heap.c_allocator.free(fields);
    }

    // Parse Config
    var config = DB.Config{};
    if (argc >= 4) {
        // ... config parsing logic (same as before) ...
        var type_result: napi_valuetype = undefined;
        if (napi_typeof(env, args[3], &type_result) == .ok and type_result == .object) {
            var max_size_val: napi_value = undefined;
            if (napi_get_named_property(env, args[3], "max_file_size", &max_size_val) == .ok) {
                var val: i64 = 0;
                if (napi_get_value_int64(env, max_size_val, &val) == .ok) config.max_file_size = @intCast(val);
            }
            var overwrite_val: napi_value = undefined;
            if (napi_get_named_property(env, args[3], "overwrite_on_full", &overwrite_val) == .ok) {
                var val: bool = false;
                if (napi_get_value_bool(env, overwrite_val, &val) == .ok) config.overwrite_on_full = val;
            }
            var flush_val: napi_value = undefined;
            if (napi_get_named_property(env, args[3], "flush_on_write", &flush_val) == .ok) {
                var val: bool = false;
                if (napi_get_value_bool(env, flush_val, &val) == .ok) config.flush_on_write = val;
            }
            var auto_inc_val: napi_value = undefined;
            if (napi_get_named_property(env, args[3], "auto_increment", &auto_inc_val) == .ok) {
                var val: bool = false;
                if (napi_get_value_bool(env, auto_inc_val, &val) == .ok) config.auto_increment = val;
            }
        }
    }

    const schema = hocdb.Schema{ .fields = fields };

    const db_ptr = std.heap.c_allocator.create(DB) catch return throwError(env, "Allocation failed");
    db_ptr.* = DB.init(ticker[0..ticker_len], path[0..path_len], std.heap.c_allocator, schema, config) catch |err| {
        std.heap.c_allocator.destroy(db_ptr);
        return throwError(env, @errorName(err));
    };
    db_ptr.initWriter();

    var result: napi_value = undefined;
    _ = napi_create_external(env, db_ptr, null, null, &result);
    return result;
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
        // Try typed array? Or just assume buffer?
        // Node.js Buffer is Uint8Array.
        // napi_get_buffer_info works for Buffer.
        return throwError(env, "Invalid data buffer");
    }

    const data = @as([*]const u8, @ptrCast(data_ptr.?))[0..data_len];

    db.append(data) catch |err| {
        return throwError(env, @errorName(err));
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
    const byte_length = data.len;

    // We pass 'data.ptr' as the data.
    // We pass 'freeData' as finalizer.
    // We don't need a hint if we use std.c.free.

    _ = napi_create_external_arraybuffer(env, data.ptr, byte_length, freeData, null, &result);
    return result;
}

// dbQuery(db: external, start: i64, end: i64): ArrayBuffer
fn dbQuery(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
    const args = getArgs(env, info, 3) catch return throwError(env, "Expected 3 arguments");

    var db_ptr: ?*anyopaque = null;
    _ = napi_get_value_external(env, args[0], &db_ptr);
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr.?)));

    var start: i64 = 0;
    var lossless: bool = true;
    if (napi_get_value_bigint_int64(env, args[1], &start, &lossless) != .ok) {
        // Fallback to Number?
        if (napi_get_value_int64(env, args[1], &start) != .ok) return throwError(env, "Invalid start timestamp (expected BigInt or Number)");
    }

    var end: i64 = 0;
    if (napi_get_value_bigint_int64(env, args[2], &end, &lossless) != .ok) {
        if (napi_get_value_int64(env, args[2], &end) != .ok) return throwError(env, "Invalid end timestamp (expected BigInt or Number)");
    }

    // Use C allocator so we can free it with std.c.free in finalizer
    const allocator = std.heap.c_allocator;

    db.flush() catch |err| {
        return throwError(env, @errorName(err));
    };

    const data = db.query(start, end, &[_]hocdb.Filter{}, allocator) catch |err| {
        return throwError(env, @errorName(err));
    };

    // Create External ArrayBuffer
    var result: napi_value = undefined;
    const byte_length = data.len;

    _ = napi_create_external_arraybuffer(env, data.ptr, byte_length, freeData, null, &result);
    return result;
}

// dbGetStats(db: external, start: i64, end: i64, field_index: u32): Object
fn dbGetStats(env: napi_env, info: napi_callback_info) callconv(.c) napi_value {
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

    var field_index: i64 = 0;
    if (napi_get_value_int64(env, args[3], &field_index) != .ok) return throwError(env, "Invalid field index");

    const stats = db.getStats(start, end, @intCast(field_index)) catch |err| {
        return throwError(env, @errorName(err));
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

    return result;
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
        return throwError(env, @errorName(err));
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

// --- Module Registration ---

export fn napi_register_module_v1(env: napi_env, exports: napi_value) napi_value {
    const descriptors = [_]napi_property_descriptor{
        .{ .utf8name = "dbInit", .name = null, .method = dbInit, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbAppend", .name = null, .method = dbAppend, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbLoad", .name = null, .method = dbLoad, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbQuery", .name = null, .method = dbQuery, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbGetStats", .name = null, .method = dbGetStats, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbGetLatest", .name = null, .method = dbGetLatest, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
        .{ .utf8name = "dbClose", .name = null, .method = dbClose, .getter = null, .setter = null, .value = null, .attributes = .default, .data = null },
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
    db_ptr.initWriter();

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

export fn hocdb_get_stats(db_ptr: *anyopaque, start_ts: i64, end_ts: i64, field_index: usize, out_stats: *hocdb.Stats) c_int {
    const db = @as(*DB, @ptrCast(@alignCast(db_ptr)));
    const stats = db.getStats(start_ts, end_ts, field_index) catch return -1;
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

export fn hocdb_free(ptr: ?*anyopaque) void {
    if (ptr) |p| {
        std.c.free(p);
    }
}
