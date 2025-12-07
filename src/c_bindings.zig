const std = @import("std");
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
    db.append(data_ptr[0..data_len]) catch |err| {
        if (err == error.InvalidRecordSize) return -2;
        if (err == error.TimestampNotMonotonic) return -3;
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
    return data.ptr;
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
