const std = @import("std");

pub const FieldType = enum(u8) {
    i64 = 1,
    f64 = 2,
    u64 = 3,
    u8 = 4,
    // Add more as needed

    pub fn size(self: FieldType) usize {
        return switch (self) {
            .i64, .f64, .u64 => 8,
            .u8 => 1,
        };
    }
};

pub const FieldInfo = struct {
    name: []const u8,
    type: FieldType,
};

pub const Schema = struct {
    fields: []const FieldInfo,

    pub fn computeHash(self: Schema) u64 {
        var hasher = std.hash.Wyhash.init(0);
        for (self.fields) |field| {
            hasher.update(field.name);
            hasher.update(@tagName(field.type));
        }
        return hasher.final();
    }

    pub fn recordSize(self: Schema) usize {
        var s: usize = 0;
        for (self.fields) |field| {
            s += field.type.size();
        }
        return s;
    }

    pub fn timestampOffset(self: Schema) ?usize {
        var offset: usize = 0;
        for (self.fields) |field| {
            if (std.mem.eql(u8, field.name, "timestamp")) {
                if (field.type == .i64) return offset;
                // We only support i64 timestamp for now for simplicity in monotonicity check
                return null;
            }
            offset += field.type.size();
        }
        return null;
    }
};

pub const DynamicTimeSeriesDB = struct {
    const Self = @This();

    pub const Config = struct {
        max_file_size: u64 = 2 * 1024 * 1024 * 1024, // 2 GiB default
        overwrite_on_full: bool = true,
        flush_on_write: bool = false,
    };

    // Magic header: "HOC1"
    const MAGIC = "HOC1".*;
    const HEADER_SIZE = @sizeOf(u32) + @sizeOf(u64);

    // Custom Buffered Writer
    const BUFFER_SIZE = 4096;
    const BufferedWriter = struct {
        file: std.fs.File,
        buffer: [BUFFER_SIZE]u8 = undefined,
        index: usize = 0,
        max_file_size: u64,
        write_cursor: *u64,
        overwrite_on_full: bool,
        is_wrapped: *bool,

        pub fn init(file: std.fs.File, max_size: u64, cursor: *u64, overwrite: bool, wrapped: *bool) @This() {
            return .{
                .file = file,
                .max_file_size = max_size,
                .write_cursor = cursor,
                .overwrite_on_full = overwrite,
                .is_wrapped = wrapped,
            };
        }

        pub fn flush(self: *@This()) !void {
            if (self.index > 0) {
                try self.writeRaw(self.buffer[0..self.index]);
                self.index = 0;
            }
        }

        fn writeRaw(self: *@This(), bytes: []const u8) !void {
            var remaining = bytes;
            while (remaining.len > 0) {
                const space_left = self.max_file_size - self.write_cursor.*;
                const chunk_size = @min(remaining.len, space_left);

                if (chunk_size == 0) {
                    // We are at the end of the file
                    if (!self.overwrite_on_full) return error.DiskFull;
                    // Wrap around
                    self.write_cursor.* = HEADER_SIZE;
                    self.is_wrapped.* = true;
                    try self.file.seekTo(HEADER_SIZE);
                    continue;
                }

                try self.file.writeAll(remaining[0..chunk_size]);
                self.write_cursor.* += chunk_size;
                remaining = remaining[chunk_size..];
            }
        }

        pub fn write(self: *@This(), bytes: []const u8) !void {
            if (self.index + bytes.len > BUFFER_SIZE) {
                try self.flush();
                if (bytes.len > BUFFER_SIZE) {
                    try self.writeRaw(bytes);
                    return;
                }
            }
            @memcpy(self.buffer[self.index .. self.index + bytes.len], bytes);
            self.index += bytes.len;
        }
    };

    file: std.fs.File,
    buffered_writer: BufferedWriter,
    last_timestamp: ?i64 = null,
    max_file_size: u64,
    overwrite_on_full: bool,
    flush_on_write: bool,
    write_cursor: u64,
    is_wrapped: bool = false,

    // Schema info
    record_size: usize,
    timestamp_offset: usize,
    schema_hash: u64,

    allocator: std.mem.Allocator,

    pub fn init(ticker: []const u8, dir_path: []const u8, allocator: std.mem.Allocator, schema: Schema, config: Config) !Self {
        const record_size = schema.recordSize();
        if (record_size == 0) return error.InvalidSchema;

        const ts_offset = schema.timestampOffset() orelse return error.MissingTimestampField;
        const schema_hash = schema.computeHash();

        if (config.max_file_size < HEADER_SIZE + record_size) return error.MaxFileSizeTooSmall;

        // Align max_file_size to record size
        const data_capacity = config.max_file_size - HEADER_SIZE;
        const aligned_capacity = (data_capacity / record_size) * record_size;
        const effective_max_size = HEADER_SIZE + aligned_capacity;

        var dir = try std.fs.cwd().makeOpenPath(dir_path, .{});
        defer dir.close();

        const filename = try std.fmt.allocPrint(allocator, "{s}.bin", .{ticker});
        defer allocator.free(filename);

        var file = try dir.createFile(filename, .{
            .read = true,
            .truncate = false, // Don't overwrite existing data
        });
        errdefer file.close();

        // Exclusive lock
        try file.lock(.exclusive);

        const stat = try file.stat();
        var last_timestamp: ?i64 = null;
        var write_cursor: u64 = HEADER_SIZE;
        var is_wrapped = false;

        if (stat.size == 0) {
            // New file: Write Header
            try file.writeAll(&MAGIC);
            try file.writeAll(std.mem.asBytes(&schema_hash));
        } else {
            // Existing file: Validate Header
            if (stat.size < HEADER_SIZE) return error.InvalidFile;

            try file.seekTo(0);
            var header_buf: [HEADER_SIZE]u8 = undefined;
            const bytes_read = try file.readAll(&header_buf);
            if (bytes_read != HEADER_SIZE) return error.UnexpectedEndOfFile;

            if (!std.mem.eql(u8, header_buf[0..4], &MAGIC)) return error.InvalidMagic;
            const file_hash = std.mem.bytesToValue(u64, header_buf[4..12]);
            if (file_hash != schema_hash) return error.SchemaMismatch;

            // Recovery logic
            if (stat.size < effective_max_size) {
                // Linear append mode
                if ((stat.size - HEADER_SIZE) % record_size != 0) return error.CorruptedData;

                if (stat.size > HEADER_SIZE) {
                    try file.seekFromEnd(-@as(i64, @intCast(record_size)));
                    // We need to read just the timestamp, but reading whole record is easier
                    const last_record = try allocator.alloc(u8, record_size);
                    defer allocator.free(last_record);

                    _ = try file.readAll(last_record);
                    last_timestamp = std.mem.bytesToValue(i64, last_record[ts_offset .. ts_offset + 8]);
                }
                write_cursor = stat.size;
            } else {
                // Ring buffer / Full file
                // Simple linear scan for now
                try file.seekTo(HEADER_SIZE);
                write_cursor = stat.size; // Default to end if we don't scan
                if (stat.size >= effective_max_size) {
                    is_wrapped = true;
                }
                // TODO: Implement scan
            }
        }

        // Seek to write cursor
        try file.seekTo(write_cursor);

        return Self{
            .file = file,
            .buffered_writer = undefined, // Init below
            .last_timestamp = last_timestamp,
            .max_file_size = effective_max_size,
            .overwrite_on_full = config.overwrite_on_full,
            .flush_on_write = config.flush_on_write,
            .write_cursor = write_cursor,
            .record_size = record_size,
            .timestamp_offset = ts_offset,
            .schema_hash = schema_hash,
            .allocator = allocator,
            .is_wrapped = is_wrapped,
        };
    }

    // Post-init to set up self-referencing buffered writer
    pub fn initWriter(self: *Self) void {
        self.buffered_writer = BufferedWriter.init(self.file, self.max_file_size, &self.write_cursor, self.overwrite_on_full, &self.is_wrapped);
    }

    pub fn deinit(self: *Self) void {
        self.flush() catch {};
        self.file.unlock();
        self.file.close();
    }

    pub fn flush(self: *Self) !void {
        try self.buffered_writer.flush();
    }

    pub fn append(self: *Self, data: []const u8) !void {
        if (data.len != self.record_size) return error.InvalidRecordSize;

        // Monotonicity Check
        const ts = std.mem.bytesToValue(i64, data[self.timestamp_offset .. self.timestamp_offset + 8]);
        if (self.last_timestamp) |last| {
            if (ts <= last) return error.TimestampNotMonotonic;
        }

        try self.buffered_writer.write(data);
        self.last_timestamp = ts;

        if (self.flush_on_write) {
            try self.flush();
        }
    }

    pub fn count(self: *Self) u64 {
        if (self.is_wrapped) {
            return (self.max_file_size - HEADER_SIZE) / self.record_size;
        } else {
            return (self.write_cursor - HEADER_SIZE) / self.record_size;
        }
    }

    fn getPhysicalOffset(self: *Self, index: u64) u64 {
        if (self.is_wrapped) {
            const data_capacity = self.max_file_size - HEADER_SIZE;
            const relative_offset = (self.write_cursor - HEADER_SIZE + index * self.record_size) % data_capacity;
            return HEADER_SIZE + relative_offset;
        } else {
            return HEADER_SIZE + index * self.record_size;
        }
    }

    fn readTimestampAt(self: *Self, index: u64) !i64 {
        const offset = self.getPhysicalOffset(index);
        var buf: [8]u8 = undefined;
        const len = try self.file.preadAll(&buf, offset + self.timestamp_offset);
        if (len != 8) return error.UnexpectedEndOfFile;
        return std.mem.bytesToValue(i64, &buf);
    }

    pub fn binarySearch(self: *Self, target: i64) !u64 {
        var left: u64 = 0;
        var right: u64 = self.count();

        while (left < right) {
            const mid = left + (right - left) / 2;
            const ts = try self.readTimestampAt(mid);
            if (ts < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    pub fn query(self: *Self, start_ts: i64, end_ts: i64, allocator: std.mem.Allocator) ![]u8 {
        try self.flush();

        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);

        if (start_idx >= end_idx) {
            return allocator.alloc(u8, 0);
        }

        const record_count = end_idx - start_idx;
        const result_size = record_count * self.record_size;
        const result = try allocator.alloc(u8, result_size);
        errdefer allocator.free(result);

        var dest_offset: usize = 0;
        var current_idx = start_idx;

        while (current_idx < end_idx) {
            const physical_offset = self.getPhysicalOffset(current_idx);

            var chunk_count: u64 = 0;
            if (self.is_wrapped) {
                const data_capacity = self.max_file_size - HEADER_SIZE;
                const offset_in_data = physical_offset - HEADER_SIZE;
                const records_until_end = (data_capacity - offset_in_data) / self.record_size;
                chunk_count = @min(end_idx - current_idx, records_until_end);
            } else {
                chunk_count = end_idx - current_idx;
            }

            const chunk_size = chunk_count * self.record_size;
            const len = try self.file.preadAll(result[dest_offset .. dest_offset + chunk_size], physical_offset);
            if (len != chunk_size) return error.UnexpectedEndOfFile;

            dest_offset += chunk_size;
            current_idx += chunk_count;
        }

        return result;
    }

    pub fn load(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        try self.file.seekTo(0);
        const stat = try self.file.stat();

        if (stat.size < HEADER_SIZE) return error.InvalidFile;

        const data_size = stat.size - HEADER_SIZE;
        if (data_size % self.record_size != 0) return error.CorruptedData;

        const result = try allocator.alloc(u8, data_size);
        errdefer allocator.free(result);

        // Skip header
        try self.file.seekTo(HEADER_SIZE);
        const read = try self.file.readAll(result);
        if (read != data_size) return error.UnexpectedEndOfFile;

        return result;
    }
};

pub fn TimeSeriesDB(comptime T: type) type {
    return struct {
        const Self = @This();

        dynamic_db: *DynamicTimeSeriesDB, // Now holds a pointer
        allocator: std.mem.Allocator, // To free the dynamic_db

        pub const Config = DynamicTimeSeriesDB.Config;

        pub fn init(ticker: []const u8, dir_path: []const u8, allocator: std.mem.Allocator, config: Config) !Self {
            // Generate schema from T
            const fields = std.meta.fields(T);
            var field_infos_storage: [fields.len]FieldInfo = undefined;
            inline for (fields, 0..) |field, i| {
                const f_type = switch (field.type) {
                    i64 => FieldType.i64,
                    f64 => FieldType.f64,
                    u64 => FieldType.u64,
                    u8 => FieldType.u8,
                    else => @compileError("Unsupported field type"),
                };
                field_infos_storage[i] = .{ .name = field.name, .type = f_type };
            }

            const schema = Schema{ .fields = &field_infos_storage };

            var dynamic_db_ptr = try allocator.create(DynamicTimeSeriesDB);
            errdefer allocator.destroy(dynamic_db_ptr);
            dynamic_db_ptr.* = try DynamicTimeSeriesDB.init(ticker, dir_path, allocator, schema, config);
            dynamic_db_ptr.initWriter();

            return Self{ .dynamic_db = dynamic_db_ptr, .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            self.dynamic_db.deinit();
            self.allocator.destroy(self.dynamic_db);
        }

        pub fn flush(self: *Self) !void {
            try self.dynamic_db.flush();
        }

        pub fn append(self: *Self, data: T) !void {
            const bytes = std.mem.asBytes(&data);
            try self.dynamic_db.append(bytes);
        }

        pub fn query(self: *Self, start_ts: i64, end_ts: i64, allocator: std.mem.Allocator) ![]T {
            const raw_bytes = try self.dynamic_db.query(start_ts, end_ts, allocator);
            errdefer allocator.free(raw_bytes);

            const record_size = self.dynamic_db.record_size;
            if (raw_bytes.len % record_size != 0) return error.CorruptedData;

            const count = raw_bytes.len / record_size;
            const result = try allocator.alloc(T, count);

            var i: usize = 0;
            while (i < count) : (i += 1) {
                const record_bytes = raw_bytes[i * record_size .. (i + 1) * record_size];
                result[i] = std.mem.bytesToValue(T, record_bytes);
            }

            allocator.free(raw_bytes);
            return result;
        }

        pub fn load(self: *Self, allocator: std.mem.Allocator) ![]T {
            const raw_bytes = try self.dynamic_db.load(allocator);
            errdefer allocator.free(raw_bytes);

            const record_size = self.dynamic_db.record_size;
            if (raw_bytes.len % record_size != 0) return error.CorruptedData;

            const count = raw_bytes.len / record_size;
            const result = try allocator.alloc(T, count);

            var i: usize = 0;
            while (i < count) : (i += 1) {
                const record_bytes = raw_bytes[i * record_size .. (i + 1) * record_size];
                result[i] = std.mem.bytesToValue(T, record_bytes);
            }

            allocator.free(raw_bytes); // Free the intermediate raw_bytes
            return result;
        }
    };
}

test "TimeSeriesDB generic usage" {
    const TestStruct = struct {
        timestamp: i64,
        value: f64,
    };

    const ticker = "TEST_TICKER";
    const dir = "test_data";

    // Cleanup
    std.fs.cwd().deleteTree(dir) catch {};
    defer std.fs.cwd().deleteTree(dir) catch {};

    const DB = TimeSeriesDB(TestStruct);

    // Write
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{});
        defer db.deinit();
        try db.append(.{ .timestamp = 100, .value = 1.1 });
        try db.append(.{ .timestamp = 200, .value = 2.2 });
    }

    // Load
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{});
        defer db.deinit();
        const data = try db.load(std.testing.allocator);
        defer std.testing.allocator.free(data);

        try std.testing.expectEqual(2, data.len);
        try std.testing.expectEqual(100, data[0].timestamp);
        try std.testing.expectEqual(2.2, data[1].value);
    }

    // Schema Mismatch Test
    {
        const WrongStruct = struct {
            timestamp: i64,
            value: f64,
            extra: u8,
        };
        const WrongDB = TimeSeriesDB(WrongStruct);
        try std.testing.expectError(error.SchemaMismatch, WrongDB.init(ticker, dir, std.testing.allocator, .{}));
    }

    // Monotonic Timestamp Test
    {
        var db = try DB.init(ticker, dir, std.testing.allocator, .{});
        defer db.deinit();

        // Last timestamp was 200
        try std.testing.expectError(error.TimestampNotMonotonic, db.append(.{ .timestamp = 199, .value = 3.3 }));
        try std.testing.expectError(error.TimestampNotMonotonic, db.append(.{ .timestamp = 200, .value = 3.3 }));

        // Valid append
        try db.append(.{ .timestamp = 201, .value = 3.3 });
    }
}
