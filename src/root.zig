const std = @import("std");

pub fn TimeSeriesDB(comptime T: type) type {
    return struct {
        const Self = @This();

        // Magic header: "HOC1"
        const MAGIC = "HOC1".*; // Dereference to get array
        const HEADER_SIZE = @sizeOf(u32) + @sizeOf(u64);

        fn computeSchemaHash() u64 {
            var hasher = std.hash.Wyhash.init(0);
            const fields = std.meta.fields(T);
            inline for (fields) |field| {
                hasher.update(field.name);
                hasher.update(@typeName(field.type));
            }
            return hasher.final();
        }

        const SCHEMA_HASH = Self.computeSchemaHash();

        // Custom Buffered Writer
        const BUFFER_SIZE = 4096;
        const BufferedWriter = struct {
            file: std.fs.File,
            buffer: [BUFFER_SIZE]u8 = undefined,
            index: usize = 0,

            pub fn init(file: std.fs.File) @This() {
                return .{ .file = file };
            }

            pub fn flush(self: *@This()) !void {
                if (self.index > 0) {
                    try self.file.writeAll(self.buffer[0..self.index]);
                    self.index = 0;
                }
            }

            pub fn write(self: *@This(), bytes: []const u8) !void {
                if (self.index + bytes.len > BUFFER_SIZE) {
                    try self.flush();
                    if (bytes.len > BUFFER_SIZE) {
                        try self.file.writeAll(bytes);
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

        pub fn init(ticker: []const u8, dir_path: []const u8) !Self {
            var dir = try std.fs.cwd().makeOpenPath(dir_path, .{});
            defer dir.close();

            const filename = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.bin", .{ticker});
            defer std.heap.page_allocator.free(filename);

            var file = try dir.createFile(filename, .{
                .read = true,
                .truncate = false, // Don't overwrite existing data
            });
            errdefer file.close();

            // Exclusive lock to prevent multiple writers
            try file.lock(.exclusive);

            const stat = try file.stat();
            var last_timestamp: ?i64 = null;

            if (stat.size == 0) {
                // New file: Write Header
                try file.writeAll(&MAGIC);
                try file.writeAll(std.mem.asBytes(&SCHEMA_HASH));
            } else {
                // Existing file: Validate Header
                if (stat.size < HEADER_SIZE) return error.InvalidFile;

                try file.seekTo(0);
                var header_buf: [HEADER_SIZE]u8 = undefined;
                const bytes_read = try file.readAll(&header_buf);
                if (bytes_read != HEADER_SIZE) return error.UnexpectedEndOfFile;

                if (!std.mem.eql(u8, header_buf[0..4], &MAGIC)) return error.InvalidMagic;
                const file_hash = std.mem.bytesToValue(u64, header_buf[4..12]);
                if (file_hash != SCHEMA_HASH) return error.SchemaMismatch;

                // Read last timestamp if there is data
                if (stat.size > HEADER_SIZE) {
                    const record_size = @sizeOf(T);
                    if ((stat.size - HEADER_SIZE) % record_size != 0) return error.CorruptedData;

                    try file.seekFromEnd(-@as(i64, @intCast(record_size)));
                    var last_record_buf: [@sizeOf(T)]u8 = undefined;
                    _ = try file.readAll(&last_record_buf);
                    const last_record = std.mem.bytesToValue(T, &last_record_buf);
                    last_timestamp = last_record.timestamp;

                    // Seek back to end for appending
                    try file.seekFromEnd(0);
                }
            }

            return Self{
                .file = file,
                .buffered_writer = BufferedWriter.init(file),
                .last_timestamp = last_timestamp,
            };
        }

        pub fn deinit(self: *Self) void {
            self.flush() catch {}; // Try to flush, ignore error on close
            self.file.unlock();
            self.file.close();
        }

        pub fn flush(self: *Self) !void {
            try self.buffered_writer.flush();
        }

        pub fn append(self: *Self, data: T) !void {
            // Monotonicity Check
            if (self.last_timestamp) |last| {
                if (data.timestamp <= last) return error.TimestampNotMonotonic;
            }

            // Write to buffer
            const bytes = std.mem.asBytes(&data);
            try self.buffered_writer.write(bytes);

            self.last_timestamp = data.timestamp;
        }

        pub fn load(self: *Self, allocator: std.mem.Allocator) ![]T {
            try self.file.seekTo(0);
            const stat = try self.file.stat();

            if (stat.size < HEADER_SIZE) return error.InvalidFile;

            const data_size = stat.size - HEADER_SIZE;
            if (data_size % @sizeOf(T) != 0) return error.CorruptedData;

            const count = data_size / @sizeOf(T);
            const result = try allocator.alloc(T, count);
            errdefer allocator.free(result);

            // Skip header
            try self.file.seekTo(HEADER_SIZE);

            const bytes_to_read = count * @sizeOf(T);
            const bytes_slice = std.mem.sliceAsBytes(result);

            const read = try self.file.readAll(bytes_slice);
            if (read != bytes_to_read) return error.UnexpectedEndOfFile;

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
        var db = try DB.init(ticker, dir);
        defer db.deinit();
        try db.append(.{ .timestamp = 100, .value = 1.1 });
        try db.append(.{ .timestamp = 200, .value = 2.2 });
    }

    // Load
    {
        var db = try DB.init(ticker, dir);
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
        try std.testing.expectError(error.SchemaMismatch, WrongDB.init(ticker, dir));
    }

    // Monotonic Timestamp Test
    {
        var db = try DB.init(ticker, dir);
        defer db.deinit();

        // Last timestamp was 200
        try std.testing.expectError(error.TimestampNotMonotonic, db.append(.{ .timestamp = 199, .value = 3.3 }));
        try std.testing.expectError(error.TimestampNotMonotonic, db.append(.{ .timestamp = 200, .value = 3.3 }));

        // Valid append
        try db.append(.{ .timestamp = 201, .value = 3.3 });
    }
}
