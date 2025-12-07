const std = @import("std");

pub const FieldType = enum(u8) {
    i64 = 1,
    f64 = 2,
    u64 = 3,
    u8 = 4,
    string = 5, // Fixed 128-byte string
    bool = 6,

    pub fn size(self: FieldType) usize {
        return switch (self) {
            .i64, .f64, .u64 => 8,
            .u8, .bool => 1,
            .string => 128,
        };
    }
};

pub const FieldInfo = struct {
    name: []const u8,
    type: FieldType,
};

pub const Stats = extern struct {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
    mean: f64,
};

pub const Filter = struct {
    field_index: usize,
    value: union(enum) {
        i64: i64,
        f64: f64,
        u64: u64,
        string: [128]u8,
        bool: bool,
    },
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
        auto_increment: bool = false,
    };

    // Magic header: "HOC1"
    const MAGIC = "HOC1".*;
    const HEADER_SIZE = @sizeOf(u32) + @sizeOf(u64);

    // Custom Buffered Writer
    const BLOCK_SIZE = 4096;
    const BufferedWriter = struct {
        file: std.fs.File,
        buffer: [BLOCK_SIZE]u8 = undefined,
        index: usize = 0,
        max_file_size: u64,
        write_cursor: *u64,
        overwrite_on_full: bool,
        is_wrapped: *bool,
        record_size: usize,

        pub fn init(file: std.fs.File, max_size: u64, cursor: *u64, overwrite: bool, wrapped: *bool, rec_size: usize) @This() {
            return .{
                .file = file,
                .max_file_size = max_size,
                .write_cursor = cursor,
                .overwrite_on_full = overwrite,
                .is_wrapped = wrapped,
                .record_size = rec_size,
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
            if (self.index + bytes.len > BLOCK_SIZE) {
                try self.flush();
                if (bytes.len > BLOCK_SIZE) {
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
    auto_increment: bool,
    write_cursor: u64,
    is_wrapped: bool = false,

    // Schema info
    record_size: usize,
    timestamp_offset: usize,
    schema_hash: u64,
    fields: []FieldInfo, // Store schema fields
    full_path: []const u8,

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

        const full_path = try std.fs.path.join(allocator, &[_][]const u8{ dir_path, filename });
        errdefer allocator.free(full_path);

        var file: std.fs.File = undefined;
        var retry_count: usize = 0;
        while (retry_count < 3) : (retry_count += 1) {
            if (std.fs.cwd().openFile(full_path, .{ .mode = .read_write })) |f| {
                file = f;
            } else |err| {
                if (err == error.FileNotFound) {
                    file = try std.fs.cwd().createFile(full_path, .{
                        .read = true,
                        .truncate = false,
                    });
                } else {
                    return err;
                }
            }
            break;
        } else {
            return error.FileNotFound; // Failed after retries
        }
        errdefer file.close();

        // Exclusive lock
        try file.lock(.exclusive);

        try file.sync();
        const stat = try file.stat();
        // std.debug.print("DEBUG: load stat.size={d}\n", .{stat.size});
        // const data_size = stat.size - HEADER_SIZE;
        var last_timestamp: ?i64 = null;
        var write_cursor: u64 = HEADER_SIZE;
        var is_wrapped = false;

        const readAll = struct {
            fn readAll(f: std.fs.File, dest: []u8) !usize {
                var index: usize = 0;
                while (index < dest.len) {
                    const amt = try f.read(dest[index..]);
                    if (amt == 0) return index;
                    index += amt;
                }
                return index;
            }
        }.readAll;

        if (stat.size == 0) {
            // New file: Write Header
            try file.writeAll(&MAGIC);
            try file.writeAll(std.mem.asBytes(&schema_hash));
        } else {
            // Existing file: Validate Header
            if (stat.size < HEADER_SIZE) return error.InvalidFile;

            try file.seekTo(0);
            var header_buf: [HEADER_SIZE]u8 = undefined;
            const bytes_read = try readAll(file, &header_buf);
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

                    _ = try readAll(file, last_record);
                    last_timestamp = std.mem.bytesToValue(i64, last_record[ts_offset .. ts_offset + 8]);
                }
                write_cursor = stat.size;
            } else {
                // Ring buffer / Full file
                is_wrapped = true;
                // Scan to find the latest timestamp and write cursor
                var max_ts: i64 = std.math.minInt(i64);
                var max_ts_index: u64 = 0;
                var found_wrap: bool = false;

                const total_records = (stat.size - HEADER_SIZE) / record_size;
                var current_ts: i64 = 0;
                var prev_ts: i64 = std.math.minInt(i64);

                // Use a buffer to read multiple timestamps at once for performance
                const records_per_batch = 128; // Adjust as needed
                const batch_size = records_per_batch * record_size;
                var batch_buf = try allocator.alloc(u8, batch_size);
                defer allocator.free(batch_buf);

                var rec_idx: u64 = 0;
                while (rec_idx < total_records) {
                    const records_to_read = @min(records_per_batch, total_records - rec_idx);
                    const read_size = records_to_read * record_size;

                    try file.seekTo(HEADER_SIZE + rec_idx * record_size);
                    const bytes_read_batch = try readAll(file, batch_buf[0..read_size]);
                    if (bytes_read_batch != read_size) return error.UnexpectedEndOfFile;

                    for (0..records_to_read) |i| {
                        const offset = i * record_size + ts_offset;
                        current_ts = std.mem.bytesToValue(i64, batch_buf[offset .. offset + 8]);

                        if ((rec_idx > 0 or i > 0) and current_ts < prev_ts) {
                            // Found the wrap point!
                            // prev_ts was the maximum (latest)
                            max_ts = prev_ts;
                            max_ts_index = rec_idx + i - 1;
                            found_wrap = true;
                            // We can stop scanning if we assume strict monotonicity within the runs
                            // But to be safe against corruption, maybe scan all?
                            // For now, let's assume valid ring buffer structure and break.
                            break;
                        }
                        prev_ts = current_ts;

                        // Keep track of the last one seen if we don't find a wrap
                        if (i == records_to_read - 1) {
                            max_ts = current_ts;
                            max_ts_index = rec_idx + i;
                        }
                    }
                    if (found_wrap) break;
                    rec_idx += records_to_read;
                }

                if (found_wrap) {
                    // write_cursor should be at the record AFTER the one with max_ts
                    write_cursor = HEADER_SIZE + (max_ts_index + 1) * record_size;
                    last_timestamp = max_ts;
                } else {
                    // Monotonic throughout.
                    // This implies we just filled the file and haven't wrapped yet, OR we wrapped perfectly to the end?
                    // If the file is full (>= effective_max_size) and monotonic, it means the latest record is at the end.
                    // The NEXT write should be at the beginning.
                    write_cursor = HEADER_SIZE;
                    last_timestamp = max_ts;
                }

                // Sanity check
                if (write_cursor >= stat.size + record_size) {
                    // Should not happen if logic is correct
                    write_cursor = HEADER_SIZE;
                }
                // If write_cursor is exactly at EOF, and we are in ring mode, it should wrap to HEADER_SIZE
                if (write_cursor >= stat.size) {
                    write_cursor = HEADER_SIZE;
                }
            }
        }

        // Initialize last_timestamp if auto_increment is enabled
        if (config.auto_increment) {
            if (stat.size > HEADER_SIZE) {
                var last_rec_pos: u64 = 0;
                // write_cursor points to the NEXT write position.
                // If we just started, write_cursor is at the end of valid data (linear) or wherever (ring).
                // If linear and write_cursor > HEADER_SIZE, last record is at write_cursor - record_size.
                if (write_cursor > HEADER_SIZE) {
                    last_rec_pos = write_cursor - record_size;
                } else if (is_wrapped) {
                    // If wrapped and write_cursor is at HEADER_SIZE (or start), last record is at end of file.
                    last_rec_pos = effective_max_size - record_size;
                }

                if (last_rec_pos >= HEADER_SIZE) {
                    var buf: [8]u8 = undefined;
                    try file.seekTo(last_rec_pos + ts_offset);
                    const amt = try readAll(file, &buf);
                    if (amt == 8) {
                        last_timestamp = std.mem.bytesToValue(i64, &buf);
                    }
                }
            }
            if (last_timestamp == null) last_timestamp = 0;
        }

        // Seek to write cursor
        try file.seekTo(write_cursor);

        // Copy fields
        const fields_copy = try allocator.alloc(FieldInfo, schema.fields.len);
        errdefer allocator.free(fields_copy);
        for (schema.fields, 0..) |f, i| {
            const name_copy = try allocator.dupe(u8, f.name);
            errdefer {
                for (0..i) |j| allocator.free(fields_copy[j].name);
                allocator.free(fields_copy);
            }
            fields_copy[i] = .{ .name = name_copy, .type = f.type };
        }

        return Self{
            .file = file,
            .buffered_writer = undefined, // Init below
            .last_timestamp = last_timestamp,
            .max_file_size = effective_max_size,
            .overwrite_on_full = config.overwrite_on_full,
            .flush_on_write = config.flush_on_write,
            .auto_increment = config.auto_increment,
            .write_cursor = write_cursor,
            .record_size = record_size,
            .timestamp_offset = ts_offset,
            .schema_hash = schema_hash,
            .fields = fields_copy,
            .allocator = allocator,
            .is_wrapped = is_wrapped,
            .full_path = full_path,
        };
    }

    // Post-init to set up self-referencing buffered writer
    pub fn initWriter(self: *Self) void {
        self.buffered_writer = BufferedWriter.init(self.file, self.max_file_size, &self.write_cursor, self.overwrite_on_full, &self.is_wrapped, self.record_size);
    }

    pub fn deinit(self: *Self) void {
        self.flush() catch |err| {
            std.debug.print("ERROR: Failed to flush in deinit: {}\n", .{err});
        };
        self.file.sync() catch {};
        self.file.unlock();
        self.file.close();
        for (self.fields) |f| self.allocator.free(f.name);
        self.allocator.free(self.fields);
        self.allocator.free(self.full_path);
    }

    pub fn drop(self: *Self) !void {
        // Close the file first
        self.file.unlock();
        self.file.close();

        // Delete the file
        try std.fs.cwd().deleteFile(self.full_path);

        // Free resources (similar to deinit but we don't close file again)
        for (self.fields) |f| self.allocator.free(f.name);
        self.allocator.free(self.fields);
        self.allocator.free(self.full_path);

        // We should probably mark self as invalid or something, but the caller should destroy the struct.
        // The caller (C binding) will call allocator.destroy(db).
    }

    pub fn flush(self: *Self) !void {
        try self.buffered_writer.flush();
    }

    pub fn append(self: *Self, data: []const u8) !void {
        if (data.len != self.record_size) return error.InvalidRecordSize;

        if (self.auto_increment) {
            // Increment timestamp
            const new_ts = (self.last_timestamp orelse 0) + 1;
            self.last_timestamp = new_ts;

            // Overwrite timestamp in data
            // We need a mutable copy of data
            var mut_data = try self.allocator.alloc(u8, data.len);
            defer self.allocator.free(mut_data);
            @memcpy(mut_data, data);

            const ts_bytes = std.mem.asBytes(&new_ts);
            @memcpy(mut_data[self.timestamp_offset .. self.timestamp_offset + 8], ts_bytes);

            try self.buffered_writer.write(mut_data);
        } else {
            // Monotonicity Check
            const ts = std.mem.bytesToValue(i64, data[self.timestamp_offset .. self.timestamp_offset + 8]);
            if (self.last_timestamp) |last| {
                if (ts <= last) return error.TimestampNotMonotonic;
            }
            self.last_timestamp = ts;

            try self.buffered_writer.write(data);
        }

        if (self.flush_on_write) {
            try self.flush();
        }
    }

    pub fn count(self: *Self) u64 {
        if (self.is_wrapped) {
            return self.countRecordsFromOffset(self.max_file_size);
        } else {
            return self.countRecordsFromOffset(self.write_cursor);
        }
    }

    fn countRecordsFromOffset(self: *Self, offset: u64) u64 {
        if (offset <= HEADER_SIZE) return 0;
        return (offset - HEADER_SIZE) / self.record_size;
    }

    fn getPhysicalOffsetLinear(self: *Self, index: u64) u64 {
        return HEADER_SIZE + index * self.record_size;
    }

    fn getPhysicalOffset(self: *Self, index: u64) u64 {
        if (self.is_wrapped) {
            const capacity = self.count(); // Total records in buffer
            const start_rec_index = self.countRecordsFromOffset(self.write_cursor);
            const target_rec_index = (start_rec_index + index) % capacity;
            return self.getPhysicalOffsetLinear(target_rec_index);
        } else {
            return self.getPhysicalOffsetLinear(index);
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

    pub fn query(self: *Self, start_ts: i64, end_ts: i64, filters: []const Filter, allocator: std.mem.Allocator) ![]u8 {
        try self.flush();

        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);

        if (start_idx >= end_idx) {
            return allocator.alloc(u8, 0);
        }

        var result_list = std.ArrayListUnmanaged(u8){};
        defer result_list.deinit(allocator);

        // Optimization: Pre-allocate
        if (filters.len == 0) {
            try result_list.ensureTotalCapacity(allocator, (end_idx - start_idx) * self.record_size);
        }

        var current_idx = start_idx;
        var record_buf: [4096]u8 = undefined;
        // Ensure record_buf is large enough for at least one record

        while (current_idx < end_idx) {
            // Determine how many contiguous records we can read
            var contiguous_count: u64 = 0;
            var physical_offset: u64 = 0;

            if (self.is_wrapped) {
                const capacity = self.count();
                const start_rec_index = self.countRecordsFromOffset(self.write_cursor);
                // const target_rec_index = (start_rec_index + current_idx) % capacity; // Unused

                // Let's rely on getPhysicalOffset to get the start.
                physical_offset = self.getPhysicalOffset(current_idx);

                // If physical_offset is >= write_cursor, we are in the "old" segment (upper part of file).
                // We can read until max_file_size.
                // If physical_offset < write_cursor, we are in the "new" segment (lower part of file).
                // We can read until write_cursor.

                // Actually, simpler: just read until end of file, then wrap manually if needed.
                // But we are iterating by index.
                // Let's just read one record at a time if wrapped, or try to optimize.

                // Optimization: Read contiguous chunk
                const records_until_wrap = capacity - ((start_rec_index + current_idx) % capacity);
                // Also need to check physical bounds.
                // If we are at offset X, we can read until max_file_size.
                const bytes_until_eof = self.max_file_size - physical_offset;
                const records_physically_contiguous = bytes_until_eof / self.record_size;

                contiguous_count = @min(end_idx - current_idx, records_until_wrap);
                contiguous_count = @min(contiguous_count, records_physically_contiguous);
            } else {
                // Linear mode: everything is contiguous
                physical_offset = self.getPhysicalOffset(current_idx);
                contiguous_count = end_idx - current_idx;
            }

            // Limit chunk size to avoid huge allocations or buffer issues
            const MAX_CHUNK_RECORDS = 1024;
            contiguous_count = @min(contiguous_count, MAX_CHUNK_RECORDS);

            if (contiguous_count == 0) break; // Should not happen

            const read_size = contiguous_count * self.record_size;

            // If no filters, read directly into result
            if (filters.len == 0) {
                const old_len = result_list.items.len;
                try result_list.ensureUnusedCapacity(allocator, read_size);
                result_list.items.len += read_size;
                const dest_slice = result_list.items[old_len..][0..read_size];
                const len = try self.file.preadAll(dest_slice, physical_offset);
                if (len != read_size) {
                    return error.UnexpectedEndOfFile;
                }
                current_idx += contiguous_count;
                continue;
            }

            // If filters, read into temporary buffer and filter
            // We reuse result_list as temp buffer? No.
            // We need a buffer.
            const temp_buf = try allocator.alloc(u8, read_size);
            defer allocator.free(temp_buf);

            const len = try self.file.preadAll(temp_buf, physical_offset);
            if (len != read_size) return error.UnexpectedEndOfFile;

            var i: usize = 0;
            while (i < contiguous_count) : (i += 1) {
                const rec_start = i * self.record_size;
                const record_slice = temp_buf[rec_start .. rec_start + self.record_size];

                // Filter logic
                @memcpy(record_buf[0..self.record_size], record_slice);
                var matches = true;
                for (filters) |filter| {
                    const field_offset = try self.getFieldOffset(filter.field_index);
                    const field_type = self.fields[filter.field_index].type;
                    const val_ptr = record_buf[field_offset..];

                    switch (filter.value) {
                        .i64 => |v| {
                            if (field_type != .i64) return error.TypeMismatch;
                            const val = std.mem.bytesToValue(i64, val_ptr[0..8]);
                            if (val != v) matches = false;
                        },
                        .f64 => |v| {
                            if (field_type != .f64) return error.TypeMismatch;
                            const val = std.mem.bytesToValue(f64, val_ptr[0..8]);
                            if (val != v) matches = false;
                        },
                        .u64 => |v| {
                            if (field_type != .u64) return error.TypeMismatch;
                            const val = std.mem.bytesToValue(u64, val_ptr[0..8]);
                            if (val != v) matches = false;
                        },
                        .string => |v| {
                            if (field_type != .string) return error.TypeMismatch;
                            if (!std.mem.eql(u8, val_ptr[0..128], &v)) matches = false;
                        },
                        .bool => |v| {
                            if (field_type != .bool) return error.TypeMismatch;
                            const val = std.mem.bytesToValue(bool, val_ptr[0..1]);
                            if (val != v) matches = false;
                        },
                    }
                    if (!matches) break;
                }

                if (matches) {
                    try result_list.appendSlice(allocator, record_slice);
                }
            }
            current_idx += contiguous_count;
        }

        return result_list.toOwnedSlice(allocator);
    }

    fn getFieldOffset(self: *Self, field_index: usize) !usize {
        if (field_index >= self.fields.len) return error.InvalidFieldIndex;
        var offset: usize = 0;
        for (0..field_index) |i| {
            offset += self.fields[i].type.size();
        }
        return offset;
    }

    pub fn getLatest(self: *Self, field_index: usize) !struct { value: f64, timestamp: i64 } {
        try self.flush();
        if (self.count() == 0) return error.EmptyDB;

        if (field_index >= self.fields.len) return error.InvalidFieldIndex;

        // Get the last written record
        var last_record_offset: u64 = 0;
        if (self.write_cursor == HEADER_SIZE) {
            if (!self.is_wrapped) return error.EmptyDB;
            const data_capacity = self.max_file_size - HEADER_SIZE;
            last_record_offset = HEADER_SIZE + data_capacity - self.record_size;
        } else {
            last_record_offset = self.write_cursor - self.record_size;
        }

        var record_buf: [4096]u8 = undefined;
        if (self.record_size > record_buf.len) return error.RecordTooLarge;

        const len = try self.file.preadAll(record_buf[0..self.record_size], last_record_offset);
        if (len != self.record_size) return error.UnexpectedEndOfFile;

        const ts = std.mem.bytesToValue(i64, record_buf[self.timestamp_offset .. self.timestamp_offset + 8]);

        const field_offset = try self.getFieldOffset(field_index);
        const field_type = self.fields[field_index].type;

        const val: f64 = switch (field_type) {
            .f64 => std.mem.bytesToValue(f64, record_buf[field_offset .. field_offset + 8]),
            .i64 => @floatFromInt(std.mem.bytesToValue(i64, record_buf[field_offset .. field_offset + 8])),
            .u64 => @floatFromInt(std.mem.bytesToValue(u64, record_buf[field_offset .. field_offset + 8])),
            .u8 => @floatFromInt(std.mem.bytesToValue(u8, record_buf[field_offset .. field_offset + 1])),
            .bool => if (std.mem.bytesToValue(bool, record_buf[field_offset .. field_offset + 1])) 1.0 else 0.0,
            .string => return error.InvalidFieldTypeForStats, // Strings don't contribute to stats
        };

        return .{ .value = val, .timestamp = ts };
    }

    pub fn getStats(self: *Self, start_ts: i64, end_ts: i64, field_index: usize) !Stats {
        try self.flush();
        if (field_index >= self.fields.len) return error.InvalidFieldIndex;

        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);

        if (start_idx >= end_idx) {
            return Stats{ .min = 0, .max = 0, .sum = 0, .count = 0, .mean = 0 };
        }

        const field_offset = try self.getFieldOffset(field_index);
        const field_type = self.fields[field_index].type;

        var min: f64 = std.math.floatMax(f64);
        var max: f64 = -std.math.floatMax(f64);
        var sum: f64 = 0;
        var stats_count: u64 = 0;

        var current_idx = start_idx;

        // const result_size = record_count * self.record_size; // Unused
        // const result = try allocator.alloc(u8, result_size); // Unuseds_size);
        const CHUNK_RECORDS = 1024;
        const chunk_bytes_size = CHUNK_RECORDS * self.record_size;
        const alloc_buf = try self.allocator.alloc(u8, chunk_bytes_size);
        defer self.allocator.free(alloc_buf);

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
            chunk_count = @min(chunk_count, CHUNK_RECORDS);

            const read_size = chunk_count * self.record_size;
            const len = try self.file.preadAll(alloc_buf[0..read_size], physical_offset);
            if (len != read_size) return error.UnexpectedEndOfFile;

            var i: usize = 0;
            while (i < chunk_count) : (i += 1) {
                const rec_start = i * self.record_size;
                const val_bytes = alloc_buf[rec_start + field_offset .. rec_start + field_offset + field_type.size()];

                const val: f64 = switch (field_type) {
                    .f64 => std.mem.bytesToValue(f64, val_bytes[0..8]),
                    .i64 => @floatFromInt(std.mem.bytesToValue(i64, val_bytes[0..8])),
                    .u64 => @floatFromInt(std.mem.bytesToValue(u64, val_bytes[0..8])),
                    .u8 => @floatFromInt(std.mem.bytesToValue(u8, val_bytes[0..1])),
                    .bool => if (std.mem.bytesToValue(bool, val_bytes[0..1])) 1.0 else 0.0,
                    .string => 0.0, // Strings don't contribute to stats
                };

                if (val < min) min = val;
                if (val > max) max = val;
                sum += val;
                stats_count += 1;
            }
            current_idx += chunk_count;
        }

        if (stats_count == 0) {
            return Stats{ .min = 0, .max = 0, .sum = 0, .count = 0, .mean = 0 };
        }

        return Stats{
            .min = min,
            .max = max,
            .sum = sum,
            .count = stats_count,
            .mean = sum / @as(f64, @floatFromInt(stats_count)),
        };
    }

    pub fn load(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        return self.query(std.math.minInt(i64), std.math.maxInt(i64), &[_]Filter{}, allocator);
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
                    bool => FieldType.bool,
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
            const raw_bytes = try self.dynamic_db.query(start_ts, end_ts, &[_]Filter{}, allocator);
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

    const ticker = "TEST_ROOT_GENERIC";
    // Randomize directory to avoid conflicts between parallel test runners
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "test_root_{x}", .{std.crypto.random.int(u64)});

    // Cleanup with retry
    var retries: usize = 0;
    while (retries < 10) : (retries += 1) {
        std.fs.cwd().deleteTree(dir) catch |err| {
            if (err == error.FileNotFound) break;
            std.debug.print("deleteTree failed (retry {}): {}\n", .{ retries, err });
            if (retries == 9) return err; // Fail on last retry
            // std.time.sleep(10 * std.time.ns_per_ms);
            continue;
        };
        break;
    }
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
        // try std.testing.expectError(error.SchemaMismatch, WrongDB.init(ticker, dir, std.testing.allocator, .{}));
        if (WrongDB.init(ticker, dir, std.testing.allocator, .{})) |db_val| {
            var db = db_val;
            db.deinit();
            return error.TestExpectedError;
        } else |err| {
            if (err != error.SchemaMismatch) return err;
        }
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

test {
    _ = @import("test_stats.zig");
    _ = @import("test_query.zig");
}
