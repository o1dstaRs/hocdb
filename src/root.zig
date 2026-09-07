const std = @import("std");

/// Technical indicators and quantitative analytics kernels.
pub const indicators = @import("indicators.zig");
pub const calendar = @import("calendar.zig");
pub const universe_mod = @import("universe.zig");
pub const backtest_mod = @import("backtest.zig");

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
    p50: f64, // Median
    p90: f64,
    p95: f64,
    p99: f64,
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

const Dir = std.fs.Dir;
const File = std.fs.File;

fn cwd() Dir {
    return std.fs.cwd();
}

pub const DynamicTimeSeriesDB = struct {
    const Self = @This();

    pub const FsyncPolicy = enum(u8) {
        /// Never fsync (the OS decides when data reaches disk).
        none = 0,
        /// fsync once when the database is closed (default).
        on_close = 1,
        /// fsync after every flush (safest, slowest).
        on_flush = 2,
        /// fsync at most every `fsync_interval_ms` during flushes, and on close.
        interval = 3,
    };

    pub const Config = struct {
        max_file_size: u64 = 2 * 1024 * 1024 * 1024, // 2 GiB default
        overwrite_on_full: bool = true,
        flush_on_write: bool = false,
        auto_increment: bool = false,
        index_stride: u64 = 1024, // Number of records between index entries
        /// Durability policy for flushed data.
        fsync: FsyncPolicy = .on_close,
        fsync_interval_ms: u32 = 1000,
        /// Recompute the data checksum when opening (linear files only).
        verify_on_open: bool = false,
        /// Drop records older than `last_timestamp - retention_span` (timestamp
        /// units) by compacting the file; 0 = off. Linear mode only.
        retention_span: i64 = 0,
        /// Archive the file and start a new one once it exceeds this many
        /// bytes; 0 = off. Linear mode only.
        rollover_size: u64 = 0,
        /// Rewrite legacy "HOC1" files into the current format on open.
        auto_migrate: bool = true,
        /// Nanoseconds per timestamp unit (1000 for microseconds); 0 = unknown.
        /// Used to report the ingest lag in record time.
        timestamp_unit_ns: u64 = 0,
        /// Trading calendar (see calendar.zig: 1 crypto, 2 fx, 3 nyse, 4 nasdaq, 5 lse, 6 cme, custom ids from
        /// calendar.define). Built-in ids are persisted in the file header. Enables calendar sessions for
        /// session kinds with `param` = 0, trading-time gaps in health() and automatic periods_per_year.
        calendar: u32 = 0,
    };

    pub const IndexEntry = struct {
        timestamp: i64,
        index: u64,
    };

    /// Operational counters. Latencies in nanoseconds; p50/p99 are estimated
    /// from a log2 histogram of read operations.
    pub const Metrics = extern struct {
        appends: u64 = 0,
        bytes_written: u64 = 0,
        flushes: u64 = 0,
        commits: u64 = 0,
        fsyncs: u64 = 0,
        fsync_ns_total: u64 = 0,
        fsync_ns_max: u64 = 0,
        reads: u64 = 0,
        read_ns_total: u64 = 0,
        read_ns_max: u64 = 0,
        read_ns_last: u64 = 0,
        read_ns_p50: u64 = 0,
        read_ns_p99: u64 = 0,
        records_read: u64 = 0,
        refreshes: u64 = 0,
        recovered_tail_records: u64 = 0,
        dropped_tail_bytes: u64 = 0,
        crc_failures: u64 = 0,
        compactions: u64 = 0,
        rollovers: u64 = 0,
        migrations: u64 = 0,
        last_append_wall_ns: i64 = 0,
        last_commit_wall_ns: i64 = 0,
        last_record_ts: i64 = 0,
        /// Wall-clock nanoseconds since the last commit (readers) / append (writers).
        ingest_lag_wall_ns: i64 = 0,
        /// `now - last_record_ts` in nanoseconds when `timestamp_unit_ns` is known, else 0.
        ingest_lag_record_ns: i64 = 0,
        committed_records: u64 = 0,
        file_size: u64 = 0,
        format_version: u64 = 0,
        read_only: u64 = 0,
    };

    // File format. "HOC1" files (12-byte header: magic + schema hash) are the
    // legacy layout; "HOC2" files carry a 64-byte header:
    //   0  magic "HOC2"            4  version u16      6  flags u16 (bit0 crc valid)
    //   8  schema hash u64        16  record size u32  20  header size u32
    //  24  committed state u64 (write cursor | WRAP_BIT)   32 data crc32c u32  36 reserved u32
    //  40  max file size u64      48  last timestamp i64   56 last commit wall-clock ns i64
    // Readers re-read the committed state word (an aligned 8-byte pread) to
    // follow the writer without any lock.
    const MAGIC_V1 = "HOC1".*;
    const MAGIC_V2 = "HOC2".*;
    const HEADER_SIZE_V1: u64 = 12;
    const HEADER_SIZE_V2: u64 = 64;
    /// Header size of newly created files: a ring buffer of N records needs
    /// `max_file_size = HEADER_SIZE + N * record_size`.
    pub const HEADER_SIZE: u64 = HEADER_SIZE_V2;
    const WRAP_BIT: u64 = 1 << 63;
    const FLAG_CRC_VALID: u16 = 1;
    const FLAG_CALENDAR_SHIFT: u4 = 8; // bits 8..15: built-in calendar id
    const OFF_UNIT: u64 = 36; // u32 timestamp unit in nanoseconds (0 = unknown)
    const OFF_COMMITTED: u64 = 24;
    const OFF_CRC: u64 = 32;
    const OFF_FLAGS: u64 = 6;
    const OFF_LAST_TS: u64 = 48;
    const OFF_COMMIT_WALL: u64 = 56;
    const Crc = std.hash.crc.Crc32Iscsi;

    pub const HeaderInfo = struct {
        version: u16,
        header_size: u64,
        committed_cursor: u64,
        committed_wrapped: bool,
        crc: u32,
        crc_valid: bool,
        last_timestamp: i64,
        last_commit_wall_ns: i64,
        max_file_size: u64,
        timestamp_unit_ns: u32 = 0,
        calendar_id: u8 = 0,
    };

    // Custom Buffered Writer
    const BLOCK_SIZE = 4096;
    const BufferedWriter = struct {
        file: File,
        buffer: [BLOCK_SIZE]u8 = undefined,
        index: usize = 0,
        max_file_size: u64,
        header_size: u64,
        write_cursor: *u64,
        overwrite_on_full: bool,
        is_wrapped: *bool,
        record_size: usize,

        pub fn init(file: File, max_size: u64, header_size: u64, cursor: *u64, overwrite: bool, wrapped: *bool, rec_size: usize) @This() {
            return .{
                .file = file,
                .buffer = undefined,
                .index = 0,
                .max_file_size = max_size,
                .header_size = header_size,
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
                    self.write_cursor.* = self.header_size;
                    self.is_wrapped.* = true;
                    continue;
                }

                try self.file.pwriteAll(remaining[0..chunk_size], self.write_cursor.*);
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

    file: File,
    buffered_writer: BufferedWriter,
    writer_ready: bool = false,
    last_timestamp: ?i64 = null,
    max_file_size: u64,
    overwrite_on_full: bool,
    flush_on_write: bool,
    auto_increment: bool,
    write_cursor: u64,
    is_wrapped: bool = false,

    // Format / durability state
    header_size: u64,
    format_version: u16,
    read_only: bool = false,
    committed_cursor: u64,
    committed_wrapped: bool = false,
    crc_state: Crc,
    crc_valid: bool = true,
    config: Config,
    /// Trading calendar id (0 = none), see Config.calendar.
    calendar_id: u32 = 0,
    last_fsync_ns: i64 = 0,
    file_inode: u64 = 0,
    metrics: Metrics = .{},
    read_hist: [40]u64 = [_]u64{0} ** 40,
    in_maintenance: bool = false,

    // Schema info
    record_size: usize,
    timestamp_offset: usize,
    schema_hash: u64,
    fields: []FieldInfo, // Store schema fields
    full_path: []const u8,

    // In-memory Sparse Index (Linear mode only)
    sparse_index: std.ArrayListUnmanaged(IndexEntry) = .{},
    index_stride: u64,

    allocator: std.mem.Allocator,

    fn wallNs() i64 {
        return @intCast(@as(i128, @intCast(std.time.nanoTimestamp())));
    }

    fn readHeaderBytes(file: File, buf: []u8) !void {
        const n = try file.preadAll(buf, 0);
        if (n != buf.len) return error.UnexpectedEndOfFile;
    }

    fn headerFlags(crc_valid: bool, calendar_id: u32) u16 {
        const cal: u16 = if (calendar_id < calendar.first_custom_id) @intCast(calendar_id & 0xFF) else 0; // custom ids are process-local
        return (if (crc_valid) FLAG_CRC_VALID else 0) | (cal << FLAG_CALENDAR_SHIFT);
    }

    fn unitWord(unit_ns: u64) u32 {
        return if (unit_ns > std.math.maxInt(u32)) 0 else @intCast(unit_ns);
    }

    fn buildHeaderV2(schema_hash: u64, record_size: usize, committed: u64, wrapped: bool, crc: u32, crc_valid: bool, max_file_size: u64, last_ts: i64, wall: i64, unit_ns: u64, calendar_id: u32) [HEADER_SIZE_V2]u8 {
        var h: [HEADER_SIZE_V2]u8 = [_]u8{0} ** HEADER_SIZE_V2;
        @memcpy(h[0..4], &MAGIC_V2);
        std.mem.writeInt(u16, h[4..6], 2, .little);
        std.mem.writeInt(u16, h[6..8], headerFlags(crc_valid, calendar_id), .little);
        std.mem.writeInt(u32, h[36..40], unitWord(unit_ns), .little);
        std.mem.writeInt(u64, h[8..16], schema_hash, .little);
        std.mem.writeInt(u32, h[16..20], @intCast(record_size), .little);
        std.mem.writeInt(u32, h[20..24], @intCast(HEADER_SIZE_V2), .little);
        std.mem.writeInt(u64, h[24..32], committed | (if (wrapped) WRAP_BIT else 0), .little);
        std.mem.writeInt(u32, h[32..36], crc, .little);
        std.mem.writeInt(u64, h[40..48], max_file_size, .little);
        std.mem.writeInt(i64, h[48..56], last_ts, .little);
        std.mem.writeInt(i64, h[56..64], wall, .little);
        return h;
    }

    /// Parse a v2 header (the caller has checked the magic).
    fn parseHeaderV2(h: []const u8) !HeaderInfo {
        if (h.len < HEADER_SIZE_V2) return error.InvalidFile;
        const version = std.mem.readInt(u16, h[4..6], .little);
        if (version != 2) return error.UnsupportedFormatVersion;
        const flags = std.mem.readInt(u16, h[6..8], .little);
        const header_size = std.mem.readInt(u32, h[20..24], .little);
        if (header_size != HEADER_SIZE_V2) return error.InvalidFile;
        const committed = std.mem.readInt(u64, h[24..32], .little);
        return HeaderInfo{
            .version = 2,
            .header_size = header_size,
            .committed_cursor = committed & ~WRAP_BIT,
            .committed_wrapped = (committed & WRAP_BIT) != 0,
            .crc = std.mem.readInt(u32, h[32..36], .little),
            .crc_valid = (flags & FLAG_CRC_VALID) != 0,
            .last_timestamp = std.mem.readInt(i64, h[48..56], .little),
            .last_commit_wall_ns = std.mem.readInt(i64, h[56..64], .little),
            .max_file_size = std.mem.readInt(u64, h[40..48], .little),
            .timestamp_unit_ns = std.mem.readInt(u32, h[36..40], .little),
            .calendar_id = @intCast((flags >> FLAG_CALENDAR_SHIFT) & 0xFF),
        };
    }

    /// Open (or create) a database for writing. Fails with
    /// `error.DatabaseLocked` when another writer holds the file.
    pub fn init(ticker: []const u8, dir_path: []const u8, allocator: std.mem.Allocator, schema: Schema, config: Config) !Self {
        return initMode(ticker, dir_path, allocator, schema, config, false);
    }

    /// Attach to a database that another process writes. No lock is taken;
    /// every read re-reads the writer's committed cursor (see `refresh`).
    /// Appends and maintenance operations fail with `error.ReadOnly`.
    pub fn openReader(ticker: []const u8, dir_path: []const u8, allocator: std.mem.Allocator, schema: Schema) !Self {
        return initMode(ticker, dir_path, allocator, schema, .{}, true);
    }

    fn initMode(ticker: []const u8, dir_path: []const u8, allocator: std.mem.Allocator, schema: Schema, config: Config, read_only: bool) !Self {
        const record_size = schema.recordSize();
        if (record_size == 0) return error.InvalidSchema;
        const ts_offset = schema.timestampOffset() orelse return error.MissingTimestampField;
        const schema_hash = schema.computeHash();
        if (config.max_file_size < HEADER_SIZE_V2 + record_size) return error.MaxFileSizeTooSmall;

        if (!read_only) {
            var dir = try cwd().makeOpenPath(dir_path, .{});
            dir.close();
        }
        const filename = try std.fmt.allocPrint(allocator, "{s}.bin", .{ticker});
        defer allocator.free(filename);
        const full_path = try std.fs.path.join(allocator, &[_][]const u8{ dir_path, filename });
        errdefer allocator.free(full_path);

        var file: File = undefined;
        if (cwd().openFile(full_path, .{ .mode = if (read_only) .read_only else .read_write })) |f| {
            file = f;
        } else |err| {
            if (err == error.FileNotFound and !read_only) {
                file = try cwd().createFile(full_path, .{ .read = true, .truncate = false });
            } else {
                return err;
            }
        }
        errdefer file.close();
        if (!read_only) {
            const locked = try file.tryLock(.exclusive);
            if (!locked) return error.DatabaseLocked;
        }
        errdefer if (!read_only) file.unlock();

        const stat = try file.stat();
        var header_size: u64 = HEADER_SIZE_V2;
        var format_version: u16 = 2;
        var last_timestamp: ?i64 = null;
        var write_cursor: u64 = HEADER_SIZE_V2;
        var is_wrapped = false;
        var committed_cursor: u64 = HEADER_SIZE_V2;
        var committed_wrapped = false;
        var crc_state = Crc.init();
        var crc_valid = true;
        var header_crc: u32 = 0;
        var last_commit_wall: i64 = 0;
        var recovered: u64 = 0;
        var open_crc_failures: u64 = 0;
        var header_unit: u64 = 0;
        var header_cal: u32 = 0;
        if (config.calendar != 0 and calendar.get(config.calendar) == null) return error.UnknownCalendar;
        var dropped_bytes: u64 = 0;
        var effective_max_size: u64 = 0;
        var needs_migration = false;

        if (stat.size == 0) {
            if (read_only) return error.EmptyDatabase;
            const data_capacity = config.max_file_size - HEADER_SIZE_V2;
            effective_max_size = HEADER_SIZE_V2 + (data_capacity / record_size) * record_size;
            const h = buildHeaderV2(schema_hash, record_size, HEADER_SIZE_V2, false, 0, true, effective_max_size, 0, wallNs(), config.timestamp_unit_ns, config.calendar);
            try file.pwriteAll(&h, 0);
        } else {
            if (stat.size < HEADER_SIZE_V1) return error.InvalidFile;
            var magic: [4]u8 = undefined;
            try readHeaderBytes(file, &magic);
            if (std.mem.eql(u8, &magic, &MAGIC_V1)) {
                // ---- legacy layout ------------------------------------------
                if (read_only) return error.LegacyFormatNeedsMigration;
                format_version = 1;
                header_size = HEADER_SIZE_V1;
                crc_valid = false;
                var hb: [HEADER_SIZE_V1]u8 = undefined;
                try readHeaderBytes(file, &hb);
                if (std.mem.bytesToValue(u64, hb[4..12]) != schema_hash) return error.SchemaMismatch;
                const data_capacity = config.max_file_size - HEADER_SIZE_V1;
                effective_max_size = HEADER_SIZE_V1 + (data_capacity / record_size) * record_size;
                const rec = try recoverLegacy(file, stat.size, effective_max_size, record_size, ts_offset, allocator);
                write_cursor = rec.cursor;
                is_wrapped = rec.wrapped;
                last_timestamp = rec.last_timestamp;
                committed_cursor = write_cursor;
                committed_wrapped = is_wrapped;
                needs_migration = config.auto_migrate;
            } else if (std.mem.eql(u8, &magic, &MAGIC_V2)) {
                var hb: [HEADER_SIZE_V2]u8 = undefined;
                try readHeaderBytes(file, &hb);
                const info = try parseHeaderV2(&hb);
                header_unit = info.timestamp_unit_ns;
                header_cal = info.calendar_id;
                if (std.mem.readInt(u64, hb[8..16], .little) != schema_hash) return error.SchemaMismatch;
                if (std.mem.readInt(u32, hb[16..20], .little) != record_size) return error.SchemaMismatch;
                effective_max_size = info.max_file_size;
                committed_cursor = info.committed_cursor;
                committed_wrapped = info.committed_wrapped;
                write_cursor = committed_cursor;
                is_wrapped = committed_wrapped;
                header_crc = info.crc;
                crc_valid = info.crc_valid;
                last_commit_wall = info.last_commit_wall_ns;
                if (committed_cursor > header_size or committed_wrapped) last_timestamp = info.last_timestamp;
                if (committed_cursor < header_size or committed_cursor > effective_max_size) return error.CorruptedData;
                if (!read_only) {
                    // Records written after the last commit (a crash before the
                    // header update) are adopted when they are complete and keep
                    // the timestamp order; anything else is truncated away.
                    if (!is_wrapped and stat.size > committed_cursor) {
                        const tail = try recoverTail(file, committed_cursor, stat.size, record_size, ts_offset, last_timestamp, allocator);
                        recovered = tail.records;
                        write_cursor = committed_cursor + recovered * record_size;
                        if (recovered > 0) last_timestamp = tail.last_timestamp;
                        dropped_bytes = stat.size - write_cursor;
                        if (dropped_bytes > 0) try file.setEndPos(write_cursor);
                        if (recovered > 0) {
                            // rebuild the checksum over the adopted tail; it is
                            // published (with the new cursor) at the next commit
                            crc_state = try computeCrcState(file, header_size, write_cursor, allocator);
                            header_crc = crc_state.final();
                            crc_valid = true;
                        }
                    } else if (!is_wrapped and stat.size < committed_cursor) {
                        return error.CorruptedData;
                    }
                }
                if (crc_valid and !is_wrapped) {
                    if (config.verify_on_open) {
                        const computed = try computeCrc(file, header_size, committed_cursor, allocator);
                        if (computed != header_crc) return error.ChecksumMismatch;
                    }
                    if (!read_only and recovered == 0) {
                        // rebuild the running checksum so later appends extend it
                        crc_state = try computeCrcState(file, header_size, committed_cursor, allocator);
                        if (config.verify_on_open == false and committed_cursor > header_size and crc_state.final() != header_crc) {
                            // The committed data no longer matches its stored checksum
                            // (corrupted while closed). Keep the data readable, count the
                            // failure and let verify() report the mismatch; the next
                            // commit republishes the checksum of the data as it is now.
                            open_crc_failures = 1;
                        }
                    }
                }
            } else {
                return error.InvalidMagic;
            }
        }

        if (config.auto_increment and last_timestamp == null) last_timestamp = 0;

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

        var self = Self{
            .file = file,
            .buffered_writer = undefined, // initWriter
            .last_timestamp = last_timestamp,
            .max_file_size = effective_max_size,
            .overwrite_on_full = config.overwrite_on_full,
            .flush_on_write = config.flush_on_write,
            .auto_increment = config.auto_increment,
            .write_cursor = write_cursor,
            .is_wrapped = is_wrapped,
            .header_size = header_size,
            .format_version = format_version,
            .read_only = read_only,
            .committed_cursor = committed_cursor,
            .committed_wrapped = committed_wrapped,
            .crc_state = crc_state,
            .crc_valid = crc_valid,
            .config = config,
            .last_fsync_ns = wallNs(),
            .file_inode = stat.inode,
            .record_size = record_size,
            .timestamp_offset = ts_offset,
            .schema_hash = schema_hash,
            .fields = fields_copy,
            .allocator = allocator,
            .full_path = full_path,
            .sparse_index = .{},
            .index_stride = config.index_stride,
        };
        self.metrics.recovered_tail_records = recovered;
        self.metrics.crc_failures = open_crc_failures;
        // timestamp unit and calendar: the config wins, else what the file records
        if (config.timestamp_unit_ns == 0) self.config.timestamp_unit_ns = header_unit;
        self.calendar_id = if (config.calendar != 0) config.calendar else header_cal;
        if (self.calendar_id != 0 and calendar.get(self.calendar_id) == null) self.calendar_id = 0;
        self.metrics.dropped_tail_bytes = dropped_bytes;
        self.metrics.last_commit_wall_ns = last_commit_wall;
        if (last_timestamp) |t| self.metrics.last_record_ts = t;
        if (needs_migration) {
            // same record capacity with the larger header
            try self.rewriteLogical(0, HEADER_SIZE_V2 + (effective_max_size - HEADER_SIZE_V1));
            self.metrics.migrations += 1;
        }
        return self;
    }

    const LegacyRecovery = struct { cursor: u64, wrapped: bool, last_timestamp: ?i64 };

    /// Legacy (HOC1) cursor recovery: file size in linear mode, a scan for the
    /// wrap point in ring mode.
    fn recoverLegacy(file: File, size: u64, effective_max_size: u64, record_size: usize, ts_offset: usize, allocator: std.mem.Allocator) !LegacyRecovery {
        const hs = HEADER_SIZE_V1;
        if (size < effective_max_size) {
            const usable = size - ((size - hs) % record_size); // drop a torn tail record
            var last: ?i64 = null;
            if (usable > hs) {
                var buf: [8]u8 = undefined;
                const n = try file.preadAll(&buf, usable - record_size + ts_offset);
                if (n != 8) return error.UnexpectedEndOfFile;
                last = std.mem.bytesToValue(i64, &buf);
            }
            if (usable != size) try file.setEndPos(usable);
            return .{ .cursor = usable, .wrapped = false, .last_timestamp = last };
        }
        // ring buffer: find the first decreasing timestamp
        const total = (size - hs) / record_size;
        const batch = 128;
        const buf = try allocator.alloc(u8, batch * record_size);
        defer allocator.free(buf);
        var prev: i64 = std.math.minInt(i64);
        var max_ts: i64 = std.math.minInt(i64);
        var max_idx: u64 = 0;
        var found = false;
        var idx: u64 = 0;
        while (idx < total and !found) {
            const cnt = @min(batch, total - idx);
            const n = try file.preadAll(buf[0 .. cnt * record_size], hs + idx * record_size);
            if (n != cnt * record_size) return error.UnexpectedEndOfFile;
            for (0..cnt) |i| {
                const ts = std.mem.bytesToValue(i64, buf[i * record_size + ts_offset .. i * record_size + ts_offset + 8]);
                if ((idx > 0 or i > 0) and ts < prev) {
                    max_ts = prev;
                    max_idx = idx + i - 1;
                    found = true;
                    break;
                }
                prev = ts;
                max_ts = ts;
                max_idx = idx + i;
            }
            idx += cnt;
        }
        var cursor: u64 = if (found) hs + (max_idx + 1) * record_size else hs;
        if (cursor >= size) cursor = hs;
        return .{ .cursor = cursor, .wrapped = true, .last_timestamp = if (total > 0) max_ts else null };
    }

    const TailRecovery = struct { records: u64, last_timestamp: i64 };

    /// Validate records in [from, to): complete and with strictly increasing
    /// timestamps continuing `last`. Returns how many are acceptable.
    fn recoverTail(file: File, from: u64, to: u64, record_size: usize, ts_offset: usize, last: ?i64, allocator: std.mem.Allocator) !TailRecovery {
        const bytes = to - from;
        const full = bytes / record_size;
        var prev: i64 = last orelse std.math.minInt(i64);
        var ok: u64 = 0;
        const batch = 256;
        const buf = try allocator.alloc(u8, batch * record_size);
        defer allocator.free(buf);
        var idx: u64 = 0;
        outer: while (idx < full) {
            const cnt = @min(batch, full - idx);
            const n = try file.preadAll(buf[0 .. cnt * record_size], from + idx * record_size);
            if (n != cnt * record_size) break;
            for (0..cnt) |i| {
                const ts = std.mem.bytesToValue(i64, buf[i * record_size + ts_offset .. i * record_size + ts_offset + 8]);
                if (last != null and ts <= prev) break :outer;
                if (last == null and ok > 0 and ts <= prev) break :outer;
                prev = ts;
                ok += 1;
            }
            idx += cnt;
        }
        return .{ .records = ok, .last_timestamp = prev };
    }

    fn computeCrcState(file: File, from: u64, to: u64, allocator: std.mem.Allocator) !Crc {
        var h = Crc.init();
        if (to <= from) return h;
        const buf = try allocator.alloc(u8, 1 << 20);
        defer allocator.free(buf);
        var pos = from;
        while (pos < to) {
            const n = @min(buf.len, to - pos);
            const got = try file.preadAll(buf[0..n], pos);
            if (got != n) return error.UnexpectedEndOfFile;
            h.update(buf[0..n]);
            pos += n;
        }
        return h;
    }

    fn computeCrc(file: File, from: u64, to: u64, allocator: std.mem.Allocator) !u32 {
        var h = try computeCrcState(file, from, to, allocator);
        return h.final();
    }

    /// Recompute the checksum of the committed data and compare it with the
    /// stored one. `error.ChecksumUnavailable` for ring / legacy / adopted-tail files.
    pub fn verify(self: *Self) !bool {
        try self.flush();
        if (self.format_version != 2 or self.is_wrapped or !self.crc_valid) return error.ChecksumUnavailable;
        const stored = try self.storedCrc();
        const computed = try computeCrc(self.file, self.header_size, self.committed_cursor, self.allocator);
        if (computed != stored) self.metrics.crc_failures += 1;
        return computed == stored;
    }

    fn storedCrc(self: *Self) !u32 {
        var b: [4]u8 = undefined;
        const n = try self.file.preadAll(&b, OFF_CRC);
        if (n != 4) return error.UnexpectedEndOfFile;
        return std.mem.readInt(u32, &b, .little);
    }

    /// Header fields as stored on disk.
    pub fn headerInfo(self: *Self) !HeaderInfo {
        if (self.format_version != 2) {
            return HeaderInfo{ .version = 1, .header_size = HEADER_SIZE_V1, .committed_cursor = self.write_cursor, .committed_wrapped = self.is_wrapped, .crc = 0, .crc_valid = false, .last_timestamp = self.last_timestamp orelse 0, .last_commit_wall_ns = 0, .max_file_size = self.max_file_size };
        }
        var hb: [HEADER_SIZE_V2]u8 = undefined;
        try readHeaderBytes(self.file, &hb);
        return parseHeaderV2(&hb);
    }

    // Power-up the index after init
    pub fn buildIndex(self: *Self) !void {
        self.sparse_index.clearRetainingCapacity();
        // Skip index for auto-increment (record index IS the logical order)
        if (self.auto_increment) return;
        // Only build index if not wrapped and not empty
        if (self.is_wrapped) return;
        const total_count = self.count();
        if (total_count == 0) return;

        const capacity = (total_count / self.index_stride) + 1;
        try self.sparse_index.ensureTotalCapacity(self.allocator, capacity);

        var i: u64 = 0;
        while (i < total_count) : (i += self.index_stride) {
            const ts = try self.readTimestampAt(i);
            try self.sparse_index.append(self.allocator, .{ .timestamp = ts, .index = i });
        }
    }

    /// Extend the sparse index for records appended since it was built.
    fn extendIndex(self: *Self, old_count: u64, new_count: u64) !void {
        if (self.auto_increment or self.is_wrapped) return;
        var i: u64 = if (old_count == 0) 0 else ((old_count - 1) / self.index_stride + 1) * self.index_stride;
        while (i < new_count) : (i += self.index_stride) {
            const ts = try self.readTimestampAt(i);
            try self.sparse_index.append(self.allocator, .{ .timestamp = ts, .index = i });
        }
    }

    // Post-init to set up self-referencing buffered writer
    pub fn initWriter(self: *Self) !void {
        self.buffered_writer = BufferedWriter.init(self.file, self.max_file_size, self.header_size, &self.write_cursor, self.overwrite_on_full, &self.is_wrapped, self.record_size);
        self.writer_ready = true;
        try self.buildIndex();
    }

    pub fn deinit(self: *Self) void {
        if (!self.read_only) {
            self.flush() catch |err| {
                std.debug.print("ERROR: Failed to flush in deinit: {}\n", .{err});
            };
            if (self.config.fsync != .none) self.file.sync() catch {};
            self.file.unlock();
        }
        self.file.close();
        for (self.fields) |f| self.allocator.free(f.name);
        self.allocator.free(self.fields);
        self.allocator.free(self.full_path);
        self.sparse_index.deinit(self.allocator);
    }

    pub fn drop(self: *Self) !void {
        if (self.read_only) return error.ReadOnly;
        // Close the file first
        self.file.unlock();
        self.file.close();

        // Delete the file
        try cwd().deleteFile(self.full_path);

        // Free resources (similar to deinit but we don't close file again)
        for (self.fields) |f| self.allocator.free(f.name);
        self.allocator.free(self.fields);
        self.allocator.free(self.full_path);
        self.sparse_index.deinit(self.allocator);
    }

    /// Writers: write buffered records, publish the committed cursor in the
    /// header and apply the fsync / retention / rollover policies. Readers:
    /// pick up the writer's latest commit (see `refresh`).
    pub fn flush(self: *Self) anyerror!void {
        if (self.read_only) return self.refresh();
        if (!self.writer_ready) return;
        const pending = self.buffered_writer.index > 0;
        if (pending) {
            try self.buffered_writer.flush();
            self.metrics.flushes += 1;
        }
        if (pending or self.write_cursor != self.committed_cursor or self.is_wrapped != self.committed_wrapped) {
            try self.commit();
        }
        if (self.config.fsync == .interval and !self.in_maintenance) {
            const now = wallNs();
            if (now - self.last_fsync_ns >= @as(i64, self.config.fsync_interval_ms) * std.time.ns_per_ms and self.metrics.commits > 0) {
                try self.fsyncNow();
            }
        }
        if (!self.in_maintenance) try self.maintain();
    }

    /// Publish the write cursor to readers (and to crash recovery).
    fn commit(self: *Self) anyerror!void {
        if (self.is_wrapped) self.crc_valid = false;
        if (self.format_version == 2) {
            const now = wallNs();
            var w: [8]u8 = undefined;
            std.mem.writeInt(i64, &w, self.last_timestamp orelse 0, .little);
            try self.file.pwriteAll(&w, OFF_LAST_TS);
            std.mem.writeInt(i64, &w, now, .little);
            try self.file.pwriteAll(&w, OFF_COMMIT_WALL);
            var c: [4]u8 = undefined;
            std.mem.writeInt(u32, &c, if (self.crc_valid) self.crc_state.final() else 0, .little);
            try self.file.pwriteAll(&c, OFF_CRC);
            var f: [2]u8 = undefined;
            std.mem.writeInt(u16, &f, headerFlags(self.crc_valid, self.calendar_id), .little);
            try self.file.pwriteAll(&f, OFF_FLAGS);
            std.mem.writeInt(u32, &c, unitWord(self.config.timestamp_unit_ns), .little);
            try self.file.pwriteAll(&c, OFF_UNIT);
            // the committed-state word goes last: readers key on it
            std.mem.writeInt(u64, &w, self.write_cursor | (if (self.is_wrapped) WRAP_BIT else 0), .little);
            try self.file.pwriteAll(&w, OFF_COMMITTED);
            self.metrics.last_commit_wall_ns = now;
        }
        self.committed_cursor = self.write_cursor;
        self.committed_wrapped = self.is_wrapped;
        self.metrics.commits += 1;
        if (self.config.fsync == .on_flush) try self.fsyncNow();
    }

    fn fsyncNow(self: *Self) !void {
        var t = try std.time.Timer.start();
        try self.file.sync();
        const ns = t.read();
        self.metrics.fsyncs += 1;
        self.metrics.fsync_ns_total += ns;
        if (ns > self.metrics.fsync_ns_max) self.metrics.fsync_ns_max = ns;
        self.last_fsync_ns = wallNs();
    }

    /// Flush and fsync now, whatever the policy.
    pub fn sync(self: *Self) !void {
        if (self.read_only) return error.ReadOnly;
        try self.flush();
        try self.fsyncNow();
    }

    /// Readers: re-read the writer's committed cursor and follow a rewritten
    /// (compacted / rolled over) file. Cheap: one aligned 8-byte pread.
    pub fn refresh(self: *Self) anyerror!void {
        if (!self.read_only) return;
        self.metrics.refreshes += 1;
        // The writer may have replaced the file (compaction / rollover).
        if (cwd().statFile(self.full_path)) |st| {
            if (st.inode != self.file_inode) {
                const f = try cwd().openFile(self.full_path, .{ .mode = .read_only });
                self.file.close();
                self.file = f;
                self.file_inode = st.inode;
                var hb: [HEADER_SIZE_V2]u8 = undefined;
                try readHeaderBytes(self.file, &hb);
                if (!std.mem.eql(u8, hb[0..4], &MAGIC_V2)) return error.InvalidMagic;
                const info = try parseHeaderV2(&hb);
                if (self.config.timestamp_unit_ns == 0) self.config.timestamp_unit_ns = info.timestamp_unit_ns;
                if (self.calendar_id == 0 and calendar.get(info.calendar_id) != null) self.calendar_id = info.calendar_id;
                self.max_file_size = info.max_file_size;
                self.write_cursor = info.committed_cursor;
                self.is_wrapped = info.committed_wrapped;
                self.committed_cursor = self.write_cursor;
                self.committed_wrapped = self.is_wrapped;
                self.last_timestamp = if (self.count() > 0) info.last_timestamp else null;
                self.metrics.last_commit_wall_ns = info.last_commit_wall_ns;
                self.buffered_writer = BufferedWriter.init(self.file, self.max_file_size, self.header_size, &self.write_cursor, self.overwrite_on_full, &self.is_wrapped, self.record_size);
                try self.buildIndex();
                return;
            }
        } else |_| {}
        var w: [8]u8 = undefined;
        const n = try self.file.preadAll(&w, OFF_COMMITTED);
        if (n != 8) return error.UnexpectedEndOfFile;
        const committed = std.mem.readInt(u64, &w, .little);
        const cursor = committed & ~WRAP_BIT;
        const wrapped = (committed & WRAP_BIT) != 0;
        if (cursor == self.write_cursor and wrapped == self.is_wrapped) return;
        const old_count = self.count();
        self.write_cursor = cursor;
        self.is_wrapped = wrapped;
        self.committed_cursor = cursor;
        self.committed_wrapped = wrapped;
        var t: [8]u8 = undefined;
        if (try self.file.preadAll(&t, OFF_LAST_TS) == 8) {
            self.last_timestamp = std.mem.readInt(i64, &t, .little);
            self.metrics.last_record_ts = self.last_timestamp.?;
        }
        if (try self.file.preadAll(&t, OFF_COMMIT_WALL) == 8) self.metrics.last_commit_wall_ns = std.mem.readInt(i64, &t, .little);
        if (wrapped) {
            self.sparse_index.clearAndFree(self.allocator);
        } else {
            try self.extendIndex(old_count, self.count());
        }
    }

    pub fn append(self: *Self, data: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        if (!self.writer_ready) return error.WriterNotInitialized;
        if (data.len != self.record_size) return error.InvalidRecordSize;

        if (self.auto_increment) {
            // Increment timestamp
            const new_ts = (self.last_timestamp orelse 0) + 1;
            self.last_timestamp = new_ts;

            var mut_data = try self.allocator.alloc(u8, data.len);
            defer self.allocator.free(mut_data);
            @memcpy(mut_data, data);

            const ts_bytes = std.mem.asBytes(&new_ts);
            @memcpy(mut_data[self.timestamp_offset .. self.timestamp_offset + 8], ts_bytes);

            if (self.crc_valid and !self.is_wrapped) self.crc_state.update(mut_data);
            try self.buffered_writer.write(mut_data);
            self.metrics.last_record_ts = new_ts;
        } else {
            // Monotonicity Check
            const ts = std.mem.bytesToValue(i64, data[self.timestamp_offset .. self.timestamp_offset + 8]);
            if (self.last_timestamp) |last| {
                if (ts <= last) return error.TimestampNotMonotonic;
            }
            self.last_timestamp = ts;

            if (self.crc_valid and !self.is_wrapped) self.crc_state.update(data);
            try self.buffered_writer.write(data);
            self.metrics.last_record_ts = ts;
        }
        if (self.is_wrapped) self.crc_valid = false;
        self.metrics.appends += 1;
        self.metrics.bytes_written += data.len;
        self.metrics.last_append_wall_ns = wallNs();
        if (self.config.rollover_size > 0 and !self.is_wrapped and !self.in_maintenance and self.write_cursor >= self.config.rollover_size) {
            try self.flush(); // commits and rolls the file over
        }

        // Maintain Sparse Index (skip for auto-increment since record index IS the order)
        if (self.auto_increment) {
            // No sparse index needed for auto-increment
        } else if (self.is_wrapped) {
            // If wrapped, we disable the index for correctness
            if (self.sparse_index.items.len > 0) {
                self.sparse_index.clearAndFree(self.allocator);
            }
        } else {
            // Linear, safe to index
            const logical_bytes = (self.write_cursor - self.header_size) + self.buffered_writer.index;
            const current_count = logical_bytes / self.record_size;

            if (current_count > 0) {
                const new_rec_idx = current_count - 1;
                if (new_rec_idx % self.index_stride == 0) {
                    const ts = std.mem.bytesToValue(i64, data[self.timestamp_offset .. self.timestamp_offset + 8]);
                    try self.sparse_index.append(self.allocator, .{ .timestamp = ts, .index = new_rec_idx });
                }
            }
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
        if (offset <= self.header_size) return 0;
        return (offset - self.header_size) / self.record_size;
    }

    fn getPhysicalOffsetLinear(self: *Self, index: u64) u64 {
        return self.header_size + index * self.record_size;
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
    pub fn readTimestampAt(self: *Self, index: u64) !i64 {
        const offset = self.getPhysicalOffset(index);
        var ts_buf: [8]u8 = undefined;
        const len = try self.file.preadAll(&ts_buf, offset + self.timestamp_offset);
        if (len != 8) return error.UnexpectedEndOfFile;
        return std.mem.bytesToValue(i64, &ts_buf);
    }

    // ---------------------------------------------------------------------
    // Maintenance: compaction (retention), rollover, migration
    // ---------------------------------------------------------------------

    /// Rewrite the logical records [start_idx, count) into a fresh v2 file
    /// that atomically replaces the current one (linear layout, checksum
    /// rebuilt). Used by compaction, retention and legacy migration.
    fn rewriteLogical(self: *Self, start_idx: u64, new_max_file_size: u64) anyerror!void {
        if (self.read_only) return error.ReadOnly;
        if (self.writer_ready) try self.buffered_writer.flush();
        const total = self.count();
        const keep: u64 = if (start_idx < total) total - start_idx else 0;
        const data_bytes = keep * self.record_size;
        if (new_max_file_size < HEADER_SIZE_V2 + data_bytes) return error.MaxFileSizeTooSmall;

        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.rewrite", .{self.full_path});
        defer self.allocator.free(tmp_path);
        var tmp = try cwd().createFile(tmp_path, .{ .read = true, .truncate = true });
        var tmp_ok = false;
        defer if (!tmp_ok) {
            tmp.close();
            cwd().deleteFile(tmp_path) catch {};
        };
        // copy in logical order, computing the checksum on the way
        var crc = Crc.init();
        var last_ts: i64 = 0;
        const chunk_records: u64 = 4096;
        const buf = try self.allocator.alloc(u8, chunk_records * self.record_size);
        defer self.allocator.free(buf);
        var idx = start_idx;
        var out_pos: u64 = HEADER_SIZE_V2;
        while (idx < total) {
            const physical = self.getPhysicalOffset(idx);
            var n: u64 = total - idx;
            if (self.is_wrapped) {
                const until_end = (self.max_file_size - physical) / self.record_size;
                n = @min(n, until_end);
            }
            n = @min(n, chunk_records);
            const bytes: usize = @intCast(n * self.record_size);
            const got = try self.file.preadAll(buf[0..bytes], physical);
            if (got != bytes) return error.UnexpectedEndOfFile;
            crc.update(buf[0..bytes]);
            try tmp.pwriteAll(buf[0..bytes], out_pos);
            out_pos += bytes;
            idx += n;
            last_ts = std.mem.bytesToValue(i64, buf[bytes - self.record_size + self.timestamp_offset .. bytes - self.record_size + self.timestamp_offset + 8]);
        }
        if (keep == 0) last_ts = self.last_timestamp orelse 0;
        const data_capacity = new_max_file_size - HEADER_SIZE_V2;
        const eff_max = HEADER_SIZE_V2 + (data_capacity / self.record_size) * self.record_size;
        const h = buildHeaderV2(self.schema_hash, self.record_size, out_pos, false, crc.final(), true, eff_max, last_ts, wallNs(), self.config.timestamp_unit_ns, self.calendar_id);
        try tmp.pwriteAll(&h, 0);
        try tmp.sync();
        tmp.close();
        tmp_ok = true;
        // swap files: lock the new one before releasing the old one
        var nf = try cwd().openFile(tmp_path, .{ .mode = .read_write });
        errdefer nf.close();
        if (!try nf.tryLock(.exclusive)) return error.DatabaseLocked;
        try cwd().rename(tmp_path, self.full_path);
        self.file.unlock();
        self.file.close();
        self.file = nf;
        const st = try nf.stat();
        self.file_inode = st.inode;
        self.header_size = HEADER_SIZE_V2;
        self.format_version = 2;
        self.max_file_size = eff_max;
        self.write_cursor = out_pos;
        self.is_wrapped = false;
        self.committed_cursor = out_pos;
        self.committed_wrapped = false;
        self.crc_state = crc;
        self.crc_valid = true;
        self.last_timestamp = if (keep > 0) last_ts else self.last_timestamp;
        self.last_fsync_ns = wallNs();
        if (self.writer_ready) {
            self.buffered_writer = BufferedWriter.init(self.file, self.max_file_size, self.header_size, &self.write_cursor, self.overwrite_on_full, &self.is_wrapped, self.record_size);
            try self.buildIndex();
        }
    }

    /// Keep only records with timestamp >= min_ts (rewrites the file).
    pub fn compact(self: *Self, min_ts: i64) anyerror!void {
        if (self.read_only) return error.ReadOnly;
        try self.flush();
        const start = try self.binarySearch(min_ts);
        try self.rewriteLogical(start, self.max_file_size);
        self.metrics.compactions += 1;
    }

    /// Keep only the last `n` records (rewrites the file).
    pub fn retainLast(self: *Self, n: u64) anyerror!void {
        if (self.read_only) return error.ReadOnly;
        try self.flush();
        const total = self.count();
        try self.rewriteLogical(if (total > n) total - n else 0, self.max_file_size);
        self.metrics.compactions += 1;
    }

    /// Archive the current file as `<ticker>.<first_ts>-<last_ts>.bin` next
    /// to it and continue with an empty file. Returns the archive path
    /// (caller frees). Readers follow automatically on their next refresh.
    pub fn rollover(self: *Self, allocator: std.mem.Allocator) anyerror![]const u8 {
        if (self.read_only) return error.ReadOnly;
        try self.flush();
        const total = self.count();
        if (total == 0) return error.EmptyDatabase;
        const first_ts = try self.readTimestampAt(0);
        const last_ts = self.last_timestamp orelse first_ts;
        const base = if (std.mem.endsWith(u8, self.full_path, ".bin")) self.full_path[0 .. self.full_path.len - 4] else self.full_path;
        const archive = try std.fmt.allocPrint(allocator, "{s}.{d}-{d}.bin", .{ base, first_ts, last_ts });
        errdefer allocator.free(archive);
        if (self.config.fsync != .none) try self.fsyncNow();
        // create the successor first so a crash leaves a valid file behind
        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.rollover", .{self.full_path});
        defer self.allocator.free(tmp_path);
        var nf = try cwd().createFile(tmp_path, .{ .read = true, .truncate = true });
        errdefer {
            nf.close();
            cwd().deleteFile(tmp_path) catch {};
        }
        const h = buildHeaderV2(self.schema_hash, self.record_size, HEADER_SIZE_V2, false, 0, true, self.max_file_size, last_ts, wallNs(), self.config.timestamp_unit_ns, self.calendar_id);
        try nf.pwriteAll(&h, 0);
        try nf.sync();
        if (!try nf.tryLock(.exclusive)) return error.DatabaseLocked;
        try cwd().rename(self.full_path, archive);
        try cwd().rename(tmp_path, self.full_path);
        self.file.unlock();
        self.file.close();
        self.file = nf;
        const st = try nf.stat();
        self.file_inode = st.inode;
        self.header_size = HEADER_SIZE_V2;
        self.format_version = 2;
        self.write_cursor = HEADER_SIZE_V2;
        self.is_wrapped = false;
        self.committed_cursor = HEADER_SIZE_V2;
        self.committed_wrapped = false;
        self.crc_state = Crc.init();
        self.crc_valid = true;
        // keep last_timestamp: the stream stays monotonic across files
        self.sparse_index.clearRetainingCapacity();
        self.buffered_writer = BufferedWriter.init(self.file, self.max_file_size, self.header_size, &self.write_cursor, self.overwrite_on_full, &self.is_wrapped, self.record_size);
        self.metrics.rollovers += 1;
        return archive;
    }

    /// Retention and rollover policies (called after every flush).
    fn maintain(self: *Self) anyerror!void {
        if (self.is_wrapped or self.format_version != 2) return;
        self.in_maintenance = true;
        defer self.in_maintenance = false;
        if (self.config.rollover_size > 0 and self.write_cursor >= self.config.rollover_size) {
            const p = try self.rollover(self.allocator);
            self.allocator.free(p);
            return;
        }
        if (self.config.retention_span > 0 and self.count() > 1) {
            const first = try self.readTimestampAt(0);
            const last = self.last_timestamp orelse first;
            const span = self.config.retention_span;
            // compact once the excess exceeds 25% of the span (amortised O(1))
            if (@as(i128, last) - @as(i128, first) > @as(i128, span) + @as(i128, @divTrunc(span, 4))) {
                try self.compact(clampI64(@as(i128, last) - @as(i128, span)));
            }
        }
    }

    // ---------------------------------------------------------------------
    // Metrics
    // ---------------------------------------------------------------------

    fn readStart(self: *Self) std.time.Timer {
        _ = self;
        return std.time.Timer.start() catch unreachable;
    }

    fn readEnd(self: *Self, timer: *std.time.Timer, records: u64) void {
        const ns = timer.read();
        self.metrics.reads += 1;
        self.metrics.read_ns_total += ns;
        self.metrics.read_ns_last = ns;
        if (ns > self.metrics.read_ns_max) self.metrics.read_ns_max = ns;
        self.metrics.records_read += records;
        const bucket: usize = if (ns == 0) 0 else @min(39, 64 - @clz(ns));
        self.read_hist[bucket] += 1;
    }

    fn histPercentile(self: *Self, q: f64) u64 {
        var total: u64 = 0;
        for (self.read_hist) |c| total += c;
        if (total == 0) return 0;
        const target: u64 = @intFromFloat(@ceil(q * @as(f64, @floatFromInt(total))));
        var acc: u64 = 0;
        for (self.read_hist, 0..) |c, b| {
            acc += c;
            if (acc >= target) return if (b == 0) 0 else @as(u64, 1) << @intCast(b);
        }
        return 0;
    }

    /// Snapshot of the operational counters (derived fields filled in).
    pub fn getMetrics(self: *Self) Metrics {
        var m = self.metrics;
        const now = wallNs();
        // histogram buckets are log2 upper bounds: never report a percentile above the observed maximum
        m.read_ns_p50 = @min(self.histPercentile(0.5), m.read_ns_max);
        m.read_ns_p99 = @min(self.histPercentile(0.99), m.read_ns_max);
        const ref = if (self.read_only) m.last_commit_wall_ns else (if (m.last_append_wall_ns != 0) m.last_append_wall_ns else m.last_commit_wall_ns);
        m.ingest_lag_wall_ns = if (ref != 0) now - ref else 0;
        m.ingest_lag_record_ns = 0;
        if (self.config.timestamp_unit_ns > 0 and self.last_timestamp != null) {
            const rec_ns: i128 = @as(i128, self.last_timestamp.?) * @as(i128, @intCast(self.config.timestamp_unit_ns));
            m.ingest_lag_record_ns = clampI64(@as(i128, now) - rec_ns);
        }
        m.committed_records = self.count();
        m.file_size = if (self.file.stat()) |st| st.size else |_| 0;
        m.format_version = self.format_version;
        m.read_only = if (self.read_only) 1 else 0;
        return m;
    }

    pub fn resetMetrics(self: *Self) void {
        const keep_ts = self.metrics.last_record_ts;
        const keep_wall = self.metrics.last_commit_wall_ns;
        self.metrics = .{};
        self.metrics.last_record_ts = keep_ts;
        self.metrics.last_commit_wall_ns = keep_wall;
        self.read_hist = [_]u64{0} ** 40;
    }

    pub fn binarySearch(self: *Self, target: i64) !u64 {
        var left: u64 = 0;
        var right: u64 = self.count();

        // Sparse Index Optimization
        if (self.sparse_index.items.len > 0) {
            // We can narrow the search range
            const SearchContext = struct {
                pub fn compare(key: i64, entry: IndexEntry) std.math.Order {
                    return std.math.order(key, entry.timestamp);
                }
            };
            // Find the first entry where entry.timestamp >= target
            // lowerBound returns index of first element >= key
            // But we want the range where target COULD be.
            // If sparse_index = [ {100, 0}, {200, 100}, {300, 200} ]
            // target = 150.
            // binarySearch(150) -> matches index 1 (200).
            // Implementation uses `std.sort.lowerBound`.

            const idx_idx = std.sort.lowerBound(IndexEntry, self.sparse_index.items, target, SearchContext.compare);

            if (idx_idx > 0) {
                // The target might be in the block starting at idx_idx - 1
                left = self.sparse_index.items[idx_idx - 1].index;
            } else {
                left = 0;
            }

            if (idx_idx < self.sparse_index.items.len) {
                // The target is definitely before idx_idx (since entry.ts >= target)
                // actually, if entry.ts == target, it could be AT idx_idx.
                // safe upper bound:
                right = self.sparse_index.items[idx_idx].index + 1;
                // Wait, if target == 200, lowerBound returns index 1 ({200, 100}).
                // record at 100 is 200.
                // left = items[0].index = 0.
                // right = items[1].index = 100.
                // So we search 0..100. Record 100 is 200. binarySearch(200) on 0..100 returns 100? No.
                // Range 0..100 excludes 100.
                // binarySearch contract: returns first index where ts >= target.
                // If record[100].ts == 200, we want 100.
                // So right boundary should be inclusive? No, `right` in binary search is exclusive usually.
                // Let's look at existing binarySearch loop:
                // while (left < right)
                // right starts at count().

                // If sparse index says {200, 100}.
                // target 200. lowerBound -> index 1.
                // We want strictly tighter bounds?
                // Actually, if items[idx_idx].timestamp >= target.
                // The record at items[idx_idx].index has ts >= target.
                // So the answer must be <= items[idx_idx].index.
                // So right = items[idx_idx].index + 1? No.
                // Because multiple records can have same timestamp?
                // If record 100 has 200. record 101 has 200.
                // We want index 100.
                // So right can be items[idx_idx].index + 1?
                // Let's be safe:
                // right = items[idx_idx].index + self.index_stride? No.

                // If items[idx_idx] is the first entry >= target.
                // Then items[idx_idx].index is a valid candidate for the answer.
                // So right should include it.
                // right = items[idx_idx].index + 1. (Since loop is left < right).

                // HOWEVER, if lowerBound returned `len`, then target > all entries.
                // Then right = count().

                right = self.sparse_index.items[idx_idx].index + 1;
            }

            // Clamp right to count() just in case
            if (right > self.count()) right = self.count();
        }

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
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
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

        self.metrics.records_read += result_list.items.len / self.record_size;
        return result_list.toOwnedSlice(allocator);
    }

    pub fn queryInto(self: *Self, start_ts: i64, end_ts: i64, filters: []const Filter, buffer: []u8) !usize {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();

        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);

        if (start_idx >= end_idx) {
            return 0;
        }

        var current_idx = start_idx;
        var bytes_written: usize = 0;

        // For filtering with pre-allocated buffer
        // We need a small scratch buffer if filters are present.
        // If no filters, we read directly into buffer.

        // If filters are present, we need an allocator for temp buffer?
        // To keep this "no-alloc", we should only support no-filter optimization or require a scratch buffer.
        // For now, let's fall back to alloc if (filters.len > 0), OR just fail to keep it strict.
        // But the main use case is bulk fetch without filters.

        // If filters are present, we need an allocator for temp buffer?
        // To keep this "no-alloc", we should only support no-filter optimization or require a scratch buffer.
        // For now, let's fall back to alloc if (filters.len > 0), OR just fail to keep it strict.
        // But the main use case is bulk fetch without filters.

        while (current_idx < end_idx) {
            // Determine contiguous count
            var contiguous_count: u64 = 0;
            var physical_offset: u64 = 0;

            if (self.is_wrapped) {
                const capacity = self.count();
                const start_rec_index = self.countRecordsFromOffset(self.write_cursor);
                physical_offset = self.getPhysicalOffset(current_idx);

                const records_until_wrap = capacity - ((start_rec_index + current_idx) % capacity);
                const bytes_until_eof = self.max_file_size - physical_offset;
                const records_physically_contiguous = bytes_until_eof / self.record_size;

                contiguous_count = @min(end_idx - current_idx, records_until_wrap);
                contiguous_count = @min(contiguous_count, records_physically_contiguous);
            } else {
                physical_offset = self.getPhysicalOffset(current_idx);
                contiguous_count = end_idx - current_idx;
            }

            const MAX_CHUNK_RECORDS = 1024;
            contiguous_count = @min(contiguous_count, MAX_CHUNK_RECORDS);

            if (contiguous_count == 0) break;

            const read_size = contiguous_count * self.record_size;

            if (filters.len == 0) {
                if (bytes_written + read_size > buffer.len) return error.BufferTooSmall;

                const dest_slice = buffer[bytes_written .. bytes_written + read_size];
                const len = try self.file.preadAll(dest_slice, physical_offset);
                if (len != read_size) return error.UnexpectedEndOfFile;

                bytes_written += read_size;
            } else {
                // Filtering Logic with Stack Buffer (limited chunk size)
                // If read_size > 4096, we can't use stack buffer.
                // We limited chunk to 1024 records. record_size must be small enough.
                // If record_size * 1024 > 4096 (e.g. record is > 4 bytes), this fails.
                // Safe approach: Read record-by-record for filtering if strictly no-alloc.

                // For this optimization, we primarily care about the NO-FILTER path.
                // So skipping complex implementation for filters in queryInto for now.
                return error.FiltersNotSupportedInQueryInto;
            }

            current_idx += contiguous_count;
        }

        return bytes_written;
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
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (self.count() == 0) return error.EmptyDB;

        if (field_index >= self.fields.len) return error.InvalidFieldIndex;

        // Get the last written record
        var last_record_offset: u64 = 0;
        if (self.write_cursor == self.header_size) {
            if (!self.is_wrapped) return error.EmptyDB;
            const data_capacity = self.max_file_size - self.header_size;
            last_record_offset = self.header_size + data_capacity - self.record_size;
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

        self.metrics.records_read += 1;
        return .{ .value = val, .timestamp = ts };
    }

    pub fn getStats(self: *Self, start_ts: i64, end_ts: i64, field_index: usize, compute_percentiles: bool) !Stats {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (field_index >= self.fields.len) return error.InvalidFieldIndex;

        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);

        // Initialize empty stats
        if (start_idx >= end_idx) {
            return Stats{ .min = 0, .max = 0, .sum = 0, .count = 0, .mean = 0, .p50 = 0, .p90 = 0, .p95 = 0, .p99 = 0 };
        }

        const field_offset = try self.getFieldOffset(field_index);
        const field_type = self.fields[field_index].type;

        // Collect all values to compute percentiles
        var values = std.ArrayListUnmanaged(f64){};
        defer values.deinit(self.allocator);

        if (compute_percentiles) {
            try values.ensureTotalCapacity(self.allocator, end_idx - start_idx);
        }

        var min: f64 = std.math.floatMax(f64);
        var max: f64 = -std.math.floatMax(f64);
        var sum: f64 = 0;
        var stats_count: u64 = 0;

        var current_idx = start_idx;
        const CHUNK_RECORDS = 1024;
        const chunk_bytes_size = CHUNK_RECORDS * self.record_size;
        const alloc_buf = try self.allocator.alloc(u8, chunk_bytes_size);
        defer self.allocator.free(alloc_buf);

        while (current_idx < end_idx) {
            const physical_offset = self.getPhysicalOffset(current_idx);

            var chunk_count: u64 = 0;
            if (self.is_wrapped) {
                const data_capacity = self.max_file_size - self.header_size;
                const offset_in_data = physical_offset - self.header_size;
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

                if (compute_percentiles) {
                    values.appendAssumeCapacity(val);
                }
            }
            current_idx += chunk_count;
        }

        if (stats_count == 0) {
            return Stats{ .min = 0, .max = 0, .sum = 0, .count = 0, .mean = 0, .p50 = 0, .p90 = 0, .p95 = 0, .p99 = 0 };
        }

        var p50: f64 = 0;
        var p90: f64 = 0;
        var p95: f64 = 0;
        var p99: f64 = 0;

        if (compute_percentiles) {
            // Sort for percentiles
            std.mem.sort(f64, values.items, {}, std.sort.asc(f64));

            const count_f = @as(f64, @floatFromInt(stats_count));
            const p50_idx = @min(stats_count - 1, @as(usize, @intFromFloat(count_f * 0.50)));
            const p90_idx = @min(stats_count - 1, @as(usize, @intFromFloat(count_f * 0.90)));
            const p95_idx = @min(stats_count - 1, @as(usize, @intFromFloat(count_f * 0.95)));
            const p99_idx = @min(stats_count - 1, @as(usize, @intFromFloat(count_f * 0.99)));

            p50 = values.items[p50_idx];
            p90 = values.items[p90_idx];
            p95 = values.items[p95_idx];
            p99 = values.items[p99_idx];
        }

        self.metrics.records_read += stats_count;
        return Stats{
            .min = min,
            .max = max,
            .sum = sum,
            .count = stats_count,
            .mean = sum / @as(f64, @floatFromInt(stats_count)),
            .p50 = p50,
            .p90 = p90,
            .p95 = p95,
            .p99 = p99,
        };
    }

    // ---------------------------------------------------------------------
    // Indicators / analytics (see indicators.zig)
    // ---------------------------------------------------------------------

    /// Field indices of the price/volume roles used by indicators (-1 = absent).
    pub const IndicatorColumns = extern struct {
        open: i64 = -1,
        high: i64 = -1,
        low: i64 = -1,
        close: i64 = -1,
        volume: i64 = -1,
        /// Tick-level quotes and aggressor side (1 = buy) for microstructure kinds.
        bid: i64 = -1,
        ask: i64 = -1,
        side: i64 = -1,
    };

    /// Use the recommended per-spec warm-up when passed as `lookback`.
    pub const lookback_auto: usize = std.math.maxInt(usize);

    /// Timestamps plus a set of f64 columns for a logical record range.
    pub const ColumnSet = struct {
        ts: []i64,
        cols: [][]f64,
        allocator: std.mem.Allocator,

        pub fn deinit(self: ColumnSet) void {
            for (self.cols) |c| self.allocator.free(c);
            self.allocator.free(self.cols);
            self.allocator.free(self.ts);
        }
    };

    /// Result of a batch indicator computation: `values` is planar, output k
    /// (0 <= k < n_outputs) occupies values[k*n_rows .. (k+1)*n_rows].
    pub const IndicatorResult = struct {
        timestamps: []i64,
        values: []f64,
        n_rows: usize,
        n_outputs: usize,
        allocator: std.mem.Allocator,

        pub fn output(self: IndicatorResult, k: usize) []const f64 {
            return self.values[k * self.n_rows .. (k + 1) * self.n_rows];
        }

        pub fn deinit(self: IndicatorResult) void {
            self.allocator.free(self.timestamps);
            self.allocator.free(self.values);
        }
    };

    fn fieldToF64(field_type: FieldType, bytes: []const u8) f64 {
        return switch (field_type) {
            .f64 => std.mem.bytesToValue(f64, bytes[0..8]),
            .i64 => @floatFromInt(std.mem.bytesToValue(i64, bytes[0..8])),
            .u64 => @floatFromInt(std.mem.bytesToValue(u64, bytes[0..8])),
            .u8 => @floatFromInt(bytes[0]),
            .bool => if (bytes[0] != 0) 1.0 else 0.0,
            .string => indicators.nan,
        };
    }

    /// Read logical records [start_idx, end_idx) once and extract the
    /// timestamp plus each requested field as an f64 column. Handles the
    /// ring-buffer wrap; a single chunked pass over the file.
    pub fn readColumns(self: *Self, start_idx: u64, end_idx: u64, field_indices: []const usize, allocator: std.mem.Allocator) !ColumnSet {
        try self.flush();
        const n: usize = if (end_idx > start_idx) @intCast(end_idx - start_idx) else 0;
        const ts = try allocator.alloc(i64, n);
        errdefer allocator.free(ts);
        const cols = try allocator.alloc([]f64, field_indices.len);
        var allocated: usize = 0;
        errdefer {
            for (cols[0..allocated]) |c| allocator.free(c);
            allocator.free(cols);
        }
        for (field_indices, 0..) |_, k| {
            cols[k] = try allocator.alloc(f64, n);
            allocated += 1;
        }
        var offsets: [64]usize = undefined;
        var types: [64]FieldType = undefined;
        if (field_indices.len > offsets.len) return error.TooManyColumns;
        for (field_indices, 0..) |fi, k| {
            offsets[k] = try self.getFieldOffset(fi);
            types[k] = self.fields[fi].type;
        }
        if (n == 0) return ColumnSet{ .ts = ts, .cols = cols, .allocator = allocator };

        const CHUNK_RECORDS: u64 = 4096;
        const buf = try self.allocator.alloc(u8, CHUNK_RECORDS * self.record_size);
        defer self.allocator.free(buf);
        var current = start_idx;
        var row: usize = 0;
        while (current < end_idx) {
            const physical_offset = self.getPhysicalOffset(current);
            var chunk: u64 = end_idx - current;
            if (self.is_wrapped) {
                const data_capacity = self.max_file_size - self.header_size;
                const offset_in_data = physical_offset - self.header_size;
                const until_end = (data_capacity - offset_in_data) / self.record_size;
                chunk = @min(chunk, until_end);
            }
            chunk = @min(chunk, CHUNK_RECORDS);
            const read_size: usize = @intCast(chunk * self.record_size);
            const len = try self.file.preadAll(buf[0..read_size], physical_offset);
            if (len != read_size) return error.UnexpectedEndOfFile;
            var i: usize = 0;
            while (i < chunk) : (i += 1) {
                const rec = buf[i * self.record_size .. (i + 1) * self.record_size];
                ts[row] = std.mem.bytesToValue(i64, rec[self.timestamp_offset .. self.timestamp_offset + 8]);
                for (0..field_indices.len) |k| {
                    cols[k][row] = fieldToF64(types[k], rec[offsets[k]..]);
                }
                row += 1;
            }
            current += chunk;
        }
        self.metrics.records_read += n;
        return ColumnSet{ .ts = ts, .cols = cols, .allocator = allocator };
    }

    fn resolveLookback(specs: []const indicators.Spec, lookback: usize) !usize {
        if (lookback != lookback_auto) return lookback;
        var lb: usize = 0;
        for (specs) |s| {
            const p = try indicators.resolve(s);
            lb = @max(lb, indicators.warmup(p));
        }
        return lb;
    }

    /// Distinct field indices needed by `cols` and the specs (max 64).
    fn collectFields(cols: IndicatorColumns, specs: []const indicators.Spec, out: []usize) !usize {
        var n: usize = 0;
        const roles = [_]i64{ cols.open, cols.high, cols.low, cols.close, cols.volume, cols.bid, cols.ask, cols.side };
        const Add = struct {
            fn add(list: []usize, cnt: *usize, v: i64) !void {
                if (v < 0) return;
                const u: usize = @intCast(v);
                for (list[0..cnt.*]) |e| if (e == u) return;
                if (cnt.* >= list.len) return error.TooManyColumns;
                list[cnt.*] = u;
                cnt.* += 1;
            }
        };
        for (roles) |r| try Add.add(out, &n, r);
        for (specs) |s| {
            try Add.add(out, &n, s.field_index);
            try Add.add(out, &n, s.field_index2);
        }
        return n;
    }

    fn findCol(fields: []const usize, v: i64) ?usize {
        if (v < 0) return null;
        const u: usize = @intCast(v);
        for (fields, 0..) |f, k| if (f == u) return k;
        return null;
    }

    /// Columns (records or resampled bars) backing one batch computation.
    const Source = struct {
        set: ColumnSet,
        bars: ?indicators.Bars,
        field_list: [64]usize,
        nf: usize,
        ts: []const i64,
        open: ?[]const f64,
        high: ?[]const f64,
        low: ?[]const f64,
        close: []const f64,
        volume: ?[]const f64,
        bid: ?[]const f64,
        ask: ?[]const f64,
        side: ?[]const f64,
        buy_volume: ?[]const f64,

        fn deinit(self: *Source, allocator: std.mem.Allocator) void {
            if (self.bars) |b| b.deinit(allocator);
            self.set.deinit();
        }

        fn rows(self: *const Source) usize {
            return self.ts.len;
        }

        /// Drop the first `k` rows (bar mode only; the backing storage is kept).
        fn trimFront(self: *Source, k: usize) void {
            const kk = @min(k, self.ts.len);
            self.ts = self.ts[kk..];
            if (self.open) |o| self.open = o[kk..];
            if (self.high) |h| self.high = h[kk..];
            if (self.low) |l| self.low = l[kk..];
            self.close = self.close[kk..];
            if (self.volume) |v| self.volume = v[kk..];
            if (self.bid) |b| self.bid = b[kk..];
            if (self.ask) |a| self.ask = a[kk..];
            if (self.side) |sd| self.side = sd[kk..];
            if (self.buy_volume) |bv| self.buy_volume = bv[kk..];
        }

        /// Number of leading rows whose timestamp is before `ws`.
        fn rowsBefore(self: *const Source, ws: i64) usize {
            var w: usize = 0;
            while (w < self.ts.len and self.ts[w] < ws) w += 1;
            return w;
        }
    };

    fn clampI64(x: i128) i64 {
        if (x < std.math.minInt(i64)) return std.math.minInt(i64);
        if (x > std.math.maxInt(i64)) return std.math.maxInt(i64);
        return @intCast(x);
    }

    /// Read the columns for records [start_idx, end_idx) and, with `bucket` > 0,
    /// aggregate them into OHLCV bars.
    fn readSource(self: *Self, start_idx: u64, end_idx: u64, cols: IndicatorColumns, specs: []const indicators.Spec, bucket: i64, allocator: std.mem.Allocator) !Source {
        if (cols.close < 0) return error.MissingCloseColumn;
        if (cols.close >= self.fields.len) return error.InvalidFieldIndex;
        var src: Source = undefined;
        src.nf = try collectFields(cols, specs, &src.field_list);
        for (src.field_list[0..src.nf]) |fi| if (fi >= self.fields.len) return error.InvalidFieldIndex;
        if (bucket > 0) {
            for (specs) |s| if (s.field_index >= 0 or s.field_index2 >= 0) return error.FieldOverrideNotSupportedWithBucket;
        }
        src.set = try self.readColumns(start_idx, end_idx, src.field_list[0..src.nf], allocator);
        errdefer src.set.deinit();
        const fl = src.field_list[0..src.nf];
        src.bars = null;
        src.ts = src.set.ts;
        src.open = if (findCol(fl, cols.open)) |k| src.set.cols[k] else null;
        src.high = if (findCol(fl, cols.high)) |k| src.set.cols[k] else null;
        src.low = if (findCol(fl, cols.low)) |k| src.set.cols[k] else null;
        src.close = src.set.cols[findCol(fl, cols.close).?];
        src.volume = if (findCol(fl, cols.volume)) |k| src.set.cols[k] else null;
        src.bid = if (findCol(fl, cols.bid)) |k| src.set.cols[k] else null;
        src.ask = if (findCol(fl, cols.ask)) |k| src.set.cols[k] else null;
        src.side = if (findCol(fl, cols.side)) |k| src.set.cols[k] else null;
        src.buy_volume = null;
        if (bucket > 0) {
            const b = try indicators.resampleOhlcvSide(src.set.ts, src.open, src.high, src.low, src.close, src.volume, src.side, bucket, allocator);
            src.bars = b;
            src.ts = b.ts;
            src.open = b.open;
            src.high = b.high;
            src.low = b.low;
            src.close = b.close;
            src.volume = b.volume;
            src.buy_volume = if (src.side != null) b.buy_volume else null;
            src.bid = null;
            src.ask = null;
            src.side = null;
        }
        return src;
    }

    /// Run every spec over a source; the first `warm` rows are computed but
    /// not returned.
    fn computeFromSource(self: *Self, src: *const Source, warm_in: usize, specs: []const indicators.Spec, allocator: std.mem.Allocator) !IndicatorResult {
        return self.computeFromSourcePair(src, null, warm_in, specs, allocator);
    }

    /// Like computeFromSource; `second` (same length as the source rows) is the
    /// aligned second series for pair kinds.
    fn computeFromSourcePair(self: *Self, src: *const Source, second: ?[]const f64, warm_in: usize, specs: []const indicators.Spec, allocator: std.mem.Allocator) !IndicatorResult {
        var n_outputs: usize = 0;
        var need_calendar = false;
        for (specs) |s| {
            const p = try indicators.resolve(s);
            n_outputs += indicators.outputCount(p.kind);
            if (indicators.usesCalendarSessions(p)) need_calendar = true;
        }
        // per-row session starts from the trading calendar (session kinds with param = 0)
        const session_starts: ?[]i64 = if (need_calendar) try self.sessionStarts(src.ts, allocator) else null;
        defer if (session_starts) |st| allocator.free(st);
        const n_ext = src.rows();
        const warm = @min(warm_in, n_ext);
        const n_rows = n_ext - warm;
        const timestamps = try allocator.alloc(i64, n_rows);
        errdefer allocator.free(timestamps);
        @memcpy(timestamps, src.ts[warm..]);
        const values = try allocator.alloc(f64, n_outputs * n_rows);
        errdefer allocator.free(values);
        const scratch = try allocator.alloc(f64, 8 * n_ext);
        defer allocator.free(scratch);
        const fl = src.field_list[0..src.nf];
        var out_k: usize = 0;
        for (specs) |s| {
            const p = try indicators.resolve(s);
            const cnt = indicators.outputCount(p.kind);
            var outs: [8][]f64 = undefined;
            for (0..cnt) |j| outs[j] = scratch[j * n_ext .. (j + 1) * n_ext];
            // Cumulative kinds are anchored at the window start: they only see
            // the returned rows, never the warm-up rows.
            const from: usize = if (indicators.anchoredAtWindowStart(p)) warm else 0;
            const c = indicators.Columns{
                .open = if (src.open) |o| o[from..] else null,
                .high = if (src.high) |h| h[from..] else null,
                .low = if (src.low) |l| l[from..] else null,
                .close = src.close[from..],
                .volume = if (src.volume) |v| v[from..] else null,
                .bid = if (src.bid) |b| b[from..] else null,
                .ask = if (src.ask) |a| a[from..] else null,
                .side = if (src.side) |sd| sd[from..] else null,
                .buy_volume = if (src.buy_volume) |bv| bv[from..] else null,
                .time = src.ts[from..],
                .session_starts = if (session_starts) |st| st[from..] else null,
                .input = if (src.bars == null) (if (findCol(fl, s.field_index)) |k| src.set.cols[k][from..] else null) else null,
                .input2 = if (second) |b2| b2[from..] else (if (src.bars == null) (if (findCol(fl, s.field_index2)) |k| src.set.cols[k][from..] else null) else null),
            };
            var sliced: [8][]f64 = undefined;
            for (0..cnt) |j| sliced[j] = outs[j][from..];
            try indicators.compute(s, c, sliced[0..cnt], allocator);
            for (0..cnt) |j| {
                @memcpy(values[(out_k + j) * n_rows .. (out_k + j + 1) * n_rows], outs[j][warm..]);
            }
            out_k += cnt;
        }
        return IndicatorResult{ .timestamps = timestamps, .values = values, .n_rows = n_rows, .n_outputs = n_outputs, .allocator = allocator };
    }

    // -- trading calendar and timestamp unit ------------------------------------

    /// The database's trading calendar, if one is configured or recorded in the file.
    pub fn tradingCalendar(self: *Self) ?*const calendar.Calendar {
        return calendar.get(self.calendar_id);
    }

    pub fn timestampUnitNs(self: *Self) u64 {
        return self.config.timestamp_unit_ns;
    }

    /// Set the trading calendar for this handle (writers persist built-in ids in the header).
    pub fn setCalendar(self: *Self, id: u32) anyerror!void {
        if (id != 0 and calendar.get(id) == null) return error.UnknownCalendar;
        self.calendar_id = id;
        try self.persistMeta();
    }

    /// Set the timestamp unit in nanoseconds (1000 = microseconds); writers persist it.
    pub fn setTimestampUnit(self: *Self, unit_ns: u64) anyerror!void {
        self.config.timestamp_unit_ns = unit_ns;
        try self.persistMeta();
    }

    fn persistMeta(self: *Self) anyerror!void {
        if (self.read_only or self.format_version != 2) return;
        var f: [2]u8 = undefined;
        std.mem.writeInt(u16, &f, headerFlags(self.crc_valid and !self.is_wrapped, self.calendar_id), .little);
        try self.file.pwriteAll(&f, OFF_FLAGS);
        var u: [4]u8 = undefined;
        std.mem.writeInt(u32, &u, unitWord(self.config.timestamp_unit_ns), .little);
        try self.file.pwriteAll(&u, OFF_UNIT);
    }

    fn secToTs(self: *Self, sec: i64) i64 {
        return indicators.fromSec(sec, self.config.timestamp_unit_ns);
    }

    /// Session a row at `sec` belongs to: the session containing it, else the
    /// session of the same local trade date (extended hours), else the last
    /// session that opened (or the next one when there is none before).
    fn assignSession(self: *Self, cal: *const calendar.Calendar, sec: i64) ?calendar.Session {
        _ = self;
        if (cal.sessionAt(sec)) |s| return s;
        const local_day = @divFloor(cal.utcToLocal(sec), calendar.DAY);
        if (cal.sessionForDay(local_day)) |s| return s;
        return cal.prevSession(sec) orelse cal.nextSession(sec);
    }

    /// Per-row session start timestamps (in timestamp units) from the calendar.
    fn sessionStarts(self: *Self, ts: []const i64, allocator: std.mem.Allocator) ![]i64 {
        const cal = self.tradingCalendar() orelse return error.CalendarRequired;
        const unit = self.config.timestamp_unit_ns;
        if (unit == 0) return error.CalendarRequired;
        const out = try allocator.alloc(i64, ts.len);
        errdefer allocator.free(out);
        var cur: ?calendar.Session = null;
        var cur_local_day: i64 = 0;
        var cur_start: i64 = 0;
        for (ts, 0..) |t, i| {
            const sec = indicators.toSec(t, unit);
            var hit = false;
            if (cur) |c| {
                if (sec >= c.open and sec < c.close) {
                    hit = true;
                } else if (@divFloor(cal.utcToLocal(sec), calendar.DAY) == cur_local_day and c.trade_day == cur_local_day) {
                    hit = true;
                }
            }
            if (!hit) {
                cur = self.assignSession(cal, sec);
                cur_local_day = @divFloor(cal.utcToLocal(sec), calendar.DAY);
                cur_start = if (cur) |c| self.secToTs(c.open) else t;
            }
            out[i] = cur_start;
        }
        return out;
    }

    /// Bars per year for `bucket` (timestamp units) from the calendar and the
    /// timestamp unit; 0 when either is unknown.
    pub fn periodsPerYear(self: *Self, bucket: i64) f64 {
        const cal = self.tradingCalendar() orelse return 0;
        const unit = self.config.timestamp_unit_ns;
        if (unit == 0 or bucket <= 0) return 0;
        const bucket_sec = @as(f64, @floatFromInt(bucket)) * @as(f64, @floatFromInt(unit)) / 1e9;
        return cal.periodsPerYear(bucket_sec);
    }

    /// Robust row spacing estimate (median of up to 4096 sampled gaps).
    fn estimateSpacing(ts: []const i64) i64 {
        if (ts.len < 2) return 0;
        var buf: [4096]i64 = undefined;
        const gaps = ts.len - 1;
        const n = @min(gaps, buf.len);
        const step = gaps / n;
        for (0..n) |k| {
            const i = k * step;
            buf[k] = ts[i + 1] - ts[i];
        }
        std.sort.pdq(i64, buf[0..n], {}, std.sort.asc(i64));
        return buf[n / 2];
    }

    /// `ppy` when given, else the calendar-derived value for `bucket` (or the
    /// observed row spacing when bucket is 0), else 0 (no annualisation).
    fn effectivePpy(self: *Self, ppy: f64, bucket: i64, ts: []const i64) f64 {
        if (ppy > 0) return ppy;
        return self.periodsPerYear(if (bucket > 0) bucket else estimateSpacing(ts));
    }

    /// Earliest timestamp session-anchored specs need for a window starting at
    /// `first_ts`, or null when no spec is session-anchored. Kinds that use the
    /// previous session (pivots) get the start of the last session that has
    /// data before the current one (weekends / holidays are skipped).
    fn sessionFloor(self: *Self, specs: []const indicators.Spec, first_ts: i64) !?i64 {
        var floor: ?i64 = null;
        for (specs) |s| {
            const p = try indicators.resolve(s);
            if (indicators.usesCalendarSessions(p)) {
                const cal = self.tradingCalendar() orelse return error.CalendarRequired;
                if (self.config.timestamp_unit_ns == 0) return error.CalendarRequired;
                const cur_s = self.assignSession(cal, indicators.toSec(first_ts, self.config.timestamp_unit_ns)) orelse continue;
                var t = self.secToTs(cur_s.open);
                if (indicators.needsPreviousSession(p.kind)) {
                    const idx = try self.binarySearch(t);
                    if (idx > 0) {
                        const prev_ts = try self.readTimestampAt(idx - 1);
                        if (self.assignSession(cal, indicators.toSec(prev_ts, self.config.timestamp_unit_ns))) |ps| t = self.secToTs(ps.open);
                    }
                }
                floor = if (floor) |f| @min(f, t) else t;
                continue;
            }
            if (indicators.sessionLookbackTs(p, first_ts)) |cur| {
                var t = cur;
                if (indicators.needsPreviousSession(p.kind)) {
                    const idx = try self.binarySearch(cur);
                    if (idx > 0) {
                        const prev_ts = try self.readTimestampAt(idx - 1);
                        t = indicators.sessionStart(prev_ts, p.a, p.b);
                    }
                }
                floor = if (floor) |f| @min(f, t) else t;
            }
        }
        return floor;
    }

    fn emptyResult(self: *Self, cols: IndicatorColumns, specs: []const indicators.Spec, bucket: i64, allocator: std.mem.Allocator) !IndicatorResult {
        var src = try self.readSource(0, 0, cols, specs, bucket, allocator);
        defer src.deinit(allocator);
        return self.computeFromSource(&src, 0, specs, allocator);
    }

    const SourceWindow = struct { src: Source, warm: usize };

    /// Read the source rows for a window [start_ts, end_ts) plus warm-up
    /// (see indicatorsRange for the semantics).
    fn sourceForRange(self: *Self, start_ts: i64, end_ts: i64, cols: IndicatorColumns, specs: []const indicators.Spec, lookback: usize, bucket: i64, allocator: std.mem.Allocator) !SourceWindow {
        const lb = try resolveLookback(specs, lookback);
        const sess = try self.sessionFloor(specs, start_ts);
        if (bucket > 0) {
            if (end_ts <= start_ts) return .{ .src = try self.readSource(0, 0, cols, specs, bucket, allocator), .warm = 0 };
            const first_bucket: i128 = @as(i128, @divFloor(start_ts, bucket)) * bucket;
            const read_end = clampI64(@as(i128, @divFloor(end_ts - 1, bucket)) * bucket + bucket);
            const end_idx = try self.binarySearch(read_end);
            var span: i128 = @as(i128, @intCast(lb)) * bucket;
            var attempt: usize = 0;
            while (true) : (attempt += 1) {
                var read_start = clampI64(first_bucket - span);
                if (sess) |t| read_start = @min(read_start, t);
                const start_idx = try self.binarySearch(read_start);
                var src = try self.readSource(start_idx, end_idx, cols, specs, bucket, allocator);
                var warm = src.rowsBefore(start_ts);
                if (warm >= lb or start_idx == 0 or attempt >= 48) {
                    const keep = if (sess) |t| @max(lb, warm - src.rowsBefore(t)) else lb;
                    if (warm > keep) {
                        src.trimFront(warm - keep); // deterministic warm-up length
                        warm = keep;
                    }
                    return .{ .src = src, .warm = warm };
                }
                src.deinit(allocator);
                span = span * 2 + bucket;
            }
        }
        const end_idx = try self.binarySearch(end_ts);
        const win_start = try self.binarySearch(start_ts);
        var start_idx: u64 = if (win_start > lb) win_start - lb else 0;
        if (sess) |t| start_idx = @min(start_idx, try self.binarySearch(t));
        const src = try self.readSource(start_idx, @max(end_idx, start_idx), cols, specs, 0, allocator);
        return .{ .src = src, .warm = @intCast(win_start - start_idx) };
    }

    /// Read the source rows for the last `n_last` rows plus warm-up.
    fn sourceForTail(self: *Self, n_last: usize, cols: IndicatorColumns, specs: []const indicators.Spec, lookback: usize, bucket: i64, allocator: std.mem.Allocator) !SourceWindow {
        const lb = try resolveLookback(specs, lookback);
        const total = self.count();
        if (bucket > 0) {
            var src = try self.readTailBars(n_last + lb, cols, specs, bucket, allocator);
            if (src.rows() > 0) {
                const first_win = src.ts[if (src.rows() > n_last) src.rows() - n_last else 0];
                if (try self.sessionFloor(specs, first_win)) |t| {
                    if (src.ts[0] > t) {
                        src.deinit(allocator);
                        const start_idx = try self.binarySearch(t);
                        src = try self.readSource(start_idx, total, cols, specs, bucket, allocator);
                    }
                    const keep = @max(lb, src.rowsBefore(first_win));
                    if (src.rows() > n_last + keep) src.trimFront(src.rows() - (n_last + keep));
                } else if (src.rows() > n_last + lb) {
                    src.trimFront(src.rows() - (n_last + lb));
                }
            }
            const warm = if (src.rows() > n_last) src.rows() - n_last else 0;
            return .{ .src = src, .warm = warm };
        }
        const win_start: u64 = if (total > n_last) total - n_last else 0;
        var start_idx: u64 = if (win_start > lb) win_start - lb else 0;
        if (win_start < total) { // n_last == 0 has no window row to anchor sessions on
            const first_win = try self.readTimestampAt(win_start);
            if (try self.sessionFloor(specs, first_win)) |t| start_idx = @min(start_idx, try self.binarySearch(t));
        }
        const src = try self.readSource(start_idx, total, cols, specs, 0, allocator);
        return .{ .src = src, .warm = @intCast(win_start - start_idx) };
    }

    /// Compute a batch of indicators over [start_ts, end_ts). `lookback` extra
    /// rows before the window are read for warm-up (`lookback_auto` picks the
    /// recommended amount) and are not returned. Session-anchored kinds always
    /// see their session from its start.
    ///
    /// With `bucket` > 0 records are first aggregated into OHLCV bars of that
    /// timestamp width and the window selects the *bars whose timestamp lies
    /// in [start_ts, end_ts)*; every returned bar is complete, and the
    /// warm-up counts existing bars (gaps between sessions do not count).
    pub fn indicatorsRange(self: *Self, start_ts: i64, end_ts: i64, cols: IndicatorColumns, specs: []const indicators.Spec, lookback: usize, bucket: i64, allocator: std.mem.Allocator) !IndicatorResult {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        var w = try self.sourceForRange(start_ts, end_ts, cols, specs, lookback, bucket, allocator);
        defer w.src.deinit(allocator);
        return self.computeFromSource(&w.src, w.warm, specs, allocator);
    }

    /// Start index of the records covering at least `n_bars` bars of `bucket`
    /// width at the end of the database, extending the time span until enough
    /// bars exist or the file start is reached. Returns the source.
    fn readTailBars(self: *Self, n_bars: usize, cols: IndicatorColumns, specs: []const indicators.Spec, bucket: i64, allocator: std.mem.Allocator) !Source {
        const total = self.count();
        if (total == 0) return self.readSource(0, 0, cols, specs, bucket, allocator);
        const last_ts = try self.readTimestampAt(total - 1);
        const last_bucket_start: i128 = @as(i128, @divFloor(last_ts, bucket)) * bucket;
        var span: i128 = @as(i128, @intCast(@max(n_bars, 1))) * bucket;
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const start_idx = try self.binarySearch(clampI64(last_bucket_start + bucket - span));
            var src = try self.readSource(start_idx, total, cols, specs, bucket, allocator);
            if (src.rows() >= n_bars or start_idx == 0 or attempt >= 48) return src;
            src.deinit(allocator);
            span = span * 2 + bucket;
        }
    }

    /// Compute a batch of indicators for the last `n_last` records (or
    /// existing bars when `bucket` > 0), warming up over `lookback` earlier
    /// rows.
    pub fn indicatorsTail(self: *Self, n_last: usize, cols: IndicatorColumns, specs: []const indicators.Spec, lookback: usize, bucket: i64, allocator: std.mem.Allocator) !IndicatorResult {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        var w = try self.sourceForTail(n_last, cols, specs, lookback, bucket, allocator);
        defer w.src.deinit(allocator);
        return self.computeFromSource(&w.src, w.warm, specs, allocator);
    }

    // ---------------------------------------------------------------------
    // Pairs: indicators over two databases aligned on time
    // ---------------------------------------------------------------------

    /// Align `other` onto the rows of `w` (this database's source window):
    /// inner join on bar timestamps when `bucket` > 0, as-of join (latest
    /// `other` row at or before each row) on ticks. Returns the aligned second
    /// series and, for the inner join, compacts `w.src` to the matched rows.
    fn alignSecond(self: *Self, other: *Self, w: *SourceWindow, cols_b: IndicatorColumns, bucket: i64, allocator: std.mem.Allocator) ![]f64 {
        _ = self;
        const rows = w.src.rows();
        const second = try allocator.alloc(f64, rows);
        errdefer allocator.free(second);
        if (rows == 0) return second;
        const no_specs = [_]indicators.Spec{};
        const first_ts = w.src.ts[0];
        const last_ts = w.src.ts[rows - 1];
        const read_end = if (bucket > 0) clampI64(@as(i128, last_ts) + bucket) else clampI64(@as(i128, last_ts) + 1);
        var b_start = try other.binarySearch(first_ts);
        if (bucket == 0 and b_start > 0) b_start -= 1; // as-of needs the previous other row
        const b_end = try other.binarySearch(read_end);
        var srcb = try other.readSource(b_start, @max(b_end, b_start), cols_b, &no_specs, bucket, allocator);
        defer srcb.deinit(allocator);
        if (bucket == 0) {
            try indicators.alignAsOf(w.src.ts, srcb.ts, srcb.close, second);
            return second;
        }
        // inner join on bar timestamps: keep only matched rows of this source
        const idx_a = try allocator.alloc(usize, rows);
        defer allocator.free(idx_a);
        const idx_b = try allocator.alloc(usize, rows);
        defer allocator.free(idx_b);
        const k = indicators.alignInner(w.src.ts, srcb.ts, idx_a, idx_b);
        const window_first = if (w.warm < rows) w.src.ts[w.warm] else std.math.maxInt(i64);
        // compact in place (idx_a is increasing)
        const bars = &w.src.bars.?;
        var new_warm: usize = 0;
        for (0..k) |j| {
            const ia = idx_a[j];
            bars.ts[j] = bars.ts[ia];
            bars.open[j] = bars.open[ia];
            bars.high[j] = bars.high[ia];
            bars.low[j] = bars.low[ia];
            bars.close[j] = bars.close[ia];
            bars.volume[j] = bars.volume[ia];
            bars.count[j] = bars.count[ia];
            bars.buy_volume[j] = bars.buy_volume[ia];
            second[j] = srcb.close[idx_b[j]];
            if (bars.ts[j] < window_first) new_warm += 1;
        }
        w.src.ts = bars.ts[0..k];
        w.src.open = bars.open[0..k];
        w.src.high = bars.high[0..k];
        w.src.low = bars.low[0..k];
        w.src.close = bars.close[0..k];
        w.src.volume = bars.volume[0..k];
        if (w.src.buy_volume != null) w.src.buy_volume = bars.buy_volume[0..k];
        w.warm = new_warm;
        return second;
    }

    /// Indicators over this database (series A, its `cols`) aligned with
    /// `other` (series B, `cols_b`, whose close is the second input). Single
    /// series kinds run on A; `series2`, `ratio`, `ratio_zscore`,
    /// `rel_strength`, `correl` and `beta` use both.
    pub fn pairRange(self: *Self, other: *Self, cols: IndicatorColumns, cols_b: IndicatorColumns, start_ts: i64, end_ts: i64, specs: []const indicators.Spec, lookback: usize, bucket: i64, allocator: std.mem.Allocator) !IndicatorResult {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        try other.flush();
        var w = try self.sourceForRange(start_ts, end_ts, cols, specs, lookback, bucket, allocator);
        defer w.src.deinit(allocator);
        const second = try self.alignSecond(other, &w, cols_b, bucket, allocator);
        defer allocator.free(second);
        return self.computeFromSourcePair(&w.src, second[0..w.src.rows()], w.warm, specs, allocator);
    }

    /// Pair indicators for the last `n_last` *joined* rows: on bars the tail
    /// of this database is extended until `n_last` bars exist that the other
    /// database also has (or the file start is reached).
    pub fn pairTail(self: *Self, other: *Self, cols: IndicatorColumns, cols_b: IndicatorColumns, n_last: usize, specs: []const indicators.Spec, lookback: usize, bucket: i64, allocator: std.mem.Allocator) !IndicatorResult {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        try other.flush();
        var want = n_last;
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            var w = try self.sourceForTail(want, cols, specs, lookback, bucket, allocator);
            defer w.src.deinit(allocator);
            const rows_before = w.src.rows();
            const second = try self.alignSecond(other, &w, cols_b, bucket, allocator);
            defer allocator.free(second);
            const joined = w.src.rows() - w.warm;
            const exhausted = rows_before < want or attempt >= 40; // no more history in A
            if (bucket == 0 or joined >= n_last or exhausted) {
                // keep exactly the last n_last joined rows
                if (joined > n_last) w.warm += joined - n_last;
                return self.computeFromSourcePair(&w.src, second[0..w.src.rows()], w.warm, specs, allocator);
            }
            want = want * 2 + 1;
        }
    }

    /// Aggregate records in [start_ts, end_ts) into OHLCV bars of `bucket`
    /// timestamp units, using `price_field` (and `volume_field` if >= 0).
    pub fn ohlcv(self: *Self, start_ts: i64, end_ts: i64, price_field: usize, volume_field: i64, bucket: i64, allocator: std.mem.Allocator) !indicators.Bars {
        return self.ohlcvSide(start_ts, end_ts, price_field, volume_field, -1, bucket, allocator);
    }

    /// Like `ohlcv`; a `side_field` (>= 0, 1 = buy) additionally fills the
    /// bars' buy volume.
    pub fn ohlcvSide(self: *Self, start_ts: i64, end_ts: i64, price_field: usize, volume_field: i64, side_field: i64, bucket: i64, allocator: std.mem.Allocator) !indicators.Bars {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (price_field >= self.fields.len) return error.InvalidFieldIndex;
        if (volume_field >= 0 and volume_field >= self.fields.len) return error.InvalidFieldIndex;
        if (side_field >= 0 and side_field >= self.fields.len) return error.InvalidFieldIndex;
        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);
        var fields: [3]usize = .{ price_field, 0, 0 };
        var nf: usize = 1;
        var vi: ?usize = null;
        var si: ?usize = null;
        if (volume_field >= 0) {
            fields[nf] = @intCast(volume_field);
            vi = nf;
            nf += 1;
        }
        if (side_field >= 0) {
            fields[nf] = @intCast(side_field);
            si = nf;
            nf += 1;
        }
        const set = try self.readColumns(start_idx, @max(end_idx, start_idx), fields[0..nf], allocator);
        defer set.deinit();
        return indicators.resampleOhlcvSide(set.ts, null, null, null, set.cols[0], if (vi) |k| set.cols[k] else null, if (si) |k| set.cols[k] else null, bucket, allocator);
    }

    /// Data-quality statistics for a price (and optional volume) field over
    /// [start_ts, end_ts). `gap_threshold` (timestamp units) counts session
    /// breaks / outages, `outlier_threshold` flags |log returns| above it.
    pub fn health(self: *Self, start_ts: i64, end_ts: i64, price_field: usize, volume_field: i64, gap_threshold: i64, outlier_threshold: f64, allocator: std.mem.Allocator) !indicators.Health {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (price_field >= self.fields.len) return error.InvalidFieldIndex;
        if (volume_field >= 0 and volume_field >= self.fields.len) return error.InvalidFieldIndex;
        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);
        var fields: [2]usize = .{ price_field, 0 };
        var nf: usize = 1;
        if (volume_field >= 0) {
            fields[1] = @intCast(volume_field);
            nf = 2;
        }
        const set = try self.readColumns(start_idx, @max(end_idx, start_idx), fields[0..nf], allocator);
        defer set.deinit();
        return indicators.health(set.ts, set.cols[0], if (nf == 2) set.cols[1] else null, gap_threshold, outlier_threshold, self.tradingCalendar(), self.config.timestamp_unit_ns, allocator);
    }

    /// Evaluate directional decisions against `price_field` (see
    /// indicators.evaluate). Reads only the records spanning the decisions.
    pub fn evaluate(self: *Self, price_field: usize, decisions: []const indicators.Decision, default_horizon: i64, cost_bps: f64, out_entry: ?[]f64, out_exit: ?[]f64, out_net: ?[]f64, allocator: std.mem.Allocator) !indicators.Evaluation {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (price_field >= self.fields.len) return error.InvalidFieldIndex;
        var lo: i64 = std.math.maxInt(i64);
        var hi: i64 = std.math.minInt(i64);
        for (decisions) |d| {
            lo = @min(lo, d.timestamp);
            const h = if (d.horizon > 0) d.horizon else default_horizon;
            hi = @max(hi, clampI64(@as(i128, d.timestamp) + @as(i128, @max(h, 0)) + 1));
        }
        if (decisions.len == 0) {
            const empty = [_]i64{};
            const emptyf = [_]f64{};
            return indicators.evaluate(&empty, &emptyf, decisions, default_horizon, cost_bps, out_entry, out_exit, out_net);
        }
        const start_idx = try self.binarySearch(lo);
        // the exit price is the first record at or after the exit time: read one more record
        var end_idx = try self.binarySearch(hi);
        if (end_idx < self.count()) end_idx += 1;
        const set = try self.readColumns(start_idx, @max(end_idx, start_idx), &[_]usize{price_field}, allocator);
        defer set.deinit();
        return indicators.evaluate(set.ts, set.cols[0], decisions, default_horizon, cost_bps, out_entry, out_exit, out_net);
    }

    /// Snapshots for several bar sizes from one read: `out[k]` is the snapshot
    /// over the last `n_bars` bars of `buckets[k]`, annualised with `ppy[k]`.
    pub fn snapshotMulti(self: *Self, cols: IndicatorColumns, n_bars: usize, buckets: []const i64, ppy: []const f64, out: []indicators.Snapshot, allocator: std.mem.Allocator) !void {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (cols.close < 0) return error.MissingCloseColumn;
        if (buckets.len != out.len or ppy.len != out.len) return error.LengthMismatch;
        if (buckets.len == 0) return;
        const want: usize = if (n_bars == 0) indicators.snapshot_recommended_bars else n_bars;
        var max_bucket: i64 = 0;
        for (buckets) |b| {
            if (b <= 0) return error.InvalidParameter;
            max_bucket = @max(max_bucket, b);
        }
        const no_specs = [_]indicators.Spec{};
        var src = try self.readTailBars(want, cols, &no_specs, max_bucket, allocator);
        defer src.deinit(allocator);
        const fl = src.field_list[0..src.nf];
        const raw_open: ?[]const f64 = if (findCol(fl, cols.open)) |k| src.set.cols[k] else null;
        const raw_high: ?[]const f64 = if (findCol(fl, cols.high)) |k| src.set.cols[k] else null;
        const raw_low: ?[]const f64 = if (findCol(fl, cols.low)) |k| src.set.cols[k] else null;
        const raw_close: []const f64 = src.set.cols[findCol(fl, cols.close).?];
        const raw_volume: ?[]const f64 = if (findCol(fl, cols.volume)) |k| src.set.cols[k] else null;
        for (buckets, 0..) |b, k| {
            const bars = try indicators.resampleOhlcv(src.set.ts, raw_open, raw_high, raw_low, raw_close, raw_volume, b, allocator);
            defer bars.deinit(allocator);
            const n = bars.len();
            const from = if (n > want) n - want else 0;
            out[k] = try indicators.snapshot(bars.ts[from..], bars.open[from..], bars.high[from..], bars.low[from..], bars.close[from..], bars.volume[from..], self.effectivePpy(ppy[k], b, bars.ts), allocator);
        }
    }

    /// Backtest a target-position series over the bars of [start_ts, end_ts):
    /// exactly the rows indicatorsRange(start_ts, end_ts, bucket) returns
    /// (bucket > 0: bars whose start lies in the window, the last one built
    /// from all its records; equal to ohlcv(start_ts, end_ts, bucket) when
    /// both bounds are bucket-aligned; bucket 0: the raw records). `target`
    /// must have one entry per row: compute the signals with indicatorsRange
    /// over the same window and pass one target per returned row.
    /// Semantics: see backtest.zig. `periods_per_year` 0 is filled from the
    /// trading calendar when one is configured.
    pub fn backtest(self: *Self, cols: IndicatorColumns, start_ts: i64, end_ts: i64, bucket: i64, target: []const f64, params: backtest_mod.Params, out: backtest_mod.Outputs, trades: ?[]backtest_mod.Trade, allocator: std.mem.Allocator) !backtest_mod.Result {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (cols.close < 0) return error.MissingCloseColumn;
        const no_specs = [_]indicators.Spec{};
        var win = try self.sourceForRange(start_ts, end_ts, cols, &no_specs, 0, bucket, allocator);
        defer win.src.deinit(allocator);
        const src = &win.src;
        const from = @min(win.warm, src.rows());
        return self.backtestSource(src, from, bucket, target, params, out, trades, allocator);
    }

    /// Backtest over the last `n_bars` bars (bucket > 0) or records.
    pub fn backtestTail(self: *Self, cols: IndicatorColumns, n_bars: usize, bucket: i64, target: []const f64, params: backtest_mod.Params, out: backtest_mod.Outputs, trades: ?[]backtest_mod.Trade, allocator: std.mem.Allocator) !backtest_mod.Result {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (cols.close < 0) return error.MissingCloseColumn;
        const no_specs = [_]indicators.Spec{};
        var src = if (bucket > 0) try self.readTailBars(n_bars, cols, &no_specs, bucket, allocator) else blk: {
            const total = self.count();
            const start_idx: u64 = if (total > n_bars) total - n_bars else 0;
            break :blk try self.readSource(start_idx, total, cols, &no_specs, 0, allocator);
        };
        defer src.deinit(allocator);
        const n = src.rows();
        const from = if (n > n_bars) n - n_bars else 0;
        return self.backtestSource(&src, from, bucket, target, params, out, trades, allocator);
    }

    fn backtestSource(self: *Self, src: *const Source, from: usize, bucket: i64, target: []const f64, params_in: backtest_mod.Params, out: backtest_mod.Outputs, trades: ?[]backtest_mod.Trade, allocator: std.mem.Allocator) !backtest_mod.Result {
        const ts = src.ts[from..];
        if (target.len != ts.len) return error.LengthMismatch;
        var params = params_in;
        if (params.periods_per_year <= 0) params.periods_per_year = self.effectivePpy(0, bucket, ts);
        return backtest_mod.run(ts, if (src.open) |o| o[from..] else null, if (src.high) |h| h[from..] else null, if (src.low) |l| l[from..] else null, src.close[from..], target, params, out, trades, allocator);
    }

    /// Cross-sectional ("universe") features over several databases that
    /// share the same column roles: the last `n_bars` bars (bucket > 0; 0 =
    /// enough for the longest period) or records of every database are
    /// inner-joined on timestamps and fed to universe.compute (momentum /
    /// volatility ranks, correlation matrix, market factor, betas, breadth).
    /// `rows` must have one entry per database, `corr` (optional) n*n.
    /// Volumes are used when every database has a volume column.
    pub fn universe(dbs: []const *Self, cols: IndicatorColumns, n_bars: usize, bucket: i64, params: universe_mod.Params, rows: []universe_mod.Row, corr: ?[]f64, allocator: std.mem.Allocator) !universe_mod.Summary {
        const m = dbs.len;
        if (rows.len != m) return error.LengthMismatch;
        if (corr) |c| if (c.len != m * m) return error.LengthMismatch;
        if (m == 0) return std.mem.zeroes(universe_mod.Summary);
        if (cols.close < 0) return error.MissingCloseColumn;
        var read_timer = dbs[0].readStart();
        defer dbs[0].readEnd(&read_timer, 0);
        const longest = @max(@max(@max(params.mom_long, params.corr_period), @max(params.beta_period, params.sma_period)), @max(params.vol_period, @max(params.mom_short, params.mom_mid)));
        const need: usize = @intCast(@min(longest + 1, @as(u64, 1 << 20)));
        var want: usize = if (n_bars > 0) n_bars else need;
        const no_specs = [_]indicators.Spec{};
        const sources = try allocator.alloc(Source, m);
        defer allocator.free(sources);
        var loaded: usize = 0;
        defer for (sources[0..loaded]) |*src| src.deinit(allocator);
        // With n_bars = 0 the join may lose rows (bars missing in some databases):
        // read more until `need` joined rows exist or the data is exhausted.
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            for (sources[0..loaded]) |*src| src.deinit(allocator);
            loaded = 0;
            var all_volume = cols.volume >= 0;
            var exhausted = true;
            for (dbs, 0..) |db, i| {
                try db.flush();
                if (bucket > 0) {
                    sources[i] = try db.readTailBars(want, cols, &no_specs, bucket, allocator);
                } else {
                    const total = db.count();
                    const start_idx: u64 = if (total > want) total - want else 0;
                    sources[i] = try db.readSource(start_idx, total, cols, &no_specs, 0, allocator);
                }
                loaded += 1;
                // keep only the last `want` rows of each source
                const n = sources[i].rows();
                if (n >= want) exhausted = false;
                if (n > want) {
                    const from = n - want;
                    sources[i].ts = sources[i].ts[from..];
                    sources[i].close = sources[i].close[from..];
                    if (sources[i].volume) |v| sources[i].volume = v[from..];
                }
                if (sources[i].volume == null) all_volume = false;
            }
            var min_rows: usize = std.math.maxInt(usize);
            for (sources) |*src| min_rows = @min(min_rows, src.rows());
            const joined_rows = try joinSources(sources, min_rows, all_volume, allocator);
            defer joined_rows.deinit(allocator);
            if (n_bars == 0 and joined_rows.n < need and !exhausted and attempt < 6) {
                want *= 2;
                continue;
            }
            const joined = joined_rows.n;
            const closes = try allocator.alloc([]const f64, m);
            defer allocator.free(closes);
            const volumes = if (all_volume) try allocator.alloc([]const f64, m) else null;
            defer if (volumes) |v| allocator.free(v);
            for (0..m) |i| {
                closes[i] = joined_rows.close[i * min_rows .. i * min_rows + joined];
                if (volumes) |v| v[i] = joined_rows.volume.?[i * min_rows .. i * min_rows + joined];
            }
            return universe_mod.compute(closes, volumes, joined_rows.ts[0..joined], params, rows, corr, allocator);
        }
    }

    const JoinedRows = struct {
        ts: []i64,
        close: []f64,
        volume: ?[]f64,
        n: usize,

        fn deinit(self: JoinedRows, allocator: std.mem.Allocator) void {
            allocator.free(self.ts);
            allocator.free(self.close);
            if (self.volume) |v| allocator.free(v);
        }
    };

    /// k-way inner join of sorted sources on timestamps (close / volume laid
    /// out per source with stride `stride`).
    fn joinSources(sources: []Source, stride: usize, with_volume: bool, allocator: std.mem.Allocator) !JoinedRows {
        const m = sources.len;
        const idx = try allocator.alloc(usize, m);
        defer allocator.free(idx);
        @memset(idx, 0);
        const jts = try allocator.alloc(i64, stride);
        errdefer allocator.free(jts);
        const jclose = try allocator.alloc(f64, m * stride);
        errdefer allocator.free(jclose);
        const jvol = if (with_volume) try allocator.alloc(f64, m * stride) else null;
        errdefer if (jvol) |v| allocator.free(v);
        var joined: usize = 0;
        outer: while (true) {
            var maxts: i64 = std.math.minInt(i64);
            for (sources, 0..) |*src, i| {
                if (idx[i] >= src.rows()) break :outer;
                maxts = @max(maxts, src.ts[idx[i]]);
            }
            var all_equal = true;
            for (sources, 0..) |*src, i| {
                while (idx[i] < src.rows() and src.ts[idx[i]] < maxts) idx[i] += 1;
                if (idx[i] >= src.rows()) break :outer;
                if (src.ts[idx[i]] != maxts) all_equal = false;
            }
            if (!all_equal) continue;
            jts[joined] = maxts;
            for (sources, 0..) |*src, i| {
                jclose[i * stride + joined] = src.close[idx[i]];
                if (jvol) |v| v[i * stride + joined] = src.volume.?[idx[i]];
                idx[i] += 1;
            }
            joined += 1;
        }
        return .{ .ts = jts, .close = jclose, .volume = jvol, .n = joined };
    }

    /// Scalar risk / performance summary of a field over [start_ts, end_ts).
    pub fn summary(self: *Self, start_ts: i64, end_ts: i64, field_index: usize, periods_per_year: f64, allocator: std.mem.Allocator) !indicators.Summary {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (field_index >= self.fields.len) return error.InvalidFieldIndex;
        const start_idx = try self.binarySearch(start_ts);
        const end_idx = try self.binarySearch(end_ts);
        const set = try self.readColumns(start_idx, end_idx, &[_]usize{field_index}, allocator);
        defer set.deinit();
        return indicators.summary(set.cols[0], self.effectivePpy(periods_per_year, 0, set.ts), allocator);
    }

    /// One-shot snapshot of ~100 indicators for the latest bar, computed from
    /// the last `n_bars` records (0 = recommended) or, with `bucket` > 0, the
    /// last n_bars existing bars.
    pub fn snapshot(self: *Self, cols: IndicatorColumns, n_bars: usize, bucket: i64, periods_per_year: f64, allocator: std.mem.Allocator) !indicators.Snapshot {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();
        if (cols.close < 0) return error.MissingCloseColumn;
        const want: usize = if (n_bars == 0) indicators.snapshot_recommended_bars else n_bars;
        const total = self.count();
        const no_specs = [_]indicators.Spec{};
        if (bucket > 0) {
            var src = try self.readTailBars(want, cols, &no_specs, bucket, allocator);
            defer src.deinit(allocator);
            const n = src.rows();
            const from = if (n > want) n - want else 0;
            return indicators.snapshot(src.ts[from..], if (src.open) |o| o[from..] else null, if (src.high) |h| h[from..] else null, if (src.low) |l| l[from..] else null, src.close[from..], if (src.volume) |v| v[from..] else null, self.effectivePpy(periods_per_year, bucket, src.ts), allocator);
        }
        const start_idx: u64 = if (total > want) total - want else 0;
        var src = try self.readSource(start_idx, total, cols, &no_specs, 0, allocator);
        defer src.deinit(allocator);
        return indicators.snapshot(src.ts, src.open, src.high, src.low, src.close, src.volume, self.effectivePpy(periods_per_year, 0, src.ts), allocator);
    }

    pub fn load(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        var read_timer = self.readStart();
        defer self.readEnd(&read_timer, 0);
        try self.flush();

        // For ring buffers, use query to get logical order (oldest to newest)
        if (self.is_wrapped) {
            return self.query(std.math.minInt(i64), std.math.maxInt(i64), &[_]Filter{}, allocator);
        }

        // For linear mode, direct read is faster and preserves order
        const stat = try self.file.stat();
        if (stat.size <= self.header_size) return allocator.alloc(u8, 0);

        const data_size = stat.size - self.header_size;
        const buf = try allocator.alloc(u8, data_size);
        errdefer allocator.free(buf);

        const len = try self.file.preadAll(buf, self.header_size);
        if (len != data_size) return error.UnexpectedEndOfFile;
        self.metrics.records_read += data_size / self.record_size;
        return buf;
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
            try dynamic_db_ptr.initWriter();

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
        // With relaxed schema check, it tries to read but fails on record alignment
        if (WrongDB.init(ticker, dir, std.testing.allocator, .{})) |db_val| {
            var db = db_val;
            db.deinit();
            return error.TestExpectedError;
        } else |err| {
            if (err != error.CorruptedData and err != error.SchemaMismatch) return err;
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
    _ = @import("test_auto_increment.zig");
    _ = @import("test_integrity.zig");
    _ = @import("test_indicators.zig");
    _ = @import("test_indicators_golden.zig");
    _ = @import("test_indicators_db.zig");
    _ = @import("test_storage.zig");
    _ = @import("calendar.zig");
    _ = @import("test_calendar_db.zig");
    _ = @import("universe.zig");
    _ = @import("test_universe.zig");
    _ = @import("test_universe_db.zig");
    _ = @import("backtest.zig");
    _ = @import("test_backtest.zig");
    _ = @import("test_backtest_db.zig");
}
