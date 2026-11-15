//! Non-interactive CLI over the existing storage API. Reads never open writers.
const std = @import("std");
const hocdb = @import("hocdb");
const io = hocdb.runtime.io;
const DB = hocdb.DynamicTimeSeriesDB;
const Allocator = std.mem.Allocator;
const version = @import("build_options").version;
const input_limit = 16 * 1024 * 1024;

const Manifest = struct {
    format_version: u32 = 1,
    ticker: []const u8 = "series",
    timestamp_unit: enum { s, ms, us, ns } = .s,
    fields: []const hocdb.FieldInfo,

    fn unitNs(self: Manifest) u64 {
        return switch (self.timestamp_unit) {
            .s => 1_000_000_000,
            .ms => 1_000_000,
            .us => 1_000,
            .ns => 1,
        };
    }
};

const ticks = [_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "price", .type = .f64 },
    .{ .name = "volume", .type = .f64 },
};
const ohlcv = [_]hocdb.FieldInfo{
    .{ .name = "timestamp", .type = .i64 },
    .{ .name = "open", .type = .f64 },
    .{ .name = "high", .type = .f64 },
    .{ .name = "low", .type = .f64 },
    .{ .name = "close", .type = .f64 },
    .{ .name = "volume", .type = .f64 },
};

const help =
    \\HOCDB — embedded time-series storage for applications and agents
    \\
    \\Usage: hocdb <command> [options]
    \\  init      --db DIR [--preset ticks|ohlcv | --schema FILE] [--timestamp-unit s|ms|us|ns]
    \\  append    --db DIR [--input FILE|-]  JSON object or array; stdin by default
    \\  info      --db DIR
    \\  query     --db DIR [--start N] [--end N] [--limit N]
    \\  stats     --db DIR [--field NAME] [--start N] [--end N]
    \\  snapshot  --db DIR [--bars N] [--bucket N] [--periods-per-year N]
    \\  health    --db DIR [--start N] [--end N] [--gap-threshold N]
    \\  describe  Machine-readable command contract (JSON)
    \\  --version Print version (JSON)
    \\
    \\Commands return JSON on stdout; failures return JSON on stderr and exit 1.
    \\No command deletes data. init requires a NEW directory. Reads are read-only.
    \\Ranges include start and exclude end, in the database's timestamp unit.
    \\query defaults to 100 rows; too many rows is an error (maximum limit 10000).
    \\Integers outside JavaScript's safe range are decimal strings; NaN becomes null.
    \\
    \\Quickstart:
    \\  hocdb init --db ./market --preset ticks
    \\  echo '{"timestamp":1700000000,"price":100,"volume":2}' | hocdb append --db ./market
    \\  hocdb query --db ./market
    \\  hocdb snapshot --db ./market
    \\Docs: https://github.com/o1dstaRs/hocdb/blob/main/docs/AGENT_GUIDE.md
    \\
;

const Options = struct {
    args: []const [:0]const u8,

    fn get(self: Options, key: []const u8) ?[]const u8 {
        var i: usize = 0;
        while (i < self.args.len) : (i += 2) {
            if (std.mem.eql(u8, self.args[i], key)) return self.args[i + 1];
        }
        return null;
    }

    fn integer(self: Options, comptime T: type, key: []const u8, default: T) !T {
        const raw = self.get(key) orelse return default;
        return std.fmt.parseInt(T, raw, 10) catch error.InvalidNumber;
    }

    fn validate(self: Options, allowed: []const []const u8) !void {
        var i: usize = 0;
        while (i < self.args.len) : (i += 2) {
            if (i + 1 >= self.args.len) return error.MissingOptionValue;
            var found = false;
            for (allowed) |name| {
                if (std.mem.eql(u8, self.args[i], name)) found = true;
            }
            if (!found) return error.UnknownOption;
            var j: usize = 0;
            while (j < i) : (j += 2) {
                if (std.mem.eql(u8, self.args[i], self.args[j])) return error.DuplicateOption;
            }
        }
    }
};

fn respond(command: []const u8, data: anytype) !void {
    var buffer: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(io, &buffer);
    try std.json.Stringify.value(.{ .ok = true, .command = command, .data = data }, .{
        .emit_nonportable_numbers_as_strings = true,
    }, &output.interface);
    try output.interface.writeByte('\n');
    try output.interface.flush();
}

pub fn reportError(err: anyerror) !void {
    const message: []const u8 = switch (err) {
        error.PathAlreadyExists => "The destination already exists. Choose a new --db directory; existing data is never replaced.",
        error.FileNotFound => "Database or input file not found. Use init for a new database and check --db/--input/--schema paths.",
        error.DatabaseLocked => "Another writer has this database open. Retry after it closes; read commands may still be used.",
        error.BufferTooSmall => "Query exceeds --limit. Narrow --start/--end or increase --limit (maximum 10000).",
        error.InvalidRecord => "Records must have exactly the schema fields with matching types and finite numbers. Run info to inspect the schema.",
        error.TimestampOutOfOrder => "Append timestamps must be strictly increasing, including relative to stored data. No input records were appended.",
        error.InvalidSchema => "Schema needs unique nonempty field names and an i64 timestamp first (maximum 64 fields).",
        error.InvalidManifest => "Unsupported or invalid hocdb.json. Keep this manifest with its database; do not edit its schema.",
        error.MissingDatabase => "Provide --db DIR. Run hocdb --help for examples.",
        error.StreamTooLong => "JSON input exceeds the 16 MiB limit. Split ingestion into smaller batches.",
        error.InvalidRange => "--start must be less than --end. Ranges include start and exclude end.",
        error.InvalidNumber => "Invalid numeric option or value. Run describe for bounds and timestamp units.",
        error.EmptyDB => "The database has no records. Append records before requesting this operation.",
        error.FileFull => "The database is full. CLI databases never overwrite earlier records; use another database directory.",
        else => "Command failed. Run hocdb --help or hocdb describe to check options, schema and limits.",
    };
    var buffer: [4096]u8 = undefined;
    var output = std.Io.File.stderr().writer(io, &buffer);
    try std.json.Stringify.value(.{ .ok = false, .@"error" = .{
        .code = @errorName(err),
        .message = message,
    } }, .{}, &output.interface);
    try output.interface.writeByte('\n');
    try output.interface.flush();
}

fn validateSchema(fields: []const hocdb.FieldInfo) !void {
    if (fields.len == 0 or fields.len > 64 or fields[0].type != .i64 or
        !std.mem.eql(u8, fields[0].name, "timestamp")) return error.InvalidSchema;
    for (fields, 0..) |field, i| {
        if (field.name.len == 0 or field.name.len > 128) return error.InvalidSchema;
        for (fields[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, field.name)) return error.InvalidSchema;
        }
    }
}

fn readInput(a: Allocator, path: []const u8) ![]const u8 {
    if (!std.mem.eql(u8, path, "-"))
        return std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(input_limit));
    var buffer: [4096]u8 = undefined;
    var input = std.Io.File.stdin().readerStreaming(io, &buffer);
    return input.interface.allocRemaining(a, .limited(input_limit));
}

fn fieldIndex(fields: []const hocdb.FieldInfo, name: []const u8) !usize {
    for (fields, 0..) |field, i| {
        if (std.mem.eql(u8, field.name, name)) return i;
    }
    return error.UnknownField;
}

fn priceIndex(fields: []const hocdb.FieldInfo) !usize {
    const index = fieldIndex(fields, "close") catch try fieldIndex(fields, "price");
    if (fields[index].type == .string) return error.InvalidFieldType;
    return index;
}

fn columns(fields: []const hocdb.FieldInfo) !DB.IndicatorColumns {
    var result: DB.IndicatorColumns = .{};
    inline for (@typeInfo(DB.IndicatorColumns).@"struct".field_names) |name| {
        @field(result, name) = if (fieldIndex(fields, name)) |i| @intCast(i) else |_| -1;
        if (@field(result, name) >= 0 and fields[@intCast(@field(result, name))].type == .string)
            return error.InvalidFieldType;
    }
    if (result.close < 0) result.close = @intCast(try priceIndex(fields));
    return result;
}

// All analytics structs contain scalars. Undefined indicator warm-up is JSON null.
fn Analytics(comptime T: type) type {
    return struct {
        value: T,
        pub fn jsonStringify(self: @This(), j: *std.json.Stringify) !void {
            try j.beginObject();
            inline for (@typeInfo(T).@"struct".field_names) |name| {
                try j.objectField(name);
                const value = @field(self.value, name);
                if (@typeInfo(@TypeOf(value)) == .float) {
                    try j.write(if (std.math.isFinite(value)) @as(?f64, value) else null);
                } else try j.write(value);
            }
            try j.endObject();
        }
    };
}

fn analytics(value: anytype) Analytics(@TypeOf(value)) {
    return .{ .value = value };
}

const Records = struct {
    bytes: []const u8,
    fields: []const hocdb.FieldInfo,
    size: usize,

    pub fn jsonStringify(self: Records, j: *std.json.Stringify) !void {
        try j.beginArray();
        var index: usize = 0;
        while (index < self.bytes.len) : (index += self.size) {
            try j.beginObject();
            var offset = index;
            for (self.fields) |field| {
                const raw = self.bytes[offset..][0..field.type.size()];
                try j.objectField(field.name);
                switch (field.type) {
                    .i64 => try j.write(std.mem.readInt(i64, raw[0..8], .little)),
                    .u64 => try j.write(std.mem.readInt(u64, raw[0..8], .little)),
                    .f64 => {
                        const value: f64 = @bitCast(std.mem.readInt(u64, raw[0..8], .little));
                        try j.write(if (std.math.isFinite(value)) @as(?f64, value) else null);
                    },
                    .u8 => try j.write(raw[0]),
                    .bool => try j.write(raw[0] != 0),
                    .string => try j.write(raw[0 .. std.mem.indexOfScalar(u8, raw, 0) orelse raw.len]),
                }
                offset += raw.len;
            }
            try j.endObject();
        }
        try j.endArray();
    }
};

fn jsonInt(comptime T: type, value: std.json.Value) !T {
    return switch (value) {
        .integer => |v| std.math.cast(T, v) orelse error.InvalidRecord,
        .number_string, .string => |v| std.fmt.parseInt(T, v, 10) catch error.InvalidRecord,
        else => error.InvalidRecord,
    };
}

fn encodeRecord(record: std.json.Value, fields: []const hocdb.FieldInfo, bytes: []u8) !i64 {
    if (record != .object or record.object.count() != fields.len) return error.InvalidRecord;
    var offset: usize = 0;
    for (fields) |field| {
        const value = record.object.get(field.name) orelse return error.InvalidRecord;
        const raw = bytes[offset..][0..field.type.size()];
        switch (field.type) {
            .i64 => std.mem.writeInt(i64, raw[0..8], try jsonInt(i64, value), .little),
            .u64 => std.mem.writeInt(u64, raw[0..8], try jsonInt(u64, value), .little),
            .u8 => raw[0] = try jsonInt(u8, value),
            .f64 => {
                const number: f64 = switch (value) {
                    .float => |v| v,
                    .integer => |v| @floatFromInt(v),
                    .number_string => |v| std.fmt.parseFloat(f64, v) catch return error.InvalidRecord,
                    else => return error.InvalidRecord,
                };
                if (!std.math.isFinite(number)) return error.InvalidRecord;
                std.mem.writeInt(u64, raw[0..8], @bitCast(number), .little);
            },
            .bool => {
                if (value != .bool) return error.InvalidRecord;
                raw[0] = @intFromBool(value.bool);
            },
            .string => {
                if (value != .string or value.string.len > 128 or std.mem.indexOfScalar(u8, value.string, 0) != null) return error.InvalidRecord;
                @memset(raw, 0);
                @memcpy(raw[0..value.string.len], value.string);
            },
        }
        offset += raw.len;
    }
    return std.mem.readInt(i64, bytes[0..8], .little);
}

pub fn run(a: Allocator, args: []const [:0]const u8) !void {
    if (args.len == 0 or std.mem.eql(u8, args[0], "--help") or std.mem.eql(u8, args[0], "help") or
        (args.len == 2 and std.mem.eql(u8, args[1], "--help")))
    {
        try std.Io.File.stdout().writeStreamingAll(io, help);
        return;
    }
    const command = args[0];
    const opt: Options = .{ .args = args[1..] };
    if (std.mem.eql(u8, command, "--version")) {
        try opt.validate(&.{});
        return respond("version", .{ .version = version, .storage_format = "HOC2", .cli_contract = 1 });
    }
    if (std.mem.eql(u8, command, "describe")) {
        try opt.validate(&.{});
        const contract = try std.json.parseFromSlice(std.json.Value, a, @embedFile("cli-schema.json"), .{});
        return respond(command, contract.value);
    }
    const Command = enum { init, append, info, query, stats, snapshot, health };
    const cmd = std.meta.stringToEnum(Command, command) orelse return error.UnknownCommand;
    try opt.validate(switch (cmd) {
        .init => &.{ "--db", "--preset", "--schema", "--timestamp-unit" },
        .append => &.{ "--db", "--input" },
        .info => &.{"--db"},
        .query => &.{ "--db", "--start", "--end", "--limit" },
        .stats => &.{ "--db", "--start", "--end", "--field" },
        .snapshot => &.{ "--db", "--bars", "--bucket", "--periods-per-year" },
        .health => &.{ "--db", "--start", "--end", "--gap-threshold" },
    });
    const path = opt.get("--db") orelse return error.MissingDatabase;
    if (path.len == 0) return error.MissingDatabase;
    const manifest_path = try std.fs.path.join(a, &.{ path, "hocdb.json" });

    if (cmd == .init) {
        if (opt.get("--preset") != null and opt.get("--schema") != null) return error.ConflictingOptions;
        var manifest: Manifest = .{ .fields = &ticks };
        if (opt.get("--schema")) |schema_path| {
            const parsed = try std.json.parseFromSlice([]const hocdb.FieldInfo, a, try readInput(a, schema_path), .{});
            manifest.fields = parsed.value;
        } else if (opt.get("--preset")) |preset| {
            if (std.mem.eql(u8, preset, "ohlcv")) manifest.fields = &ohlcv else if (!std.mem.eql(u8, preset, "ticks")) return error.UnknownPreset;
        }
        if (opt.get("--timestamp-unit")) |unit| {
            manifest.timestamp_unit = std.meta.stringToEnum(@TypeOf(manifest.timestamp_unit), unit) orelse return error.InvalidTimestampUnit;
        }
        try validateSchema(manifest.fields);
        const manifest_json = try std.json.Stringify.valueAlloc(a, manifest, .{ .whitespace = .indent_2 });
        // Fail for any existing destination before touching a file.
        try std.Io.Dir.cwd().createDir(io, path, .default_dir);
        var db = try DB.init("series", path, a, .{ .fields = manifest.fields }, .{
            .overwrite_on_full = false,
            .fsync = .on_flush,
            .timestamp_unit_ns = manifest.unitNs(),
        });
        defer db.deinit();
        try db.initWriter();
        try db.flush();
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = manifest_path, .data = manifest_json, .flags = .{ .exclusive = true } });
        return respond(command, manifest);
    }

    const raw_manifest = try std.Io.Dir.cwd().readFileAlloc(io, manifest_path, a, .limited(64 * 1024));
    const parsed = try std.json.parseFromSlice(Manifest, a, raw_manifest, .{});
    const manifest = parsed.value;
    if (manifest.format_version != 1 or !std.mem.eql(u8, manifest.ticker, "series")) return error.InvalidManifest;
    try validateSchema(manifest.fields);
    const schema: hocdb.Schema = .{ .fields = manifest.fields };
    const start = try opt.integer(i64, "--start", std.math.minInt(i64));
    const end = try opt.integer(i64, "--end", std.math.maxInt(i64));
    if (start >= end) return error.InvalidRange;

    // Append must never recreate a missing data file.
    const data_path = try std.fs.path.join(a, &.{ path, "series.bin" });
    const existing = try std.Io.Dir.cwd().openFile(io, data_path, .{});
    existing.close(io);
    var db = if (cmd == .append)
        try DB.init("series", path, a, schema, .{ .overwrite_on_full = false, .fsync = .on_flush, .auto_migrate = false, .timestamp_unit_ns = manifest.unitNs() })
    else
        try DB.openReader("series", path, a, schema);
    defer db.deinit();
    if (cmd == .append) try db.initWriter();

    switch (cmd) {
        .init => unreachable,
        .info => return respond(command, .{ .version = version, .schema = manifest, .records = db.count(), .read_only = true, .storage_format = "HOC2" }),
        .append => {
            const input = try readInput(a, opt.get("--input") orelse "-");
            const records = try std.json.parseFromSlice(std.json.Value, a, input, .{
                .allocate = .alloc_always,
                .parse_numbers = false, // preserve numeric u64 and nanosecond timestamps exactly
            });
            const items: []const std.json.Value = switch (records.value) {
                .array => |array| array.items,
                .object => &.{records.value},
                else => return error.InvalidRecord,
            };
            const size = schema.recordSize();
            const bytes = try a.alloc(u8, try std.math.mul(usize, items.len, size));
            var previous = db.last_timestamp;
            // Validate the whole batch before appending any record.
            for (items, 0..) |record, i| {
                const ts = try encodeRecord(record, manifest.fields, bytes[i * size ..][0..size]);
                if (ts < 0 or ts == std.math.maxInt(i64)) return error.InvalidRecord; // Unix epoch; exclusive upper bound
                if (previous) |last| {
                    if (ts <= last) return error.TimestampOutOfOrder;
                }
                previous = ts;
            }
            if (bytes.len > db.max_file_size - db.header_size - db.count() * size) return error.FileFull;
            for (0..items.len) |i| try db.append(bytes[i * size ..][0..size]);
            try db.flush();
            return respond(command, .{ .appended = items.len, .records = db.count() });
        },
        .query => {
            const limit = try opt.integer(usize, "--limit", 100);
            if (limit == 0 or limit > 10000) return error.InvalidNumber;
            const bytes = try a.alloc(u8, limit * schema.recordSize());
            const size = try db.queryInto(start, end, &.{}, bytes);
            return respond(command, .{ .count = size / schema.recordSize(), .records = Records{ .bytes = bytes[0..size], .fields = manifest.fields, .size = schema.recordSize() } });
        },
        .stats => {
            const field = if (opt.get("--field")) |name| try fieldIndex(manifest.fields, name) else try priceIndex(manifest.fields);
            if (manifest.fields[field].type == .string) return error.InvalidFieldType;
            const stats = try db.getStats(start, end, field, false);
            return respond(command, .{ .count = stats.count, .min = if (stats.count > 0) @as(?f64, stats.min) else null, .max = if (stats.count > 0) @as(?f64, stats.max) else null, .sum = if (std.math.isFinite(stats.sum)) @as(?f64, stats.sum) else null, .mean = if (stats.count > 0 and std.math.isFinite(stats.mean)) @as(?f64, stats.mean) else null });
        },
        .snapshot => {
            const bars = try opt.integer(usize, "--bars", 2500);
            const bucket = try opt.integer(i64, "--bucket", 0);
            const ppy = if (opt.get("--periods-per-year")) |v| std.fmt.parseFloat(f64, v) catch return error.InvalidNumber else 0;
            if (bars == 0 or bars > 100000 or bucket < 0 or !std.math.isFinite(ppy) or ppy < 0) return error.InvalidNumber;
            return respond(command, analytics(try db.snapshot(try columns(manifest.fields), bars, bucket, ppy, a)));
        },
        .health => {
            const gap = try opt.integer(i64, "--gap-threshold", 0);
            if (gap < 0) return error.InvalidNumber;
            const volume: i64 = if (fieldIndex(manifest.fields, "volume")) |v| @intCast(v) else |_| -1;
            if (volume >= 0 and manifest.fields[@intCast(volume)].type == .string) return error.InvalidFieldType;
            return respond(command, analytics(try db.health(start, end, try priceIndex(manifest.fields), volume, gap, 0.1, a)));
        },
    }
}
