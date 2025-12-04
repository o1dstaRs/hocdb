const std = @import("std");
const hocdb = @import("hocdb");

const BenchRecord = struct {
    timestamp: i64,
    usd: f64,
    volume: f64,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    const DB = hocdb.TimeSeriesDB(BenchRecord);
    const ticker = "BENCH_MARK";
    const data_dir = "bench_data";

    // Cleanup
    std.fs.cwd().deleteTree(data_dir) catch {};
    defer std.fs.cwd().deleteTree(data_dir) catch {};

    try stdout.print("Running HOCDB Benchmark...\n", .{});
    try stdout.print("Record Size: {d} bytes\n", .{@sizeOf(BenchRecord)});

    // --- WRITE BENCHMARK ---
    var total_records: usize = 0;
    {
        var db = try DB.init(ticker, data_dir);
        defer db.deinit();

        var latencies = try allocator.alloc(u64, 20_000_000);
        defer allocator.free(latencies);
        var latency_count: usize = 0;

        const duration_ns = 30 * std.time.ns_per_s;

        try stdout.print("Starting Write Benchmark...\n", .{});
        try stdout.print("Duration: 30 seconds\n", .{});
        try stdout.print("Target: As many writes as possible\n\n", .{});
        try stdout.flush();

        var timer = try std.time.Timer.start();
        const start_time = timer.read();

        var i: usize = 0;
        var last_print_ns: u64 = 0;
        const print_interval_ns = 100 * std.time.ns_per_ms; // Update every 100ms

        while (true) : (i += 1) {
            const now = timer.read();
            const elapsed = now - start_time;
            if (elapsed >= duration_ns) break;

            // Progress Update
            if (now - last_print_ns >= print_interval_ns) {
                const percent = (elapsed * 100) / duration_ns;
                try stdout.print("\rProgress: {d}%...", .{percent});
                try stdout.flush();
                last_print_ns = now;
            }

            const op_start = timer.read();
            try db.append(.{
                .timestamp = @intCast(i),
                .usd = @floatFromInt(i),
                .volume = @floatFromInt(i),
            });
            const op_end = timer.read();

            if (latency_count < latencies.len) {
                latencies[latency_count] = op_end - op_start;
                latency_count += 1;
            }
        }
        try stdout.print("\rProgress: 100%...\n", .{});
        total_records = i;

        const total_time_ns = timer.read() - start_time;
        const total_time_s = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(total_records)) / total_time_s;
        const mb_per_sec = (ops_per_sec * @sizeOf(BenchRecord)) / (1024 * 1024);

        // Calculate Latency Stats
        const items = latencies[0..latency_count];
        std.mem.sort(u64, items, {}, std.sort.asc(u64));

        var sum: u128 = 0;
        for (items) |lat| sum += lat;
        const mean = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(items.len));

        const p50 = items[items.len / 2];
        const p90 = items[(items.len * 90) / 100];
        const p99 = items[(items.len * 99) / 100];

        try stdout.print("\n[WRITE] {d} records in {d:.2}s\n", .{ total_records, total_time_s });
        try stdout.print("Throughput: {d:.2} ops/sec\n", .{ops_per_sec});
        try stdout.print("Bandwidth:  {d:.2} MB/sec\n", .{mb_per_sec});
        try stdout.print("Latency:\n", .{});
        try stdout.print("  Mean: {d:.2} ns\n", .{mean});
        try stdout.print("  p50:  {d} ns\n", .{p50});
        try stdout.print("  p90:  {d} ns\n", .{p90});
        try stdout.print("  p99:  {d} ns\n", .{p99});
    }

    // --- READ & AGGREGATE BENCHMARK ---
    {
        var db = try DB.init(ticker, data_dir);
        defer db.deinit();

        var timer = try std.time.Timer.start();
        const start = timer.read();

        const data = try db.load(allocator);
        defer allocator.free(data);

        const load_end = timer.read();
        const load_time_s = @as(f64, @floatFromInt(load_end - start)) / 1_000_000_000.0;

        try stdout.print("\n[READ/LOAD] {d} records\n", .{data.len});
        try stdout.print("Time: {d:.4}s\n", .{load_time_s});
        const load_ops_per_sec = @as(f64, @floatFromInt(total_records)) / load_time_s;
        const load_mb_per_sec = (load_ops_per_sec * @sizeOf(BenchRecord)) / (1024 * 1024);
        try stdout.print("Throughput: {d:.2} ops/sec\n", .{load_ops_per_sec});
        try stdout.print("Bandwidth:  {d:.2} MB/sec\n", .{load_mb_per_sec});

        // --- AGGREGATION ---
        const agg_start = timer.read();

        var frame_count: usize = 0;
        var i: usize = 0;
        const frame_size = 1000;

        // Prevent compiler optimization
        var total_volume_checksum: f64 = 0;

        while (i + frame_size <= data.len) : (i += frame_size) {
            const frame = data[i .. i + frame_size];
            var usd_sum: f64 = 0;
            var vol_sum: f64 = 0;

            for (frame) |record| {
                usd_sum += record.usd;
                vol_sum += record.volume;
            }

            const usd_mean = usd_sum / @as(f64, @floatFromInt(frame_size));
            total_volume_checksum += vol_sum + usd_mean; // Use values
            frame_count += 1;
        }

        const agg_end = timer.read();
        const agg_time_ns = agg_end - agg_start;
        const agg_time_s = @as(f64, @floatFromInt(agg_time_ns)) / 1_000_000_000.0;
        const frames_per_sec = @as(f64, @floatFromInt(frame_count)) / agg_time_s;
        const records_per_sec = @as(f64, @floatFromInt(frame_count * frame_size)) / agg_time_s;

        try stdout.print("\n[AGGREGATION] {d} frames (1000 records each)\n", .{frame_count});
        try stdout.print("Time: {d:.6}s\n", .{agg_time_s});
        try stdout.print("Throughput: {d:.2} frames/sec\n", .{frames_per_sec});
        try stdout.print("Processing: {d:.2} records/sec\n", .{records_per_sec});
        try stdout.print("Checksum:   {d:.2}\n", .{total_volume_checksum});
    }
    try stdout.flush();
}
