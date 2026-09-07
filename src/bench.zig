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
        var db = try DB.init(ticker, data_dir, allocator, .{});
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
        var db = try DB.init(ticker, data_dir, allocator, .{});
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

    // --- QUERY BENCHMARK ---
    {
        try stdout.print("\n[QUERY BENCHMARK]\n", .{});
        // Cleanup
        std.fs.cwd().deleteTree(data_dir) catch {};

        var db = try DB.init(ticker, data_dir, allocator, .{});
        defer db.deinit();

        const num_records = 1_000_000;
        try stdout.print("Generating {d} records...\n", .{num_records});

        // Batch write for speed
        var i: usize = 0;
        while (i < num_records) : (i += 1) {
            try db.append(.{
                .timestamp = @intCast(i * 1000), // 1ms intervals
                .usd = @floatFromInt(i),
                .volume = @floatFromInt(i),
            });
        }
        try db.flush();

        try stdout.print("Running 10,000 random range queries...\n", .{});

        var latencies = try allocator.alloc(u64, 10_000);
        defer allocator.free(latencies);

        var rng = std.Random.DefaultPrng.init(0);
        const random = rng.random();

        var timer = try std.time.Timer.start();
        const start_time = timer.read();

        var q: usize = 0;
        while (q < 10_000) : (q += 1) {
            // Random start between 0 and num_records - 1000
            const start_idx = random.intRangeAtMost(usize, 0, num_records - 1000);
            const range_len = random.intRangeAtMost(usize, 10, 1000); // Query 10 to 1000 records

            const start_ts = @as(i64, @intCast(start_idx * 1000));
            const end_ts = start_ts + @as(i64, @intCast(range_len * 1000));

            const op_start = timer.read();
            const res = try db.query(start_ts, end_ts, allocator);
            const op_end = timer.read();
            allocator.free(res);

            latencies[q] = op_end - op_start;
        }

        const total_time_ns = timer.read() - start_time;
        const total_time_s = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        const qps = 10_000.0 / total_time_s;

        // Calculate Latency Stats
        std.mem.sort(u64, latencies, {}, std.sort.asc(u64));

        var sum: u128 = 0;
        for (latencies) |lat| sum += lat;
        const mean = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(latencies.len));
        const p50 = latencies[latencies.len / 2];
        const p99 = latencies[(latencies.len * 99) / 100];

        try stdout.print("Throughput: {d:.2} queries/sec\n", .{qps});
        try stdout.print("Latency (Mean): {d:.2} ns\n", .{mean});
        try stdout.print("Latency (p50):  {d} ns\n", .{p50});
        try stdout.print("Latency (p99):  {d} ns\n", .{p99});
    }

    // --- FLUSH-ON-WRITE BENCHMARK ---
    {
        try stdout.print("\n[FLUSH-ON-WRITE BENCHMARK]\n", .{});
        // Cleanup
        std.fs.cwd().deleteTree(data_dir) catch {};

        var db = try DB.init(ticker, data_dir, allocator, .{ .flush_on_write = true });
        defer db.deinit();

        var latencies = try allocator.alloc(u64, 1_000_000);
        defer allocator.free(latencies);
        var latency_count: usize = 0;

        // Run for shorter duration as it will be slower
        const duration_ns = 10 * std.time.ns_per_s;

        try stdout.print("Duration: 10 seconds\n", .{});
        try stdout.flush();

        var timer = try std.time.Timer.start();
        const start_time = timer.read();

        var i: usize = 0;
        var last_print_ns: u64 = 0;
        const print_interval_ns = 500 * std.time.ns_per_ms;

        while (true) : (i += 1) {
            const now = timer.read();
            const elapsed = now - start_time;
            if (elapsed >= duration_ns) break;

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

        const total_time_ns = timer.read() - start_time;
        const total_time_s = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(i)) / total_time_s;

        // Calculate Latency Stats
        const items = latencies[0..latency_count];
        std.mem.sort(u64, items, {}, std.sort.asc(u64));

        var sum: u128 = 0;
        for (items) |lat| sum += lat;
        const mean = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(items.len));
        const p50 = items[items.len / 2];
        const p99 = items[(items.len * 99) / 100];

        try stdout.print("Throughput: {d:.2} ops/sec\n", .{ops_per_sec});
        try stdout.print("Latency (Mean): {d:.2} ns\n", .{mean});
        try stdout.print("Latency (p50):  {d} ns\n", .{p50});
        try stdout.print("Latency (p99):  {d} ns\n", .{p99});
    }

    // --- INDICATOR BENCHMARK ---
    {
        const ind = hocdb.indicators;
        try stdout.print("\n[INDICATOR BENCHMARK] (SIMD lanes: {d})\n", .{ind.lanes});
        const n: usize = 1_000_000;
        const close = try allocator.alloc(f64, n);
        defer allocator.free(close);
        const high = try allocator.alloc(f64, n);
        defer allocator.free(high);
        const low = try allocator.alloc(f64, n);
        defer allocator.free(low);
        const volume = try allocator.alloc(f64, n);
        defer allocator.free(volume);
        const out = try allocator.alloc(f64, 5 * n);
        defer allocator.free(out);
        var prng = std.Random.DefaultPrng.init(42);
        const rnd = prng.random();
        var px: f64 = 100.0;
        for (0..n) |k| {
            px *= @exp(rnd.floatNorm(f64) * 0.005);
            close[k] = px;
            high[k] = px * 1.002;
            low[k] = px * 0.998;
            volume[k] = 1000.0 + @as(f64, @floatFromInt(k % 100));
        }
        const o0 = out[0..n];
        const o1 = out[n .. 2 * n];
        const o2 = out[2 * n .. 3 * n];
        const o3 = out[3 * n .. 4 * n];
        const o4 = out[4 * n .. 5 * n];
        const Case = struct { name: []const u8, run: *const fn (ind_close: []const f64, h: []const f64, l: []const f64, v: []const f64, a: std.mem.Allocator, outs: [5][]f64) anyerror!void };
        const cases = [_]Case{
            .{ .name = "sma(20)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.sma(c, 20, outs[0]);
                }
            }.f },
            .{ .name = "ema(20)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.ema(c, 20, outs[0]);
                }
            }.f },
            .{ .name = "rsi(14)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.rsi(c, 14, outs[0]);
                }
            }.f },
            .{ .name = "macd(12,26,9)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.macd(c, 12, 26, 9, outs[0], outs[1], outs[2]);
                }
            }.f },
            .{ .name = "bbands(20)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.bbands(c, 20, 2.0, outs[0], outs[1], outs[2], outs[3], outs[4]);
                }
            }.f },
            .{ .name = "atr(14)", .run = struct {
                fn f(c: []const f64, h: []const f64, l: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.atr(h, l, c, 14, outs[0]);
                }
            }.f },
            .{ .name = "adx(14)", .run = struct {
                fn f(c: []const f64, h: []const f64, l: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.adx(h, l, c, 14, outs[0], outs[1], outs[2]);
                }
            }.f },
            .{ .name = "stoch(14,3,3)", .run = struct {
                fn f(c: []const f64, h: []const f64, l: []const f64, _: []const f64, a: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.stoch(h, l, c, 14, 3, 3, outs[0], outs[1], a);
                }
            }.f },
            .{ .name = "rolling_max(50)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, a: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.rollingMax(c, 50, outs[0], a);
                }
            }.f },
            .{ .name = "obv", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, v: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.obv(c, v, outs[0]);
                }
            }.f },
            .{ .name = "mfi(14)", .run = struct {
                fn f(c: []const f64, h: []const f64, l: []const f64, v: []const f64, a: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.mfi(h, l, c, v, 14, outs[0], a);
                }
            }.f },
            .{ .name = "linreg(20)", .run = struct {
                fn f(c: []const f64, _: []const f64, _: []const f64, _: []const f64, _: std.mem.Allocator, outs: [5][]f64) anyerror!void {
                    try ind.linreg(c, 20, outs[0], outs[1], outs[2], outs[3]);
                }
            }.f },
        };
        var checksum: f64 = 0;
        for (cases) |cs| {
            var timer = try std.time.Timer.start();
            try cs.run(close, high, low, volume, allocator, .{ o0, o1, o2, o3, o4 });
            const ns = timer.read();
            checksum += o0[n - 1];
            const per_sec = @as(f64, @floatFromInt(n)) / (@as(f64, @floatFromInt(ns)) / 1e9);
            try stdout.print("  {s:<16} {d:>7.2} ms   {d:>14.0} records/sec\n", .{ cs.name, @as(f64, @floatFromInt(ns)) / 1e6, per_sec });
        }
        // Snapshot latency (the agent's "one shot" call) on 2500 bars.
        {
            const m: usize = 2500;
            const ts = try allocator.alloc(i64, m);
            defer allocator.free(ts);
            for (0..m) |k| ts[k] = @intCast(k);
            const reps: usize = 200;
            var timer = try std.time.Timer.start();
            var acc: f64 = 0;
            for (0..reps) |_| {
                const snap = try ind.snapshot(ts, null, high[0..m], low[0..m], close[0..m], volume[0..m], 252, allocator);
                acc += snap.rsi_14;
            }
            const ns = timer.read() / reps;
            checksum += acc;
            try stdout.print("  snapshot(2500 bars, ~100 fields): {d:.1} us per call\n", .{@as(f64, @floatFromInt(ns)) / 1e3});
        }
        // Scalar analytics over the whole series.
        {
            var timer = try std.time.Timer.start();
            const sm = try ind.summary(close, 252, allocator);
            const ns = timer.read();
            checksum += sm.sharpe;
            try stdout.print("  summary({d} bars, 29 stats): {d:.2} ms\n", .{ n, @as(f64, @floatFromInt(ns)) / 1e6 });
        }
        // Signal backtester: SMA-crossover targets over the whole series.
        {
            const bt = hocdb.backtest_mod;
            const target = try allocator.alloc(f64, n);
            defer allocator.free(target);
            const fast = try allocator.alloc(f64, n);
            defer allocator.free(fast);
            const slow = try allocator.alloc(f64, n);
            defer allocator.free(slow);
            try ind.sma(close, 10, fast);
            try ind.sma(close, 50, slow);
            for (0..n) |i| target[i] = if (ind.isNan(slow[i])) 0 else if (fast[i] > slow[i]) 1 else -1;
            const bars_ts = try allocator.alloc(i64, n);
            defer allocator.free(bars_ts);
            for (0..n) |i| bars_ts[i] = @intCast(i * 60);
            var timer = try std.time.Timer.start();
            const r = try bt.run(bars_ts, null, null, null, close, target, .{ .initial_equity = 1_000_000, .cost_bps = 5, .slippage_bps = 1, .stop_loss = 0.02, .position_mode = 1, .periods_per_year = 252 * 390 }, .{}, null, allocator);
            const ns = timer.read();
            checksum += r.final_equity;
            try stdout.print("  backtest({d} bars, {d} trades, stops): {d:.2} ms  {d:.0} bars/sec\n", .{ n, r.n_trades, @as(f64, @floatFromInt(ns)) / 1e6, @as(f64, @floatFromInt(n)) * 1e9 / @as(f64, @floatFromInt(ns)) });
        }
        // Universe features: 50 tickers x 500 bars (ranks, correlation matrix, betas).
        {
            const uni = hocdb.universe_mod;
            const m: usize = 50;
            const nb: usize = 500;
            const buf = try allocator.alloc(f64, m * nb);
            defer allocator.free(buf);
            var uprng = std.Random.DefaultPrng.init(7);
            const urnd = uprng.random();
            const closes = try allocator.alloc([]const f64, m);
            defer allocator.free(closes);
            for (0..m) |k| {
                var p: f64 = 100;
                for (0..nb) |i| {
                    p *= @exp(urnd.floatNorm(f64) * 0.01);
                    buf[k * nb + i] = p;
                }
                closes[k] = buf[k * nb .. (k + 1) * nb];
            }
            const rows = try allocator.alloc(uni.Row, m);
            defer allocator.free(rows);
            const corr = try allocator.alloc(f64, m * m);
            defer allocator.free(corr);
            var timer = try std.time.Timer.start();
            const reps: usize = 20;
            var acc: f64 = 0;
            for (0..reps) |_| {
                const sm = try uni.compute(closes, null, null, .{}, rows, corr, allocator);
                acc += sm.avg_pair_corr;
            }
            const ns = timer.read() / reps;
            checksum += acc;
            try stdout.print("  universe({d} tickers x {d} bars, corr matrix): {d:.1} us per call\n", .{ m, nb, @as(f64, @floatFromInt(ns)) / 1e3 });
        }
        try stdout.print("  Checksum: {d:.4}\n", .{checksum});
        try stdout.flush();
    }
    try stdout.flush();
}
