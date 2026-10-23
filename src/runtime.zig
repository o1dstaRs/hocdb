//! Synchronous I/O for the existing embedding APIs. No worker pool or signal
//! handlers are installed in the host process. Zig callers may supply their
//! own I/O implementation through initWithIo / openReaderWithIo.
const std = @import("std");

var backend: std.Io.Threaded = .init_single_threaded;
pub const io: std.Io = backend.io();

pub fn randomInt(comptime T: type) T {
    var value: T = undefined;
    io.random(std.mem.asBytes(&value));
    return value;
}

/// Monotonic elapsed time, independent of changes to the wall clock.
pub const Timer = struct {
    context: std.Io,
    started: std.Io.Timestamp,

    pub fn start() !Timer {
        return startWithIo(io);
    }

    pub fn startWithIo(context: std.Io) Timer {
        return .{ .context = context, .started = .now(context, .awake) };
    }

    pub fn read(self: *const Timer) u64 {
        const now = std.Io.Timestamp.now(self.context, .awake);
        return @intCast(@max(0, self.started.durationTo(now).nanoseconds));
    }
};
