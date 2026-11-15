const std = @import("std");
const cli = @import("cli.zig");

pub fn main(init: std.process.Init) void {
    const allocator = init.arena.allocator();
    const args = init.minimal.args.toSlice(allocator) catch std.process.exit(1);
    cli.run(allocator, args[1..]) catch |err| {
        cli.reportError(err) catch {};
        std.process.exit(1);
    };
}
