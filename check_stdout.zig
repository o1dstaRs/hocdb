const std = @import("std");

pub fn main() !void {
    const stdout = std.io.getStdOut();
    const T = @TypeOf(stdout);
    std.debug.print("stdout type: {s}\n", .{@typeName(T)});

    if (@hasDecl(std.fs, "File")) {
        if (T == std.fs.File) {
            std.debug.print("Matches std.fs.File\n", .{});
        }
    }
}
