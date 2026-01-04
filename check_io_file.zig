const std = @import("std");

pub fn main() !void {
    if (@hasDecl(std.io, "File")) {
        std.debug.print("std.io.File exists\n", .{});
    } else {
        std.debug.print("std.io.File does NOT exist locally\n", .{});
    }
}
