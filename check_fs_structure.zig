const std = @import("std");

pub fn main() !void {
    if (@hasDecl(std, "Io")) {
        std.debug.print("std.Io exists\n", .{});
        if (@hasDecl(std.Io, "Dir")) {
            std.debug.print("std.Io.Dir exists\n", .{});
        }
    }

    if (@hasDecl(std.io, "Dir")) {
        std.debug.print("std.io.Dir exists\n", .{});
        const Dir = std.io.Dir;
        if (@hasDecl(Dir, "cwd")) {
            std.debug.print("std.io.Dir.cwd exists\n", .{});
        }
    }
}
