const std = @import("std");

pub fn main() !void {
    if (!@hasDecl(std, "Io")) {
        std.debug.print("std.Io missing\n", .{});
        return;
    }
    const Io = std.Io;
    std.debug.print("std.Io exists\n", .{});

    if (@hasDecl(Io, "getStdIo")) std.debug.print("Io.getStdIo exists\n", .{});
    if (@hasDecl(Io, "getStdIn")) std.debug.print("Io.getStdIn exists\n", .{});

    if (@hasDecl(Io, "File")) {
        const File = Io.File;
        std.debug.print("Io.File exists. Decls:\n", .{});
        if (@hasDecl(File, "pwriteAll")) std.debug.print("  pwriteAll\n", .{});
        if (@hasDecl(File, "pwrite")) std.debug.print("  pwrite\n", .{});
        if (@hasDecl(File, "writeAt")) std.debug.print("  writeAt\n", .{});
        if (@hasDecl(File, "seekTo")) std.debug.print("  seekTo\n", .{});
    }

    if (@hasDecl(Io, "Dir")) {
        const Dir = Io.Dir;
        std.debug.print("Io.Dir exists. Decls:\n", .{});
        if (@hasDecl(Dir, "openDir")) std.debug.print("  openDir\n", .{});
        if (@hasDecl(Dir, "deleteFile")) std.debug.print("  deleteFile\n", .{});
    }
}
