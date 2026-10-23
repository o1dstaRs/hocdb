//! Streaming CRC-32C with the same on-disk checksum as HOC2.
const std = @import("std");
const builtin = @import("builtin");

pub const Crc32c = if (builtin.cpu.hasAll(.aarch64, &.{.crc})) Arm else std.hash.crc.@"CRC-32/ISCSI";

const Arm = struct {
    crc: u32,

    pub fn init() Arm {
        return .{ .crc = 0xffffffff };
    }

    pub fn update(self: *Arm, bytes: []const u8) void {
        var crc = self.crc;
        var i: usize = 0;
        while (i + 8 <= bytes.len) : (i += 8) {
            const word = std.mem.readInt(u64, bytes[i..][0..8], .little);
            crc = asm ("crc32cx w0, w0, x1"
                : [result] "={w0}" (-> u32),
                : [state] "{w0}" (crc),
                  [word] "{x1}" (word),
            );
        }
        while (i < bytes.len) : (i += 1) {
            crc = asm ("crc32cb w0, w0, w1"
                : [result] "={w0}" (-> u32),
                : [state] "{w0}" (crc),
                  [byte] "{w1}" (@as(u32, bytes[i])),
            );
        }
        self.crc = crc;
    }

    pub fn final(self: Arm) u32 {
        return ~self.crc;
    }

    pub fn hash(bytes: []const u8) u32 {
        var state = init();
        state.update(bytes);
        return state.final();
    }
};

test "CRC32C matches the generic implementation for offsets, tails and streaming updates" {
    const Reference = std.hash.crc.Generic(u32, .{
        .polynomial = 0x1edc6f41,
        .initial = 0xffffffff,
        .reflect_input = true,
        .reflect_output = true,
        .xor_output = 0xffffffff,
    });
    try std.testing.expectEqual(@as(u32, 0xe3069283), Crc32c.hash("123456789"));
    var bytes: [1040]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(1984);
    rng.random().bytes(&bytes);
    for (0..16) |offset| {
        for (0..1025) |len| {
            const input = bytes[offset..][0..len];
            const expected = Reference.hash(input);
            try std.testing.expectEqual(expected, Crc32c.hash(input));
            var state = Crc32c.init();
            const split = len / 3;
            state.update(input[0..split]);
            state.update("");
            state.update(input[split..]);
            try std.testing.expectEqual(expected, state.final());
        }
    }
}
