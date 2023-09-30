package org.tisonkun.morax.proto.bookie;

import java.nio.ByteBuffer;

public record EntryId(long ledgerId, long entryId) {
    public byte[] toBytes() {
        final byte[] bytes = new byte[Long.BYTES + Long.BYTES];
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(ledgerId);
        buffer.putLong(entryId);
        return bytes;
    }
}
