/*
 * Copyright 2023 tison <wander4096@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tisonkun.morax.proto.bookie;

import java.nio.ByteBuffer;

public record EntryLocation(int logId, long position) {
    public byte[] toBytes() {
        final byte[] bytes = new byte[Integer.BYTES + Long.BYTES];
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(logId);
        buffer.putLong(position);
        return bytes;
    }

    public static EntryLocation fromBytes(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final int logId = buffer.getInt();
        final long position = buffer.getLong();
        return new EntryLocation(logId, position);
    }
}
