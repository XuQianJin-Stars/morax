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

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Default implementation of {@link Entry} based on in-memory {@link ByteBuf}.
 */
@Data
public class DefaultEntry implements Entry {
    private final long ledgerId;
    private final long entryId;
    private final long lastConfirmed;
    private final ByteBuf payload;

    @EqualsAndHashCode.Exclude
    private transient volatile ByteBuf cachedBytes;

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public long getLastConfirmed() {
        return lastConfirmed;
    }

    @Override
    public ByteBuf getPayload() {
        return payload;
    }

    @Override
    public ByteBuf toBytes() {
        return cachedBytes();
    }

    @Override
    public EntryProto toEntryProto() {
        return EntryProto.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setLastConfirmed(lastConfirmed)
                .setPayload(ByteString.copyFrom(payload.nioBuffer()))
                .build();
    }

    private ByteBuf cachedBytes() {
        if (cachedBytes != null) {
            return cachedBytes;
        }
        final ByteBuf result = Unpooled.buffer(8 + 8 + 8 + payload.readableBytes());
        result.writeLong(ledgerId);
        result.writeLong(entryId);
        result.writeLong(lastConfirmed);
        result.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        cachedBytes = result;
        return result;
    }

    @Override
    public String toString() {
        return "DefaultEntry{" + "ledgerId="
                + ledgerId + ", entryId="
                + entryId + ", lastConfirmed="
                + lastConfirmed + ", payload="
                + payload.toString(StandardCharsets.UTF_8) + '}';
    }
}
