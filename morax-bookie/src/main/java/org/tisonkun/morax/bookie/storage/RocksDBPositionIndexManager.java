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

package org.tisonkun.morax.bookie.storage;

import com.google.common.base.Preconditions;
import org.tisonkun.morax.bookie.utils.LongPairWrapper;
import org.tisonkun.morax.bookie.utils.RocksDBUtil;
import org.tisonkun.morax.proto.bookie.EntryLocation;

import java.io.IOException;
import java.nio.file.Path;

import static org.tisonkun.morax.proto.exception.ExceptionMessageBuilder.exMsg;

public class RocksDBPositionIndexManager extends PositionIndexManager {
    /**
     * (ledgerId, EntryId) -> (logId, position)
     */
    private final RocksDBUtil rocksDB;

    public RocksDBPositionIndexManager(Path basePath) throws IOException {
        rocksDB = new RocksDBUtil(basePath);
    }


    @Override
    public void addPosition(long ledgerId, long entryId, int logId, long position) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongPairWrapper value = LongPairWrapper.get();

        EntryLocation prev = null;
        if (rocksDB.get(key.array, value.array) >= 0) {
            prev = new EntryLocation(value.getFirst(), value.getSecond());
        } else {
            value = LongPairWrapper.get(logId, position);
        }

        Preconditions.checkState(
                prev == null,
                exMsg("Conflict position").kv("key", key).kv("value", value).toString());

        rocksDB.put(key.array, value.array);
    }

    @Override
    public EntryLocation findPosition(long ledgerId, long entryId) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongPairWrapper value = LongPairWrapper.get();

        EntryLocation curr = null;
        if (rocksDB.get(key.array, value.array) >= 0) {
            curr = new EntryLocation(value.getFirst(), value.getSecond());
        }
        return curr;
    }

    @Override
    public void close() {
        rocksDB.close();
    }
}
