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

package org.tisonkun.morax.bookie;

import static org.assertj.core.api.Assertions.assertThat;
import static org.tisonkun.morax.bookie.storage.PositionIndexType.MEMORY;
import static org.tisonkun.morax.bookie.storage.PositionIndexType.ROCKSDB;

import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.tisonkun.morax.bookie.storage.EntryLogIds;
import org.tisonkun.morax.bookie.storage.LedgerDirs;
import org.tisonkun.morax.bookie.storage.MemoryPositionIndexManager;
import org.tisonkun.morax.bookie.storage.PositionIndexManager;
import org.tisonkun.morax.bookie.storage.PositionIndexType;
import org.tisonkun.morax.bookie.storage.RocksDBPositionIndexManager;
import org.tisonkun.morax.proto.bookie.DefaultEntry;
import org.tisonkun.morax.proto.bookie.Entry;

class LedgerTest {

    @TempDir
    private Path tempDir;

    private static Stream<Arguments> getPositionIndexType() {
        return Stream.of(
                Arguments.of(PositionIndexType.MEMORY),
                Arguments.of(PositionIndexType.ROCKSDB));
    }


    @ParameterizedTest
    @MethodSource("getPositionIndexType")
    void testAddAndGetEntry(PositionIndexType indexType) throws Exception {
        final LedgerDirs ledgerDirs = new LedgerDirs(Collections.singletonList(tempDir.toFile()));
        final EntryLogIds logIds = new EntryLogIds(ledgerDirs);
        final long ledgerId = 1;
        final Executor writeExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("EntryLogWrite"));
        final PositionIndexManager indexManager = createPositionIndexManager(tempDir, indexType);
        final Ledger ledger = new Ledger(ledgerId, tempDir, logIds, indexManager, writeExecutor);
        final Entry[] entries = new Entry[]{
                new DefaultEntry(ledgerId, 1, 1, Unpooled.copiedBuffer("testAddAndGetEntry-1", StandardCharsets.UTF_8)),
                new DefaultEntry(ledgerId, 2, 2, Unpooled.copiedBuffer("testAddAndGetEntry-2", StandardCharsets.UTF_8)),
        };

        for (Entry entry : entries) {
            ledger.addEntry(entry);
        }
        ledger.flush();

        for (int i = 0; i < entries.length; i++) {
            final int idx = entries.length - 1 - i;
            final Entry actual = ledger.readEntry(entries[idx].getEntryId());
            assertThat(actual).isEqualTo(entries[idx]);
        }

        for (int i = 0; i < 100; i++) {
            final int idx = Math.floorMod(new Random().nextInt(), entries.length);
            final Entry actual = ledger.readEntry(entries[idx].getEntryId());
            assertThat(actual).isEqualTo(entries[idx]);
        }
    }

    private static PositionIndexManager createPositionIndexManager(Path tempDir, PositionIndexType indexType) throws Exception {
        return switch (indexType) {
            case MEMORY -> new MemoryPositionIndexManager();
            case ROCKSDB -> new RocksDBPositionIndexManager(tempDir);
        };
    }
}
