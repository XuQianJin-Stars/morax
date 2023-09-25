/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tisonkun.morax.bookie;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ChecksumType;
import org.rocksdb.Options;
import org.tisonkun.morax.bookie.storage.KVStorage;
import org.tisonkun.morax.bookie.storage.KVStorageRocksDB;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVStorageRocksDBTest {

    @Test
    public void testInitiate(@TempDir Path tempDir) throws Exception {
        try (KVStorageRocksDB rocksDB = new KVStorageRocksDB(tempDir)) {
            Options options = (Options) rocksDB.getOptions();
            assertEquals(64 * 1024 * 1024, options.writeBufferSize());
            assertEquals(4, options.maxWriteBufferNumber());
            assertEquals(256 * 1024 * 1024, options.maxBytesForLevelBase());
            assertTrue(options.levelCompactionDynamicLevelBytes());
            assertEquals(ChecksumType.kxxHash, ((BlockBasedTableConfig) options.tableFormatConfig()).checksumType());
        }
    }

    @Test
    public void testPut(@TempDir Path tempDir) throws Exception {
        byte[] key = "1".getBytes(StandardCharsets.UTF_8);
        byte[] value = "1".getBytes(StandardCharsets.UTF_8);

        try (KVStorageRocksDB rocksDB = new KVStorageRocksDB(tempDir)) {
            rocksDB.put(key, value);
            assertEquals(
                    new String(value, StandardCharsets.UTF_8),
                    new String(rocksDB.get(key), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testDelete(@TempDir Path tempDir) throws Exception {
        byte[] key = "1".getBytes(StandardCharsets.UTF_8);
        byte[] value = "1".getBytes(StandardCharsets.UTF_8);

        try (KVStorageRocksDB rocksDB = new KVStorageRocksDB(tempDir)) {
            rocksDB.put(key, value);
            assertEquals(1, rocksDB.count());

            rocksDB.delete(key);
            assertEquals(0, rocksDB.count());
        }
    }

    @Test
    public void testCompact(@TempDir Path tempDir) throws Exception {
        try (KVStorageRocksDB rocksDB = new KVStorageRocksDB(tempDir)) {
            for (int i = 0; i < 10000; i++) {
                byte[] key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
                rocksDB.put(key, key);
            }

            assertEquals(10000, rocksDB.count());

            rocksDB.compact();

            assertEquals(10000, rocksDB.count());
        }
    }

    @Test
    public void testBatchPut(@TempDir Path tempDir) throws Exception {
        try (KVStorageRocksDB rocksDB = new KVStorageRocksDB(tempDir)) {
            KVStorage.Batch batch = rocksDB.newBatch();
            for (int i = 0; i < 10000; i++) {
                byte[] key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
                if (batch != null) {
                    batch.put(key, key);
                }
            }

            assertEquals(0, rocksDB.count());

            if (batch != null) {
                batch.flush();
            }

            assertEquals(10000, rocksDB.count());
        }
    }
}
