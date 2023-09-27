/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.tisonkun.morax.bookie.storage;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ChecksumType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.netty.util.internal.PlatformDependent.maxDirectMemory;

/**
 * RocksDB based implementation of the KeyValueStorage.
 */
@Slf4j
public class KVStorageRocksDB implements KVStorage {
    static KVStorageFactory factory = KVStorageRocksDB::new;

    private Cache cache;

    private final RocksDB db;
    @Getter
    private RocksObject options;
    @Getter
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors;

    private final WriteOptions optionSync;
    private final WriteOptions optionDontSync;

    private final ReadOptions optionCache;
    private final ReadOptions optionDontCache;
    private final WriteBatch writeBatch;
    private final int writeBatchMaxSize;

    private String dbPath;

    public KVStorageRocksDB(Path basePath) throws IOException {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            throw new IOException("Failed to load RocksDB JNI library", t);
        }

        this.optionSync = new WriteOptions();
        this.optionDontSync = new WriteOptions();
        this.optionCache = new ReadOptions();
        this.optionDontCache = new ReadOptions();
        this.writeBatch = new WriteBatch();

        log.info("Searching for a RocksDB configuration file in {}", basePath.toString());
        db = initialize(basePath);

        optionSync.setSync(true);
        optionDontSync.setSync(false);

        optionCache.setFillCache(true);
        optionDontCache.setFillCache(false);

        // TODO: Default first and then modify to unified config
        this.writeBatchMaxSize = 100000;
    }

    private RocksDB initialize(Path basePath) throws IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        ChecksumType checksumType = ChecksumType.valueOf("kxxHash");

        /* Set default RocksDB block-cache size to 10% / numberOfLedgers of direct memory, unless override */
        int ledgerDirsSize = 1;
        long defaultRocksDBBlockCacheSizeBytes = maxDirectMemory() / ledgerDirsSize / 10;
        long blockCacheSize = defaultRocksDBBlockCacheSizeBytes;

        long writeBufferSizeMB = 64;
        long sstSizeMB = 64;
        int numLevels = -1;
        int numFilesInLevel0 = 4;
        long maxSizeInLevel1MB = 256;
        int blockSize = 64 * 1024;
        int bloomFilterBitsPerKey = 10;
        boolean lz4CompressionEnabled = true;
        int formatVersion = 2;

        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setWriteBufferSize(writeBufferSizeMB * 1024 * 1024);
        options.setMaxWriteBufferNumber(4);
        options.setLevelZeroFileNumCompactionTrigger(numFilesInLevel0);
        options.setMaxBytesForLevelBase(maxSizeInLevel1MB * 1024 * 1024);
        options.setMaxBackgroundJobs(32);
        options.setIncreaseParallelism(32);
        options.setMaxTotalWalSize(512 * 1024 * 1024);
        options.setMaxOpenFiles(-1);
        options.setTargetFileSizeBase(sstSizeMB * 1024 * 1024);
        options.setDeleteObsoleteFilesPeriodMicros(TimeUnit.HOURS.toMicros(1));

        this.cache = new LRUCache(blockCacheSize);
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockSize(blockSize);
        tableOptions.setBlockCache(cache);
        tableOptions.setFormatVersion(formatVersion);
        tableOptions.setChecksumType(checksumType);
        tableOptions.setFilterPolicy(new BloomFilter(bloomFilterBitsPerKey, false));

        // Options best suited for HDDs
        tableOptions.setCacheIndexAndFilterBlocks(true);
        options.setLevelCompactionDynamicLevelBytes(true);

        options.setTableFormatConfig(tableOptions);

        // Configure file path
        Path logPathSetting = FileSystems.getDefault().getPath(basePath.toString(), "log");
        this.dbPath = FileSystems.getDefault().getPath(basePath.toString(), "log").toFile().toString();

        Files.createDirectories(logPathSetting);
        log.info("RocksDB log path: {}", logPathSetting);
        options.setDbLogDir(logPathSetting.toString());

        // Configure log level
        String logLevel = "info";
        switch (logLevel) {
            case "debug":
                options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
                break;
            case "info":
                options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                break;
            case "warn":
                options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
                break;
            case "error":
                options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
                break;
            default:
                log.warn("Unrecognized RockDB log level: {}", logLevel);
        }

        // Keep log files for 1month
        options.setKeepLogFileNum(30);
        options.setLogFileTimeToRoll(TimeUnit.DAYS.toSeconds(1));
        this.options = options;
        try {
            return RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            throw new IOException("Error open RocksDB database", e);
        }
    }

    @Override
    public void close() {
        db.close();
        if (options != null) {
            options.close();
        }
        optionSync.close();
        optionDontSync.close();
        optionCache.close();
        optionDontCache.close();
        writeBatch.close();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(optionDontSync, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB put", e);
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB get", e);
        }
    }

    @Override
    public int get(byte[] key, byte[] value) throws IOException {
        try {
            int res = db.get(key, value);
            if (res == RocksDB.NOT_FOUND) {
                return -1;
            } else if (res > value.length) {
                throw new IOException("Value array is too small to fit the result");
            } else {
                return res;
            }
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB get", e);
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        try {
            db.delete(optionDontSync, key);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB delete", e);
        }
    }

    @Override
    public String getDBPath() {
        return dbPath;
    }

    @Override
    public void compact(byte[] firstKey, byte[] lastKey) throws IOException {
        try {
            db.compactRange(firstKey, lastKey);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB compact", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            final long start = System.currentTimeMillis();
            final int oriRocksDBFileCount = db.getLiveFilesMetaData().size();
            final long oriRocksDBSize = getRocksDBSize();
            log.info("Starting RocksDB {} compact, current RocksDB hold {} files and {} Bytes.",
                    db.getName(), oriRocksDBFileCount, oriRocksDBSize);

            db.compactRange();

            final long end = System.currentTimeMillis();
            final int rocksDBFileCount = db.getLiveFilesMetaData().size();
            final long rocksDBSize = getRocksDBSize();
            log.info("RocksDB {} compact finished {} ms, space reduced {} Bytes, current hold {} files and {} Bytes.",
                    db.getName(), end - start, oriRocksDBSize - rocksDBSize, rocksDBFileCount, rocksDBSize);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB compact", e);
        }
    }

    private long getRocksDBSize() {
        List<LiveFileMetaData> liveFilesMetaData = db.getLiveFilesMetaData();
        long rocksDBFileSize = 0L;
        for (LiveFileMetaData fileMetaData : liveFilesMetaData) {
            rocksDBFileSize += fileMetaData.size();
        }
        return rocksDBFileSize;
    }

    @Override
    public void sync() throws IOException {
        try {
            db.write(optionSync, writeBatch);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public CloseableIterator<byte[]> keys() {
        final RocksIterator iterator = db.newIterator(optionCache);
        iterator.seekToFirst();

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public byte[] next() {
                checkState(iterator.isValid());
                byte[] key = iterator.key();
                iterator.next();
                return key;
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    @Override
    public CloseableIterator<byte[]> keys(byte[] firstKey, byte[] lastKey) {
        final Slice upperBound = new Slice(lastKey);
        final ReadOptions option = new ReadOptions(optionCache).setIterateUpperBound(upperBound);
        final RocksIterator iterator = db.newIterator(option);
        iterator.seek(firstKey);

        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public byte[] next() {
                checkState(iterator.isValid());
                byte[] key = iterator.key();
                iterator.next();
                return key;
            }

            @Override
            public void close() {
                iterator.close();
                option.close();
                upperBound.close();
            }
        };
    }

    @Override
    public CloseableIterator<Entry<byte[], byte[]>> iterator() {
        final RocksIterator iterator = db.newIterator(optionDontCache);
        iterator.seekToFirst();
        final EntryWrapper entryWrapper = new EntryWrapper();

        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public Entry<byte[], byte[]> next() {
                checkState(iterator.isValid());
                entryWrapper.key = iterator.key();
                entryWrapper.value = iterator.value();
                iterator.next();
                return entryWrapper;
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    @Override
    public long count() throws IOException {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new IOException("Error in getting records count", e);
        }
    }

    @Override
    public Batch newBatch() {
        return new RocksDBBatch(writeBatchMaxSize);
    }

    private class RocksDBBatch implements Batch {
        private final WriteBatch writeBatch = new WriteBatch();
        private final int batchSize;
        private int batchCount = 0;

        RocksDBBatch(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void close() {
            writeBatch.close();
            batchCount = 0;
        }

        @Override
        public void put(byte[] key, byte[] value) throws IOException {
            try {
                writeBatch.put(key, value);
                countBatchAndFlushIfNeeded();
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void remove(byte[] key) throws IOException {
            try {
                writeBatch.delete(key);
                countBatchAndFlushIfNeeded();
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void clear() {
            writeBatch.clear();
            batchCount = 0;
        }

        @Override
        public void deleteRange(byte[] beginKey, byte[] endKey) throws IOException {
            try {
                writeBatch.deleteRange(beginKey, endKey);
                countBatchAndFlushIfNeeded();
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        private void countBatchAndFlushIfNeeded() throws IOException {
            if (++batchCount >= batchSize) {
                flush();
                clear();
            }
        }

        @Override
        public int batchCount() {
            return batchCount;
        }

        @Override
        public void flush() throws IOException {
            try {
                db.write(optionSync, writeBatch);
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }
    }

    private static final class EntryWrapper implements Entry<byte[], byte[]> {
        // This is not final since the iterator will reuse the same EntryWrapper
        // instance at each step
        private byte[] key;
        private byte[] value;

        public EntryWrapper() {
            this.key = null;
            this.value = null;
        }

        public EntryWrapper(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] setValue(byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] getKey() {
            return key;
        }
    }

    public RocksDB db() {
        return db;
    }
}
