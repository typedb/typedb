/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.graph.iid.InfixIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.Statistics;
import org.rocksdb.UInt64AddOperator;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.collection.Bytes.KB;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static org.rocksdb.CompressionType.LZ4_COMPRESSION;
import static org.rocksdb.CompressionType.NO_COMPRESSION;

public class RocksConfiguration {

    private final long dataCacheSize;
    private final long indexCacheSize;
    private final boolean logStatistics;
    private final int logStatisticsPeriodSec;

    public RocksConfiguration(long dataCacheSize, long indexCacheSize, boolean logStatistics, int logStatisticsPeriodSec) {
        this.dataCacheSize = dataCacheSize;
        this.indexCacheSize = indexCacheSize;
        this.logStatistics = logStatistics;
        this.logStatisticsPeriodSec = logStatisticsPeriodSec;
    }

    /**
     * WARNING: we can break backward compatibility or corrupt user data by changing these options and using them
     * with existing databases
     */
    org.rocksdb.Options schemaOptions() {
        return new org.rocksdb.Options()
                .setCreateIfMissing(true)
                .setCompressionType(NO_COMPRESSION)
                .setTableFormatConfig(schemaTableOptions());
    }

    private BlockBasedTableConfig schemaTableOptions() {
        BlockBasedTableConfig rocksDBTableOptions = new BlockBasedTableConfig();
        LRUCache uncompressedCache = new LRUCache(64 * MB);
        rocksDBTableOptions.setBlockSize(16 * KB);
        rocksDBTableOptions.setFormatVersion(5);
        rocksDBTableOptions.setIndexBlockRestartInterval(16);
        rocksDBTableOptions.setEnableIndexCompression(false);
        rocksDBTableOptions.setBlockCache(uncompressedCache);
        return rocksDBTableOptions;
    }

    /**
     * WARNING: we can break backward compatibility or corrupt user data by changing these options and using them
     * with existing databases
     */
    org.rocksdb.Options dataOptions() {
        org.rocksdb.Options options = new org.rocksdb.Options().setCreateIfMissing(true);
        configureWriteBuffer(options);
        configureCompaction(options);
        configureSSTAndCompression(options);
        configurePrefixExtractorAndMergeOperator(options);
        options.setTableFormatConfig(dataTableOptions());
        if (logStatistics) configureStatistics(options);
        return options;
    }

    /**
     * Much of this information comes from: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
     *
     * Write buffers determine how much unsorted, uncompacted data is kept in memory before being flushed to L0.
     *
     * Tradeoff: the more we can delay compaction and accumulate writes in the write buffer, the faster our pure write speed.
     * However, through this process we lose reads, since the data is unsorted.
     *
     * We increase the default 64Mb to 128MB write buffer, and set the number of write buffers maximum to 4 from the default of 2.
     * This means we can have up to 512MB of unsorted data in memory. This gives a decent tradeoff between write speed, memory usage,
     * and read speed. The more write buffers there are, the slower reads: for each read, all write buffers have to be checked.
     *
     * To achieve the best performance, we should match the size of L1 with the size of L0. To do this we should set
     * `maxBytesForLevelBase` to the number of write buffers * size of memtable. This makes L0 -> L1 compactions fast as possible,
     * which is a single-threaded operation that doesn't scale very well. So in addition, the larger we make L0/L1, the
     * more we rely on the single-threaded compaction during contious loading/mixed read-write operation.
     *
     * According to the option documentation (https://javadoc.io/static/org.rocksdb/rocksdbjni/6.25.3/org/rocksdb/Options.html)
     * `maxWriteBufferNumberToMaintain`, is used to control how long _old_ write buffers are retained for conflict checking
     * open snapshots against past writes. Given that in TypeDB we perform our own consistency checks,
     * we do not need to keep any at all, freeing up memory. So, we should always set it to 0.
     *
     * To enable higher memtable throughput we can set `concurrentMemtableWrite` to `true`, along with a `enableWriteThreadAdaptiveYield`,
     * though we have not provably seen much benefit from these.
     *
     * With 4x128MB write buffers, with concurrent memtable writes and adaptive yield, during a bulk load we saw only
     * 25 seconds of stalling in 8 hours of data loading. Stalls are also only really seen when doing straight writes,
     * without mixed reads (the norm).
     */
    private void configureWriteBuffer(Options options) {
        long writeBufferSize = 128 * MB;
        int writeBuffersMaxCount = 4;
        options.setWriteBufferSize(writeBufferSize)
                .setMaxWriteBufferNumber(writeBuffersMaxCount)
                // don't maintain any old write buffers since we handle key conflict detection ourselves
                .setMaxWriteBufferNumberToMaintain(0)
                // L1 should match L0 size for best performance, since L0 -> L1 compaction is single threaded
                .setMaxBytesForLevelBase(writeBufferSize * writeBuffersMaxCount)
                .setEnableWriteThreadAdaptiveYield(true)
                .setAllowConcurrentMemtableWrite(true);
    }

    /**
     * By default RocksDB uses 1 thread for flush and 1 thread for compaction. We can give RocksDB permission to use many threads
     * for background jobs (eg. compaction and flush) with `maxBackgroundJobs`.
     *
     * To help relieve write stalls due to compaction at the higher levels (L0 -> L1 is single threaded), we can allow subcompactions
     * by setting max subcompaction threads to a value higher than 0 as well.
     */
    private void configureCompaction(Options options) {
        options.setMaxSubcompactions(RocksTypeDB.MAX_THREADS).setMaxBackgroundJobs(RocksTypeDB.MAX_THREADS);
    }

    /**
     * SST Size
     * By default, RocksDB will use a 64MB SST size, constant on each level (we could set a multiplier `targetFileSizeMultiplier`,
     * but we use RocksDB defaults here). We can use `targetFileSizeBase` to set the SST size for L0. This affects compaction
     * and bloom filters if we set them (larger SST = higher false positive rate in bloom filters).
     *
     * With a 64MB SST file, and 50 byte keys on average and no compression, an SST will contain on average 1.2 million keys.
     * With 4x compression (like from LZ4), we fit 5 million keys into a 64MB SST.
     *
     * Note: with 64MB SST files, if we have a max_open_files from the OS of 1024, we can only end up with a 64GB data set, etc.
     * As such, we should clearly tell the users to configure the max open files to be around 48k (~ enough for 3TB of data)
     *
     * Without much evidence to support the best file size, we follow RocksDB's default of 64MB SST files.
     *
     * Compression
     * We tested with various compression levels. Not having any compression leads to an approximately 5x write amplification
     * when comparing an uncompressed CSV source to the database size. However, a compressed CSV compared to an uncompressed
     * database is a 20x write amplification!
     *
     * There are many compression options available in RocksDB. The best "heavy" compression is ZLIB, and the best "light"
     * compression is LZ4. ZLIB comes with a heavy penalty of about 20% performance, and compresses data by 7x.
     * LZ4 has no noticable cost, and compresses data by about 3x, and great compression and decompression speeds (https://github.com/lz4/lz4).
     *
     * It may actually be true for us that compression on some level pays for itself in terms of cost: the OS will keep
     * pages from memory cached under the hood. If the data is compressed, more data can be kept in memory by the OS,
     * leading to less disk access and faster overall performance. This hasn't been observed yet, but may already be in play
     * which leads to LZ4 compression having "no" visible performance impact.
     *
     * We end up using the same settings as RockSet (https://rockset.com/blog/how-we-use-rocksdb-at-rockset/), by setting
     * no compression on L0 and L1, where a many smaller sets of data flow through rapidly before being written into lower levels,
     * and using lightweight LZ4 compression below that (levels 2 to 7).
     *
     * Following RocksDB advise at https://github.com/facebook/rocksdb/wiki/Space-Tuning, we should disable
     * index compression, to make sure indexes are always rapidly accessible (at the expense of some CPU and memory).
     */
    private void configureSSTAndCompression(Options options) {
        options
                // explicitly set SST file size to avoid relying on RocksDB defaults that could change
                .setTargetFileSizeBase(64 * MB)
                // best performance-space tradeoff: apply lightweight LZ4 compression to levels that change less
                .setCompressionPerLevel(list(NO_COMPRESSION, NO_COMPRESSION, LZ4_COMPRESSION, LZ4_COMPRESSION,
                        LZ4_COMPRESSION, LZ4_COMPRESSION, LZ4_COMPRESSION));
    }

    private void configurePrefixExtractorAndMergeOperator(Options options) {
        // common prefix-seek is a vertex + infix + vertex prefix with type (eg. edge scan), significant performance boost
        options.useCappedPrefixExtractor(VertexIID.Thing.DEFAULT_LENGTH + InfixIID.Thing.LENGTH + VertexIID.Thing.PREFIX_W_TYPE_LENGTH)
                .setMergeOperator(new UInt64AddOperator());
    }

    /**
     * We can make RocksDB print statistics for block cache, filtering, get/write timing statistics, we have to set two options:
     * `setStatistics(new Statistics())` is required, and one can read the the Java statistics option to get the values back.
     * However, if we want RocksDB to print the statistics into its own LOG file with `statsDumpPeriodSec`.
     */
    private void configureStatistics(Options options) {
        options.setStatistics(new Statistics());
        options.setStatsDumpPeriodSec(logStatisticsPeriodSec);
    }

    private BlockBasedTableConfig dataTableOptions() {
        BlockBasedTableConfig rocksDBTableOptions = new BlockBasedTableConfig();
        // best performance when entire index is uncompressed, as it is accessed often
        rocksDBTableOptions.setEnableIndexCompression(false);
        configureBlocks(rocksDBTableOptions);
        configureBloomFilter(rocksDBTableOptions);
        configureBlockCache(rocksDBTableOptions);
        return rocksDBTableOptions;
    }

    /**
     * The default RocksDB block size is 4KB. We increase it to 16KB - the larger the data blocks, the smaller the overall size
     * of indexes in the database. However, if the block is too large, doing a point lookup of a particular key can cost more
     * if it is not already in memory, since we have to fetch the entire 16KB block first!
     *
     * The rough formula for index size is `~= (database size / block size) * avg key size`.  See https://github.com/facebook/rocksdb/issues/719
     * for the source of the equation. In general, we see that around 1% of data size is index size, and the index should live in memory.
     * The larger the block size, the less memory we require for index structures.
     */
    private void configureBlocks(BlockBasedTableConfig rocksDBTableOptions) {
        // hardcode block size and format version to avoid relying on RocksDB defaults that could change
        rocksDBTableOptions.setBlockSize(16 * KB);
        rocksDBTableOptions.setFormatVersion(5);
        rocksDBTableOptions.setIndexBlockRestartInterval(16);
    }

    /**
     * A bloom filter has a similar performance impact (~20%) to a block cache, but the memory cost grows linearly for optimal performance.
     *
     * We can estimate the footprint of bloom filters with: `num_keys * per_key_bloom_size`. In a test, we loaded 150 million
     * concepts, which turned into 1.7 billion keys, into a 22GB rocks database with LZ4 compression. The size of the bloom
     * with 10 bits per key is filter is predicted to be 17 billion bits ~= 2 billion bytes = 2 GB of data. The actual test
     * gave 1.8GB of space for bloom filters, so this is a good predictor.
     *
     * Note that in a compressed 22GB rocks database, we end up with 10% of data as bloom filter.
     *
     * _There is a huge penalty_ if the bloom filters do not entirely fit into memory. However, to avoid a ballooning memory
     * footprint, we want to restrict the filters to fit into a predictable memory size. This is done with
     * `cacheIndexAndFilterBlocksWithHighPriority`, and setting aside a part of the block cache for filter/index blocks.
     *
     * However, if the cache is not large enough, we get a 10x performance drop. To help with this, Rocks introduced filter
     * partitioning, which introduces an index over the bloom filters. To utilise this, one must also set
     * `indexType(kTwoLevelIndexSearch)` with `partitionFilters`. Using this, along with `cacheIndexAndFilterBlocksWithHighPriority` and
     * the further option `pinTopLevelIndexAndFilter` which keeps indexes and filter partitions in the cache,
     * we can keep good performance when the bloom filters don't ALL fit in memory, and only the partitions do.
     * Note that with these settings, we can generally increase performance by increasing cache size to fit more of the bloom
     * filter blocks in memory.
     *
     * *Bloom filter efficiency*: with "full" bloom filters, where one filter is compute per SST, and 64MB files,
     * and LZ4 compression that gives a 4x compression ratio, we end up with about 5 million keys per file.
     * With 10 bits per key we end up with 50 mil bits = 6 mil bytes = 6MB bloom filter per file (10%, empirically
     * confirmed). Using online calculators, we can confirm 10 bits per keys gives a good false positive rate 1%.
     * So if we do enable bloom filters, 10 bits per key given 10 million keys per SST is a good tradeoff. 8 bits per key
     * leads to 5MB filter, and a 2% false positive rate, which is still good.
     *
     * *Prefix filter*: we see a solid performance increase (> 10%) when enabling bloom filters for prefixes compared
     * to just full-key filters. Tested with a prefix of (vertex IID length + infix length + vertex iid prefix with type).
     * Note: prefix extractors are defined directly on Options, not tableOptions
     */
    private void configureBloomFilter(BlockBasedTableConfig rocksDBTableOptions) {
        // bloom filter is important for good random-read performance
        rocksDBTableOptions.setFilterPolicy(new BloomFilter(10, false));
        // partition bloom filters to avoid needing to have all bloom filters reside in memory - only index over blooms must
        rocksDBTableOptions.setPartitionFilters(true);
        // WARNING: this must be set to make partitioned filters take effect
        rocksDBTableOptions.setIndexType(IndexType.kTwoLevelIndexSearch);
        rocksDBTableOptions.setOptimizeFiltersForMemory(true);
        // L0 index/filter should always be in memory, and is small
        rocksDBTableOptions.setPinL0FilterAndIndexBlocksInCache(true);
        // to cap memory usage, we must pin filter blocks and indexes in memory
        rocksDBTableOptions.setCacheIndexAndFilterBlocks(true);
        // ensure that the bloom filter partitioning index, plus the data index live in the cache always
        rocksDBTableOptions.setPinTopLevelIndexAndFilter(true);
        // use reserved section of block cache for blooms/index - cause massive thrashing if not using the high priority cache section
        // WARNING: must configure block cache with a reserved region for high priority blocks
        rocksDBTableOptions.setCacheIndexAndFilterBlocksWithHighPriority(true);
    }

    /**
     * Even a moderate block cache has a huge performance impact -- disabled vs enabled (800MB) block cache leads to a 20% reduction
     * in load time. However, there is a memory cost of about 2x the set cache size for using the cache size. So for example,
     * setting a 1GB block cache will lead to 2GB of ram usage during operation, from empirical tests.
     *
     * From various sources (https://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html is an explicit guideline)
     * when most data/working data subset doesn't fit into memory, it is best to give the block cache around 20% of the total memory,
     * and let the OS use the remaining space to prefetch and buffer pages read from disk.
     *
     * This guide also has a comment outlining that the compressed cache is not commonly used and note widely tested,
     * and it is better to let the OS handle pre-fetching pages from disk.
     *
     * Note: the ClockCache exposed in the JNI does not work: setting a 1GB clock cache still leads to an 8MB cache size.
     * In addition, the ClockCache should not be used in production, due to a critical bug that is known:
     * https://github.com/facebook/rocksdb/wiki/Block-Cache.
     *
     * We set aside a portion of the cache for high-priority blocks such as index/bloom filter structures, otherwise we get
     * unacceptable cache thrashing and a performance drop.
     */
    private void configureBlockCache(BlockBasedTableConfig rocksDBTableOptions) {
        long blockCacheSize = dataCacheSize + indexCacheSize;
        float indexAndFilterRatio = ((float) indexCacheSize) / (blockCacheSize);
        // block cache will contain data, plus space reserved for index and bloom filters to make memory usage predictable
        LRUCache blockCache = new LRUCache(blockCacheSize, -1, false, indexAndFilterRatio);
        rocksDBTableOptions.setBlockCache(blockCache);
    }
}
