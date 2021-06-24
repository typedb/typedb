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

import com.google.ortools.Loader;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ClockCache;
import org.rocksdb.RocksDB;
import org.rocksdb.UInt64AddOperator;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.TYPEDB_CLOSED;
import static java.lang.Math.max;

public class RocksTypeDB implements TypeDB {

    private static final Logger LOG = LoggerFactory.getLogger(RocksTypeDB.class);
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();

    static {
        RocksDB.loadLibrary();
        Loader.loadNativeLibraries();
        ErrorMessage.loadConstants();
    }

    private final Options.Database typeDBOptions;
    private final org.rocksdb.Options rocksDBOptions;
    private final RocksDatabaseManager databaseMgr;
    private final AtomicBoolean isOpen;

    protected RocksTypeDB(Options.Database options, Factory.DatabaseManager databaseMgrFactory) {
        if (!Executors.isInitialised()) Executors.initialise(MAX_THREADS);
        this.typeDBOptions = options;
        this.rocksDBOptions = initRocksDBOptions();
        this.databaseMgr = databaseMgrFactory.databaseManager(this);
        this.databaseMgr.loadAll();
        this.isOpen = new AtomicBoolean(true);
    }

    private org.rocksdb.Options initRocksDBOptions() {
        return new org.rocksdb.Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(128 * SizeUnit.MB)
                .setMaxWriteBufferNumber(max(MAX_THREADS / 2, 1))
                .setMaxWriteBufferNumberToMaintain(max(MAX_THREADS / 8, 1))
                .setMinWriteBufferNumberToMerge(max(MAX_THREADS / 16, 1))
                .setLevel0FileNumCompactionTrigger(MAX_THREADS * 4)
                .setMaxSubcompactions(MAX_THREADS)
                .setMaxBackgroundJobs(MAX_THREADS)
                .setUnorderedWrite(true)
                .setTableFormatConfig(initRocksDBTableOptions())
                .setMergeOperator(new UInt64AddOperator());
    }

    private BlockBasedTableConfig initRocksDBTableOptions() {
        BlockBasedTableConfig rocksDBTableOptions = new BlockBasedTableConfig();
        long blockSize = Math.round(Runtime.getRuntime().maxMemory() * 0.4); // TODO: generalise through typedb.properties
        ClockCache uncompressedCache = new ClockCache(blockSize);
        ClockCache compressedCache = new ClockCache(blockSize);
        rocksDBTableOptions.setBlockCache(uncompressedCache).setBlockCacheCompressed(compressedCache);
        rocksDBTableOptions.setFilterPolicy(new BloomFilter(16, false));
        return rocksDBTableOptions;
    }

    public static RocksTypeDB open(Path directory, Factory typeDBFactory) {
        return open(new Options.Database().dataDir(directory), typeDBFactory);
    }

    public static RocksTypeDB open(Options.Database options) {
        return open(options, new RocksFactory());
    }

    public static RocksTypeDB open(Options.Database options, Factory typeDBFactory) {
        return typeDBFactory.typedb(options);
    }

    public Path directory() {
        return typeDBOptions.dataDir();
    }

    org.rocksdb.Options rocksDBOptions() {
        return rocksDBOptions;
    }

    public Options.Database options() {
        return typeDBOptions;
    }

    @Override
    public RocksSession session(String database, Arguments.Session.Type type) {
        return session(database, type, new Options.Session());
    }

    @Override
    public RocksSession session(String database, Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw TypeDBException.of(TYPEDB_CLOSED);
        if (databaseMgr.contains(database)) return databaseMgr.get(database).createAndOpenSession(type, options);
        else throw TypeDBException.of(DATABASE_NOT_FOUND, database);
    }

    @Override
    public RocksDatabaseManager databases() {
        return databaseMgr;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            closeResources();
        }
    }

    /**
     * Responsible for committing the initial schema of a database.
     * A different implementation of this class may override it.
     */
    protected void closeResources() {
        databaseMgr.close();
        rocksDBOptions.close();
    }
}
