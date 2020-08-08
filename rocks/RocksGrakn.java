/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.rocks;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.options.GraknOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.UInt64AddOperator;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.exception.ErrorMessage.DatabaseManager.DATABASE_NOT_FOUND;

/**
 * A Grakn implementation with RocksDB
 */
public class RocksGrakn implements Grakn {

    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final GraknOptions.Global options;
    private final Options rocksConfig;
    private final AtomicBoolean isOpen;
    private final RocksDatabaseManager databaseMgr;

    private RocksGrakn(Path directory, GraknOptions.Global options) {
        this.directory = directory;
        this.options = options;
        this.rocksConfig = new Options()
                .setCreateIfMissing(true)
                .setMergeOperator(new UInt64AddOperator());

        databaseMgr = new RocksDatabaseManager(this);
        databaseMgr.loadAll();

        isOpen = new AtomicBoolean(true);
    }

    public static RocksGrakn open(Path directory) {
        return open(directory, new GraknOptions.Global());
    }

    public static RocksGrakn open(Path directory, GraknOptions.Global options) {
        return new RocksGrakn(directory, options);
    }

    Path directory() {
        return directory;
    }

    Options rocksConfig() {
        return rocksConfig;
    }

    @Override
    public GraknOptions.Global options() {
        return options;
    }

    @Override
    public RocksSession session(String database, Grakn.Session.Type type) {
        return session(database, type, new GraknOptions.Session());
    }

    @Override
    public RocksSession session(String database, Session.Type type, GraknOptions.Session options) {
        if (databaseMgr.contains(database)) {
            return databaseMgr.get(database).createAndOpenSession(type, options);
        } else {
            throw new GraknException(DATABASE_NOT_FOUND.message(database));
        }
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
            databaseMgr.all().parallelStream().forEach(RocksDatabase::close);
            rocksConfig.close();
        }
    }
}
