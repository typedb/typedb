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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.UInt64AddOperator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Grakn implementation with RocksDB
 */
public class RocksGrakn implements Grakn {

    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final Options rocksOptions;
    private final AtomicBoolean isOpen;
    private final RocksProperties properties;
    private final RocksDatabaseManager databaseMgr;

    private RocksGrakn(String directory, Properties properties) {
        this.directory = Paths.get(directory);
        this.properties = new RocksProperties(properties);

        rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true);
        rocksOptions.setMergeOperator(new UInt64AddOperator());
        setOptionsFromProperties();

        databaseMgr = new RocksDatabaseManager(this);
        databaseMgr.loadAll();

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    public static RocksGrakn open(String directory) {
        return open(directory, new Properties());
    }

    public static RocksGrakn open(String directory, Properties properties) {
        return new RocksGrakn(directory, properties);
    }

    private void setOptionsFromProperties() {
        // TODO: configure optimisation paramaters
    }

    Path directory() {
        return directory;
    }

    RocksProperties properties() {
        return properties;
    }

    Options rocksOptions() {
        return rocksOptions;
    }

    @Override
    public RocksSession session(String database, Grakn.Session.Type type) {
        if (databaseMgr.contains(database)) {
            return databaseMgr.get(database).createAndOpenSession(type);
        } else {
            throw new GraknException("There does not exists a database with the name: " + database);
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
            databaseMgr.getAll().parallelStream().forEach(RocksDatabase::close);
            rocksOptions.close();
        }
    }
}
