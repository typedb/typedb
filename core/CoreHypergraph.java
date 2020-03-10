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

package hypergraph.core;

import hypergraph.Hypergraph;
import hypergraph.common.HypergraphException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Hypergraph implementation with RocksDB
 */
public class CoreHypergraph implements Hypergraph {

    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final Options options;
    private final AtomicBoolean isOpen;
    private final CoreKeyspaceManager keyspaceMgr;

    public static CoreHypergraph open(String directory) {
        return open(directory, new Properties());
    }

    public static CoreHypergraph open(String directory, Properties properties) {
        return new CoreHypergraph(directory, properties);
    }

    private CoreHypergraph(String directory, Properties properties) {
        this.directory = Paths.get(directory);

        options = new Options().setCreateIfMissing(true);
        setOptionsFromProperties(properties);

        keyspaceMgr = new CoreKeyspaceManager(this);
        keyspaceMgr.loadAll();

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    private void setOptionsFromProperties(Properties properties) {
        // TODO: configure optimisation paramaters
    }

    Path directory() {
        return directory;
    }

    Options options() {
        return options;
    }

    @Override
    public CoreSession session(String keyspace) {
        Keyspace k = keyspaceMgr.get(keyspace);
        if (k != null){
            return keyspaceMgr.get(keyspace).sessionCreateAndOpen();
        } else {
            throw new HypergraphException("There does not exists a keyspace with the name: " + keyspace);
        }
    }

    @Override
    public KeyspaceManager keyspaces() {
        return keyspaceMgr;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            for (Keyspace keyspace : keyspaceMgr.getAll()) {
                keyspace.close();
            }
            options.close();
        }
    }

}
