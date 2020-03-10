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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final CoreKeyspaceManager keyspaces;

    public static CoreHypergraph open(String directory) {
        return open(directory, new Properties());
    }

    public static CoreHypergraph open(String directory, Properties properties) {
        return new CoreHypergraph(directory, properties);
    }

    private CoreHypergraph(String directory, Properties properties) {
        this.directory = Paths.get(directory);

        keyspaces = new CoreKeyspaceManager();
        options = new Options().setCreateIfMissing(true);

        setOptionsFromProperties(properties);
        loadIndexes();

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    private void setOptionsFromProperties(Properties properties) {
        // TODO: configure optimisation paramaters
    }

    private void loadIndexes() {
        // TODO: load indexes for every pre-existing keyspace
    }

    Path directory() {
        return directory;
    }

    Options options() {
        return options;
    }

    @Override
    public CoreSession session(String keyspace) {
        Keyspace k = keyspaces.get(keyspace);
        if (k != null){
            return keyspaces.get(keyspace).sessionCreateAndOpen();
        } else {
            throw new HypergraphException("There does not exists a keyspace with name: " + keyspace);
        }
    }

    @Override
    public KeyspaceManager keyspaces() {
        return keyspaces;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            for (Keyspace keyspace : keyspaces.getAll()) {
                keyspace.close();
            }
            options.close();
        }
    }

    class CoreKeyspaceManager implements KeyspaceManager {

        private final Map<String, CoreKeyspace> keyspaces;
        CoreKeyspaceManager() {
            keyspaces = new ConcurrentHashMap<>();
        }

        @Override
        public CoreKeyspace create(String name) {
            CoreKeyspace keyspace = new CoreKeyspace(CoreHypergraph.this, name);
            keyspaces.put(name, keyspace);
            return keyspace;
        }

        @Override
        public CoreKeyspace get(String keyspace) {
            return keyspaces.get(keyspace);
        }

        @Override
        public Set<Hypergraph.Keyspace> getAll() {
            return new HashSet<>(keyspaces.values());
        }

        @Override
        public void delete(String keyspace) {
            // TODO
        }
    }

}
