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

package hypergraph.rocks;

import hypergraph.Hypergraph;
import hypergraph.common.exception.HypergraphException;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RocksKeyspaceManager implements Hypergraph.KeyspaceManager {

    private final RocksHypergraph core;
    private final ConcurrentMap<String, RocksKeyspace> keyspaces;

    RocksKeyspaceManager(RocksHypergraph core) {
        this.core = core;
        keyspaces = new ConcurrentHashMap<>();
    }

    void loadAll() {
        File[] keyspaceDirectories = core.directory().toFile().listFiles(File::isDirectory);
        if (keyspaceDirectories != null && keyspaceDirectories.length > 0) {
            Arrays.stream(keyspaceDirectories).parallel().forEach(directory -> {
                String name = directory.getName();
                RocksKeyspace keyspace = RocksKeyspace.loadExistingAndOpen(core, name);
                keyspaces.put(name, keyspace);
            });
        }
    }

    @Override
    public boolean contains(String keyspace) {
        return keyspaces.containsKey(keyspace);
    }

    @Override
    public RocksKeyspace create(String name) {
        if (keyspaces.containsKey(name)) throw new HypergraphException("Keyspace Already Exist: " + name);

        RocksKeyspace keyspace = RocksKeyspace.createNewAndOpen(core, name);
        keyspaces.put(name, keyspace);
        return keyspace;
    }

    @Override
    public RocksKeyspace get(String keyspace) {
        return keyspaces.get(keyspace);
    }

    @Override
    public Set<RocksKeyspace> getAll() {
        return new HashSet<>(keyspaces.values());
    }

    void remove(RocksKeyspace keyspace) {
        keyspaces.remove(keyspace.name());
    }
}
