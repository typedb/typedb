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

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class CoreKeyspaceManager implements Hypergraph.KeyspaceManager {

    private final CoreHypergraph core;
    private final Map<String, CoreKeyspace> keyspaces;

    CoreKeyspaceManager(CoreHypergraph core) {
        this.core = core;
        keyspaces = new ConcurrentHashMap<>();
    }

    void loadAll() {
        File[] keyspaceDirectories = core.directory().toFile().listFiles(File::isDirectory);
        if (keyspaceDirectories != null && keyspaceDirectories.length > 0) {
            Arrays.stream(keyspaceDirectories).parallel().forEach(directory -> {
                String name = directory.getName();
                CoreKeyspace keyspace = new CoreKeyspace(core, name);
                keyspace.loadAndOpen();
                keyspaces.put(name, keyspace);
            });
        }
    }

    @Override
    public CoreKeyspace create(String name) {
        if (keyspaces.containsKey(name)) throw new HypergraphException("Keyspace Already Exist: " + name);

        CoreKeyspace keyspace = new CoreKeyspace(core, name).initialiseAndOpen();
        keyspaces.put(name, keyspace);
        return keyspace;
    }

    @Override
    public CoreKeyspace get(String keyspace) {
        return keyspaces.get(keyspace);
    }

    @Override
    public Set<CoreKeyspace> getAll() {
        return new HashSet<>(keyspaces.values());
    }
}
