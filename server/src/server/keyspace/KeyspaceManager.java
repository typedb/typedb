/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */

package grakn.core.server.keyspace;

import com.datastax.driver.core.Cluster;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * KeyspaceManager used to store all existing keyspaces inside Grakn system keyspace.
 */
public class KeyspaceManager {
    private final Cluster storage;
    private final Set<String> internals = new HashSet<>(Arrays.asList("system_traces", "system", "system_distributed", "system_schema", "system_auth"));
    
    public KeyspaceManager(Cluster storage) {
        this.storage = storage;
    }

    public Set<KeyspaceImpl> keyspaces() {
        return storage.connect().getCluster().getMetadata().getKeyspaces().stream()
                .map(keyspaceMetadata -> KeyspaceImpl.of(keyspaceMetadata.getName()))
                .filter(keyspace -> !internals.contains(keyspace.name()))
                .collect(Collectors.toSet());
    }
}