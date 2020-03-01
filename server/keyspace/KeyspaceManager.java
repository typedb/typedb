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
 */

package grakn.core.server.keyspace;

import com.datastax.oss.driver.api.core.CqlSession;
import grakn.core.kb.server.keyspace.Keyspace;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * KeyspaceManager used to retrieve all available keyspaces in Cassandra Storage.
 */
public class KeyspaceManager {
    private final CqlSession cqlSession;
    private final Set<String> internals = new HashSet<>(Arrays.asList("system_traces", "system", "system_distributed", "system_schema", "system_auth"));

    public KeyspaceManager(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public Set<Keyspace> keyspaces() {
        return cqlSession.refreshSchema().getKeyspaces().values().stream()
                .map(keyspaceMetadata -> new KeyspaceImpl(keyspaceMetadata.getName().toString()))
                .filter(keyspace -> !internals.contains(keyspace.name()))
                .collect(Collectors.toSet());
    }
}