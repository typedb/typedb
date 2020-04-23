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
import grakn.core.kb.server.exception.GraknServerException;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * KeyspaceManager used to retrieve and delete keyspaces in Cassandra Storage.
 */
public class KeyspaceManager {
    private final CqlSession cqlSession;
    private final JanusGraphFactory janusGraphFactory;
    private final SessionFactory sessionFactory;
    private final Set<String> internals = new HashSet<>(Arrays.asList("system_traces", "system", "system_distributed", "system_schema", "system_auth"));

    public KeyspaceManager(CqlSession cqlSession, JanusGraphFactory janusGraphFactory, SessionFactory sessionFactory) {
        this.cqlSession = cqlSession;
        this.janusGraphFactory = janusGraphFactory;
        this.sessionFactory = sessionFactory;
    }

    public Set<Keyspace> keyspaces() {
        return cqlSession.refreshSchema().getKeyspaces().values().stream()
                .map(keyspaceMetadata -> new KeyspaceImpl(keyspaceMetadata.getName().toString()))
                .filter(keyspace -> !internals.contains(keyspace.name()))
                .collect(Collectors.toSet());
    }

    public void delete(Keyspace keyspace) {
        if (!keyspaces().contains(keyspace)) {
            throw GraknServerException.create("It is not possible to delete keyspace [" + keyspace.name() + "] as it does not exist.");
        } else {
            // removing references to open keyspaces JanusGraph instances
            sessionFactory.deleteKeyspace(keyspace);
            // actually remove the keyspace
            janusGraphFactory.drop(keyspace.name());
        }
    }
}