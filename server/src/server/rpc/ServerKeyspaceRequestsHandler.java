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

package grakn.core.server.rpc;

import grakn.core.server.exception.GraknServerException;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionFactory;
import grakn.protocol.keyspace.KeyspaceProto;

import java.util.stream.Collectors;

public class ServerKeyspaceRequestsHandler implements KeyspaceRequestsHandler {
    private final KeyspaceManager keyspaceManager;
    private final SessionFactory sessionFactory;
    private final JanusGraphFactory janusGraphFactory;


    public ServerKeyspaceRequestsHandler(KeyspaceManager keyspaceManager, SessionFactory sessionFactory, JanusGraphFactory janusGraphFactory) {
        this.keyspaceManager = keyspaceManager;
        this.sessionFactory = sessionFactory;
        this.janusGraphFactory = janusGraphFactory;
    }

    @Override
    public Iterable<String> retrieve(KeyspaceProto.Keyspace.Retrieve.Req request) {
        return this.keyspaceManager.keyspaces().stream().map(KeyspaceImpl::name).collect(Collectors.toSet());
    }

    @Override
    public void delete(KeyspaceProto.Keyspace.Delete.Req request) {
        KeyspaceImpl keyspace = KeyspaceImpl.of(request.getName());

        if (!keyspaceManager.keyspaces().contains(keyspace)) {
            throw GraknServerException.create("It is not possible to delete keyspace [" + keyspace.name() + "] as it does not exist.");
        }
        else {
            // removing references to open keyspaces JanusGraph instances
            sessionFactory.deleteKeyspace(keyspace);
            // actually remove the keyspace
            janusGraphFactory.drop(keyspace.name());
        }
    }
}
