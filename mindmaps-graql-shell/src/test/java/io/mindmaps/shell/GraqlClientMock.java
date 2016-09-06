/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.shell;import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.GraqlClient;
import io.mindmaps.graql.GraqlShell;
import io.mindmaps.engine.session.RemoteSession;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

class GraqlClientMock implements GraqlClient {

    private RemoteSession server = new RemoteSession(namespace -> {
        this.namespace = namespace;
        return MindmapsTestGraphFactory.newEmptyGraph();
    });

    private String namespace = null;
    private URI uri;

    String getNamespace() {
        return namespace;
    }

    URI getURI() {
        return uri;
    }

    @Override
    public void connect(Object websocket, URI uri) {
        this.uri = uri;

        GraqlShell client = (GraqlShell) websocket;

        SessionMock serverSession = new SessionMock(client::onMessage);
        server.onConnect(serverSession);

        SessionMock clientSession = new SessionMock(serverSession, server::onMessage);
        try {
            client.onConnect(clientSession);
        } catch (IOException | InterruptedException | ExecutionException e) {
            fail();
        }
    }
}
