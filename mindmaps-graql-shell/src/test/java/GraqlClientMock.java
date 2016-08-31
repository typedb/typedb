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

import io.mindmaps.graql.GraqlClient;
import io.mindmaps.graql.GraqlShell;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

class GraqlClientMock implements GraqlClient {

    private Optional<GraqlShell> shell = Optional.empty();
    private Optional<URI> uri = Optional.empty();
    private final SessionMock session = new SessionMock();
    private boolean sessionProvided = false;
    private boolean closed = false;
    private Optional<String> namespace = Optional.empty();

    Optional<String> getNamespace() {
        return namespace;
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public void connect(GraqlShell shell, URI uri) {
        assert !closed;
        assert !sessionProvided;
        assert !this.shell.isPresent();
        assert !this.uri.isPresent();
        assert !this.namespace.isPresent();

        this.shell = Optional.of(shell);
        this.uri = Optional.of(uri);

        try {
            shell.onConnect(session);
        } catch (IOException | InterruptedException | ExecutionException e) {
            fail();
        }
    }

    @Override
    public void setSession(Session session, String namespace) throws IOException {
        assert !closed;
        assert !sessionProvided;
        assert !this.namespace.isPresent();
        assertEquals(session, this.session);

        sessionProvided = true;
        this.namespace = Optional.of(namespace);
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {
        assert !closed;
        closed = true;
    }

    @Override
    public void sendJson(Json json) throws IOException, ExecutionException, InterruptedException {
        assert !closed;
    }
}
