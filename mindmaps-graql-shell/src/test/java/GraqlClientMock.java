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
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;

public class GraqlClientMock implements GraqlClient {

    private Optional<Session> session = Optional.empty();
    private boolean closed = false;
    private final List<Json> requests = new ArrayList<>();

    public Optional<Session> getSession() {
        return session;
    }

    public boolean isClosed() {
        return closed;
    }

    public List<Json> getRequests() {
        return requests;
    }

    @Override
    public void setSession(Session session) throws IOException {
        assertFalse(this.session.isPresent());
        this.session = Optional.of(session);
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {
        assertFalse(closed);
        closed = true;
    }

    @Override
    public void sendJson(Json json) throws IOException, ExecutionException, InterruptedException {
        requests.add(json);
    }
}
