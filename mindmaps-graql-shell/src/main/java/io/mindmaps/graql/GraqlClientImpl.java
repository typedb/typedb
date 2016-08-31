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

package io.mindmaps.graql;

import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.constants.RESTUtil.RemoteShell.*;

public class GraqlClientImpl implements GraqlClient {

    private final CompletableFuture<Session> session = new CompletableFuture<>();

    @Override
    public void setSession(Session session) throws IOException {
        sendJson(Json.object(ACTION, ACTION_NAMESPACE, NAMESPACE, "HELLOOOOOO"), session);
        this.session.complete(session);
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {
        session.get().close();
    }

    @Override
    public void sendJson(Json json) throws IOException, ExecutionException, InterruptedException {
        session.get().getRemote().sendString(json.toString());
    }

    private void sendJson(Json json, Session session) throws IOException {
        session.getRemote().sendString(json.toString());
    }
}
