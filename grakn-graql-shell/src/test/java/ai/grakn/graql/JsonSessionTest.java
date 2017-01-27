/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.graql;

import mjson.Json;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JsonSessionTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private GraqlClient client;
    private RemoteEndpoint remote;
    private final URI uri = URI.create("localhost:4567");

    @Before
    public void setUp() {
        client = mock(GraqlClient.class);
        Session session = mock(Session.class);
        remote = mock(RemoteEndpoint.class);
        CompletableFuture<Session> future = new CompletableFuture<>();
        future.complete(session);

        when(client.connect(any(), any())).thenReturn(future);
        when(session.getRemote()).thenReturn(remote);
    }

    @Test
    public void whenSendJsonSeesAWebSocketExceptionThenItShouldThrowItBack() throws IOException {
        doThrow(WebSocketException.class).when(remote).sendString(any());

        JsonSession jsonSession = new JsonSession(client, uri);

        exception.expect(WebSocketException.class);

        jsonSession.sendJson(Json.object(ACTION, ACTION_END));
    }

    @Test
    public void whenCreatingAJsonSessionNoNewNonDaemonThreadsShouldBeCreated() {
        long activeNonDaemonThreadsBefore =
                Thread.getAllStackTraces().keySet().stream().filter(thread -> !thread.isDaemon()).count();

        new JsonSession(client, uri).sendJson(Json.object());

        long activeNonDaemonThreadsAfter =
                Thread.getAllStackTraces().keySet().stream().filter(thread -> !thread.isDaemon()).count();

        assertEquals(activeNonDaemonThreadsBefore, activeNonDaemonThreadsAfter);
    }
}