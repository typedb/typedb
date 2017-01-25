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
import org.eclipse.jetty.websocket.api.Session;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraqlShellTest {

    private GraqlClient client;
    private final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    @Before
    public void createMock() {
        client = mock(GraqlClient.class);

        when(client.connect(any(), any())).thenAnswer(inv -> {
            // Send 'ready' message when connected
            Json message = Json.object(ACTION, ACTION_END);
            ((JsonSession) inv.getArgument(0)).onMessage(message.toString());

            CompletableFuture<Session> session = new CompletableFuture<>();
            session.complete(mock(Session.class, RETURNS_DEEP_STUBS));

            return session;
        });
    }

   @Test
    public void testDefaultUri() throws IOException {
       GraqlShell.runShell(new String[]{}, expectedVersion, historyFile, client);
       verify(client).connect(any(), eq(URI.create("ws://localhost:4567/shell/remote")));
    }

    @Test
    public void testSpecifiedUri() throws IOException {
        GraqlShell.runShell(new String[]{"-r", "1.2.3.4:5678"}, expectedVersion, historyFile, client);
        verify(client).connect(any(), eq(URI.create("ws://1.2.3.4:5678/shell/remote")));
    }
}
