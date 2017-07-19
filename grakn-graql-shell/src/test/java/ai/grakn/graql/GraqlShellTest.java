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

import ai.grakn.Grakn;
import ai.grakn.client.BatchMutatorClient;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraqlShellTest {

    private GraqlClient client;
    private final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";
    private BatchMutatorClient batchMutatorClient;

    @Before
    public void createMocks() {
        client = mock(GraqlClient.class);

        when(client.connect(any(), any())).thenAnswer(inv -> {
            // Send 'ready' message when connected
            Json message = Json.object(ACTION, ACTION_END);
            ((JsonSession) inv.getArgument(0)).onMessage(message.toString());

            CompletableFuture<Session> session = new CompletableFuture<>();
            session.complete(mock(Session.class, RETURNS_DEEP_STUBS));

            return session;
        });

        when(client.serverIsRunning(any())).thenReturn(true);

        batchMutatorClient = mock(BatchMutatorClient.class);

        when(client.loaderClient(anyString(), anyString())).thenReturn(batchMutatorClient);
    }

   @Test
    public void testDefaultUri() throws IOException {
       GraqlShell.runShell(new String[]{}, expectedVersion, historyFile, client);
       verify(client).connect(any(), eq(URI.create(String.format("ws://%s/shell/remote", Grakn.DEFAULT_URI))));
    }

    @Test
    public void testSpecifiedUri() throws IOException {
        GraqlShell.runShell(new String[]{"-r", "1.2.3.4:5678"}, expectedVersion, historyFile, client);
        verify(client).connect(any(), eq(URI.create("ws://1.2.3.4:5678/shell/remote")));
    }

    @Test
    public void testBatchArgsReachLoaderClient() throws IOException {
        String testFilePath = GraqlShellTest.class.getClassLoader().getResource("test-query.gql").getPath();
        int batchSize = 100;
        int activeTasks = 1000;
        GraqlShell.runShell(new String[]{"-s", String.valueOf(batchSize), "-a", String.valueOf(activeTasks), "-b", testFilePath}, expectedVersion, historyFile, client);
        verify(batchMutatorClient).setNumberActiveTasks(activeTasks);
        verify(batchMutatorClient).setBatchSize(batchSize);
    }

    @Test
    public void testBatchArgsDontHaveToBePresent() {
        String testFilePath = GraqlShellTest.class.getClassLoader().getResource("test-query.gql").getPath();
        GraqlShell.runShell(new String[]{"-b", testFilePath}, expectedVersion, historyFile, client);
        verify(batchMutatorClient, never()).setNumberActiveTasks(anyInt());
        verify(batchMutatorClient, never()).setBatchSize(anyInt());
    }

}
