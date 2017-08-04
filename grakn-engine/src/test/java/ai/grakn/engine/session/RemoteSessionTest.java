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

package ai.grakn.engine.session;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineStatus;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.util.EmbeddedCassandra;
import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static ai.grakn.util.REST.RemoteShell.ACTION_ERROR;
import static ai.grakn.util.REST.RemoteShell.ACTION_INIT;
import static ai.grakn.util.REST.RemoteShell.ACTION_QUERY;
import static ai.grakn.util.REST.RemoteShell.ERROR;
import static ai.grakn.util.REST.RemoteShell.INFER;
import static ai.grakn.util.REST.RemoteShell.KEYSPACE;
import static ai.grakn.util.REST.RemoteShell.MATERIALISE;
import static ai.grakn.util.REST.RemoteShell.OUTPUT_FORMAT;
import static ai.grakn.util.REST.RemoteShell.QUERY;
import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import mjson.Json;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mockito;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class RemoteSessionTest {

    private static final Json INIT_JSON = Json.object(
            ACTION, ACTION_INIT,
            KEYSPACE, "yes",
            OUTPUT_FORMAT, "graql",
            INFER, false,
            MATERIALISE, false
    );

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        EmbeddedCassandra.start();
        Properties properties = GraknEngineConfig.create().getProperties();
        EngineGraknGraphFactory factory = EngineGraknGraphFactory.createAndLoadSystemOntology(properties);
        new SystemController(factory, spark, new GraknEngineStatus(), new MetricRegistry());
    }).port(4567);

    private final BlockingQueue<Json> responses = new LinkedBlockingDeque<>();
    private final RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    private final Set<RemoteSession> remoteSessions = new HashSet<>();

    @Before
    public void setUp() throws IOException {
        doAnswer(invocation -> {
            responses.offer(Json.read((String)invocation.getArgument(0)));
            return null;
        }).when(remoteEndpoint).sendString(any());
    }

    @After
    public void tearDown() {
        responses.clear();

        for (RemoteSession session : remoteSessions) {
            session.onWebSocketClose(1000, "test finished");
        }
    }

    @Test
    public void whenUserMakesAMistake_ANewSessionWillStillFunction() throws Exception {

        String badQuery = "insert r sub resource datatype string; e sub entity has r has nothing;";

        RemoteSession session1 = createRemoteSession();

        sendJson(session1, INIT_JSON);

        pollUntilEndMessage();

        sendJson(session1, Json.object(ACTION, ACTION_QUERY, QUERY, badQuery));
        sendJson(session1, Json.object(ACTION, ACTION_END));

        String errorMessage = null;

        for (Json message : pollUntilEndMessage()) {
            if (message.is(ACTION, ACTION_ERROR)) {
                errorMessage = message.at(ACTION_ERROR).asString();
            }
        }

        // We expect an error message
        assertNotNull(errorMessage);

        RemoteSession session2 = createRemoteSession();

        sendJson(session2, INIT_JSON);

        for (Json message : pollUntilEndMessage()) {
            assertNull("There should be no errors when opening a new session", message.at(ERROR));
        }
    }

    private RemoteSession createRemoteSession() throws InterruptedException {
        Session mockSession = mock(Session.class, Mockito.RETURNS_DEEP_STUBS);
        when(mockSession.isOpen()).thenReturn(true);
        when(mockSession.getRemote()).thenReturn(remoteEndpoint);

        RemoteSession session = RemoteSession.create();
        remoteSessions.add(session);
        session.onWebSocketConnect(mockSession);

        return session;
    }

    private void sendJson(RemoteSession session, Json json) {
        session.onWebSocketText(json.toString());
    }

    private List<Json> pollUntilEndMessage() throws InterruptedException {
        List<Json> list = new ArrayList<>();

        Json response;
        do {
            response = responses.poll(120, TimeUnit.SECONDS);
            list.add(response);
        } while (!response.at(ACTION).asString().equals(ACTION_END));

        return list;
    }
}