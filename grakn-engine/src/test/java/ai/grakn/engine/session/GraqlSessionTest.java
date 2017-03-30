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

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.analytics.CountQuery;
import com.google.common.collect.ImmutableList;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static ai.grakn.util.REST.RemoteShell.QUERY;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraqlSessionTest {

    @Test
    public void whenRunningAComputeQueryThenExiting_TheComputeQueryIsKilled() throws ExecutionException, InterruptedException {
        Session jettySesssion = mock(Session.class, RETURNS_DEEP_STUBS);

        GraknSession factory = mock(GraknSession.class);
        GraknGraph graph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);
        QueryBuilder qb = mock(QueryBuilder.class);
        CountQuery count = mock(CountQuery.class);

        when(factory.open(GraknTxType.WRITE)).thenReturn(graph);
        when(graph.graql()).thenReturn(qb);
        when(qb.infer(false)).thenReturn(qb);
        when(qb.materialise(false)).thenReturn(qb);
        when(qb.parseList("compute count;")).thenReturn(ImmutableList.of(count));

        GraqlSession session = new GraqlSession(jettySesssion, factory, "json", false, false, false);
        session.receiveQuery(Json.object(QUERY, "compute count;"));
        session.executeQuery().get();

        // Make sure `kill` is called only after closing
        verify(count, never()).kill();
        session.close();
        verify(count).kill();
    }
}