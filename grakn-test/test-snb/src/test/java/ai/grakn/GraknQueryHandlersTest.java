/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn;

import ai.grakn.engine.GraknConfig;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.GraknTxFactoryBuilder;
import com.ldbc.driver.DbException;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Date;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * These LDBC SNB tests are not unit tests. They are for debugging the queries while they are being constructed. In
 * order to return answers they need to be executed against a running instance of Grakn with the SNB validation graph
 * loaded.
 */
@Ignore
public class GraknQueryHandlersTest extends TestCase {

    GraknDbConnectionState mockConnectionState;
    ResultReporter mockReporter;
    GraknSession graknSession;


    @Before
    public void setUp() throws Exception {
        super.setUp();

        // connect to the graph
        graknSession = EmbeddedGraknSession.createEngineSession(Keyspace.of("snb"), GraknConfig.create(), GraknTxFactoryBuilder.getInstance());

        // mock the graph connection
        mockConnectionState = mock(GraknDbConnectionState.class);
        when(mockConnectionState.session()).thenReturn(graknSession);


        // mock the result reporter
        mockReporter = mock(ResultReporter.class);
    }

    @Test
    public void testQuery2Execution() throws DbException {
        // mock the query parameters
        LdbcQuery2 mockQuery = mock(LdbcQuery2.class);
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            ((List) args[1]).forEach(System.out::println);
            return null;
        }).when(mockReporter).report(anyInt(),anyList(),any());

        // validation
        when(mockQuery.personId()).thenReturn(4398046511718L);
        when(mockQuery.maxDate()).thenReturn(Date.from(Instant.ofEpochMilli(1290902400000L)));
        when(mockQuery.limit()).thenReturn(20);

        GraknQueryHandlers.LdbcQuery2Handler query2Handler = new GraknQueryHandlers.LdbcQuery2Handler();
        query2Handler.executeOperation(mockQuery,mockConnectionState,mockReporter);
    }

    @Test
    public void testQuery8Execution() throws DbException {
        LdbcQuery8 mockQuery = mock(LdbcQuery8.class);

        // validation
        when(mockQuery.personId()).thenReturn(1099511628362L);
        when(mockQuery.limit()).thenReturn(20);

        GraknQueryHandlers.LdbcQuery8Handler query8Handler = new GraknQueryHandlers.LdbcQuery8Handler();
        query8Handler.executeOperation(mockQuery,mockConnectionState,mockReporter);
    }

    @Test
    public void testQuery13Execution() throws DbException {
        LdbcQuery13 mockQuery = mock(LdbcQuery13.class);

        // validation
        when(mockQuery.person1Id()).thenReturn(2979L);
        when(mockQuery.person2Id()).thenReturn(4398046511979L);

        GraknQueryHandlers.LdbcQuery13Handler query13Handler = new GraknQueryHandlers.LdbcQuery13Handler();
        query13Handler.executeOperation(mockQuery,mockConnectionState,mockReporter);
    }

    @Test
    public void testQuery1Execution() throws DbException {
        // This query is REALLY slow so not registering it in GraphDb
        LdbcQuery1 mockQuery = mock(LdbcQuery1.class);

        // validation
        when(mockQuery.personId()).thenReturn(1099511628726L);
        when(mockQuery.firstName()).thenReturn("Ken");
        when(mockQuery.limit()).thenReturn(20);
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            System.out.println(args[1]);
            return null;
        }).when(mockReporter).report(anyInt(),any(),any());

        GraknQueryHandlers.LdbcQuery1Handler query1Handler = new GraknQueryHandlers.LdbcQuery1Handler();
        query1Handler.executeOperation(mockQuery,mockConnectionState,mockReporter);
    }

    @Test
    public void testShortQuery1Execution() throws DbException {
        LdbcShortQuery1PersonProfile mockQuery = mock(LdbcShortQuery1PersonProfile.class);
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            System.out.println(args[1]);
            return null;
        }).when(mockReporter).report(anyInt(),any(),any());

        // validation query
        when(mockQuery.personId()).thenReturn(2199023257132L);

        GraknShortQueryHandlers.LdbcShortQuery1PersonProfileHandler queryHandler = new GraknShortQueryHandlers.LdbcShortQuery1PersonProfileHandler();
        queryHandler.executeOperation(mockQuery, mockConnectionState, mockReporter);
    }

    @Test
    public void testShortQuery3Execution() throws DbException {
        LdbcShortQuery3PersonFriends mockQuery = mock(LdbcShortQuery3PersonFriends.class);
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            ((List) args[1]).forEach(System.out::println);
            return null;
        }).when(mockReporter).report(anyInt(),anyList(),any());

        // validation query
        when(mockQuery.personId()).thenReturn(2199023257132L);

        GraknShortQueryHandlers.LdbcShortQuery3PersonFriendsHandler queryHandler = new GraknShortQueryHandlers.LdbcShortQuery3PersonFriendsHandler();
        queryHandler.executeOperation(mockQuery, mockConnectionState, mockReporter);
    }

    @Test
    public void testShortQuery2Execution() throws DbException {
        LdbcShortQuery2PersonPosts mockQuery = mock(LdbcShortQuery2PersonPosts.class);
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            ((List) args[1]).forEach(System.out::println);
            return null;
        }).when(mockReporter).report(anyInt(),anyList(),any());

        // validation query
        when(mockQuery.personId()).thenReturn(2199023257132L);
        when(mockQuery.limit()).thenReturn(10);

        GraknShortQueryHandlers.LdbcShortQuery2PersonPostsHandler queryHandler = new GraknShortQueryHandlers.LdbcShortQuery2PersonPostsHandler();
        queryHandler.executeOperation(mockQuery, mockConnectionState, mockReporter);
    }

    @Test
    public void testShortQuery6Execution() throws DbException {
        LdbcShortQuery6MessageForum mockQuery = mock(LdbcShortQuery6MessageForum.class);
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            ((List) args[1]).forEach(System.out::println);
            return null;
        }).when(mockReporter).report(anyInt(),anyList(),any());

        when(mockQuery.messageId()).thenReturn(8590136012L);

        GraknShortQueryHandlers.LdbcShortQuery6MessageForumHandler queryHandler = new GraknShortQueryHandlers.LdbcShortQuery6MessageForumHandler();
        queryHandler.executeOperation(mockQuery, mockConnectionState, mockReporter);
    }

    @After
    public void tearDown() throws Exception {
        graknSession.close();
    }

}