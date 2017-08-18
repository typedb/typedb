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
 */

package ai.grakn.engine.controller;

import ai.grakn.GraknTx;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.exception;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraqlControllerDeleteTest {

    private static GraknTx mockGraph;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext graphContext = SampleKBContext.preLoad(MovieKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new GraqlController(mockFactory, spark, new MetricRegistry());
    });

    @Before
    public void setupMock(){
        mockQueryBuilder = mock(QueryBuilder.class);

        when(mockQueryBuilder.materialise(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.infer(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.parse(any()))
                .thenAnswer(invocation -> graphContext.tx().graql().parse(invocation.getArgument(0)));

        mockGraph = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");
        when(mockGraph.graql()).thenReturn(mockQueryBuilder);

        when(mockFactory.getGraph(eq(mockGraph.getKeyspace()), any())).thenReturn(mockGraph);
    }

    @Test
    public void DELETEGraqlDelete_GraphCommitCalled(){
        String query = "match $x isa person; limit 1; delete $x;";

        verify(mockGraph, times(0)).commit();

        sendRequest(query);

        verify(mockGraph, times(1)).commit();
    }

    @Test
    public void DELETEMalformedGraqlQuery_ResponseStatusIs400(){
        String query = "match $x isa ; delete;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void DELETEMalformedGraqlQuery_ResponseExceptionContainsSyntaxError(){
        String query = "match $x isa ; delete;";
        Response response = sendRequest(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void DELETEWithNoKeyspace_ResponseStatusIs400(){
        String query = "match $x isa person; limit 1; delete;";

        Response response = RestAssured.with()
                .body(query)
                .post(REST.WebPath.KB.ANY_GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void DELETEWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .post(REST.WebPath.KB.ANY_GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void DELETEGraqlDelete_ResponseStatusIs200(){
        String query = "match $x has name \"Robert De Niro\"; limit 1; delete $x;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void DELETEGraqlDelete_DeleteWasExecutedOnGraph(){
        doAnswer(answer -> {
            graphContext.tx().commit();
            return null;
        }).when(mockGraph).commit();

        String query = "match $x has title \"Godfather\"; delete $x;";

        long movieCountBefore = graphContext.tx().getEntityType("movie").instances().count();

        sendRequest(query);

        // refresh graph
        graphContext.tx().close();

        long movieCountAfter = graphContext.tx().getEntityType("movie").instances().count();

        assertEquals(movieCountBefore - 1, movieCountAfter);
    }

    @Test
    public void DELETEGraqlDeleteNotValid_ResponseStatusCodeIs422(){
        // Not allowed to delete roles with incoming edges
        Response response = sendRequest("match $x label \"production-being-directed\"; delete $x;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void DELETEGraqlDeleteNotValid_ResponseExceptionContainsValidationErrorMessage(){
        // Not allowed to delete roles with incoming edges
        Response response = sendRequest("match $x label \"production-being-directed\"; delete $x;");

        assertThat(exception(response), containsString("cannot be deleted"));
    }

    @Test
    public void DELETEGraqlDelete_ResponseContentTypeIsText(){
        Response response = sendRequest("match $x has name \"Harry\"; limit 1; delete $x;");

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    private Response sendRequest(String query){
        return RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(INFER, false)
                .queryParam(MATERIALISE, false)
                .accept(APPLICATION_TEXT)
                .body(query)
                .post(REST.WebPath.KB.ANY_GRAQL);
    }
}
