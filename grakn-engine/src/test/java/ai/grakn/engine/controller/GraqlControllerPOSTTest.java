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

import ai.grakn.GraknGraph;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.engine.controller.GraqlControllerGETTest.exception;
import static ai.grakn.engine.controller.GraqlControllerGETTest.stringResponse;
import static ai.grakn.engine.controller.GraqlControllerGETTest.jsonResponse;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
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

public class GraqlControllerPOSTTest {

    private static GraknGraph mockGraph;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknGraphFactory mockFactory = mock(EngineGraknGraphFactory.class);

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

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
                .thenAnswer(invocation -> graphContext.graph().graql().parse(invocation.getArgument(0)));

        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");
        when(mockGraph.graql()).thenReturn(mockQueryBuilder);

        when(mockFactory.getGraph(eq(mockGraph.getKeyspace()), any())).thenReturn(mockGraph);
    }

    @Test
    public void POSTGraqlInsert_InsertWasExecutedOnGraph(){
        doAnswer(answer -> {
            graphContext.graph().commit();
            return null;
        }).when(mockGraph).commit();

        String query = "insert $x isa movie;";

        int genreCountBefore = graphContext.graph().getEntityType("movie").instances().size();

        sendPOST(query);

        // refresh graph
        graphContext.graph().close();

        int genreCountAfter = graphContext.graph().getEntityType("movie").instances().size();

        assertEquals(genreCountBefore + 1, genreCountAfter);
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseStatusIs400(){
        String query = "insert $x isa ;";
        Response response = sendPOST(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseExceptionContainsSyntaxError(){
        String query = "insert $x isa ;";
        Response response = sendPOST(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void POSTWithNoKeyspace_ResponseStatusIs400(){
        String query = "insert $x isa person;";

        Response response = RestAssured.with()
                .body(query)
                .post(REST.WebPath.Graph.GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void POSTWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .post(REST.WebPath.Graph.GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void POSTGraqlMatch_ResponseStatusCodeIs405NotSupported(){
        Response response = sendPOST("match $x isa person;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void POSTGraqlMatch_ResponseExceptionContainsInsertOnlyAllowedMessage(){
        Response response = sendPOST("match $x isa person;");

        assertThat(exception(response), containsString("Only INSERT queries are allowed."));
    }

    @Test
    public void POSTGraqlDelete_ResponseStatusCodeIs405NotSupported(){
        Response response = sendPOST("match $x isa person; delete $x;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void POSTGraqlDelete_ResponseExceptionContainsInsertOnlyAllowedMessage(){
        Response response = sendPOST("match $x isa person; delete $x;");

        assertThat(exception(response), containsString("Only INSERT queries are allowed."));
    }

    @Test
    public void POSTGraqlCompute_ResponseStatusCodeIs405NotSupported(){
        Response response = sendPOST("compute count in person;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void POSTGraqlCompute_ResponseExceptionContainsInsertOnlyAllowedMessage(){
        Response response = sendPOST("compute count in person;");

        assertThat(exception(response), containsString("Only INSERT queries are allowed."));
    }

    @Test
    public void POSTGraqlInsert_ResponseStatusIs200(){
        String query = "insert $x isa person;";
        Response response = sendPOST(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void POSTGraqlInsertNotValid_ResponseStatusCodeIs422(){
        Response response = sendPOST("insert person plays movie;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void POSTGraqlInsertNotValid_ResponseExceptionContainsValidationErrorMessage(){
        Response response = sendPOST("insert person plays movie;");

        assertThat(exception(response), containsString("is not of type"));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseContentTypeIsJson(){
        Response response = sendPOST("insert $x isa person;", APPLICATION_JSON_GRAQL);

        assertThat(response.contentType(), equalTo(APPLICATION_JSON_GRAQL));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseIsCorrectJson(){
        Response response = sendPOST("insert $x isa person;", APPLICATION_JSON_GRAQL);

        assertThat(jsonResponse(response).asJsonList().size(), equalTo(1));
    }

    @Test
    public void POSTGraqlInsertWithTextType_ResponseIsTextType(){
        Response response = sendPOST("insert $x isa person;", APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void POSTGraqlInsertWithTextType_ResponseIsCorrectText(){
        Response response = sendPOST("insert $x isa person;", APPLICATION_TEXT);

        assertThat(stringResponse(response), containsString("isa person"));
    }

    @Test
    public void POSTGraqlInsertWithHALType_ErrorIsThrown(){
        Response response = sendPOST("insert $x isa person;", APPLICATION_HAL);

        assertThat(exception(response), containsString("Unsupported query type in HAL formatter"));
    }

    @Test
    public void POSTGraqlInsertWithOntology_GraphCommitIsCalled(){
        String query = "insert thingy sub entity;";

        verify(mockGraph, times(0)).commit();

        sendPOST(query);

        verify(mockGraph, times(1)).commit();
    }

    private Response sendPOST(String query){
        return sendPOST(query, APPLICATION_TEXT);
    }

    private Response sendPOST(String query, String acceptType){
        return RestAssured.with()
                .accept(acceptType)
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .body(query)
                .post(REST.WebPath.Graph.GRAQL);
    }
}
