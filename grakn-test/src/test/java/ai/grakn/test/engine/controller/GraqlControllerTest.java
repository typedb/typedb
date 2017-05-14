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

package ai.grakn.test.engine.controller;

import ai.grakn.GraknGraph;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.GraphContext;
import ai.grakn.test.engine.controller.TasksControllerTest.JsonMapper;
import ai.grakn.util.REST;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import java.util.Collections;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;

import org.junit.runners.MethodSorters;
import spark.Service;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALArrayData;
import static ai.grakn.test.GraknTestEnv.usingTitan;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.EXCEPTION;
import static ai.grakn.util.REST.Response.Graql.ORIGINAL_QUERY;
import static ai.grakn.util.REST.Response.Graql.RESPONSE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//TODO Run in name order until TP Bug #13730 Fixed
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GraqlControllerTest {

    private static final String HOST = "localhost";
    private static final int PORT = 4567;
    private static Service spark;

    private static GraknGraph mockGraph;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknGraphFactory mockFactory;

    private static final JsonMapper jsonMapper = new JsonMapper();

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @BeforeClass
    public static void setup(){
        spark = Service.ignite();
        configureSpark(spark, PORT);

        mockFactory = mock(EngineGraknGraphFactory.class);

        new SystemController(spark);
        new GraqlController(mockFactory, spark);

        spark.awaitInitialization();
    }

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

    @AfterClass
    public static void shutdown(){
        spark.stop();
    }

    @Test
    public void GETGraqlMatch_QueryIsExecuted(){
        String query = "match $x isa person;";
        sendGET(query, APPLICATION_TEXT);

        verify(mockGraph.graql().materialise(anyBoolean()).infer(anyBoolean()))
                .parse(argThat(argument -> argument.equals(query)));
    }

    @Test
    public void GETGraqlMatch_ResponseStatusIs200(){
        Response response = sendGET(APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETMalformedGraqlMatch_ResponseStatusCodeIs400(){
        String query = "match $x isa ;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void GETMalformedGraqlMatch_ResponseExceptionContainsSyntaxError(){
        String query = "match $x isa ;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("syntax error"));
    }

    // This is so that the word "Exception" does not appear on the dashboard when there is a syntax error
    @Test
    public void GETMalformedGraqlMatch_ResponseExceptionDoesNotContainWordException(){
        String query = "match $x isa ;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(exception(response), not(containsString("Exception")));
    }

    @Test
    public void GETGraqlMatchWithInvalidAcceptType_ResponseStatusIs406(){
        Response response = sendGET("invalid");

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(UNSUPPORTED_CONTENT_TYPE.getMessage("invalid")));
    }

    @Test
    public void GETGraqlMatchWithNoKeyspace_ResponseStatusIs400(){
        Response response = RestAssured.with().get(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void GETGraqlMatchWithNoQuery_ResponseStatusIs400(){
        Response response = RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .get(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(QUERY)));
    }

    @Test
    public void GETGraqlMatchNoMaterialise_ResponseStatusIs400(){
        Response response = RestAssured.with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(QUERY, "match $x isa person;")
                .queryParam(INFER, true)
                .accept(APPLICATION_TEXT)
                .get(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(MATERIALISE)));
    }

    @Test
    public void GETGraqlMatchWithReasonerTrue_ReasonerIsOnWhenExecuting(){
        sendGET("match $x isa person;", APPLICATION_TEXT, true, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> arg));
    }

    @Test
    public void GETGraqlMatchWithReasonerFalse_ReasonerIsOffWhenExecuting(){
        sendGET("match $x isa person;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> !arg));
    }

    @Test
    public void GETGraqlMatchWithNoInfer_ResponseStatusIs400(){
        Response response = RestAssured.with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(QUERY, "match $x isa person;")
                .accept(APPLICATION_TEXT)
                .get(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(INFER)));
    }

    @Test
    public void GETGraqlMatchWithMaterialiseFalse_MaterialiseIsOffWhenExecuting(){
        sendGET("match $x isa person;", APPLICATION_TEXT, false, false, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> !arg));
    }

    @Test
    public void GETGraqlMatchWithMaterialiseTrue_MaterialiseIsOnWhenExecuting(){
        sendGET("match $x isa person;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> arg));
    }

    @Test
    public void GETGraqlMatchWithHALTypeAndNumberEmbedded1_ResponsesContainAtMost1Concept(){
        Response response =
                sendGET("match $x isa person;", APPLICATION_HAL, false, true,1);

        jsonResponse(response).asJsonList().forEach(e -> {
             assertThat(e.asJsonMap().get("_embedded").asJsonMap().size(), lessThanOrEqualTo(1));
        });
    }

    @Test
    public void GETGraqlMatchWithHALType_ResponseIsCorrectHal(){
        String queryString = "match $x isa movie;";
        Response response = sendGET(queryString, APPLICATION_HAL);

        Json expectedResponse = renderHALArrayData(
                graphContext.graph().graql().parse(queryString), 0, -1);
        assertThat(jsonResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void GETGraqlMatchWithHALType_ResponseContentTypeIsHal(){
        Response response = sendGET(APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void GETGraqlMatchWithHALTypeAndEmptyResponse_ResponseIsEmptyJsonArray(){
        Response response = sendGET("match $x isa runtime;", APPLICATION_HAL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void GETGraqlMatchWithHALType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void GETGraqlMatchWithTextType_ResponseContentTypeIsGraql(){
        Response response = sendGET(APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void GETGraqlMatchWithTextType_ResponseIsCorrectGraql(){
        Response response = sendGET(APPLICATION_TEXT);

        assertThat(stringResponse(response).length(), greaterThan(0));
        assertThat(stringResponse(response), stringContainsInOrder(Collections.nCopies(10, "isa person")));
    }

    @Test
    public void GETGraqlMatchWithTextType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendGET(APPLICATION_TEXT);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void GETGraqlMatchWithTextTypeAndEmptyResponse_ResponseIsEmptyString(){
        Response response = sendGET("match $x isa \"runtime\";", APPLICATION_TEXT);

        assertThat(stringResponse(response), isEmptyString());
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonType_ResponseContentTypeIsGraqlJson(){
        Response response = sendGET(APPLICATION_JSON_GRAQL);

        assertThat(response.contentType(), equalTo(APPLICATION_JSON_GRAQL));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonType_ResponseIsCorrectGraql(){
        String query = "match $x isa person;";
        Response response = sendGET(APPLICATION_JSON_GRAQL);

        Json expectedResponse = Json.read(
                Printers.json().graqlString(graphContext.graph().graql().parse(query).execute()));
        assertThat(jsonResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendGET(APPLICATION_JSON_GRAQL);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonTypeAndEmptyResponse_ResponseIsEmptyJsonObject(){
        Response response = sendGET("match $x isa \"runtime\";", APPLICATION_JSON_GRAQL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void GETGraqlInsert_ResponseExceptionContainsReadOnlyAllowedMessage(){
        String query = "insert $x isa person;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("read-only"));
    }

    @Test
    public void GETGraqlInsert_ResponseStatusCodeIs405NotSupported(){
        String query = "insert $x isa person;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void GETGraqlAggregateWithHalType_ResponseStatusCodeIs406(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(406));
    }

    @Test
    public void GETGraqlAggregateWithTextType_ResponseStatusIs200(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETGraqlAggregateWithTextType_ResponseIsCorrect(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendGET(query, APPLICATION_TEXT);

        // refresh graph
        graphContext.graph().close();

        int numberPeople = graphContext.graph().getEntityType("person").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    @Test
    public void GETGraqlAggregateWithTextType_ResponseContentTypeIsText(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void GETGraqlCompute_ResponseContainsOriginalQuery(){
        String query = "compute count in person;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void GETGraqlComputeWithTextType_ResponseContentTypeIsText(){
        String query = "compute count in person;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void GETGraqlComputeWithTextType_ResponseStatusIs200(){
        String query = "compute count in person;";
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETGraqlComputeWithTextType_ResponseIsCorrect(){
        String query = "compute count in person;";
        Response response = sendGET(query, APPLICATION_TEXT);

        int numberPeople = graphContext.graph().getEntityType("person").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    @Test
    public void GETGraqlComputeWithHALType_ResponseStatusIs406(){
        String query = "compute count in person;";
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(APPLICATION_HAL));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithTextType_ResponseIsCorrect(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendGET(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(stringResponse(response), containsString("isa has-cast"));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALType_ResponseContentTypeIsHAL(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALType_ResponseStatusIs200(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(200));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALType_ResponseIsCorrect(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(jsonResponse(response).asJsonList().size(), greaterThan(0));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALTypeAndNoPath_ResponseIsEmptyJson(){
        String fromId = graphContext.graph().getResourcesByValue("Marlon Brando").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendGET(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void POSTGraqlInsert_GraphCommitCalled(){
        String query = "insert $x isa person;";

        verify(mockGraph, times(0)).commit();

        sendPOST(query);

        verify(mockGraph, times(1)).commit();
    }

    @Test
    public void POSTGraqlInsert_InsertWasExecutedOnGraph(){
        doAnswer(answer -> {
            graphContext.graph().commit();
            return null;
        }).when(mockGraph).commit();

        String query = "insert $x isa person;";

        int personCountBefore = graphContext.graph().getEntityType("person").instances().size();

        sendPOST(query);

        // refresh graph
        graphContext.graph().close();

        int personCountAfter = graphContext.graph().getEntityType("person").instances().size();

        assertEquals(personCountBefore + 1, personCountAfter);
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
                .post(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void POSTWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .post(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void POSTGraqlMatch_ResponseStatusCodeIs405NotSupported(){
        Response response = sendPOST("match $x isa name;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void POSTGraqlMatch_ResponseExceptionContainsInsertOnlyAllowedMessage(){
        Response response = sendPOST("match $x isa name;");

        assertThat(exception(response), containsString("Only INSERT queries are allowed."));
    }

    @Test
    public void POSTGraqlDelete_ResponseStatusCodeIs405NotSupported(){
        Response response = sendPOST("match $x isa name; delete $x;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void POSTGraqlDelete_ResponseExceptionContainsInsertOnlyAllowedMessage(){
        Response response = sendPOST("match $x isa name; delete $x;");

        assertThat(exception(response), containsString("Only INSERT queries are allowed."));
    }

    @Test
    public void POSTGraqlCompute_ResponseStatusCodeIs405NotSupported(){
        Response response = sendPOST("compute count in name;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void POSTGraqlCompute_ResponseExceptionContainsInsertOnlyAllowedMessage(){
        Response response = sendPOST("compute count in name;");

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
    public void POSTGraqlInsert_ResponseContentTypeIsJson(){
        Response response = sendPOST("insert $x isa movie;");

        assertThat(response.contentType(), equalTo(APPLICATION_JSON));
    }

    @Test
    public void POSTGraqlInsert_ResponseContainsOriginalQuery(){
        String query = "insert $x isa person;";
        Response response = sendPOST(query);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void POSTGraqlInsert_ResponseContainsCorrectNumberOfIds(){
        String query = "insert $x isa person;";
        Response response = sendPOST(query);

        assertThat(1, equalTo(jsonResponse(response).asJsonList().size()));
    }

    @Test
    public void POSTGraqlInsertWithOntology_GraphCommitIsCalled(){
        String query = "insert thing sub entity;";

        verify(mockGraph, times(0)).commit();

        sendPOST(query);

        verify(mockGraph, times(1)).commit();
    }

    @Test
    public void DELETEGraqlDelete_GraphCommitCalled(){
        String query = "match $x has title \"Heat\"; limit 1; delete $x;";

        verify(mockGraph, times(0)).commit();

        sendDELETE(query);

        verify(mockGraph, times(1)).commit();
    }

    @Test
    public void DELETEMalformedGraqlQuery_ResponseStatusIs400(){
        String query = "match $x isa ; delete;";
        Response response = sendDELETE(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void DELETEMalformedGraqlQuery_ResponseExceptionContainsSyntaxError(){
        String query = "match $x isa ; delete;";
        Response response = sendDELETE(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void DELETEWithNoKeyspace_ResponseStatusIs400(){
        String query = "match $x isa person; delete;";

        Response response = RestAssured.with()
                .body(query)
                .delete(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void DELETEWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .delete(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void DELETEGraqlMatch_ResponseStatusCodeIs405NotSupported(){
        Response response = sendDELETE("match $x isa name;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void DELETEGraqlMatch_ResponseExceptionContainsDELETEOnlyAllowedMessage(){
        Response response = sendDELETE("match $x isa name;");

        assertThat(exception(response), containsString("Only DELETE queries are allowed."));
    }

    @Test
    public void DELETEGraqlInsert_ResponseStatusCodeIs405NotSupported(){
        Response response = sendDELETE("insert $x isa person;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void DELETEGraqlInsert_ResponseExceptionContainsDELETEOnlyAllowedMessage(){
        Response response = sendDELETE("insert $x isa person;");

        assertThat(exception(response), containsString("Only DELETE queries are allowed."));
    }

    @Test
    public void DELETEGraqlCompute_ResponseStatusCodeIs405NotSupported(){
        Response response = sendDELETE("compute count in name;");

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void DELETEGraqlCompute_ResponseExceptionContainsDELETEOnlyAllowedMessage(){
        Response response = sendDELETE("compute count in name;");

        assertThat(exception(response), containsString("Only DELETE queries are allowed."));
    }

    @Test
    public void DELETEGraqlDelete_ResponseStatusIs200(){
        String query = "match $x has name \"Robert De Niro\"; limit 1; delete $x;";
        Response response = sendDELETE(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void DELETEGraqlDelete_DeleteWasExecutedOnGraph(){
        doAnswer(answer -> {
            graphContext.graph().commit();
            return null;
        }).when(mockGraph).commit();

        String query = "match $x has name \"Martin Sheen\"; limit 1; delete $x;";

        int personCountBefore = graphContext.graph().getEntityType("person").instances().size();

        sendDELETE(query);

        // refresh graph
        graphContext.graph().close();

        int personCountAfter = graphContext.graph().getEntityType("person").instances().size();

        assertEquals(personCountBefore - 1, personCountAfter);
    }

    @Test
    public void DELETEGraqlDeleteNotValid_ResponseStatusCodeIs422(){
        // Not allowed to delete roles with incoming edges
        Response response = sendDELETE("match $x label \"production-being-directed\"; delete $x;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void DELETEGraqlDeleteNotValid_ResponseExceptionContainsValidationErrorMessage(){
        // Not allowed to delete roles with incoming edges
        Response response = sendDELETE("match $x label \"production-being-directed\"; delete $x;");

        assertThat(exception(response), containsString("cannot be deleted"));
    }

    @Test
    public void DELETEGraqlDelete_ResponseContentTypeIsJson(){
        Response response = sendDELETE("match $x has name \"Harry\"; limit 1; delete $x;");

        assertThat(response.contentType(), equalTo(APPLICATION_JSON));
    }

    @Test
    public void DELETEGraqlDelete_ResponseContainsOriginalQuery(){
        String query = "match $x has name \"Miranda Heart\"; limit 1; delete $x;";
        Response response = sendDELETE(query);

        assertThat(originalQuery(response), equalTo(query));
    }

    private Response sendGET(String acceptType){
        return sendGET("match $x isa person;", acceptType, false, false, -1);
    }

    private Response sendGET(String match, String acceptType){
        return sendGET(match, acceptType, false, false, -1);
    }

    private Response sendGET(String match, String acceptType, boolean reasonser,
                             boolean materialise, int limitEmbedded){
        return RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(QUERY, match)
                .queryParam(INFER, reasonser)
                .queryParam(MATERIALISE, materialise)
                .queryParam(LIMIT_EMBEDDED, limitEmbedded)
                .accept(acceptType)
                .get(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));
    }

    private Response sendPOST(String query){
        return RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .body(query)
                .post(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));
    }

    private Response sendDELETE(String query){
        return RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .body(query)
                .delete(String.format("http://%s:%s%s", HOST, PORT, REST.WebPath.Graph.GRAQL));
    }

    protected static String exception(Response response){
        return response.getBody().as(Json.class, jsonMapper).at(EXCEPTION).asString();
    }

    protected static String stringResponse(Response response){
        return response.getBody().as(Json.class, jsonMapper).at(RESPONSE).asString();
    }

    protected static Json jsonResponse(Response response){
        return response.getBody().as(Json.class, jsonMapper).at(RESPONSE);
    }

    protected static String originalQuery(Response response){
        return response.getBody().as(Json.class, jsonMapper).at(ORIGINAL_QUERY).asString();
    }
}
