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
import ai.grakn.GraknTxType;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.GraphContext;
import ai.grakn.test.engine.controller.TasksControllerTest.JsonMapper;
import com.jayway.restassured.response.Response;
import java.util.Collections;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import spark.Service;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALArrayData;
import static ai.grakn.test.GraknTestEnv.usingTitan;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_PARAMETERS;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.EXCEPTION;
import static ai.grakn.util.REST.Response.Graql.ORIGINAL_QUERY;
import static ai.grakn.util.REST.Response.Graql.RESPONSE;
import static ai.grakn.util.REST.WebPath.Graph.GRAQL;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.READ)).thenReturn(mockGraph);
    }

    @AfterClass
    public static void shutdown(){
        spark.stop();
    }

    @Test
    public void sendingGraqlMatch_QueryIsExecuted(){
        String query = "match $x isa person;";
        sendMatch(query, APPLICATION_TEXT);

        verify(mockGraph.graql().materialise(anyBoolean()).infer(anyBoolean()))
                .parse(argThat(argument -> argument.equals(query)));
    }

    @Test
    public void sendingGraqlMatch_ResponseStatusIs200(){
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void sendingMalformedGraqlMatch_ResponseStatusCodeIs400(){
        String query = "match $x isa ;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void sendingMalformedGraqlMatch_ResponseExceptionContainsSyntaxError(){
        String query = "match $x isa ;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("syntax error"));
    }

    // This is so that the word "Exception" does not appear on the dashboard when there is a syntax error
    @Test
    public void sendingMalformedGraqlMatch_ResponseExceptionDoesNotContainWordException(){
        String query = "match $x isa ;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(exception(response), not(containsString("Exception")));
    }

    @Test
    public void sendingGraqlMatchWithInvalidAcceptType_ResponseStatusIs406(){
        Response response = sendMatch("invalid");

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(UNSUPPORTED_CONTENT_TYPE.getMessage("invalid")));
    }

    @Test
    public void sendingGraqlMatchWithNoKeyspace_ResponseStatusIs400(){
        Response response = with().get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void sendingGraqlMatchWithNoQuery_ResponseStatusIs400(){
        Response response = with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage(QUERY)));
    }

    @Test
    public void sendingGraqlMatchNoMaterialise_ResponseStatusIs400(){
        Response response = with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(QUERY, "match $x isa person;")
                .queryParam(INFER, true)
                .accept(APPLICATION_TEXT)
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage(MATERIALISE)));
    }

    @Test
    public void sendingGraqlMatchWithReasonerTrue_ReasonerIsOnWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, true, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> arg));
    }

    @Test
    public void sendingGraqlMatchWithReasonerFalse_ReasonerIsOffWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> !arg));
    }

    @Test
    public void sendingGraqlMatchWithNoInfer_ResponseStatusIs400(){
        Response response = with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(QUERY, "match $x isa person;")
                .accept(APPLICATION_TEXT)
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage(INFER)));
    }

    @Test
    public void sendingGraqlMatchWithMaterialiseFalse_MaterialiseIsOffWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, false, false, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> !arg));
    }

    @Test
    public void sendingGraqlMatchWithMaterialiseTrue_MaterialiseIsOnWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> arg));
    }

    @Test
    public void sendingGraqlMatchWithHALTypeAndNumberEmbedded1_ResponsesContainAtMost1Concept(){
        Response response =
                sendMatch("match $x isa person;", APPLICATION_HAL, false, true,1);

        jsonResponse(response).asJsonList().forEach(e -> {
             assertThat(e.asJsonMap().get("_embedded").asJsonMap().size(), lessThanOrEqualTo(1));
        });
    }

    @Test
    public void sendingGraqlMatchWithHALType_ResponseIsCorrectHal(){
        String queryString = "match $x isa movie;";
        Response response = sendMatch(queryString, APPLICATION_HAL);

        Json expectedResponse = renderHALArrayData(
                graphContext.graph().graql().parse(queryString), 0, -1);
        assertThat(jsonResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void sendingGraqlMatchWithHALType_ResponseContentTypeIsHal(){
        Response response = sendMatch(APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void sendingGraqlMatchWithHALTypeAndEmptyResponse_ResponseIsEmptyJsonArray(){
        Response response = sendMatch("match $x isa runtime;", APPLICATION_HAL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void sendingGraqlMatchWithHALType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_HAL);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void sendingGraqlMatchWithTextType_ResponseContentTypeIsGraql(){
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void sendingGraqlMatchWithTextType_ResponseIsCorrectGraql(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(stringResponse(response).length(), greaterThan(0));
        assertThat(stringResponse(response), stringContainsInOrder(Collections.nCopies(10, "isa person")));
    }

    @Test
    public void sendingGraqlMatchWithTextType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void sendingGraqlMatchWithTextTypeAndEmptyResponse_ResponseIsEmptyString(){
        Response response = sendMatch("match $x isa \"runtime\";", APPLICATION_TEXT);

        assertThat(stringResponse(response), isEmptyString());
    }

    @Test
    public void sendingGraqlMatchWithGraqlJsonType_ResponseContentTypeIsGraqlJson(){
        Response response = sendMatch(APPLICATION_JSON_GRAQL);

        assertThat(response.contentType(), equalTo(APPLICATION_JSON_GRAQL));
    }

    @Test
    public void sendingGraqlMatchWithGraqlJsonType_ResponseIsCorrectGraql(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_JSON_GRAQL);

        Json expectedResponse = Json.read(
                Printers.json().graqlString(graphContext.graph().graql().parse(query).execute()));
        assertThat(jsonResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void sendingGraqlMatchWithGraqlJsonType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_JSON_GRAQL);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void sendingGraqlMatchWithGraqlJsonTypeAndEmptyResponse_ResponseIsEmptyJsonObject(){
        Response response = sendMatch("match $x isa \"runtime\";", APPLICATION_JSON_GRAQL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void sendingGraqlInsert_ResponseExceptionContainsReadOnlyAllowedMessage(){
        String query = "insert $x isa person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("read-only"));
    }

    @Test
    public void sendingGraqlInsert_ResponseStatusCodeIs405NotSupported(){
        String query = "insert $x isa person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(405));
    }

    @Test
    public void sendingGraqlAggregateWithHalType_ResponseStatusCodeIs406(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(406));
    }

    @Test
    public void sendingGraqlAggregateWithTextType_ResponseStatusIs200(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void sendingGraqlAggregateWithTextType_ResponseIsCorrect(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        int numberPeople = graphContext.graph().getEntityType("person").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    @Test
    public void sendingGraqlAggregateWithTextType_ResponseContentTypeIsText(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void sendingGraqlCompute_ResponseContainsOriginalQuery(){
        String query = "compute count in person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void sendingGraqlComputeWithTextType_ResponseContentTypeIsText(){
        String query = "compute count in person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void sendingGraqlComputeWithTextType_ResponseStatusIs200(){
        String query = "compute count in person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void sendingGraqlComputeWithTextType_ResponseIsCorrect(){
        String query = "compute count in person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        int numberPeople = graphContext.graph().getEntityType("person").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    @Test
    public void sendingGraqlComputeWithHALType_ResponseStatusIs406(){
        String query = "compute count in person;";
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(APPLICATION_HAL));
    }

    @Test
    public void sendingGraqlComputePathWithTextType_ResponseIsCorrect(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(stringResponse(response), containsString("isa has-cast"));
    }

    @Test
    public void sendingGraqlComputePathWithHALType_ResponseContentTypeIsHAL(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void sendingGraqlComputePathWithHALType_ResponseStatusIs200(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void sendingGraqlComputePathWithHALType_ResponseIsCorrect(){
        assumeTrue(usingTitan());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(jsonResponse(response).asJsonList().size(), greaterThan(0));
    }

    @Test
    public void sendingGraqlComputePathWithHALTypeAndNoPath_ResponseIsEmptyJson(){
        String fromId = graphContext.graph().getResourcesByValue("Marlon Brando").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("Kermit The Frog").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    private Response sendMatch(String acceptType){
        return sendMatch("match $x isa person;", acceptType, false, false, -1);
    }

    private Response sendMatch(String match, String acceptType){
        return sendMatch(match, acceptType, false, false, -1);
    }

    private Response sendMatch(String match, String acceptType, boolean reasonser,
                               boolean materialise, int limitEmbedded){
        return with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(QUERY, match)
                .queryParam(INFER, reasonser)
                .queryParam(MATERIALISE, materialise)
                .queryParam(LIMIT_EMBEDDED, limitEmbedded)
                .accept(acceptType)
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));
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
