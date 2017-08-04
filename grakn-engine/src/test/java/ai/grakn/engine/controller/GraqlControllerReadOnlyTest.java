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
import ai.grakn.engine.GraknEngineStatus;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import static ai.grakn.graql.internal.hal.HALUtils.BASETYPE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.ID_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.TYPE_PROPERTY;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import ai.grakn.util.REST;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static ai.grakn.util.REST.Response.EXCEPTION;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import java.util.Collections;
import static junit.framework.TestCase.assertTrue;
import mjson.Json;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assume.assumeTrue;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//TODO Run in name order until TP Bug #13730 Fixed
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GraqlControllerReadOnlyTest {

    private static GraknGraph mockGraph;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknGraphFactory mockFactory = mock(EngineGraknGraphFactory.class);
    private static SystemKeyspace mockSystemKeyspace = mock(SystemKeyspace.class);

    private static final JsonMapper jsonMapper = new JsonMapper();

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        MetricRegistry metricRegistry = new MetricRegistry();
        new SystemController(mockFactory, spark, new GraknEngineStatus(), metricRegistry);
        new GraqlController(mockFactory, spark, metricRegistry);
    });

    @Before
    public void setupMock() {
        mockQueryBuilder = mock(QueryBuilder.class);

        when(mockQueryBuilder.materialise(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.infer(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.parse(any()))
                .thenAnswer(invocation -> graphContext.graph().graql().parse(invocation.getArgument(0)));

        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");
        when(mockGraph.graql()).thenReturn(mockQueryBuilder);

        when(mockSystemKeyspace.ensureKeyspaceInitialised(any())).thenReturn(true);

        when(mockFactory.getGraph(eq(mockGraph.getKeyspace()), any())).thenReturn(mockGraph);
        when(mockFactory.systemKeyspace()).thenReturn(mockSystemKeyspace);
        when(mockFactory.properties()).thenReturn(sparkContext.config().getProperties());
    }

    @Test
    public void GETGraqlMatch_QueryIsExecuted() {
        String query = "match $x isa movie;";
        sendRequest(query, APPLICATION_TEXT);

        verify(mockGraph.graql().materialise(anyBoolean()).infer(anyBoolean()))
                .parse(argThat(argument -> argument.equals(query)));
    }

    @Test
    public void GETGraqlMatch_ResponseStatusIs200() {
        Response response = sendRequest(APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETMalformedGraqlMatch_ResponseStatusCodeIs400() {
        String query = "match $x isa ;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void GETMalformedGraqlMatch_ResponseExceptionContainsSyntaxError() {
        String query = "match $x isa ;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("syntax error"));
    }

    // This is so that the word "Exception" does not appear on the dashboard when there is a syntax error
    @Test
    public void GETMalformedGraqlMatch_ResponseExceptionDoesNotContainWordException() {
        String query = "match $x isa ;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(exception(response), not(containsString("Exception")));
    }

    @Test
    public void GETGraqlMatchWithInvalidAcceptType_ResponseStatusIs406() {
        Response response = sendRequest("invalid");

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(UNSUPPORTED_CONTENT_TYPE.getMessage("invalid")));
    }

    @Test
    public void GETGraqlMatchWithNoKeyspace_ResponseStatusIs400() {
        Response response = RestAssured.with().body("match $x isa movie;").post(REST.WebPath.Graph.ANY_GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(KEYSPACE)));
    }

    @Test
    public void GETGraqlMatchWithNoQuery_ResponseStatusIs400() {
        Response response = RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .post(REST.WebPath.Graph.ANY_GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage(QUERY)));
    }

    @Test
    public void GETGraqlMatchNoMaterialise_ResponseStatusIs400() {
        Response response = RestAssured.with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .body("match $x isa movie;")
                .queryParam(INFER, true)
                .accept(APPLICATION_TEXT)
                .post(REST.WebPath.Graph.ANY_GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(MATERIALISE)));
    }

    @Test
    public void GETGraqlMatchWithReasonerTrue_ReasonerIsOnWhenExecuting() {
        sendRequest("match $x isa movie;", APPLICATION_TEXT, true, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> arg));
    }

    @Test
    public void GETGraqlMatchWithReasonerFalse_ReasonerIsOffWhenExecuting() {
        sendRequest("match $x isa movie;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> !arg));
    }

    @Test
    public void GETGraqlMatchWithNoInfer_ResponseStatusIs400() {
        Response response = RestAssured.with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .body("match $x isa movie;")
                .accept(APPLICATION_TEXT)
                .post(REST.WebPath.Graph.ANY_GRAQL);

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(INFER)));
    }

    @Test
    public void GETGraqlMatchWithMaterialiseFalse_MaterialiseIsOffWhenExecuting() {
        sendRequest("match $x isa movie;", APPLICATION_TEXT, false, false, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> !arg));
    }

    @Test
    public void GETGraqlMatchWithMaterialiseTrue_MaterialiseIsOnWhenExecuting() {
        sendRequest("match $x isa movie;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> arg));
    }

    @Test
    public void GETGraqlMatchWithHALTypeAndNumberEmbedded1_ResponsesContainAtMost1Concept() {
        Response response =
                sendRequest("match $x isa movie;", APPLICATION_HAL, false, true, 1);

        jsonResponse(response).asJsonList().forEach(e -> {
            Json embedded = e.asJsonMap().get("x").asJsonMap().get("_embedded");
            if (embedded != null) {
                assertThat(embedded.asJsonMap().size(), lessThanOrEqualTo(1));
            }
        });
    }

    @Test
    public void GETGraqlMatchWithHALType_ResponseIsCorrectHal() {
        String queryString = "match $x isa movie;";
        Response response = sendRequest(queryString, APPLICATION_HAL);

        Printer<?> printer = Printers.hal(mockGraph.getKeyspace(), -1);
        Query<?> query = graphContext.graph().graql().parse(queryString);
        Json expectedResponse = Json.read(printer.graqlString(query.execute()));
        assertThat(jsonResponse(response), equalTo(expectedResponse));

    }

    @Test
    public void GETGraqlMatchWithHALType_ResponseContentTypeIsHal() {
        Response response = sendRequest(APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void GETGraqlMatchWithHALTypeAndEmptyResponse_ResponseIsEmptyJsonArray() {
        Response response = sendRequest("match $x isa runtime;", APPLICATION_HAL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void GETGraqlMatchWithTextType_ResponseContentTypeIsGraql() {
        Response response = sendRequest(APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void GETGraqlMatchWithTextType_ResponseIsCorrectGraql() {
        Response response = sendRequest(APPLICATION_TEXT);

        assertThat(stringResponse(response).length(), greaterThan(0));
        assertThat(stringResponse(response), stringContainsInOrder(Collections.nCopies(10, "isa movie")));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonType_ResponseContentTypeIsGraqlJson() {
        Response response = sendRequest(APPLICATION_JSON_GRAQL);

        assertThat(response.contentType(), equalTo(APPLICATION_JSON_GRAQL));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonType_ResponseIsCorrectGraql() {
        String query = "match $x isa movie;";
        Response response = sendRequest(APPLICATION_JSON_GRAQL);

        Json expectedResponse = Json.read(
                Printers.json().graqlString(graphContext.graph().graql().parse(query).execute()));
        assertThat(jsonResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonTypeAndEmptyResponse_ResponseIsEmptyJsonObject() {
        Response response = sendRequest("match $x isa \"runtime\";", APPLICATION_JSON_GRAQL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void GETGraqlAggregateWithTextType_ResponseStatusIs200() {
        String query = "match $x isa movie; aggregate count;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETGraqlAggregateWithTextType_ResponseIsCorrect() {
        String query = "match $x isa movie; aggregate count;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        // refresh graph
        graphContext.graph().close();

        int numberPeople = graphContext.graph().getEntityType("movie").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    @Test
    public void GETGraqlAggregateWithTextType_ResponseContentTypeIsText() {
        String query = "match $x isa movie; aggregate count;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void GETGraqlComputeWithTextType_ResponseContentTypeIsText() {
        String query = "compute count in movie;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void GETGraqlComputeWithTextType_ResponseStatusIs200() {
        String query = "compute count in movie;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    @Ignore // TODO: Fix this. Probably related to mocks and analytics
    public void GETGraqlComputeWithTextType_ResponseIsCorrect() {
        String query = "compute count in movie;";
        Response response = sendRequest(query, APPLICATION_TEXT);

        int numberPeople = graphContext.graph().getEntityType("movie").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    @Ignore
    public void ZGETGraqlComputePathWithTextType_ResponseIsCorrect() {
        assumeTrue(GraknTestSetup.usingJanus());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("comedy").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendRequest(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(stringResponse(response), containsString("isa has-genre"));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALType_ResponseContentTypeIsHAL() {
        assumeTrue(GraknTestSetup.usingJanus());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("comedy").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendRequest(query, APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALType_ResponseStatusIs200() {
        assumeTrue(GraknTestSetup.usingJanus());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("comedy").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendRequest(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(200));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    @Ignore
    public void ZGETGraqlComputePathWithHALType_ResponseIsNotEmpty() {
        assumeTrue(GraknTestSetup.usingJanus());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("comedy").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendRequest(query, APPLICATION_HAL);

        assertThat(jsonResponse(response).asJsonList().size(), greaterThan(0));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    @Ignore // TODO: Fix this. Probably related to mocks and analytics
    public void ZGETGraqlComputePathWithHALType_ResponseContainsValidHALObjects() {
        assumeTrue(GraknTestSetup.usingJanus());

        String fromId = graphContext.graph().getResourcesByValue("The Muppets").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("comedy").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendRequest(query, APPLICATION_HAL);

        jsonResponse(response).asJsonList().forEach(object -> {
            assertTrue(object.has(ID_PROPERTY));
            assertTrue(object.has(BASETYPE_PROPERTY));
            assertTrue(object.has(TYPE_PROPERTY));
        });
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithHALTypeAndNoPath_ResponseIsEmptyJson() {
        String fromId = graphContext.graph().getResourcesByValue("Apocalypse Now").iterator().next().owner().getId().getValue();
        String toId = graphContext.graph().getResourcesByValue("comedy").iterator().next().owner().getId().getValue();

        String query = String.format("compute path from \"%s\" to \"%s\";", fromId, toId);
        Response response = sendRequest(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(jsonResponse(response), equalTo(Json.nil()));
    }

    private Response sendRequest(String acceptType) {
        return sendRequest("match $x isa movie;", acceptType, false, false, -1);
    }

    private Response sendRequest(String match, String acceptType) {
        return sendRequest(match, acceptType, false, false, -1);
    }

    private Response sendRequest(String match, String acceptType, boolean reasonser,
                                 boolean materialise, int limitEmbedded) {
        return RestAssured.with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .body(match)
                .queryParam(INFER, reasonser)
                .queryParam(MATERIALISE, materialise)
                .queryParam(LIMIT_EMBEDDED, limitEmbedded)
                .accept(acceptType)
                .post(REST.WebPath.Graph.ANY_GRAQL);
    }

    protected static String exception(Response response) {
        return response.getBody().as(Json.class, jsonMapper).at(EXCEPTION).asString();
    }

    protected static String stringResponse(Response response) {
        return response.getBody().asString();
    }

    protected static Json jsonResponse(Response response) {
        return response.getBody().as(Json.class, jsonMapper);
    }
}

