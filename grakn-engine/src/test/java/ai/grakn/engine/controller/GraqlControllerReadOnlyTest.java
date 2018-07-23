/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller;

import ai.grakn.Keyspace;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.engine.ServerStatus;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.EXCEPTION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeTrue;
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

    private static EmbeddedGraknTx mockTx;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);
    private static KeyspaceStore mockKeyspaceStore = mock(KeyspaceStoreImpl.class);
    private static final Printer printer = mock(Printer.class);

    private static final JsonMapper jsonMapper = new JsonMapper();

    @ClassRule
    public static SampleKBContext sampleKB = MovieKB.context();

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers((spark, config) -> {
        MetricRegistry metricRegistry = new MetricRegistry();
        new SystemController(mockFactory.config(), mockFactory.keyspaceStore(), new ServerStatus(), metricRegistry).start(spark);
        new GraqlController(mockFactory, mock(PostProcessor.class), printer, metricRegistry).start(spark);
    });

    @Before
    public void setupMock() {
        mockQueryBuilder = mock(QueryBuilder.class);

        when(mockQueryBuilder.infer(anyBoolean())).thenReturn(mockQueryBuilder);

        when(printer.toString(any())).thenReturn(Json.object().toString());

        QueryParser mockParser = mock(QueryParser.class);

        when(mockQueryBuilder.parser()).thenReturn(mockParser);
        when(mockParser.parseQuery(any()))
                .thenAnswer(invocation -> sampleKB.tx().graql().parse(invocation.getArgument(0)));

        mockTx = mock(EmbeddedGraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.keyspace()).thenReturn(Keyspace.of("randomkeyspace"));
        when(mockTx.graql()).thenReturn(mockQueryBuilder);

        when(mockFactory.tx(eq(mockTx.keyspace()), any())).thenReturn(mockTx);
        when(mockFactory.keyspaceStore()).thenReturn(mockKeyspaceStore);
        when(mockFactory.config()).thenReturn(sparkContext.config());
    }

    @Test
    public void GETGraqlMatch_QueryIsExecuted() {
        String query = "match $x isa movie;";
        sendRequest(query);

        verify(mockTx.graql().infer(anyBoolean()).parser())
                .parseQuery(argThat(argument -> argument.equals(query)));
    }

    @Test
    public void GETGraqlMatch_ResponseStatusIs200() {
        String query = "match $x isa movie; get;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETMalformedGraqlMatch_ResponseStatusCodeIs400() {
        String query = "match $x isa ;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void GETMalformedGraqlMatch_ResponseExceptionContainsSyntaxError() {
        String query = "match $x isa ;";
        Response response = sendRequest(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    // This is so that the word "Exception" does not appear on the dashboard when there is a syntax error
    @Test
    public void GETMalformedGraqlMatch_ResponseExceptionDoesNotContainWordException() {
        String query = "match $x isa ;";
        Response response = sendRequest(query);

        assertThat(exception(response), not(containsString("Exception")));
    }

    @Test
    public void GETGraqlMatchWithNoQuery_ResponseStatusIs400() {
        Response response = RestAssured.with()
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, mockTx.keyspace().getValue()));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage(QUERY)));
    }


    @Test
    public void GETGraqlMatchWithReasonerTrue_ReasonerIsOnWhenExecuting() {
        sendRequest("match $x isa movie;",  true);

        verify(mockQueryBuilder).infer(booleanThat(arg -> arg));
    }

    @Test
    public void GETGraqlMatchWithReasonerFalse_ReasonerIsOffWhenExecuting() {
        sendRequest("match $x isa movie;", false);

        verify(mockQueryBuilder).infer(booleanThat(arg -> !arg));
    }

    @Test
    public void GETGraqlMatchWithNoInfer_ResponseStatusIs200() {
        Response response = RestAssured.with()
                .body("match $x isa movie; get;")
                .accept(APPLICATION_JSON)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, mockTx.keyspace().getValue()));

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETGraqlMatchWithGraqlJsonTypeAndEmptyResponse_ResponseIsEmptyJsonObject() {
        when(printer.toString(any())).thenReturn(Json.array().toString());
        Response response = sendRequest("match $x isa \"runtime\"; get;");

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void GETGraqlAggregate_ResponseStatusIs200() {
        String query = "match $x isa movie; aggregate count;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void GETGraqlAggregate_ResponseIsCorrect() {
        String query = "match $x isa movie; aggregate count;";
        long numberPeople = sampleKB.tx().getEntityType("movie").instances().count();
        when(printer.toString(any())).thenReturn(String.valueOf(numberPeople));

        Response response = sendRequest(query);

        // refresh graph
        sampleKB.tx().close();

        assertThat(stringResponse(response), equalTo(Long.toString(numberPeople)));
    }

    @Test
    public void GETGraqlCompute_ResponseStatusIs200() {
        String query = "compute count in movie;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    @Ignore // TODO: Fix this. Probably related to mocks and analytics
    public void GETGraqlCompute_ResponseIsCorrect() {
        String query = "compute count in movie;";
        Response response = sendRequest(query);

        Long numberPeople = sampleKB.tx().getEntityType("movie").instances().count();
        assertThat(stringResponse(response), equalTo(Long.toString(numberPeople)));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    @Ignore
    public void ZGETGraqlComputePath_ResponseIsCorrect() {
        assumeTrue(GraknTestUtil.usingJanus());

        String fromId = sampleKB.tx().getAttributesByValue("The Muppets").iterator().next().owner().id().getValue();
        String toId = sampleKB.tx().getAttributesByValue("comedy").iterator().next().owner().id().getValue();

        String query = String.format("compute path from \"%s\", to \"%s\";", fromId, toId);
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(stringResponse(response), containsString("isa has-genre"));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePath_ResponseStatusIs200() {
        assumeTrue(GraknTestUtil.usingJanus());

        String fromId = sampleKB.tx().getAttributesByValue("The Muppets").iterator().next().owner().id().getValue();
        String toId = sampleKB.tx().getAttributesByValue("comedy").iterator().next().owner().id().getValue();

        String query = String.format("compute path from \"%s\", to \"%s\";", fromId, toId);
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    @Ignore
    public void ZGETGraqlComputePath_ResponseIsNotEmpty() {
        assumeTrue(GraknTestUtil.usingJanus());

        String fromId = sampleKB.tx().getAttributesByValue("The Muppets").iterator().next().owner().id().getValue();
        String toId = sampleKB.tx().getAttributesByValue("comedy").iterator().next().owner().id().getValue();

        String query = String.format("compute path from \"%s\", to \"%s\";", fromId, toId);
        Response response = sendRequest(query);

        assertThat(jsonResponse(response).asJsonList().size(), greaterThan(0));
    }

    //TODO Prefix with Z to run last until TP Bug #13730 Fixed
    @Test
    public void ZGETGraqlComputePathWithNoPath_ResponseIsEmptyJson() {
        String fromId = sampleKB.tx().getAttributesByValue("Apocalypse Now").iterator().next().owner().id().getValue();
        String toId = sampleKB.tx().getAttributesByValue("comedy").iterator().next().owner().id().getValue();

        String query = String.format("compute path from \"%s\", to \"%s\";", fromId, toId);
        when(printer.toString(any())).thenReturn("null");
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
        assertThat(jsonResponse(response), equalTo(Json.nil()));
    }

    private Response sendRequest(String match) {
        return sendRequest(match, false);
    }

    private Response sendRequest(String match,  boolean reasonser) {
        return RestAssured.with()
                .queryParam(KEYSPACE_PARAM, mockTx.keyspace().getValue())
                .body(match)
                .queryParam(EXECUTE_WITH_INFERENCE, reasonser)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, mockTx.keyspace().getValue()));
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

