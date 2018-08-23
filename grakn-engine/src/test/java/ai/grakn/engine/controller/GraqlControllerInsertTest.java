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

package ai.grakn.engine.controller;

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.attribute.uniqueness.AttributeDeduplicatorDaemon;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.Optional;

import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.exception;
import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.jsonResponse;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraqlControllerInsertTest {

    private final EmbeddedGraknTx tx = mock(EmbeddedGraknTx.class, RETURNS_DEEP_STUBS);

    private static final Keyspace keyspace = Keyspace.of("akeyspace");
    private static final AttributeDeduplicatorDaemon ATTRIBUTE_DEDUPLICATOR = mock(AttributeDeduplicatorDaemon.class);
    private static final EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);
    private static final Printer printer = mock(Printer.class);

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(
            new GraqlController(mockFactory, ATTRIBUTE_DEDUPLICATOR, printer, new MetricRegistry())
    );

    @Before
    public void setupMock() {
        when(mockFactory.tx(eq(keyspace), any())).thenReturn(tx);
        when(tx.keyspace()).thenReturn(keyspace);
        when(printer.toString(any())).thenReturn(Json.object().toString());

        // Describe expected response to a typical query
        Query<ConceptMap> query = tx.graql().parser().parseQuery("insert $x isa person;");
        Concept person = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(person.id()).thenReturn(ConceptId.of("V123"));
        when(person.isThing()).thenReturn(true);
        when(person.asThing().type().label()).thenReturn(Label.of("person"));

        when(query.execute()).thenReturn(ImmutableList.of(
                new ConceptMapImpl(ImmutableMap.of(var("x"), person))
        ));
    }

    @After
    public void clearExceptions() {
    }

    @Test
    public void POSTGraqlInsert_InsertWasExecutedOnGraph() {
        String queryString = "insert $x isa movie;";

        sendRequest(queryString);

        Query<?> query = tx.graql().parser().parseQuery(queryString);

        InOrder inOrder = inOrder(query, tx);

        inOrder.verify(query).execute();
        inOrder.verify(tx, times(1)).commitAndGetLogs();
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseStatusIs400() {
        GraqlSyntaxException syntaxError = GraqlSyntaxException.create("syntax error");
        when(tx.graql().parser().parseQuery("insert $x isa ;")).thenThrow(syntaxError);

        String query = "insert $x isa ;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseExceptionContainsSyntaxError() {
        GraqlSyntaxException syntaxError = GraqlSyntaxException.create("syntax error");
        when(tx.graql().parser().parseQuery("insert $x isa ;")).thenThrow(syntaxError);

        String query = "insert $x isa ;";
        Response response = sendRequest(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void POSTWithNoQueryInBody_ResponseIs400() {
        Response response = RestAssured.with()
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, "somekb"));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void POSTGraqlInsert_ResponseStatusIs200() {
        String query = "insert $x isa person;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void POSTGraqlDefineNotValid_ResponseStatusCodeIs422() {
        GraknTxOperationException exception = GraknTxOperationException.invalidCasting(Object.class, Object.class);
        when(tx.graql().parser().parseQuery("define person plays movie;").execute()).thenThrow(exception);

        Response response = sendRequest("define person plays movie;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void POSTGraqlDefineNotValid_ResponseExceptionContainsValidationErrorMessage() {
        GraknTxOperationException exception = GraknTxOperationException.invalidCasting(Object.class, Object.class);
        when(tx.graql().parser().parseQuery("define person plays movie;").execute()).thenThrow(exception);

        Response response = sendRequest("define person plays movie;");

        assertThat(exception(response), containsString("is not of type"));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseContentTypeIsJson() {
        Response response = sendRequest("insert $x isa person;");

        assertThat(response.contentType(), equalTo(APPLICATION_JSON));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseIsCorrectJson() {
        when(printer.toString(any())).thenReturn(Json.array().toString());
        Response response = sendRequest("insert $x isa person;");
        assertThat(jsonResponse(response).asJsonList().size(), equalTo(0));
    }

    @Test
    public void POSTGraqlDefine_GraphCommitNeverCalled() {
        String query = "define thingy sub entity;";

        sendRequest(query);

        verify(tx, times(0)).commit();
    }

    @Test
    public void POSTGraqlDefine_GraphCommitSubmitNoLogsIsCalled() {
        String query = "define thingy sub entity;";

        verify(tx, times(0)).commitAndGetLogs();

        sendRequest(query);

        verify(tx, times(1)).commitAndGetLogs();
    }

    @Test
    public void POSTGraqlInsert_CommitLogsAreSubmitted() {
        String query = "insert $x isa person has name 'Alice';";

        CommitLog commitLog = CommitLog.create(tx.keyspace(), Collections.emptyMap(), Collections.emptyMap());
        when(tx.commitAndGetLogs()).thenReturn(Optional.of(commitLog));
    }

    private Response sendRequest(String query) {
        return RestAssured.with()
                .queryParam(EXECUTE_WITH_INFERENCE, false)
                .body(query)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, keyspace.getValue()));
    }
}
