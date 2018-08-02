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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.graql.Query;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.InOrder;

import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.exception;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
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

public class GraqlControllerDeleteTest {

    private final EmbeddedGraknTx tx = mock(EmbeddedGraknTx.class, RETURNS_DEEP_STUBS);

    private static final Keyspace keyspace = Keyspace.of("akeyspace");
    private static final PostProcessor postProcessor = mock(PostProcessor.class);
    private static final EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);
    private static final Printer printer = mock(Printer.class);

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(new GraqlController(mockFactory, postProcessor, printer, new MetricRegistry()));

    @Before
    public void setupMock(){
        when(mockFactory.tx(eq(keyspace), any())).thenReturn(tx);
        when(printer.toString(any())).thenReturn(Json.object().toString());
    }

    @Test
    public void DELETEGraqlDelete_GraphCommitNeverCalled(){
        String query = "match $x isa person; limit 1; delete $x;";

        sendRequest(query);

        verify(tx, times(0)).commit();
    }

    @Test
    public void DELETEGraqlDelete_GraphCommitSubmitNoLogsCalled(){
        String query = "match $x isa person; limit 1; delete $x;";

        verify(tx, times(0)).commitSubmitNoLogs();

        sendRequest(query);

        verify(tx, times(1)).commitSubmitNoLogs();
    }

    @Test
    public void DELETEMalformedGraqlQuery_ResponseStatusIs400(){
        GraqlSyntaxException syntaxError = GraqlSyntaxException.create("syntax error");
        when(tx.graql().parser().parseQuery("match $x isa ; delete;")).thenThrow(syntaxError);

        String query = "match $x isa ; delete;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void DELETEMalformedGraqlQuery_ResponseExceptionContainsSyntaxError(){
        GraqlSyntaxException syntaxError = GraqlSyntaxException.create("syntax error");
        when(tx.graql().parser().parseQuery("match $x isa ; delete;")).thenThrow(syntaxError);

        String query = "match $x isa ; delete;";
        Response response = sendRequest(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void DELETEWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, "somekb"));

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
    public void DELETEGraqlDelete_DeleteWasExecutedOnTx(){
        String queryString = "match $x has title \"Godfather\"; delete $x;";

        sendRequest(queryString);

        Query<?> query = tx.graql().parser().parseQuery(queryString);

        InOrder inOrder = inOrder(query, tx);

        inOrder.verify(query).execute();
        inOrder.verify(tx, times(1)).commitSubmitNoLogs();
    }

    @Test
    public void DELETEGraqlDeleteNotValid_ResponseStatusCodeIs422(){
        GraknTxOperationException exception = GraknTxOperationException.cannotBeDeleted(mock(SchemaConcept.class));

        // Not allowed to delete roles with incoming edges
        when(tx.graql().parser().parseQuery("undefine production-being-directed sub work;").execute())
                .thenThrow(exception);

        Response response = sendRequest("undefine production-being-directed sub work;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void DELETEGraqlDeleteNotValid_ResponseExceptionContainsValidationErrorMessage(){
        GraknTxOperationException exception = GraknTxOperationException.cannotBeDeleted(mock(SchemaConcept.class));

        // Not allowed to delete roles with incoming edges
        when(tx.graql().parser().parseQuery("undefine production-being-directed sub work;").execute())
                .thenThrow(exception);

        Response response = sendRequest("undefine production-being-directed sub work;");

        assertThat(exception(response), containsString("cannot be deleted"));
    }

    private Response sendRequest(String query){
        return RestAssured.with()
                .queryParam(EXECUTE_WITH_INFERENCE, false)
                .body(query)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, keyspace.getValue()));
    }
}
