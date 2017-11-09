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
import ai.grakn.graql.QueryParser;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.REST;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.exception;
import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.jsonResponse;
import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.stringResponse;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.Graql.INFER;
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

public class GraqlControllerInsertTest {

    private static GraknTx mockTx;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext sampleKB = MovieKB.context();

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new GraqlController(mockFactory, spark, new MetricRegistry());
    });

    @Before
    public void setupMock(){
        mockQueryBuilder = mock(QueryBuilder.class);

        when(mockQueryBuilder.materialise(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.infer(anyBoolean())).thenReturn(mockQueryBuilder);

        QueryParser mockParser = mock(QueryParser.class);

        when(mockQueryBuilder.parser()).thenReturn(mockParser);
        when(mockParser.parseQuery(any()))
                .thenAnswer(invocation -> sampleKB.tx().graql().parse(invocation.getArgument(0)));

        mockTx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.keyspace()).thenReturn(SampleKBLoader.randomKeyspace());
        when(mockTx.graql()).thenReturn(mockQueryBuilder);

        when(mockFactory.tx(eq(mockTx.keyspace()), any())).thenReturn(mockTx);
    }

    @Test
    public void POSTGraqlInsert_InsertWasExecutedOnGraph(){
        doAnswer(answer -> {
            sampleKB.tx().commit();
            return null;
        }).when(mockTx).commit();

        String query = "insert $x isa movie;";

        long genreCountBefore = sampleKB.tx().getEntityType("movie").instances().count();

        sendRequest(query);

        // refresh graph
        sampleKB.tx().close();

        long genreCountAfter = sampleKB.tx().getEntityType("movie").instances().count();

        assertEquals(genreCountBefore + 1, genreCountAfter);
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseStatusIs400(){
        String query = "insert $x isa ;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseExceptionContainsSyntaxError(){
        String query = "insert $x isa ;";
        Response response = sendRequest(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void POSTWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .post(REST.resolveTemplate(REST.WebPath.KB.ANY_GRAQL, "some-kb"));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void POSTGraqlInsert_ResponseStatusIs200(){
        String query = "insert $x isa person;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void POSTGraqlDefineNotValid_ResponseStatusCodeIs422(){
        Response response = sendRequest("define person plays movie;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void POSTGraqlDefineNotValid_ResponseExceptionContainsValidationErrorMessage(){
        Response response = sendRequest("define person plays movie;");

        assertThat(exception(response), containsString("is not of type"));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseContentTypeIsJson(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_JSON_GRAQL);

        assertThat(response.contentType(), equalTo(APPLICATION_JSON_GRAQL));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseIsCorrectJson(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_JSON_GRAQL);

        assertThat(jsonResponse(response).asJsonList().size(), equalTo(1));
    }

    @Test
    public void POSTGraqlInsertWithTextType_ResponseIsTextType(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void POSTGraqlInsertWithTextType_ResponseIsCorrectText(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_TEXT);

        assertThat(stringResponse(response), containsString("isa person"));
    }

    @Test
    public void POSTGraqlDefine_GraphCommitIsCalled(){
        String query = "define thingy sub entity;";

        verify(mockTx, times(0)).commit();

        sendRequest(query);

        verify(mockTx, times(1)).commit();
    }

    private Response sendRequest(String query){
        return sendRequest(query, APPLICATION_TEXT);
    }

    private Response sendRequest(String query, String acceptType){
        return RestAssured.with()
                .accept(acceptType)
                .queryParam(INFER, false)
                .body(query)
                .post(REST.resolveTemplate(REST.WebPath.KB.ANY_GRAQL, mockTx.keyspace().getValue()));
    }
}
