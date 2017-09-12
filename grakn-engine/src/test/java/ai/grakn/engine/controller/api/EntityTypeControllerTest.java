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

package ai.grakn.engine.controller.api;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * <p>
 *     Endpoint tests for Graph API
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class EntityTypeControllerTest {
    private static GraknTx mockTx;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext sampleKB = SampleKBContext.preLoad(MovieKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new EntityTypeController(mockFactory, spark);
    });

    @Before
    public void setupMock(){
        mockTx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.getKeyspace()).thenReturn(Keyspace.of("randomKeyspace"));

        when(mockTx.putEntityType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().putEntityType((String) invocation.getArgument(0)));
        when(mockTx.getEntityType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getEntityType(invocation.getArgument(0)));
        when(mockTx.getAttributeType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getAttributeType(invocation.getArgument(0)));

        when(mockFactory.tx(mockTx.getKeyspace(), GraknTxType.READ)).thenReturn(mockTx);
        when(mockFactory.tx(mockTx.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockTx);
    }

    @Test
    public void getEntityTypeFromMovieGraphShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace())
            .get("/api/entityType/production");

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at("entityType").at("conceptId").asString(), notNullValue());
        assertThat(responseBody.at("entityType").at("label").asString(), equalTo("production"));
    }

    @Test
    public void postEntityTypeShouldExecuteSuccessfully() {
        Json body = Json.object("entityType", Json.object("label", "newEntityType"));

        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace())
            .body(body.toString())
            .post("/api/entityType");

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at("entityType").at("conceptId").asString(), notNullValue());
        assertThat(responseBody.at("entityType").at("label").asString(), equalTo("newEntityType"));
    }

//    @Test // TODO: create a test once an implementation is made
//    public void deleteEntityTypeShouldExecuteSuccessfully() {
//
//    }

    @Test
    public void assignAttributeToEntityTypeShouldExecuteSuccessfully() {
        Json body = Json.object(
            "entityTypeLabel", "production",
            "attributeTypeLabel", "runtime"
            );

        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace())
            .put("/api/entityType/production/attribute/runtime");

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
    }

//    @Test // TODO: create a test once an implementation is made
//    public void deleteAttributeTypeToEntityTypeAssignmentShouldExecuteSuccessfully() {
//
//    }

//    @Test // TODO
//    public void assignRoleToEntityTypeShouldExecuteSuccessfully() {
//
//    }

//    @Test // TODO: create a test once an implementation is made
//    public void deleteRoleToEntityTypeShouldExecuteSuccessfully() {
//
//    }
}
