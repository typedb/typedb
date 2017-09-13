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
import ai.grakn.util.SampleKBLoader;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
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

public class EntityControllerTest {
    private static GraknTx mockTx;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext sampleKB = SampleKBContext.preLoad(MovieKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new AttributeController(mockFactory, spark);
        new EntityController(mockFactory, spark);
    });

    @Before
    public void setupMock(){
        mockTx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.getKeyspace()).thenReturn(SampleKBLoader.randomKeyspace());

        when(mockTx.getEntityType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getEntityType(invocation.getArgument(0)));
        when(mockTx.getAttributeType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getAttributeType(invocation.getArgument(0)));
        when(mockTx.getConcept(any())).thenAnswer(invocation ->
            sampleKB.tx().getConcept(invocation.getArgument(0)));

        when(mockFactory.tx(mockTx.getKeyspace(), GraknTxType.READ)).thenReturn(mockTx);
        when(mockFactory.tx(mockTx.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockTx);
    }

    @Test
    public void postEntityShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .post("/api/entityType/production");

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at("entity").at("conceptId").asString(), notNullValue());
    }

    @Test
    @Ignore("There is a problem in Janus with adding attribute and accessig it from different endpoint for some reason. This error does not happen to the in-memory test")
    public void assignAttributeToEntityShouldExecuteSuccessfully() {
        // add an entity
        Response addEntityResponse = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .post("/api/entityType/person");

        // add an attribute
        Response addAttributeResponse = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .body(Json.object("value", "attributeValue").toString())
            .post("/api/attributeType/real-name");

        // get their ids
        String entityConceptId = Json.read(addEntityResponse.body().asString()).at("entity").at("conceptId").asString();
        String attributeConceptId = Json.read(addAttributeResponse.body().asString()).at("attribute").at("conceptId").asString();

        // assign the attribute to the entity
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .put("/api/entity/" + entityConceptId + "/attribute/" + attributeConceptId);

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
    }
}
