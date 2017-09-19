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
import org.mockito.Mockito;

import static ai.grakn.util.REST.Request.ATTRIBUTE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.ENTITY_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.VALUE_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.API_PREFIX;
import static ai.grakn.util.REST.WebPath.Api.ATTRIBUTE_TYPE;
import static ai.grakn.util.REST.WebPath.Api.ENTITY_TYPE;
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

        Mockito.doAnswer(e -> { sampleKB.tx().commit(); return null; } ).when(mockTx).commit();

        when(mockFactory.tx(mockTx.getKeyspace(), GraknTxType.READ)).thenReturn(mockTx);
        when(mockFactory.tx(mockTx.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockTx);
    }

    @Test
    public void postEntityShouldExecuteSuccessfully() {
        String entityType = "production";
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .post(ENTITY_TYPE + "/" + entityType);

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at(ENTITY_OBJECT_JSON_FIELD).at(CONCEPT_ID_JSON_FIELD).asString(), notNullValue());
    }

    @Test
    public void assignAttributeToEntityShouldExecuteSuccessfully() {
        String person = "person";
        String realName = "real-name";
        String attributeValue = "attributeValue";

        // get their ids
        String entityConceptId;
        String attributeConceptId;
        try (GraknTx tx = mockFactory.tx(mockTx.getKeyspace(), GraknTxType.WRITE)) {
            entityConceptId = tx.getEntityType(person).addEntity().getId().getValue();
            attributeConceptId = tx.getAttributeType(realName).putAttribute(attributeValue).getId().getValue();
            tx.commit();
        }

        // assign the attribute to the entity
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .put(API_PREFIX + "/entity/" + entityConceptId + "/attribute/" + attributeConceptId);

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
    }

    @Test
    public void deleteAttributeToEntityAssignmentShouldExecuteSuccessfully() {
        String person = "person";
        String realName = "real-name";
        String attributeValue = "attributeValue";

        // get their ids
        String entityConceptId;
        String attributeConceptId;
        try (GraknTx tx = mockFactory.tx(mockTx.getKeyspace(), GraknTxType.WRITE)) {
            entityConceptId = tx.getEntityType(person).addEntity().getId().getValue();
            attributeConceptId = tx.getAttributeType(realName).putAttribute(attributeValue).getId().getValue();
            tx.commit();
        }

        // assign the attribute to the entity
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace().getValue())
            .delete(API_PREFIX + "/entity/" + entityConceptId + "/attribute/" + attributeConceptId);

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
    }
}
