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

import ai.grakn.factory.GraphFactory;
import com.jayway.restassured.response.Response;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.util.REST;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static ai.grakn.util.REST.Request.GRAQL_CONTENTTYPE;
import static ai.grakn.util.REST.Request.HAL_CONTENTTYPE;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.with;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ai.grakn.util.REST.Request.QUERY_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;

public class VisualiserControllerTest extends GraknEngineTestBase {

    private static String keyspace = "specialtestgraph";
    private String entityId;

    // Adding Ignore since I have to re-write the tests, give that the HAL format is now different.

    @Before
    public void setUp() throws Exception {
        GraknGraph graph = GraphFactory.getInstance().getGraph(keyspace);
        EntityType man = graph.putEntityType("Man");
        entityId = man.addEntity().getId();
        graph.commit();
    }

    @Ignore
     public void notExistingID() {
        Response response = with()
                .queryParam(KEYSPACE_PARAM, keyspace)
                .get(REST.WebPath.CONCEPT_BY_ID_URI + "6573gehjiok")
                .then().statusCode(200).extract().response().andReturn();
        String  message = response.getBody().asString();
        assertTrue(message.equals("ID [6573gehjio] not found in the graph."));
    }

    @Ignore
    public void getEntityByID() {
        Response response = with()
                .queryParam(KEYSPACE_PARAM, keyspace)
                .get(REST.WebPath.CONCEPT_BY_ID_URI + entityId)
                .then().statusCode(200).extract().response().andReturn();

        JSONObject message = new JSONObject(response.getBody().asString());
        makeSureThisIsOurMan(message);
    }

    @Test
    public void halContentTypeTest(){
        Response response = with()
                .queryParam(KEYSPACE_PARAM, keyspace)
                .queryParam(QUERY_FIELD, "match $x isa Man;")
                .accept(HAL_CONTENTTYPE)
                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
                .then().statusCode(200).extract().response().andReturn();
        JSONArray resultArray = new JSONArray(response.getBody().asString());
        makeSureThisIsOurMan(resultArray.getJSONObject(0));
    }

    @Test
    public void graqlContentTypeTest(){
        Response response = with()
                .queryParam(KEYSPACE_PARAM, keyspace)
                .queryParam(QUERY_FIELD, "match $x isa Man;")
                .accept(GRAQL_CONTENTTYPE)
                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
                .then().statusCode(200).extract().response().andReturn();

        String graql = response.getBody().asString();
        assertTrue(graql.contains("isa Man"));
    }

    @Test
    public void syntacticallyWrongMatchQuery() {
        Response response = get(REST.WebPath.GRAPH_MATCH_QUERY_URI+"?keyspace="+keyspace+"&query=match ersouiuiwne is ieeui;").then().statusCode(500).extract().response().andReturn();
        System.out.println(response.body().asString());

    }

    private void makeSureThisIsOurMan(JSONObject message){
        System.out.println(message);
        assertEquals(message.getString("_type"), "Man");
        assertEquals(message.getString("_id"),entityId);
        assertEquals(message.getString("_baseType"),"entity-type");
        assertEquals(message.getJSONObject("_links").getJSONObject("self").getString("href"),"/graph/concept/" + entityId);

        JSONObject embeddedType = message.getJSONObject("_embedded").getJSONArray("isa").getJSONObject(0);
        assertEquals(embeddedType.getString("_baseType"),"type");
        assertEquals(embeddedType.getString("_type"),"entity-type");
    }

    @Ignore
    public void getTypeByID() {
        Response response = with()
                .queryParam(KEYSPACE_PARAM, keyspace)
                .get(REST.WebPath.CONCEPT_BY_ID_URI + "Man")
                .then().statusCode(200).extract().response().andReturn();

        JSONObject message = new JSONObject(response.getBody().asString());

        assertEquals(message.getString("_type"),"entity-type");
        assertEquals(message.getString("_id"),"Man");
        assertEquals(message.getString("_baseType"),"type");
        assertEquals(message.getJSONObject("_links").getJSONObject("self").getString("href"),"/graph/concept/Man");

        JSONArray isaEmbeddedArray = message.getJSONObject("_embedded").getJSONArray("isa");
        System.out.println(isaEmbeddedArray);
        Set<String> ids = new HashSet<>();
        isaEmbeddedArray.forEach(x ->ids.add(((JSONObject)x).getString("_id")));
        assertTrue(ids.contains(entityId));
        assertTrue(ids.contains("entity-type"));
    }

    @Test
    public void notExistingIDInDefaultGraph() {
        get("/graph/concept/" + entityId).then().statusCode(500).extract().response().andReturn();
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraph(keyspace).clear();
    }
}
