/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.controller;

import com.jayway.restassured.response.Response;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.engine.MindmapsEngineTestBase;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.REST;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VisualiserControllerTest extends MindmapsEngineTestBase{

    private static String graphName = "specialtestgraph";

    // Adding Ignore since I have to re-write the tests, give that the HAL format is now different.

    @Before
    public void setUp() throws Exception {
        MindmapsGraph graph = GraphFactory.getInstance().getGraph(graphName);
        EntityType man = graph.putEntityType("Man");
        graph.putEntity("actor-123", man);
        graph.commit();
    }

    @Ignore
     public void notExistingID() {
        Response response = get(REST.WebPath.CONCEPT_BY_ID_URI+"6573gehjio?graphName="+graphName).then().statusCode(404).extract().response().andReturn();
        String  message = response.getBody().asString();
        assertTrue(message.equals("ID [6573gehjio] not found in the graph."));
    }

    @Ignore
    public void getEntityByID() {
        Response response = get(REST.WebPath.CONCEPT_BY_ID_URI+"actor-123?graphName="+graphName).then().statusCode(200).extract().response().andReturn();
        JSONObject message = new JSONObject(response.getBody().asString());
        makeSureThisIsOurMan(message);
    }

    @Ignore
    public void getEntityByMatchQuery() {
        Response response = get(REST.WebPath.GRAPH_MATCH_QUERY_URI+"?graphName="+graphName+"&query=match $x isa Man;").then().statusCode(200).extract().response().andReturn();
        JSONArray resultArray = new JSONArray(response.getBody().asString());
        makeSureThisIsOurMan(resultArray.getJSONObject(0));
    }

    @Ignore
    public void syntacticallyWrongMatchQuery() {
        Response response = get(REST.WebPath.GRAPH_MATCH_QUERY_URI+"?graphName="+graphName+"&query=match ersouiuiwne is ieeui;").then().statusCode(500).extract().response().andReturn();
        System.out.println(response.body().asString());

    }

    private void makeSureThisIsOurMan(JSONObject message){
        assertEquals(message.getString("_type"),"Man");
        assertEquals(message.getString("_id"),"actor-123");
        assertEquals(message.getString("_baseType"),"entity-type");
        assertEquals(message.getJSONObject("_links").getJSONObject("self").getString("href"),"/graph/concept/actor-123");

        JSONObject embeddedType = message.getJSONObject("_embedded").getJSONArray("isa").getJSONObject(0);
        assertEquals(embeddedType.getString("_baseType"),"type");
        assertEquals(embeddedType.getString("_type"),"entity-type");
        assertEquals(embeddedType.getString("_id"),"Man");
    }

    @Ignore
    public void getTypeByID() {
        Response response = get(REST.WebPath.CONCEPT_BY_ID_URI+"Man?graphName="+graphName).then().statusCode(200).extract().response().andReturn();
        JSONObject message = new JSONObject(response.getBody().asString());

        assertEquals(message.getString("_type"),"entity-type");
        assertEquals(message.getString("_id"),"Man");
        assertEquals(message.getString("_baseType"),"type");
        assertEquals(message.getJSONObject("_links").getJSONObject("self").getString("href"),"/graph/concept/Man");

        JSONArray isaEmbeddedArray = message.getJSONObject("_embedded").getJSONArray("isa");
        Set<String> ids = new HashSet<>();
        isaEmbeddedArray.forEach(x ->ids.add(((JSONObject)x).getString("_id")));
        assertTrue(ids.contains("actor-123"));
        assertTrue(ids.contains("entity-type"));
    }

    @Test
    public void notExistingIDInDefaultGraph() {
        get("/graph/concept/actor-123").then().statusCode(500).extract().response().andReturn();
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraph(graphName).clear();
    }
}
