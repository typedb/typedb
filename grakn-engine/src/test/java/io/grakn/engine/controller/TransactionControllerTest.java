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

package io.grakn.engine.controller;

import io.grakn.GraknGraph;
import io.grakn.engine.GraknEngineTestBase;
import io.grakn.engine.loader.TransactionState;
import io.grakn.engine.util.ConfigProperties;
import io.grakn.factory.GraphFactory;
import io.grakn.util.REST;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TransactionControllerTest extends GraknEngineTestBase {

    private String graphName;

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        GraknGraph graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);
        graph.putEntityType("Man");
        graph.commit();
    }

    @Test
    public void insertValidQuery() {
        String exampleInsertQuery = "insert id \"actor-123\" isa Man;";
        String transactionUUID = given().body(exampleInsertQuery).
                when().post(REST.WebPath.NEW_TRANSACTION_URI + "?graphName=mindmapstest").body().asString();
        int i = 0;
        String status = "QUEUED";
        while (i < 5 && !status.equals("FINISHED")) {
            i++;
            try {
                Thread.sleep(500);
                status = new JSONObject(get("/transaction/status/" + transactionUUID).then().extract().response().asString()).getString("state");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getConcept("actor-123"));
    }

    @Test
    public void insertInvalidQuery() {
        String exampleInvalidInsertQuery = "insert id ?Cdcs;w4. '' ervalue;";
        String transactionUUID = given().body(exampleInvalidInsertQuery).
                when().post(REST.WebPath.NEW_TRANSACTION_URI + "?graphName=mindmapstest").body().asString();
        int i = 0;
        String status = "QUEUED";
        while (i < 10 && !status.equals("ERROR")) {
            i++;
            try {
                Thread.sleep(500);
                System.out.println(get("/transaction/status/" + transactionUUID).then().extract().response().asString());
                status = new JSONObject(get("/transaction/status/" + transactionUUID).then().extract().response().asString()).getString("state");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        assertEquals("ERROR", status);
    }

    @Test
    public void checkLoaderStateTest() {
        String exampleInvalidInsertQuery = "insert id ?Cdcs;w4. '' ervalue;";
        given().body(exampleInvalidInsertQuery).
                when().post(REST.WebPath.NEW_TRANSACTION_URI + "?graphName=mindmapstest").body().asString();
        JSONObject resultObj = new JSONObject(get(REST.WebPath.LOADER_STATE_URI).then().statusCode(200).and().extract().body().asString());
        assertTrue(resultObj.has(TransactionState.State.QUEUED.name()));
        assertTrue(resultObj.has(TransactionState.State.ERROR.name()));
        assertTrue(resultObj.has(TransactionState.State.FINISHED.name()));
        assertTrue(resultObj.has(TransactionState.State.LOADING.name()));

    }

    @After
    public void cleanGraph() {
        GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }
}
