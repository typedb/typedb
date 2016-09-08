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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.util.REST;
import io.mindmaps.engine.Util;
import io.mindmaps.engine.loader.TransactionState;
import io.mindmaps.engine.postprocessing.BackgroundTasks;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TransactionControllerTest {

    String graphName;


    @Before
    public void setUp() throws Exception {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);

        new TransactionController();
        new CommitLogController();
        new GraphFactoryController();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        MindmapsGraph graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);
        graph.putEntityType("Man");
        graph.commit();
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());
    }

    @Test
    public void insertValidQuery() {
        String exampleInsertQuery = "insert id \"actor-123\" isa Man";
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
        //check that post processing starts periodically, in this case at least once.
        while(!BackgroundTasks.getInstance().isPostProcessingRunning())

        assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getConcept("actor-123"));
    }

    @Test
    public void insertInvalidQuery() {
        String exampleInvalidInsertQuery = "insert id ?Cdcs;w4. '' ervalue;";
        String transactionUUID = given().body(exampleInvalidInsertQuery).
                when().post(REST.WebPath.NEW_TRANSACTION_URI + "?graphName=mindmapstest").body().asString();
        int i = 0;
        String status = "QUEUED";
        while (i < 1 && !status.equals("ERROR")) {
            i++;
            try {
                Thread.sleep(500);
                System.out.println(get("/transaction/status/" + transactionUUID).then().extract().response().asString());
                status = new JSONObject(get("/transaction/status/" + transactionUUID).then().extract().response().asString()).getString("state");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        assertTrue(status.equals("ERROR"));
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
