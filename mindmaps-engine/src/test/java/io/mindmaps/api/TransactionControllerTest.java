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

package io.mindmaps.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.RestAssured;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.RESTUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

public class TransactionControllerTest {

    Properties prop = new Properties();
    String graphName;


    @Before
    public void setUp() throws Exception {
        new TransactionController();
        new CommitLogController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        try {
            prop.load(VisualiserControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        graphName = prop.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        MindmapsGraph graph = GraphFactory.getInstance().getGraph(graphName);
        MindmapsTransaction transaction = graph.newTransaction();
        transaction.putEntityType("Man");
        transaction.commit();
        RestAssured.baseURI = prop.getProperty("server.url");
    }

    @Test
    public void insertValidQuery() {
        String exampleInsertQuery = "insert id \"actor-123\" isa Man, value \"Al Pacino\";";
        String transactionUUID = given().body(exampleInsertQuery).
                when().post(RESTUtil.WebPath.NEW_TRANSACTION_URI + "?graphName=mindmapstest").body().asString();
        int i = 0;
        String status = "QUEUED";
        while (i < 5 && !status.equals("FINISHED")) {
            i++;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            status = get("/transactionStatus/" + transactionUUID).then().extract().response().asString();

        }
        assertTrue(GraphFactory.getInstance().getGraph(graphName).newTransaction().getConcept("actor-123").asEntity().getValue().equals("Al Pacino"));
    }

    @Test
    public void insertInvalidQuery() {
        String exampleInvalidInsertQuery = "insert id ?Cdcs;w4. '' ervalue;";
        String transactionUUID = given().body(exampleInvalidInsertQuery).
                when().post(RESTUtil.WebPath.NEW_TRANSACTION_URI + "?graphName=mindmapstest").body().asString();
        int i = 0;
        String status = "QUEUED";
        while (i < 1 && !status.equals("CANCELLED")) {
            i++;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            status = get("/transactionStatus/" + transactionUUID).then().extract().response().asString();
        }
        assertTrue(status.equals("CANCELLED"));
    }

    @After
    public void cleanGraph() {
        GraphFactory.getInstance().getGraph(graphName).clear();
    }
}
