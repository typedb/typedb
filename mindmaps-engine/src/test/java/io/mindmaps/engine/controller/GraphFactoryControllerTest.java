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
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.Util;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.util.REST.GraphConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Spark;

import static com.jayway.restassured.RestAssured.get;
import static io.mindmaps.util.REST.Request.GRAPH_CONFIG_PARAM;
import static io.mindmaps.util.REST.WebPath.GRAPH_FACTORY_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class GraphFactoryControllerTest {

    @BeforeClass
    public static void setUpController() throws InterruptedException {
        Spark.stop();
        Thread.sleep(5000);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());
        new GraphFactoryController();
        Thread.sleep(5000);
    }

    @Test
    public void testConfigWorking() {
        Response response = get(GRAPH_FACTORY_URI).then().statusCode(200).extract().response().andReturn();
        String config = response.getBody().prettyPrint();
        assertTrue(config.contains("factory"));
    }

    @Test
    public void testSpecificConfigWorking() {
        String endPoint = GRAPH_FACTORY_URI + "?" + GRAPH_CONFIG_PARAM + "=";

        Response responseDefault = get(endPoint + GraphConfig.DEFAULT).
                then().statusCode(200).extract().response().andReturn();

        Response responseBatch = get(endPoint + GraphConfig.BATCH).
                then().statusCode(200).extract().response().andReturn();

        Response responseComputer = get(endPoint + GraphConfig.COMPUTER).
                then().statusCode(200).extract().response().andReturn();

        assertNotEquals(responseDefault, responseBatch);
        assertNotEquals(responseComputer, responseBatch);
    }

    @Test
    public void testMindmapsClientBatch() {
        MindmapsGraph batch = Mindmaps.factory(Mindmaps.DEFAULT_URI, "mindmapstest").getGraphBatchLoading();
        assertTrue(((AbstractMindmapsGraph) batch).isBatchLoadingEnabled());
    }

    @Test
    public void testMindmaps() {
        AbstractMindmapsGraph graph = (AbstractMindmapsGraph) Mindmaps.factory(Mindmaps.DEFAULT_URI, "mindmapstest").getGraph();
        AbstractMindmapsGraph graph2 = (AbstractMindmapsGraph) Mindmaps.factory(Mindmaps.DEFAULT_URI, "mindmapstest2").getGraph();
        AbstractMindmapsGraph graphCopy = (AbstractMindmapsGraph) Mindmaps.factory(Mindmaps.DEFAULT_URI, "mindmapstest").getGraph();
        assertNotEquals(0, graph.getTinkerPopGraph().traversal().V().toList().size());
        assertFalse(graph.isBatchLoadingEnabled());
        assertNotEquals(graph, graph2);
        assertEquals(graph, graphCopy);
        graph.close();

        AbstractMindmapsGraph batch = (AbstractMindmapsGraph) Mindmaps.factory(Mindmaps.DEFAULT_URI, "mindmapstest").getGraphBatchLoading();
        assertTrue(batch.isBatchLoadingEnabled());
        assertNotEquals(graph, batch);

    }

}