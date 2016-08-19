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
import com.jayway.restassured.response.Response;
import io.mindmaps.MindmapsComputerImpl;
import io.mindmaps.Util;
import io.mindmaps.constants.RESTUtil.GraphConfig;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.util.ConfigProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.get;
import static io.mindmaps.constants.RESTUtil.Request.GRAPH_CONFIG_PARAM;
import static io.mindmaps.constants.RESTUtil.WebPath.GRAPH_FACTORY_URI;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class GraphFactoryControllerTest {
    private Properties prop = new Properties();

    @Before
    public void setUp() throws Exception {
        new GraphFactoryController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        try {
            prop.load(ImportControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        Util.setRestAssuredBaseURI(prop);
    }

    @Test
    public void testConfigWorking(){
        Response response = get(GRAPH_FACTORY_URI).then().statusCode(200).extract().response().andReturn();
        String config = response.getBody().prettyPrint();
        assertTrue(config.contains("factory"));
    }

    @Test
    public void testSpecificConfigWorking(){
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
    public void testMindmapsClient(){
        MindmapsGraph graph = MindmapsClient.getGraph("mindmapstest");
        MindmapsGraph graph2 = MindmapsClient.getGraph("mindmapstest2");
        MindmapsGraph graphCopy = MindmapsClient.getGraph("mindmapstest");
        assertNotEquals(0, ((AbstractMindmapsGraph) graph).getGraph().traversal().V().toList().size());
        assertNotEquals(graph, graph2);
        assertEquals(graph, graphCopy);
        graph.close();

        assertThat(MindmapsClient.getGraphComputer(), instanceOf(MindmapsComputerImpl.class));
    }
}