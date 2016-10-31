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

import com.jayway.restassured.response.Response;
import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.engine.GraknEngineTestBase;
import io.grakn.graph.internal.AbstractGraknGraph;
import io.grakn.util.REST.GraphConfig;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static io.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static io.grakn.util.REST.WebPath.GRAPH_FACTORY_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class GraphFactoryControllerTest extends GraknEngineTestBase {

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
    public void testGraknClientBatch() {
        GraknGraph batch = Grakn.factory(Grakn.DEFAULT_URI, "grakntest").getGraphBatchLoading();
        assertTrue(((AbstractGraknGraph) batch).isBatchLoadingEnabled());
    }

    @Test
    public void testGrakn() {
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest").getGraph();
        AbstractGraknGraph graph2 = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest2").getGraph();
        AbstractGraknGraph graphCopy = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest").getGraph();
        assertNotEquals(0, graph.getTinkerPopGraph().traversal().V().toList().size());
        assertFalse(graph.isBatchLoadingEnabled());
        assertNotEquals(graph, graph2);
        assertEquals(graph, graphCopy);
        graph.close();

        AbstractGraknGraph batch = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest").getGraphBatchLoading();
        assertTrue(batch.isBatchLoadingEnabled());
        assertNotEquals(graph, batch);

    }

}