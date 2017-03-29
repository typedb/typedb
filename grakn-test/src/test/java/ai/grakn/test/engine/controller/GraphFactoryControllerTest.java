/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.controller;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST.GraphConfig;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static ai.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static ai.grakn.util.REST.WebPath.GRAPH_FACTORY_URI;
import static ai.grakn.util.REST.WebPath.KEYSPACE_LIST;
import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class GraphFactoryControllerTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

	@Test
	public void testKeyspaceList() throws GraknValidationException {
		Grakn.factory(Grakn.DEFAULT_URI, "grakntest1").getGraph().close();
        Grakn.factory(Grakn.DEFAULT_URI, "grakntest2").getGraph().close();
        Response response = get(KEYSPACE_LIST).then().statusCode(200).extract().response();
        Json result = Json.read(response.body().asString());
        Assert.assertTrue(result.asJsonList().contains(Json.make("grakntest")));
        Assert.assertTrue(result.asJsonList().contains(Json.make("grakntest2")));
	}
	
    @Test
    public void testConfigWorking() {
        Response response = get(GRAPH_FACTORY_URI).then().statusCode(200).extract().response().andReturn();
        String config = response.getBody().asString();
        assertTrue(config.contains("factory"));
    }

    @Test
    public void testSpecificConfigWorking() {
        String endPoint = GRAPH_FACTORY_URI + "?" + GRAPH_CONFIG_PARAM + "=";

        Response responseDefault = get(endPoint + GraphConfig.DEFAULT).
                then().statusCode(200).extract().response().andReturn();

        Response responseComputer = get(endPoint + GraphConfig.COMPUTER).
                then().statusCode(200).extract().response().andReturn();

        assertNotEquals(responseDefault, responseComputer);
    }

    @Test
    public void testGraknClientBatch() {
        GraknGraph batch = Grakn.factory(Grakn.DEFAULT_URI, "grakntestagain").getGraphBatchLoading();
        assertTrue(((AbstractGraknGraph) batch).isBatchLoadingEnabled());
    }

    @Test
    public void testGrakn() throws GraknValidationException {
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest").getGraph();
        AbstractGraknGraph graph2 = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest2").getGraph();
        assertNotEquals(0, graph.getTinkerPopGraph().traversal().V().toList().size());
        assertFalse(graph.isBatchLoadingEnabled());
        assertNotEquals(graph, graph2);
        graph.close();

        AbstractGraknGraph batch = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, "grakntest").getGraphBatchLoading();
        assertTrue(batch.isBatchLoadingEnabled());
        assertNotEquals(graph, batch);

        graph.close();
        batch.close();
        graph2.close();
    }

    @Test
    public void testGraphSingleton(){
        assumeTrue(usingTinker());
        String keyspace = "grakntest";
        AbstractGraknGraph graphNormal = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        AbstractGraknGraph graphBatch = (AbstractGraknGraph) Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraphBatchLoading();

        assertNotEquals(graphNormal, graphBatch);
        //This is only true for tinkergraph
        assertEquals(graphNormal.getTinkerPopGraph(), graphBatch.getTinkerPopGraph());
    }

}