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
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST.GraphConfig;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static ai.grakn.util.REST.WebPath.System.CONFIGURATION;
import static ai.grakn.util.REST.WebPath.System.KEYSPACES;
import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SystemControllerTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

	@Test
	public void testKeyspaceList() throws GraknValidationException {
		Grakn.session(Grakn.DEFAULT_URI, "grakntest1").open(GraknTxType.WRITE).close();
        Grakn.session(Grakn.DEFAULT_URI, "grakntest2").open(GraknTxType.WRITE).close();
        Response response = get(KEYSPACES).then().statusCode(200).extract().response();
        Json result = Json.read(response.body().asString());
        Assert.assertTrue(result.asJsonList().contains(Json.make("grakntest")));
        Assert.assertTrue(result.asJsonList().contains(Json.make("grakntest2")));
	}
	
    @Test
    public void testConfigWorking() {
        Response response = get(CONFIGURATION).then().statusCode(200).extract().response().andReturn();
        String config = response.getBody().asString();
        assertTrue(config.contains("factory"));
        assertFalse(config.contains(GraknEngineConfig.JWT_SECRET_PROPERTY));
    }

    @Test
    public void testSpecificConfigWorking() {
        String endPoint = CONFIGURATION + "?" + GRAPH_CONFIG_PARAM + "=";

        Response responseDefault = get(endPoint + GraphConfig.DEFAULT).
                then().statusCode(200).extract().response().andReturn();

        Response responseComputer = get(endPoint + GraphConfig.COMPUTER).
                then().statusCode(200).extract().response().andReturn();

        assertNotEquals(responseDefault, responseComputer);
    }


    @Test
    public void testGraknClientBatch() {
        GraknGraph batch = Grakn.session(Grakn.DEFAULT_URI, "grakntestagain").open(GraknTxType.BATCH);
        assertTrue(((AbstractGraknGraph) batch).isBatchLoadingEnabled());
    }

    @Test
    public void testGrakn() throws GraknValidationException {
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.session(Grakn.DEFAULT_URI, "grakntest").open(GraknTxType.WRITE);
        AbstractGraknGraph graph2 = (AbstractGraknGraph) Grakn.session(Grakn.DEFAULT_URI, "grakntest2").open(GraknTxType.WRITE);
        assertNotEquals(0, graph.getTinkerPopGraph().traversal().V().toList().size());
        assertFalse(graph.isBatchLoadingEnabled());
        assertNotEquals(graph, graph2);
        graph.close();

        AbstractGraknGraph batch = (AbstractGraknGraph) Grakn.session(Grakn.DEFAULT_URI, "grakntest").open(GraknTxType.BATCH);
        assertTrue(batch.isBatchLoadingEnabled());
        assertNotEquals(graph, batch);

        graph.close();
        batch.close();
        graph2.close();
    }
}