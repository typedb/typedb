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

package ai.grakn.engine.controller;

import com.jayway.restassured.response.Response;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertTrue;

public class RemoteShellControllerTest extends GraknEngineTestBase {

    private String graphName;
    private String entityId;

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        GraknGraph graph = GraphFactory.getInstance().getGraph(graphName);

        EntityType man = graph.putEntityType("Man");
        entityId = graph.addEntity(man).getId();
        graph.commit();
    }

    // TODO: Fix this test (possibly related to Spark.stop() in setup)
    @Ignore
    @Test
    public void existingID() {
        Response response = get("/shell/match?graphName=" + graphName + "&query=match $x isa Man").then().statusCode(200).extract().response().andReturn();
        String message = response.getBody().asString();
        assertTrue(message.contains(entityId));
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraph(graphName).clear();
    }

}
