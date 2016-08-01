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

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.factory.MindmapsClient;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class GraphFactoryControllerTest {

    @Before
    public void setUp() throws Exception {
        new GraphFactoryController();
        RestAssured.baseURI = "http://localhost:4567";
    }

    @Test
    public void testConfigWorking(){
        Response response = get("/graph_factory").then().statusCode(200).extract().response().andReturn();
        String config = response.getBody().prettyPrint();
        assertTrue(config.contains("factory"));
    }

    @Test
    public void testMindmapsClient(){
        MindmapsGraph graph = MindmapsClient.getGraph("mindmapstest");
        MindmapsGraph graph2 = MindmapsClient.getGraph("mindmapstest2");
        MindmapsGraph graphCopy = MindmapsClient.getGraph("mindmapstest");
        assertNotEquals(0, graph.getGraph().traversal().V().toList().size());
        assertNotEquals(graph, graph2);
        assertEquals(graph, graphCopy);
        graph.close();
    }
}