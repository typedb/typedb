package io.mindmaps.api;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.factory.MindmapsClient;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
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
        MindmapsGraph graph = MindmapsClient.newGraph();
        assertNotEquals(0, graph.getGraph().traversal().V().toList().size());
        graph.close();
    }
}