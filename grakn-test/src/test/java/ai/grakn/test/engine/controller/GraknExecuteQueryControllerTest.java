package ai.grakn.test.engine.controller;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.test.GraphContext;
import ai.grakn.test.SparkContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.REST;

public class GraknExecuteQueryControllerTest {
    
    private Response sendQuery(String query) {
        return sendQuery(query, APPLICATION_JSON_GRAQL, true, false, -1);
    }
    
    private Response sendQuery(String query, 
                               String acceptType, 
                               boolean reasonser,
                               boolean materialise, 
                               int limitEmbedded) {
        return RestAssured.with()
            .queryParam(KEYSPACE, graphContext.graph().getKeyspace())
            .queryParam(QUERY, query)
            .queryParam(INFER, reasonser)
            .queryParam(MATERIALISE, materialise)
            .queryParam(LIMIT_EMBEDDED, limitEmbedded)
            .accept(acceptType)
            .post(REST.WebPath.Graph.ANY_GRAQL);
    }
    
    private static EngineGraknGraphFactory engineGraknGraphFactory = 
            EngineGraknGraphFactory.create(GraknEngineConfig.create().getProperties());
    
    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new GraqlController(engineGraknGraphFactory, spark);
    });
        
    @Test
    public void testMatchQuery() {
        Response resp = sendQuery("match $x isa movie;");
        resp.then().statusCode(200);        
        Assert.assertFalse(resp.jsonPath().getList("response").isEmpty());
    }
    
    @Test
    public void testInsertQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);        
        Assert.assertFalse(resp.jsonPath().getList("response").isEmpty());
    }
    
    @Test
    public void testDeleteQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);
        String id = resp.jsonPath().getList("response").get(0).toString();
        resp = sendQuery("match $x id \"" + id + "\"; delete $x; ");
        resp.then().statusCode(200);
    }
    
    @Test
    public void testAggregateQuery() {
        Response resp = sendQuery("match $x isa movie; aggregate count;");
        resp.then().statusCode(200);
        Assert.assertEquals(7,resp.jsonPath().getInt("response"));
    }
    
    @Test
    public void testAskQuery() {
        Response resp = sendQuery("match $x isa movie; ask;");
        resp.then().statusCode(200);
        Assert.assertEquals(true,resp.jsonPath().getBoolean("response"));
    }
    
    @Test
    public void testBadQuery() {
        sendQuery(" gibberish ads a;49 agfdgdsf").then().statusCode(400);
    }
    
    @Test
    public void testBadAccept() {
        sendQuery("match $x isa movie; ask;", "application/msword", true, false, -1).then().statusCode(406);        
    }
    
}
