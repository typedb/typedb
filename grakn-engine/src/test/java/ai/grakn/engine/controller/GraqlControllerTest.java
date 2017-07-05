package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.REST;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static org.junit.Assert.assertEquals;

public class GraqlControllerTest {
    
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
            .body(query)
            .queryParam(INFER, reasonser)
            .queryParam(MATERIALISE, materialise)
            .queryParam(LIMIT_EMBEDDED, limitEmbedded)
            .accept(acceptType)
            .post(REST.WebPath.Graph.ANY_GRAQL);
    }

    //TODO: Cleanup this hack. The problem is that <code>EngineGraknGraphFactory</code> is called before the graph context. This means that cassandra has not yet started when we try to create the engine graph.
    private static GraphContext graphContextTemp = GraphContext.preLoad(MovieGraph.get());

    private static EngineGraknGraphFactory engineGraknGraphFactory =
            EngineGraknGraphFactory.create(GraknEngineConfig.create().getProperties());

    @ClassRule
    public static GraphContext graphContext = graphContextTemp;

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new GraqlController(engineGraknGraphFactory, spark);
    });

    @Test
    public void testMatchQuery() {
        Response resp = sendQuery("match $x isa movie;");
        resp.then().statusCode(200);        
        Assert.assertFalse(jsonResponse(resp).asJsonList().isEmpty());
    }
    
    @Test
    public void testInsertQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);
        Assert.assertFalse(jsonResponse(resp).asJsonList().isEmpty());
    }
    
    @Test
    public void testDeleteQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);

        String id =jsonResponse(resp).at(0).asJsonMap().get("x").asJsonMap().get("id").toString();
        resp = sendQuery("match $x id " + id + "; delete $x; ");
        resp.then().statusCode(200);
    }
    
    @Test
    public void testAggregateQuery() {
        Response resp = sendQuery("match $x isa movie; aggregate count;");
        resp.then().statusCode(200);
        assertEquals(7, jsonResponse(resp).asInteger());
    }
    
    @Test
    public void testAskQuery() {
        Response resp = sendQuery("match $x isa movie; ask;");
        resp.then().statusCode(200);

        assertEquals(true, jsonResponse(resp).asBoolean());
    }
    
    @Test
    public void testBadQuery() {
        sendQuery(" gibberish ads a;49 agfdgdsf").then().statusCode(400);
    }
    
    @Test
    public void testBadAccept() {
        sendQuery("match $x isa movie; ask;", "application/msword", true, false, -1).then().statusCode(406);        
    }

    protected static Json jsonResponse(Response response) {
        return response.getBody().as(Json.class, new JsonMapper());
    }
    
}
