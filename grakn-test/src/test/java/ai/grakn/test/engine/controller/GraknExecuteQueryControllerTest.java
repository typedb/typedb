package ai.grakn.test.engine.controller;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.GraphContext;
import ai.grakn.test.SparkContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.REST;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.function.Function;

import static ai.grakn.util.REST.Request.Graql.*;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GraknExecuteQueryControllerTest {

    private Printer<Json> jsonPrinter;
    private Printer<Function<StringBuilder, StringBuilder>> graqlPrinter;
    private Printer halPrinter;

    private Response sendQuery(String query) {
        return sendQuery(query, APPLICATION_JSON_GRAQL);
    }

    private Response sendQuery(String query, String acceptType) {
        return sendQuery(query, acceptType, true, false, -1);
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

    @Before
    public void setUp() {
        jsonPrinter = Printers.json();
        graqlPrinter = Printers.graql(false);
        halPrinter = Printers.hal(graphContext.graph().getKeyspace());
    }

    @Test
    public void whenRunningMatchQuery_JsonResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie;", jsonPrinter, APPLICATION_JSON_GRAQL);
    }

    @Test
    public void whenRunningMatchQuery_GraqlResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie;", graqlPrinter, APPLICATION_TEXT);
    }

    @Test
    public void whenRunningMatchQuery_HalResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie;", halPrinter, APPLICATION_HAL);
    }

    @Test
    public void testInsertQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);
        Assert.assertFalse(resp.jsonPath().getList("response").isEmpty());
    }

    @Test
    public void whenRunningIdempotentInsertQuery_JsonResponseIsTheSameAsJavaGraql() {
        assertResponseSameAsJavaGraql("insert $x label movie;", jsonPrinter, APPLICATION_JSON_GRAQL);
    }

    @Test
    public void whenRunningIdempotentInsertQuery_GraqlResponseIsTheSameAsJavaGraql() {
        assertResponseSameAsJavaGraql("insert $x label movie;", graqlPrinter, APPLICATION_TEXT);
    }

    @Test
    public void whenRunningIdempotentInsertQuery_HalResponseIsTheSameAsJavaGraql() {
        assertResponseSameAsJavaGraql("insert $x label movie;", halPrinter, APPLICATION_HAL);
    }

    @Test
    public void testDeleteQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);
        String id = resp.jsonPath().getList("response").get(0).toString();
        resp = sendQuery("match $x id \"" + id + "\"; delete $x; ");
        resp.then().statusCode(200);
        assertNull(resp.jsonPath().get("response"));
    }
    
    @Test
    public void wehnRunningAggregateQuery_JsonResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; aggregate count;", jsonPrinter, APPLICATION_JSON_GRAQL);
    }

    @Test
    public void whenRunningAggregateQuery_GraqlResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; aggregate count;", graqlPrinter, APPLICATION_TEXT);
    }

    @Test
    public void whenRunningAggregateQuery_HalResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; aggregate count;", halPrinter, APPLICATION_HAL);
    }

    @Test
    public void whenRunningAskQuery_JsonResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; ask;", jsonPrinter, APPLICATION_JSON_GRAQL);
    }

    @Test
    public void whenRunningAskQuery_GraqlResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; ask;", graqlPrinter, APPLICATION_TEXT);
    }

    @Test
    public void whenRunningAskQuery_HalResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; ask;", halPrinter, APPLICATION_HAL);
    }

    @Test
    public void testBadQuery() {
        sendQuery(" gibberish ads a;49 agfdgdsf").then().statusCode(400);
    }
    
    @Test
    public void testBadAccept() {
        sendQuery("match $x isa movie; ask;", "application/msword", true, false, -1).then().statusCode(406);        
    }

    private void assertResponseSameAsJavaGraql(String queryString, Printer<?> printer, String acceptType) {
        Response resp = sendQuery(queryString, acceptType);
        resp.then().statusCode(200);

        String response = Json.read(resp.body().asString()).at("response").toString();

        Query<?> query = graphContext.graph().graql().parse(queryString);

        String expectedString = printer.graqlString(query.execute());

        Object expected = acceptType.equals(APPLICATION_TEXT) ? expectedString : Json.read(expectedString);

        assertEquals(Json.make(expected).toString(), response);
    }

}
