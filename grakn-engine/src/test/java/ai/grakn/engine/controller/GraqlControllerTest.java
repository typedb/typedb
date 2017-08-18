package ai.grakn.engine.controller;

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.function.Function;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static org.junit.Assert.assertEquals;

public class GraqlControllerTest {

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
            .body(query)
            .queryParam(INFER, reasonser)
            .queryParam(MATERIALISE, materialise)
            .queryParam(LIMIT_EMBEDDED, limitEmbedded)
            .accept(acceptType)
            .post(REST.WebPath.KB.ANY_GRAQL);
    }

    @ClassRule
    public static SampleKBContext graphContext = SampleKBContext.preLoad(MovieKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        EngineGraknTxFactory factory = EngineGraknTxFactory.createAndLoadSystemSchema(GraknEngineConfig.create().getProperties());
        new GraqlController(factory, spark, new MetricRegistry());
    });

    @Before
    public void setUp() {
        jsonPrinter = Printers.json();
        graqlPrinter = Printers.graql(false);
        halPrinter = Printers.hal(graphContext.graph().getKeyspace(), -1);
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
        try {
            resp.then().statusCode(200);
            Assert.assertFalse(resp.jsonPath().getList(".").isEmpty());
        } finally {
            ConceptId id = ConceptId.of(resp.jsonPath().getList("x.id").get(0).toString());
            graphContext.rollback();
            graphContext.graph().graql().match(var("x").id(id)).delete("x").execute();
        }
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
        String id = resp.jsonPath().getList("x.id").get(0).toString();
        resp = sendQuery("match $x id \"" + id + "\"; delete $x; ");
        resp.then().statusCode(200);
        assertEquals(Json.nil(), Json.read(resp.asString()));
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
        assertResponseSameAsJavaGraql("match $x isa movie; aggregate ask;", jsonPrinter, APPLICATION_JSON_GRAQL);
    }

    @Test
    public void whenRunningAskQuery_GraqlResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; aggregate ask;", graqlPrinter, APPLICATION_TEXT);
    }

    @Test
    public void whenRunningAskQuery_HalResponseIsTheSameAsJava() {
        assertResponseSameAsJavaGraql("match $x isa movie; aggregate ask;", halPrinter, APPLICATION_HAL);
    }

    @Test
    public void whenRunningQueryWithLimitEmbedded_HalResponseIsTheSameAsJava() {
        String queryString = "match $x isa movie;";
        int limitEmbedded = 42;
        Response resp = sendQuery(queryString, APPLICATION_HAL, false, false, limitEmbedded);
        Printer printer = Printers.hal(graphContext.graph().getKeyspace(), limitEmbedded);
        assertResponseSameAsJavaGraql(resp, queryString, printer, APPLICATION_HAL);
    }

    @Test
    public void testBadQuery() {
        sendQuery(" gibberish ads a;49 agfdgdsf").then().statusCode(400);
    }
    
    @Test
    public void whenAcceptHeaderIsInvalid_Return406Code() {
        sendQuery("match $x isa movie; aggregate ask;", "application/msword", true, false, -1).then().statusCode(406);
    }

    private void assertResponseSameAsJavaGraql(String queryString, Printer<?> printer, String acceptType) {
        Response resp = sendQuery(queryString, acceptType);
        assertResponseSameAsJavaGraql(resp, queryString, printer, acceptType);
    }

    private void assertResponseSameAsJavaGraql(Response resp, String queryString, Printer<?> printer, String acceptType) {
        resp.then().statusCode(200);

        graphContext.rollback();
        Query<?> query = graphContext.graph().graql().parse(queryString);

        String expectedString = printer.graqlString(query.execute());

        Object expected = acceptType.equals(APPLICATION_TEXT) ? expectedString : Json.read(expectedString);

        assertEquals(expected.toString(), resp.body().asString());
    }
}
