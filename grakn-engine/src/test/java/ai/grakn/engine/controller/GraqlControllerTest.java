package ai.grakn.engine.controller;

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.printer.JacksonPrinter;
import ai.grakn.graql.Query;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.REST.Request.Graql.ALLOW_MULTIPLE_QUERIES;
import static ai.grakn.util.REST.Request.Graql.DEFINE_ALL_VARS;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraqlControllerTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final LockProvider mockLockProvider = mock(LockProvider.class);
    private static final Lock mockLock = mock(Lock.class);

    private JacksonPrinter printer;

    private Response sendQuery(String query) {
        return sendQuery(query, APPLICATION_JSON);
    }

    private Response sendQuery(String query, String acceptType) {
        return sendQuery(query, acceptType, true, -1, sampleKB.tx().keyspace().getValue(), false);
    }

    private Response sendQuery(String query,
                               String acceptType,
                               boolean reasoner,
                               int limitEmbedded,
                               boolean multi) {
        return sendQuery(query, acceptType, reasoner, limitEmbedded, sampleKB.tx().keyspace().getValue(), multi);
    }


    private Response sendQuery(String query,
                               String acceptType,
                               boolean reasoner,
                               int limitEmbedded,
                               String keyspace, boolean multi) {
        return RestAssured.with()
                .body(query)
                .queryParam(EXECUTE_WITH_INFERENCE, reasoner)
                .queryParam(LIMIT_EMBEDDED, limitEmbedded)
                .queryParam(ALLOW_MULTIPLE_QUERIES, multi)
                .queryParam(DEFINE_ALL_VARS, true)
                .accept(acceptType)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, keyspace));
    }


    public static SampleKBContext sampleKB = MovieKB.context();

    public static SampleKBContext genealogyKB = GenealogyKB.context();

    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        EngineGraknTxFactory factory = EngineGraknTxFactory
                .createAndLoadSystemSchema(mockLockProvider, GraknConfig.create());
        new GraqlController(factory, spark, new MetricRegistry());
    });

    @ClassRule
    public static final RuleChain chain = RuleChain.emptyRuleChain().around(sampleKB).around(genealogyKB).around(sparkContext);

    @Before
    public void setUp() {
        printer = JacksonPrinter.create();
        when(mockLockProvider.getLock(any())).thenReturn(mockLock);
    }

    @Test
    public void whenRunningGetQuery_JsonResponseIsTheSameAsJava() {
        assertResponseMatchesExpectedObject("match $x isa movie; get;");
    }

    @Test
    public void testInsertQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        try {
            resp.then().statusCode(200);
            Assert.assertFalse(resp.jsonPath().getList(".").isEmpty());
        } finally {
            ConceptId id = ConceptId.of(resp.jsonPath().getList("x.id").get(0).toString());
            sampleKB.rollback();
            sampleKB.tx().graql().match(var("x").id(id)).delete("x").execute();
        }
    }

    @Test
    public void whenRunningIdempotentInsertQuery_JsonResponseCanBeMappedToJavaObject() {
        assertResponseMatchesExpectedObject("insert $x label movie;");
    }

    @Test
    public void whenRunningMultiIdempotentInsertQuery_JsonResponseIsTheSameAsJavaGraql() {
        String single = "insert $x label movie;";
        String queryString = single + "\n" + single;
        Response resp = sendQuery(queryString, APPLICATION_JSON, true, -1, true);
        resp.then().statusCode(200);
        sampleKB.rollback();
        Stream<Query<?>> query = sampleKB.tx().graql().parser().parseList(queryString);
        String graqlResult = printer.graqlString(query.map(Query::execute).collect(
                Collectors.toList()));
        Json expected = Json.read(graqlResult);
        assertEquals(expected, Json.read(resp.body().asString()));

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
        assertResponseMatchesExpectedObject("match $x isa movie; aggregate count;");
    }

    @Test
    public void whenRunningAskQuery_JsonResponseIsTheSameAsJava() {
        assertResponseMatchesExpectedObject("match $x isa movie; aggregate ask;");
    }

    @Test
    public void whenMatchingRules_ResponseStatusIs200() {
        String queryString = "match $x sub " + Schema.MetaSchema.RULE.getLabel().getValue() + "; get;";
        int limitEmbedded = 10;
        Response resp = sendQuery(queryString, APPLICATION_JSON, false, limitEmbedded, false);
        resp.then().statusCode(200);
    }

    @Test
    public void whenMatchingRuleExplanation_HALResponseContainsInferredRelation() {
        String queryString = "match ($x,$y) isa marriage; offset 0; limit 1; get;";
        int limitEmbedded = 10;
        Response resp = sendQuery(queryString, APPLICATION_JSON, true, limitEmbedded, genealogyKB.tx().keyspace().getValue(), false);
        resp.then().statusCode(200);
        Json jsonResp = Json.read(resp.body().asString());
        jsonResp.asJsonList().stream()
                .flatMap(x -> x.asJsonMap().entrySet().stream())
                .filter(entry -> (!entry.getKey().equals("x") && !entry.getKey().equals("y")))
                .map(entry -> entry.getValue())
                .filter(thing -> thing.has("_type") && thing.at("_type").asString().equals("marriage"))
                .forEach(thing -> {
                    assertEquals("INFERRED_RELATIONSHIP", thing.at("_baseType").asString());
                    thing.at("_embedded").at("spouse").asJsonList().forEach(spouse -> {
                        assertEquals("ENTITY", spouse.at("_baseType").asString());
                    });
                });

    }

    @Test
    public void whenMatchingJoinExplanation_HALResponseContainsInferredRelation() {
        String queryString = "match ($x,$y) isa siblings; $z isa person; offset 0; limit 2; get;";
        int limitEmbedded = 10;
        Response resp = sendQuery(queryString, APPLICATION_JSON, true, limitEmbedded, genealogyKB.tx().keyspace().getValue(), false);
        resp.then().statusCode(200);
        Json jsonResp = Json.read(resp.body().asString());
        jsonResp.asJsonList().stream()
                .flatMap(x -> x.asJsonMap().entrySet().stream())
                .filter(entry -> (!entry.getKey().equals("x") && !entry.getKey().equals("y") && !entry.getKey().equals("z")))
                .map(entry -> entry.getValue())
                .filter(thing -> thing.has("_type") && thing.at("_type").asString().equals("marriage"))
                .forEach(thing -> {
                    assertEquals("INFERRED_RELATIONSHIP", thing.at("_baseType").asString());
                });

    }

    @Test
    public void whenMatchingNotInferredRelation_HALResponseContainsRelation() {
        String queryString = "match ($x,$y) isa directed-by; offset 0; limit 25; get;";
        int limitEmbedded = 10;
        Response resp = sendQuery(queryString, APPLICATION_JSON, true, limitEmbedded, false);
        resp.then().statusCode(200);
        Json jsonResp = Json.read(resp.body().asString());
        jsonResp.asJsonList().stream()
                .flatMap(x -> x.asJsonMap().entrySet().stream())
                .filter(entry -> (!entry.getKey().equals("x") && !entry.getKey().equals("y")))
                .map(entry -> entry.getValue())
                .filter(thing -> thing.has("_type") && thing.at("_type").asString().equals("directed-by"))
                .forEach(thing -> {
                    assertEquals("RELATIONSHIP", thing.at("_baseType").asString());
                });

    }

    @Test
    public void testBadQuery() {
        sendQuery(" gibberish ads a;49 agfdgdsf").then().statusCode(400);
    }

    @Test
    public void whenAcceptHeaderIsInvalid_Return406Code() {
        sendQuery("match $x isa movie; aggregate ask;", "application/msword", true, -1,
                false).then().statusCode(406);
    }

    private void assertResponseMatchesExpectedObject(String queryString) {
        Response resp = sendQuery(queryString, APPLICATION_JSON);
        assertResponseMatchesExpectedObject(resp, queryString);
    }

    private void assertResponseMatchesExpectedObject(Response resp, String queryString) {
        resp.then().statusCode(200);

        sampleKB.rollback();
        Query<?> query = sampleKB.tx().graql().parse(queryString);

        String expectedObject = printer.graqlString(query.execute());

        Object expected = Json.read(expectedObject);

        assertEquals(expected.toString(), resp.body().asString());
    }
}
