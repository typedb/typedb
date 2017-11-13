package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.REST;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DashboardControllerTest {
    private static final LockProvider mockLockProvider = mock(LockProvider.class);
    private static final Lock mockLock = mock(Lock.class);
    private Printer<Json> jsonPrinter;

    private Response sendQueryExplain(String query) {
        return RestAssured.with()
                .queryParam(QUERY, query)
                .queryParam(KEYSPACE, genealogyKB.tx().keyspace().getValue())
                .queryParam(INFER, true)
                .queryParam(LIMIT_EMBEDDED, -1)
                .accept(APPLICATION_HAL)
                .get(REST.WebPath.Dashboard.EXPLAIN);
    }

    private Response sendQueryExplore(String id) {
        return RestAssured.with()
                .queryParam(KEYSPACE, genealogyKB.tx().keyspace().getValue())
                .queryParam(INFER, true)
                .queryParam(LIMIT_EMBEDDED, -1)
                .accept(APPLICATION_HAL)
                .get(REST.WebPath.Dashboard.EXPLORE + id);
    }

    public static final SampleKBContext genealogyKB = GenealogyKB.context();

    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        EngineGraknTxFactory factory = EngineGraknTxFactory.createAndLoadSystemSchema(mockLockProvider, GraknEngineConfig.create().getProperties());
        new DashboardController(factory, spark);
    });

    @ClassRule
    public static final RuleChain chain = RuleChain.emptyRuleChain().around(genealogyKB).around(sparkContext);

    @Before
    public void setUp() {
        jsonPrinter = Printers.json();
        when(mockLockProvider.getLock(any())).thenReturn(mockLock);
    }

    @Test
    public void whenExecutingExplainQuery_responseIsValid() {
        Query<?> query = genealogyKB.tx().graql().infer(true).parse("match ($x,$y) isa marriage; offset 0; limit 1; get;");
        Map<String, Json> marriage = Json.read(jsonPrinter.graqlString(query.execute())).asJsonList().get(0).asJsonMap();
        String id1 = marriage.get("x").asJsonMap().get("id").asString();
        String id2 = marriage.get("y").asJsonMap().get("id").asString();

        String queryString = String.format("match  $a id '%s'; $b id '%s';  ($a,$b) isa marriage; get;", id1, id2);

        Response explainResponse = sendQueryExplain(queryString);
        explainResponse.then().statusCode(200);
        Json jsonResponse = Json.read(explainResponse.asString());
        jsonResponse.asJsonList()
                .stream().flatMap(obj -> obj.asJsonMap().values().stream())
                .forEach(concept -> {
                    assertTrue(concept.has("_baseType"));
                    assertTrue(concept.has("_links"));
                    assertTrue(concept.has("_id"));
                });
    }

    @Test
    public void whenExecutingExploreQueryOnRole_responseIsValid() {
        Query<?> query = genealogyKB.tx().graql().infer(true).parse("match $x label spouse; offset 0; limit 1; get;");
        Map<String, Json> spouse = Json.read(jsonPrinter.graqlString(query.execute())).asJsonList().get(0).asJsonMap();
        String id = spouse.get("x").asJsonMap().get("id").asString();


        Response exploreResponse = sendQueryExplore(id);
        exploreResponse.then().statusCode(200);
        Json jsonResponse = Json.read(exploreResponse.body().asString());
        assertTrue(jsonResponse.has("_baseType"));
        assertTrue(jsonResponse.has("_name"));
        assertTrue(jsonResponse.has("_id"));

    }

    @Test
    public void whenExecutingExploreQueryOnType_responseIsValid() {
        Query<?> query = genealogyKB.tx().graql().infer(true).parse("match $x label marriage; offset 0; limit 1; get;");
        Map<String, Json> marriageType = Json.read(jsonPrinter.graqlString(query.execute())).asJsonList().get(0).asJsonMap();
        String id = marriageType.get("x").asJsonMap().get("id").asString();


        Response exploreResponse = sendQueryExplore(id);
        exploreResponse.then().statusCode(200);
        Json jsonResponse = Json.read(exploreResponse.body().asString());
        assertTrue(jsonResponse.has("_baseType"));
        assertTrue(jsonResponse.has("_name"));
        assertTrue(jsonResponse.has("_id"));

    }

}
