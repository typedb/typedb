package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.util.REST;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.function.Function;

import static ai.grakn.util.REST.Request.Graql.*;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class DashboardControllerTest {

    private Printer<Json> jsonPrinter;

    private Response sendQueryExplain(String query) {
        return RestAssured.with()
                .queryParam(QUERY, query)
                .queryParam(KEYSPACE, genealogyKB.tx().getKeyspace())
                .queryParam(INFER, true)
                .queryParam(MATERIALISE, false)
                .queryParam(LIMIT_EMBEDDED, -1)
                .accept(APPLICATION_HAL)
                .get(REST.WebPath.Dashboard.EXPLAIN);
    }


    @ClassRule
    public static final SampleKBContext genealogyKB = SampleKBContext.preLoad(GenealogyKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        EngineGraknTxFactory factory = EngineGraknTxFactory.createAndLoadSystemSchema(GraknEngineConfig.create().getProperties());
        new DashboardController(factory, spark);
    });

    @Before
    public void setUp() {
        jsonPrinter = Printers.json();
    }

    @Test
    public void whenExecutingExploreQuery_responseIsValid(){
        Query<?> query = genealogyKB.tx().graql().infer(true).parse("match ($x,$y) isa marriage; offset 0; limit 1; get;");
        Map<String,Json> marriage = Json.read(jsonPrinter.graqlString(query.execute())).asJsonList().get(0).asJsonMap();
        String id1 = marriage.get("x").asJsonMap().get("id").asString();
        String id2 = marriage.get("y").asJsonMap().get("id").asString();

        String queryString = String.format("match  $a id '%s'; $b id '%s';  ($a,$b) isa marriage; get;",id1,id2);

        Response explainResponse = sendQueryExplain(queryString);
        explainResponse.then().statusCode(200);
        Json jsonResponse = Json.read(explainResponse.asString());
        jsonResponse.asJsonList().forEach(concept->{
            assertTrue(concept.has("_baseType"));
            assertTrue(concept.has("_type"));
            assertTrue(concept.has("_id"));
        });
    }

}
