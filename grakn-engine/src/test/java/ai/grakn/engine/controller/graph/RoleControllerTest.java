package ai.grakn.engine.controller.graph;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoleControllerTest {
    private static GraknGraph mockGraph;
    private static EngineGraknGraphFactory mockFactory = mock(EngineGraknGraphFactory.class);

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        MetricRegistry metricRegistry = new MetricRegistry();

        new RoleController(mockFactory, spark, metricRegistry);
    });

    @Before
    public void setupMock(){
        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");

        when(mockGraph.putRole(anyString())).thenAnswer(invocation ->
            graphContext.graph().putRole((String) invocation.getArgument(0)));
        when(mockGraph.getRole(anyString())).thenAnswer(invocation ->
            graphContext.graph().getRole(invocation.getArgument(0)));

        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.READ)).thenReturn(mockGraph);
        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockGraph);
    }

//    @Test
    public void postRoleTypeShouldExecuteSuccessfully() {
        Json body = Json.object("roleLabel", "newRole");
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .body(body.toString())
            .post("/graph/role");

        Map<String, Object> responseBody = Json.read(response.body().asString()).asMap();

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.get("conceptId"), notNullValue());
        assertThat(responseBody.get("roleLabel"), equalTo("newRole"));
    }

    @Test
    public void getRoleFromMovieGraphShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .get("/graph/role/production-with-cluster");

        Map<String, Object> responseBody = Json.read(response.body().asString()).asMap();

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.get("conceptId"), notNullValue());
        assertThat(responseBody.get("roleLabel"), equalTo("production-with-cluster"));
    }
}
