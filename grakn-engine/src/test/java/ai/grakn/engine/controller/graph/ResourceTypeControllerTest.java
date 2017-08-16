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

public class ResourceTypeControllerTest {
    private static GraknGraph mockGraph;
    private static EngineGraknGraphFactory mockFactory = mock(EngineGraknGraphFactory.class);

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        MetricRegistry metricRegistry = new MetricRegistry();

        new ResourceTypeController(mockFactory, spark, metricRegistry);
    });

    @Before
    public void setupMock(){
        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");

        when(mockGraph.putResourceType(anyString(), any())).thenAnswer(invocation -> {
            String label = invocation.getArgument(0);
            ResourceType.DataType<?> dataType = invocation.getArgument(1);
            return graphContext.graph().putResourceType(label, dataType);
        });
        when(mockGraph.getResourceType(anyString())).thenAnswer(invocation ->
            graphContext.graph().getResourceType(invocation.getArgument(0)));

        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.READ)).thenReturn(mockGraph);
        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockGraph);
    }

//    @Test
    public void postResourceTypeShouldExecuteSuccessfully() {
        Json body = Json.object(
            "resourceTypeLabel", "newResource",
            "resourceTypeDataType", "string"
            );
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .body(body.toString())
            .post("/graph/resourceType");

        Map<String, Object> responseBody = Json.read(response.body().asString()).asMap();

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.get("conceptId"), notNullValue());
        assertThat(responseBody.get("resourceTypeLabel"), equalTo("newResource"));
    }

    @Test
    public void getResourceTypeFromMovieGraphShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .get("/graph/resourceType/tmdb-vote-count");

        Map<String, Object> responseBody = Json.read(response.body().asString()).asMap();

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.get("conceptId"), notNullValue());
        assertThat(responseBody.get("resourceTypeLabel"), equalTo("tmdb-vote-count"));
        assertThat(responseBody.get("resourceTypeDataType"), equalTo("long"));
    }
}
