package ai.grakn.engine.controller.api;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.REST;
import ai.grakn.util.SampleKBLoader;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.RELATIONSHIP_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.RELATIONSHIP_ENTITY_ROLE_ASSIGNMENT;
import static ai.grakn.util.REST.WebPath.Api.RELATIONSHIP_TYPE;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelationshipControllerTest {
    private static GraknTx mockTx;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext sampleKB = MovieKB.context();

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new RelationshipController(mockFactory, spark);
        new EntityController(mockFactory, spark);
    });

    @Before
    public void setupMock(){
        mockTx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.keyspace()).thenReturn(SampleKBLoader.randomKeyspace());

        when(mockTx.getRelationshipType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getRelationshipType(invocation.getArgument(0)));
        when(mockTx.getEntityType(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getEntityType(invocation.getArgument(0)));
        when(mockTx.getConcept(any())).thenAnswer(invocation ->
            sampleKB.tx().getConcept(invocation.getArgument(0)));
        when(mockTx.getRole(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getRole(invocation.getArgument(0)));
        Mockito.doAnswer(e -> { sampleKB.tx().commit(); return null; } ).when(mockTx).commit();

        when(mockFactory.tx(mockTx.keyspace(), GraknTxType.READ)).thenReturn(mockTx);
        when(mockFactory.tx(mockTx.keyspace(), GraknTxType.WRITE)).thenReturn(mockTx);
    }

    @Test
    public void postRelationshipShouldExecuteSuccessfully() {
        String directedBy = "directed-by";

        Response response = with()
            .post(REST.resolveTemplate(RELATIONSHIP_TYPE + "/" + directedBy, mockTx.keyspace().getValue()));

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at(RELATIONSHIP_OBJECT_JSON_FIELD).at(CONCEPT_ID_JSON_FIELD).asString(), notNullValue());
    }

    @Test
    public void assignEntityAndRoleToRelationshipShouldExecuteSuccessfully() {
        // directed-by (relT) -- production-being-directed (role) -- director (role)
        String relationshipTypeLabel = "directed-by";
        String roleLabel = "director";
        String entityTypeLabel = "person";

        String entityConceptId;
        String relationshipConceptId;
        try (GraknTx tx = mockFactory.tx(mockTx.keyspace(), GraknTxType.WRITE)) {
            entityConceptId = tx.getEntityType(entityTypeLabel).addEntity().getId().getValue();
            relationshipConceptId = tx.getRelationshipType(relationshipTypeLabel).addRelationship().getId().getValue();
            tx.commit();
        }

        String path = REST.resolveTemplate(
                RELATIONSHIP_ENTITY_ROLE_ASSIGNMENT, mockTx.keyspace().getValue(), relationshipConceptId,
                entityConceptId, roleLabel
        );
        Response response = with().put(path);

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
    }
}
