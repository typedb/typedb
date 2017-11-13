package ai.grakn.graql.internal.printer;

import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryParser;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.AcademyKB;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.util.Schema;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.internal.hal.HALBuilder.HALExploreConcept;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HALPrinterTest {

    @ClassRule
    public static final SampleKBContext academyKB = AcademyKB.context();

    public static final SampleKBContext genealogyKB = GenealogyKB.context();

    @Test
    public void whenReceivingHALResponse_EnsureResponseContainsConceptDetails() {
        Json response = printHAL(academyKB.tx(), "match $x isa entity; limit 1; get;");
        Keyspace keyspace = academyKB.tx().keyspace();
        assertEquals(1, response.asList().size());

        Json halObj = response.asJsonList().get(0).at("x");
        assertEquals(halObj.at("_baseType").asString(), "ENTITY");
        assertTrue(halObj.at("_links").at("self").at("href").asString().contains(keyspace.getValue()));
        assertTrue(halObj.at("_links").at("explore").asJsonList().get(0).at("href").asString().contains("explore"));
        assertTrue(halObj.has("_type"));
        assertTrue(halObj.has("_id"));
        assertFalse(halObj.has("_implicit"));

    }

    @Test
    public void whenExecuteExploreHAL_EnsureHALResponseContainsCorrectExploreLinks() {
        Json response = printHAL(academyKB.tx(), "match $x isa entity; limit 5; get;");
        String conceptId = response.asJsonList().get(0).at("x").at("_id").asString();
        Json halObj = getHALExploreRepresentation(academyKB.tx(), conceptId);
        assertTrue(halObj.at("_links").at("explore").asJsonList().get(0).at("href").asString().contains("explore"));
    }

    @Test
    public void whenExecuteExploreHALOnAttribute_EnsureHALResponseContainsCorrectDataType() {
        Json response = printHAL(academyKB.tx(), "match $x label 'name'; get;");
        String conceptId = response.asJsonList().get(0).at("x").at("_id").asString();
        Json halObj = getHALExploreRepresentation(academyKB.tx(), conceptId);
        assertEquals("java.lang.String", halObj.at("_dataType").asString());
    }

    @Test
    public void whenAskForRelationTypes_EnsureAllObjectsHaveImplicitField() {
        Json response = printHAL(academyKB.tx(), "match $x sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + "; get;");
        response.asJsonList().stream().map(x -> x.at("x")).forEach(halObj -> {
            assertTrue(halObj.has("_implicit"));
            if (halObj.at("_name").asString().startsWith("has-")) {
                assertTrue(halObj.at("_implicit").asBoolean());
            }
        });
    }

    @Test
    public void whenUseSelectInQueryUsingInference_EnsureWeReceiveAValidHALResponse() {
        Json response = printHAL(academyKB.tx(), "match $article isa article has subject \"Italian Referendum\";\n" +
                "$platform isa oil-platform has distance-from-coast <= 18;\n" +
                "(location: $country, located: $platform) isa located-in;\n" +
                "$country isa country has name \"Italy\";\n" +
                "(owner: $company, owned: $platform) isa owns;\n" +
                "(issuer: $company, issued: $bond) isa issues;\n" +
                "offset 0; limit 2; get $bond, $article;");

        assertEquals(2, response.asList().size());
        response.asJsonList().stream().map(x -> x.at("bond")).forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("bond"));
        });
        response.asJsonList().stream().map(x -> x.at("article")).forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("article"));
        });
    }

    @Test
    public void whenUseSelectInQueryWithoutUsingInference_EnsureWeReceiveAValidHALResponse() {
        Json response = getHALRepresentationNoInference(academyKB.tx(), "match $article isa article has subject \"Italian Referendum\";\n" +
                "$platform isa oil-platform has distance-from-coast <= 18;\n" +
                "(location: $country, located: $platform) isa located-in;\n" +
                "$country isa country has name \"Italy\";\n" +
                "(owner: $company, owned: $platform) isa owns;\n" +
                "(issuer: $company, issued: $bond) isa issues;\n" +
                "offset 0; limit 2; get $bond, $article;");

        assertEquals(2, response.asList().size());
        response.asJsonList().stream().map(x -> x.at("bond")).forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("bond"));
        });
        response.asJsonList().stream().map(x -> x.at("article")).forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("article"));
        });
    }

    @Test
    public void whenSelectInferredRelationWithSingleVar_EnsureValidExplanationHrefIsContainedInResponse() {
        Json response = printHAL(genealogyKB.tx(), "match $x isa marriage; offset 0; limit 5; get;");
        assertEquals(5, response.asList().size());
        response.asJsonList().stream().map(x -> x.at("x")).forEach(halObj -> {
            assertEquals("INFERRED_RELATIONSHIP", halObj.at("_baseType").asString());
            String href = halObj.at("_links").at("self").at("href").asString();
            assertTrue(href.contains("&query=match {"));
        });
    }

    @Test
    public void whenSelectRelationshipType_EnsureRolesAreEmbeddedWithNames() {
        Json response = printHAL(genealogyKB.tx(), "match $x label parentship; offset 0; limit 5; get;");
        String conceptId = response.asJsonList().get(0).at("x").at("_id").asString();
        Json halObj = getHALExploreRepresentation(genealogyKB.tx(), conceptId);
        halObj.at("_embedded").at("relates").asJsonList().forEach(role -> {
            assertTrue(role.has("_name"));
        });
    }

    @Test
    public void whenTriggerReasonerWithTransitiveRule_EnsureWeReceiveAValidHALResponseWithRelationshipVariableIncluded() {
        Json response = printHAL(academyKB.tx(), "match $x isa region; $y isa oil-platform; (located: $y, location: $x) isa located-in; limit 20; get;");
        // Limit to 20 results, each result will contain 3 variables, expected size 60
        assertEquals(20, response.asList().size());
        response.asJsonList().forEach(resultMap -> {
            assertEquals(3, resultMap.asJsonMap().keySet().size());
        });
        response.asJsonList().stream().map(x -> x.at("x")).forEach(halObj -> {
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("region"));
        });
        response.asJsonList().stream().map(x -> x.at("y")).forEach(halObj -> {
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("oil-platform"));
        });
        response.asJsonList().stream().flatMap(x -> x.asJsonMap()
                .entrySet().stream()).filter(entry -> (!entry.getKey().equals("x") && !entry.getKey().equals("y")))
                .forEach(halObj -> {
                    String relationshipType = halObj.getValue().at("_type").asString();
                    String relationshipBaseType = halObj.getValue().at("_baseType").asString();
                    assertTrue(relationshipType.equals("located-in"));
                    assertTrue(relationshipBaseType.equals("INFERRED_RELATIONSHIP"));
                });
    }


    private Json getHALExploreRepresentation(GraknTx graph, String conceptId) {
        Concept concept = graph.getConcept(ConceptId.of(conceptId));
        return Json.read(HALExploreConcept(concept, graph.keyspace(), 0, 5));
    }

    private Json printHAL(GraknTx graph, String queryString) {
        Printer<?> printer = Printers.hal(graph.keyspace(), 5);
        QueryParser parser = graph.graql().infer(true).parser();
        parser.defineAllVars(true);
        Query<?> query = parser.parseQuery(queryString);
        return Json.read(printer.graqlString(query.execute()));
    }

    private Json getHALRepresentationNoInference(GraknTx graph, String queryString) {
        Printer<?> printer = Printers.hal(graph.keyspace(), 5);
        QueryParser parser = graph.graql().infer(false).parser();
        Query<?> query = parser.parseQuery(queryString);
        return Json.read(printer.graqlString(query.execute()));
    }

}
