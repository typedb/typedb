/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.hal;

import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.GetQuery;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.AcademyKB;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.util.Schema;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.internal.hal.HALBuilder.HALExploreConcept;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALArrayData;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class HALBuilderTest {

    @ClassRule
    public static final SampleKBContext academyKB = SampleKBContext.preLoad(AcademyKB.get());

    public static final SampleKBContext genealogyKB = SampleKBContext.preLoad(GenealogyKB.get());


    @Test
    public void whenReceivingHALResponse_EnsureResponseContainsConceptDetails() {
        Json response = getHALRepresentation(academyKB.tx(), "match $x isa entity; limit 5; get;");
        Keyspace keyspace = academyKB.tx().getKeyspace();
        assertEquals(5, response.asList().size());

        response.asJsonList().forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            assertTrue(halObj.at("_links").at("self").at("href").asString().contains(keyspace.getValue()));
            assertTrue(halObj.at("_links").at("explore").asJsonList().get(0).at("href").asString().contains("explore"));
            assertTrue(halObj.has("_type"));
            assertTrue(halObj.has("_id"));
            assertFalse(halObj.has("_implicit"));
        });
    }

    @Test
    public void whenExecuteExploreHAL_EnsureHALResponseContainsCorrectExploreLinks() {
        Json response = getHALRepresentation(academyKB.tx(), "match $x isa entity; limit 5; get;");
        String conceptId = response.asJsonList().get(0).at("_id").asString();
        Json halObj = getHALExploreRepresentation(academyKB.tx(), conceptId);
        assertTrue(halObj.at("_links").at("explore").asJsonList().get(0).at("href").asString().contains("explore"));
    }

    @Test
    public void whenExecuteExploreHALOnAttribute_EnsureHALResponseContainsCorrectDataType() {
        Json response = getHALRepresentation(academyKB.tx(), "match $x label 'name'; get;");
        String conceptId = response.asJsonList().get(0).at("_id").asString();
        Json halObj = getHALExploreRepresentation(academyKB.tx(), conceptId);
        assertEquals("java.lang.String", halObj.at("_dataType").asString());
    }

    @Test
    public void whenAskForRelationTypes_EnsureAllObjectsHaveImplicitField() {
        Json response = getHALRepresentation(academyKB.tx(), "match $x sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + "; get;");
        response.asJsonList().forEach(halObj -> {
            assertTrue(halObj.has("_implicit"));
            if (halObj.at("_name").asString().startsWith("has-")) {
                assertTrue(halObj.at("_implicit").asBoolean());
            }
        });
    }

    @Test
    public void whenUseSelectInQueryUsingInference_EnsureWeReceiveAValidHALResponse() {
        Json response = getHALRepresentation(academyKB.tx(), "match $article isa article has subject \"Italian Referendum\";\n" +
                "$platform isa oil-platform has distance-from-coast <= 18;\n" +
                "(location: $country, located: $platform) isa located-in;\n" +
                "$country isa country has name \"Italy\";\n" +
                "(owner: $company, owned: $platform) isa owns;\n" +
                "(issuer: $company, issued: $bond) isa issues;\n" +
                "offset 0; limit 5; get $bond, $article;");

        // Limit to 5 results, each result will contain 2 variables, so the expected size of HAL results is 10.
        assertEquals(10, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("bond") || entityType.equals("article"));
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
                "offset 0; limit 5; get $bond, $article;");

        // Limit to 5 results, each result will contain 2 variables, so the expected size of HAL results is 10.
        assertEquals(10, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("bond") || entityType.equals("article"));
        });
    }

    @Test
    public void whenSelectInferredRelationWithSingleVar_EnsureValidExplanationHrefIsContainedInResponse() {
        Json response = getHALRepresentation(genealogyKB.tx(), "match $x isa marriage; offset 0; limit 5; get;");
        assertEquals(5, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            assertEquals("inferred-relationship", halObj.at("_baseType").asString());
            String href = halObj.at("_links").at("self").at("href").asString();
            assertTrue(href.contains("($a,$b) isa marriage;"));
        });
    }

    @Test
    public void whenTriggerReasonerWithTransitiveRule_EnsureWeReceiveAValidHALResponse() {
        Json response = getHALRepresentation(academyKB.tx(), "match $x isa region; $y isa oil-platform; (located: $y, location: $x) isa located-in; limit 20; get;");
        // Limit to 20 results, each result will contain 3 variables, expected size 60
        assertEquals(60, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("oil-platform") || entityType.equals("region") || entityType.equals("located-in"));
        });
    }

    private Json getHALRepresentation(GraknTx graph, String queryString) {
        GetQuery query = graph.graql().materialise(false).infer(true).parse(queryString);
        return renderHALArrayData(query, 0, 5);
    }

    private Json getHALExploreRepresentation(GraknTx graph, String conceptId) {
        Concept concept = graph.getConcept(ConceptId.of(conceptId));
        return Json.read(HALExploreConcept(concept, graph.getKeyspace(), 0, 5));
    }

    private Json getHALRepresentationNoInference(GraknTx graph, String queryString) {
        GetQuery query = graph.graql().materialise(false).infer(false).parse(queryString);
        return renderHALArrayData(query, 0, 5);
    }

}
