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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Query;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.AcademyGraph;
import ai.grakn.test.graphs.GenealogyGraph;
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
    public static final GraphContext academyGraph = GraphContext.preLoad(AcademyGraph.get());

    public static final GraphContext genealogyGraph = GraphContext.preLoad(GenealogyGraph.get());


    @Test
    public void whenReceivingHALResponse_EnsureResponseContainsConceptDetails() {
        Json response = getHALRepresentation(academyGraph.graph(), "match $x isa entity; limit 5;");
        String keyspace = academyGraph.graph().getKeyspace();
        assertEquals(5, response.asList().size());

        response.asJsonList().forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            assertTrue(halObj.at("_links").at("self").at("href").asString().contains(keyspace));
            assertTrue(halObj.at("_links").at("explore").asJsonList().get(0).at("href").asString().contains("explore"));
            assertTrue(halObj.has("_type"));
            assertTrue(halObj.has("_id"));
            assertFalse(halObj.has("_implicit"));
        });
    }

    @Test
    public void whenExecuteExploreHAL_EnsureHALResponseContainsCorrectExploreLinks() {
        Json response = getHALRepresentation(academyGraph.graph(), "match $x isa entity; limit 5;");
        String conceptId = response.asJsonList().get(0).at("_id").asString();
        Json halObj = getHALExploreRepresentation(academyGraph.graph(), conceptId);
        assertTrue(halObj.at("_links").at("explore").asJsonList().get(0).at("href").asString().contains("explore"));
    }

    @Test
    public void whenAskForRelationTypes_EnsureAllObjectsHaveImplicitField() {
        Json response = getHALRepresentation(academyGraph.graph(), "match $x sub relation;");
        response.asJsonList().forEach(halObj -> {
            assertTrue(halObj.has("_implicit"));
            if(halObj.at("_name").asString().startsWith("has-")){
                assertTrue(halObj.at("_implicit").asBoolean());
            }
        });
    }

    @Test
    public void whenUseSelectInQueryUsingInference_EnsureWeReceiveAValidHALResponse() {
        Json response = getHALRepresentation(academyGraph.graph(), "match $article isa article has subject \"Italian Referendum\";\n" +
                "$platform isa oil-platform has distance-from-coast <= 18;\n" +
                "(location: $country, located: $platform) isa located-in;\n" +
                "$country isa country has name \"Italy\";\n" +
                "(owner: $company, owned: $platform) isa owns;\n" +
                "(issuer: $company, issued: $bond) isa issues;\n" +
                "select $bond, $article; offset 0; limit 5;");

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
        Json response = getHALRepresentationNoInference(academyGraph.graph(), "match $article isa article has subject \"Italian Referendum\";\n" +
                "$platform isa oil-platform has distance-from-coast <= 18;\n" +
                "(location: $country, located: $platform) isa located-in;\n" +
                "$country isa country has name \"Italy\";\n" +
                "(owner: $company, owned: $platform) isa owns;\n" +
                "(issuer: $company, issued: $bond) isa issues;\n" +
                "select $bond, $article; offset 0; limit 5;");

        // Limit to 5 results, each result will contain 2 variables, so the expected size of HAL results is 10.
        assertEquals(10, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            assertEquals(halObj.at("_baseType").asString(), "ENTITY");
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("bond") || entityType.equals("article"));
        });
    }

    @Test
    public void whenSelectInferredRelationWithSingleVar_EnsureValidExplanationHrefIsContainedInResponse(){
        Json response = getHALRepresentation(genealogyGraph.graph(), "match $x isa marriage; offset 0; limit 5;");
        assertEquals(5, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            assertEquals("inferred-relation", halObj.at("_baseType").asString());
            String href = halObj.at("_links").at("self").at("href").asString();
            assertTrue(href.contains("($a,$b) isa marriage;"));
        });
    }

    @Test
    public void whenTriggerReasonerWithTransitiveRule_EnsureWeReceiveAValidHALResponse() {
        Json response = getHALRepresentation(academyGraph.graph(), "match $x isa region; $y isa oil-platform; (located: $y, location: $x) isa located-in; limit 20;");
        // Limit to 20 results, each result will contain 3 variables, expected size 60
        assertEquals(60, response.asList().size());
        response.asJsonList().forEach(halObj -> {
            String entityType = halObj.at("_type").asString();
            assertTrue(entityType.equals("oil-platform") || entityType.equals("region") || entityType.equals("located-in"));
        });
    }

    private Json getHALRepresentation(GraknGraph graph, String queryString) {
        Query<?> query = graph.graql().materialise(false).infer(true).parse(queryString);
        return renderHALArrayData((MatchQuery) query, 0, 5);
    }

    private Json getHALExploreRepresentation(GraknGraph graph, String conceptId) {
        Concept concept = graph.getConcept(ConceptId.of(conceptId));
        return Json.read(HALExploreConcept(concept, graph.getKeyspace(), 0, 5));
    }

    private Json getHALRepresentationNoInference(GraknGraph graph, String queryString) {
        Query<?> query = graph.graql().materialise(false).infer(false).parse(queryString);
        return renderHALArrayData((MatchQuery) query, 0, 5);
    }

}
