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

import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.hal.HALBuilder.explanationAnswersToHAL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HALBuilderTest {

    @ClassRule
    public static SampleKBContext sampleKB = MovieKB.context();

    @ClassRule
    public static SampleKBContext genealogyKB = GenealogyKB.context();

    @Test
    public void renderHALConceptData_producesCorrectHALObject() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.renderHALConceptData(concept, false, 0, sampleKB.tx().keyspace(), 0, 10);
        Json halObject = Json.read(halString);
        assertTrue(halObject.has("_id"));
        assertTrue(halObject.has("_type"));
        assertTrue(halObject.has("_baseType"));
        assertEquals("movie", halObject.at("_type").asString());
        assertEquals("ENTITY", halObject.at("_baseType").asString());

    }

    @Test
    public void renderHALConceptDataWithSeparationDegree0_producedHALWithoutEmbedded() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.renderHALConceptData(concept, false, 0, sampleKB.tx().keyspace(), 0, 10);
        Json halObject = Json.read(halString);
        assertFalse(halObject.has("_embedded"));
    }

    @Test
    public void renderHALConceptDataWithSeparationDegree1_producedHALWithEmbedded() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.renderHALConceptData(concept, false, 1, sampleKB.tx().keyspace(), 0, 10);
        Json halObject = Json.read(halString);
        assertTrue(halObject.has("_embedded"));
    }

    @Test
    public void testHALExploreConceptWithThing_producesCorrectHALObject() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.HALExploreConcept(concept, sampleKB.tx().keyspace(), 0, 10);

        //Check Explore Thing embeds thing's attributes
        Json halObject = Json.read(halString);
        halObject.at("_embedded").asJsonMap().values().forEach(attr -> {
            assertEquals("ATTRIBUTE", attr.asJsonList().get(0).at("_baseType").asString());
        });
    }

    @Test
    public void testHALExploreConceptWithSchemaConcept_producesCorrectHALObject() {
        Concept concept = sampleKB.tx().getEntityType("movie");
        String halString = HALBuilder.HALExploreConcept(concept, sampleKB.tx().keyspace(), 0, 10);

        //Check Explore Schema Concept embeds attribute types and roles played by concept
        Json halObject = Json.read(halString);
        Set<String> embeddedKeys = halObject.at("_embedded").asJsonMap().keySet();
        assertEquals(2, embeddedKeys.size());
        assertTrue(embeddedKeys.contains("has"));
        assertTrue(embeddedKeys.contains("plays"));
    }


    @Test
    public void whenExplainInferred_returnsLinkedExplanation() {
        String mainQuery = "match ($x, $y) isa cousins; limit 15; get;";
        List<Answer> answers = genealogyKB.tx().graql().infer(true).parser().<GetQuery>parseQuery(mainQuery).execute();
        answers.forEach(answer -> {
            String cousin1 = answer.get("x").getId().getValue();
            String cousin2 = answer.get("y").getId().getValue();
            String specificQuery = "match " +
                    "$x id " + cousin1 + ";" +
                    "$y id " + cousin2 + ";" +
                    "(cousin: $x, cousin: $y) isa cousins; get;";
            Printer<?> printer = Printers.hal(genealogyKB.tx().keyspace(), 5);
            GetQuery query = genealogyKB.tx().graql().infer(true).parse(specificQuery);
            Answer specificAnswer = query.execute().stream().findFirst().orElse(new QueryAnswer());

            Json expl = explanationAnswersToHAL(specificAnswer.getExplanation().getAnswers(), printer);

            Json siblings = expl.asJsonList().stream()
                    .filter(x -> x.asJsonMap().values().stream()
                                .map(entry->entry.asJsonMap().get("_type").asString())
                                .anyMatch(sub->sub.equals("siblings")))
                    .findFirst().orElse(Json.array());
            // Two parents IDs
            Set<String> siblingsSet = siblings.asJsonMap().values()
                    .stream()
                    .filter(x->x.asJsonMap().get("_type").asString().equals("person"))
                    .map(x-> x.asJsonMap().get("_id").asString())
                    .collect(Collectors.toSet());
            Optional<Json> parentship1 = expl.asJsonList().stream()
                    .filter(sub->sub.asJsonMap().values().stream().anyMatch(value->value.asJsonMap().get("_id").asString().equals(cousin1)))
                    .findFirst();
            Optional<Json> parentship2 = expl.asJsonList().stream()
                    .filter(sub->sub.asJsonMap().values().stream().anyMatch(value->value.asJsonMap().get("_id").asString().equals(cousin2)))
                    .findFirst();
            assertTrue(parentship1.isPresent());
            assertTrue(parentship2.isPresent());
            Set<String> parentShipIds = parentship1.get().asJsonMap().values().stream().map(x->x.asJsonMap().get("_id").asString())
                        .collect(Collectors.toSet());
            Set<String> parentShip2Ids = parentship2.get().asJsonMap().values().stream().map(x->x.asJsonMap().get("_id").asString())
                    .collect(Collectors.toSet());
            // In parentshipIds all the 4 ids: 2 parents 2 children (that should be the cousins ids)
            parentShipIds.addAll(parentShip2Ids);
            // Check 2 ids in siblings are actually contained in parentship relationships
            siblingsSet.forEach(id->{
                assertTrue(parentShipIds.contains(id));
            });
            // Check 2 cousins ids are actually contained in parentship relationships
            assertTrue(parentShipIds.contains(cousin1));
            assertTrue(parentShipIds.contains(cousin2));

        });

    }
}
