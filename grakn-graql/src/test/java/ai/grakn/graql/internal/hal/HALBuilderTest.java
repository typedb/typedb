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
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HALBuilderTest {

    @ClassRule
    public static SampleKBContext sampleKB = MovieKB.context();

    @Test
    public void renderHALConceptData_producesCorrectHALObject() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.renderHALConceptData(concept, false, 0, sampleKB.tx().getKeyspace(), 0, 10);
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
        String halString = HALBuilder.renderHALConceptData(concept, false, 0, sampleKB.tx().getKeyspace(), 0, 10);
        Json halObject = Json.read(halString);
        assertFalse(halObject.has("_embedded"));
    }

    @Test
    public void renderHALConceptDataWithSeparationDegree1_producedHALWithEmbedded() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.renderHALConceptData(concept, false, 1, sampleKB.tx().getKeyspace(), 0, 10);
        Json halObject = Json.read(halString);
        assertTrue(halObject.has("_embedded"));
    }

    @Test
    public void testHALExploreConceptWithThing_producesCorrectHALObject() {
        Concept concept = sampleKB.tx().getEntityType("movie").instances().iterator().next();
        String halString = HALBuilder.HALExploreConcept(concept, sampleKB.tx().getKeyspace(), 0, 10);

        //Check Explore Thing embeds thing's attributes
        Json halObject = Json.read(halString);
        halObject.at("_embedded").asJsonMap().values().forEach(attr -> {
            assertEquals("ATTRIBUTE", attr.asJsonList().get(0).at("_baseType").asString());
        });
    }

    @Test
    public void testHALExploreConceptWithSchemaConcept_producesCorrectHALObject() {
        Concept concept = sampleKB.tx().getEntityType("movie");
        String halString = HALBuilder.HALExploreConcept(concept, sampleKB.tx().getKeyspace(), 0, 10);

        //Check Explore Schema Concept embeds attribute types and roles played by concept
        Json halObject = Json.read(halString);
        Set<String> embeddedKeys = halObject.at("_embedded").asJsonMap().keySet();
        assertEquals(2, embeddedKeys.size());
        assertTrue(embeddedKeys.contains("has"));
        assertTrue(embeddedKeys.contains("plays"));
    }


}
