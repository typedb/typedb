/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.query;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.ResourceType;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class QueryUtil {

    static final String[] movies = new String[] {
        "Godfather", "The Muppets", "Apocalypse Now", "Heat", "Hocus Pocus", "Spy", "Chinese Coffee"
    };

    public static void assertResultsMatch(
            Iterable<Map<String, Concept>> query, String var, String type, ResourceType resourceType, String... expectedResources
    ) {
        Set<String> expectedSet = Sets.newHashSet(expectedResources);
        Set<String> unfoundSet = Sets.newHashSet(expectedResources);

        query.forEach(results -> {
            Concept result = results.get(var);
            assertNotNull(result);

            String resourceValue = result.getId();
            if(result.isEntity()){
                resourceValue = result.asEntity().resources(resourceType).iterator().next().asResource().getValue().toString();
            }

            assertTrue("Unexpected value: " + resourceValue, expectedSet.contains(resourceValue));
            unfoundSet.remove(resourceValue);
            if (type != null) assertEquals(type, result.type().getId());
        });

        assertTrue("expected values not found: " + unfoundSet, unfoundSet.isEmpty());
    }
}
