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

package io.mindmaps.graql.query;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Resource;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

class QueryUtil {

    static final String[] movies = new String[] {
        "Godfather", "The-Muppets", "Apocalypse-Now", "Heat", "Hocus-Pocus", "Spy", "Chinese-Coffee"
    };

    public static void assertResultsMatchId(
            Iterable<Map<String, Concept>> query, String var, String type, String... expectedIds
    ) {
        Set<String> expectedSet = Sets.newHashSet(expectedIds);
        Set<String> unfoundSet = Sets.newHashSet(expectedIds);

        query.forEach(results -> {
            Concept result = results.get(var);
            assertNotNull(result);
            String id = result.getId();

            assertTrue("Unexpected id: " + id, expectedSet.contains(id));
            unfoundSet.remove(id);
            if (type != null) assertEquals(type, result.type().getId());
        });

        assertTrue("expected ids not found: " + unfoundSet, unfoundSet.isEmpty());
    }

    public static void assertResultsMatchName(
            Iterable<Map<String, Concept>> query, String var, String type, String... expectedNames
    ) {
        Set<String> expectedSet = Sets.newHashSet(expectedNames);
        Set<String> unfoundSet = Sets.newHashSet(expectedNames);

        query.forEach(results -> {
            Concept result = results.get(var);
            assertNotNull(result);

            Collection<Resource<?>> resources = result.asEntity().resources();

            String name = (String) resources.stream()
                    .filter(r -> r.type().getId().equals("name"))
                    .findAny().get().getValue();

            assertTrue("Unexpected name: " + name, expectedSet.contains(name));
            unfoundSet.remove(name);
            if (type != null) assertEquals(type, result.type().getId());
        });

        assertTrue("expected ids not found: " + unfoundSet, unfoundSet.isEmpty());
    }
}
