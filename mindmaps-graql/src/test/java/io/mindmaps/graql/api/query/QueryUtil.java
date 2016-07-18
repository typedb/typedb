package io.mindmaps.graql.api.query;

import com.google.common.collect.Sets;
import io.mindmaps.core.model.Concept;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class QueryUtil {

    static final String[] movies = new String[] {
        "Godfather", "The-Muppets", "Apocalypse-Now", "Heat", "Hocus-Pocus", "Spy", "Chinese-Coffee"
    };

    public static void assertResultsMatch(
            Iterable<Map<String, Concept>> query, String var, String type, String... expectedIds
    ) {
        Set<String> expectedSet = Sets.newHashSet(expectedIds);
        Set<String> unfoundSet = Sets.newHashSet(expectedIds);

        query.forEach(results -> {
            assertEquals(1, results.size());
            Concept result = results.get(var);
            assertNotNull(result);
            String id = result.getId();
            assertTrue("Unexpected id: " + id, expectedSet.contains(id));
            unfoundSet.remove(id);
            if (type != null) assertEquals(type, result.type().getId());
        });

        assertTrue("expected ids not found: " + unfoundSet, unfoundSet.isEmpty());
    }
}
