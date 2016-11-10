package io.mindmaps.test.graql.printer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.Printer;
import io.mindmaps.graql.internal.printer.Printers;
import io.mindmaps.test.AbstractMovieGraphTest;
import mjson.Json;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class JsonPrinterTest extends AbstractMovieGraphTest {

    private Printer printer;

    @Before
    public void setUp() {
        printer = Printers.json();
    }

    @Test
    public void testJsonEmptyMap() {
        assertJsonEquals(Json.object(), new HashMap<>());
    }

    @Test
    public void testJsonMapStringString() {
        assertJsonEquals(Json.object("key", "value"), ImmutableMap.of("key", "value"));
    }

    @Test
    public void testJsonMapIntInt() {
        assertJsonEquals(Json.object("1", 2), ImmutableMap.of(1, 2));
    }

    @Test
    public void testJsonEmptyList() {
        assertJsonEquals(Json.array(), Lists.newArrayList());
    }

    @Test
    public void testJsonListOptionalEmpty() {
        assertJsonEquals(Json.array(Json.nil()), ImmutableList.of(Optional.empty()));
    }

    @Test
    public void testJsonListOptionalPresent() {
        assertJsonEquals(Json.array(true), ImmutableList.of(Optional.of(true)));
    }

    @Test
    public void testJsonMetaType() {
        assertJsonEquals(Json.object("id", "entity-type"), graph.getMetaEntityType());
    }

    @Test
    public void testJsonEntityType() {
        assertJsonEquals(Json.object("id", "movie", "isa", "entity-type"), graph.getEntityType("movie"));
    }

    @Test
    public void testJsonResource() {
        ResourceType<String> resourceType = graph.getResourceType("title");
        Resource<String> resource = graph.getResource("The Muppets", resourceType);

        assertJsonEquals(
                Json.object("id", resource.getId(), "isa", "title", "value", "The Muppets"),
                resource
        );
    }

    @Test
    public void testJsonRule() {
        Rule rule = graph.getResource("expectation-rule", graph.getResourceType("name")).owner().asRule();
        assertJsonEquals(
                Json.object("id", rule.getId(), "isa", "a-rule-type", "lhs", rule.getLHS().toString(), "rhs", rule.getRHS().toString()),
                rule
        );
    }

    private void assertJsonEquals(Json expected, Object object) {
        Json json = Json.read(printer.graqlString(object));
        assertEquals(expected, json);
    }
}
