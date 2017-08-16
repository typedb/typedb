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

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Rule;
import ai.grakn.graql.Printer;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class JsonPrinterTest {

    private Printer printer;

    @ClassRule
    public static final GraphContext movieContext = GraphContext.preLoad(MovieGraph.get());

    @Before
    public void setUp() {
        printer = Printers.json();
    }

    @Test
    public void testJsonNull() {
        assertJsonEquals(Json.nil(), null);
    }

    @Test
    public void testJsonEmptyMap() {
        assertJsonEquals(Json.object(), new HashMap<>());
    }

    @Test
    public void testJsonMapNullKey() {
        Map<String, String> map = new HashMap<>();
        map.put(null, "hello");
        assertJsonEquals(Json.object("", "hello"), map);
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
        ConceptId id = movieContext.graph().admin().getMetaEntityType().getId();
        assertJsonEquals(Json.object("id", id.getValue(), "name", "entity", "sub", Schema.MetaSchema.THING.getLabel().getValue()), movieContext.graph().admin().getMetaEntityType());
    }

    @Test
    public void testJsonEntityType() {
        ConceptId id = movieContext.graph().getEntityType("movie").getId();
        assertJsonEquals(Json.object("id", id.getValue(), "name", "movie", "sub", "production"), movieContext.graph().getEntityType("movie"));
    }

    @Test
    public void testJsonResource() {
        AttributeType<String> attributeType = movieContext.graph().getResourceType("title");
        Attribute<String> attribute = attributeType.getResource("The Muppets");

        assertJsonEquals(
                Json.object("id", attribute.getId().getValue(), "isa", "title", "value", "The Muppets"),
                attribute
        );
    }

    @Test
    public void testJsonRule() {
        Rule jsonRule = movieContext.graph().getResourceType("name").getResource("expectation-rule").owner().asRule();
        assertJsonEquals(
                Json.object("id", jsonRule.getId().getValue(), "isa", "a-rule-type", "when", jsonRule.getWhen().toString(), "then", jsonRule.getThen().toString()),
                jsonRule
        );
    }

    @Test
    public void whenPrintingRole_PrintWithLabel() {
        ConceptId id = movieContext.graph().getRole("actor").getId();
        assertJsonEquals(Json.object("id", id.getValue(), "name", "actor", "sub", "role"), movieContext.graph().getRole("actor"));
    }

    private void assertJsonEquals(Json expected, Object object) {
        Json json = Json.read(printer.graqlString(object));
        assertEquals(expected, json);
    }
}
