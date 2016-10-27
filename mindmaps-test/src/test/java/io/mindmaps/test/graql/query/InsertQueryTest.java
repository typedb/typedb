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
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Var;
import io.mindmaps.test.AbstractMovieGraphTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.concept.ResourceType.DataType.LONG;
import static io.mindmaps.concept.ResourceType.DataType.STRING;
import static io.mindmaps.graql.Graql.gt;
import static io.mindmaps.graql.Graql.id;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static io.mindmaps.util.Schema.MetaType.ENTITY_TYPE;
import static io.mindmaps.util.Schema.MetaType.RELATION_TYPE;
import static io.mindmaps.util.Schema.MetaType.RESOURCE_TYPE;
import static io.mindmaps.util.Schema.MetaType.ROLE_TYPE;
import static io.mindmaps.util.Schema.MetaType.RULE_TYPE;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class InsertQueryTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        // TODO: Fix delete queries in titan
        assumeFalse(usingTitan());

        graph = factoryWithNewKeyspace().getGraph();
        MovieGraphFactory.loadGraph(graph);

        qb = withGraph(graph);
    }

    @Test
    public void testInsertId() {
        assertInsert(var("x").id("abc").isa("genre"));
    }

    @Test
    public void testInsertValue() {
        assertInsert(var("x").value(12109038210380L).isa("release-date"));
    }

    @Test
    public void testInsertIsa() {
        assertInsert(var("x").id("Titanic").isa("movie"));
    }

    @Test
    public void testInsertSub() {
        assertInsert(var("x").id("http://mindmaps.io/cool-movie").sub("movie"));
    }

    @Test
    public void testInsertMultiple() {
        assertInsert(
                var("x").id("123").isa("person"),
                var("y").value(123L).isa("runtime"),
                var("z").isa("language")
        );
    }

    @Test
    public void testInsertResourceOrName() {
        assertInsert(var("x").isa("movie").id("123").has("runtime", 100L));
    }

    @Test
    public void testInsertResource() {
        assertInsert(var("x").isa("movie").id("123").has("runtime", 100L));
    }

    @Test
    public void testInsertName() {
        assertInsert(var("x").isa("movie").id("123").has("title", "Hello"));
    }

    @Test
    public void testInsertRelation() {
        Var rel = var("r").isa("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y");
        Var x = var("x").id("Godfather").isa("movie");
        Var y = var("y").id("comedy").isa("genre");
        Var[] vars = new Var[] {rel, x, y};
        Pattern[] patterns = new Pattern[] {rel, x, y};

        assertFalse(qb.match(patterns).ask().execute());

        qb.insert(vars).execute();
        assertTrue(qb.match(patterns).ask().execute());

        qb.match(patterns).delete("r").execute();
        assertFalse(qb.match(patterns).ask().execute());
    }

    @Test
    public void testInsertSameVarName() {
        qb.insert(var("x").id("123"), var("x").has("title", "Star Wars").isa("movie")).execute();

        assertTrue(qb.match(var().isa("movie").id("123")).ask().execute());
        assertTrue(qb.match(var().has("title", "Star Wars")).ask().execute());
        assertTrue(qb.match(var().isa("movie").id("123").has("title", "Star Wars")).ask().execute());
    }

    @Test
    public void testInsertRepeat() {
        Var language = var("x").has("name", "123").isa("language");
        InsertQuery query = qb.insert(language);

        assertEquals(0, qb.match(language).stream().count());
        query.execute();
        assertEquals(1, qb.match(language).stream().count());
        query.execute();
        assertEquals(2, qb.match(language).stream().count());
        query.execute();
        assertEquals(3, qb.match(language).stream().count());

        qb.match(language).delete("x").execute();
        assertEquals(0, qb.match(language).stream().count());
    }

    @Test
    public void testMatchInsertQuery() {
        Var language1 = var().isa("language").id("123");
        Var language2 = var().isa("language").id("456");

        qb.insert(language1, language2).execute();
        assertTrue(qb.match(language1).ask().execute());
        assertTrue(qb.match(language2).ask().execute());

        qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO")).execute();
        assertTrue(qb.match(var().isa("language").id("123").has("name", "HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").id("456").has("name", "HELLO")).ask().execute());

        qb.match(var("x").isa("language")).delete("x").execute();
        assertFalse(qb.match(language1).ask().execute());
        assertFalse(qb.match(language2).ask().execute());
    }

    @Test
    public void testInsertOntology() {
        qb.insert(
                id("pokemon").isa(ENTITY_TYPE.getId()),
                id("evolution").isa(RELATION_TYPE.getId()),
                id("evolves-from").isa(ROLE_TYPE.getId()),
                id("evolves-to").isa(ROLE_TYPE.getId()),
                id("evolution").hasRole("evolves-from").hasRole("evolves-to"),
                id("pokemon").playsRole("evolves-from").playsRole("evolves-to"),

                var("x").id("Pichu").isa("pokemon"),
                var("y").id("Pikachu").isa("pokemon"),
                var("z").id("Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).execute();

        assertTrue(qb.match(id("pokemon").isa(ENTITY_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolution").isa(RELATION_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolves-from").isa(ROLE_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolves-to").isa(ROLE_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolution").hasRole("evolves-from").hasRole("evolves-to")).ask().execute());
        assertTrue(qb.match(id("pokemon").playsRole("evolves-from").playsRole("evolves-to")).ask().execute());

        assertTrue(qb.match(
                var("x").id("Pichu").isa("pokemon"),
                var("y").id("Pikachu").isa("pokemon"),
                var("z").id("Raichu").isa("pokemon")
        ).ask().execute());

        assertTrue(qb.match(
                var("x").id("Pichu").isa("pokemon"),
                var("y").id("Pikachu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution")
        ).ask().execute());

        assertTrue(qb.match(
                var("y").id("Pikachu").isa("pokemon"),
                var("z").id("Raichu").isa("pokemon"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).ask().execute());
    }

    @Test
    public void testInsertIsAbstract() {
        qb.insert(
                id("concrete-type").isa(ENTITY_TYPE.getId()),
                id("abstract-type").isAbstract().isa(ENTITY_TYPE.getId())
        ).execute();

        assertFalse(qb.match(id("concrete-type").isAbstract()).ask().execute());
        assertTrue(qb.match(id("abstract-type").isAbstract()).ask().execute());
    }

    @Test
    public void testInsertDatatype() {
        qb.insert(
                id("my-type").isa(RESOURCE_TYPE.getId()).datatype(LONG)
        ).execute();

        MatchQuery query = qb.match(var("x").id("my-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(LONG, datatype);
    }

    @Test
    public void testInsertSubResourceType() {
        qb.insert(
                id("my-type").isa(RESOURCE_TYPE.getId()).datatype(STRING),
                id("sub-type").sub("my-type")
        ).execute();

        MatchQuery query = qb.match(var("x").id("sub-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(STRING, datatype);
    }

    @Test
    public void testInsertSubRoleType() {
        qb.insert(
                id("marriage").isa(RELATION_TYPE.getId()).hasRole("spouse1").hasRole("spouse2"),
                id("spouse").isa(ROLE_TYPE.getId()).isAbstract(),
                id("spouse1").sub("spouse"),
                id("spouse2").sub("spouse")
        ).execute();

        assertTrue(qb.match(id("spouse1")).ask().execute());
    }

    @Test
    public void testReferenceByIdAndVariableName() {
        qb.insert(
                id("123").isa("person"),
                id("456").isa("movie"),
                var("abc").id("123"),
                var("def").id("456"),
                var().rel("director", "abc").rel("production-being-directed", "def").isa("directed-by")
        ).execute();

        assertTrue(qb.match(id("123").isa("person")).ask().execute());
        assertTrue(qb.match(id("456").isa("movie")).ask().execute());
        assertTrue(qb.match(var().rel("director", id("123")).rel(id("456"))).ask().execute());
    }

    @Test
    public void testReferenceByVariableNameAndId() {
        qb.insert(
                var("abc").isa("person"),
                var("def").isa("movie"),
                var("abc").id("123"),
                var("def").id("456"),
                var().rel("director", id("123")).rel("production-being-directed", id("456")).isa("directed-by")
        ).execute();

        assertTrue(qb.match(id("123").isa("person")).ask().execute());
        assertTrue(qb.match(id("456").isa("movie")).ask().execute());
        assertTrue(qb.match(var().rel("director", id("123")).rel(id("456"))).ask().execute());
    }

    @Test
    public void testIterateInsertResults() {
        InsertQuery insert = qb.insert(
                var("x").id("123").isa("person"),
                var("z").id("xyz").isa("language")
        );

        Set<String> addedIds = insert.stream().map(Concept::getId).collect(Collectors.toSet());
        Set<String> expectedIds = Sets.newHashSet("123", "person", "xyz", "language");

        assertEquals(expectedIds, addedIds);
    }

    @Test
    public void testErrorWhenInsertWithPredicate() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(containsString("predicate"));
        qb.insert(var().id("123").value(gt(3))).execute();
    }

    @Test
    public void testErrorWhenInsertWithMultipleIds() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("123"), containsString("456")));
        qb.insert(var().id("123").id("456").isa("movie")).execute();
    }

    @Test
    public void testErrorWhenInsertWithMultipleValues() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("value"), containsString("123"), containsString("456")));
        qb.insert(var().value("123").value("456").isa("title")).execute();
    }

    @Test
    public void testErrorWhenSubRelation() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("isa"), containsString("relation")));
        qb.insert(
                var().sub("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y"),
                var("x").id("Godfather").isa("movie"),
                var("y").id("comedy").isa("genre")
        ).execute();
    }

    @Test
    public void testInsertReferenceById() {
        qb.insert(
                var().id("new-type").isa(ENTITY_TYPE.getId()),
                id("new-type").isAbstract(),
                var().id("new-type").playsRole("has-title-owner"),
                var().id("new-thing").isa("new-type")
        ).execute();

        MatchQuery typeQuery = qb.match(var("n").id("new-type"));

        assertEquals(1, typeQuery.stream().count());

        // We checked count ahead of time
        //noinspection OptionalGetWithoutIsPresent
        EntityType newType = typeQuery.get("n").findFirst().get().asEntityType();

        assertTrue(newType.asEntityType().isAbstract());
        assertTrue(newType.playsRoles().contains(graph.getRoleType("has-title-owner")));

        assertTrue(qb.match(var().isa("new-type")).ask().execute());
    }

    @Test
    public void testInsertRuleType() {
        assertInsert(var("x").id("my-inference-rule").isa(RULE_TYPE.getId()));
    }

    @Test
    public void testInsertRule() {
        assertInsert(var("x").id("123").isa("a-rule-type").lhs("lhs").rhs("rhs"));
    }

    @Test
    public void testInsertRuleSub() {
        assertInsert(var("x").id("an-sub-rule-type").sub("a-rule-type"));
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").id("123").isa("movie").isa("movie"));
    }

    @Test
    public void testInsertResourceTypeAndInstance() {
        qb.insert(
                var("x").isa("movie").has("my-resource", "look a string"),
                id("my-resource").isa("resource-type").datatype(STRING),
                id("movie").hasResource("my-resource")
        ).execute();
    }

    @Test
    public void testHasResource() {
        qb.insert(
                id("a-new-type").isa("entity-type").hasResource("a-new-resource-type"),
                id("a-new-resource-type").isa("resource-type").datatype(STRING),
                id("an-unconnected-resource-type").isa("resource-type").datatype(LONG)
        ).execute();

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertTrue(qb.match(id("a-new-type").isa("entity-type").hasResource("a-new-resource-type")).ask().execute());
        assertFalse(qb.match(id("a-new-type").hasResource("title")).ask().execute());
        assertFalse(qb.match(id("movie").hasResource("a-new-resource-type")).ask().execute());
        assertFalse(qb.match(id("a-new-type").hasResource("an-unconnected-resource-type")).ask().execute());

        // Make sure the expected ontology elements are created
        assertTrue(qb.match(id("has-a-new-resource-type").isa("relation-type")).ask().execute());
        assertTrue(qb.match(id("has-a-new-resource-type-owner").isa("role-type")).ask().execute());
        assertTrue(qb.match(id("has-a-new-resource-type-value").isa("role-type")).ask().execute());
        assertTrue(qb.match(id("has-a-new-resource-type").hasRole("has-a-new-resource-type-owner")).ask().execute());
        assertTrue(qb.match(id("has-a-new-resource-type").hasRole("has-a-new-resource-type-value")).ask().execute());
        assertTrue(qb.match(id("a-new-type").playsRole("has-a-new-resource-type-owner")).ask().execute());
        assertTrue(qb.match(id("a-new-resource-type").playsRole("has-a-new-resource-type-value")).ask().execute());
    }

    @Test
    public void testResourceTypeRegex() {
        qb.insert(id("greeting").isa("resource-type").datatype(STRING).regex("hello|good day")).execute();

        MatchQuery match = qb.match(var("x").id("greeting"));
        assertEquals("hello|good day", match.get("x").findFirst().get().asResourceType().getRegex());
    }

    @Test
    public void testErrorWhenInsertRelationWithEmptyRolePlayer() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(
                allOf(containsString("$y"), containsString("id"), containsString("isa"), containsString("sub"))
        );
        qb.insert(
                var().rel("genre-of-production", "x").rel("production-with-genre", "y").isa("has-genre"),
                var("x").isa("genre").has("name", "drama")
        ).execute();
    }

    @Test
    public void testErrorResourceTypeWithoutDataType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        qb.insert(id("my-resource").isa(RESOURCE_TYPE.getId())).execute();
    }

    @Test
    public void testErrorWhenAddingMetaType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(
                allOf(containsString("meta-type"), containsString("my-thing"), containsString(RELATION_TYPE.getId()))
        );
        qb.insert(id("my-thing").sub(RELATION_TYPE.getId())).execute();
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        qb.insert(id("thingy").isa("thingy")).execute();
    }

    @Test
    public void testErrorTypeWithoutId() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("type"), containsString("id")));
        qb.insert(var().isa("entity-type")).execute();
    }

    @Test
    public void testErrorInsertResourceWithoutValue() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("resource"), containsString("value")));
        qb.insert(var("x").isa("name")).execute();
    }

    @Test
    public void testErrorInsertResourceWithId() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("resource"), containsString("id"), containsString("bobby")));
        qb.insert(id("bobby").value("bob").isa("name")).execute();
    }

    @Test
    public void testInsertDuplicatePattern() {
        qb.insert(var().isa("person").has("name", "a name"), var().isa("person").has("name", "a name")).execute();
        assertEquals(2, qb.match(var().has("name", "a name")).stream().count());
    }

    @Test
    public void testInsertInstanceWithoutType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("123"), containsString("isa")));
        qb.insert(id("123").has("name", "Bob")).execute();
    }

    private void assertInsert(Var... vars) {
        // Make sure vars don't exist
        for (Var var : vars) {
            assertFalse(qb.match(var).ask().execute());
        }

        // Insert all vars
        qb.insert(vars).execute();

        // Make sure all vars exist
        for (Var var : vars) {
            assertTrue(qb.match(var).ask().execute());
        }

        // Delete all vars
        for (Var var : vars) {
            qb.match(var).delete(var.admin().getName()).execute();
        }

        // Make sure vars don't exist
        for (Var var : vars) {
            assertFalse(qb.match(var).ask().execute());
        }
    }
}
