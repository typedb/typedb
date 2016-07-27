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

package io.mindmaps.graql.api.query;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.core.implementation.DataType.ConceptMeta.*;
import static io.mindmaps.graql.api.query.QueryBuilder.id;
import static io.mindmaps.graql.api.query.QueryBuilder.var;
import static io.mindmaps.graql.api.query.ValuePredicate.gt;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class InsertQueryTest {

    private MindmapsTransaction transaction;
    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
        qb = QueryBuilder.build(transaction);
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
        assertInsert(var("x").value("Titanic").isa("movie"));
    }

    @Test
    public void testInsertAko() {
        assertInsert(var("x").id("http://mindmaps.io/cool-movie").ako("movie"));
    }

    @Test
    public void testInsertMultiple() {
        assertInsert(
                var("x").id("123").isa("person"),
                var("y").value(123L).id("321").isa("runtime"),
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
        Var[] patterns = new Var[] {rel, x, y};

        assertFalse(qb.match(patterns).ask().execute());

        qb.insert(patterns).execute();
        assertTrue(qb.match(patterns).ask().execute());

        qb.match(patterns).delete("r").execute();
        assertFalse(qb.match(patterns).ask().execute());
    }

    @Test
    public void testInsertSameVarName() {
        qb.insert(var("x").id("123"), var("x").value("Star Wars").has("title", "Star Wars").isa("movie")).execute();

        assertTrue(qb.match(var().isa("movie").id("123")).ask().execute());
        assertTrue(qb.match(var().value("Star Wars").has("title", "Star Wars")).ask().execute());
        assertTrue(qb.match(var().isa("movie").id("123").value("Star Wars").has("title", "Star Wars")).ask().execute());

        qb.match(var("x").value("Star Wars")).delete("x");
        qb.match(var("x").id("123")).delete("x");
    }

    @Test
    public void testInsertRepeat() {
        Var language = var("x").value("123").isa("language");
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

        qb.match(var("x").isa("language")).insert(var("x").value("HELLO")).execute();
        assertTrue(qb.match(var().isa("language").id("123").value("HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").id("456").value("HELLO")).ask().execute());

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
                var("z").id("Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).ask().execute());
    }

    @Test
    public void testChangeValue() {
        qb.insert(var().id("123").value("abc").isa("character")).execute();

        assertTrue(qb.match(var().id("123").value("abc")).ask().execute());

        qb.insert(var().id("123").value("def").isa("language")).execute();

        assertFalse(qb.match(var().id("123").value("abc")).ask().execute());
        assertTrue(qb.match(var().id("123").value("def")).ask().execute());

        qb.match(var("x").id("123")).delete("x").execute();
        assertFalse(qb.match(var().id("123")).ask().execute());
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
                id("my-type").isa(RESOURCE_TYPE.getId()).datatype(Data.LONG)
        ).execute();

        MatchQuery query = qb.match(var("x").id("my-type"));
        Data datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(Data.LONG, datatype);
    }

    @Test
    public void testInsertAkoResourceType() {
        qb.insert(
                id("my-type").isa(RESOURCE_TYPE.getId()).datatype(Data.STRING),
                id("ako-type").ako("my-type")
        ).execute();

        MatchQuery query = qb.match(var("x").id("ako-type"));
        Data datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(Data.STRING, datatype);
    }

    @Test
    public void testInsertAkoRoleType() {
        qb.insert(
                id("marriage").isa(RELATION_TYPE.getId()).hasRole("spouse1").hasRole("spouse2"),
                id("spouse").isa(ROLE_TYPE.getId()).isAbstract(),
                id("spouse1").ako("spouse"),
                id("spouse2").ako("spouse")
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
                var("y").value(123L).id("321").isa("runtime"),
                var("z").id("xyz").isa("language")
        );

        Set<String> addedIds = insert.stream().map(Concept::getId).collect(Collectors.toSet());
        Set<String> expectedIds = Sets.newHashSet("123", "person", "321", "runtime", "xyz", "language");

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
        qb.insert(var().value("123").value("456").isa("person")).execute();
    }

    @Test
    public void testErrorWhenAkoRelation() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("isa"), containsString("relation")));
        qb.insert(
                var().ako("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y"),
                var("x").id("Godfather").isa("movie"),
                var("y").id("comedy").isa("genre")
        ).execute();
    }

    @Test
    public void testInsertReferenceById() {
        qb.insert(
                var().id("new-type").isa(ENTITY_TYPE.getId()),
                id("new-type").value("A value"),
                var().id("new-type").playsRole("has-title-owner"),
                var().id("new-thing").isa("new-type")
        ).execute();

        MatchQuery typeQuery = qb.match(var("n").id("new-type"));

        assertEquals(1, typeQuery.stream().count());

        EntityType newType = typeQuery.stream().findFirst().get().get("n").asEntityType();

        assertEquals("A value", newType.getValue());
        assertTrue(newType.playsRoles().contains(transaction.getRoleType("has-title-owner")));

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
    public void testInsertRuleAko() {
        assertInsert(var("x").id("an-ako-rule-type").ako("a-rule-type"));
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").id("123").isa("movie").isa("movie"));
    }

    @Test
    public void testInsertResourceTypeAndInstance() {
        qb.insert(
                var("x").isa("movie").has("my-resource", "look a string"),
                id("my-resource").isa("resource-type").datatype(Data.STRING),
                id("movie").hasResource("my-resource")
        ).execute();
    }

    @Test
    public void testHasResource() {
        qb.insert(
                id("a-new-type").isa("entity-type").hasResource("a-new-resource-type"),
                id("a-new-resource-type").isa("resource-type").datatype(Data.STRING),
                id("an-unconnected-resource-type").isa("resource-type").datatype(Data.LONG)
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
    public void testErrorWhenInsertRelationWithEmptyRolePlayer() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(
                allOf(containsString("exist"), containsString("$y"), containsString("isa"), containsString("ako"))
        );
        qb.insert(
                var().rel("genre-of-production", "x").rel("production-with-genre", "y").isa("has-genre"),
                var("x").isa("genre").value("drama")
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
        qb.insert(id("my-thing").ako(RELATION_TYPE.getId())).execute();
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        qb.insert(id("thingy").isa("thingy")).execute();
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
