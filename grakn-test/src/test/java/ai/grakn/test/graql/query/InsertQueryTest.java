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

package ai.grakn.test.graql.query;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.test.GraphContext;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static ai.grakn.util.ErrorMessage.INSERT_UNSUPPORTED_PROPERTY;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class InsertQueryTest {

    private QueryBuilder qb;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraphContext movieGraph = GraphContext.preLoad(MovieGraph.get());

    @Before
    public void setUp() {
        movieGraph.graph().showImplicitConcepts(true);
        qb = movieGraph.graph().graql();
    }

    @After
    public void clear(){
        movieGraph.rollback();
    }

    @After
    public void tearDown() {
        if (movieGraph.graph() != null) movieGraph.graph().showImplicitConcepts(false);
    }

    @Test
    public void testInsertId() {
        assertInsert(var("x").has("name", "abc").isa("genre"));
    }

    @Test
    public void testInsertValue() {
        assertInsert(var("x").value(12109038210380L).isa("release-date"));
    }

    @Test
    public void testInsertIsa() {
        assertInsert(var("x").has("title", "Titanic").isa("movie"));
    }

    @Test
    public void testInsertSub() {
        assertInsert(var("x").name("cool-movie").sub("movie"));
    }

    @Test
    public void testInsertMultiple() {
        assertInsert(
                var("x").has("name", "123").isa("person"),
                var("y").value(123L).isa("runtime"),
                var("z").isa("language")
        );
    }

    @Test
    public void testInsertResource() {
        assertInsert(var("x").isa("movie").has("title", "Gladiator").has("runtime", 100L));
    }

    @Test
    public void testInsertName() {
        assertInsert(var("x").isa("movie").has("title", "Hello"));
    }

    @Test
    public void testInsertRelation() {
        Var rel = var("r").isa("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y");
        Var x = var("x").has("title", "Godfather").isa("movie");
        Var y = var("y").has("name", "comedy").isa("genre");
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
        qb.insert(var("x").has("title", "SW"), var("x").has("title", "Star Wars").isa("movie")).execute();

        assertTrue(qb.match(var().isa("movie").has("title", "SW")).ask().execute());
        assertTrue(qb.match(var().isa("movie").has("title", "Star Wars")).ask().execute());
        assertTrue(qb.match(var().isa("movie").has("title", "SW").has("title", "Star Wars")).ask().execute());
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
        Var language1 = var().isa("language").has("name", "123");
        Var language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertTrue(qb.match(language1).ask().execute());
        assertTrue(qb.match(language2).ask().execute());

        qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO")).execute();
        assertTrue(qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask().execute());

        qb.match(var("x").isa("language")).delete("x").execute();
        assertFalse(qb.match(language1).ask().execute());
        assertFalse(qb.match(language2).ask().execute());
    }

    @Test
    public void testInsertOntology() {
        qb.insert(
                name("pokemon").sub(Schema.MetaSchema.ENTITY.getName().getValue()),
                name("evolution").sub(Schema.MetaSchema.RELATION.getName().getValue()),
                name("evolves-from").sub(Schema.MetaSchema.ROLE.getName().getValue()),
                name("evolves-to").sub(Schema.MetaSchema.ROLE.getName().getValue()),
                name("evolution").hasRole("evolves-from").hasRole("evolves-to"),
                name("pokemon").playsRole("evolves-from").playsRole("evolves-to").hasResource("name"),

                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).execute();

        assertTrue(qb.match(name("pokemon").sub(Schema.MetaSchema.ENTITY.getName().getValue())).ask().execute());
        assertTrue(qb.match(name("evolution").sub(Schema.MetaSchema.RELATION.getName().getValue())).ask().execute());
        assertTrue(qb.match(name("evolves-from").sub(Schema.MetaSchema.ROLE.getName().getValue())).ask().execute());
        assertTrue(qb.match(name("evolves-to").sub(Schema.MetaSchema.ROLE.getName().getValue())).ask().execute());
        assertTrue(qb.match(name("evolution").hasRole("evolves-from").hasRole("evolves-to")).ask().execute());
        assertTrue(qb.match(name("pokemon").playsRole("evolves-from").playsRole("evolves-to")).ask().execute());

        assertTrue(qb.match(
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon")
        ).ask().execute());

        assertTrue(qb.match(
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution")
        ).ask().execute());

        assertTrue(qb.match(
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).ask().execute());
    }

    @Test
    public void testInsertIsAbstract() {
        qb.insert(
                name("concrete-type").sub(Schema.MetaSchema.ENTITY.getName().getValue()),
                name("abstract-type").isAbstract().sub(Schema.MetaSchema.ENTITY.getName().getValue())
        ).execute();

        assertFalse(qb.match(name("concrete-type").isAbstract()).ask().execute());
        assertTrue(qb.match(name("abstract-type").isAbstract()).ask().execute());
    }

    @Test
    public void testInsertDatatype() {
        qb.insert(
                name("my-type").sub(Schema.MetaSchema.RESOURCE.getName().getValue()).datatype(ResourceType.DataType.LONG)
        ).execute();

        MatchQuery query = qb.match(var("x").name("my-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        Assert.assertEquals(ResourceType.DataType.LONG, datatype);
    }

    @Test
    public void testInsertSubResourceType() {
        qb.insert(
                name("my-type").sub(Schema.MetaSchema.RESOURCE.getName().getValue()).datatype(ResourceType.DataType.STRING),
                name("sub-type").sub("my-type")
        ).execute();

        MatchQuery query = qb.match(var("x").name("sub-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        Assert.assertEquals(ResourceType.DataType.STRING, datatype);
    }

    @Test
    public void testInsertSubRoleType() {
        qb.insert(
                name("marriage").sub(Schema.MetaSchema.RELATION.getName().getValue()).hasRole("spouse1").hasRole("spouse2"),
                name("spouse").sub(Schema.MetaSchema.ROLE.getName().getValue()).isAbstract(),
                name("spouse1").sub("spouse"),
                name("spouse2").sub("spouse")
        ).execute();

        assertTrue(qb.match(name("spouse1")).ask().execute());
    }

    @Test
    public void testReferenceByVariableNameAndTypeName() {
        qb.insert(
                var("abc").sub("entity"),
                var("abc").name("123"),
                name("123").playsRole("actor"),
                var("abc").playsRole("director")
        ).execute();

        assertTrue(qb.match(name("123").sub("entity")).ask().execute());
        assertTrue(qb.match(name("123").playsRole("actor")).ask().execute());
        assertTrue(qb.match(name("123").playsRole("director")).ask().execute());
    }

    @Test
    public void testIterateInsertResults() {
        InsertQuery insert = qb.insert(
                var("x").has("name", "123").isa("person"),
                var("z").has("name", "xyz").isa("language")
        );

        Set<Map<String, Concept>> results = insert.stream().collect(Collectors.toSet());
        assertEquals(1, results.size());
        Map<String, Concept> result = results.iterator().next();
        assertEquals(ImmutableSet.of("x", "z"), result.keySet());
        assertThat(result.values(), Matchers.everyItem(notNullValue(Concept.class)));
    }

    @Test
    public void testIterateMatchInsertResults() {
        Var language1 = var().isa("language").has("name", "123");
        Var language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertTrue(qb.match(language1).ask().execute());
        assertTrue(qb.match(language2).ask().execute());

        InsertQuery query = qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO"));
        Iterator<Map<String, Concept>> results = query.iterator();

        assertFalse(qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask().execute());
        assertFalse(qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask().execute());

        Map<String, Concept> result1 = results.next();
        assertEquals(ImmutableSet.of("x"), result1.keySet());

        AskQuery query123 = qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask();
        AskQuery query456 = qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask();

        //Check if one of the matches have had the insert executed correctly
        boolean oneExists;
        if(query123.execute()){
            oneExists = !query456.execute();
        } else {
            oneExists = query456.execute();
        }
        assertTrue("A match insert was not executed correctly for only one match", oneExists);

        //Check that both are inserted correctly
        Map<String, Concept> result2 = results.next();
        assertEquals(ImmutableSet.of("x"), result1.keySet());
        assertTrue(qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask().execute());
        assertFalse(results.hasNext());

        assertNotEquals(result1.get("x"), result2.get("x"));
    }

    @Test
    public void testErrorWhenInsertWithPredicate() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("predicate");
        qb.insert(var().id(ConceptId.of("123")).value(gt(3))).execute();
    }

    @Test
    public void testErrorWhenInsertWithMultipleIds() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("123"), containsString("456")));
        qb.insert(var().id(ConceptId.of("123")).id(ConceptId.of("456")).isa("movie")).execute();
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
                var("x").id(ConceptId.of("Godfather")).isa("movie"),
                var("y").id(ConceptId.of("comedy")).isa("genre")
        ).execute();
    }

    @Test
    public void testInsertReferenceByName() {
        qb.insert(
                name("new-type").sub(Schema.MetaSchema.ENTITY.getName().getValue()),
                name("new-type").isAbstract(),
                name("new-type").playsRole("has-title-owner"),
                var("x").isa("new-type")
        ).execute();

        MatchQuery typeQuery = qb.match(var("n").name("new-type"));

        assertEquals(1, typeQuery.stream().count());

        // We checked count ahead of time
        //noinspection OptionalGetWithoutIsPresent
        EntityType newType = typeQuery.get("n").findFirst().get().asEntityType();

        assertTrue(newType.asEntityType().isAbstract());
        assertTrue(newType.playsRoles().contains(movieGraph.graph().getRoleType("has-title-owner")));

        assertTrue(qb.match(var().isa("new-type")).ask().execute());
    }

    @Test
    public void testInsertRuleType() {
        assertInsert(var("x").name("my-inference-rule").sub(RULE.getName().getValue()));
    }

    @Test
    public void testInsertRule() {
        String ruleTypeId = "a-rule-type";
        Pattern lhsPattern = qb.parsePattern("$x sub entity");
        Pattern rhsPattern = qb.parsePattern("$x sub entity");
        Var vars = var("x").isa(ruleTypeId).lhs(lhsPattern).rhs(rhsPattern);
        qb.insert(vars).execute();

        RuleType ruleType = movieGraph.graph().getRuleType(ruleTypeId);
        boolean found = false;
        for (ai.grakn.concept.Rule rule : ruleType.instances()) {
            if(lhsPattern.equals(rule.getLHS()) && rhsPattern.equals(rule.getRHS())){
                found = true;
                break;
            }
        }
        assertTrue("Unable to find rule with lhs [" + lhsPattern + "] and rhs [" + rhsPattern + "]", found);
    }

    @Test
    public void testInsertRuleSub() {
        assertInsert(var("x").name("an-sub-rule-type").sub("a-rule-type"));
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").has("title", "WOW A TITLE").isa("movie").isa("movie"));
    }

    @Test
    public void testInsertResourceTypeAndInstance() {
        qb.insert(
                name("movie").hasResource("my-resource"),
                name("my-resource").sub("resource").datatype(ResourceType.DataType.STRING),
                var("x").isa("movie").has("my-resource", "look a string")
        ).execute();
    }

    @Test
    public void testHasResource() {
        qb.insert(
                name("a-new-type").sub("entity").hasResource("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                name("an-unconnected-resource-type").sub("resource").datatype(ResourceType.DataType.LONG)
        ).execute();

        movieGraph.graph().showImplicitConcepts(true);

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertTrue(qb.match(name("a-new-type").sub("entity").hasResource("a-new-resource-type")).ask().execute());
        assertFalse(qb.match(name("a-new-type").hasResource("title")).ask().execute());
        assertFalse(qb.match(name("movie").hasResource("a-new-resource-type")).ask().execute());
        assertFalse(qb.match(name("a-new-type").hasResource("an-unconnected-resource-type")).ask().execute());

        // Make sure the expected ontology elements are created
        assertTrue(qb.match(name("has-a-new-resource-type").sub("relation")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type-owner").sub("role")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type-value").sub("role")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type").hasRole("has-a-new-resource-type-owner")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type").hasRole("has-a-new-resource-type-value")).ask().execute());
        assertTrue(qb.match(name("a-new-type").playsRole("has-a-new-resource-type-owner")).ask().execute());
        assertTrue(qb.match(name("a-new-resource-type").playsRole("has-a-new-resource-type-value")).ask().execute());
    }

    @Test
    public void testKey() {
        qb.insert(
                name("a-new-type").sub("entity").hasKey("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING)
        ).execute();

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertTrue(qb.match(name("a-new-type").sub("entity").hasResource("a-new-resource-type")).ask().execute());
        assertTrue(qb.match(name("a-new-type").sub("entity").hasKey("a-new-resource-type")).ask().execute());
        assertFalse(qb.match(name("a-new-type").sub("entity").hasKey("title")).ask().execute());
        assertFalse(qb.match(name("movie").sub("entity").hasKey("a-new-resource-type")).ask().execute());

        // Make sure the expected ontology elements are created
        assertTrue(qb.match(name("has-a-new-resource-type").sub("relation")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type-owner").sub("role")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type-value").sub("role")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type").hasRole("has-a-new-resource-type-owner")).ask().execute());
        assertTrue(qb.match(name("has-a-new-resource-type").hasRole("has-a-new-resource-type-value")).ask().execute());
        assertTrue(qb.match(name("a-new-type").playsRole("has-a-new-resource-type-owner")).ask().execute());
        assertTrue(qb.match(name("a-new-resource-type").playsRole("has-a-new-resource-type-value")).ask().execute());
    }

    @Test
    public void testKeyCorrectUsage() throws GraknValidationException {
        // This should only run on tinker because it commits
        assumeTrue(usingTinker());

        qb.insert(
                name("a-new-type").sub("entity").hasKey("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();
    }

    @Test
    public void testKeyUniqueOwner() throws GraknValidationException {
        // This should only run on tinker because it commits
        assumeTrue(usingTinker());

        qb.insert(
                name("a-new-type").sub("entity").hasKey("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello").has("a-new-resource-type", "goodbye")
        ).execute();

        exception.expect(GraknValidationException.class);
        movieGraph.graph().commitOnClose();
        movieGraph.graph().close();
    }

    @Test
    public void testKeyUniqueValue() throws GraknValidationException {
        qb.insert(
                name("a-new-type").sub("entity").hasKey("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello"),
                var().isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();

        exception.expect(GraknValidationException.class);
        movieGraph.graph().commitOnClose();
        movieGraph.graph().close();
    }

    @Test
    public void testKeyRequiredOwner() throws GraknValidationException {
        qb.insert(
                name("a-new-type").sub("entity").hasKey("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type")
        ).execute();

        exception.expect(GraknValidationException.class);
        movieGraph.graph().commitOnClose();
        movieGraph.graph().close();
    }

    @Test
    public void testKeyRequiredValue() throws GraknValidationException {
        qb.insert(
                name("a-new-type").sub("entity").hasKey("a-new-resource-type"),
                name("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-resource-type").value("hello")
        ).execute();

        exception.expect(GraknValidationException.class);
        movieGraph.graph().commitOnClose();
        movieGraph.graph().close();
    }

    @Test
    public void testResourceTypeRegex() {
        qb.insert(name("greeting").sub("resource").datatype(ResourceType.DataType.STRING).regex("hello|good day")).execute();

        MatchQuery match = qb.match(var("x").name("greeting"));
        assertEquals("hello|good day", match.get("x").findFirst().get().asResourceType().getRegex());
    }

    @Test
    public void testInsertExecuteResult() {
        InsertQuery query = qb.insert(var("x").isa("movie"));

        List<Map<String, Concept>> results = query.execute();
        assertEquals(1, results.size());
        Map<String, Concept> result = results.get(0);
        assertEquals(Sets.newHashSet("x"), result.keySet());
        Entity x = result.get("x").asEntity();
        assertEquals("movie", x.type().getName().getValue());
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
        qb.insert(name("my-resource").sub(Schema.MetaSchema.RESOURCE.getName().getValue())).execute();
    }

    @Test
    public void testErrorWhenAddingInstanceOfConcept() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(
                allOf(containsString("meta-type"), containsString("my-thing"), containsString(Schema.MetaSchema.CONCEPT.getName().getValue()))
        );
        qb.insert(var("my-thing").isa(Schema.MetaSchema.CONCEPT.getName().getValue())).execute();
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        qb.insert(name("thingy").sub("thingy")).execute();
    }

    @Test
    public void testErrorTypeWithoutId() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("type"), containsString("name")));
        qb.insert(var().sub("entity")).execute();
    }

    @Test
    public void testErrorInsertResourceWithoutValue() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("resource"), containsString("value")));
        qb.insert(var("x").isa("name")).execute();
    }

    @Test
    public void testErrorInsertInstanceWithName() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("instance"), containsString("name"), containsString("abc")));
        qb.insert(name("abc").isa("movie")).execute();
    }

    @Test
    public void testErrorInsertResourceWithName() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("instance"), containsString("name"), containsString("bobby")));
        qb.insert(name("bobby").value("bob").isa("name")).execute();
    }

    @Test
    public void testInsertDuplicatePattern() {
        qb.insert(var().isa("person").has("name", "a name"), var().isa("person").has("name", "a name")).execute();
        assertEquals(2, qb.match(var().has("name", "a name")).stream().count());
    }

    @Test
    public void testInsertResourceOnExistingId() {
        ConceptId apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now")).get("x").findAny().get().getId();

        assertFalse(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertTrue(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
    }

    @Test
    public void testInsertResourceOnExistingIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now")).get("x").findAny().get().getId();

        assertFalse(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
        qb.insert(var().id(apocalypseNow).isa("movie").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertTrue(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
    }

    @Test
    public void testInsertResourceOnExistingResourceId() {
        ConceptId apocalypseNow = qb.match(var("x").value("Apocalypse Now")).get("x").findAny().get().getId();

        assertFalse(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertTrue(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
    }

    @Test
    public void testInsertResourceOnExistingResourceIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").value("Apocalypse Now")).get("x").findAny().get().getId();

        assertFalse(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
        qb.insert(var().id(apocalypseNow).isa("title").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertTrue(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
    }

    @Test
    public void testInsertInstanceWithoutType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("123"), containsString("isa")));
        qb.insert(var().id(ConceptId.of("123")).has("name", "Bob")).execute();
    }

    @Test
    public void testInsertRuleWithoutLhs() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("lhs")));
        qb.insert(var().isa("inference-rule").rhs(var("x").isa("movie"))).execute();
    }

    @Test
    public void testInsertRuleWithoutRhs() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("rhs")));
        qb.insert(var().isa("inference-rule").lhs(var("x").isa("movie"))).execute();
    }

    @Test
    public void testInsertNonRuleWithLhs() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(INSERT_UNSUPPORTED_PROPERTY.getMessage("lhs", RULE.getName()));
        qb.insert(var().isa("movie").lhs(var("x"))).execute();
    }

    @Test
    public void testInsertNonRuleWithRHS() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(INSERT_UNSUPPORTED_PROPERTY.getMessage("rhs", RULE.getName()));
        qb.insert(name("thing").sub("movie").rhs(var("x"))).execute();
    }

    @Test
    public void testErrorWhenNonExistentResource() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("nothing");
        qb.insert(name("blah this").sub("entity").hasResource("nothing")).execute();
    }

    @Test
    public void whenInsertingMetaType_Throw() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(ErrorMessage.INSERT_METATYPE.getMessage("my-metatype", "concept"));
        qb.insert(name("my-metatype").sub("concept")).execute();
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
            qb.match(var).delete(var(var.admin().getVarName())).execute();
        }

        // Make sure vars don't exist
        for (Var var : vars) {
            assertFalse(qb.match(var).ask().execute());
        }
    }
}
