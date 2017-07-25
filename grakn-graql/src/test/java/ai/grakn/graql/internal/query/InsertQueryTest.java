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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Relation;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraphLoader;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.INSERT_UNSUPPORTED_PROPERTY;
import static ai.grakn.util.ErrorMessage.NO_PATTERNS;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
import static ai.grakn.util.Schema.ImplicitType.KEY;
import static ai.grakn.util.Schema.ImplicitType.KEY_OWNER;
import static ai.grakn.util.Schema.ImplicitType.KEY_VALUE;
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
        qb = movieGraph.graph().graql();
    }

    @After
    public void clear(){
        movieGraph.rollback();
    }

    @Test
    public void testInsertId() {
        assertInsert(var("x").has("name", "abc").isa("genre"));
    }

    @Test
    public void testInsertValue() {
        assertInsert(var("x").val(LocalDateTime.of(1992, 10, 7, 13, 14, 15)).isa("release-date"));
    }

    @Test
    public void testInsertIsa() {
        assertInsert(var("x").has("title", "Titanic").isa("movie"));
    }

    @Test
    public void testInsertSub() {
        assertInsert(var("x").label("cool-movie").sub("movie"));
    }

    @Test
    public void testInsertMultiple() {
        assertInsert(
                var("x").has("name", "123").isa("person"),
                var("y").val(123L).isa("runtime"),
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
        VarPattern rel = var("r").isa("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y");
        VarPattern x = var("x").has("title", "Godfather").isa("movie");
        VarPattern y = var("y").has("name", "comedy").isa("genre");
        VarPattern[] vars = new VarPattern[] {rel, x, y};
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
        VarPattern language = var("x").has("name", "123").isa("language");
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
        VarPattern language1 = var().isa("language").has("name", "123");
        VarPattern language2 = var().isa("language").has("name", "456");

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
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATION.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name"),

                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).execute();

        assertTrue(qb.match(label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue())).ask().execute());
        assertTrue(qb.match(label("evolution").sub(Schema.MetaSchema.RELATION.getLabel().getValue())).ask().execute());
        assertTrue(qb.match(label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue())).ask().execute());
        assertTrue(qb.match(label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue())).ask().execute());
        assertTrue(qb.match(label("evolution").relates("evolves-from").relates("evolves-to")).ask().execute());
        assertTrue(qb.match(label("pokemon").plays("evolves-from").plays("evolves-to")).ask().execute());

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
                label("concrete-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("abstract-type").isAbstract().sub(Schema.MetaSchema.ENTITY.getLabel().getValue())
        ).execute();

        assertFalse(qb.match(label("concrete-type").isAbstract()).ask().execute());
        assertTrue(qb.match(label("abstract-type").isAbstract()).ask().execute());
    }

    @Test
    public void testInsertDatatype() {
        qb.insert(
                label("my-type").sub(Schema.MetaSchema.RESOURCE.getLabel().getValue()).datatype(ResourceType.DataType.LONG)
        ).execute();

        MatchQuery query = qb.match(var("x").label("my-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        Assert.assertEquals(ResourceType.DataType.LONG, datatype);
    }

    @Test
    public void testInsertSubResourceType() {
        qb.insert(
                label("my-type").sub(Schema.MetaSchema.RESOURCE.getLabel().getValue()).datatype(ResourceType.DataType.STRING),
                label("sub-type").sub("my-type")
        ).execute();

        MatchQuery query = qb.match(var("x").label("sub-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        Assert.assertEquals(ResourceType.DataType.STRING, datatype);
    }

    @Test
    public void testInsertSubRoleType() {
        qb.insert(
                label("marriage").sub(Schema.MetaSchema.RELATION.getLabel().getValue()).relates("spouse1").relates("spouse2"),
                label("spouse").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("spouse1").sub("spouse"),
                label("spouse2").sub("spouse")
        ).execute();

        assertTrue(qb.match(label("spouse1")).ask().execute());
    }

    @Test
    public void testReferenceByVariableNameAndTypeLabel() {
        qb.insert(
                var("abc").sub("entity"),
                var("abc").label("123"),
                label("123").plays("actor"),
                var("abc").plays("director")
        ).execute();

        assertTrue(qb.match(label("123").sub("entity")).ask().execute());
        assertTrue(qb.match(label("123").plays("actor")).ask().execute());
        assertTrue(qb.match(label("123").plays("director")).ask().execute());
    }

    @Test
    public void testIterateInsertResults() {
        InsertQuery insert = qb.insert(
                var("x").has("name", "123").isa("person"),
                var("z").has("name", "xyz").isa("language")
        );

        Set<Answer> results = insert.stream().collect(Collectors.toSet());
        assertEquals(1, results.size());
        Answer result = results.iterator().next();
        assertEquals(ImmutableSet.of(Graql.var("x"), Graql.var("z")), result.keySet());
        assertThat(result.values(), Matchers.everyItem(notNullValue(Concept.class)));
    }

    @Test
    public void testIterateMatchInsertResults() {
        VarPattern language1 = var().isa("language").has("name", "123");
        VarPattern language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertTrue(qb.match(language1).ask().execute());
        assertTrue(qb.match(language2).ask().execute());

        InsertQuery query = qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO"));
        Iterator<Answer> results = query.iterator();

        assertFalse(qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask().execute());
        assertFalse(qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask().execute());

        Answer result1 = results.next();
        assertEquals(ImmutableSet.of(Graql.var("x")), result1.keySet());

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
        Answer result2 = results.next();
        assertEquals(ImmutableSet.of(Graql.var("x")), result1.keySet());
        assertTrue(qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask().execute());
        assertFalse(results.hasNext());

        assertNotEquals(result1.get("x"), result2.get("x"));
    }

    @Test
    public void testErrorWhenInsertWithPredicate() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("predicate");
        qb.insert(var().id(ConceptId.of("123")).val(gt(3))).execute();
    }

    @Test
    public void testErrorWhenInsertWithMultipleIds() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("123"), containsString("456")));
        qb.insert(var().id(ConceptId.of("123")).id(ConceptId.of("456")).isa("movie")).execute();
    }

    @Test
    public void testErrorWhenInsertWithMultipleValues() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("value"), containsString("123"), containsString("456")));
        qb.insert(var().val("123").val("456").isa("title")).execute();
    }

    @Test
    public void testErrorWhenSubRelation() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("isa"), containsString("relation")));
        qb.insert(
                var().sub("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y"),
                var("x").id(ConceptId.of("Godfather")).isa("movie"),
                var("y").id(ConceptId.of("comedy")).isa("genre")
        ).execute();
    }

    @Test
    public void testInsertReferenceByName() {
        String roleTypeLabel = HAS_OWNER.getLabel("title").getValue();
        qb.insert(
                label("new-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("new-type").plays(roleTypeLabel),
                var("x").isa("new-type")
        ).execute();

        MatchQuery typeQuery = qb.match(var("n").label("new-type"));

        assertEquals(1, typeQuery.stream().count());

        // We checked count ahead of time
        //noinspection OptionalGetWithoutIsPresent
        EntityType newType = typeQuery.get("n").findFirst().get().asEntityType();

        assertTrue(newType.plays().contains(movieGraph.graph().getRole(roleTypeLabel)));

        assertTrue(qb.match(var().isa("new-type")).ask().execute());
    }

    @Test
    public void testInsertRuleType() {
        assertInsert(var("x").label("my-inference-rule").sub(RULE.getLabel().getValue()));
    }

    @Test
    public void testInsertRule() {
        String ruleTypeId = "a-rule-type";
        Pattern when = qb.parsePattern("$x isa entity");
        Pattern then = qb.parsePattern("$x isa entity");
        VarPattern vars = var("x").isa(ruleTypeId).when(when).then(then);
        qb.insert(vars).execute();

        RuleType ruleType = movieGraph.graph().getRuleType(ruleTypeId);
        boolean found = false;
        for (ai.grakn.concept.Rule rule : ruleType.instances()) {
            if(when.equals(rule.getWhen()) && then.equals(rule.getThen())){
                found = true;
                break;
            }
        }
        assertTrue("Unable to find rule with when [" + when + "] and then [" + then + "]", found);
    }

    @Test
    public void testInsertRuleSub() {
        assertInsert(var("x").label("an-sub-rule-type").sub("a-rule-type"));
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").has("title", "WOW A TITLE").isa("movie").isa("movie"));
    }

    @Test
    public void testInsertResourceTypeAndInstance() {
        qb.insert(
                label("movie").has("my-resource"),
                label("my-resource").sub("resource").datatype(ResourceType.DataType.STRING),
                var("x").isa("movie").has("my-resource", "look a string")
        ).execute();
    }

    @Test
    public void testHas() {
        String resourceType = "a-new-resource-type";

        qb.insert(
                label("a-new-type").sub("entity").has(resourceType),
                label(resourceType).sub("resource").datatype(ResourceType.DataType.STRING),
                label("an-unconnected-resource-type").sub("resource").datatype(ResourceType.DataType.LONG)
        ).execute();

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertTrue(qb.match(label("a-new-type").sub("entity").has(resourceType)).ask().execute());
        assertFalse(qb.match(label("a-new-type").has("title")).ask().execute());
        assertFalse(qb.match(label("movie").has(resourceType)).ask().execute());
        assertFalse(qb.match(label("a-new-type").has("an-unconnected-resource-type")).ask().execute());

        VarPattern hasResource = Graql.label(HAS.getLabel(resourceType));
        VarPattern hasResourceOwner = Graql.label(HAS_OWNER.getLabel(resourceType));
        VarPattern hasResourceValue = Graql.label(HAS_VALUE.getLabel(resourceType));

        // Make sure the expected ontology elements are created
        assertTrue(qb.match(hasResource.sub("relation")).ask().execute());
        assertTrue(qb.match(hasResourceOwner.sub("role")).ask().execute());
        assertTrue(qb.match(hasResourceValue.sub("role")).ask().execute());
        assertTrue(qb.match(hasResource.relates(hasResourceOwner)).ask().execute());
        assertTrue(qb.match(hasResource.relates(hasResourceValue)).ask().execute());
        assertTrue(qb.match(label("a-new-type").plays(hasResourceOwner)).ask().execute());
        assertTrue(qb.match(label(resourceType).plays(hasResourceValue)).ask().execute());
    }

    @Test
    public void testKey() {
        String resourceType = "a-new-resource-type";

        qb.insert(
                label("a-new-type").sub("entity").key(resourceType),
                label(resourceType).sub("resource").datatype(ResourceType.DataType.STRING)
        ).execute();

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertTrue(qb.match(label("a-new-type").sub("entity").key(resourceType)).ask().execute());
        assertTrue(qb.match(label("a-new-type").sub("entity").has(resourceType)).ask().execute());
        assertFalse(qb.match(label("a-new-type").sub("entity").key("title")).ask().execute());
        assertFalse(qb.match(label("movie").sub("entity").key(resourceType)).ask().execute());

        VarPattern key = Graql.label(KEY.getLabel(resourceType));
        VarPattern keyOwner = Graql.label(KEY_OWNER.getLabel(resourceType));
        VarPattern keyValue = Graql.label(KEY_VALUE.getLabel(resourceType));

        // Make sure the expected ontology elements are created
        assertTrue(qb.match(key.sub("relation")).ask().execute());
        assertTrue(qb.match(keyOwner.sub("role")).ask().execute());
        assertTrue(qb.match(keyValue.sub("role")).ask().execute());
        assertTrue(qb.match(key.relates(keyOwner)).ask().execute());
        assertTrue(qb.match(key.relates(keyValue)).ask().execute());
        assertTrue(qb.match(label("a-new-type").plays(keyOwner)).ask().execute());
        assertTrue(qb.match(label(resourceType).plays(keyValue)).ask().execute());
    }

    @Test
    public void testKeyCorrectUsage() throws InvalidGraphException {
        // This should only run on tinker because it commits
        assumeTrue(GraknTestSetup.usingTinker());

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();
    }

    @Test
    public void whenInsertingAThingWithTwoKeyResources_Throw() throws InvalidGraphException {
        assumeTrue(GraknTestSetup.usingTinker()); // This should only run on tinker because it commits

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello").has("a-new-resource-type", "goodbye")
        ).execute();

        exception.expect(InvalidGraphException.class);
        movieGraph.graph().commit();
    }

    @Ignore // TODO: Un-ignore this when constraints are designed and implemented
    @Test
    public void testKeyUniqueValue() throws InvalidGraphException {
        assumeTrue(GraknTestSetup.usingTinker()); // This should only run on tinker because it commits

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var("x").isa("a-new-type").has("a-new-resource-type", "hello"),
                var("y").isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();

        exception.expect(InvalidGraphException.class);
        movieGraph.graph().commit();
    }

    @Test
    public void testKeyRequiredOwner() throws InvalidGraphException {
        assumeTrue(GraknTestSetup.usingTinker()); // This should only run on tinker because it commits

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub("resource").datatype(ResourceType.DataType.STRING),
                var().isa("a-new-type")
        ).execute();

        exception.expect(InvalidGraphException.class);
        movieGraph.graph().commit();
    }

    @Test
    public void testResourceTypeRegex() {
        qb.insert(label("greeting").sub("resource").datatype(ResourceType.DataType.STRING).regex("hello|good day")).execute();

        MatchQuery match = qb.match(var("x").label("greeting"));
        assertEquals("hello|good day", match.get("x").findFirst().get().asResourceType().getRegex());
    }

    @Test
    public void testInsertExecuteResult() {
        InsertQuery query = qb.insert(var("x").isa("movie"));

        List<Answer> results = query.execute();
        assertEquals(1, results.size());
        Answer result = results.get(0);
        assertEquals(Sets.newHashSet(Graql.var("x")), result.keySet());
        Entity x = result.get("x").asEntity();
        assertEquals("movie", x.type().getLabel().getValue());
    }

    @Test
    public void testErrorWhenInsertRelationWithEmptyRolePlayer() {
        exception.expect(GraqlQueryException.class);
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
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        qb.insert(label("my-resource").sub(Schema.MetaSchema.RESOURCE.getLabel().getValue())).execute();
    }

    @Test
    public void testErrorWhenAddingInstanceOfConcept() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("meta-type"), containsString("my-thing"), containsString(Schema.MetaSchema.THING.getLabel().getValue()))
        );
        qb.insert(var("my-thing").isa(Schema.MetaSchema.THING.getLabel().getValue())).execute();
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        qb.insert(label("thingy").sub("thingy")).execute();
    }

    @Test
    public void testErrorTypeWithoutLabel() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("type"), containsString("label")));
        qb.insert(var().sub("entity")).execute();
    }

    @Test
    public void testErrorInsertResourceWithoutValue() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("resource"), containsString("value")));
        qb.insert(var("x").isa("name")).execute();
    }

    @Test
    public void testErrorInsertInstanceWithName() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("instance"), containsString("name"), containsString("abc")));
        qb.insert(label("abc").isa("movie")).execute();
    }

    @Test
    public void testErrorInsertResourceWithName() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("instance"), containsString("name"), containsString("bobby")));
        qb.insert(label("bobby").val("bob").isa("name")).execute();
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
        ConceptId apocalypseNow = qb.match(var("x").val("Apocalypse Now")).get("x").findAny().get().getId();

        assertFalse(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertTrue(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
    }

    @Test
    public void testInsertResourceOnExistingResourceIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").val("Apocalypse Now")).get("x").findAny().get().getId();

        assertFalse(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
        qb.insert(var().id(apocalypseNow).isa("title").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertTrue(qb.match(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).ask().execute());
    }

    @Test
    public void testInsertInstanceWithoutType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("isa")));
        qb.insert(var().has("name", "Bob")).execute();
    }

    @Test
    public void testInsertRuleWithoutLhs() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("when")));
        qb.insert(var().isa("inference-rule").then(var("x").isa("movie"))).execute();
    }

    @Test
    public void testInsertRuleWithoutRhs() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("then")));
        qb.insert(var().isa("inference-rule").when(var("x").isa("movie"))).execute();
    }

    @Test
    public void testInsertNonRuleWithWhen() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(INSERT_UNSUPPORTED_PROPERTY.getMessage("when", RULE.getLabel()));
        qb.insert(var().isa("movie").when(var("x"))).execute();
    }

    @Test
    public void testInsertNonRuleWithThen() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(INSERT_UNSUPPORTED_PROPERTY.getMessage("then", RULE.getLabel()));
        qb.insert(label("thingy").sub("movie").then(var("x"))).execute();
    }

    @Test
    public void testErrorWhenNonExistentResource() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("nothing");
        qb.insert(label("blah this").sub("entity").has("nothing")).execute();
    }

    @Test
    public void whenInsertingMetaType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.INSERT_METATYPE.getMessage("my-metatype", Schema.MetaSchema.THING.getLabel().getValue()));
        qb.insert(label("my-metatype").sub(Schema.MetaSchema.THING.getLabel().getValue())).execute();
    }

    @Test
    public void whenInsertingMultipleRolePlayers_BothRolePlayersAreAdded() {
        List<Answer> results = qb.match(
                var("g").has("title", "Godfather"),
                var("m").has("title", "The Muppets")
        ).insert(
                var("c").isa("cluster").has("name", "2"),
                var("r").rel("cluster-of-production", "c").rel("production-with-cluster", "g").rel("production-with-cluster", "m").isa("has-cluster")
        ).execute();

        Thing cluster = results.get(0).get("c").asThing();
        Thing godfather = results.get(0).get("g").asThing();
        Thing muppets = results.get(0).get("m").asThing();
        Relation relation = results.get(0).get("r").asRelation();

        Role clusterOfProduction = movieGraph.graph().getRole("cluster-of-production");
        Role productionWithCluster = movieGraph.graph().getRole("production-with-cluster");

        assertEquals(relation.rolePlayers(), ImmutableSet.of(cluster, godfather, muppets));
        assertEquals(relation.rolePlayers(clusterOfProduction), ImmutableSet.of(cluster));
        assertEquals(relation.rolePlayers(productionWithCluster), ImmutableSet.of(godfather, muppets));
    }

    @Test(expected = Exception.class)
    public void matchInsertNullVar() {
        movieGraph.graph().graql().match(var("x").isa("movie")).insert((VarPattern) null).execute();
    }

    @Test(expected = Exception.class)
    public void matchInsertNullCollection() {
        movieGraph.graph().graql().match(var("x").isa("movie")).insert((Collection<? extends VarPattern>) null).execute();
    }

    @Test
    public void whenMatchInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        movieGraph.graph().graql().match(var()).insert(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullVar() {
        movieGraph.graph().graql().insert((VarPattern) null).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullCollection() {
        movieGraph.graph().graql().insert((Collection<? extends VarPattern>) null).execute();
    }

    @Test
    public void whenInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        movieGraph.graph().graql().insert(Collections.EMPTY_SET).execute();
    }

    private void assertInsert(VarPattern... vars) {
        // Make sure vars don't exist
        for (VarPattern var : vars) {
            assertFalse(qb.match(var).ask().execute());
        }

        // Insert all vars
        qb.insert(vars).execute();

        // Make sure all vars exist
        for (VarPattern var : vars) {
            assertTrue(qb.match(var).ask().execute());
        }

        // Delete all vars
        for (VarPattern var : vars) {
            qb.match(var).delete(var.admin().getVarName()).execute();
        }

        // Make sure vars don't exist
        for (VarPattern var : vars) {
            assertFalse(qb.match(var).ask().execute());
        }
    }

    @Test
    public void deleteMe(){
        GraphLoader loader = GraphLoader.empty();

        //Resources
        loader.graph().graql().parse("# The resources as defined in the ldbc-snb benchmark\n" +
                "insert\n" +
                "\n" +
                "# proxy for all integers\n" +
                "\"Integer\" sub resource is-abstract datatype long;\n" +
                "\"integer-32\" sub Integer;\n" +
                "\n" +
                "# resources\n" +
                "\"length\" sub integer-32;\n" +
                "\"work-from\" sub integer-32;\n" +
                "\n").execute();
        loader.graph().commit();

        //Relation Which Has Resources
        loader.graph().graql().parse("# The relationships defined in the ldbc-snb benchmark\n" +
                "insert\n" +
                "\n" +
                "\"employee\" sub role;\n" +
                "\"employer\" sub role;\n" +
                "\"work-at\" sub relation\n" +
                "\thas work-from\n" +
                "\trelates employer,\n" +
                "\trelates employee;\n").execute();
        loader.graph().commit();

        //Entities Which Has Resourced
        loader.graph().graql().parse("# The entities defined in the ldbc-snb benchmark\n" +
                "insert\n" +
                "\n" +
                "\"message\" sub entity\n" +
                "\tis-abstract\n" +
                "\thas length;\n" +
                "\n").execute();
        loader.graph().commit();
    }
}
