/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.StatementImpl;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.NO_PATTERNS;
import static grakn.core.graql.internal.Schema.MetaSchema.ENTITY;
import static grakn.core.graql.query.Graql.gt;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class InsertQueryIT {

    private static final Variable w = var("w");
    private static final Variable x = var("x");
    private static final Variable y = var("y");
    private static final Variable z = var("z");

    private static final Label title = Label.of("title");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();

    public static SessionImpl session;

    private TransactionImpl tx;
    private QueryBuilder qb;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
        qb = tx.graql();
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
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
        Statement rel = var("r").isa("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y");
        Statement x = var("x").has("title", "Godfather").isa("movie");
        Statement y = var("y").has("name", "comedy").isa("genre");
        Statement[] vars = new Statement[]{rel, x, y};
        Pattern[] patterns = new Pattern[]{rel, x, y};

        assertNotExists(qb.match(patterns));

        qb.insert(vars).execute();
        assertExists(qb, patterns);

        qb.match(patterns).delete("r").execute();
        assertNotExists(qb, patterns);
    }

    @Test
    public void testInsertSameVarName() {
        qb.insert(var("x").has("title", "SW"), var("x").has("title", "Star Wars").isa("movie")).execute();

        assertExists(qb, var().isa("movie").has("title", "SW"));
        assertExists(qb, var().isa("movie").has("title", "Star Wars"));
        assertExists(qb, var().isa("movie").has("title", "SW").has("title", "Star Wars"));
    }

    @Test
    public void testInsertRepeat() {
        Statement language = var("x").has("name", "123").isa("language");
        InsertQuery query = qb.insert(language);

        assertEquals(0, tx.stream(Graql.match(language)).count());
        query.execute();
        assertEquals(1, tx.stream(Graql.match(language)).count());
        query.execute();
        assertEquals(2, tx.stream(Graql.match(language)).count());
        query.execute();
        assertEquals(3, tx.stream(Graql.match(language)).count());

        qb.match(language).delete("x").execute();
        assertEquals(0, tx.stream(Graql.match(language)).count());
    }

    @Test
    public void testMatchInsertQuery() {
        Statement language1 = var().isa("language").has("name", "123");
        Statement language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertExists(qb, language1);
        assertExists(qb, language2);

        qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO")).execute();
        assertExists(qb, var().isa("language").has("name", "123").has("name", "HELLO"));
        assertExists(qb, var().isa("language").has("name", "456").has("name", "HELLO"));

        qb.match(var("x").isa("language")).delete("x").execute();
        assertNotExists(qb, language1);
        assertNotExists(qb, language2);
    }

    @Test
    public void testIterateInsertResults() {
        InsertQuery insert = qb.insert(
                var("x").has("name", "123").isa("person"),
                var("z").has("name", "xyz").isa("language")
        );

        Set<ConceptMap> results = insert.stream().collect(toSet());
        assertEquals(1, results.size());
        ConceptMap result = results.iterator().next();
        assertEquals(ImmutableSet.of(var("x"), var("z")), result.vars());
        assertThat(result.concepts(), Matchers.everyItem(notNullValue(Concept.class)));
    }

    @Test
    public void testMatchInsertShouldInsertDataEvenWhenResultsAreNotCollected() {
        Statement language1 = var().isa("language").has("name", "123");
        Statement language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertExists(qb, language1);
        assertExists(qb, language2);

        InsertQuery query = qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO"));
        query.stream();

        assertExists(qb, var().isa("language").has("name", "123").has("name", "HELLO"));
        assertExists(qb, var().isa("language").has("name", "456").has("name", "HELLO"));
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
    public void whenInsertingAResourceWithMultipleValues_Throw() {
        Statement varPattern = var().val("123").val("456").isa("title");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(isOneOf(
                GraqlQueryException.insertMultipleProperties(varPattern, "", "123", "456").getMessage(),
                GraqlQueryException.insertMultipleProperties(varPattern, "", "456", "123").getMessage()
        ));

        qb.insert(varPattern).execute();
    }

    @Test
    public void testErrorWhenSubRelation() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.insertUnsupportedProperty("sub").getMessage());
        qb.insert(
                var().sub("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y"),
                var("x").id(ConceptId.of("Godfather")).isa("movie"),
                var("y").id(ConceptId.of("comedy")).isa("genre")
        ).execute();
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").has("title", "WOW A TITLE").isa("movie").isa("movie"));
    }

    @Test
    public void testKeyCorrectUsage() throws InvalidKBException {
        qb.define(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ).execute();

        qb.insert(var().isa("a-new-type").has("a-new-resource-type", "hello")).execute();
    }

    @Test
    public void whenInsertingAThingWithTwoKeyResources_Throw() throws InvalidKBException {
        qb.define(
                label("a-new-type").sub("entity").key("a-new-attribute-type"),
                label("a-new-attribute-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ).execute();

        qb.insert(
                var().isa("a-new-type").has("a-new-attribute-type", "hello").has("a-new-attribute-type", "goodbye")
        ).execute();

        exception.expect(InvalidKBException.class);
        tx.commit();
    }

    @Ignore // TODO: Un-ignore this when constraints are designed and implemented
    @Test
    public void testKeyUniqueValue() throws InvalidKBException {
        qb.define(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type")
                        .sub(label(Schema.MetaSchema.ATTRIBUTE.getLabel()))
                        .datatype(AttributeType.DataType.STRING)
        ).execute();

        qb.insert(
                var("x").isa("a-new-type").has("a-new-resource-type", "hello"),
                var("y").isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();

        exception.expect(InvalidKBException.class);
        tx.commit();
    }

    @Test
    public void testKeyRequiredOwner() throws InvalidKBException {
        qb.define(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ).execute();

        qb.insert(var().isa("a-new-type")).execute();

        exception.expect(InvalidKBException.class);
        tx.commit();
    }

    @Test
    public void whenExecutingAnInsertQuery_ResultContainsAllInsertedVars() {
        Variable x = var("x");
        Variable type = var("type");

        // Note that two variables refer to the same type. They should both be in the result
        InsertQuery query = qb.insert(x.isa(type), type.label("movie"));

        ConceptMap result = Iterables.getOnlyElement(query);
        assertThat(result.vars(), containsInAnyOrder(x, type));
        assertEquals(result.get(type), result.get(x).asEntity().type());
        assertEquals(result.get(type).asType().label(), Label.of("movie"));
    }

    @Test
    public void whenAddingAnAttributeRelationshipWithProvenance_TheAttributeAndProvenanceAreAdded() {
        InsertQuery query = qb.insert(
                y.has("provenance", z.val("Someone told me")),
                w.isa("movie").has(title, x.val("My Movie"), y)
        );

        ConceptMap answer = Iterables.getOnlyElement(query.execute());

        Entity movie = answer.get(w).asEntity();
        Attribute<String> theTitle = answer.get(x).asAttribute();
        Relationship hasTitle = answer.get(y).asRelationship();
        Attribute<String> provenance = answer.get(z).asAttribute();

        assertThat(hasTitle.rolePlayers().toArray(), arrayContainingInAnyOrder(movie, theTitle));
        assertThat(hasTitle.attributes().toArray(), arrayContaining(provenance));
    }

    @Test
    public void whenAddingProvenanceToAnExistingRelationship_TheProvenanceIsAdded() {
        InsertQuery query = qb
                .match(w.isa("movie").has(title, x.val("The Muppets"), y))
                .insert(x, w, y.has("provenance", z.val("Someone told me")));

        ConceptMap answer = Iterables.getOnlyElement(query.execute());

        Entity movie = answer.get(w).asEntity();
        Attribute<String> theTitle = answer.get(x).asAttribute();
        Relationship hasTitle = answer.get(y).asRelationship();
        Attribute<String> provenance = answer.get(z).asAttribute();

        assertThat(hasTitle.rolePlayers().toArray(), arrayContainingInAnyOrder(movie, theTitle));
        assertThat(hasTitle.attributes().toArray(), arrayContaining(provenance));
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
    public void testErrorWhenAddingInstanceOfConcept() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("meta-type"), containsString("my-thing"), containsString(Schema.MetaSchema.THING.getLabel().getValue()))
        );
        qb.insert(var("my-thing").isa(Schema.MetaSchema.THING.getLabel().getValue())).execute();
    }

    @Test
    public void whenInsertingAResourceWithoutAValue_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("name")));
        qb.insert(var("x").isa("name")).execute();
    }

    @Test
    public void whenInsertingAnInstanceWithALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("label"), containsString("abc")));
        qb.insert(label("abc").isa("movie")).execute();
    }

    @Test
    public void whenInsertingAResourceWithALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("label"), containsString("bobby")));
        qb.insert(label("bobby").val("bob").isa("name")).execute();
    }

    @Test
    public void testInsertDuplicatePattern() {
        qb.insert(var().isa("person").has("name", "a name"), var().isa("person").has("name", "a name")).execute();
        assertEquals(2, tx.stream(Graql.match(x.has("name", "a name"))).count());
    }

    @Test
    public void testInsertResourceOnExistingId() {
        ConceptId apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now")).get("x")
                .stream().map(ans -> ans.get("x")).findAny().get().id();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertResourceOnExistingIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now")).get("x")
                .stream().map(ans -> ans.get("x")).findAny().get().id();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).isa("movie").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertResourceOnExistingResourceId() {
        ConceptId apocalypseNow = qb.match(var("x").val("Apocalypse Now")).get("x")
                .stream().map(ans -> ans.get("x")).findAny().get().id();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Not Right Now"));
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Not Right Now")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Not Right Now"));
    }

    @Test
    public void testInsertResourceOnExistingResourceIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").val("Apocalypse Now")).get("x")
                .stream().map(ans -> ans.get("x")).findAny().get().id();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).isa("title").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertInstanceWithoutType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("isa")));
        qb.insert(var().has("name", "Bob")).execute();
    }

    @Test
    public void whenInsertingMultipleRolePlayers_BothRolePlayersAreAdded() {
        List<ConceptMap> results = qb.match(
                var("g").has("title", "Godfather"),
                var("m").has("title", "The Muppets")
        ).insert(
                var("c").isa("cluster").has("name", "2"),
                var("r").rel("cluster-of-production", "c").rel("production-with-cluster", "g").rel("production-with-cluster", "m").isa("has-cluster")
        ).execute();

        Thing cluster = results.get(0).get("c").asThing();
        Thing godfather = results.get(0).get("g").asThing();
        Thing muppets = results.get(0).get("m").asThing();
        Relationship relationship = results.get(0).get("r").asRelationship();

        Role clusterOfProduction = tx.getRole("cluster-of-production");
        Role productionWithCluster = tx.getRole("production-with-cluster");

        assertEquals(relationship.rolePlayers().collect(toSet()), ImmutableSet.of(cluster, godfather, muppets));
        assertEquals(relationship.rolePlayers(clusterOfProduction).collect(toSet()), ImmutableSet.of(cluster));
        assertEquals(relationship.rolePlayers(productionWithCluster).collect(toSet()), ImmutableSet.of(godfather, muppets));
    }

    @Test
    public void whenInsertingWithAMatch_ProjectMatchResultsOnVariablesInTheInsert() {
        qb.define(
                label("maybe-friends").relates("friend").sub("relationship"),
                label("person").plays("friend")
        ).execute();

        InsertQuery query = qb.match(
                var().rel("actor", x).rel("production-with-cast", z),
                var().rel("actor", y).rel("production-with-cast", z)
        ).insert(
                w.rel("friend", x).rel("friend", y).isa("maybe-friends")
        );

        List<ConceptMap> answers = query.execute();

        for (ConceptMap answer : answers) {
            assertThat(
                    "Should contain only variables mentioned in the insert (so excludes `$z`)",
                    answer.vars(),
                    containsInAnyOrder(x, y, w)
            );
        }

        assertEquals("Should contain only distinct results", answers.size(), Sets.newHashSet(answers).size());
    }

    @Test(expected = Exception.class)
    public void matchInsertNullVar() {
        tx.graql().match(var("x").isa("movie")).insert((Statement) null).execute();
    }

    @Test(expected = Exception.class)
    public void matchInsertNullCollection() {
        tx.graql().match(var("x").isa("movie")).insert((Collection<? extends Statement>) null).execute();
    }

    @Test
    public void whenMatchInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        tx.graql().match(var()).insert(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullVar() {
        tx.graql().insert((Statement) null).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullCollection() {
        tx.graql().insert((Collection<? extends Statement>) null).execute();
    }

    @Test
    public void whenInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        tx.graql().insert(Collections.EMPTY_SET).execute();
    }

    @Test
    public void whenSettingTwoTypes_Throw() {
        EntityType movie = tx.getEntityType("movie");
        EntityType person = tx.getEntityType("person");

        // We have to construct it this way because you can't have two `isa`s normally
        // TODO: less bad way?
        Statement varPattern = new StatementImpl(
                var("x"),
                ImmutableSet.of(new IsaProperty(label("movie")), new IsaProperty(label("person")))
        );

        // We don't know in what order the message will be
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(isOneOf(
                GraqlQueryException.insertMultipleProperties(varPattern, "isa", movie, person).getMessage(),
                GraqlQueryException.insertMultipleProperties(varPattern, "isa", person, movie).getMessage()
        ));

        tx.graql().insert(var("x").isa("movie"), var("x").isa("person")).execute();
    }

    @Test
    public void whenSpecifyingExistingConceptIdWithIncorrectType_Throw() {
        EntityType movie = tx.getEntityType("movie");
        EntityType person = tx.getEntityType("person");

        Concept aMovie = movie.instances().iterator().next();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.insertPropertyOnExistingConcept("isa", person, aMovie).getMessage());

        tx.graql().insert(var("x").id(aMovie.id()).isa("person")).execute();
    }

    @Test
    public void whenInsertingASchemaConcept_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.insertUnsupportedProperty(SubProperty.NAME).getMessage());

        qb.insert(label("new-type").sub(label(ENTITY.getLabel()))).execute();
    }

    @Test
    public void whenModifyingASchemaConceptInAnInsertQuery_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.insertUnsupportedProperty(PlaysProperty.NAME).getMessage());

        qb.insert(label("movie").plays("actor")).execute();
    }

    private void assertInsert(Statement... vars) {
        // Make sure vars don't exist
        for (Statement var : vars) {
            assertNotExists(qb, var);
        }

        // Insert all vars
        qb.insert(vars).execute();

        // Make sure all vars exist
        for (Statement var : vars) {
            assertExists(qb, var);
        }

        // Delete all vars
        for (Statement var : vars) {
            qb.match(var).delete(var.var()).execute();
        }

        // Make sure vars don't exist
        for (Statement var : vars) {
            assertNotExists(qb, var);
        }
    }
}
