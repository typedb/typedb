/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.Void;
import grakn.core.core.Schema;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.pattern.Pattern;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementThing;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.VARIABLE_OUT_OF_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "Duplicates"})
public class GraqlDeleteIT {

    public static final Statement ENTITY = type(Graql.Token.Type.ENTITY);
    public static final Statement x = var("x");
    public static final Statement y = var("y");

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static Session session;
    public Transaction tx;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private MatchClause kurtz;
    private MatchClause marlonBrando;
    private MatchClause apocalypseNow;
    private MatchClause kurtzCastRelation;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.writeTransaction();

        kurtz = Graql.match(x.has("name", "Colonel Walter E. Kurtz"));
        marlonBrando = Graql.match(x.has("name", "Marlon Brando"));
        apocalypseNow = Graql.match(x.has("title", "Apocalypse Now"));
        kurtzCastRelation =
                Graql.match(var("a").rel("character-being-played", var().has("name", "Colonel Walter E. Kurtz")));
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
    public void testGetSort() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y")
        );

        assertEquals("Al Pacino", answers.get(0).get("y").asAttribute().value());
        assertEquals("Bette Midler", answers.get(1).get("y").asAttribute().value());
        assertEquals("Jude Law", answers.get(2).get("y").asAttribute().value());
        assertEquals("Kermit The Frog", answers.get(3).get("y").asAttribute().value());

        Set<Concept> toDelete = answers.stream().map(answer -> answer.get("x")).collect(Collectors.toSet());

        Void deleted = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .delete().sort("y")
        ).get(0);

        assertTrue(deleted.message().contains("success"));
        for (Concept concept : toDelete) {
            assertTrue(concept.isDeleted());
        }
    }

    @Test
    public void testGetSortAscLimit() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y", "asc").limit(3)
        );

        assertEquals(3, answers.size());
        assertEquals("Al Pacino", answers.get(0).get("y").asAttribute().value());
        assertEquals("Bette Midler", answers.get(1).get("y").asAttribute().value());
        assertEquals("Jude Law", answers.get(2).get("y").asAttribute().value());

        Set<Concept> toDelete = answers.stream().map(answer -> answer.get("x")).collect(Collectors.toSet());

        Void deleted = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .delete().sort("y", "asc").limit(3)
        ).get(0);

        assertTrue(deleted.message().contains("success"));
        for (Concept concept : toDelete) {
            assertTrue(concept.isDeleted());
        }
    }

    @Test
    public void testGetSortDescOffsetLimit() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y", "desc").offset(3).limit(4)
        );

        assertEquals(4, answers.size());
        assertEquals("Miranda Heart", answers.get(0).get("y").asAttribute().value());
        assertEquals("Martin Sheen", answers.get(1).get("y").asAttribute().value());
        assertEquals("Marlon Brando", answers.get(2).get("y").asAttribute().value());
        assertEquals("Kermit The Frog", answers.get(3).get("y").asAttribute().value());

        Set<Concept> toDelete = answers.stream().map(answer -> answer.get("x")).collect(Collectors.toSet());

        Void deleted = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .delete().sort("y", "desc").offset(3).limit(4)
        ).get(0);

        assertTrue(deleted.message().contains("success"));
        for (Concept concept : toDelete) {
            assertTrue(concept.isDeleted());
        }
    }

    @Test
    public void testDeleteMultiple() {
        tx.execute(Graql.define(type("fake-type").sub(ENTITY)));
        tx.execute(Graql.insert(x.isa("fake-type"), y.isa("fake-type")));

        assertEquals(2, tx.stream(Graql.match(x.isa("fake-type"))).count());

        tx.execute(Graql.match(x.isa("fake-type")).delete(x.var()));

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Test
    public void testDeleteEntity() {

        assertExists(tx, var().has("title", "Godfather"));
        assertExists(tx, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(tx, var().has("name", "Don Vito Corleone"));

        tx.execute(Graql.match(x.has("title", "Godfather")).delete(x.var()));

        assertNotExists(tx, var().has("title", "Godfather"));
        assertNotExists(tx, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(tx, var().has("name", "Don Vito Corleone"));
    }

    @Test
    public void testDeleteRelation() {
        assertExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertExists(tx, kurtzCastRelation);

        tx.execute(kurtzCastRelation.delete("a"));

        assertExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertNotExists(tx, kurtzCastRelation);
    }

    @Test
    public void testDeleteAllRolePlayers() {
        ConceptId id = tx.stream(kurtzCastRelation.get("a")).map(ans -> ans.get("a")).findFirst().get().id();

        assertExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertTrue(checkIdExists(tx, id));

        tx.execute(kurtz.delete(x.var()));

        assertNotExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertTrue(checkIdExists(tx, id));

        tx.execute(marlonBrando.delete(x.var()));

        assertNotExists(tx, kurtz);
        assertNotExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertTrue(checkIdExists(tx, id));

        tx.execute(apocalypseNow.delete(x.var()));

        assertNotExists(tx, kurtz);
        assertNotExists(tx, marlonBrando);
        assertNotExists(tx, apocalypseNow);
        assertFalse(checkIdExists(tx, id));
    }

    private boolean checkIdExists(Transaction tx, ConceptId id) {
        boolean exists;
        try {
            exists = !tx.execute(Graql.match(var().id(id.getValue()))).isEmpty();
        } catch (GraqlSemanticException e) {
            exists = false;
        }
        return exists;
    }

    @Test
    public void whenDeletingAResource_TheResourceAndImplicitRelationsAreDeleted() {
        ConceptId id = tx.stream(Graql.match(
                x.has("title", "Godfather"),
                var("a").rel(x).rel(y).isa(Schema.ImplicitType.HAS.getLabel("tmdb-vote-count").getValue())
        ).get("a")).map(ans -> ans.get("a")).findFirst().get().id();

        assertExists(tx, var().has("title", "Godfather"));
        assertExists(tx, var().id(id.getValue()));
        assertExists(tx, var().val(1000L).isa("tmdb-vote-count"));

        tx.execute(Graql.match(x.val(1000L).isa("tmdb-vote-count")).delete(x.var()));

        assertExists(tx, var().has("title", "Godfather"));
        assertNotExists(tx, var().id(id.getValue()));
        assertNotExists(tx, var().val(1000L).isa("tmdb-vote-count"));
    }

    @Test
    public void whenDeletingAResourceOwnerAndImplicitRelation_NoErrorIsThrown() {
        /*
        Failure was discovered from this exact insert, followed by deleting all the returned IDs in benchmark
        Because we delete relations first, if a relation with a non-reified attribute relation is deleted, the
        Janus edge representing the non-reified attribute relation is deleted automatically

        note the failure is very hard to reproduce reliable due to nondeterministic sorting on the server
        This test illustrates the failure case but doesn't cause it usually (fails more when there are around 2k instances in DB)
        */

        Session session = graknServer.sessionWithNewKeyspace();
        Transaction tx = session.writeTransaction();

        tx.execute(Graql.parse("define" +
                "    unique-key sub attribute, datatype long;" +
                "    name sub attribute, datatype string," +
                "        key unique-key;" +
                "    region-code sub attribute, datatype long," +
                "        key unique-key;" +
                "    road sub entity," +
                "        plays endpoint," +
                "        has name," +
                "        key unique-key;" +
                "    intersection sub relation," +
                "        relates endpoint, " +
                "        has region-code," +
                "        key unique-key;").asDefine());
        tx.execute(Graql.parse("define" +
                "    @has-name key unique-key;" +
                "    @has-region-code key unique-key;").asDefine());

        tx.commit();
        tx = session.writeTransaction();

        List<ConceptMap> answers = tx.execute(Graql.parse("insert" +
                "      $intersection1 (endpoint: $r1, endpoint: $r2, endpoint: $r3) isa intersection, has unique-key $k1, has region-code $rc via $imp1; $k1 -128;" +
                "      $imp1 has unique-key $k2; $k2 -129;" +
                "      $rc -1000; $rc has unique-key $k3; $k3 -130;" +
                "      $r1 isa road, has unique-key $k4, has name $n1 via $imp2; $k4 -131 ;$n1 \"Street\"; $n1 has unique-key $k5; $k5 -132; $imp2 has unique-key $k6; $k6 -133;" +
                "      $r2 isa road, has unique-key $k7, has name $n2 via $imp3; $k7 -134; $n2 \"Avenue\"; $n2 has unique-key $k107; $k107 -135; $imp3 has unique-key $k8; $k8 -136;" +
                "      $r3 isa road, has unique-key $k9, has name $n3 via $imp4; $k9 -137; $n3 \"Boulevard\"; $n3 has unique-key $k10; $k10 -138; $imp4 has unique-key $k11; $k11 -139;" +
                "      $intersection2 (endpoint: $r1, endpoint: $r4) isa intersection, has region-code $rc2 via $imp5, has unique-key $k12; $k12 -140;" +
                "      $imp5 has unique-key $k13; $k13 -141;" +
                "      $rc2 -2000; $rc2 has unique-key $k14; $k14 -142;" +
                "      $r4 isa road, has unique-key $k15, has name $n4 via $imp6; $k15 -143; $n4 \"Alice\"; $n4 has unique-key $k16; $k16 -144; $imp6 has unique-key $k17; $k17 -145;").asInsert());

        List<ConceptId> insertedIds = answers.stream().flatMap(conceptMap -> conceptMap.concepts().stream()).map(Concept::id).collect(Collectors.toList());

        tx.commit();
        tx = session.writeTransaction();

        List<Pattern> idPatterns = new ArrayList<>();
        for (int i = 0; i < insertedIds.size(); i++) {
            StatementThing id = var("v" + i).id(insertedIds.get(i).toString());
            idPatterns.add(id);
        }
        List<ConceptMap> answersById = tx.execute(Graql.match(idPatterns).get());
        assertEquals(answersById.size(), 1);

        tx.execute(Graql.match(idPatterns).delete(idPatterns.stream().flatMap(pattern -> pattern.variables().stream()).collect(Collectors.toList())));
        tx.commit();

    }

    @Test
    public void deleteRelationWithReifiedImplicitWithAttribute() {
        Session session = graknServer.sessionWithNewKeyspace();
        Transaction tx = session.writeTransaction();

        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<Long> year = tx.putAttributeType("year", AttributeType.DataType.LONG);
        Role work = tx.putRole("work");
        EntityType somework = tx.putEntityType("somework").plays(work);
        Role author = tx.putRole("author");
        EntityType person = tx.putEntityType("person").plays(author);
        tx.putRelationType("authored-by").relates(work).relates(author).has(year);
        tx.getRelationType("@has-year").has(name);

        Stream<ConceptMap> answers = tx.stream(Graql.parse("insert $x isa person;" +
                "$y isa somework; " +
                "$a isa year; $a 2020; " +
                "$r (author: $x, work: $y) isa authored-by; $r has year $a via $imp; " +
                "$imp has name $name; $name \"testing\";").asInsert());

        List<ConceptId> insertedIds = answers.flatMap(conceptMap -> conceptMap.concepts().stream().map(Concept::id)).collect(Collectors.toList());
        tx.commit();
        tx = session.writeTransaction();

        List<Pattern> idPatterns = new ArrayList<>();
        for (int i = 0; i < insertedIds.size(); i++) {
            StatementThing id = var("v" + i).id(insertedIds.get(i).toString());
            idPatterns.add(id);
        }

        // clean up, delete the IDs we inserted for this test
        tx.execute(Graql.match(idPatterns).delete(idPatterns.stream().flatMap(pattern -> pattern.variables().stream()).collect(Collectors.toList())));
        tx.commit();
    }

    @Test
    public void deleteRelation_AttributeOwnershipsDeleted() {
        Session session = graknServer.sessionWithNewKeyspace();
        Transaction tx = session.writeTransaction();

        Role author = tx.putRole("author");
        Role work = tx.putRole("work");
        RelationType authoredBy = tx.putRelationType("authored-by").relates(author).relates(work);
        EntityType person = tx.putEntityType("person").plays(author);
        EntityType production = tx.putEntityType("production").plays(work);
        AttributeType<String> provenance = tx.putAttributeType("provenance", AttributeType.DataType.STRING);
        authoredBy.has(provenance);

        Entity aPerson = person.create();
        Relation aRelation = authoredBy.create();
        aRelation.assign(author, aPerson);
        Attribute<String> aProvenance = provenance.create("hello");
        aRelation.has(aProvenance);
        ConceptId relationId = aRelation.id();

        tx.commit();
        tx = session.writeTransaction();

        aProvenance = tx.getAttributesByValue("hello").iterator().next();
        assertTrue(aProvenance.type().label().toString().equals("provenance"));
        assertEquals(aProvenance.owners().count(), 1);

        aRelation = tx.getConcept(relationId);
        aRelation.delete();
        assertTrue(aRelation.isDeleted());

        assertEquals(aProvenance.owners().collect(Collectors.toList()).size(), 0);
    }

    @Test
    public void deleteLastRolePlayer_RelationAndAttrOwnershipIsRemoved() {
        Session session = graknServer.sessionWithNewKeyspace();
        Transaction tx = session.writeTransaction();

        Role author = tx.putRole("author");
        Role work = tx.putRole("work");
        RelationType authoredBy = tx.putRelationType("authored-by").relates(author).relates(work);
        EntityType person = tx.putEntityType("person").plays(author);
        EntityType production = tx.putEntityType("production").plays(work);
        AttributeType<String> provenance = tx.putAttributeType("provenance", AttributeType.DataType.STRING);
        authoredBy.has(provenance);

        Entity aPerson = person.create();
        ConceptId personId = aPerson.id();
        Relation aRelation = authoredBy.create();
        aRelation.assign(author, aPerson);
        Attribute<String> aProvenance = provenance.create("hello");
        aRelation.has(aProvenance);
        ConceptId relationId = aRelation.id();

        tx.commit();
        tx = session.writeTransaction();

        aProvenance = tx.getAttributesByValue("hello").iterator().next();
        assertTrue(aProvenance.type().label().toString().equals("provenance"));
        assertEquals(aProvenance.owners().count(), 1);

        aRelation = tx.getConcept(relationId);
        tx.getConcept(personId).delete();
        assertTrue(aRelation.isDeleted());

        assertEquals(aProvenance.owners().collect(Collectors.toList()).size(), 0);
    }


    @Test
    public void afterDeletingAllInstances_TheTypeCanBeUndefined() {
        MatchClause movie = Graql.match(x.isa("movie"));

        assertNotNull(tx.getEntityType("movie"));
        assertExists(tx, movie);

        tx.execute(movie.delete(x.var()));

        assertNotNull(tx.getEntityType("movie"));
        assertNotExists(tx, movie);

        tx.execute(Graql.undefine(type("movie").sub("production")));

        assertNull(tx.getEntityType("movie"));
    }

    @Test
    public void whenDeletingMultipleVariables_AllVariablesGetDeleted() {
        tx.execute(Graql.define(type("fake-type").sub(ENTITY)));
        tx.execute(Graql.insert(x.isa("fake-type"), y.isa("fake-type")));

        assertEquals(2, tx.stream(Graql.match(x.isa("fake-type"))).count());

        tx.execute(Graql.match(x.isa("fake-type"), y.isa("fake-type"), x.not(y.var())).delete(x.var(), y.var()));

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingWithNoArguments_AllVariablesGetDeleted() {
        tx.execute(Graql.define(type("fake-type").sub(Graql.Token.Type.ENTITY)));
        tx.execute(Graql.insert(x.isa("fake-type"), y.isa("fake-type")));

        assertEquals(2, tx.stream(Graql.match(x.isa("fake-type"))).count());

        tx.execute(Graql.match(x.isa("fake-type"), y.isa("fake-type"), x.not(y.var())).delete());

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(VARIABLE_OUT_OF_SCOPE.getMessage(y.var()));
        tx.execute(Graql.match(x.isa("movie")).delete(y.var()));
    }

    @Test
    public void whenDeletingASchemaConcept_Throw() {
        SchemaConcept newType = tx.execute(Graql.define(x.type("new-type").sub(ENTITY))).get(0).get(x.var()).asSchemaConcept();

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.deleteSchemaConcept(newType).getMessage());
        tx.execute(Graql.match(x.type("new-type")).delete(x.var()));
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        tx.execute(Graql.match(var()).delete((String) null));
    }

    @Test
    public void whenSortVarIsNotInQuery_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(VARIABLE_OUT_OF_SCOPE.getMessage(new Variable("z")));
        tx.execute(Graql.match(var("x").isa("movie").has("title", var("y"))).get().sort("z"));
    }


    @Test
    public void whenLimitingDelete_CorrectNumberAreDeleted() {
        Session session = graknServer.sessionWithNewKeyspace();
        // load some schema and data
        try (Transaction tx = session.writeTransaction()) {
            tx.execute(Graql.parse("define person sub entity;").asDefine());
            tx.execute(Graql.parse("insert $x isa person; $y isa person; $z isa person; $a isa person; $b isa person;").asInsert());
            tx.commit();
        }

        // try and delete two of the five
        try (Transaction tx = session.writeTransaction()) {
            List<ConceptMap> toDelete = tx.execute(Graql.parse("match $x isa person; get; limit 2;").asGet());
            List<Void> deleted = tx.execute(Graql.parse("match $x isa person; delete $x; limit 2;").asDelete());
            long conceptsDeleted = toDelete.stream().filter(conceptMap -> conceptMap.get("x").isDeleted()).count();
            assertEquals(2, conceptsDeleted);

            List<Numeric> count = tx.execute(Graql.parse("match $x isa person; get $x; count;").asGetAggregate());
            assertEquals(3, count.get(0).number().intValue());
        }

        session.close()
        ;
    }
}
