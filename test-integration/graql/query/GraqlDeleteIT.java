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

import com.google.common.collect.Iterators;
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
import grakn.core.kb.graql.exception.GraqlQueryException;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.UNBOUND_DELETE_VARIABLE;
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
    public void testDeleteMultiple() {
        tx.execute(Graql.define(type("fake-type").sub(ENTITY)));
        tx.execute(Graql.insert(x.isa("fake-type"), y.isa("fake-type")));

        assertEquals(2, tx.stream(Graql.match(x.isa("fake-type"))).count());

        tx.execute(Graql.match(x.isa("fake-type")).delete(x.isa("fake-type")));

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Test
    public void testDeleteEntity() {

        assertExists(tx, var().has("title", "Godfather"));
        assertExists(tx, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(tx, var().has("name", "Don Vito Corleone"));

        tx.execute(Graql.match(x.has("title", "Godfather")).delete(x.isa("thing")));

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

        tx.execute(kurtzCastRelation.delete(var("a").isa("thing")));

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

        tx.execute(kurtz.delete(x.isa("thing")));

        assertNotExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertTrue(checkIdExists(tx, id));

        tx.execute(marlonBrando.delete(x.isa("thing")));

        assertNotExists(tx, kurtz);
        assertNotExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertTrue(checkIdExists(tx, id));

        tx.execute(apocalypseNow.delete(x.isa("thing")));

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

        tx.execute(Graql.match(x.val(1000L).isa("tmdb-vote-count")).delete(x.isa("tmdb-vote-count")));

        assertExists(tx, var().has("title", "Godfather"));
        assertNotExists(tx, var().id(id.getValue()));
        assertNotExists(tx, var().val(1000L).isa("tmdb-vote-count"));
    }


    @Test
    public void whenDeletingAResourceOwnerAndImplicitRelation_NoErrorIsThrown() {
        /*
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
        List<Statement> deletePatterns = new ArrayList<>();
        for (int i = 0; i < insertedIds.size(); i++) {
            StatementThing id = var("v" + i).id(insertedIds.get(i).toString());
            Statement delete = var("v" + i).isa("thing");
            idPatterns.add(id);
            deletePatterns.add(delete);
        }

        System.out.println("Ids: " + idPatterns);
        List<ConceptMap> answersById = tx.execute(Graql.match(idPatterns).get());
        assertEquals(answersById.size(), 1);

        for (int i = 0; i < idPatterns.size(); i++) {
            tx.execute(Graql.match(idPatterns.get(i)).delete(deletePatterns.get(i)));
        }

        // TODO re-enable this batch delete once implicit attribute relations are removed!
//        tx.execute(Graql.match(idPatterns).delete(deletePatterns));
        tx.commit();

    }

    /*
    TODO re-enable test when Hypergraph backend is integrated
    This currently fails because we can propagate a deletion and prompt a clean up of a concept (eg. role players
    are deleted by ID, prompting clean up of relation, whose ID is no longer valid)
    While it is still in the `match` stream. This is resolved if we can reflect deletions/changes in the input stream
    as we're writing at the same time.
     */
    @Ignore
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
        List<Statement> deletePatterns = new ArrayList<>();
        for (int i = 0; i < insertedIds.size(); i++) {
            StatementThing id = var("v" + i).id(insertedIds.get(i).toString());
            Statement delete = var("v" + i).isa("thing");
            idPatterns.add(id);
            deletePatterns.add(delete);
        }
        // clean up, delete the IDs we inserted for this test
        tx.execute(Graql.match(idPatterns).delete(deletePatterns));
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

        tx.execute(movie.delete(x.isa("movie")));

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

        tx.execute(Graql.match(x.isa("fake-type"), y.isa("fake-type"), x.not(y.var())).delete(x.isa("fake-type"), y.isa("fake-type")));

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingWithNoArguments_AllVariablesGetDeleted() {
        tx.execute(Graql.define(type("fake-type").sub(Graql.Token.Type.ENTITY)));
        tx.execute(Graql.insert(x.isa("fake-type"), y.isa("fake-type")));

        assertEquals(2, tx.stream(Graql.match(x.isa("fake-type"))).count());

        tx.execute(Graql.match(x.isa("fake-type"), y.isa("fake-type"), x.not(y.var()))
                .delete(var("x").isa("fake-type"), var("y").isa("fake-type")));

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(UNBOUND_DELETE_VARIABLE.getMessage(y.var()));
        tx.execute(Graql.match(x.isa("movie")).delete(y.isa("thing")));
    }

    @Test
    public void whenDeletingASchemaConcept_Throw() {
        SchemaConcept newType = tx.execute(Graql.define(x.type("new-type").sub(ENTITY))).get(0).get(x.var()).asSchemaConcept();
        exception.expect(GraqlQueryException.class);
//        exception.expectMessage(GraqlSemanticException.deleteSchemaConcept(newType).getMessage());
        // TODO error message, better reporting for deleting schema concepts
        tx.execute(Graql.match(x.type("new-type")).delete(x.isa("thing")));
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        tx.execute(Graql.match(var()).delete((Statement) null));
    }


    @Test
    public void whenTypeDoesNotMatch_Throw() {
        tx.execute(Graql.insert(var().isa("production")));

        exception.expect(GraqlQueryException.class);
//        exception.expectMessage("Type does not match");
        // TODO error message
        tx.execute(Graql.match(var("x").isa("production")).delete(var("x").isa("movie")));
    }


    @Test
    public void whenDeletingAttributeOwnership_onlyOwnershipIsDeleted() {
        List<ConceptMap> answers = tx.execute(Graql.parse("match $x isa thing, has attribute $a; get; limit 1;").asGet());
        ConceptId ownerId = answers.get(0).get("x").id();
        ConceptId attrId = answers.get(0).get("a").id();

        tx.execute(Graql.parse("match $x id " + ownerId + "; $x has attribute $a; $a id " + attrId + "; delete $x has attribute $a;").asDelete());

        assertExists(tx, var().id(ownerId.toString()));
        assertExists(tx, var().id(attrId.toString()));
        assertNotExists(tx, Graql.and(var().id(ownerId.toString()).has("attribute", "a"), var("a").id(attrId.toString())));
    }

    @Test
    public void whenDeletingAttributeOwnershipWithSupertype_onlyOwnershipIsDeleted() {
        // using `attribute`
        List<ConceptMap> answers = tx.execute(Graql.parse("match $x isa thing, has title $a; get; limit 1;").asGet());
        ConceptId ownerId = answers.get(0).get("x").id();
        ConceptId attrId = answers.get(0).get("a").id();

        tx.execute(Graql.parse("match $x id " + ownerId + "; $x has title $a; $a id " + attrId + "; delete $x has attribute $a;").asDelete());

        assertExists(tx, var().id(ownerId.toString()));
        assertExists(tx, var().id(attrId.toString()));
        assertNotExists(tx, Graql.and(var().id(ownerId.toString()).has("title", "a"), var("a").id(attrId.toString())));

        // using `thing` throws
        answers = tx.execute(Graql.parse("match $x isa thing, has title $a; get; limit 1;").asGet());
        ownerId = answers.get(0).get("x").id();
        attrId = answers.get(0).get("a").id();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot delete attribute ownership, concept [$a]");
        exception.expectMessage("is not of required attribute type [thing]");
        tx.execute(Graql.parse("match $x id " + ownerId + "; $x has title $a; $a id " + attrId + "; delete $x has thing $a;").asDelete());
    }

    @Test
    public void whenDeletingAttributeOwnershipWithSubtype_Throw() {
        AttributeType<String> firstName = tx.putAttributeType("first-name", AttributeType.DataType.STRING);
        firstName.sup(tx.getAttributeType("name"));
        tx.getEntityType("person").has(firstName);

        List<ConceptMap> answers = tx.execute(Graql.parse("match $x isa person, has name $n; get;").asGet());
        assertEquals(10, answers.size());
        tx.execute(Graql.parse("insert $x isa person, has first-name \"john\";").asInsert());

        answers = tx.execute(Graql.parse("match $x isa person, has name $n; get;").asGet());
        assertEquals(11, answers.size());

        // `first-name` will not be satisfied by all `name` attributes
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot delete attribute ownership, concept [$n]");
        exception.expectMessage("is not of required attribute type [first-name]");
        tx.execute(Graql.parse("match $x isa person, has name $n; delete $x has first-name $n;").asDelete());
    }


    @Test
    public void matchInstance_DeleteWithSupertype() {
        List<ConceptMap> answers = tx.execute(Graql.parse("match $m isa movie, has title \"Godfather\"; get;").asGet());
        assertEquals(1, answers.size());
        tx.execute(Graql.parse("match $m isa movie, has title \"Godfather\"; delete $m isa entity;").asDelete());
        answers = tx.execute(Graql.parse("match $m isa movie, has title \"Godfather\"; get;").asGet());
        assertEquals(0, answers.size());

        answers = tx.execute(Graql.parse("match $m isa movie, has title \"The Muppets\"; get;").asGet());
        assertEquals(1, answers.size());
        tx.execute(Graql.parse("match $m isa movie, has title \"The Muppets\"; delete $m isa thing;").asDelete());
        answers = tx.execute(Graql.parse("match $m isa movie, has title \"The Muppets\"; get;").asGet());
        assertEquals(0, answers.size());
    }


    @Ignore //TODO re-enable when Hypergraph Backend is used
    @Test
    public void whenDeletingPartOfMatchQuery_UpdateIsReflected() {
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r ($x, $y) isa has-genre; get;").asGet());
        assertEquals(34, answers.size()); // 17 x 2
        // delete one of the two role players arbitrarily. This then leads to only 1 role player which doesn't match anymore
        // so we shouldn't end up deleting the second RP too
        tx.execute(Graql.parse("match $r ($x, $y) isa has-genre; delete $r (role: $x);").asDelete());
        answers = tx.execute(Graql.parse("match $r ($x) isa has-genre; get;").asGet());
        assertEquals(17, answers.size());
        answers.forEach(conceptMap -> {
            assertEquals(1, conceptMap.get("r").asRelation().rolePlayers().count());
        });
    }

    @Test
    public void whenDeletingSingleRolePlayer_RelationSurvives() {
        tx.execute(Graql.parse("match $r (production-being-directed: $x, director: $y) isa directed-by; delete $r (production-being-directed: $x);").asDelete());
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r (director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        assertEquals(1, answers.get(0).get("r").asRelation().rolePlayers().count());
    }

    @Test
    public void whenDeletingRolePlayerIndividually_RelationIsDeleted() {
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        tx.execute(Graql.parse("match $r ($x) isa directed-by; delete $r (role: $x);").asDelete());
        answers = tx.execute(Graql.parse("match $r isa directed-by; get;").asGet());
        assertEquals(0, answers.size());
    }

    @Test
    public void whenDeletingSomeDuplicateRolePlayers_SomeAreDeleted() {
        tx.execute(Graql.parse("define reflexive sub relation, relates refl; athing sub entity, plays refl;").asDefine());
        tx.execute(Graql.insert(
                var("r")
                        .isa("reflexive")
                        .rel("refl", "p")
                        .rel("refl", "p")
                        .rel("refl", "p"),
                var("p").isa("athing"))
        );

        List<ConceptMap> answers = tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x").rel("refl", "x")).get());
        assertEquals(1, answers.size());
        assertEquals(3, Iterators.getOnlyElement(answers.get(0).get("r").asRelation().rolePlayersMap().values().iterator()).size());

        tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x").rel("refl", "x"))
                .delete(var("r").rel("refl", "x").rel("refl", "x")));

        answers = tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x")).get());
        assertEquals(1, answers.size());
        assertEquals(1, Iterators.getOnlyElement(answers.get(0).get("r").asRelation().rolePlayersMap().values().iterator()).size());
    }

    @Test
    public void whenDeletingTooManyDuplicateRolePlayers_Throw() {
        tx.execute(Graql.parse("define reflexive sub relation, relates refl; athing sub entity, plays refl;").asDefine());
        tx.execute(Graql.insert(
                var("r")
                        .isa("reflexive")
                        .rel("refl", "p"),
                var("p").isa("athing"))
        );

        List<ConceptMap> answers = tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x")).get());
        assertEquals(1, answers.size());
        assertEquals(1, Iterators.getOnlyElement(answers.get(0).get("r").asRelation().rolePlayersMap().values().iterator()).size());

        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot delete role player [$x]");
        exception.expectMessage("it does not play required role (or subtypes of) [refl]");
        // match it once, delete it as a duplicate but we only have a single player! Should throw
        tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x"))
                .delete(var("r").rel("refl", "x").rel("refl", "x")));
    }

    /*
    We allow the following generalised role in the delete clause:
    match $r (sub-role: $x) isa relation; delete $r (super-role: $x);
     */
    @Test
    public void whenDeletingRolePlayerAsSuperRole_PlayerIsRemoved() {
        tx.execute(Graql.parse("match $r (production-being-directed: $x, director: $y) isa directed-by; delete $r (role: $x);").asDelete());
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r (director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        assertEquals(1, answers.get(0).get("r").asRelation().rolePlayers().count());
    }

    /*
    We don't allow the following downcasted role in the delete clause:
    match $r (super-role: $x) isa relation; delete $r (sub-role: $x);
     */
    @Test
    public void whenDeletingRolePlayerAsSubRole_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot delete role player [$x]");
        exception.expectMessage("it does not play required role (or subtypes of) [production-being-directed]");
        tx.execute(Graql.parse("match $r (role: $x) isa directed-by; delete $r (production-being-directed: $x);").asDelete());
    }

    /*
    Even when a $role variable matches multiple roles (will always match `role` unless constrained)
    We only delete role player edges until the `match` is no longer satisfied

    For example
    ```match $r ($role1: $x, director: $y) isa directed-by; // concrete instance matches: $r (production: $x, director: $y) isa directed-by;
    delete $r ($role1: $x);```
    We will match `$role1` = ROLE meta type. Using this first answer we will remove $x from $r via the `production role`.
    This means the match clause is no longer satisfiable, and no error should be thrown!

    This test should pass in with Grakn 2.0's new backend
     */
    @Ignore // TODO re-enable with Hypergraph backend
    @Test
    public void whenDeletingRolePlayersWithVariableRoles_RemoveRolePlayer() {
        List<ConceptMap> answers0 = tx.execute(Graql.parse("match $r ($role1: $x, director: $y) isa directed-by; get;").asGet());
        for (ConceptMap answer : answers0) {
            System.out.println(answer.get("role1"));
        }
        tx.execute(Graql.parse("match $r ($role1: $x, director: $y) isa directed-by; delete $r ($role1: $x);").asDelete());
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r (director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        assertEquals(1, answers.get(0).get("r").asRelation().rolePlayers().count());
    }

    /*
    Even when a $role variable matches multiple roles (will always match `role` unless constrained)
    We only delete role player edges until the `match` is no longer satisfied.

    **Sometimes this means multiple duplicate role players will be unassigned **

    For example
    ```
    // concrete instance:  $r (production: $x, production: $x, director: $y) isa directed-by;
    match $r ($role1: $x, director: $y) isa directed-by;
    delete $r ($role1: $x);```
    First, we will match `$role1` = ROLE meta type. Using this answer we will remove a single $x from $r via the `production`.
    Next, we will match `$role1` = WORK role. Using this answer we will remove a the second $x from $r via the `production` role.
    Now, the match clause is not satisfiable anymore.

    This test should pass in with Grakn 2.0's new backend

    */
    @Ignore // TODO re-enable with Hypergraph backend
    @Test
    public void whenDeletingDuplicateRolePlayersWithVariableRoles_BothDuplicatesRemoved() {
        // update the directed-by to have a duplicate production
        tx.execute(Graql.parse("match $r (production-being-directed: $x, director: $y) isa directed-by; " +
                "insert $r (production-being-directed: $x);").asInsert());
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r (production-being-directed: $x, production-being-directed: $x, director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        tx.execute(Graql.parse("match $r ($role1: $x, director: $y) isa directed-by; delete $r ($role1: $x);").asDelete());
        answers = tx.execute(Graql.parse("match $r (director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        assertEquals(1, answers.get(0).get("r").asRelation().rolePlayers().count());
    }

    @Test
    public void whenDeletingSomeDuplicateInSeparateStatements_SomeAreDeleted() {
        tx.execute(Graql.parse("define reflexive sub relation, relates refl; athing sub entity, plays refl;").asDefine());
        tx.execute(Graql.insert(
                var("r")
                        .isa("reflexive")
                        .rel("refl", "p")
                        .rel("refl", "p")
                        .rel("refl", "p"),
                var("p").isa("athing"))
        );

        List<ConceptMap> answers = tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x").rel("refl", "x")).get());
        assertEquals(1, answers.size());
        assertEquals(3, Iterators.getOnlyElement(answers.get(0).get("r").asRelation().rolePlayersMap().values().iterator()).size());

        tx.execute(Graql.match(
                var("r").isa("reflexive").rel("refl", "x").rel("refl", "x"))
                .delete(
                        var("r").rel("refl", "x"),
                        var("r").rel("refl", "x")
                ));

        answers = tx.execute(Graql.match(var("r").isa("reflexive").rel("refl", "x")).get());
        assertEquals(1, answers.size());
        assertEquals(1, Iterators.getOnlyElement(answers.get(0).get("r").asRelation().rolePlayersMap().values().iterator()).size());
    }

    @Ignore
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
