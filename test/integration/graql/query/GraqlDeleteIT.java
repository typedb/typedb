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
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
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
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.stream.Collectors;

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
        tx = session.transaction(Transaction.Type.WRITE);

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

    // TODO: migrate
    @Test
    public void testDeleteMultiple() {
        tx.execute(Graql.define(type("fake-type").sub(ENTITY)));
        tx.execute(Graql.insert(x.isa("fake-type"), y.isa("fake-type")));

        assertEquals(2, tx.stream(Graql.match(x.isa("fake-type"))).count());

        tx.execute(Graql.match(x.isa("fake-type")).delete(x.isa("fake-type")));

        assertNotExists(tx, var().isa("fake-type"));
    }

    @Ignore // TODO update now that we removed implicit attrs from higher level code, then migrate if applicable
    @Test
    public void whenDeletingAResource_TheResourceAndOwnershipsAreDeleted() {
        ConceptId id = tx.stream(Graql.match(
                x.has("title", var("y")),
                var("attr").value("Godfather")
        ).get()).map(ans -> ans.get("attr")).findFirst().get().id();

        assertExists(tx, var().has("title", "Godfather"));
        assertExists(tx, var().id(id.getValue()));
        assertExists(tx, var().val(1000L).isa("tmdb-vote-count"));

        tx.execute(Graql.match(x.id(id.getValue())).delete(x.isa("attribute")));

        assertNotExists(tx, var().has("title", "Godfather"));
        assertNotExists(tx, var().id(id.getValue()));
    }

    // TODO: migrate
    @Test
    public void deleteRelation_AttributeOwnershipsDeleted() {
        Session session = graknServer.sessionWithNewKeyspace();
        Transaction tx = session.transaction(Transaction.Type.WRITE);

        Role author = tx.putRole("author");
        Role work = tx.putRole("work");
        RelationType authoredBy = tx.putRelationType("authored-by").relates(author).relates(work);
        EntityType person = tx.putEntityType("person").plays(author);
        EntityType production = tx.putEntityType("production").plays(work);
        AttributeType<String> provenance = tx.putAttributeType("provenance", AttributeType.ValueType.STRING);
        authoredBy.has(provenance);

        Entity aPerson = person.create();
        Relation aRelation = authoredBy.create();
        aRelation.assign(author, aPerson);
        Attribute<String> aProvenance = provenance.create("hello");
        aRelation.has(aProvenance);
        ConceptId relationId = aRelation.id();

        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        aProvenance = tx.getAttributesByValue("hello").iterator().next();
        assertTrue(aProvenance.type().label().toString().equals("provenance"));
        assertEquals(aProvenance.owners().count(), 1);

        aRelation = tx.getConcept(relationId);
        aRelation.delete();
        assertTrue(aRelation.isDeleted());

        assertEquals(aProvenance.owners().collect(Collectors.toList()).size(), 0);
    }

    // TODO: migrate
    @Test
    public void deleteLastRolePlayer_RelationAndAttrOwnershipIsRemoved() {
        Session session = graknServer.sessionWithNewKeyspace();
        Transaction tx = session.transaction(Transaction.Type.WRITE);

        Role author = tx.putRole("author");
        Role work = tx.putRole("work");
        RelationType authoredBy = tx.putRelationType("authored-by").relates(author).relates(work);
        EntityType person = tx.putEntityType("person").plays(author);
        EntityType production = tx.putEntityType("production").plays(work);
        AttributeType<String> provenance = tx.putAttributeType("provenance", AttributeType.ValueType.STRING);
        authoredBy.has(provenance);

        Entity aPerson = person.create();
        ConceptId personId = aPerson.id();
        Relation aRelation = authoredBy.create();
        aRelation.assign(author, aPerson);
        Attribute<String> aProvenance = provenance.create("hello");
        aRelation.has(aProvenance);
        ConceptId relationId = aRelation.id();

        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        aProvenance = tx.getAttributesByValue("hello").iterator().next();
        assertTrue(aProvenance.type().label().toString().equals("provenance"));
        assertEquals(aProvenance.owners().count(), 1);

        aRelation = tx.getConcept(relationId);
        tx.getConcept(personId).delete();
        assertTrue(aRelation.isDeleted());

        assertEquals(aProvenance.owners().collect(Collectors.toList()).size(), 0);
    }

    // TODO: migrate without error message
    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(UNBOUND_DELETE_VARIABLE.getMessage(y.var()));
        tx.execute(Graql.match(x.isa("movie")).delete(y.isa("thing")));
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        tx.execute(Graql.match(var()).delete((Statement) null));
    }

    // TODO: migrate (should specifically test $x has "attribute" $a; delete $x has "attribute" $a)
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

    // TODO: migrate (one test for 'has title, delete has attribute'; one test for using 'thing' throws)
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

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("Cannot delete attribute ownership, concept [$a]");
        exception.expectMessage("is not of required attribute type [thing]");
        tx.execute(Graql.parse("match $x id " + ownerId + "; $x has title $a; $a id " + attrId + "; delete $x has thing $a;").asDelete());
    }

    // TODO: migrate
    @Test
    public void deleteWithDirectType_ThrowsWhenNotDirectType() {
        List<ConceptMap> answers = tx.execute(Graql.parse("match $m isa movie, has title \"Godfather\"; get;").asGet());
        assertEquals(1, answers.size());
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("it is not of the required direct type [entity]");
        tx.execute(Graql.parse("match $m isa movie, has title \"Godfather\"; delete $m isa! entity;").asDelete());
    }

    // TODO: migrate
    /*
    Even when a $role variable matches multiple roles (will always match `role` unless constrained)
    We only delete role player edges until the `match` is no longer satisfied

    For example
    ```match $r ($role1: $x, director: $y) isa directed-by; // concrete instance matches: $r (production: $x, director: $y) isa directed-by;
    delete $r ($role1: $x);```
    We will match `$role1` = ROLE meta type. Using this first answer we will remove $x from $r via the `production role`.
    This means the match clause is no longer satisfiable, and should throw the next (identical, up to role type) answer that is matched.

    So, if the user does not specify a specific-enough roles, we may throw.
     */
    @Test
    public void whenDeletingRolePlayersWithVariableRoles_throwsIfMatchingMultipleTimes() {
        List<ConceptMap> answers0 = tx.execute(Graql.parse("match $r ($role1: $x, director: $y) isa directed-by; get;").asGet());
        // this should match 3 roles for the same role-playing: 'role', 'work', and 'production-being-directed'
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("it does not play required role (or subtypes of) [work]");
        tx.execute(Graql.parse("match $r ($role1: $x, director: $y) isa directed-by; delete $r ($role1: $x);").asDelete());
    }

    // TODO: migrate
    /*
    Even when a $role variable matches multiple roles (will always match `role` unless constrained)
    We only delete role player edges until the `match` is no longer satisfied.

    **Sometimes this means multiple duplicate role players will be unassigned **

    For example
    ```
    // concrete instance:  $r (production: $x, production: $x, production: $x, director: $y) isa directed-by;
    match $r ($role1: $x, director: $y) isa directed-by; $type sub work;
    delete $r ($role1: $x);```
    First, we will match `$role1` = ROLE meta role. Using this answer we will remove a single $x from $r via the `production`.
    Next, we will match `$role1` = WORK role, and we delete another `production` player. This repeats again for $role=`production`.
    */
    @Test
    public void whenDeletingDuplicateRolePlayersWithVariableRoles_BothDuplicatesRemoved() {
        // update the directed-by to have a duplicate production
        tx.execute(Graql.parse("match $r (production-being-directed: $x, director: $y) isa directed-by; " +
                "insert $r (production-being-directed: $x, production-being-directed: $x);").asInsert());
        List<ConceptMap> answers = tx.execute(Graql.parse("match $r (production-being-directed: $x, production-being-directed: $x, director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        tx.execute(Graql.parse("match $r ($role1: $x, director: $y) isa directed-by; delete $r ($role1: $x);").asDelete());
        answers = tx.execute(Graql.parse("match $r (director: $y) isa directed-by; get;").asGet());
        assertEquals(1, answers.size());
        assertEquals(1, answers.get(0).get("r").asRelation().rolePlayers().count());
    }
}
