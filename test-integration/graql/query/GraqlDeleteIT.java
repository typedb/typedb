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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.concept.ConceptId;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.VARIABLE_OUT_OF_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "Duplicates"})
public class GraqlDeleteIT {

    public static final Statement ENTITY = type(Schema.MetaSchema.ENTITY.getLabel().getValue());
    public static final Statement x = var("x");
    public static final Statement y = var("y");

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static SessionImpl session;
    public TransactionOLTP tx;

    @Rule
    public  final ExpectedException exception = ExpectedException.none();

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
    public void closeTransaction(){
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

        Set<ConceptId> toDelete = answers.stream().map(answer -> answer.get("x").id()).collect(Collectors.toSet());

        ConceptSet deleted = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .delete().sort("y")
        ).get(0);

        assertTrue(deleted.set().containsAll(toDelete));
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

        Set<ConceptId> toDelete = answers.stream().map(answer -> answer.get("x").id()).collect(Collectors.toSet());

        ConceptSet deleted = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .delete().sort("y", "asc").limit(3)
        ).get(0);

        assertTrue(deleted.set().containsAll(toDelete));
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

        Set<ConceptId> toDelete = answers.stream().map(answer -> answer.get("x").id()).collect(Collectors.toSet());

        ConceptSet deleted = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .delete().sort("y", "desc").offset(3).limit(4)
        ).get(0);

        assertTrue(deleted.set().containsAll(toDelete));
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
        MatchClause relation = Graql.match(var().id(id.getValue()));

        assertExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertExists(tx, relation);

        tx.execute(kurtz.delete(x.var()));

        assertNotExists(tx, kurtz);
        assertExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertExists(tx, relation);

        tx.execute(marlonBrando.delete(x.var()));

        assertNotExists(tx, kurtz);
        assertNotExists(tx, marlonBrando);
        assertExists(tx, apocalypseNow);
        assertExists(tx, relation);

        tx.execute(apocalypseNow.delete(x.var()));

        assertNotExists(tx, kurtz);
        assertNotExists(tx, marlonBrando);
        assertNotExists(tx, apocalypseNow);
        assertNotExists(tx, relation);
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
        tx.execute(Graql.define(type("fake-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue())));
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

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.deleteSchemaConcept(newType).getMessage());
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
}
