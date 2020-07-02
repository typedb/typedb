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

import com.google.common.collect.ImmutableList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.property.IsaProperty;
import graql.lang.query.GraqlInsert;
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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static grakn.core.test.common.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.test.common.GraqlTestUtil.assertExists;
import static grakn.core.test.common.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.NO_PATTERNS;
import static org.hamcrest.Matchers.isOneOf;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "Duplicates"})
public class GraqlInsertIT {

    private static final Statement w = var("w");
    private static final Statement x = var("x");
    private static final Statement y = var("y");
    private static final Statement z = var("z");

    private static final String title = "title";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();

    public static Session session;

    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
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
    public void testMatchInsertShouldInsertDataEvenWhenResultsAreNotCollected() {
        Statement language1 = var("x").isa("language").has("name", "123");
        Statement language2 = var("x").isa("language").has("name", "456");

        tx.execute(Graql.insert(language1, language2));
        assertExists(tx, language1);
        assertExists(tx, language2);

        GraqlInsert query = Graql.match(var("x").isa("language")).insert(var("x").has("name", "HELLO"));
        tx.stream(query);

        assertExists(tx, var("x").isa("language").has("name", "123").has("name", "HELLO"));
        assertExists(tx, var("x").isa("language").has("name", "456").has("name", "HELLO"));
    }

    @Test
    public void whenInsertingAResourceWithMultipleValues_Throw() {
        Statement varPattern = var().val("123").val("456").isa("title");

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(isOneOf(
                GraqlSemanticException.insertMultipleProperties(varPattern, "", "123", "456").getMessage(),
                GraqlSemanticException.insertMultipleProperties(varPattern, "", "456", "123").getMessage()
        ));

        tx.execute(Graql.insert(varPattern));
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").has("title", "WOW A TITLE").isa("movie").isa("movie"));
    }

    @Test
    public void whenInsertingAnInstanceWithALabel_Throw() {
        exception.expect(IllegalArgumentException.class);
        tx.execute(Graql.insert(type("abc").isa("movie")));
    }

    @Test
    public void whenInsertingAResourceWithALabel_Throw() {
        exception.expect(IllegalArgumentException.class);
        tx.execute(Graql.insert(type("bobby").val("bob").isa("name")));
    }

    @Test(expected = Exception.class)
    public void matchInsertNullVar() {
        tx.execute(Graql.match(var("x").isa("movie")).insert((Statement) null));
    }

    @Test(expected = Exception.class)
    public void matchInsertNullCollection() {
        tx.execute(Graql.match(var("x").isa("movie")).insert((Collection<? extends Statement>) null));
    }

    @Test
    public void whenMatchInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        tx.execute(Graql.match(var()).insert(Collections.EMPTY_SET));
    }

    @Test(expected = Exception.class)
    public void insertNullVar() {
        tx.execute(Graql.insert((Statement) null));
    }

    @Test(expected = Exception.class)
    public void insertNullCollection() {
        tx.execute(Graql.insert((Collection<? extends Statement>) null));
    }

    @Test
    public void whenInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        tx.execute(Graql.insert(Collections.EMPTY_SET));
    }

    @Test
    public void whenSettingTwoTypes_Throw() {
        EntityType movie = tx.getEntityType("movie");
        EntityType person = tx.getEntityType("person");

        // We have to construct it this way because you can't have two `isa`s normally
        // TODO: less bad way?
        Statement varPattern = Statement.create(
                new Variable("x"),
                new LinkedHashSet<>(ImmutableList.of(new IsaProperty(type("movie")), new IsaProperty(type("person"))))
        );

        // We don't know in what order the message will be
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(isOneOf(
                GraqlSemanticException.insertMultipleProperties(varPattern, "isa", movie, person).getMessage(),
                GraqlSemanticException.insertMultipleProperties(varPattern, "isa", person, movie).getMessage()
        ));

        tx.execute(Graql.insert(var("x").isa("movie"), var("x").isa("person")));
    }

    @Test
    public void whenInsertingASchemaConcept_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.insertUnsupportedProperty(Graql.Token.Property.SUB.toString()).getMessage());

        tx.execute(Graql.insert(type("new-type").sub(Graql.Token.Type.ENTITY)));
    }

    @Test
    public void whenModifyingASchemaConceptInAnInsertQuery_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.insertUnsupportedProperty(Graql.Token.Property.PLAYS.toString()).getMessage());

        tx.execute(Graql.insert(type("movie").plays("actor", "casting")));
    }


    // ------ match-insert the same resource/extending resources tests
    @Test
    public void whenMatchInsertingExistingConcept_weDoNoOp() {
        Statement matchStatement = var("x").isa("movie");
        Statement insertStatement = var("x");

        List<ConceptMap> before = tx.execute(Graql.match(matchStatement));
        tx.execute(Graql.match(matchStatement).insert(insertStatement));
        assertCollectionsNonTriviallyEqual(before, tx.execute(Graql.match(matchStatement)));
    }

    private void assertInsert(Statement... vars) {
        // Make sure vars don't exist
        for (Statement var : vars) {
            assertNotExists(tx, var);
        }

        // Insert all vars
        ConceptMap answer = tx.execute(Graql.insert(vars)).get(0);

        // Make sure all vars exist
        for (Statement var : vars) {
            assertExists(tx, var);
        }

        for (Statement statement: vars) {
            tx.execute(Graql.match(statement).delete(Graql.var(statement.var()).isa("thing")));
        }

        // Make sure vars don't exist
        for (Statement var : vars) {
            assertNotExists(tx, var);
        }
    }
}
