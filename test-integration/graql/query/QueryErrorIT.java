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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.INVALID_VALUE;
import static grakn.core.common.exception.ErrorMessage.NO_PATTERNS;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class QueryErrorIT {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    private static SessionImpl session;
    private TransactionOLTP tx;

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
    public void testErrorNonExistentConceptType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("film");
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("film")));
    }

    @Test
    public void testErrorNotARole() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("role"), containsString("person"), containsString("isa person")));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("movie"), var().rel("person", "y").rel("x")));
    }

    @Test
    public void testErrorNonExistentResourceType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("thingy");
        tx.execute(Graql.match(var("x").has("thingy", "value")).delete("x"));
    }

    @Test
    public void whenMatchingWildcardHas_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.noLabelSpecifiedForHas(var("x")).getMessage());
        tx.execute(Graql.match(label("thing").has(var("x"))).get());
    }

    @Test
    public void whenMatchingHasWithNonExistentType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.labelNotFound(Label.of("heffalump")).getMessage());
        tx.execute(Graql.match(var("x").has("heffalump", "foo")).get());
    }

    @Test
    public void testErrorNotARelation() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("movie"), containsString("separate"), containsString(";")));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var().isa("movie").rel("x").rel("y")));
    }

    @Test
    public void testErrorInvalidNonExistentRole() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.NOT_A_ROLE_TYPE.getMessage("character-in-production", "character-in-production"));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var().isa("has-cast").rel("character-in-production", "x")));
    }

    @Test
    public void testErrorMultipleIsa() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("isa"), containsString("person"), containsString("has-cast")
        ));
        //noinspection ResultOfMethodCallIgnored
        Graql.match(var("abc").isa("person").isa("has-cast"));
    }

    @Test
    public void whenSpecifyingMultipleSubs_ThrowIncludingInformationAboutTheConceptAndBothSupers() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("sub"), containsString("person"), containsString("has-cast")
        ));
        tx.execute(Graql.define(label("abc").sub("person"), label("abc").sub("has-cast")));
    }

    @Test
    public void testErrorHasGenreQuery() {
        // 'has genre' is not allowed because genre is an entity type
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage("genre"));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("movie").has("genre", "Drama")));
    }

    @Test
    public void testExceptionWhenNoPatternsProvided() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        //noinspection ResultOfMethodCallIgnored
        Graql.match();
    }

    @Test
    public void testExceptionWhenNullValue() {
        exception.expect(NullPointerException.class);
        //noinspection ResultOfMethodCallIgnored
        var("x").val(null);
    }

    @Test
    public void testExceptionWhenNoHasResourceRelation() throws InvalidKBException {
        // Create a fresh graph, with no has between person and name
        Session newSession = graknServer.sessionWithNewKeyspace();
        try (Transaction newTx = newSession.transaction(Transaction.Type.WRITE)) {
            newTx.execute(Graql.define(
                    label("person").sub("entity"),
                    label("name").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.STRING)
            ));

            exception.expect(TransactionException.class);
            exception.expectMessage(allOf(
                    containsString("person"),
                    containsString("name")
            ));
            newTx.execute(Graql.insert(var().isa("person").has("name", "Bob")));
        }
    }

    @Test
    public void testExceptionInstanceOfRoleType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.cannotGetInstancesOfNonType(Label.of("actor")).getMessage());
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("actor")));
    }

    @Test
    public void testExceptionInstanceOfRule() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.cannotGetInstancesOfNonType(Label.of("rule")).getMessage());
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("rule")));
    }

    @Test
    public void testAdditionalSemicolon() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("plays product-type")));
        tx.execute(Graql.<DefineQuery>parse(
                "define " +
                        "tag-group sub role; product-type sub role;" +
                        "category sub entity, plays tag-group; plays product-type;"
        ));
    }

    @Test
    public void testGetNonExistentVariable() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.VARIABLE_NOT_IN_QUERY.getMessage(var("y")));

        MatchClause match = Graql.match(var("x").isa("movie"));
        Stream<Concept> concepts = tx.stream(match.get("y")).map(ans -> ans.get("y"));
    }

    @Test
    public void whenUsingInvalidResourceValue_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(INVALID_VALUE.getMessage(tx.getClass()));
        Graql.match(var("x").val(tx));
    }

    @Test
    public void whenTryingToSetExistingInstanceType_Throw() {
        Thing movie = tx.getEntityType("movie").instances().iterator().next();
        Type person = tx.getEntityType("person");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(containsString("person"));

        tx.execute(Graql.match(var("x").id(movie.id().getValue())).insert(var("x").isa(label(person.label()))));
    }
}
