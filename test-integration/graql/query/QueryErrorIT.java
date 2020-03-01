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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.property.ValueProperty;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.stream.Stream;

import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.NO_PATTERNS;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class QueryErrorIT {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.writeTransaction();
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
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("film");
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("film")));
    }

    @Test
    public void testErrorNotARole() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("role"), containsString("person"), containsString("isa person")));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("movie"), var().rel("person", "y").rel("x")));
    }

    @Test
    public void testErrorNonExistentResourceType() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("thingy");
        tx.execute(Graql.match(var("x").has("thingy", "value")).delete("x"));
    }

    @Test @Ignore // TODO: enable this properly after fixing issue #4664
    public void whenMatchingWildcardHas_Throw() {
        exception.expect(GraqlSemanticException.class);
        tx.execute(Graql.match(type("thing").has(var("x"))).get());
    }

    @Test
    public void whenMatchingHasWithNonExistentType_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.labelNotFound(Label.of("heffalump")).getMessage());
        tx.execute(Graql.match(var("x").has("heffalump", "foo")).get());
    }

    @Test
    public void testErrorNotARelation() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("movie"), containsString("separate"), containsString(";")));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var().isa("movie").rel("x").rel("y")));
    }

    @Test
    public void testErrorInvalidNonExistentRole() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(ErrorMessage.NOT_A_ROLE_TYPE.getMessage("character-in-production", "character-in-production"));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var().isa("has-cast").rel("character-in-production", "x")));
    }

    @Test
    public void testErrorMultipleIsa() {
        exception.expect(GraqlException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("isa"), containsString("person"), containsString("has-cast")
        ));
        //noinspection ResultOfMethodCallIgnored
        Graql.match(var("abc").isa("person").isa("has-cast"));
    }

    @Test
    public void whenSpecifyingMultipleSubs_ThrowIncludingInformationAboutTheConceptAndBothSupers() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("sub"), containsString("person"), containsString("has-cast")
        ));
        tx.execute(Graql.define(type("abc").sub("person"), type("abc").sub("has-cast")));
    }

    @Test
    public void testErrorHasGenreQuery() {
        // 'has genre' is not allowed because genre is an entity type
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage("genre"));
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("movie").has("genre", "Drama")));
    }

    @Test
    public void testExceptionWhenNoPatternsProvided() {
        exception.expect(GraqlException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        //noinspection ResultOfMethodCallIgnored
        Graql.match();
    }

    @Test
    public void testExceptionWhenNullValue() {
        exception.expect(NullPointerException.class);
        //noinspection ResultOfMethodCallIgnored
        ValueProperty property = null;
        Statement s = var("x").attribute(property);
    }

    @Test
    public void testExceptionWhenNoHasResourceRelation() throws InvalidKBException {
        // Create a fresh graph, with no has between person and name
        Session newSession = graknServer.sessionWithNewKeyspace();
        try (Transaction newTx = newSession.writeTransaction()) {
            newTx.execute(Graql.define(
                    type("person").sub("entity"),
                    type("name").sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.STRING)
            ));

            exception.expect(GraknConceptException.class);
            exception.expectMessage(allOf(
                    containsString("person"),
                    containsString("name")
            ));
            newTx.execute(Graql.insert(var().isa("person").has("name", "Bob")));
        }
    }

    @Test
    public void testExceptionInstanceOfRoleType() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.cannotGetInstancesOfNonType(Label.of("actor")).getMessage());
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("actor")));
    }

    @Test
    public void testExceptionInstanceOfRule() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.cannotGetInstancesOfNonType(Label.of("rule")).getMessage());
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(var("x").isa("rule")));
    }

    @Test
    public void testAdditionalSemicolon() {
        exception.expect(GraqlException.class);
        exception.expectMessage(allOf(containsString("plays product-type")));
        tx.execute(Graql.parse(
                "define " +
                        "tag-group sub role; product-type sub role; " +
                        "category sub entity, plays tag-group; plays product-type;"
        ).asDefine());
    }

    @Test
    public void testGetNonExistentVariable() {
        exception.expect(GraqlException.class);
        exception.expectMessage(graql.lang.exception.ErrorMessage.VARIABLE_OUT_OF_SCOPE.getMessage(new Variable("y")));

        MatchClause match = Graql.match(var("x").isa("movie"));
        Stream<Concept> concepts = tx.stream(match.get("y")).map(ans -> ans.get("y"));
    }

    @Test
    public void whenTryingToSetExistingInstanceType_Throw() {
        Thing movie = tx.getEntityType("movie").instances().iterator().next();
        Type person = tx.getEntityType("person");

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(containsString("person"));

        tx.execute(Graql.match(var("x").id(movie.id().getValue())).insert(var("x").isa(type(person.label().getValue()))));
    }
}
