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
import grakn.core.common.exception.GraknException;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDefine;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class GraqlDefineIT {

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
    public void whenDefiningARuleUsingParsedPatterns_ruleIsPersistedCorrectly() {
        Pattern when = Graql.parsePattern("$x isa entity;");
        Pattern then = Graql.parsePattern("$x isa entity;");
        Statement rule = type("my-rule").sub(Graql.Token.Type.RULE).when(when).then(then);
        tx.execute(Graql.define(rule));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void whenDefiningARuleUsingCoreAPI_ruleIsPersistedCorrectly(){
        tx.execute(Graql.define(type("good-movie").sub("movie")));
        GraqlDefine ruleDefinition = Graql.define(
                type("high-average-movies-are-good").sub("rule")
                        .when(
                                var("m").isa("movie").has("tmdb-vote-average", Graql.gte(7.5)))
                        .then(
                                var("m").isa("good-movie")
                )
        );
        tx.execute(ruleDefinition);
    }

    @Test
    public void whenDefiningAnOntologyConceptWithoutALabel_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("entity"), containsString("type")));
        tx.execute(Graql.define(var().sub("entity")));
    }

    @Test
    public void whenDefiningAThing_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.ISA.toString()).getMessage());

        tx.execute(Graql.define(var("x").isa("movie")));
    }

    @Test
    public void whenCallingToStringOfDefineQuery_ReturnCorrectRepresentation() {
        String queryString = "define my-entity sub entity;";
        GraqlDefine defineQuery = Graql.parse(queryString).asDefine();
        assertEquals(queryString, defineQuery.toString());
    }
}
