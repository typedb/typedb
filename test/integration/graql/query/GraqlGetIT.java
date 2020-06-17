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

import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Variable;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.VARIABLE_OUT_OF_SCOPE;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class GraqlGetIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
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
    public void testEmptyMatchThrows() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(Matchers.containsString("at least one property"));
        tx.execute(Graql.match(var()).get());
    }
}
