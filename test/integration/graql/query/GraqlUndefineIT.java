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
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlUndefine;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class GraqlUndefineIT {
    private static final Statement THING = type(Graql.Token.Type.THING);
    private static final Statement ENTITY = type(Graql.Token.Type.ENTITY);
    private static final Statement RELATION = type(Graql.Token.Type.RELATION);
    private static final Statement ATTRIBUTE = type(Graql.Token.Type.ATTRIBUTE);
    private static final Statement ROLE = type(Graql.Token.Type.ROLE);
    private static final Label NEW_TYPE = Label.of("new-type");
    private static final Statement x = var("x");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();

    public static Session session;
    private Transaction tx;

    @Before
    public void newTransaction() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void closeTransaction() {
        tx.close();
        session.close();
    }

    @Test @Ignore // TODO: investigate how this is possible in the first place
    public void whenUndefiningById_TheSchemaConceptIsDeleted() {
        Type newType = tx.execute(Graql.define(x.type(NEW_TYPE.getValue()).sub(ENTITY))).get(0).get(x.var()).asType();
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(var().id(newType.id().getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningAnInstanceProperty_Throw() {
        Concept movie = tx.execute(Graql.insert(x.isa("movie"))).get(0).get(x.var());

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.ISA.toString()).getMessage());

        tx.execute(Graql.undefine(var().id(movie.id().getValue()).isa("movie")));
    }
}
