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

import com.google.common.collect.ImmutableList;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
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

import java.util.Collection;

import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
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

public class UndefineQueryIT {

    private static final Statement THING = Pattern.label(Schema.MetaSchema.THING.getLabel());
    private static final Statement ENTITY = Pattern.label(Schema.MetaSchema.ENTITY.getLabel());
    private static final Statement RELATIONSHIP = Pattern.label(Schema.MetaSchema.RELATIONSHIP.getLabel());
    private static final Statement ATTRIBUTE = Pattern.label(Schema.MetaSchema.ATTRIBUTE.getLabel());
    private static final Statement ROLE = Pattern.label(Schema.MetaSchema.ROLE.getLabel());
    private static final Label NEW_TYPE = Label.of("new-type");
    private static final Variable x = var("x");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();

    public static SessionImpl session;
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
    public void whenUndefiningDataType_DoNothing() {
        tx.execute(Graql.undefine(label("name").datatype(Query.DataType.STRING)));

        assertEquals(AttributeType.DataType.STRING, tx.getAttributeType("name").dataType());
    }

    @Test
    public void whenUndefiningHas_TheHasLinkIsDeleted() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).has("name")));

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(label(NEW_TYPE).has("name")));

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningHasWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).has("name")));

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(label(NEW_TYPE).has("title")));

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test
    public void whenUndefiningKey_TheKeyLinkIsDeleted() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).key("name")));

        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(label(NEW_TYPE).key("name")));

        assertThat(tx.getType(NEW_TYPE).keys().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningKeyWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).key("name")));

        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(label(NEW_TYPE).key("title")));

        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test
    public void whenUndefiningById_TheSchemaConceptIsDeleted() {
        Type newType = tx.execute(Graql.define(x.label(NEW_TYPE).sub(ENTITY))).get(0).get(x).asType();

        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(var().id(newType.id().getValue()).sub(ENTITY)));

        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningIsAbstract_TheTypeIsNoLongerAbstract() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).isAbstract()));

        assertTrue(tx.getType(NEW_TYPE).isAbstract());

        tx.execute(Graql.undefine(label(NEW_TYPE).isAbstract()));

        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningIsAbstractOnNonAbstractType_DoNothing() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY)));

        assertFalse(tx.getType(NEW_TYPE).isAbstract());

        tx.execute(Graql.undefine(label(NEW_TYPE).isAbstract()));

        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningPlays_TheTypeNoLongerPlaysTheRole() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).plays("actor")));

        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("actor")));

        tx.execute(Graql.undefine(label(NEW_TYPE).plays("actor")));

        assertThat(tx.getType(NEW_TYPE).playing().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningPlaysWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY).plays("production-with-cast")));

        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("production-with-cast")));

        tx.execute(Graql.undefine(label(NEW_TYPE).plays("actor")));

        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("production-with-cast")));
    }

    @Test
    public void whenUndefiningRegexProperty_TheAttributeTypeHasNoRegex() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ATTRIBUTE).datatype(Query.DataType.STRING).regex("abc")));

        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());

        tx.execute(Graql.undefine(label(NEW_TYPE).regex("abc")));

        assertNull(tx.<AttributeType>getType(NEW_TYPE).regex());
    }

    @Test
    public void whenUndefiningRegexPropertyWithWrongRegex_DoNothing() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ATTRIBUTE).datatype(Query.DataType.STRING).regex("abc")));

        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());

        tx.execute(Graql.undefine(label(NEW_TYPE).regex("xyz")));

        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());
    }

    @Test
    public void whenUndefiningRelatesProperty_TheRelationshipTypeNoLongerRelatesTheRole() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(RELATIONSHIP).relates("actor")));

        assertThat(tx.<RelationType>getType(NEW_TYPE).roles().toArray(), hasItemInArray(tx.getRole("actor")));

        tx.execute(Graql.undefine(label(NEW_TYPE).relates("actor")));

        assertThat(tx.<RelationType>getType(NEW_TYPE).roles().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningSub_TheSchemaConceptIsDeleted() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY)));

        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(label(NEW_TYPE).sub(ENTITY)));

        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningSubWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY)));

        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(label(NEW_TYPE).sub(THING)));

        assertNotNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningComplexSchema_TheEntireSchemaIsRemoved() {
        Collection<Statement> schema = ImmutableList.of(
                label("pokemon").sub(ENTITY).has("pokedex-no").plays("ancestor").plays("descendant"),
                label("pokedex-no").sub(ATTRIBUTE).datatype(Query.DataType.INTEGER),
                label("evolution").sub(RELATIONSHIP).relates("ancestor").relates("descendant"),
                label("ancestor").sub(ROLE),
                label("descendant").sub(ROLE)
        );

        tx.execute(Graql.define(schema));

        EntityType pokemon = tx.getEntityType("pokemon");
        RelationType evolution = tx.getRelationshipType("evolution");
        AttributeType<Long> pokedexNo = tx.getAttributeType("pokedex-no");
        Role ancestor = tx.getRole("ancestor");
        Role descendant = tx.getRole("descendant");

        assertThat(pokemon.attributes().toArray(), arrayContaining(pokedexNo));
        assertThat(evolution.roles().toArray(), arrayContainingInAnyOrder(ancestor, descendant));
        assertThat(pokemon.playing().filter(r -> !r.isImplicit()).toArray(), arrayContainingInAnyOrder(ancestor, descendant));

        tx.execute(Graql.undefine(schema));

        assertNull(tx.getEntityType("pokemon"));
        assertNull(tx.getEntityType("evolution"));
        assertNull(tx.getAttributeType("pokedex-no"));
        assertNull(tx.getRole("ancestor"));
        assertNull(tx.getRole("descendant"));
    }

    @Test
    public void whenUndefiningATypeWithInstances_Throw() {
        assertExists(tx, x.label("movie").sub("entity"));
        assertExists(tx, x.isa("movie"));

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("movie"), containsString("delet")));
        tx.execute(Graql.undefine(label("movie").sub("production")));
    }

    @Test
    public void whenUndefiningASuperConcept_Throw() {
        assertExists(tx, x.label("production").sub("entity"));

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("production"), containsString("delet")));
        tx.execute(Graql.undefine(label("production").sub(ENTITY)));
    }

    @Test
    public void whenUndefiningARoleWithPlayers_Throw() {
        assertExists(tx, x.label("actor"));

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("actor"), containsString("delet")));
        tx.execute(Graql.undefine(label("actor").sub(ROLE)));
    }

    @Test
    public void whenUndefiningAnInstanceProperty_Throw() {
        Concept movie = tx.execute(Graql.insert(x.isa("movie"))).get(0).get(x);

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.defineUnsupportedProperty(Query.Property.ISA.toString()).getMessage());

        tx.execute(Graql.undefine(var().id(movie.id().getValue()).isa("movie")));
    }

    @Test
    public void whenGettingResultsString_ResultIsEmptyAndQueryExecutes() {
        tx.execute(Graql.define(label(NEW_TYPE).sub(ENTITY)));
        Type newType = tx.getType(NEW_TYPE);
        assertNotNull(newType);

        tx.execute(Graql.undefine(label(NEW_TYPE).sub(ENTITY)));
        assertNull(tx.getType(NEW_TYPE));
    }
}
