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
import grakn.core.rule.GraknTestServer;
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
    // TODO: This test class should be cleaned up.
    //       Either use a shared dataset across all test, or make all tests independent.

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
        tx = session.writeTransaction();
    }

    @After
    public void closeTransaction() {
        tx.close();
        session.close();
    }

    @Test
    public void whenUndefiningDataType_DoNothing() {
        tx.execute(Graql.undefine(type("name").datatype(Graql.Token.DataType.STRING)));
        tx.commit();
        tx = session.writeTransaction();
        assertEquals(AttributeType.DataType.STRING, tx.getAttributeType("name").dataType());
    }

    @Test
    public void whenUndefiningHas_TheHasLinkIsDeleted() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).has("name")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).has("name")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningHasWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).has("name")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).has("title")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test
    public void whenUndefiningKey_TheKeyLinkIsDeleted() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).key("name")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).key("name")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningKeyWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).key("name")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).key("title")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test @Ignore // TODO: investigate how this is possible in the first place
    public void whenUndefiningById_TheSchemaConceptIsDeleted() {
        Type newType = tx.execute(Graql.define(x.type(NEW_TYPE.getValue()).sub(ENTITY))).get(0).get(x.var()).asType();
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(var().id(newType.id().getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.writeTransaction();
        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningIsAbstract_TheTypeIsNoLongerAbstract() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).isAbstract()));
        tx.commit();
        tx = session.writeTransaction();
        assertTrue(tx.getType(NEW_TYPE).isAbstract());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).isAbstract()));
        tx.commit();
        tx = session.writeTransaction();
        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningIsAbstractOnNonAbstractType_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.writeTransaction();
        assertFalse(tx.getType(NEW_TYPE).isAbstract());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).isAbstract()));
        tx.commit();
        tx = session.writeTransaction();
        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningPlays_TheTypeNoLongerPlaysTheRole() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).plays("actor")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("actor")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).plays("actor")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningPlaysWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).plays("production-with-cast")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("production-with-cast")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).plays("actor")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("production-with-cast")));
    }

    @Test
    public void whenUndefiningRegexProperty_TheAttributeTypeHasNoRegex() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ATTRIBUTE).datatype(Graql.Token.DataType.STRING).regex("abc")));
        tx.commit();
        tx = session.writeTransaction();
        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).regex("abc")));
        tx.commit();
        tx = session.writeTransaction();
        assertNull(tx.<AttributeType>getType(NEW_TYPE).regex());
    }

    @Test
    public void whenUndefiningRegexPropertyWithWrongRegex_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ATTRIBUTE).datatype(Graql.Token.DataType.STRING).regex("abc")));
        tx.commit();
        tx = session.writeTransaction();
        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).regex("xyz")));
        tx.commit();
        tx = session.writeTransaction();
        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());
    }

    @Test
    public void whenUndefiningRelatesPropertyWithoutCommit_TheRelationTypeNoLongerRelatesTheRole() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(RELATION).relates("actor")));
        tx.commit();
        tx = session.writeTransaction();
        assertThat(tx.<RelationType>getType(NEW_TYPE).roles().toArray(), hasItemInArray(tx.getRole("actor")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).relates("actor")));
        assertThat(tx.<RelationType>getType(NEW_TYPE).roles().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningSub_TheSchemaConceptIsDeleted() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.writeTransaction();
        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningSubWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).sub(THING)));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void undefineRelationAndRoles() {
        tx.execute(Graql.define(type("employment").sub("relation").relates("employee")));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(Label.of("employment")));

        GraqlUndefine undefineQuery = Graql.undefine(
                type("employment").sub("relation").relates("employee"),
                type("employee").sub("role")
        );

        tx.execute(undefineQuery);
        tx.commit();
        tx = session.writeTransaction();
        assertNull(tx.getType(Label.of("employment")));
    }

    @Test
    public void undefineTypeAndTheirAttributes() {
        tx.execute(Graql.define(
                type("company").sub("entity").has("registration"),
                type("registration").sub("attribute").datatype(Graql.Token.DataType.STRING)
        ));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(Label.of("company")));
        assertNotNull(tx.getType(Label.of("registration")));

        GraqlUndefine undefineQuery = Graql.undefine(
                type("company").sub("entity").has("registration"),
                type("registration").sub("attribute")
        );

        tx.execute(undefineQuery);
        tx.commit();
        tx = session.writeTransaction();
        assertNull(tx.getType(Label.of("company")));
        assertNull(tx.getType(Label.of("registration")));
    }

    @Test
    public void undefineTypeAndTheirAttributeAndRolesAndRelations() {
        Collection<Statement> schema = ImmutableList.of(
                type("pokemon").sub(ENTITY).has("pokedex-no").plays("ancestor").plays("descendant"),
                type("pokedex-no").sub(ATTRIBUTE).datatype(Graql.Token.DataType.LONG),
                type("evolution").sub(RELATION).relates("ancestor").relates("descendant"),
                type("ancestor").sub(ROLE),
                type("descendant").sub(ROLE)
        );
        tx.execute(Graql.define(schema));
        tx.commit();
        tx = session.writeTransaction();

        EntityType pokemon = tx.getEntityType("pokemon");
        RelationType evolution = tx.getRelationType("evolution");
        AttributeType<Long> pokedexNo = tx.getAttributeType("pokedex-no");
        Role ancestor = tx.getRole("ancestor");
        Role descendant = tx.getRole("descendant");

        assertThat(pokemon.attributes().toArray(), arrayContaining(pokedexNo));
        assertThat(evolution.roles().toArray(), arrayContainingInAnyOrder(ancestor, descendant));
        assertThat(pokemon.playing().filter(r -> !r.isImplicit()).toArray(), arrayContainingInAnyOrder(ancestor, descendant));

        tx.execute(Graql.undefine(schema));
        tx.commit();
        tx = session.writeTransaction();

        assertNull(tx.getEntityType("pokemon"));
        assertNull(tx.getEntityType("evolution"));
        assertNull(tx.getAttributeType("pokedex-no"));
        assertNull(tx.getRole("ancestor"));
        assertNull(tx.getRole("descendant"));
    }

    @Test
    public void undefineTypeAndTheirAttributeWhenThereIsAnInstanceOfThem_Throw() {
        tx.execute(Graql.define(
                type("registration").sub("attribute").datatype(Graql.Token.DataType.STRING),
                type("company").sub("entity").has("registration")
        ));
        tx.execute(Graql.insert(
                var("x").isa("company").has("registration", "12345")
        ));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(Label.of("company")));
        assertNotNull(tx.getType(Label.of("registration")));
        assertTrue(tx.getType(Label.of("company")).instances().iterator().hasNext());

        exception.expect(GraknConceptException.class);
        exception.expectMessage(GraknConceptException.illegalUnhasWithInstance(
                "company", "registration", false).getMessage());
        tx.execute(Graql.undefine(type("company").has("registration")));
    }

    @Test
    public void whenUndefiningATypeAndTheirInheritedAttribute_Throw() {
        tx.execute(Graql.define(
                type("registration").sub("attribute").datatype(Graql.Token.DataType.STRING),
                type("company").sub("entity").has("registration"),
                type("sub-company").sub("company")
        ));
        tx.commit();
        tx = session.writeTransaction();
        assertNotNull(tx.getType(Label.of("company")));
        assertNotNull(tx.getType(Label.of("sub-company")));
        assertNotNull(tx.getType(Label.of("registration")));

        exception.expect(GraknConceptException.class);
        exception.expectMessage(GraknConceptException.illegalUnhasInherited(
                "sub-company", "registration", false).getMessage());
        tx.execute(Graql.undefine(type("sub-company").has("registration")));
    }

    @Test
    public void whenUndefiningATypeWithInstances_Throw() {
        assertExists(tx, x.type("movie").sub("entity"));
        assertExists(tx, x.isa("movie"));

        exception.expect(GraknConceptException.class);
        exception.expectMessage(allOf(containsString("movie"), containsString("delet")));
        tx.execute(Graql.undefine(type("movie").sub("production")));
    }

    @Test
    public void whenUndefiningASuperConcept_Throw() {
        assertExists(tx, x.type("production").sub("entity"));

        exception.expect(GraknConceptException.class);
        exception.expectMessage(allOf(containsString("production"), containsString("delet")));
        tx.execute(Graql.undefine(type("production").sub(ENTITY)));
    }

    @Test
    public void whenUndefiningARoleWithPlayers_Throw() {
        assertExists(tx, x.type("actor"));

        exception.expect(GraknConceptException.class);
        exception.expectMessage(allOf(containsString("actor"), containsString("delet")));
        tx.execute(Graql.undefine(type("actor").sub(ROLE)));
    }

    @Test
    public void whenUndefiningAnInstanceProperty_Throw() {
        Concept movie = tx.execute(Graql.insert(x.isa("movie"))).get(0).get(x.var());

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.ISA.toString()).getMessage());

        tx.execute(Graql.undefine(var().id(movie.id().getValue()).isa("movie")));
    }
}
