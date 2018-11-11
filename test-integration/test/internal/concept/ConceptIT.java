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

package grakn.core.kb.internal.concept;

import grakn.core.GraknTxType;
import grakn.core.concept.Concept;
import grakn.core.concept.Entity;
import grakn.core.concept.EntityType;
import grakn.core.concept.Thing;
import grakn.core.concept.Type;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.factory.EmbeddedGraknSession;
import grakn.core.kb.internal.EmbeddedGraknTx;
import grakn.core.kb.internal.structure.EdgeElement;
import grakn.core.test.rule.ConcurrentGraknServer;
import grakn.core.graql.internal.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ConceptIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private EmbeddedGraknTx tx;
    private EmbeddedGraknSession session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(GraknTxType.WRITE);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenComparingConcepts_EnsureEqualityIsBasedOnConceptID() {
        Concept v1_1 = tx.putEntityType("Value_1");
        Concept v1_2 = tx.getEntityType("Value_1");
        Concept v1_3 = tx.putEntityType("Value_1");

        Concept v2_1 = tx.putEntityType("Value_2");

        assertEquals(v1_1, v1_2);
        assertNotEquals(v1_1, v2_1);
        assertNotEquals(v1_1.id(), v2_1.id());

        HashSet<Concept> concepts = new HashSet<>();
        concepts.add(v1_1);
        concepts.add(v1_2);
        concepts.add(v1_3);
        assertEquals(1, concepts.size());

        concepts.add(v2_1);
        assertEquals(2, concepts.size());
    }

    @Test
    public void checkToStringHasMinimalInformation() {
        EntityType concept = tx.putEntityType("a");
        Thing entity = concept.create();

        assertTrue(entity.toString().contains(Schema.BaseType.ENTITY.name()));
        assertTrue(entity.toString().contains(entity.id().getValue()));
    }

    @Test
    public void whenGettingEdgesFromAConcept_EdgesFilteredByLabelAreReturned(){
        EntityType entityType1 = tx.putEntityType("entity type");
        EntityTypeImpl entityType2 = (EntityTypeImpl) tx.putEntityType("entity type 1").sup(entityType1);
        EntityType entityType3 = tx.putEntityType("entity type 2").sup(entityType2);

        Set<EdgeElement> superType = entityType2.vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SUB).collect(Collectors.toSet());
        Set<EdgeElement> subs = entityType2.vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SUB).collect(Collectors.toSet());

        assertThat(superType, is(not(empty())));
        assertThat(subs, is(not(empty())));

        superType.forEach(edge -> assertEquals(entityType1, tx.factory().buildConcept(edge.target())));
        subs.forEach(edge -> assertEquals(entityType3, tx.factory().buildConcept(edge.source())));
    }

    @Test
    public void whenCastingToCorrectType_ReturnCorrectType(){
        Concept concept = tx.putEntityType("Test");
        assertTrue("Concept is not of type [" + EntityType.class.getName() + "]", concept.isEntityType());
        EntityType type = concept.asEntityType();
        assertEquals(type, concept);
    }


    @Test
    public void whenCastingToInCorrectType_Throw(){
        EntityType thingType = tx.putEntityType("thing type");
        Entity thing = thingType.create();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.invalidCasting(thing, Type.class).getMessage());

        //noinspection ResultOfMethodCallIgnored
        thing.asType();
    }

    @Test
    public void whenAConceptIsNotDeleted_CallingIsDeletedReturnsFalse() {
        Concept stillAlive = tx.putEntityType("still-alive");

        assertFalse(stillAlive.isDeleted());
    }
}