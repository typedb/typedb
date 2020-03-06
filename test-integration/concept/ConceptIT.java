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
 *
 */

package grakn.core.concept;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.server.Session;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import grakn.core.util.ConceptDowncasting;
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
    public static final GraknTestStorage storage = new GraknTestStorage();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TestTransactionProvider.TestTransaction tx;
    private Session session;

    @Before
    public void setUp() {
        session = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        tx = (TestTransactionProvider.TestTransaction)session.writeTransaction();
    }

    @After
    public void tearDown() {
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
    public void whenGettingEdgesFromAConcept_EdgesFilteredByLabelAreReturned() {
        EntityType entityType1 = tx.putEntityType("entity type");
        EntityType entityType2 = tx.putEntityType("entity type 1").sup(entityType1);
        EntityType entityType3 = tx.putEntityType("entity type 2").sup(entityType2);

        Set<EdgeElement> superType = ConceptDowncasting.concept(entityType2).vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SUB).collect(Collectors.toSet());
        Set<EdgeElement> subs = ConceptDowncasting.concept(entityType2).vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SUB).collect(Collectors.toSet());

        assertThat(superType, is(not(empty())));
        assertThat(subs, is(not(empty())));

        superType.forEach(edge -> assertEquals(entityType1, tx.conceptManager().buildConcept(edge.target())));
        subs.forEach(edge -> assertEquals(entityType3, tx.conceptManager().buildConcept(edge.source())));
    }

    @Test
    public void whenCastingToCorrectType_ReturnCorrectType() {
        Concept concept = tx.putEntityType("Test");
        assertTrue("Concept is not of type [" + EntityType.class.getName() + "]", concept.isEntityType());
        EntityType type = concept.asEntityType();
        assertEquals(type, concept);
    }


    @Test
    public void whenCastingToInCorrectType_Throw() {
        EntityType thingType = tx.putEntityType("thing type");
        Entity thing = thingType.create();

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.invalidCasting(thing, Type.class).getMessage());

        //noinspection ResultOfMethodCallIgnored
        thing.asType();
    }

    @Test
    public void whenAConceptIsNotDeleted_CallingIsDeletedReturnsFalse() {
        Concept stillAlive = tx.putEntityType("still-alive");

        assertFalse(stillAlive.isDeleted());
    }
}