/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.INVALID_OBJECT_TYPE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ConceptTest extends GraphTestBase{

    @Test
    public void whenComparingConcepts_EnsureEqualityIsBasedOnConceptID() {
        Concept v1_1 = graknGraph.putEntityType("Value_1");
        Concept v1_2 = graknGraph.getEntityType("Value_1");
        Concept v1_3 = graknGraph.putEntityType("Value_1");

        Concept v2_1 = graknGraph.putEntityType("Value_2");

        assertEquals(v1_1, v1_2);
        assertNotEquals(v1_1, v2_1);
        assertNotEquals(v1_1.getId().getRawValue(), v2_1.getId().getRawValue());

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
        EntityType concept = graknGraph.putEntityType("a");
        Instance entity = concept.addEntity();

        assertTrue(entity.toString().contains(Schema.BaseType.ENTITY.name()));
        assertTrue(entity.toString().contains(entity.getId().getValue()));
    }

    @Test
    public void whenGettingEdgesFromAConcept_EdgesFilteredByLabelAreReturned(){
        EntityType entityType1 = graknGraph.putEntityType("entity type");
        EntityTypeImpl entityType2 = (EntityTypeImpl) graknGraph.putEntityType("entity type 1").superType(entityType1);
        EntityType entityType3 = graknGraph.putEntityType("entity type 2").superType(entityType2);

        Entity entity1 = entityType2.addEntity();
        Entity entity2 = entityType2.addEntity();

        Set<EdgeImpl> superType = entityType2.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SUB).collect(Collectors.toSet());
        Set<EdgeImpl> subs = entityType2.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SUB).collect(Collectors.toSet());
        Set<EdgeImpl> instances = entityType2.getEdgesOfType(Direction.IN, Schema.EdgeLabel.ISA).collect(Collectors.toSet());

        assertThat(superType, is(not(empty())));
        assertThat(subs, is(not(empty())));
        assertThat(instances, is(not(empty())));

        superType.forEach(edge -> assertEquals(entityType1, edge.getTarget()));
        subs.forEach(edge -> assertEquals(entityType3, edge.getSource()));
        assertThat(instances.stream().map(EdgeImpl::getSource).collect(Collectors.toSet()),
                containsInAnyOrder(entity1, entity2));
    }

    @Test
    public void whenCastingToCorrectType_ReturnCorrectType(){
        Concept concept = graknGraph.putEntityType("Test");
        assertTrue("Concept is not of type [" + EntityType.class.getName() + "]", concept.isEntityType());
        EntityType type = concept.asEntityType();
        assertEquals(type, concept);
    }


    @Test
    public void whenCastingToInCorrectType_Throw(){
        EntityType thingType = graknGraph.putEntityType("thing type");
        Entity thing = thingType.addEntity();

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(INVALID_OBJECT_TYPE.getMessage(thing, Type.class));

        thing.asType();
    }
}