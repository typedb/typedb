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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.graph.internal.concept.EntityTypeImpl;
import ai.grakn.graph.internal.concept.ResourceTypeImpl;
import ai.grakn.graph.internal.structure.EdgeElement;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OntologyConceptTest extends GraphTestBase {

    @Test
    public void whenChangingOntologyConceptLabel_EnsureLabelIsChangedAndOldLabelIsDead(){
        Label originalLabel = Label.of("my original label");
        Label newLabel = Label.of("my new label");
        EntityType entityType = graknGraph.putEntityType(originalLabel.getValue());

        //Original label works
        assertEquals(entityType, graknGraph.getType(originalLabel));

        //Change The Label
        entityType.setLabel(newLabel);

        //Check the label is changes
        assertEquals(newLabel, entityType.getLabel());
        assertEquals(entityType, graknGraph.getType(newLabel));

        //Check old label is dead
        assertNull(graknGraph.getType(originalLabel));
    }

    @Test
    public void whenSpecifyingTheResourceTypeOfAnEntityType_EnsureTheImplicitStructureIsCreated(){
        Label resourceLabel = Label.of("Resource Type");
        EntityType entityType = graknGraph.putEntityType("Entity1");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        //Implicit Names
        Label hasResourceOwnerLabel = Schema.ImplicitType.HAS_OWNER.getLabel(resourceLabel);
        Label hasResourceValueLabel = Schema.ImplicitType.HAS_VALUE.getLabel(resourceLabel);
        Label hasResourceLabel = Schema.ImplicitType.HAS.getLabel(resourceLabel);

        entityType.resource(resourceType);

        RelationType relationType = graknGraph.getRelationType(hasResourceLabel.getValue());
        Assert.assertEquals(hasResourceLabel, relationType.getLabel());

        Set<Label> roleLabels = relationType.relates().stream().map(OntologyConcept::getLabel).collect(toSet());
        assertThat(roleLabels, containsInAnyOrder(hasResourceOwnerLabel, hasResourceValueLabel));

        assertThat(entityType.plays(), containsInAnyOrder(graknGraph.getRole(hasResourceOwnerLabel.getValue())));
        assertThat(resourceType.plays(), containsInAnyOrder(graknGraph.getRole(hasResourceValueLabel.getValue())));

        //Check everything is implicit
        assertTrue(relationType.isImplicit());
        relationType.relates().forEach(role -> assertTrue(role.isImplicit()));

        // Check that resource is not required
        EdgeElement entityPlays = ((EntityTypeImpl) entityType).vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).iterator().next();
        assertFalse(entityPlays.propertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeElement resourcePlays = ((ResourceTypeImpl<?>) resourceType).vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).iterator().next();
        assertFalse(resourcePlays.propertyBoolean(Schema.EdgeProperty.REQUIRED));
    }

    @Test
    public void whenChangingTheLabelOfOntologyConceptAndThatLabelIsTakenByAnotherConcept_Throw(){
        Label label = Label.of("mylabel");

        EntityType e1 = graknGraph.putEntityType("Entity1");
        graknGraph.putEntityType(label);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(ErrorMessage.LABEL_TAKEN.getMessage(label));

        e1.setLabel(label);
    }
}
