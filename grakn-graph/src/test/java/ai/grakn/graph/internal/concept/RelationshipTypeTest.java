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

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.util.Schema;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RelationshipTypeTest extends GraphTestBase {
    @Test
    public void whenGettingTheRolesOfRelationTypes_AllTheRolesAreReturned() throws Exception {
        RelationshipType relationshipType = graknGraph.putRelationshipType("relationTypes");
        Role role1 = graknGraph.putRole("role1");
        Role role2 = graknGraph.putRole("role2");
        Role role3 = graknGraph.putRole("role3");
        relationshipType.relates(role1).relates(role2).relates(role3);
        assertThat(relationshipType.relates().collect(toSet()), containsInAnyOrder(role1, role2, role3));
    }

    @Test
    public void whenMutatingRolesOfRelationType_EnsureRelationTypeRolesAreAlwaysUpdated(){
        RelationshipType relationshipType = graknGraph.putRelationshipType("c1");
        Role role1 = graknGraph.putRole("c2");
        Role role2 = graknGraph.putRole("c3");
        assertThat(relationshipType.relates().collect(toSet()), empty());

        relationshipType.relates(role1).relates(role2);
        assertThat(relationshipType.relates().collect(toSet()), containsInAnyOrder(role1, role2));

        relationshipType.deleteRelates(role1);
        assertThat(relationshipType.relates().collect(toSet()), containsInAnyOrder(role2));
    }

    @Test
    public void whenCallingInstancesOnImplicitRelationType_RelationEdgesAreReturned(){
        ResourceType<String> resourceType = graknGraph.putResourceType("My Special Resource Type", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("Ad thing");

        EntityType entityType = graknGraph.putEntityType("My Special Entity Type").resource(resourceType);
        Entity entity = entityType.addEntity();

        RelationshipType implicitRelationshipType = graknGraph.getRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()).getValue());

        assertNotNull(implicitRelationshipType);
        assertThat(implicitRelationshipType.instances().collect(toSet()), empty());

        entity.resource(resource);

        assertEquals(1, implicitRelationshipType.instances().count());
    }

    @Test
    public void whenSettingAnImplicitRelationTypeWithInstancesAbstract_Throw(){
        ResourceType<String> resourceType = graknGraph.putResourceType("My Special Resource Type", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("Ad thing");

        EntityType entityType = graknGraph.putEntityType("My Special Entity Type").resource(resourceType);
        entityType.addEntity().resource(resource);

        RelationshipType implicitRelationshipType = graknGraph.getRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()).getValue());

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.addingInstancesToAbstractType(implicitRelationshipType).getMessage());

        implicitRelationshipType.setAbstract(true);
    }
}