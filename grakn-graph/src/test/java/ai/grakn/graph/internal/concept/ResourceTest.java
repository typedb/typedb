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
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.util.Schema;
import com.google.common.collect.Iterables;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResourceTest extends GraphTestBase {
    @Test
    public void whenCreatingResource_EnsureTheResourcesDataTypeIsTheSameAsItsType() throws Exception {
        ResourceType<String> resourceType = graknGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        Resource resource = resourceType.putResource("resource");
        assertEquals(ResourceType.DataType.STRING, resource.dataType());
    }

    @Test
    public void whenAttachingResourcesToInstances_EnsureInstancesAreReturnedAsOwners() throws Exception {
        EntityType randomThing = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType hasResource = graknGraph.putRelationType("Has Resource");
        Role resourceRole = graknGraph.putRole("Resource Role");
        Role actorRole = graknGraph.putRole("Actor");
        Thing pacino = randomThing.addEntity();
        Thing jennifer = randomThing.addEntity();
        Thing bob = randomThing.addEntity();
        Thing alice = randomThing.addEntity();
        Resource<String> birthDate = resourceType.putResource("10/10/10");
        hasResource.relates(resourceRole).relates(actorRole);

        assertThat(birthDate.ownerInstances(), empty());

        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, pacino);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, jennifer);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, bob);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, alice);

        assertThat(birthDate.ownerInstances(), containsInAnyOrder(pacino, jennifer, bob, alice));
    }

    // this is due to the generic of getResourcesByValue
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResources_EnsureDataTypesAreEnforced(){
        ResourceType<String> strings = graknGraph.putResourceType("String Type", ResourceType.DataType.STRING);
        ResourceType<Long> longs = graknGraph.putResourceType("Long Type", ResourceType.DataType.LONG);
        ResourceType<Double> doubles = graknGraph.putResourceType("Double Type", ResourceType.DataType.DOUBLE);
        ResourceType<Boolean> booleans = graknGraph.putResourceType("Boolean Type", ResourceType.DataType.BOOLEAN);

        Resource<String> resource1 = strings.putResource("1");
        Resource<Long> resource2 = longs.putResource(1L);
        Resource<Double> resource3 = doubles.putResource(1.0);
        Resource<Boolean> resource4 = booleans.putResource(true);

        assertEquals("1", graknGraph.<Resource>getConcept(resource1.getId()).getValue());
        assertEquals(1L, graknGraph.<Resource>getConcept(resource2.getId()).getValue());
        assertEquals(1.0, graknGraph.<Resource>getConcept(resource3.getId()).getValue());
        assertEquals(true, graknGraph.<Resource>getConcept(resource4.getId()).getValue());

        assertThat(graknGraph.<Resource>getConcept(resource1.getId()).getValue(), instanceOf(String.class));
        assertThat(graknGraph.<Resource>getConcept(resource2.getId()).getValue(), instanceOf(Long.class));
        assertThat(graknGraph.<Resource>getConcept(resource3.getId()).getValue(), instanceOf(Double.class));
        assertThat(graknGraph.<Resource>getConcept(resource4.getId()).getValue(), instanceOf(Boolean.class));

        assertThat(graknGraph.getResourcesByValue("1"), containsInAnyOrder(resource1));
        assertThat(graknGraph.getResourcesByValue(1L), containsInAnyOrder(resource2));
        assertThat(graknGraph.getResourcesByValue(1.0), containsInAnyOrder(resource3));
        assertThat(graknGraph.getResourcesByValue(true), containsInAnyOrder(resource4));
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_Throw(){
        String invalidThing = "Invalid Thing";
        ResourceType longResourceType = graknGraph.putResourceType("long", ResourceType.DataType.LONG);
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.invalidResourceValue(invalidThing, ResourceType.DataType.LONG).getMessage());
        longResourceType.putResource(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_DoNotCreateTheResource(){
        ResourceType longResourceType = graknGraph.putResourceType("long", ResourceType.DataType.LONG);

        try {
            longResourceType.putResource("Invalid Thing");
            fail("Expected to throw");
        } catch (GraphOperationException e) {
            // expected failure
        }

        Collection<Resource> instances = longResourceType.instances();

        assertThat(instances, empty());
    }

    @Test
    public void whenSavingDateIntoResource_DateIsReturnedInSameFormat(){
        LocalDateTime date = LocalDateTime.now();
        ResourceType<LocalDateTime> resourceType = graknGraph.putResourceType("My Birthday", ResourceType.DataType.DATE);
        Resource<LocalDateTime> myBirthday = resourceType.putResource(date);

        assertEquals(date, myBirthday.getValue());
        assertEquals(myBirthday, resourceType.getResource(date));
        assertThat(graknGraph.getResourcesByValue(date), containsInAnyOrder(myBirthday));
    }

    @Test
    public void whenLinkingResourcesToThings_EnsureTheRelationIsAnEdge(){
        ResourceType<String> resourceType = graknGraph.putResourceType("My resource type", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("A String");

        EntityType entityType = graknGraph.putEntityType("My entity type").resource(resourceType);
        Entity entity = entityType.addEntity();

        entity.resource(resource);

        RelationStructure relationStructure = RelationImpl.from(Iterables.getOnlyElement(entity.relations())).structure();
        assertThat(relationStructure, instanceOf(RelationEdge.class));
        assertTrue("Edge Relation id not starting with [" + Schema.PREFIX_EDGE + "]",relationStructure.getId().getValue().startsWith(Schema.PREFIX_EDGE));
        assertEquals(entity, resource.owner());
        assertThat(entity.resources(), containsInAnyOrder(resource));
    }

    @Test
    public void whenAddingRolePlayerToRelationEdge_RelationAutomaticallyReifies(){
        //Create boring resource which creates a relation edge
        ResourceType<String> resourceType = graknGraph.putResourceType("My resource type", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("A String");
        EntityType entityType = graknGraph.putEntityType("My entity type").resource(resourceType);
        Entity entity = entityType.addEntity();
        entity.resource(resource);
        RelationImpl relation = RelationImpl.from(entity.relations().iterator().next());

        //Check it's a relation edge.
        RelationStructure relationStructureBefore = relation.structure();
        assertThat(relationStructureBefore, instanceOf(RelationEdge.class));

        //Get the roles and role players via the relation edge:
        Map<Role, Set<Thing>> allRolePlayerBefore = relationStructureBefore.allRolePlayers();

        //Expand Ontology to allow new role
        Role newRole = graknGraph.putRole("My New Role");
        entityType.plays(newRole);
        relation.type().relates(newRole);
        Entity newEntity = entityType.addEntity();

        //Now actually add the new role player
        relation.addRolePlayer(newRole, newEntity);

        //Check it's a relation reified now.
        RelationStructure relationStructureAfter = relation.structure();
        assertThat(relationStructureAfter, instanceOf(RelationReified.class));

        //Check IDs are equal
        assertEquals(relationStructureBefore.getId(), relation.getId());
        assertEquals(relationStructureBefore.getId(), relationStructureAfter.getId());

        //Check Role Players have been transferred
        allRolePlayerBefore.forEach((role, player) -> assertEquals(player, relationStructureAfter.rolePlayers(role)));

        //Check Type Has Been Transferred
        assertEquals(relationStructureBefore.type(), relationStructureAfter.type());

        //Check new role player has been added as well
        assertEquals(newEntity, Iterables.getOnlyElement(relationStructureAfter.rolePlayers(newRole)));
    }

    @Test
    public void whenInsertingAThingWithTwoKeys_Throw(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Key Thingy", ResourceType.DataType.STRING);
        EntityType entityType = graknGraph.putEntityType("Entity Type Thingy").key(resourceType);
        Entity entity = entityType.addEntity();

        Resource<String> key1 = resourceType.putResource("key 1");
        Resource<String> key2 = resourceType.putResource("key 2");

        entity.resource(key1);
        entity.resource(key2);

        expectedException.expect(InvalidGraphException.class);

        graknGraph.commit();
    }

    @Test
    public void whenGettingTheRelationsOfResources_EnsureIncomingResourceEdgesAreTakingIntoAccount(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type Thingy", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("Thingy");

        EntityType entityType = graknGraph.putEntityType("Entity Type Thingy").key(resourceType);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();

        assertThat(resource.relations(), empty());

        e1.resource(resource);
        e2.resource(resource);

        Relation rel1 = Iterables.getOnlyElement(e1.relations());
        Relation rel2 = Iterables.getOnlyElement(e2.relations());

        assertThat(resource.relations(), containsInAnyOrder(rel1, rel2));
    }

}