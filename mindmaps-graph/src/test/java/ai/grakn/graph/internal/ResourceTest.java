/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.util.ErrorMessage;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class ResourceTest extends GraphTestBase{
    @Test
    public void testDataType() throws Exception {
        ResourceType resourceType = mindmapsGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        Resource resource = mindmapsGraph.putResource("resource", resourceType);
        assertEquals(ResourceType.DataType.STRING, resource.dataType());
    }

    @Test
    public void testOwnerInstances() throws Exception {
        EntityType randomThing = mindmapsGraph.putEntityType("A Thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType hasResource = mindmapsGraph.putRelationType("Has Resource");
        RoleType resourceRole = mindmapsGraph.putRoleType("Resource Role");
        RoleType actorRole = mindmapsGraph.putRoleType("Actor");
        Instance pacino = mindmapsGraph.addEntity(randomThing);
        Instance jennifer = mindmapsGraph.addEntity(randomThing);
        Instance bob = mindmapsGraph.addEntity(randomThing);
        Instance alice = mindmapsGraph.addEntity(randomThing);
        Resource birthDate = mindmapsGraph.putResource("10/10/10", resourceType);
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        assertEquals(0, birthDate.ownerInstances().size());

        mindmapsGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, pacino);
        mindmapsGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, jennifer);
        mindmapsGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, bob);
        mindmapsGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, alice);

        assertEquals(4, birthDate.ownerInstances().size());
        assertTrue(birthDate.ownerInstances().contains(pacino));
        assertTrue(birthDate.ownerInstances().contains(jennifer));
        assertTrue(birthDate.ownerInstances().contains(bob));
        assertTrue(birthDate.ownerInstances().contains(alice));
    }

    @Test
    public void checkResourceDataTypes(){
        ResourceType<String> strings = mindmapsGraph.putResourceType("String Type", ResourceType.DataType.STRING);
        ResourceType<Long> longs = mindmapsGraph.putResourceType("Long Type", ResourceType.DataType.LONG);
        ResourceType<Double> doubles = mindmapsGraph.putResourceType("Double Type", ResourceType.DataType.DOUBLE);
        ResourceType<Boolean> booleans = mindmapsGraph.putResourceType("Boolean Type", ResourceType.DataType.BOOLEAN);

        Resource<String> resource1 = mindmapsGraph.putResource("1", strings);
        Resource<Long> resource2 = mindmapsGraph.putResource(1L, longs);
        Resource<Double> resource3 = mindmapsGraph.putResource(1.0, doubles);
        Resource<Boolean> resource4 = mindmapsGraph.putResource(true, booleans);

        Resource<String> resource5 = mindmapsGraph.putResource("5", strings);
        Resource<Long> resource6 = mindmapsGraph.putResource(1L, longs);
        Resource<Double> resource7 = mindmapsGraph.putResource(1.0, doubles);
        Resource<Boolean> resource8 = mindmapsGraph.putResource(true, booleans);

        assertEquals("1", mindmapsGraph.getResource(resource1.getId()).getValue());
        assertEquals(1L, mindmapsGraph.getResource(resource2.getId()).getValue());
        assertEquals(1.0, mindmapsGraph.getResource(resource3.getId()).getValue());
        assertEquals(true, mindmapsGraph.getResource(resource4.getId()).getValue());

        assertThat(mindmapsGraph.getResource(resource1.getId()).getValue(), instanceOf(String.class));
        assertThat(mindmapsGraph.getResource(resource2.getId()).getValue(), instanceOf(Long.class));
        assertThat(mindmapsGraph.getResource(resource3.getId()).getValue(), instanceOf(Double.class));
        assertThat(mindmapsGraph.getResource(resource4.getId()).getValue(), instanceOf(Boolean.class));

        assertEquals(1, mindmapsGraph.getResourcesByValue("1").size());
        assertEquals(1, mindmapsGraph.getResourcesByValue(1L).size());
        assertEquals(1, mindmapsGraph.getResourcesByValue(1.0).size());
        assertEquals(1, mindmapsGraph.getResourcesByValue(true).size());
    }

    @Test
    public void setInvalidResourceTest (){
        ResourceType longResourceType = mindmapsGraph.putResourceType("long", ResourceType.DataType.LONG);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("Invalid Thing", Long.class.getName()))
        ));
        mindmapsGraph.putResource("Invalid Thing", longResourceType);
    }

    @Test
    public void datatypeTest2(){
        ResourceType<Double> doubleResourceType = mindmapsGraph.putResourceType("doubleType", ResourceType.DataType.DOUBLE);
        Resource thing = mindmapsGraph.putResource(2.0, doubleResourceType);
        assertEquals(2.0, thing.getValue());
    }

    @Test
    public void testToString() {
        ResourceType<String> concept = mindmapsGraph.putResourceType("a", ResourceType.DataType.STRING);
        Resource<String> concept2 = mindmapsGraph.putResource("concept2", concept);
        assertTrue(concept2.toString().contains("Value"));
    }

    @Test
    public void testInvalidDataType(){
        ResourceType stringResourceType = mindmapsGraph.putResourceType("Strung", ResourceType.DataType.STRING);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("1", String.class.getName()))
        ));
        mindmapsGraph.putResource(1L, stringResourceType);
    }

    @Test
    public void testUniqueResource(){
        //Create Ontology
        RoleType primaryKeyRole = mindmapsGraph.putRoleType("Primary Key Role");
        RoleType entityRole = mindmapsGraph.putRoleType("Entity Role");
        RelationType hasPrimaryKey = mindmapsGraph.putRelationType("Has Parimary Key").hasRole(primaryKeyRole).hasRole(entityRole);


        //Create Resources
        ResourceType primaryKeyType = mindmapsGraph.putResourceTypeUnique("My Primary Key", ResourceType.DataType.STRING).playsRole(primaryKeyRole);
        Resource pimaryKey1 = mindmapsGraph.putResource("A Primary Key 1", primaryKeyType);
        Resource pimaryKey2 = mindmapsGraph.putResource("A Primary Key 2", primaryKeyType);

        //Create Entities
        EntityType entityType = mindmapsGraph.putEntityType("My Entity Type").playsRole(entityRole);
        Entity entity1 = mindmapsGraph.addEntity(entityType);
        Entity entity2 = mindmapsGraph.addEntity(entityType);
        Entity entity3 = mindmapsGraph.addEntity(entityType);

        //Link Entities to resources
        assertNull(pimaryKey1.owner());
        mindmapsGraph.addRelation(hasPrimaryKey).putRolePlayer(primaryKeyRole, pimaryKey1).putRolePlayer(entityRole, entity1);
        assertEquals(entity1, pimaryKey1.owner());

        mindmapsGraph.addRelation(hasPrimaryKey).putRolePlayer(primaryKeyRole, pimaryKey2).putRolePlayer(entityRole, entity2);

        expectedException.expect(ConceptNotUniqueException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RESOURCE_TYPE_UNIQUE.getMessage(pimaryKey1.getId(), entity1.getId()))
        ));

        mindmapsGraph.addRelation(hasPrimaryKey).putRolePlayer(primaryKeyRole, pimaryKey1).putRolePlayer(entityRole, entity3);
    }

    @Test
    public void testNonUniqueResource(){
        ResourceType resourceType = mindmapsGraph.putResourceType("A resourceType", ResourceType.DataType.STRING);
        Resource resource = mindmapsGraph.putResource("A Thing", resourceType);
        assertNull(resource.owner());
    }
}