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
        ResourceType resourceType = graknGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        Resource resource = resourceType.putResource("resource");
        assertEquals(ResourceType.DataType.STRING, resource.dataType());
    }

    @Test
    public void testOwnerInstances() throws Exception {
        EntityType randomThing = graknGraph.putEntityType("A Thing");
        ResourceType resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType hasResource = graknGraph.putRelationType("Has Resource");
        RoleType resourceRole = graknGraph.putRoleType("Resource Role");
        RoleType actorRole = graknGraph.putRoleType("Actor");
        Instance pacino = randomThing.addEntity();
        Instance jennifer = randomThing.addEntity();
        Instance bob = randomThing.addEntity();
        Instance alice = randomThing.addEntity();
        Resource birthDate = resourceType.putResource("10/10/10");
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        assertEquals(0, birthDate.ownerInstances().size());

        hasResource.addRelation().
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, pacino);
        hasResource.addRelation().
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, jennifer);
        hasResource.addRelation().
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, bob);
        hasResource.addRelation().
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, alice);

        assertEquals(4, birthDate.ownerInstances().size());
        assertTrue(birthDate.ownerInstances().contains(pacino));
        assertTrue(birthDate.ownerInstances().contains(jennifer));
        assertTrue(birthDate.ownerInstances().contains(bob));
        assertTrue(birthDate.ownerInstances().contains(alice));
    }

    @Test
    public void checkResourceDataTypes(){
        ResourceType<String> strings = graknGraph.putResourceType("String Type", ResourceType.DataType.STRING);
        ResourceType<Long> longs = graknGraph.putResourceType("Long Type", ResourceType.DataType.LONG);
        ResourceType<Double> doubles = graknGraph.putResourceType("Double Type", ResourceType.DataType.DOUBLE);
        ResourceType<Boolean> booleans = graknGraph.putResourceType("Boolean Type", ResourceType.DataType.BOOLEAN);

        Resource<String> resource1 = strings.putResource("1");
        Resource<Long> resource2 = longs.putResource(1L);
        Resource<Double> resource3 = doubles.putResource(1.0);
        Resource<Boolean> resource4 = booleans.putResource(true);

        Resource<String> resource5 = strings.putResource("5");
        Resource<Long> resource6 = longs.putResource(1L);
        Resource<Double> resource7 = doubles.putResource(1.0);
        Resource<Boolean> resource8 = booleans.putResource(true);

        assertEquals("1", graknGraph.getConcept(resource1.getId()).asResource().getValue());
        assertEquals(1L, graknGraph.getConcept(resource2.getId()).asResource().getValue());
        assertEquals(1.0, graknGraph.getConcept(resource3.getId()).asResource().getValue());
        assertEquals(true, graknGraph.getConcept(resource4.getId()).asResource().getValue());

        assertThat(graknGraph.getConcept(resource1.getId()).asResource().getValue(), instanceOf(String.class));
        assertThat(graknGraph.getConcept(resource2.getId()).asResource().getValue(), instanceOf(Long.class));
        assertThat(graknGraph.getConcept(resource3.getId()).asResource().getValue(), instanceOf(Double.class));
        assertThat(graknGraph.getConcept(resource4.getId()).asResource().getValue(), instanceOf(Boolean.class));

        assertEquals(1, graknGraph.getResourcesByValue("1").size());
        assertEquals(1, graknGraph.getResourcesByValue(1L).size());
        assertEquals(1, graknGraph.getResourcesByValue(1.0).size());
        assertEquals(1, graknGraph.getResourcesByValue(true).size());
    }

    @Test
    public void setInvalidResourceTest (){
        ResourceType longResourceType = graknGraph.putResourceType("long", ResourceType.DataType.LONG);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("Invalid Thing", Long.class.getName()))
        ));
        longResourceType.putResource("Invalid Thing");
    }

    @Test
    public void datatypeTest2(){
        ResourceType<Double> doubleResourceType = graknGraph.putResourceType("doubleType", ResourceType.DataType.DOUBLE);
        Resource thing = doubleResourceType.putResource(2.0);
        assertEquals(2.0, thing.getValue());
    }

    @Test
    public void testToString() {
        ResourceType<String> concept = graknGraph.putResourceType("a", ResourceType.DataType.STRING);
        Resource<String> concept2 = concept.putResource("concept2");
        assertTrue(concept2.toString().contains("Value"));
    }

    @Test
    public void testInvalidDataType(){
        ResourceType stringResourceType = graknGraph.putResourceType("Strung", ResourceType.DataType.STRING);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("1", String.class.getName()))
        ));
        stringResourceType.putResource(1L);
    }

    @Test
    public void testUniqueResource(){
        //Create Ontology
        RoleType primaryKeyRole = graknGraph.putRoleType("Primary Key Role");
        RoleType entityRole = graknGraph.putRoleType("Entity Role");
        RelationType hasPrimaryKey = graknGraph.putRelationType("Has Parimary Key").hasRole(primaryKeyRole).hasRole(entityRole);


        //Create Resources
        ResourceType primaryKeyType = graknGraph.putResourceTypeUnique("My Primary Key", ResourceType.DataType.STRING).playsRole(primaryKeyRole);
        Resource pimaryKey1 = primaryKeyType.putResource("A Primary Key 1");
        Resource pimaryKey2 = primaryKeyType.putResource("A Primary Key 2");

        //Create Entities
        EntityType entityType = graknGraph.putEntityType("My Entity Type").playsRole(entityRole);
        Entity entity1 = entityType.addEntity();
        Entity entity2 = entityType.addEntity();
        Entity entity3 = entityType.addEntity();

        //Link Entities to resources
        assertNull(pimaryKey1.owner());
        hasPrimaryKey.addRelation().putRolePlayer(primaryKeyRole, pimaryKey1).putRolePlayer(entityRole, entity1);
        assertEquals(entity1, pimaryKey1.owner());

        hasPrimaryKey.addRelation().putRolePlayer(primaryKeyRole, pimaryKey2).putRolePlayer(entityRole, entity2);

        expectedException.expect(ConceptNotUniqueException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RESOURCE_TYPE_UNIQUE.getMessage(pimaryKey1.getId(), entity1.getId()))
        ));

        hasPrimaryKey.addRelation().putRolePlayer(primaryKeyRole, pimaryKey1).putRolePlayer(entityRole, entity3);
    }

    @Test
    public void testNonUniqueResource(){
        ResourceType resourceType = graknGraph.putResourceType("A resourceType", ResourceType.DataType.STRING);
        Resource resource = resourceType.putResource("A Thing");
        assertNull(resource.owner());
    }
}