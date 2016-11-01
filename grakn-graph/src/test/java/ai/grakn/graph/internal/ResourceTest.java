/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.util.ErrorMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class ResourceTest {

    private AbstractGraknGraph graknGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph() {
        graknGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        graknGraph.initialiseMetaConcepts();
    }

    @Test
    public void testDataType() throws Exception {
        ResourceType resourceType = graknGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        Resource resource = graknGraph.putResource("resource", resourceType);
        assertEquals(ResourceType.DataType.STRING, resource.dataType());
    }

    @Test
    public void testOwnerInstances() throws Exception {
        EntityType randomThing = graknGraph.putEntityType("A Thing");
        ResourceType resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType hasResource = graknGraph.putRelationType("Has Resource");
        RoleType resourceRole = graknGraph.putRoleType("Resource Role");
        RoleType actorRole = graknGraph.putRoleType("Actor");
        Instance pacino = graknGraph.addEntity(randomThing);
        Instance jennifer = graknGraph.addEntity(randomThing);
        Instance bob = graknGraph.addEntity(randomThing);
        Instance alice = graknGraph.addEntity(randomThing);
        Resource birthDate = graknGraph.putResource("10/10/10", resourceType);
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        assertEquals(0, birthDate.ownerInstances().size());

        graknGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, pacino);
        graknGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, jennifer);
        graknGraph.addRelation(hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, bob);
        graknGraph.addRelation(hasResource).
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

        Resource<String> resource1 = graknGraph.putResource("1", strings);
        Resource<Long> resource2 = graknGraph.putResource(1L, longs);
        Resource<Double> resource3 = graknGraph.putResource(1.0, doubles);
        Resource<Boolean> resource4 = graknGraph.putResource(true, booleans);

        Resource<String> resource5 = graknGraph.putResource("5", strings);
        Resource<Long> resource6 = graknGraph.putResource(1L, longs);
        Resource<Double> resource7 = graknGraph.putResource(1.0, doubles);
        Resource<Boolean> resource8 = graknGraph.putResource(true, booleans);

        assertEquals("1", graknGraph.getResource(resource1.getId()).getValue());
        assertEquals(1L, graknGraph.getResource(resource2.getId()).getValue());
        assertEquals(1.0, graknGraph.getResource(resource3.getId()).getValue());
        assertEquals(true, graknGraph.getResource(resource4.getId()).getValue());

        assertThat(graknGraph.getResource(resource1.getId()).getValue(), instanceOf(String.class));
        assertThat(graknGraph.getResource(resource2.getId()).getValue(), instanceOf(Long.class));
        assertThat(graknGraph.getResource(resource3.getId()).getValue(), instanceOf(Double.class));
        assertThat(graknGraph.getResource(resource4.getId()).getValue(), instanceOf(Boolean.class));

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
        graknGraph.putResource("Invalid Thing", longResourceType);
    }

    @Test
    public void datatypeTest2(){
        ResourceType<Double> doubleResourceType = graknGraph.putResourceType("doubleType", ResourceType.DataType.DOUBLE);
        Resource thing = graknGraph.putResource(2.0, doubleResourceType);
        assertEquals(2.0, thing.getValue());
    }

    @Test
    public void testToString() {
        ResourceType<String> concept = graknGraph.putResourceType("a", ResourceType.DataType.STRING);
        Resource<String> concept2 = graknGraph.putResource("concept2", concept);
        assertTrue(concept2.toString().contains("Value"));
    }

    @Test
    public void testInvalidDataType(){
        ResourceType stringResourceType = graknGraph.putResourceType("Strung", ResourceType.DataType.STRING);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("1", String.class.getName()))
        ));
        graknGraph.putResource(1L, stringResourceType);
    }

    @Test
    public void testUniqueResource(){
        //Create Ontology
        RoleType primaryKeyRole = graknGraph.putRoleType("Primary Key Role");
        RoleType entityRole = graknGraph.putRoleType("Entity Role");
        RelationType hasPrimaryKey = graknGraph.putRelationType("Has Parimary Key").hasRole(primaryKeyRole).hasRole(entityRole);


        //Create Resources
        ResourceType primaryKeyType = graknGraph.putResourceTypeUnique("My Primary Key", ResourceType.DataType.STRING).playsRole(primaryKeyRole);
        Resource pimaryKey1 = graknGraph.putResource("A Primary Key 1", primaryKeyType);
        Resource pimaryKey2 = graknGraph.putResource("A Primary Key 2", primaryKeyType);

        //Create Entities
        EntityType entityType = graknGraph.putEntityType("My Entity Type").playsRole(entityRole);
        Entity entity1 = graknGraph.addEntity(entityType);
        Entity entity2 = graknGraph.addEntity(entityType);
        Entity entity3 = graknGraph.addEntity(entityType);

        //Link Entities to resources
        graknGraph.addRelation(hasPrimaryKey).putRolePlayer(primaryKeyRole, pimaryKey1).putRolePlayer(entityRole, entity1);
        graknGraph.addRelation(hasPrimaryKey).putRolePlayer(primaryKeyRole, pimaryKey2).putRolePlayer(entityRole, entity2);

        expectedException.expect(ConceptNotUniqueException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RESOURCE_TYPE_UNIQUE.getMessage(pimaryKey1.getId(), entity1.getId()))
        ));

        graknGraph.addRelation(hasPrimaryKey).putRolePlayer(primaryKeyRole, pimaryKey1).putRolePlayer(entityRole, entity3);
    }
}