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

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import org.junit.Test;

import java.time.LocalDateTime;

import static ai.grakn.util.ErrorMessage.INVALID_DATATYPE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResourceTest extends GraphTestBase{
    @Test
    public void testDataType() throws Exception {
        ResourceType<String> resourceType = graknGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        Resource resource = resourceType.putResource("resource");
        assertEquals(ResourceType.DataType.STRING, resource.dataType());
    }

    @Test
    public void testOwnerInstances() throws Exception {
        EntityType randomThing = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType hasResource = graknGraph.putRelationType("Has Resource");
        RoleType resourceRole = graknGraph.putRoleType("Resource Role");
        RoleType actorRole = graknGraph.putRoleType("Actor");
        Instance pacino = randomThing.addEntity();
        Instance jennifer = randomThing.addEntity();
        Instance bob = randomThing.addEntity();
        Instance alice = randomThing.addEntity();
        Resource birthDate = resourceType.putResource("10/10/10");
        hasResource.relates(resourceRole).relates(actorRole);

        assertEquals(0, birthDate.ownerInstances().size());

        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, pacino);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, jennifer);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, bob);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, alice);

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

        assertEquals("1", graknGraph.<Resource>getConcept(resource1.getId()).getValue());
        assertEquals(1L, graknGraph.<Resource>getConcept(resource2.getId()).getValue());
        assertEquals(1.0, graknGraph.<Resource>getConcept(resource3.getId()).getValue());
        assertEquals(true, graknGraph.<Resource>getConcept(resource4.getId()).getValue());

        assertThat(graknGraph.<Resource>getConcept(resource1.getId()).getValue(), instanceOf(String.class));
        assertThat(graknGraph.<Resource>getConcept(resource2.getId()).getValue(), instanceOf(Long.class));
        assertThat(graknGraph.<Resource>getConcept(resource3.getId()).getValue(), instanceOf(Double.class));
        assertThat(graknGraph.<Resource>getConcept(resource4.getId()).getValue(), instanceOf(Boolean.class));

        assertEquals(1, graknGraph.getResourcesByValue("1").size());
        assertEquals(1, graknGraph.getResourcesByValue(1L).size());
        assertEquals(1, graknGraph.getResourcesByValue(1.0).size());
        assertEquals(1, graknGraph.getResourcesByValue(true).size());
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void setInvalidResourceTest (){
        ResourceType longResourceType = graknGraph.putResourceType("long", ResourceType.DataType.LONG);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(INVALID_DATATYPE.getMessage("Invalid Thing", Long.class.getName()));
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

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidDataType(){
        ResourceType stringResourceType = graknGraph.putResourceType("Strung", ResourceType.DataType.STRING);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(INVALID_DATATYPE.getMessage("1", String.class.getName()));
        stringResourceType.putResource(1L);
    }

    @Test
    public void testNonUniqueResource(){
        ResourceType<String> resourceType = graknGraph.putResourceType("A resourceType", ResourceType.DataType.STRING);
        Resource resource = resourceType.putResource("A Thing");
        assertNull(resource.owner());
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
}