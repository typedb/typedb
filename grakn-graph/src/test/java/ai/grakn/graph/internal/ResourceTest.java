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
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

public class ResourceTest extends GraphTestBase{
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
        RoleType resourceRole = graknGraph.putRoleType("Resource Role");
        RoleType actorRole = graknGraph.putRoleType("Actor");
        Instance pacino = randomThing.addEntity();
        Instance jennifer = randomThing.addEntity();
        Instance bob = randomThing.addEntity();
        Instance alice = randomThing.addEntity();
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
        ResourceType longResourceType = graknGraph.putResourceType("long", ResourceType.DataType.LONG);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(INVALID_DATATYPE.getMessage("Invalid Thing", Long.class.getName()));
        longResourceType.putResource("Invalid Thing");
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