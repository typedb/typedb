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

package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class ResourceTest {

    private MindmapsTransactionImpl mindmapsGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph() {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        mindmapsGraph.initialiseMetaConcepts();
    }

    @Test
    public void testDataType() throws Exception {
        ResourceType resourceType = mindmapsGraph.putResourceType("resourceType", Data.STRING);
        Resource resource = mindmapsGraph.putResource("resource", resourceType);
        assertEquals(Data.STRING, resource.dataType());
    }

    @Test
    public void testOverride(){
        ResourceType resourceType = mindmapsGraph.putResourceType("resourceType", Data.STRING);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.ID_ALREADY_TAKEN.getMessage("resourceType", resourceType.toString()))
        ));

        Resource resource = mindmapsGraph.putResource("resourceType", resourceType);
    }

    @Test
    public void testOwnerInstances() throws Exception {
        EntityType randomThing = mindmapsGraph.putEntityType("A Thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A Resource Thing", Data.STRING);
        RelationType hasResource = mindmapsGraph.putRelationType("Has Resource");
        RoleType resourceRole = mindmapsGraph.putRoleType("Resource Role");
        RoleType actorRole = mindmapsGraph.putRoleType("Actor");
        Instance pacino = mindmapsGraph.putEntity("pacino", randomThing);
        Instance jennifer = mindmapsGraph.putEntity("jennifer", randomThing);
        Instance bob = mindmapsGraph.putEntity("bob", randomThing);
        Instance alice = mindmapsGraph.putEntity("alice", randomThing);
        Resource birthDate = mindmapsGraph.putResource("10/10/10", resourceType);
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        assertEquals(0, birthDate.ownerInstances().size());

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, pacino);
        mindmapsGraph.putRelation(UUID.randomUUID().toString(), hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, jennifer);
        mindmapsGraph.putRelation(UUID.randomUUID().toString(), hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, bob);
        mindmapsGraph.putRelation(UUID.randomUUID().toString(), hasResource).
                putRolePlayer(resourceRole, birthDate).putRolePlayer(actorRole, alice);

        assertEquals(4, birthDate.ownerInstances().size());
        assertTrue(birthDate.ownerInstances().contains(pacino));
        assertTrue(birthDate.ownerInstances().contains(jennifer));
        assertTrue(birthDate.ownerInstances().contains(bob));
        assertTrue(birthDate.ownerInstances().contains(alice));
    }

    @Test
    public void checkResourceDataTypes(){
        ResourceType<String> strings = mindmapsGraph.putResourceType("String Type", Data.STRING);
        ResourceType<Long> longs = mindmapsGraph.putResourceType("Long Type", Data.LONG);
        ResourceType<Double> doubles = mindmapsGraph.putResourceType("Double Type", Data.DOUBLE);
        ResourceType<Boolean> booleans = mindmapsGraph.putResourceType("Boolean Type", Data.BOOLEAN);

        mindmapsGraph.putResource("1", strings).setValue("1");
        mindmapsGraph.putResource("2", longs).setValue(1L);
        mindmapsGraph.putResource("3", doubles).setValue(1.0);
        mindmapsGraph.putResource("4", booleans).setValue(true);

        mindmapsGraph.putResource("5", strings).setValue("1");
        mindmapsGraph.putResource("6", longs).setValue(1L);
        mindmapsGraph.putResource("7", doubles).setValue(1.0);
        mindmapsGraph.putResource("8", booleans).setValue(true);

        assertEquals("1", mindmapsGraph.getResource("1").getValue());
        assertEquals(1L, mindmapsGraph.getResource("2").getValue());
        assertEquals(1.0, mindmapsGraph.getResource("3").getValue());
        assertEquals(true, mindmapsGraph.getResource("4").getValue());

        assertThat(mindmapsGraph.getResource("1").getValue(), instanceOf(String.class));
        assertThat(mindmapsGraph.getResource("2").getValue(), instanceOf(Long.class));
        assertThat(mindmapsGraph.getResource("3").getValue(), instanceOf(Double.class));
        assertThat(mindmapsGraph.getResource("4").getValue(), instanceOf(Boolean.class));

        assertEquals(2, mindmapsGraph.getConceptsByValue("1").size());
        assertEquals(2, mindmapsGraph.getConceptsByValue(1).size());
        assertEquals(2, mindmapsGraph.getResourcesByValue("1").size());
        assertEquals(2, mindmapsGraph.getResourcesByValue(1L).size());
        assertEquals(2, mindmapsGraph.getResourcesByValue(1.0).size());
        assertEquals(2, mindmapsGraph.getResourcesByValue(true).size());
    }

    @Test
    public void setInvalidResourceTest (){
        ResourceType<Long> longResourceType = mindmapsGraph.putResourceType("long", Data.LONG);
        Resource invalidThing = mindmapsGraph.putResource("Invalid Thing", longResourceType);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("Bad Type", invalidThing.toString(), Data.LONG.getName()))
        ));

        invalidThing.setValue("Bad Type");
    }

    @Test
    public void datatypeTest(){
        ResourceType<Long> longResourceType = mindmapsGraph.putResourceType("longType", Data.LONG);
        Resource thing = mindmapsGraph.putResource("long", longResourceType);
        thing.setValue(2);

        assertEquals(2L, thing.getValue());

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_DATATYPE.getMessage("2.0", thing, Data.LONG.getName()))
        ));

        thing.setValue(2.0);
    }

    @Test
    public void datatypeTest2(){
        ResourceType<Double> doubleResourceType = mindmapsGraph.putResourceType("doubleType", Data.DOUBLE);
        Resource thing = mindmapsGraph.putResource("double", doubleResourceType);
        thing.setValue(2);
        assertEquals(2.0, thing.getValue());
    }
}