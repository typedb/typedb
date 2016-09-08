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

package io.mindmaps.graph.internal;

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MoreThanOneEdgeException;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RoleTypeTest {
    private AbstractMindmapsGraph mindmapsGraph;
    private RoleType roleType;
    private RelationType relationType;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph(){
        mindmapsGraph = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.initialiseMetaConcepts();

        roleType = mindmapsGraph.putRoleType("RoleType");
        relationType = mindmapsGraph.putRelationType("RelationType");
    }
    @After
    public void destroyGraph()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void overrideFail(){
        RelationType relationType = mindmapsGraph.putRelationType("original");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.ID_ALREADY_TAKEN.getMessage("original", relationType.toString()))
        ));


        mindmapsGraph.putRoleType("original");
    }

    @Test
    public void testRoleTypeItemIdentifier(){
        RoleType roleType = mindmapsGraph.putRoleType("test");
        assertEquals("test", roleType.getId());
    }

    @Test
    public void testGetRelation() throws Exception {
        relationType.hasRole(roleType);
        assertEquals(relationType, roleType.relationType());
    }

    @Test
    public void testGetRelationFailNoRelationShip() throws Exception {
        assertNull(roleType.relationType());
    }

    @Test
    public void testGetRelationFailTooManyRelationShip() throws Exception {
        expectedException.expect(MoreThanOneEdgeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.MORE_THAN_ONE_EDGE.getMessage(roleType.toString(), Schema.EdgeLabel.HAS_ROLE.name()))
        ));

        RelationType relationType2 = mindmapsGraph.putRelationType("relationType2");
        relationType.hasRole(roleType);
        relationType2.hasRole(roleType);

        roleType.relationType();
    }

    @Test
    public void testRolePlayerConceptType(){
        Type type1 = mindmapsGraph.putEntityType("CT1").playsRole(roleType);
        Type type2 = mindmapsGraph.putEntityType("CT2").playsRole(roleType);
        Type type3 = mindmapsGraph.putEntityType("CT3").playsRole(roleType);
        Type type4 = mindmapsGraph.putEntityType("CT4").playsRole(roleType);

        assertEquals(4, roleType.playedByTypes().size());
        assertTrue(roleType.playedByTypes().contains(type1));
        assertTrue(roleType.playedByTypes().contains(type2));
        assertTrue(roleType.playedByTypes().contains(type3));
        assertTrue(roleType.playedByTypes().contains(type4));
    }

    @Test
    public  void getInstancesTest(){
        RoleType roleA = mindmapsGraph.putRoleType("roleA");
        RoleType roleB = mindmapsGraph.putRoleType("roleB");
        RelationType relationType = mindmapsGraph.putRelationType("relationType").hasRole(roleA).hasRole(roleB);
        EntityType entityType = mindmapsGraph.putEntityType("entityType").playsRole(roleA).playsRole(roleB);

        Entity a = mindmapsGraph.putEntity("a", entityType);
        Entity b = mindmapsGraph.putEntity("b", entityType);
        Entity c = mindmapsGraph.putEntity("c", entityType);
        Entity d = mindmapsGraph.putEntity("d", entityType);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, b);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(roleA, c).
                putRolePlayer(roleB, d);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, c);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(roleA, c).
                putRolePlayer(roleB, b);

        assertEquals(roleA.instances().size(), 2);
        assertTrue(roleA.instances().contains(a));
        assertTrue(roleA.instances().contains(c));

        assertEquals(roleB.instances().size(), 3);
        assertTrue(roleB.instances().contains(b));
        assertTrue(roleB.instances().contains(c));
        assertTrue(roleB.instances().contains(d));
    }
}