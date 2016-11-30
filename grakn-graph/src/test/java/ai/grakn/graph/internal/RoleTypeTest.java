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
import ai.grakn.concept.RelationType;
import ai.grakn.util.ErrorMessage;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.MoreThanOneEdgeException;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RoleTypeTest extends GraphTestBase {
    private RoleType roleType;
    private RelationType relationType;

    @Before
    public void buildGraph(){
        roleType = graknGraph.putRoleType("RoleType");
        relationType = graknGraph.putRelationType("RelationType");
    }

    @Test
    public void overrideFail(){
        RelationType relationType = graknGraph.putRelationType("original");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.ID_ALREADY_TAKEN.getMessage("original", relationType.toString()))
        ));


        graknGraph.putRoleType("original");
    }

    @Test
    public void testRoleTypeName(){
        RoleType roleType = graknGraph.putRoleType("test");
        assertEquals("test", roleType.getName());
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

        RelationType relationType2 = graknGraph.putRelationType("relationType2");
        relationType.hasRole(roleType);
        relationType2.hasRole(roleType);

        roleType.relationType();
    }

    @Test
    public void testRolePlayerConceptType(){
        Type type1 = graknGraph.putEntityType("CT1").playsRole(roleType);
        Type type2 = graknGraph.putEntityType("CT2").playsRole(roleType);
        Type type3 = graknGraph.putEntityType("CT3").playsRole(roleType);
        Type type4 = graknGraph.putEntityType("CT4").playsRole(roleType);

        assertEquals(4, roleType.playedByTypes().size());
        assertTrue(roleType.playedByTypes().contains(type1));
        assertTrue(roleType.playedByTypes().contains(type2));
        assertTrue(roleType.playedByTypes().contains(type3));
        assertTrue(roleType.playedByTypes().contains(type4));
    }

    @Test
    public void testPlayedByTypes(){
        RoleType crewMember = graknGraph.putRoleType("crew-member").setAbstract(true);
        EntityType person = graknGraph.putEntityType("person").playsRole(crewMember);

        assertEquals(1, crewMember.playedByTypes().size());
        assertEquals(person, crewMember.playedByTypes().iterator().next());
    }

    @Test
    public  void getInstancesTest(){
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        RelationType relationType = graknGraph.putRelationType("relationType").hasRole(roleA).hasRole(roleB);
        EntityType entityType = graknGraph.putEntityType("entityType").playsRole(roleA).playsRole(roleB);

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();
        Entity c = entityType.addEntity();
        Entity d = entityType.addEntity();

        relationType.addRelation().
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, b);

        relationType.addRelation().
                putRolePlayer(roleA, c).
                putRolePlayer(roleB, d);

        relationType.addRelation().
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, c);

        relationType.addRelation().
                putRolePlayer(roleA, c).
                putRolePlayer(roleB, b);

        assertEquals(roleA.instances().size(), 0);
        assertEquals(roleB.instances().size(), 0);
    }
}