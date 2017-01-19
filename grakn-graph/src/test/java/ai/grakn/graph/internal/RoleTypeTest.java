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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.ErrorMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    public void testOverrideFail(){
        RelationType relationType = graknGraph.putRelationType("original");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage("original", relationType.toString()));


        graknGraph.putRoleType("original");
    }

    @Test
    public void testRoleTypeName(){
        RoleType roleType = graknGraph.putRoleType("test");
        assertEquals("test", roleType.getName().getValue());
    }

    @Test
    public void testGetRelation() throws Exception {
        relationType.hasRole(roleType);
        assertEquals(relationType, roleType.relationTypes().iterator().next());
    }

    @Test
    public void testGetRelationFailNoRelationShip() throws Exception {
        assertTrue(roleType.relationTypes().isEmpty());
    }

    @Test
    public void testRoleTypeCannotPlayItself(){
        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.ROLE_TYPE_ERROR.getMessage(roleType.getName()));

        roleType.playsRole(roleType);
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
    public  void testGetInstancesTest(){
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        RelationType relationType = graknGraph.putRelationType("relationTypes").hasRole(roleA).hasRole(roleB);
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

    @Test
    public void testDeleteRoleTypeWithPlaysRole(){
        assertNotNull(graknGraph.getRoleType("RoleType"));
        graknGraph.getRoleType("RoleType").delete();
        assertNull(graknGraph.getRoleType("RoleType"));

        RoleType roleType = graknGraph.putRoleType("New Role Type");
        graknGraph.putEntityType("Entity Type").playsRole(roleType);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleType.getName()));

        roleType.delete();
    }

    @Test
    public void testDeleteRoleTypeWithHasRole(){
        RoleType roleType2 = graknGraph.putRoleType("New Role Type");
        graknGraph.putRelationType("Thing").hasRole(roleType2).hasRole(roleType);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleType2.getName()));

        roleType2.delete();
    }

    @Test
    public void testDeleteRoleTypeWithPlayers(){
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        EntityType entityType = graknGraph.putEntityType("entityType");

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();

        relationType.addRelation().
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, b);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleA.getName()));

        roleA.delete();
    }

    @Test
    public void testSharingRole() throws GraknValidationException {
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        relationType.hasRole(roleA).hasRole(roleType);
        RelationType relationType2 = graknGraph.putRelationType("relationType2").hasRole(roleB).hasRole(roleType);
        graknGraph.commit();

        assertEquals(1, roleA.relationTypes().size());
        assertEquals(1, roleB.relationTypes().size());
        assertTrue(roleA.relationTypes().contains(relationType));
        assertTrue(roleB.relationTypes().contains(relationType2));

        assertEquals(2, roleType.relationTypes().size());
        assertTrue(roleType.relationTypes().contains(relationType));
        assertTrue(roleType.relationTypes().contains(relationType2));
    }
}