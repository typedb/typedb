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
    public void testRoleTypeLabel(){
        RoleType roleType = graknGraph.putRoleType("test");
        assertEquals("test", roleType.getLabel().getValue());
    }

    @Test
    public void testGetRelation() throws Exception {
        relationType.relates(roleType);
        assertEquals(relationType, roleType.relationTypes().iterator().next());
    }

    @Test
    public void testGetRelationFailNoRelationShip() throws Exception {
        assertTrue(roleType.relationTypes().isEmpty());
    }

    @Test
    public void testRoleTypeCannotPlayItself(){
        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.ROLE_TYPE_ERROR.getMessage(roleType.getLabel()));

        roleType.plays(roleType);
    }

    @Test
    public void testRolePlayerConceptType(){
        Type type1 = graknGraph.putEntityType("CT1").plays(roleType);
        Type type2 = graknGraph.putEntityType("CT2").plays(roleType);
        Type type3 = graknGraph.putEntityType("CT3").plays(roleType);
        Type type4 = graknGraph.putEntityType("CT4").plays(roleType);

        assertEquals(4, roleType.playedByTypes().size());
        assertTrue(roleType.playedByTypes().contains(type1));
        assertTrue(roleType.playedByTypes().contains(type2));
        assertTrue(roleType.playedByTypes().contains(type3));
        assertTrue(roleType.playedByTypes().contains(type4));
    }

    @Test
    public void testPlayedByTypes(){
        RoleType crewMember = graknGraph.putRoleType("crew-member").setAbstract(true);
        EntityType human = graknGraph.putEntityType("human").plays(crewMember);
        EntityType person = graknGraph.putEntityType("person").superType(human);

        assertEquals(2, crewMember.playedByTypes().size());
        assertTrue(crewMember.playedByTypes().contains(human));
        assertTrue(crewMember.playedByTypes().contains(person));
    }

    @Test
    public  void testGetInstancesTest(){
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        RelationType relationType = graknGraph.putRelationType("relationTypes").relates(roleA).relates(roleB);
        EntityType entityType = graknGraph.putEntityType("entityType").plays(roleA).plays(roleB);

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();
        Entity c = entityType.addEntity();
        Entity d = entityType.addEntity();

        relationType.addRelation().
                addRolePlayer(roleA, a).
                addRolePlayer(roleB, b);

        relationType.addRelation().
                addRolePlayer(roleA, c).
                addRolePlayer(roleB, d);

        relationType.addRelation().
                addRolePlayer(roleA, a).
                addRolePlayer(roleB, c);

        relationType.addRelation().
                addRolePlayer(roleA, c).
                addRolePlayer(roleB, b);

        assertEquals(roleA.instances().size(), 0);
        assertEquals(roleB.instances().size(), 0);
    }

    @Test
    public void testDeleteRoleTypeWithPlays(){
        assertNotNull(graknGraph.getRoleType("RoleType"));
        graknGraph.getRoleType("RoleType").delete();
        assertNull(graknGraph.getRoleType("RoleType"));

        RoleType roleType = graknGraph.putRoleType("New Role Type");
        graknGraph.putEntityType("Entity Type").plays(roleType);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleType.getLabel()));

        roleType.delete();
    }

    @Test
    public void testDeleteRoleTypeWithRelates(){
        RoleType roleType2 = graknGraph.putRoleType("New Role Type");
        graknGraph.putRelationType("Thing").relates(roleType2).relates(roleType);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleType2.getLabel()));

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
                addRolePlayer(roleA, a).
                addRolePlayer(roleB, b);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleA.getLabel()));

        roleA.delete();
    }

    @Test
    public void testSharingRole() throws GraknValidationException {
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        relationType.relates(roleA).relates(roleType);
        RelationType relationType2 = graknGraph.putRelationType("relationType2").relates(roleB).relates(roleType);
        graknGraph.commit();

        assertEquals(1, roleA.relationTypes().size());
        assertEquals(1, roleB.relationTypes().size());
        assertTrue(roleA.relationTypes().contains(relationType));
        assertTrue(roleB.relationTypes().contains(relationType2));

        assertEquals(2, roleType.relationTypes().size());
        assertTrue(roleType.relationTypes().contains(relationType));
        assertTrue(roleType.relationTypes().contains(relationType2));
    }

    @Test
    public void testCastingsAreReturnedFromRoleType(){
        RoleTypeImpl roleA = (RoleTypeImpl) graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        EntityType entityType = graknGraph.putEntityType("entityType");
        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();

        relationType.addRelation().
                addRolePlayer(roleA, a).
                addRolePlayer(roleB, b);

        assertEquals(1, roleA.castings().size());
        CastingImpl casting = roleA.castings().iterator().next();
        assertEquals(roleA, casting.type());
    }
}