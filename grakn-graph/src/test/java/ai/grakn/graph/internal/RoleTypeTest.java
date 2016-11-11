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
import ai.grakn.concept.RelationType;
import ai.grakn.util.ErrorMessage;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.MoreThanOneEdgeException;
import ai.grakn.util.ErrorMessage;
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
        roleType = mindmapsGraph.putRoleType("RoleType");
        relationType = mindmapsGraph.putRelationType("RelationType");
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
    public void testPlayedByTypes(){
        RoleType crewMember = mindmapsGraph.putRoleType("crew-member").setAbstract(true);
        EntityType person = mindmapsGraph.putEntityType("person").playsRole(crewMember);
        RoleType productionDesigner = mindmapsGraph.putRoleType("production-designer").superType(crewMember);

        assertEquals(1, productionDesigner.playedByTypes().size());
        assertEquals(person, productionDesigner.playedByTypes().iterator().next());
    }

    @Test
    public  void getInstancesTest(){
        RoleType roleA = mindmapsGraph.putRoleType("roleA");
        RoleType roleB = mindmapsGraph.putRoleType("roleB");
        RelationType relationType = mindmapsGraph.putRelationType("relationType").hasRole(roleA).hasRole(roleB);
        EntityType entityType = mindmapsGraph.putEntityType("entityType").playsRole(roleA).playsRole(roleB);

        Entity a = mindmapsGraph.addEntity(entityType);
        Entity b = mindmapsGraph.addEntity(entityType);
        Entity c = mindmapsGraph.addEntity(entityType);
        Entity d = mindmapsGraph.addEntity(entityType);

        mindmapsGraph.addRelation(relationType).
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, b);

        mindmapsGraph.addRelation(relationType).
                putRolePlayer(roleA, c).
                putRolePlayer(roleB, d);

        mindmapsGraph.addRelation(relationType).
                putRolePlayer(roleA, a).
                putRolePlayer(roleB, c);

        mindmapsGraph.addRelation(relationType).
                putRolePlayer(roleA, c).
                putRolePlayer(roleB, b);

        assertEquals(roleA.instances().size(), 0);
        assertEquals(roleB.instances().size(), 0);
    }
}