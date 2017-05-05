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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class RoleTypeTest extends GraphTestBase {
    private RoleType roleType;
    private RelationType relationType;

    @Before
    public void buildGraph(){
        roleType = graknGraph.putRoleType("RoleType");
        relationType = graknGraph.putRelationType("RelationType");
    }

    @Test
    public void whenGettingTheRelationTypesARoleIsInvolvedIn_ReturnTheRelationTypes() throws Exception {
        assertThat(roleType.relationTypes(), empty());
        relationType.relates(roleType);
        assertThat(roleType.relationTypes(), containsInAnyOrder(relationType));
    }

    @Test
    public void whenSettingRoleTypeToPlayItself_Throw(){
        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.ROLE_TYPE_ERROR.getMessage(roleType.getLabel()));
        roleType.plays(roleType);
    }

    @Test
    public void whenGettingTypeEntityTypesAllowedToPlayARole_ReturnTheEntityTypes(){
        Type type1 = graknGraph.putEntityType("CT1").plays(roleType);
        Type type2 = graknGraph.putEntityType("CT2").plays(roleType);
        Type type3 = graknGraph.putEntityType("CT3").plays(roleType);
        Type type4 = graknGraph.putEntityType("CT4").plays(roleType);
        assertThat(roleType.playedByTypes(), containsInAnyOrder(type1, type2, type3, type4));
    }

    @Test
    public void whenGettingInstancesOfARoleType_EnsureNothingIsReturned(){
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        RelationType relationType = graknGraph.putRelationType("relationTypes").relates(roleA).relates(roleB);
        EntityType entityType = graknGraph.putEntityType("entityType").plays(roleA).plays(roleB);

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();
        Entity c = entityType.addEntity();
        Entity d = entityType.addEntity();

        relationType.addRelation().addRolePlayer(roleA, a).addRolePlayer(roleB, b);
        relationType.addRelation().addRolePlayer(roleA, c).addRolePlayer(roleB, d);
        relationType.addRelation().addRolePlayer(roleA, a).addRolePlayer(roleB, c);
        relationType.addRelation().addRolePlayer(roleA, c).addRolePlayer(roleB, b);

        assertThat(roleA.instances(), empty());
        assertThat(roleB.instances(), empty());
    }

    @Test
    public void whenDeletingRoleTypeWithTypesWhichCanPlayIt_Throw(){
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
    public void whenDeletingRoleTypeWithRelationTypes_Throw(){
        RoleType roleType2 = graknGraph.putRoleType("New Role Type");
        graknGraph.putRelationType("Thing").relates(roleType2).relates(roleType);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleType2.getLabel()));

        roleType2.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRolePlayers_Throw(){
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        EntityType entityType = graknGraph.putEntityType("entityType");

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();

        relationType.addRelation().addRolePlayer(roleA, a).addRolePlayer(roleB, b);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(roleA.getLabel()));

        roleA.delete();
    }

    @Test
    public void whenAddingRoleTypeToMultipleRelationTypes_EnsureItLinkedToBothRelationTypes() throws GraknValidationException {
        RoleType roleA = graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        relationType.relates(roleA).relates(roleType);
        RelationType relationType2 = graknGraph.putRelationType("relationType2").relates(roleB).relates(roleType);
        graknGraph.commit();

        assertThat(roleA.relationTypes(), containsInAnyOrder(relationType));
        assertThat(roleB.relationTypes(), containsInAnyOrder(relationType2));
        assertThat(roleType.relationTypes(), containsInAnyOrder(relationType, relationType2));
    }

    @Test
    public void whenAddingRolePlayers_EnsureTheTypeOfTheResultingCastingIsTheRoleType(){
        RoleTypeImpl roleA = (RoleTypeImpl) graknGraph.putRoleType("roleA");
        RoleType roleB = graknGraph.putRoleType("roleB");
        EntityType entityType = graknGraph.putEntityType("entityType");
        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();

        relationType.addRelation().addRolePlayer(roleA, a).addRolePlayer(roleB, b);

        CastingImpl casting = roleA.castings().iterator().next();
        assertEquals(roleA, casting.type());
    }
}