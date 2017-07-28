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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.GraphTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class RoleTest extends GraphTestBase {
    private Role role;
    private RelationType relationType;

    @Before
    public void buildGraph(){
        role = graknGraph.putRole("My Role");
        relationType = graknGraph.putRelationType("RelationType");
    }

    @Test
    public void whenGettingTheRelationTypesARoleIsInvolvedIn_ReturnTheRelationTypes() throws Exception {
        assertThat(role.relationTypes(), empty());
        relationType.relates(role);
        assertThat(role.relationTypes(), containsInAnyOrder(relationType));
    }

    @Test
    public void whenGettingTypeEntityTypesAllowedToPlayARole_ReturnTheEntityTypes(){
        Type type1 = graknGraph.putEntityType("CT1").plays(role);
        Type type2 = graknGraph.putEntityType("CT2").plays(role);
        Type type3 = graknGraph.putEntityType("CT3").plays(role);
        Type type4 = graknGraph.putEntityType("CT4").plays(role);
        assertThat(role.playedByTypes(), containsInAnyOrder(type1, type2, type3, type4));
    }

    @Test
    public void whenDeletingRoleTypeWithTypesWhichCanPlayIt_Throw(){
        Role foundType = graknGraph.getRole("My Role");
        assertNotNull(foundType);
        foundType.delete();
        assertNull(graknGraph.getRole("My Role"));

        Role role = graknGraph.putRole("New Role Type");
        graknGraph.putEntityType("Entity Type").plays(role);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.cannotBeDeleted(role).getMessage());

        role.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRelationTypes_Throw(){
        Role role2 = graknGraph.putRole("New Role Type");
        graknGraph.putRelationType("Thing").relates(role2).relates(role);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.cannotBeDeleted(role2).getMessage());

        role2.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRolePlayers_Throw(){
        Role roleA = graknGraph.putRole("roleA");
        Role roleB = graknGraph.putRole("roleB");
        RelationType relationType = graknGraph.putRelationType("relationTypes").relates(roleA).relates(roleB);
        EntityType entityType = graknGraph.putEntityType("entityType").plays(roleA).plays(roleB);

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();

        relationType.addRelation().addRolePlayer(roleA, a).addRolePlayer(roleB, b);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.cannotBeDeleted(roleA).getMessage());

        roleA.delete();
    }

    @Test
    public void whenAddingRoleTypeToMultipleRelationTypes_EnsureItLinkedToBothRelationTypes() throws InvalidGraphException {
        Role roleA = graknGraph.putRole("roleA");
        Role roleB = graknGraph.putRole("roleB");
        relationType.relates(roleA).relates(role);
        RelationType relationType2 = graknGraph.putRelationType("relationType2").relates(roleB).relates(role);
        graknGraph.commit();

        assertThat(roleA.relationTypes(), containsInAnyOrder(relationType));
        assertThat(roleB.relationTypes(), containsInAnyOrder(relationType2));
        assertThat(role.relationTypes(), containsInAnyOrder(relationType, relationType2));
    }
}