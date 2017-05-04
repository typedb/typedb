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

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

public class RelationTypeTest extends GraphTestBase{
    @Test
    public void whenGettingTheRolesOfRelationTypes_AllTheRolesAreReturned() throws Exception {
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RoleType role3 = graknGraph.putRoleType("role3");
        relationType.relates(role1).relates(role2).relates(role3);
        assertThat(relationType.relates(), containsInAnyOrder(role1, role2, role3));
    }

    @Test
    public void whenMutatingRolesOfRelationType_EnsureRelationTypeRolesAreAlwaysUpdated(){
        RelationType relationType = graknGraph.putRelationType("c1");
        RoleType role1 = graknGraph.putRoleType("c2");
        RoleType role2 = graknGraph.putRoleType("c3");
        assertThat(relationType.relates(), empty());

        relationType.relates(role1).relates(role2);
        assertThat(relationType.relates(), containsInAnyOrder(role1, role2));

        relationType.deleteRelates(role1);
        assertThat(relationType.relates(), containsInAnyOrder(role2));
    }
}