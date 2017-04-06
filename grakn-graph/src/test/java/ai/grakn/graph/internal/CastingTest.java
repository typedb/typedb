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
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;


public class CastingTest extends GraphTestBase{
    private CastingImpl casting1;
    private CastingImpl casting2;
    private Entity entity1;
    private Entity entity2;
    private RoleType role1;
    private RoleType role2;
    private Relation relation;

    @Before
    public void crateRelation() {
        role1 = graknGraph.putRoleType("role 1");
        role2 = graknGraph.putRoleType("role 2");
        RelationType relationType = graknGraph.putRelationType("Relation Type").relates(role1).relates(role2);
        EntityType entityType = graknGraph.putEntityType("An Entity Type").plays(role1).plays(role2);

        entity1 = entityType.addEntity();
        entity2 = entityType.addEntity();

        relation = relationType.addRelation().addRolePlayer(role1, entity1).addRolePlayer(role2, entity2);

        //Get castings via internal index
        casting1 = graknGraph.getConcept(Schema.ConceptProperty.INDEX,
                CastingImpl.generateNewHash((RoleTypeImpl) role1, (InstanceImpl) entity1));
        casting2 = graknGraph.getConcept(Schema.ConceptProperty.INDEX,
                CastingImpl.generateNewHash((RoleTypeImpl) role2, (InstanceImpl) entity2));

        assertNotNull(casting1);
        assertNotNull(casting2);
        assertNotEquals(casting1, casting2);
    }

    @Test
    public void whenGettingRolePlayerViaCasting_ReturnRolePlayer() {
        assertEquals(entity1, casting1.getRolePlayer());
        assertEquals(entity2, casting2.getRolePlayer());
    }

    @Test
    public void whenGettingRoleViaCasting_ReturnRole(){
        assertEquals(role1, casting1.getRole());
        assertEquals(role2, casting2.getRole());
    }

    @Test
    public void whenGettingRelationViaCasting_ReturnRelation(){
        assertThat(casting1.getRelations(), containsInAnyOrder(relation));
        assertThat(casting2.getRelations(), containsInAnyOrder(relation));
    }
}