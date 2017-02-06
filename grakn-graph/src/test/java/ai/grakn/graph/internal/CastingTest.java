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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CastingTest extends GraphTestBase{

    private CastingImpl casting;
    private RoleTypeImpl role;
    private RelationImpl relation;
    private InstanceImpl rolePlayer;


    @Before
    public void setUp() {
        role = (RoleTypeImpl) graknGraph.putRoleType("Role");
        EntityTypeImpl conceptType = (EntityTypeImpl) graknGraph.putEntityType("A thing");
        rolePlayer = (InstanceImpl) conceptType.addEntity();
        RelationTypeImpl relationType = (RelationTypeImpl) graknGraph.putRelationType("A type");
        relation = (RelationImpl) relationType.addRelation();
        casting = graknGraph.putCasting(role, rolePlayer, relation);
    }

    @Test
    public void testEquals() throws Exception {
        Graph graph = graknGraph.getTinkerPopGraph();
        Vertex v = graph.traversal().V(relation.getId().getRawValue()).out(Schema.EdgeLabel.CASTING.getLabel()).next();
        CastingImpl castingCopy = graknGraph.getConcept(ConceptId.of(v.id().toString()));
        assertEquals(casting, castingCopy);

        EntityType type = graknGraph.putEntityType("Another entity type");
        RoleTypeImpl role = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        InstanceImpl rolePlayer = (InstanceImpl) type.addEntity();
        CastingImpl casting2 = graknGraph.putCasting(role, rolePlayer, relation);
        assertNotEquals(casting, casting2);
    }

    @Test
    public void testGetRolePlayer() throws Exception {
        assertEquals(rolePlayer, casting.getRolePlayer());
    }

    @Test (expected = RuntimeException.class)
    public void testGetRolePlayerFail() throws Exception {
        Concept anotherConcept = graknGraph.putEntityType("ac'");
        casting.addEdge((ConceptImpl) anotherConcept, Schema.EdgeLabel.ROLE_PLAYER);
        casting.getRolePlayer();
    }
}