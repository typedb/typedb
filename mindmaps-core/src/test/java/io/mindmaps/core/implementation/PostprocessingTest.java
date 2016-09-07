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

package io.mindmaps.core.implementation;

import io.mindmaps.constants.DataType;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PostprocessingTest {
    private AbstractMindmapsGraph graph;
    private RoleType roleType1;
    private RoleType roleType2;
    private RelationType relationType;
    private InstanceImpl instance1;
    private InstanceImpl instance2;
    private InstanceImpl instance3;
    private InstanceImpl instance4;

    @Before
    public void buildGraphAccessManager(){
        graph = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
        graph.initialiseMetaConcepts();

        roleType1 = graph.putRoleType("role 1");
        roleType2 = graph.putRoleType("role 2");
        relationType = graph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        EntityType thing = graph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);
        instance1 = (InstanceImpl) graph.putEntity("1", thing);
        instance2 = (InstanceImpl) graph.putEntity("2", thing);
        instance3 = (InstanceImpl) graph.putEntity("3", thing);
        instance4 = (InstanceImpl) graph.putEntity("4", thing);

        graph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
        assertEquals(1, instance1.castings().size());
        assertEquals(2, graph.getTinkerPopGraph().traversal().E().
                hasLabel(DataType.EdgeLabel.SHORTCUT.getLabel()).toList().size());
    }
    @After
    public void destroyGraphAccessManager()  throws Exception{
        graph.close();
    }

    @Test
    public void testMergingDuplicateCasting(){
        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();
        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3);
        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance4);
        assertEquals(3, instance1.castings().size());

        graph.fixDuplicateCasting(mainCasting.getId());
        assertEquals(1, instance1.castings().size());
    }

    private void buildDuplicateCastingWithNewRelation(RelationType relationType, RoleTypeImpl mainRoleType, InstanceImpl mainInstance, RoleType otherRoleType, InstanceImpl otherInstance){
        RelationImpl relation = (RelationImpl) graph.addRelation(relationType).putRolePlayer(otherRoleType, otherInstance);

        //Create Fake Casting
        Vertex castingVertex = graph.getTinkerPopGraph().addVertex(DataType.BaseType.CASTING.name());
        castingVertex.addEdge(DataType.EdgeLabel.ISA.getLabel(), mainRoleType.getVertex());

        Edge edge = castingVertex.addEdge(DataType.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstance.getVertex());
        edge.property(DataType.EdgeProperty.ROLE_TYPE.name(), mainRoleType.getId());

        edge = relation.getVertex().addEdge(DataType.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(DataType.EdgeProperty.ROLE_TYPE.name(), mainRoleType.getId());

        putFakeShortcutEdge(relationType, relation, mainRoleType, mainInstance, otherRoleType, otherInstance);
        putFakeShortcutEdge(relationType, relation, otherRoleType, otherInstance, mainRoleType, mainInstance);
    }

    private void putFakeShortcutEdge(RelationType relationType, Relation relation, RoleType fromRole, InstanceImpl fromInstance, RoleType toRole, InstanceImpl toInstance){
        Edge tinkerEdge = fromInstance.getVertex().addEdge(DataType.EdgeLabel.SHORTCUT.getLabel(), toInstance.getVertex());
        EdgeImpl edge = new EdgeImpl(tinkerEdge, graph);

        edge.setProperty(DataType.EdgeProperty.RELATION_TYPE_ID, relationType.getId());
        edge.setProperty(DataType.EdgeProperty.RELATION_ID, relation.getId());

        if (fromInstance.getId() != null)
            edge.setProperty(DataType.EdgeProperty.FROM_ID, fromInstance.getId());
        edge.setProperty(DataType.EdgeProperty.FROM_ROLE, fromRole.getId());

        if (toInstance.getId() != null)
            edge.setProperty(DataType.EdgeProperty.TO_ID, toInstance.getId());
        edge.setProperty(DataType.EdgeProperty.TO_ROLE, toRole.getId());

        edge.setProperty(DataType.EdgeProperty.FROM_TYPE, fromInstance.getParentIsa().getId());
        edge.setProperty(DataType.EdgeProperty.TO_TYPE, toInstance.getParentIsa().getId());
    }

    @Test
    public void testMergingDuplicateRelationsDueToDuplicateCastings() {
        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();

        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance2);
        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3);

        assertEquals(3, instance1.relations().size());
        assertEquals(2, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(6, graph.getTinkerPopGraph().traversal().E().
                hasLabel(DataType.EdgeLabel.SHORTCUT.getLabel()).toList().size());

        graph.fixDuplicateCasting(mainCasting.getId());

        assertEquals(2, instance1.relations().size());
        assertEquals(1, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(4, graph.getTinkerTraversal().E().
                hasLabel(DataType.EdgeLabel.SHORTCUT.getLabel()).toList().size());

    }
}
