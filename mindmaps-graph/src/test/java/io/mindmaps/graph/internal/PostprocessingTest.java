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

package io.mindmaps.graph.internal;

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());
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

        graph.fixDuplicateCasting(mainCasting.getBaseIdentifier());
        assertEquals(1, instance1.castings().size());
    }

    private void buildDuplicateCastingWithNewRelation(RelationType relationType, RoleTypeImpl mainRoleType, InstanceImpl mainInstance, RoleType otherRoleType, InstanceImpl otherInstance){
        RelationImpl relation = (RelationImpl) graph.addRelation(relationType).putRolePlayer(otherRoleType, otherInstance);

        //Create Fake Casting
        Vertex castingVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleType.getVertex());

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstance.getVertex());
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleType.getId());

        edge = relation.getVertex().addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleType.getId());

        putFakeShortcutEdge(relationType, relation, mainRoleType, mainInstance, otherRoleType, otherInstance);
        putFakeShortcutEdge(relationType, relation, otherRoleType, otherInstance, mainRoleType, mainInstance);
    }

    private void putFakeShortcutEdge(RelationType relationType, Relation relation, RoleType fromRole, InstanceImpl fromInstance, RoleType toRole, InstanceImpl toInstance){
        Edge tinkerEdge = fromInstance.getVertex().addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), toInstance.getVertex());
        EdgeImpl edge = new EdgeImpl(tinkerEdge, graph);

        edge.setProperty(Schema.EdgeProperty.RELATION_TYPE_ID, relationType.getId());
        edge.setProperty(Schema.EdgeProperty.RELATION_ID, relation.getId());

        if (fromInstance.getId() != null)
            edge.setProperty(Schema.EdgeProperty.FROM_ID, fromInstance.getId());
        edge.setProperty(Schema.EdgeProperty.FROM_ROLE, fromRole.getId());

        if (toInstance.getId() != null)
            edge.setProperty(Schema.EdgeProperty.TO_ID, toInstance.getId());
        edge.setProperty(Schema.EdgeProperty.TO_ROLE, toRole.getId());

        edge.setProperty(Schema.EdgeProperty.FROM_TYPE, fromInstance.getParentIsa().getId());
        edge.setProperty(Schema.EdgeProperty.TO_TYPE, toInstance.getParentIsa().getId());
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
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());

        graph.fixDuplicateCasting(mainCasting.getBaseIdentifier());

        assertEquals(2, instance1.relations().size());
        assertEquals(1, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(4, graph.getTinkerTraversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());

    }

    @Test
    public void testMergingResourcesSimple(){
        ResourceType resourceType = graph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        //Create fake resources
        Set<Object> resourceIds = new HashSet<>();
        resourceIds.add(createFakeResource(resourceType, "1").getBaseIdentifier());
        resourceIds.add(createFakeResource(resourceType, "1").getBaseIdentifier());
        resourceIds.add(createFakeResource(resourceType, "1").getBaseIdentifier());
        resourceIds.add(createFakeResource(resourceType, "2").getBaseIdentifier());
        resourceIds.add(createFakeResource(resourceType, "3").getBaseIdentifier());

        //Check we have duplicate resources
        assertEquals(5, resourceType.instances().size());

        //Fix duplicates
        graph.fixDuplicateResources(resourceIds);

        //Check we no longer have duplicates
        assertEquals(3, resourceType.instances().size());
    }

    @Test
    public void testMergingResourcesWithRelations(){
        RoleType roleEntity = graph.putRoleType("A Entity Role Type");
        RoleType roleResource = graph.putRoleType("A Resource Role Type");
        RelationType relationType = graph.putRelationType("A Relation Type").hasRole(roleEntity).hasRole(roleResource);
        ResourceType<String> resourceType = graph.putResourceType("Resource Type", ResourceType.DataType.STRING).playsRole(roleResource);
        EntityType entityType = graph.putEntityType("An Entity Type").playsRole(roleEntity);
        Entity e1 = graph.addEntity(entityType);
        Entity e2 = graph.addEntity(entityType);
        Entity e3 = graph.addEntity(entityType);

        //Create fake resources
        Set<Object> resourceIds = new HashSet<>();
        ResourceImpl<?> r1 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r11 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r111 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r2 = createFakeResource(resourceType, "2");
        ResourceImpl<?> r3 = createFakeResource(resourceType, "3");

        resourceIds.add(r1.getBaseIdentifier());
        resourceIds.add(r11.getBaseIdentifier());
        resourceIds.add(r111.getBaseIdentifier());
        resourceIds.add(r2.getBaseIdentifier());
        resourceIds.add(r3.getBaseIdentifier());

        //Give resources some relationships
        graph.addRelation(relationType).putRolePlayer(roleResource, r1).putRolePlayer(roleEntity, e1);
        graph.addRelation(relationType).putRolePlayer(roleResource, r11).putRolePlayer(roleEntity, e1); //When merging this relation should not be absorbed
        graph.addRelation(relationType).putRolePlayer(roleResource, r11).putRolePlayer(roleEntity, e2); //Absorb
        graph.addRelation(relationType).putRolePlayer(roleResource, r111).putRolePlayer(roleEntity, e2); //Don't Absorb
        graph.addRelation(relationType).putRolePlayer(roleResource, r111).putRolePlayer(roleEntity, e3); //Absorb

        //Check everything is broken
        assertEquals(5, resourceType.instances().size());
        assertEquals(1, r1.relations().size());
        assertEquals(2, r11.relations().size());
        assertEquals(1, r1.relations().size());
        assertEquals(6, graph.getTinkerTraversal().V().hasLabel(Schema.BaseType.RELATION.name()).toList().size());

        r1.relations().forEach(rel -> assertTrue(rel.rolePlayers().values().contains(e1)));

        //Now fix everything
        graph.fixDuplicateResources(resourceIds);

        //Check everything is in order
        assertEquals(3, resourceType.instances().size());

        //Get back the surviving resource
        Resource<String> foundR1 = null;
        for (Resource<String> resource : resourceType.instances()) {
            if(resource.getValue().equals("1")){
                foundR1 = resource;
                break;
            }
        }

        assertNotNull(foundR1);
        assertEquals(3, foundR1.relations().size());
        assertTrue(foundR1.ownerInstances().contains(e1));
        assertTrue(foundR1.ownerInstances().contains(e2));

        assertEquals(4, graph.getTinkerTraversal().V().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
    }


    private ResourceImpl createFakeResource(ResourceType type, String value){
        String index = ResourceImpl.generateResourceIndex(type.getId(), value);
        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), ((ResourceTypeImpl)type).getVertex());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(), index);
        resourceVertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), UUID.randomUUID().toString());
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), value);

        return new ResourceImpl(resourceVertex, graph);
    }
}
