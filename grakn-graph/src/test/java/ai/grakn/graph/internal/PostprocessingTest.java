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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PostprocessingTest extends GraphTestBase{
    private RoleType roleType1;
    private RoleType roleType2;
    private RelationType relationType;
    private InstanceImpl instance1;
    private InstanceImpl instance2;
    private InstanceImpl instance3;
    private InstanceImpl instance4;

    @Before
    public void buildGraphAccessManager(){
        roleType1 = graknGraph.putRoleType("role 1");
        roleType2 = graknGraph.putRoleType("role 2");
        relationType = graknGraph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        EntityType thing = graknGraph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);
        instance1 = (InstanceImpl) thing.addEntity();
        instance2 = (InstanceImpl) thing.addEntity();
        instance3 = (InstanceImpl) thing.addEntity();
        instance4 = (InstanceImpl) thing.addEntity();

        relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);
        assertEquals(1, instance1.castings().size());
        assertEquals(2, graknGraph.getTinkerPopGraph().traversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());
    }

    @Test
    public void testMergingDuplicateCasting(){
        Set<ConceptId> castingVertexIds = new HashSet<>();
        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();
        castingVertexIds.add(mainCasting.getId());
        castingVertexIds.add(ConceptId.of(buildDuplicateCastingWithNewRelation(mainCasting, relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3).getId().getValue()));
        castingVertexIds.add(ConceptId.of(buildDuplicateCastingWithNewRelation(mainCasting, relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance4).getId().getValue()));
        assertEquals(3, instance1.castings().size());

        graknGraph.fixDuplicateCastings(mainCasting.getIndex(), castingVertexIds);
        assertEquals(1, instance1.castings().size());
    }

    private CastingImpl buildDuplicateCastingWithNewRelation(CastingImpl mainCasting, RelationType relationType, RoleTypeImpl mainRoleType, InstanceImpl mainInstance, RoleType otherRoleType, InstanceImpl otherInstance){
        RelationImpl relation = (RelationImpl) relationType.addRelation().addRolePlayer(otherRoleType, otherInstance);

        //Create Fake Casting
        Vertex castingVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        castingVertex.property(Schema.ConceptProperty.ID.name(), castingVertex.id().toString());
        castingVertex.property(Schema.ConceptProperty.INDEX.name(), mainCasting.getIndex());
        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleType.getVertex());

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstance.getVertex());
        edge.property(Schema.EdgeProperty.ROLE_TYPE_NAME.name(), mainRoleType.getId());

        edge = relation.getVertex().addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE_NAME.name(), mainRoleType.getId());

        putFakeShortcutEdge(relationType, relation, mainRoleType, mainInstance, otherRoleType, otherInstance);
        putFakeShortcutEdge(relationType, relation, otherRoleType, otherInstance, mainRoleType, mainInstance);

        return graknGraph.admin().buildConcept(castingVertex);
    }

    private void putFakeShortcutEdge(RelationType relationType, Relation relation, RoleType fromRole, InstanceImpl fromInstance, RoleType toRole, InstanceImpl toInstance){
        Edge tinkerEdge = fromInstance.getVertex().addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), toInstance.getVertex());
        EdgeImpl edge = new EdgeImpl(tinkerEdge, graknGraph);

        edge.setProperty(Schema.EdgeProperty.RELATION_TYPE_NAME, relationType.getName().getValue());
        edge.setProperty(Schema.EdgeProperty.RELATION_ID, relation.getId().getValue());

        if (fromInstance.getId() != null)
            edge.setProperty(Schema.EdgeProperty.FROM_ID, fromInstance.getId().getValue());
        edge.setProperty(Schema.EdgeProperty.FROM_ROLE_NAME, fromRole.getName().getValue());

        if (toInstance.getId() != null)
            edge.setProperty(Schema.EdgeProperty.TO_ID, toInstance.getId().getValue());
        edge.setProperty(Schema.EdgeProperty.TO_ROLE_NAME, toRole.getName().getValue());

        edge.setProperty(Schema.EdgeProperty.FROM_TYPE_NAME, fromInstance.type().getName().getValue());
        edge.setProperty(Schema.EdgeProperty.TO_TYPE_NAME, toInstance.type().getName().getValue());
    }

    @Test
    public void testMergingDuplicateRelationsDueToDuplicateCastings() {
        Set<ConceptId> castingVertexIds = new HashSet<>();

        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();
        castingVertexIds.add(mainCasting.getId());
        castingVertexIds.add(ConceptId.of(buildDuplicateCastingWithNewRelation(mainCasting, relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance2).getId().getValue()));
        castingVertexIds.add(ConceptId.of(buildDuplicateCastingWithNewRelation(mainCasting, relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3).getId().getValue()));

        assertEquals(3, instance1.relations().size());
        assertEquals(2, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(6, graknGraph.getTinkerPopGraph().traversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());

        graknGraph.fixDuplicateCastings(mainCasting.getIndex(), castingVertexIds);

        assertEquals(2, instance1.relations().size());
        assertEquals(1, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(4, graknGraph.getTinkerPopGraph().traversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());

    }

    @Test
    public void testMergingResourcesSimple(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        //Create fake resources
        Set<ConceptId> resourceIds = new HashSet<>();
        ResourceImpl<String> mainResource = createFakeResource(resourceType, "1");
        resourceIds.add(mainResource.getId());
        resourceIds.add(createFakeResource(resourceType, "1").getId());
        resourceIds.add(createFakeResource(resourceType, "1").getId());

        //Check we have duplicate resources
        assertEquals(3, resourceType.instances().size());

        //Fix duplicates
        graknGraph.fixDuplicateResources(mainResource.getIndex(), resourceIds);

        //Check we no longer have duplicates
        assertEquals(1, resourceType.instances().size());
    }

    @Test
    public void testMergingResourcesWithRelations(){
        RoleType roleEntity = graknGraph.putRoleType("Entity Role");
        RoleType roleResource = graknGraph.putRoleType("Resource Role");
        RelationType relationType = graknGraph.putRelationType("Relation Type").hasRole(roleEntity).hasRole(roleResource);
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING).playsRole(roleResource);
        EntityType entityType = graknGraph.putEntityType("Entity Type").playsRole(roleEntity);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Entity e3 = entityType.addEntity();

        //Create fake resources
        Set<ConceptId> resourceIds = new HashSet<>();
        ResourceImpl<?> r1 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r11 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r111 = createFakeResource(resourceType, "1");

        resourceIds.add(r1.getId());
        resourceIds.add(r11.getId());
        resourceIds.add(r111.getId());

        //Give resources some relationships
        relationType.addRelation().addRolePlayer(roleResource, r1).addRolePlayer(roleEntity, e1);
        relationType.addRelation().addRolePlayer(roleResource, r11).addRolePlayer(roleEntity, e1); //When merging this relation should not be absorbed
        relationType.addRelation().addRolePlayer(roleResource, r11).addRolePlayer(roleEntity, e2); //Absorb
        relationType.addRelation().addRolePlayer(roleResource, r111).addRolePlayer(roleEntity, e2); //Don't Absorb
        relationType.addRelation().addRolePlayer(roleResource, r111).addRolePlayer(roleEntity, e3); //Absorb

        //Check everything is broken
        assertEquals(3, resourceType.instances().size());
        assertEquals(1, r1.relations().size());
        assertEquals(2, r11.relations().size());
        assertEquals(1, r1.relations().size());
        assertEquals(6, graknGraph.getTinkerTraversal().hasLabel(Schema.BaseType.RELATION.name()).toList().size());

        r1.relations().forEach(rel -> assertTrue(rel.rolePlayers().contains(e1)));

        //Now fix everything
        graknGraph.fixDuplicateResources(r1.getIndex(), resourceIds);

        //Check everything is in order
        assertEquals(1, resourceType.instances().size());

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

        assertEquals(4, graknGraph.getTinkerTraversal().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
    }


    private ResourceImpl<String> createFakeResource(ResourceType<String> type, String value){
        String index = ResourceImpl.generateResourceIndex(type, value);
        Vertex resourceVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), ((ResourceTypeImpl)type).getVertex());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(), index);
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), value);
        resourceVertex.property(Schema.ConceptProperty.ID.name(), resourceVertex.id().toString());

        return new ResourceImpl<>(graknGraph, resourceVertex);
    }
}
