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
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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

        relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
        assertEquals(1, instance1.castings().size());
        assertEquals(2, graknGraph.getTinkerPopGraph().traversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());
    }

    @Test
    public void testMergingDuplicateCasting(){
        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();
        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3);
        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance4);
        assertEquals(3, instance1.castings().size());

        graknGraph.fixDuplicateCasting(mainCasting.getBaseIdentifier());
        assertEquals(1, instance1.castings().size());
    }

    private void buildDuplicateCastingWithNewRelation(RelationType relationType, RoleTypeImpl mainRoleType, InstanceImpl mainInstance, RoleType otherRoleType, InstanceImpl otherInstance){
        RelationImpl relation = (RelationImpl) relationType.addRelation().putRolePlayer(otherRoleType, otherInstance);

        //Create Fake Casting
        Vertex castingVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        castingVertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), UUID.randomUUID().toString());
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
        EdgeImpl edge = new EdgeImpl(tinkerEdge, graknGraph);

        edge.setProperty(Schema.EdgeProperty.RELATION_TYPE_NAME, relationType.getId());
        edge.setProperty(Schema.EdgeProperty.RELATION_ID, relation.getId());

        if (fromInstance.getId() != null)
            edge.setProperty(Schema.EdgeProperty.FROM_ID, fromInstance.getId());
        edge.setProperty(Schema.EdgeProperty.FROM_ROLE_NAME, fromRole.getId());

        if (toInstance.getId() != null)
            edge.setProperty(Schema.EdgeProperty.TO_ID, toInstance.getId());
        edge.setProperty(Schema.EdgeProperty.TO_ROLE_NAME, toRole.getId());

        edge.setProperty(Schema.EdgeProperty.FROM_TYPE_NAME, fromInstance.getParentIsa().getId());
        edge.setProperty(Schema.EdgeProperty.TO_TYPE_NAME, toInstance.getParentIsa().getId());
    }

    @Test
    public void testMergingDuplicateRelationsDueToDuplicateCastings() {
        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();

        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance2);
        buildDuplicateCastingWithNewRelation(relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3);

        assertEquals(3, instance1.relations().size());
        assertEquals(2, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(6, graknGraph.getTinkerPopGraph().traversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());

        graknGraph.fixDuplicateCasting(mainCasting.getBaseIdentifier());

        assertEquals(2, instance1.relations().size());
        assertEquals(1, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        assertEquals(4, graknGraph.getTinkerPopGraph().traversal().E().
                hasLabel(Schema.EdgeLabel.SHORTCUT.getLabel()).toList().size());

    }

    @Test
    public void testMergingResourcesSimple(){
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

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
        graknGraph.fixDuplicateResources(resourceIds);

        //Check we no longer have duplicates
        assertEquals(3, resourceType.instances().size());
    }

    @Test
    public void testMergingResourcesWithRelations(){
        RoleType roleEntity = graknGraph.putRoleType("A Entity Role Type");
        RoleType roleResource = graknGraph.putRoleType("A Resource Role Type");
        RelationType relationType = graknGraph.putRelationType("A Relation Type").hasRole(roleEntity).hasRole(roleResource);
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING).playsRole(roleResource);
        EntityType entityType = graknGraph.putEntityType("An Entity Type").playsRole(roleEntity);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Entity e3 = entityType.addEntity();

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
        relationType.addRelation().putRolePlayer(roleResource, r1).putRolePlayer(roleEntity, e1);
        relationType.addRelation().putRolePlayer(roleResource, r11).putRolePlayer(roleEntity, e1); //When merging this relation should not be absorbed
        relationType.addRelation().putRolePlayer(roleResource, r11).putRolePlayer(roleEntity, e2); //Absorb
        relationType.addRelation().putRolePlayer(roleResource, r111).putRolePlayer(roleEntity, e2); //Don't Absorb
        relationType.addRelation().putRolePlayer(roleResource, r111).putRolePlayer(roleEntity, e3); //Absorb

        //Check everything is broken
        assertEquals(5, resourceType.instances().size());
        assertEquals(1, r1.relations().size());
        assertEquals(2, r11.relations().size());
        assertEquals(1, r1.relations().size());
        assertEquals(6, graknGraph.getTinkerTraversal().hasLabel(Schema.BaseType.RELATION.name()).toList().size());

        r1.relations().forEach(rel -> assertTrue(rel.rolePlayers().values().contains(e1)));

        //Now fix everything
        graknGraph.fixDuplicateResources(resourceIds);

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

        assertEquals(4, graknGraph.getTinkerTraversal().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
    }


    private ResourceImpl createFakeResource(ResourceType type, String value){
        String index = ResourceImpl.generateResourceIndex(type.getId(), value);
        Vertex resourceVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), ((ResourceTypeImpl)type).getVertex());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(), index);
        resourceVertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), UUID.randomUUID().toString());
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), value);

        return new ResourceImpl(resourceVertex, type, graknGraph);
    }
}
