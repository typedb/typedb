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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PostProcessingTest extends GraphTestBase{
    private RoleType roleType1;
    private RoleType roleType2;
    private RelationType relationType;
    private InstanceImpl instance1;
    private InstanceImpl instance2;
    private InstanceImpl instance3;
    private InstanceImpl instance4;

    @Before
    public void buildSampleGraph(){
        roleType1 = graknGraph.putRoleType("role 1");
        roleType2 = graknGraph.putRoleType("role 2");
        relationType = graknGraph.putRelationType("rel type").relates(roleType1).relates(roleType2);
        EntityType thing = graknGraph.putEntityType("thing").plays(roleType1).plays(roleType2);
        instance1 = (InstanceImpl) thing.addEntity();
        instance2 = (InstanceImpl) thing.addEntity();
        instance3 = (InstanceImpl) thing.addEntity();
        instance4 = (InstanceImpl) thing.addEntity();

        relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);
    }

    @Test
    public void whenMergingDuplicateCastings_EnsureOnlyOneCastingRemains(){
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
        edge.property(Schema.EdgeProperty.ROLE_TYPE_LABEL.name(), mainRoleType.getId());

        edge = relation.getVertex().addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE_LABEL.name(), mainRoleType.getId());

        return graknGraph.admin().buildConcept(castingVertex);
    }

    @Test
    public void whenMergingCastingWhichResultInDuplicateRelations_MergeDuplicateRelations() {
        Set<ConceptId> castingVertexIds = new HashSet<>();

        CastingImpl mainCasting = (CastingImpl) instance1.castings().iterator().next();
        castingVertexIds.add(mainCasting.getId());
        castingVertexIds.add(ConceptId.of(buildDuplicateCastingWithNewRelation(mainCasting, relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance2).getId().getValue()));
        castingVertexIds.add(ConceptId.of(buildDuplicateCastingWithNewRelation(mainCasting, relationType, (RoleTypeImpl) roleType1, instance1, roleType2, instance3).getId().getValue()));

        assertEquals(3, instance1.relations().size());
        assertEquals(2, instance2.relations().size());
        assertEquals(1, instance3.relations().size());

        graknGraph.fixDuplicateCastings(mainCasting.getIndex(), castingVertexIds);

        assertEquals(2, instance1.relations().size());
        assertEquals(1, instance2.relations().size());
        assertEquals(1, instance3.relations().size());
    }

    @Test
    public void whenMergingDuplicateResources_EnsureSingleResourceRemains(){
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
    public void whenMergingDuplicateResourcesWithRelations_EnsureSingleResourceRemainsAndNoDuplicateRelationsAreCreated(){
        RoleType roleEntity = graknGraph.putRoleType("Entity Role");
        RoleType roleResource = graknGraph.putRoleType("Resource Role");
        RelationType relationType = graknGraph.putRelationType("Relation Type").relates(roleEntity).relates(roleResource);
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING).plays(roleResource);
        EntityType entityType = graknGraph.putEntityType("Entity Type").plays(roleEntity);
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
        assertThat(foundR1.ownerInstances(), containsInAnyOrder(e1, e2, e3));
        assertEquals(4, graknGraph.admin().getMetaRelationType().instances().size());
    }


    private ResourceImpl<String> createFakeResource(ResourceType<String> type, String value){
        String index = Schema.generateResourceIndex(type.getLabel(), value);
        Vertex resourceVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), ((ResourceTypeImpl)type).getVertex());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(), index);
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), value);
        resourceVertex.property(Schema.ConceptProperty.ID.name(), resourceVertex.id().toString());

        return new ResourceImpl<>(graknGraph, resourceVertex);
    }

    @Test
    public void whenUpdatingTheCountsOfTypes_TheTypesHaveNewCounts() {
        Map<TypeLabel, Long> types = new HashMap<>();
        //Create Some Types;
        EntityTypeImpl t1 = (EntityTypeImpl) graknGraph.putEntityType("t1");
        ResourceTypeImpl t2 = (ResourceTypeImpl)  graknGraph.putResourceType("t2", ResourceType.DataType.STRING);
        RelationTypeImpl t3 = (RelationTypeImpl) graknGraph.putRelationType("t3");

        //Lets Set Some Counts
        types.put(t1.getLabel(), 5L);
        types.put(t2.getLabel(), 6L);
        types.put(t3.getLabel(), 2L);

        graknGraph.admin().updateTypeCounts(types);
        types.entrySet().forEach(entry ->
                assertEquals((long) entry.getValue(), ((TypeImpl) graknGraph.getType(entry.getKey())).getInstanceCount()));

        //Lets Set Some Counts
        types.put(t1.getLabel(), -5L);
        types.put(t2.getLabel(), -2L);
        types.put(t3.getLabel(), 3L);
        graknGraph.admin().updateTypeCounts(types);

        assertEquals(0L, t1.getInstanceCount());
        assertEquals(4L, t2.getInstanceCount());
        assertEquals(5L, t3.getInstanceCount());
    }
}
