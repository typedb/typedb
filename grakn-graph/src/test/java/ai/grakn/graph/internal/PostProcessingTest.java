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
import ai.grakn.concept.Role;
import ai.grakn.util.Schema;
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
    private Role role1;
    private Role role2;
    private RelationType relationType;
    private ThingImpl instance1;
    private ThingImpl instance2;

    @Before
    public void buildSampleGraph(){
        role1 = graknGraph.putRole("role 1");
        role2 = graknGraph.putRole("role 2");
        relationType = graknGraph.putRelationType("rel type").relates(role1).relates(role2);
        EntityType thing = graknGraph.putEntityType("thingy").plays(role1).plays(role2);
        instance1 = (ThingImpl) thing.addEntity();
        instance2 = (ThingImpl) thing.addEntity();
        thing.addEntity();
        thing.addEntity();

        relationType.addRelation().addRolePlayer(role1, instance1).addRolePlayer(role2, instance2);
    }

    @Test
    public void whenMergingDuplicateResources_EnsureSingleResourceRemains(){
        ResourceTypeImpl<String> resourceType = (ResourceTypeImpl<String>) graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

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
        Role roleEntity = graknGraph.putRole("Entity Role");
        Role roleResource = graknGraph.putRole("Resource Role");
        RelationType relationType = graknGraph.putRelationType("Relation Type").relates(roleEntity).relates(roleResource);
        ResourceTypeImpl<String> resourceType = (ResourceTypeImpl<String>) graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING).plays(roleResource);
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
        Relation rel1 = relationType.addRelation().
                addRolePlayer(roleResource, r1).addRolePlayer(roleEntity, e1);
        Relation rel2 = relationType.addRelation().
                addRolePlayer(roleResource, r11).addRolePlayer(roleEntity, e1); //When merging this relation should not be absorbed
        Relation rel3 = relationType.addRelation().
                addRolePlayer(roleResource, r11).addRolePlayer(roleEntity, e2); //Absorb
        Relation rel4 = relationType.addRelation().
                addRolePlayer(roleResource, r111).addRolePlayer(roleEntity, e2); //Don't Absorb

        EdgeElement resourceEdge = EntityImpl.from(e3).putEdge(r111, Schema.EdgeLabel.RESOURCE);
        new RelationImpl(new RelationEdge(relationType, roleEntity, roleResource, resourceEdge)); // Absorb

        RelationImpl.from(rel1).reified().get().setHash();
        RelationImpl.from(rel2).reified().get().setHash();
        RelationImpl.from(rel3).reified().get().setHash();
        RelationImpl.from(rel4).reified().get().setHash();

        //Check everything is broken
        assertEquals(3, resourceType.instances().size());
        assertEquals(1, r1.relations().size());
        assertEquals(2, r11.relations().size());
        assertEquals(1, r1.relations().size());
        assertEquals(5, graknGraph.getTinkerTraversal().V().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
        assertEquals(1, graknGraph.getTinkerTraversal().E().hasLabel(Schema.EdgeLabel.RESOURCE.getLabel()).toList().size());

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
        assertEquals(3, graknGraph.admin().getMetaRelationType().instances().size());
    }


    private ResourceImpl<String> createFakeResource(ResourceTypeImpl<String> type, String value){
        String index = Schema.generateResourceIndex(type.getLabel(), value);
        Vertex resourceVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), type.currentShard().vertex().element());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(), index);
        resourceVertex.property(Schema.VertexProperty.VALUE_STRING.name(), value);
        resourceVertex.property(Schema.VertexProperty.ID.name(), Schema.PREFIX_VERTEX + resourceVertex.id().toString());

        return new ResourceImpl<>(new VertexElement(graknGraph, resourceVertex));
    }

    @Test
    public void whenUpdatingTheCountsOfTypes_TheTypesHaveNewCounts() {
        Map<ConceptId, Long> types = new HashMap<>();
        //Create Some Types;
        EntityTypeImpl t1 = (EntityTypeImpl) graknGraph.putEntityType("t1");
        ResourceTypeImpl t2 = (ResourceTypeImpl)  graknGraph.putResourceType("t2", ResourceType.DataType.STRING);
        RelationTypeImpl t3 = (RelationTypeImpl) graknGraph.putRelationType("t3");

        //Lets Set Some Counts
        types.put(t1.getId(), 5L);
        types.put(t2.getId(), 6L);
        types.put(t3.getId(), 2L);

        graknGraph.admin().updateConceptCounts(types);
        types.entrySet().forEach(entry ->
                assertEquals((long) entry.getValue(), ((ConceptImpl) graknGraph.getConcept(entry.getKey())).getShardCount()));

        //Lets Set Some Counts
        types.put(t1.getId(), -5L);
        types.put(t2.getId(), -2L);
        types.put(t3.getId(), 3L);
        graknGraph.admin().updateConceptCounts(types);

        assertEquals(0L, t1.getShardCount());
        assertEquals(4L, t2.getShardCount());
        assertEquals(5L, t3.getShardCount());
    }

    @Test
    public void whenMergingDuplicateResourceEdges_EnsureNoDuplicatesRemain(){
        ResourceTypeImpl<String> resourceType = (ResourceTypeImpl <String>) graknGraph.putResourceType("My Sad Resource", ResourceType.DataType.STRING);
        EntityType entityType = graknGraph.putEntityType("My Happy EntityType");
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Entity e3 = entityType.addEntity();

        ResourceImpl<String> r1dup1 = createFakeResource(resourceType, "1");
        ResourceImpl<String> r1dup2 = createFakeResource(resourceType, "1");
        ResourceImpl<String> r1dup3 = createFakeResource(resourceType, "1");

        Resource<String> r2dup1 = resourceType.putResource("2");
        Resource<String> r2dup2 = resourceType.putResource("2");
        Resource<String> r2dup3 = resourceType.putResource("2");
    }
}
