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
import ai.grakn.graph.internal.concept.ConceptImpl;
import ai.grakn.graph.internal.concept.EntityTypeImpl;
import ai.grakn.graph.internal.concept.RelationImpl;
import ai.grakn.graph.internal.concept.RelationTypeImpl;
import ai.grakn.graph.internal.concept.ResourceImpl;
import ai.grakn.graph.internal.concept.ResourceTypeImpl;
import ai.grakn.graph.internal.concept.ThingImpl;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
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
        EntityType entityType = graknGraph.putEntityType("Entity Type").plays(roleEntity).resource(resourceType);
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
        addReifiedRelation(roleEntity, roleResource, relationType, e1, r1);

        //When merging this relation should not be absorbed
        addReifiedRelation(roleEntity, roleResource, relationType, e1, r11);

        //Absorb
        addReifiedRelation(roleEntity, roleResource, relationType, e2, r11);

        //Don't Absorb
        addEdgeRelation(e2, r111);

        // Absorb
        addEdgeRelation(e3, r111);

        //Check everything is broken
        assertEquals(3, resourceType.instances().size());
        assertEquals(1, r1.relations().size());
        assertEquals(2, r11.relations().size());
        assertEquals(1, r1.relations().size());
        assertEquals(4, graknGraph.getTinkerTraversal().V().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
        assertEquals(2, graknGraph.getTinkerTraversal().E().hasLabel(Schema.EdgeLabel.RESOURCE.getLabel()).toList().size());

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
        assertEquals(5, graknGraph.admin().getMetaRelationType().instances().size());
    }

    private void addEdgeRelation(Entity entity, Resource<?> resource) {
        entity.resource(resource);
    }

    private void addReifiedRelation(Role roleEntity, Role roleResource, RelationType relationType, Entity entity, Resource<?> resource) {
        Relation relation = relationType.addRelation().addRolePlayer(roleResource, resource).addRolePlayer(roleEntity, entity);
        RelationImpl.from(relation).reify().setHash();
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
        types.forEach((key, value) -> assertEquals((long) value, ((ConceptImpl) graknGraph.getConcept(key)).getShardCount()));

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
        EntityType entityType = graknGraph.putEntityType("My Happy EntityType").resource(resourceType);
        RelationType relationType = graknGraph.putRelationType("My Miserable RelationType").resource(resourceType);
        Entity entity = entityType.addEntity();
        Relation relation = relationType.addRelation();

        ResourceImpl<?> r1dup1 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r1dup2 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r1dup3 = createFakeResource(resourceType, "1");

        ResourceImpl<?> r2dup1 = createFakeResource(resourceType, "2");
        ResourceImpl<?> r2dup2 = createFakeResource(resourceType, "2");
        ResourceImpl<?> r2dup3 = createFakeResource(resourceType, "2");

        entity.resource(r1dup1);
        entity.resource(r1dup2);
        entity.resource(r1dup3);

        relation.resource(r1dup1);
        relation.resource(r1dup2);
        relation.resource(r1dup3);

        entity.resource(r2dup1);

        //Check everything is broken
        //Entities Too Many Resources
        assertEquals(4, entity.resources().size());
        assertEquals(3, relation.resources().size());

        //There are too many resources
        assertEquals(6, graknGraph.admin().getMetaResourceType().instances().size());

        //Now fix everything for resource 1
        graknGraph.fixDuplicateResources(r1dup1.getIndex(), new HashSet<>(Arrays.asList(r1dup1.getId(), r1dup2.getId(), r1dup3.getId())));

        //Check resource one has been sorted out
        assertEquals(2, entity.resources().size());
        assertEquals(2, entity.resources().size());
        assertEquals(4, graknGraph.admin().getMetaResourceType().instances().size()); // 4 because we still have 2 dups on r2

        //Now fix everything for resource 2
        graknGraph.fixDuplicateResources(r2dup1.getIndex(), new HashSet<>(Arrays.asList(r2dup1.getId(), r2dup2.getId(), r2dup3.getId())));

        //Check resource one has been sorted out
        assertEquals(2, entity.resources().size());
        assertEquals(2, entity.resources().size());
        assertEquals(2, graknGraph.admin().getMetaResourceType().instances().size()); // 4 because we still have 2 dups on r2
    }
}
