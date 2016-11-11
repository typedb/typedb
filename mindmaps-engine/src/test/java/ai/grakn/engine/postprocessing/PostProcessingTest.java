/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

package ai.grakn.engine.postprocessing;

import ai.grakn.Mindmaps;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.graph.internal.AbstractMindmapsGraph;
import ai.grakn.Mindmaps;
import ai.grakn.MindmapsGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.MindmapsEngineTestBase;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.graph.internal.AbstractMindmapsGraph;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class PostProcessingTest extends MindmapsEngineTestBase{
    private PostProcessing postProcessing;
    private MindmapsGraph mindmapsGraph;
    private Cache cache;
    private String keyspace;

    @Before
    public void setUp() throws Exception {
        cache = Cache.getInstance();
        keyspace = UUID.randomUUID().toString().replaceAll("-", "a");
        postProcessing = PostProcessing.getInstance();
        mindmapsGraph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraphBatchLoading();
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs(keyspace).clear();
        cache.getResourceJobs(keyspace).clear();
    }

    @Test
    public void testMergingCastings() throws Exception {
        //Create Scenario
        RoleType roleType1 = mindmapsGraph.putRoleType("role 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role 2");
        RelationType relationType = mindmapsGraph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        EntityType thing = mindmapsGraph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = mindmapsGraph.addEntity(thing);
        Instance instance2 = mindmapsGraph.addEntity(thing);
        Instance instance3 = mindmapsGraph.addEntity(thing);
        Instance instance4 = mindmapsGraph.addEntity(thing);

        mindmapsGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        //Record Needed Ids
        String relationTypeId = relationType.getId();
        String mainRoleTypeId = roleType1.getId();
        String mainInstanceId = instance1.getId();
        String otherRoleTypeId = roleType2.getId();
        String otherInstanceId3 = instance3.getId();
        String otherInstanceId4 = instance4.getId();

        mindmapsGraph.commit();

        //Check Number of castings is as expected
        Assert.assertEquals(2, ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        //Break The Graph With Fake Castings
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId3);
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId4);

        //Check the graph is broken
        assertEquals(6, ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        waitForCache(true, keyspace, 4);
        //Now fix everything
        postProcessing.run();

        //Check it's all fixed
        assertEquals(4, ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());
    }

    private void buildDuplicateCasting(String relationTypeId, String mainRoleTypeId, String mainInstanceId, String otherRoleTypeId, String otherInstanceId) throws Exception {
        //Get Needed Mindmaps Objects
        RelationType relationType = mindmapsGraph.getRelationType(relationTypeId);
        Instance otherInstance = mindmapsGraph.getInstance(otherInstanceId);
        RoleType otherRoleType = mindmapsGraph.getRoleType(otherRoleTypeId);
        Relation relation = mindmapsGraph.addRelation(relationType).putRolePlayer(otherRoleType, otherInstance);
        String relationId = relation.getId();

        mindmapsGraph.commit();

        Graph rawGraph = ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph();

        //Get Needed Vertices
        Vertex mainRoleTypeVertex = rawGraph.traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), mainRoleTypeId).next();

        Vertex relationVertex = rawGraph.traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), relationId).next();

        Vertex mainInstanceVertex = rawGraph.traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), mainInstanceId).next();

        //Create Fake Casting
        Vertex castingVertex = rawGraph.addVertex(Schema.BaseType.CASTING.name());
        castingVertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), UUID.randomUUID().toString());
        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleTypeVertex);

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstanceVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        edge = relationVertex.addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);
    }

    @Test
    public void testMergeDuplicateResources() throws MindmapsValidationException, InterruptedException {
        String keyspace = "testbatchgraph";
        String value = "1";
        String sample = "Sample";
        ExecutorService pool = Executors.newFixedThreadPool(10);

        //Create Graph With Duplicate Resources
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraphBatchLoading();
        ResourceType<String> resourceType = graph.putResourceType(sample, ResourceType.DataType.STRING);
        Resource<String> resource = graph.putResource(value, resourceType);
        graph.commit();
        assertEquals(1, resourceType.instances().size());
        waitForCache(false, keyspace, 1);

        //Check duplicates have been created
        createDuplicateResource(graph, resourceType, resource);
        createDuplicateResource(graph, resourceType, resource);
        createDuplicateResource(graph, resourceType, resource);
        createDuplicateResource(graph, resourceType, resource);
        assertEquals(5, resourceType.instances().size());
        waitForCache(false, keyspace, 5);

        //Now fix everything
        postProcessing.run();

        //Check it's fixed
        assertEquals(1, graph.getResourceType(sample).instances().size());
    }
    private void createDuplicateResource(MindmapsGraph mindmapsGraph, ResourceType resourceType, Resource resource){
        AbstractMindmapsGraph graph = (AbstractMindmapsGraph) mindmapsGraph;
        Vertex originalResource = (Vertex) graph.getTinkerTraversal()
                .has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), resource.getId()).next();
        Vertex vertexResourceType = (Vertex) graph.getTinkerTraversal()
                .has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), resourceType.getId()).next();

        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(),originalResource.value(Schema.ConceptProperty.INDEX.name()));
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), originalResource.value(Schema.ConceptProperty.VALUE_STRING.name()));
        resourceVertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), UUID.randomUUID().toString());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceType);

        cache.getResourceJobs(mindmapsGraph.getKeyspace()).add(resourceVertex.id().toString());
    }

    private void waitForCache(boolean isCasting, String keyspace, int value) throws InterruptedException {
        boolean flag = true;
        while(flag){
            if(isCasting){
                if(cache.getCastingJobs(keyspace).size() < value){
                    Thread.sleep(1000);
                } else{
                    flag = false;
                }
            } else {
                if(cache.getResourceJobs(keyspace).size() < value){
                    Thread.sleep(1000);
                } else {
                    flag = false;
                }
            }
        }
    }
}