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

package ai.grakn.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
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

public class PostProcessingTest extends GraknEngineTestBase {
    private PostProcessing postProcessing;
    private GraknGraph graknGraph;
    private Cache cache;
    private String keyspace;

    @Before
    public void setUp() throws Exception {
        cache = Cache.getInstance();
        keyspace = UUID.randomUUID().toString().replaceAll("-", "a");
        postProcessing = PostProcessing.getInstance();
        graknGraph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs(keyspace).clear();
        cache.getResourceJobs(keyspace).clear();
    }

    @Test
    public void testMergingCastings() throws Exception {
        //Create Scenario
        RoleType roleType1 = graknGraph.putRoleType("role 1");
        RoleType roleType2 = graknGraph.putRoleType("role 2");
        graknGraph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        graknGraph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);

        graknGraph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraphBatchLoading();
        roleType1 = graknGraph.getRoleType("role 1");
        roleType2 = graknGraph.getRoleType("role 2");
        RelationType relationType = graknGraph.getRelationType("rel type");
        EntityType thing = graknGraph.getEntityType("thing");

        Instance instance1 = thing.addEntity();
        Instance instance2 = thing.addEntity();
        Instance instance3 = thing.addEntity();
        Instance instance4 = thing.addEntity();

        relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        //Record Needed Ids
        String relationTypeId = relationType.getId();
        String mainRoleTypeId = roleType1.getId();
        String mainInstanceId = instance1.getId();
        String otherRoleTypeId = roleType2.getId();
        String otherInstanceId3 = instance3.getId();
        String otherInstanceId4 = instance4.getId();

        graknGraph.commit();

        //Check Number of castings is as expected
        Assert.assertEquals(2, ((AbstractGraknGraph) this.graknGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        //Break The Graph With Fake Castings
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId3);
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId4);

        //Check the graph is broken
        assertEquals(6, ((AbstractGraknGraph) this.graknGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        waitForCache(true, keyspace, 4);
        //Now fix everything
        postProcessing.run();

        //Check it's all fixed
        assertEquals(4, ((AbstractGraknGraph) this.graknGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());
    }

    private void buildDuplicateCasting(String relationTypeId, String mainRoleTypeId, String mainInstanceId, String otherRoleTypeId, String otherInstanceId) throws Exception {
        //Get Needed Grakn Objects
        RelationType relationType = graknGraph.getConcept(relationTypeId);
        Instance otherInstance = graknGraph.getConcept(otherInstanceId);
        RoleType otherRoleType = graknGraph.getConcept(otherRoleTypeId);
        Relation relation = relationType.addRelation().putRolePlayer(otherRoleType, otherInstance);
        String relationId = relation.getId();

        graknGraph.commit();

        Graph rawGraph = ((AbstractGraknGraph) this.graknGraph).getTinkerPopGraph();

        //Get Needed Vertices
        Vertex mainRoleTypeVertex = rawGraph.traversal().V().
                hasId(mainRoleTypeId).next();

        Vertex relationVertex = rawGraph.traversal().V().
                hasId(relationId).next();

        Vertex mainInstanceVertex = rawGraph.traversal().V().
                hasId(mainInstanceId).next();

        //Create Fake Casting
        Vertex castingVertex = rawGraph.addVertex(Schema.BaseType.CASTING.name());
        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleTypeVertex);

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstanceVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        edge = relationVertex.addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);
    }

    @Test
    public void testMergeDuplicateResources() throws GraknValidationException, InterruptedException {
        String keyspace = "testbatchgraph";
        String value = "1";
        String sample = "Sample";
        //ExecutorService pool = Executors.newFixedThreadPool(10);

        //Create Graph With Duplicate Resources
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        graph.putResourceType(sample, ResourceType.DataType.STRING);

        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraphBatchLoading();
        ResourceType<String> resourceType = graph.getResourceType(sample);

        Resource<String> resource = resourceType.putResource(value);
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
    private void createDuplicateResource(GraknGraph graknGraph, ResourceType resourceType, Resource resource){
        AbstractGraknGraph graph = (AbstractGraknGraph) graknGraph;
        Vertex originalResource = (Vertex) graph.getTinkerTraversal()
                .hasId(resource.getId()).next();
        Vertex vertexResourceType = (Vertex) graph.getTinkerTraversal()
                .hasId(resourceType.getId()).next();

        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(),originalResource.value(Schema.ConceptProperty.INDEX.name()));
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), originalResource.value(Schema.ConceptProperty.VALUE_STRING.name()));

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceType);

        cache.getResourceJobs(graknGraph.getKeyspace()).add(resourceVertex.id().toString());
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