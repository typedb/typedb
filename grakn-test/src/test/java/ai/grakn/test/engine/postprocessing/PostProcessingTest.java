/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.postprocessing.EngineCache;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static ai.grakn.test.GraknTestEnv.*;

public class PostProcessingTest {
    private PostProcessing postProcessing = PostProcessing.getInstance();
    private EngineCache cache = EngineCache.getInstance();

    private GraknGraph graph;

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(usingTinker());
    }

    @Before
    public void setUp() throws Exception {
        graph = engine.factoryWithNewKeyspace().getGraph();
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs(graph.getKeyspace()).clear();
        cache.getResourceJobs(graph.getKeyspace()).clear();
    }

    @Test
    public void testMergingCastings() throws Exception {
        //Create Scenario
        RoleType roleType1 = graph.putRoleType("role 1");
        RoleType roleType2 = graph.putRoleType("role 2");
        graph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        graph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);

        GraknGraphFactory factory = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace());
        graph = factory.getGraph();
        roleType1 = graph.getRoleType("role 1");
        roleType2 = graph.getRoleType("role 2");
        RelationType relationType = graph.getRelationType("rel type");
        EntityType thing = graph.getEntityType("thing");

        Instance instance1 = thing.addEntity();
        Instance instance2 = thing.addEntity();
        Instance instance3 = thing.addEntity();
        Instance instance4 = thing.addEntity();

        relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        //Record Needed Ids
        ConceptId relationTypeId = relationType.getId();
        ConceptId mainRoleTypeId = roleType1.getId();
        ConceptId mainInstanceId = instance1.getId();
        ConceptId otherRoleTypeId = roleType2.getId();
        ConceptId otherInstanceId3 = instance3.getId();
        ConceptId otherInstanceId4 = instance4.getId();

        graph.commitOnClose();
        graph.close();
        graph = factory.getGraph();

        //Check Number of castings is as expected
        Assert.assertEquals(2, ((AbstractGraknGraph) this.graph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        //Break The Graph With Fake Castings
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId3);
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId4);

        //Check the graph is broken
        assertEquals(6, ((AbstractGraknGraph) this.graph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        waitForCache(true, graph.getKeyspace(), 4);
        //Now fix everything
        postProcessing.run();

        //Check it's all fixed
        assertEquals(4, ((AbstractGraknGraph) this.graph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        //Check the cache has been cleaned
        assertEquals(0, cache.getNumJobs(graph.getKeyspace()));
    }

    private void buildDuplicateCasting(ConceptId relationTypeId, ConceptId mainRoleTypeId, ConceptId mainInstanceId, ConceptId otherRoleTypeId, ConceptId otherInstanceId) throws Exception {
        //Get Needed Grakn Objects
        RelationType relationType = graph.getConcept(relationTypeId);
        Instance otherInstance = graph.getConcept(otherInstanceId);
        RoleType otherRoleType = graph.getConcept(otherRoleTypeId);
        Relation relation = relationType.addRelation().putRolePlayer(otherRoleType, otherInstance);
        ConceptId relationId = relation.getId();

        Graph rawGraph = ((AbstractGraknGraph) this.graph).getTinkerPopGraph();

        //Get Needed Vertices
        Vertex mainRoleTypeVertex = rawGraph.traversal().V().
                hasId(mainRoleTypeId.getValue()).next();

        Vertex relationVertex = rawGraph.traversal().V().
                hasId(relationId.getValue()).next();

        Vertex mainInstanceVertex = rawGraph.traversal().V().
                hasId(mainInstanceId.getValue()).next();

        Vertex otherCasting = mainRoleTypeVertex.edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).next().outVertex();

        //Create Fake Casting
        Vertex castingVertex = rawGraph.addVertex(Schema.BaseType.CASTING.name());

        castingVertex.property(Schema.ConceptProperty.ID.name(), castingVertex.id().toString());
        castingVertex.property(Schema.ConceptProperty.INDEX.name(), otherCasting.value(Schema.ConceptProperty.INDEX.name()));

        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleTypeVertex);

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstanceVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        edge = relationVertex.addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        cache.addJobCasting(graph.getKeyspace(), castingVertex.value(Schema.ConceptProperty.INDEX.name()).toString(), ConceptId.of(castingVertex.id().toString()));
    }

    @Test
    public void testMergeDuplicateResources() throws GraknValidationException, InterruptedException {
        String keyspace = "testbatchgraph";
        String value = "1";
        String sample = "Sample";
        //ExecutorService pool = Executors.newFixedThreadPool(10);

        //Create Graph With Duplicate Resources
        GraknGraphFactory factory = Grakn.factory(Grakn.DEFAULT_URI, keyspace);
        GraknGraph graph = factory.getGraph();
        graph.putResourceType(sample, ResourceType.DataType.STRING);

        graph = factory.getGraph();
        ResourceType<String> resourceType = graph.getResourceType(sample);

        Resource<String> resource = resourceType.putResource(value);
        graph.commitOnClose();
        graph.close();
        graph = factory.getGraph();

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

        //Check the cache has been cleared
        assertEquals(0, cache.getNumJobs(graph.getKeyspace()));
    }
    private void createDuplicateResource(GraknGraph graknGraph, ResourceType resourceType, Resource resource){
        AbstractGraknGraph graph = (AbstractGraknGraph) graknGraph;
        Vertex originalResource = (Vertex) graph.getTinkerTraversal()
                .hasId(resource.getId().getValue()).next();
        Vertex vertexResourceType = (Vertex) graph.getTinkerTraversal()
                .hasId(resourceType.getId().getValue()).next();

        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(),originalResource.value(Schema.ConceptProperty.INDEX.name()));
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), originalResource.value(Schema.ConceptProperty.VALUE_STRING.name()));
        resourceVertex.property(Schema.ConceptProperty.ID.name(), resourceVertex.id().toString());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceType);

        cache.addJobResource(graknGraph.getKeyspace(), resourceVertex.value(Schema.ConceptProperty.INDEX.name()).toString(), ConceptId.of(resourceVertex.id().toString()));
    }

    private void waitForCache(boolean isCasting, String keyspace, int value) throws InterruptedException {
        boolean flag = true;
        while(flag){
            if(isCasting){
                if(cache.getNumCastingJobs(keyspace) < value){
                    Thread.sleep(1000);
                } else{
                    flag = false;
                }
            } else {
                if(cache.getNumResourceJobs(keyspace) < value){
                    Thread.sleep(1000);
                } else {
                    flag = false;
                }
            }
        }
    }
}