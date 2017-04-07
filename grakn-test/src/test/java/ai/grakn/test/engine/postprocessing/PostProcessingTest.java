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
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {
    private PostProcessing postProcessing = PostProcessing.getInstance();
    private ConceptCache cache = EngineCacheProvider.getCache();

    private GraknGraph graph;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(usingTinker());
    }

    @Before
    public void setUp() throws Exception {
        EngineCacheProvider.getCache().getKeyspaces().forEach(k -> EngineCacheProvider.getCache().clearAllJobs(k));
        graph = engine.factoryWithNewKeyspace().open(GraknTxType.WRITE);
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs(graph.getKeyspace()).clear();
        cache.getResourceJobs(graph.getKeyspace()).clear();
        graph.close();
    }

    @Test
    public void testMergingCastings() throws Exception {
        //Create Scenario
        RoleType roleType1 = graph.putRoleType("role 1");
        RoleType roleType2 = graph.putRoleType("role 2");
        graph.putRelationType("rel type").relates(roleType1).relates(roleType2);
        graph.putEntityType("thing").plays(roleType1).plays(roleType2);

        GraknSession factory = Grakn.session(Grakn.DEFAULT_URI, graph.getKeyspace());
        roleType1 = graph.getRoleType("role 1");
        roleType2 = graph.getRoleType("role 2");
        RelationType relationType = graph.getRelationType("rel type");
        EntityType thing = graph.getEntityType("thing");

        Instance instance1 = thing.addEntity();
        Instance instance2 = thing.addEntity();
        Instance instance3 = thing.addEntity();
        Instance instance4 = thing.addEntity();

        relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);

        //Record Needed Ids
        ConceptId relationTypeId = relationType.getId();
        ConceptId mainRoleTypeId = roleType1.getId();
        ConceptId mainInstanceId = instance1.getId();
        ConceptId otherRoleTypeId = roleType2.getId();
        ConceptId otherInstanceId3 = instance3.getId();
        ConceptId otherInstanceId4 = instance4.getId();

        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

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
        Relation relation = relationType.addRelation().addRolePlayer(otherRoleType, otherInstance);
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
        edge.property(Schema.EdgeProperty.ROLE_TYPE_LABEL.name(), mainRoleTypeId);

        edge = relationVertex.addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE_LABEL.name(), mainRoleTypeId);

        cache.addJobCasting(graph.getKeyspace(), castingVertex.value(Schema.ConceptProperty.INDEX.name()).toString(), ConceptId.of(castingVertex.id().toString()));
    }

    @Test
    public void testMergeDuplicateResources() throws GraknValidationException, InterruptedException {
        String keyspace = "testbatchgraph";
        String value = "1";
        String sample = "Sample";
        //ExecutorService pool = Executors.newFixedThreadPool(10);

        //Create Graph With Duplicate Resources
        GraknSession factory = Grakn.session(Grakn.DEFAULT_URI, keyspace);
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        ResourceType<String> resourceType = graph.putResourceType(sample, ResourceType.DataType.STRING);


        Resource<String> resource = resourceType.putResource(value);
        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        assertEquals(1, resourceType.instances().size());
        waitForCache(false, keyspace, 1);

        //Check duplicates have been created
        PostProcessingTestUtils.createDuplicateResource(graph, cache, resourceType, resource);
        createDuplicateResource(graph, cache, resourceType, resource);
        createDuplicateResource(graph, cache, resourceType, resource);
        createDuplicateResource(graph, cache, resourceType, resource);
        assertEquals(5, resourceType.instances().size());
        waitForCache(false, keyspace, 5);

        //Now fix everything
        postProcessing.run();

        //Check it's fixed
        assertEquals(1, graph.getResourceType(sample).instances().size());

        //Check the cache has been cleared
        assertEquals(0, cache.getNumJobs(graph.getKeyspace()));
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