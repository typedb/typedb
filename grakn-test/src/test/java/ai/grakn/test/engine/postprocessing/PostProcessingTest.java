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
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import mjson.Json;
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

import java.util.Set;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.Schema.ConceptProperty.INDEX;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {

    private GraknSession session;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(usingTinker());
    }

    @Before
    public void setUp() throws Exception {
        session = engine.factoryWithNewKeyspace();
    }

    @After
    public void takeDown() throws InterruptedException {
        session.close();
    }

    @Test
    public void testMergingCastings() throws Exception {
        GraknGraph graph = session.open(GraknTxType.WRITE);

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
        ConceptId mainInstanceId = instance1.getId();
        ConceptId otherRoleTypeId = roleType2.getId();
        ConceptId otherInstanceId3 = instance3.getId();
        ConceptId otherInstanceId4 = instance4.getId();

        graph.admin().commitNoLogs();

        graph = factory.open(GraknTxType.WRITE);

        //Check Number of castings is as expected
        Assert.assertEquals(2, ((AbstractGraknGraph) graph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        //Break The Graph With Fake Castings
        Set<Vertex> castings1 = buildDuplicateCasting(graph, relationTypeId, roleType1, mainInstanceId, otherRoleTypeId, otherInstanceId3);
        Set<Vertex> castings2 = buildDuplicateCasting(graph, relationTypeId, roleType1, mainInstanceId, otherRoleTypeId, otherInstanceId4);

        //Check the graph is broken
        assertEquals(6, ((AbstractGraknGraph) graph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        // Get the index
        String castingIndex = castings1.iterator().next().value(INDEX.name()).toString();

        // Merge the castings sets
        Set<Vertex> merged = Sets.newHashSet();
        merged.addAll(castings1);
        merged.addAll(castings2);

        // Casting sets as ConceptIds
        Set<String> castingConcepts = merged.stream().map(c -> c.id().toString()).collect(toSet());

        graph.close();

        //Now fix everything
        PostProcessingTask task = new PostProcessingTask();
        TaskConfiguration configuration = TaskConfiguration.of(
                Json.object(
                        KEYSPACE, graph.getKeyspace(),
                        REST.Request.COMMIT_LOG_FIXING, Json.object(
                                Schema.BaseType.CASTING.name(), Json.object(castingIndex, castingConcepts),
                                Schema.BaseType.RESOURCE.name(), Json.object()
                        ))
        );

        task.start(null, configuration);

        graph = session.open(GraknTxType.READ);

        //Check it's all fixed
        assertEquals(4, ((AbstractGraknGraph) graph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        graph.close();
    }

    private Set<Vertex> buildDuplicateCasting(GraknGraph graph, ConceptId relationTypeId, RoleType mainRoleType, ConceptId mainInstanceId, ConceptId otherRoleTypeId, ConceptId otherInstanceId) throws Exception {
        //Get Needed Grakn Objects
        RelationType relationType = graph.getConcept(relationTypeId);
        Instance otherInstance = graph.getConcept(otherInstanceId);
        RoleType otherRoleType = graph.getConcept(otherRoleTypeId);
        Relation relation = relationType.addRelation().addRolePlayer(otherRoleType, otherInstance);
        ConceptId relationId = relation.getId();

        Graph rawGraph = ((AbstractGraknGraph) graph).getTinkerPopGraph();

        //Get Needed Vertices
        Vertex mainRoleTypeVertexShard = rawGraph.traversal().V().
                has(Schema.ConceptProperty.TYPE_ID.name(), mainRoleType.getTypeId()).in(Schema.EdgeLabel.SHARD.getLabel()).next();

        Vertex relationVertex = rawGraph.traversal().V().
                hasId(relationId.getValue()).next();

        Vertex mainInstanceVertex = rawGraph.traversal().V().
                hasId(mainInstanceId.getValue()).next();

        Vertex otherCasting = mainRoleTypeVertexShard.edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).next().outVertex();

        //Create Fake Casting
        Vertex castingVertex = rawGraph.addVertex(Schema.BaseType.CASTING.name());

        castingVertex.property(Schema.ConceptProperty.ID.name(), castingVertex.id().toString());
        castingVertex.property(INDEX.name(), otherCasting.value(INDEX.name()));

        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleTypeVertexShard);

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstanceVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE_ID.name(), mainRoleType.getId());

        edge = relationVertex.addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE_ID.name(), mainRoleType.getId());

        return Sets.newHashSet(otherCasting, castingVertex);
    }

    @Test
    public void testMergeDuplicateResources() throws GraknValidationException, InterruptedException {
        String value = "1";
        String sample = "Sample";

        //Create Graph With Duplicate Resources
        GraknGraph graph = session.open(GraknTxType.WRITE);
        ResourceType<String> resourceType = graph.putResourceType(sample, ResourceType.DataType.STRING);

        Resource<String> resource = resourceType.putResource(value);
        graph.admin().commitNoLogs();
        graph = session.open(GraknTxType.WRITE);

        assertEquals(1, resourceType.instances().size());

        //Check duplicates have been created
        Set<Vertex> resource1 = createDuplicateResource(graph, resourceType, resource);
        Set<Vertex> resource2 = createDuplicateResource(graph, resourceType, resource);
        Set<Vertex> resource3 = createDuplicateResource(graph, resourceType, resource);
        Set<Vertex> resource4 = createDuplicateResource(graph, resourceType, resource);
        assertEquals(5, resourceType.instances().size());

        // Resource vertex index
        String resourceIndex = resource1.iterator().next().value(INDEX.name()).toString();

        // Merge the resource sets
        Set<Vertex> merged = Sets.newHashSet();
        merged.addAll(resource1);
        merged.addAll(resource2);
        merged.addAll(resource3);
        merged.addAll(resource4);

        graph.close();

        //Now fix everything

        // Casting sets as ConceptIds
        Set<String> resourceConcepts = merged.stream().map(c -> c.id().toString()).collect(toSet());

        //Now fix everything
        PostProcessingTask task = new PostProcessingTask();
        TaskConfiguration configuration = TaskConfiguration.of(
                Json.object(
                        KEYSPACE, graph.getKeyspace(),
                        REST.Request.COMMIT_LOG_FIXING, Json.object(
                                Schema.BaseType.CASTING.name(), Json.object(),
                                Schema.BaseType.RESOURCE.name(), Json.object(resourceIndex, resourceConcepts)
                        ))
        );

        task.start(null, configuration);

        graph = session.open(GraknTxType.READ);

        //Check it's fixed
        assertEquals(1, graph.getResourceType(sample).instances().size());

        graph.close();
    }
}