package io.mindmaps.factory;

import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.graph.internal.MindmapsOrientDBGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class MindmapsOrientDBGraphFactoryTest {
    private final static String TEST_NAME = "MyGraph";
    private final static String TEST_URI = "memory";
    private static MindmapsOrientDBGraphFactory orientGraphFactory ;

    @Before
    public void setUp() throws Exception {
        orientGraphFactory = new MindmapsOrientDBGraphFactory();
    }

    @After
    public void clear() throws MindmapsValidationException {
        MindmapsOrientDBGraph graph = orientGraphFactory.getGraph(TEST_NAME, TEST_URI, null, false);
        graph.clear();
    }

    @Test
    public void testBuildSimpleGraph() throws Exception {
        AbstractMindmapsGraph mindmapsGraph = orientGraphFactory.getGraph(TEST_NAME, TEST_URI, null, false);
        assertThat(mindmapsGraph.getTinkerPopGraph(), instanceOf(OrientGraph.class));
        assertEquals(8, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testBuildSingletonGraphs(){
        AbstractMindmapsGraph<OrientGraph> mindmapsGraph1 = orientGraphFactory.getGraph(TEST_NAME + "1", TEST_URI, null, false);
        AbstractMindmapsGraph<OrientGraph> mindmapsGraph2 = orientGraphFactory.getGraph(TEST_NAME + "2", TEST_URI, null, false);
        AbstractMindmapsGraph<OrientGraph> mindmapsGraph3 = orientGraphFactory.getGraph(TEST_NAME + "3", TEST_URI, null, false);

        assertNotEquals(mindmapsGraph1, mindmapsGraph2);
        assertNotEquals(mindmapsGraph2, mindmapsGraph3);

        assertEquals(8, mindmapsGraph1.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(8, mindmapsGraph2.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(8, mindmapsGraph3.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testBuildGraph() throws MindmapsValidationException {
        MindmapsOrientDBGraph mindmapsGraph = orientGraphFactory.getGraph(TEST_NAME, TEST_URI, null, false);

        assertEquals(8, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertNotNull(mindmapsGraph.getMetaEntityType());

        EntityType entityType = mindmapsGraph.putEntityType("My Entity Type");
        mindmapsGraph.addEntity(entityType);
        mindmapsGraph.addEntity(entityType);
        mindmapsGraph.addEntity(entityType);
        mindmapsGraph.commit();
        assertEquals(12, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());

        RoleType role1 = mindmapsGraph.putRoleType("Role 1");
        RoleType role2 = mindmapsGraph.putRoleType("Role 2");
        mindmapsGraph.putRelationType("My Relation Type").hasRole(role1).hasRole(role2);
        mindmapsGraph.commit();
        assertEquals(15, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }


}