package io.grakn.factory;

import io.grakn.concept.EntityType;
import io.grakn.concept.RoleType;
import io.grakn.exception.GraknValidationException;
import io.grakn.graph.internal.AbstractGraknGraph;
import io.grakn.graph.internal.GraknOrientDBGraph;
import io.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class GraknOrientDBGraphFactoryTest {
    private final static String TEST_NAME = "MyGraph";
    private final static String TEST_URI = "memory";
    private static GraknOrientDBInternalFactory orientGraphFactory ;

    @Before
    public void setUp() throws Exception {
        orientGraphFactory = new GraknOrientDBInternalFactory(TEST_NAME, TEST_URI, null);
    }

    @After
    public void clear() throws GraknValidationException {
        GraknOrientDBGraph graph = orientGraphFactory.getGraph(false);
        graph.clear();
    }

    @Test
    public void testBuildSimpleGraph() throws Exception {
        AbstractGraknGraph mindmapsGraph = orientGraphFactory.getGraph(false);
        assertThat(mindmapsGraph.getTinkerPopGraph(), instanceOf(OrientGraph.class));
        assertEquals(8, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testBuildSingletonGraphs(){
        AbstractGraknGraph<OrientGraph> mindmapsGraph1 = orientGraphFactory.getGraph(false);
        AbstractGraknGraph<OrientGraph> mindmapsGraph2 = orientGraphFactory.getGraph(false);
        AbstractGraknGraph<OrientGraph> mindmapsGraph3 = orientGraphFactory.getGraph(true);

        assertEquals(mindmapsGraph1, mindmapsGraph2);
        assertNotEquals(mindmapsGraph2, mindmapsGraph3);

        assertEquals(8, mindmapsGraph1.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(8, mindmapsGraph2.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(8, mindmapsGraph3.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testBuildGraph() throws GraknValidationException {
        GraknOrientDBGraph mindmapsGraph = orientGraphFactory.getGraph(false);

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

    @Test
    public void testVertexIndices(){
        GraknOrientDBGraph mindmapsGraph = orientGraphFactory.getGraph(false);
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            assertEquals(6, mindmapsGraph.getTinkerPopGraph().getVertexIndexedKeys(baseType.name()).size());
        }

        assertNotNull(mindmapsGraph.getMetaEntityType());
        assertNotNull(mindmapsGraph.getMetaRelationType());
        assertNotNull(mindmapsGraph.getMetaType());
    }



}