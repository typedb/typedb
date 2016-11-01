package ai.grakn.factory;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraknOrientDBGraph;
import ai.grakn.util.Schema;
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
        AbstractGraknGraph graknGraph = orientGraphFactory.getGraph(false);
        assertThat(graknGraph.getTinkerPopGraph(), instanceOf(OrientGraph.class));
        assertEquals(8, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testBuildSingletonGraphs(){
        AbstractGraknGraph<OrientGraph> graknGraph1 = orientGraphFactory.getGraph(false);
        AbstractGraknGraph<OrientGraph> graknGraph2 = orientGraphFactory.getGraph(false);
        AbstractGraknGraph<OrientGraph> graknGraph3 = orientGraphFactory.getGraph(true);

        assertEquals(graknGraph1, graknGraph2);
        assertNotEquals(graknGraph2, graknGraph3);

        assertEquals(8, graknGraph1.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(8, graknGraph2.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(8, graknGraph3.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testBuildGraph() throws GraknValidationException {
        GraknOrientDBGraph graknGraph = orientGraphFactory.getGraph(false);

        assertEquals(8, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertNotNull(graknGraph.getMetaEntityType());

        EntityType entityType = graknGraph.putEntityType("My Entity Type");
        graknGraph.addEntity(entityType);
        graknGraph.addEntity(entityType);
        graknGraph.addEntity(entityType);
        graknGraph.commit();
        assertEquals(12, graknGraph.getTinkerPopGraph().traversal().V().toList().size());

        RoleType role1 = graknGraph.putRoleType("Role 1");
        RoleType role2 = graknGraph.putRoleType("Role 2");
        graknGraph.putRelationType("My Relation Type").hasRole(role1).hasRole(role2);
        graknGraph.commit();
        assertEquals(15, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testVertexIndices(){
        GraknOrientDBGraph graknGraph = orientGraphFactory.getGraph(false);
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            assertEquals(6, graknGraph.getTinkerPopGraph().getVertexIndexedKeys(baseType.name()).size());
        }

        assertNotNull(graknGraph.getMetaEntityType());
        assertNotNull(graknGraph.getMetaRelationType());
        assertNotNull(graknGraph.getMetaType());
    }



}