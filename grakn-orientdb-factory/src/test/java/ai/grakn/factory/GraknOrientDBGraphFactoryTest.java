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

package ai.grakn.factory;

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.Grakn;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.GraknOrientDBGraph;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class GraknOrientDBGraphFactoryTest {
    private final static String TEST_NAME = "MyGraph";
    private final static String TEST_URI = Grakn.IN_MEMORY;
    private static OrientDBInternalFactory orientGraphFactory ;

    @Before
    public void setUp() throws Exception {
        orientGraphFactory = new OrientDBInternalFactory(TEST_NAME, TEST_URI, null);
    }

    @After
    public void clear() throws GraknValidationException {
        GraknOrientDBGraph graph = orientGraphFactory.getGraph(false);
        graph.clear();
    }

    @Ignore
    @Test
    public void testBuildSimpleGraph() throws Exception {
        AbstractGraknGraph graknGraph = orientGraphFactory.getGraph(false);
        assertThat(graknGraph.getTinkerPopGraph(), instanceOf(OrientGraph.class));
        assertEquals(8, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Ignore
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

    @Ignore// Null index does not work on orientDB
    @Test
    public void testBuildGraph() throws GraknValidationException {
        GraknOrientDBGraph graknGraph = orientGraphFactory.getGraph(false);

        assertEquals(8, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertNotNull(graknGraph.getMetaEntityType());

        EntityType entityType = graknGraph.putEntityType("My Entity Type");
        entityType.addEntity();
        entityType.addEntity();
        entityType.addEntity();
        graknGraph.commit();
        assertEquals(12, graknGraph.getTinkerPopGraph().traversal().V().toList().size());

        RoleType role1 = graknGraph.putRoleType("Role 1");
        RoleType role2 = graknGraph.putRoleType("Role 2");
        graknGraph.putRelationType("My Relation Type").hasRole(role1).hasRole(role2);
        graknGraph.commit();
        assertEquals(15, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Ignore
    @Test
    public void testVertexIndices(){
        GraknOrientDBGraph graknOrientDBGraph = orientGraphFactory.getGraph(false);
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            assertEquals(6, graknOrientDBGraph.getTinkerPopGraph().getVertexIndexedKeys(baseType.name()).size());
        }

        assertNotNull(graknOrientDBGraph.getMetaEntityType());
        assertNotNull(graknOrientDBGraph.getMetaRelationType());
        assertNotNull(graknOrientDBGraph.getMetaConcept());
    }



}