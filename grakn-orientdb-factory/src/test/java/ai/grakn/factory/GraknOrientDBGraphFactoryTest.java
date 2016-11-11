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

package ai.grakn.factory;

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
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