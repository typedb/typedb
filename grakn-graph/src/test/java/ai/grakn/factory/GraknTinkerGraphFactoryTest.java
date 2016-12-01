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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraknTinkerGraph;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class GraknTinkerGraphFactoryTest {
    private InternalFactory tinkerGraphFactory;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setTinkerGraphFactory(){
        tinkerGraphFactory = new TinkerInternalFactory("test", Grakn.IN_MEMORY, null);
    }

    @Test
    public void testBuildTinkerGraph() throws Exception {
        GraknGraph graph = tinkerGraphFactory.getGraph(false);
        assertThat(graph, instanceOf(GraknTinkerGraph.class));
        assertThat(graph, instanceOf(AbstractGraknGraph.class));

        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFactorySingleton(){
        GraknGraph graph1 = tinkerGraphFactory.getGraph(false);
        GraknGraph graph1_copy = tinkerGraphFactory.getGraph(false);

        GraknGraph graph2 = tinkerGraphFactory.getGraph(true);
        GraknGraph graph2_copy = tinkerGraphFactory.getGraph(true);

        assertEquals(graph1, graph1_copy);
        assertEquals(graph2, graph2_copy);

        assertNotEquals(graph1, graph2);

        TinkerGraph tinkerGraph1 = ((GraknTinkerGraph) graph1).getTinkerPopGraph();
        TinkerGraph tinkerGraph2 = ((GraknTinkerGraph) graph2).getTinkerPopGraph();

        assertEquals(tinkerGraph1, tinkerGraph2);
    }

    @Test
    public void testSimpleBuild(){
        GraknTinkerGraph mg1 = (GraknTinkerGraph) tinkerGraphFactory.getGraph(true);
        GraknTinkerGraph mg2 = (GraknTinkerGraph) tinkerGraphFactory.getGraph(false);

        assertTrue(mg1.isBatchLoadingEnabled());
        assertFalse(mg2.isBatchLoadingEnabled());
        assertNotEquals(mg1, mg2);
        assertEquals(mg1.getTinkerPopGraph(), mg2.getTinkerPopGraph());
    }

    @Test
    public void testGetTinkerPopGraph(){
        Graph mg1 = tinkerGraphFactory.getTinkerPopGraph(false);
        assertThat(mg1, instanceOf(TinkerGraph.class));
    }

    @Test
    public void testGetNullKeySpace(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.NULL_VALUE.getMessage("keyspace"))
        ));

        tinkerGraphFactory = new TinkerInternalFactory(null, null, null);
        tinkerGraphFactory.getGraph(false);
    }

}