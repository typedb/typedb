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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.ErrorMessage.NULL_VALUE;
import static ai.grakn.util.ErrorMessage.TRANSACTION_ALREADY_OPEN;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class GraknTinkerGraphFactoryTest {
    private InternalFactory tinkerGraphFactory;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupTinkerGraphFactory(){
        tinkerGraphFactory = new TinkerInternalFactory("test", Grakn.IN_MEMORY, null);
    }

    @Test
    public void whenBuildingGraphUsingTinkerFactory_ReturnGraknTinkerGraph() throws Exception {
        GraknGraph graph = tinkerGraphFactory.getGraph(false);
        assertThat(graph, instanceOf(GraknTinkerGraph.class));
        assertThat(graph, instanceOf(AbstractGraknGraph.class));
    }

    @Test
    public void whenBuildingGraphFromTheSameFactory_ReturnSingletonGraphs(){
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
    public void whenRetrievingGraphFromGraknTinkerGraph_ReturnTinkerGraph(){
        assertThat(tinkerGraphFactory.getTinkerPopGraph(false), instanceOf(TinkerGraph.class));
    }

    @Test
    public void whenCreatingFactoryWithNullKeyspace_Throw(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(NULL_VALUE.getMessage("keyspace"));
        tinkerGraphFactory = new TinkerInternalFactory(null, null, null);
    }

    @Test
    public void whenGettingGraphFromFactoryWithAlreadyOpenGraph_Throw(){
        TinkerInternalFactory factory = new TinkerInternalFactory("MyTest", Grakn.IN_MEMORY, null);
        factory.getGraph(false);
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(TRANSACTION_ALREADY_OPEN.getMessage("MyTest"));
    }

}