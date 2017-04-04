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
import ai.grakn.GraknTxType;
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
        GraknGraph graph = tinkerGraphFactory.open(GraknTxType.WRITE);
        assertThat(graph, instanceOf(GraknTinkerGraph.class));
        assertThat(graph, instanceOf(AbstractGraknGraph.class));
    }

    @Test
    public void whenBuildingGraphFromTheSameFactory_ReturnSingletonGraphs(){
        GraknGraph graph1 = tinkerGraphFactory.open(GraknTxType.WRITE);
        TinkerGraph tinkerGraph1 = ((GraknTinkerGraph) graph1).getTinkerPopGraph();
        graph1.close();
        GraknGraph graph1_copy = tinkerGraphFactory.open(GraknTxType.WRITE);
        graph1_copy.close();

        GraknGraph graph2 = tinkerGraphFactory.open(GraknTxType.BATCH);
        TinkerGraph tinkerGraph2 = ((GraknTinkerGraph) graph2).getTinkerPopGraph();
        graph2.close();
        GraknGraph graph2_copy = tinkerGraphFactory.open(GraknTxType.BATCH);

        assertEquals(graph1, graph1_copy);
        assertEquals(graph2, graph2_copy);

        assertNotEquals(graph1, graph2);
        assertEquals(tinkerGraph1, tinkerGraph2);
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
        TinkerInternalFactory factory = new TinkerInternalFactory("mytest", Grakn.IN_MEMORY, null);
        factory.open(GraknTxType.WRITE);
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(TRANSACTION_ALREADY_OPEN.getMessage("mytest"));
        factory.open(GraknTxType.WRITE);
    }

    @Test
    public void whenGettingGraphFromFactoryClosingItAndGettingItAgain_ReturnGraph(){
        TinkerInternalFactory factory = new TinkerInternalFactory("mytest", Grakn.IN_MEMORY, null);
        GraknGraph graph1 = factory.open(GraknTxType.WRITE);
        graph1.close();
        GraknTinkerGraph graph2 = factory.open(GraknTxType.WRITE);
        assertEquals(graph1, graph2);
    }

}