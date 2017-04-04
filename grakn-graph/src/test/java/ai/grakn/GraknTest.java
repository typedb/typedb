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

package ai.grakn;

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraknTinkerGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class GraknTest {

    @Test
    public void testInMemory(){
        assertThat(Grakn.session(Grakn.IN_MEMORY, "test").open(GraknTxType.WRITE), instanceOf(GraknTinkerGraph.class));
    }

    @Test
    public void testInMemorySingleton(){
        GraknGraph test1 = Grakn.session(Grakn.IN_MEMORY, "test1").open(GraknTxType.WRITE);
        test1.close();
        GraknGraph test11 = Grakn.session(Grakn.IN_MEMORY, "test1").open(GraknTxType.WRITE);
        GraknGraph test2 = Grakn.session(Grakn.IN_MEMORY, "test2").open(GraknTxType.WRITE);

        assertEquals(test1, test11);
        assertNotEquals(test1, test2);
    }

    @Test
    public void testInMemoryClear(){
        GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "default").open(GraknTxType.WRITE);
        graph.clear();
        graph = Grakn.session(Grakn.IN_MEMORY, "default").open(GraknTxType.WRITE);
        graph.putEntityType("A thing");
        assertNotNull(graph.getEntityType("A thing"));
    }

    @Test
    public void testComputer(){
        assertThat(Grakn.session(Grakn.IN_MEMORY, "bob").getGraphComputer(), instanceOf(GraknComputer.class));
    }

    @Test
    public void testSingletonBetweenBatchAndNormalInMemory(){
        String keyspace = "test1";
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.WRITE);
        Graph tinkerGraph = graph.getTinkerPopGraph();
        graph.close();
        AbstractGraknGraph batchGraph = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.BATCH);

        assertNotEquals(graph, batchGraph);
        assertEquals(tinkerGraph, batchGraph.getTinkerPopGraph());

        graph.close();
        batchGraph.close();
    }
}