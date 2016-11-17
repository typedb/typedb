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
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class GraknTest {

    @Test
    public void testInMemory(){
        assertThat(Grakn.factory(Grakn.IN_MEMORY, "test").getGraph(), instanceOf(GraknTinkerGraph.class));
    }

    @Test
    public void testInMemorySingleton(){
        GraknGraph test1 = Grakn.factory(Grakn.IN_MEMORY, "test1").getGraph();
        GraknGraph test11 = Grakn.factory(Grakn.IN_MEMORY, "test1").getGraph();
        GraknGraph test2 = Grakn.factory(Grakn.IN_MEMORY, "test2").getGraph();

        assertEquals(test1, test11);
        assertNotEquals(test1, test2);
    }

    @Test
    public void testInMemoryClear(){
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, "default").getGraph();
        graph.clear();
        graph =  Grakn.factory(Grakn.IN_MEMORY, "default").getGraph();
        graph.putEntityType("A thing");
        assertNotNull(graph.getEntityType("A thing"));
    }

    @Test
    public void testComputer(){
        assertThat(Grakn.factory(Grakn.IN_MEMORY, "bob").getGraphComputer(), instanceOf(GraknComputer.class));
    }

    @Test
    public void testSingletonBetweenBatchAndNormalInMemory(){
        String keyspace = "test1";
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraph();
        AbstractGraknGraph batchGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraphBatchLoading();

        assertNotEquals(graph, batchGraph);
        assertEquals(graph.getTinkerPopGraph(), batchGraph.getTinkerPopGraph());
    }
}