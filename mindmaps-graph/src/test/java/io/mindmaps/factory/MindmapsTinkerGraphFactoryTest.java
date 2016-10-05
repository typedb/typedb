/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.graph.internal.MindmapsTinkerGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class MindmapsTinkerGraphFactoryTest {
    private MindmapsInternalFactory tinkerGraphFactory;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setTinkerGraphFactory(){
        tinkerGraphFactory = new MindmapsTinkerInternalFactory("test", null, null);
    }

    @Test
    public void testBuildTinkerGraph() throws Exception {
        MindmapsGraph graph = tinkerGraphFactory.getGraph(false);
        assertThat(graph, instanceOf(MindmapsTinkerGraph.class));
        assertThat(graph, instanceOf(AbstractMindmapsGraph.class));

        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFactorySingleton(){
        MindmapsGraph graph1 = tinkerGraphFactory.getGraph(false);
        MindmapsGraph graph1_copy = tinkerGraphFactory.getGraph(false);

        MindmapsGraph graph2 = tinkerGraphFactory.getGraph(true);
        MindmapsGraph graph2_copy = tinkerGraphFactory.getGraph(true);

        assertEquals(graph1, graph1_copy);
        assertEquals(graph2, graph2_copy);

        assertNotEquals(graph1, graph2);
    }

    @Test
    public void testSimpleBuild(){
        MindmapsTinkerGraph mg1 = (MindmapsTinkerGraph) tinkerGraphFactory.getGraph(true);
        MindmapsTinkerGraph mg2 = (MindmapsTinkerGraph) tinkerGraphFactory.getGraph(false);

        assertTrue(mg1.isBatchLoadingEnabled());
        assertFalse(mg2.isBatchLoadingEnabled());
        assertNotEquals(mg1, mg2);
        assertNotEquals(mg1.getTinkerPopGraph(), mg2.getTinkerPopGraph());
    }

    @Test
    public void testGetTinkerPopGraph(){
        Graph mg1 = tinkerGraphFactory.getTinkerPopGraph(false);
        assertThat(mg1, instanceOf(TinkerGraph.class));
    }

}