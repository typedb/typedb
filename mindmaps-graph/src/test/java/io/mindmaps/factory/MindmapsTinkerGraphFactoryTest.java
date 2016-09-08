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
    private MindmapsGraphFactory tinkerGraphFactory;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        tinkerGraphFactory = new MindmapsTinkerGraphFactory();
    }

    @Test
    public void testBuildTinkerGraph() throws Exception {
        MindmapsGraph graph = tinkerGraphFactory.getGraph("test", null, null, false);
        assertThat(graph, instanceOf(MindmapsTinkerGraph.class));
        assertThat(graph, instanceOf(AbstractMindmapsGraph.class));

        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFactoryMap(){
        MindmapsGraph graph1 = tinkerGraphFactory.getGraph("graph1", null, null, false);
        MindmapsGraph graph2 = tinkerGraphFactory.getGraph("graph2", null, null, false);
        MindmapsGraph graph1_copy = tinkerGraphFactory.getGraph("graph1", null, null, false);

        assertNotEquals(graph1, graph2);
        assertEquals(graph1, graph1_copy);
    }

    @Test
    public void testSimpleBuild(){
        MindmapsTinkerGraph mg1 = (MindmapsTinkerGraph) tinkerGraphFactory.getGraph("test", null, null, true);
        MindmapsTinkerGraph mg2 = (MindmapsTinkerGraph) tinkerGraphFactory.getGraph("test", null, null, false);

        assertTrue(mg1.isBatchLoadingEnabled());
        assertFalse(mg2.isBatchLoadingEnabled());
        assertNotEquals(mg1, mg2);
        assertNotEquals(mg1.getTinkerPopGraph(), mg2.getTinkerPopGraph());
    }

    @Test
    public void testGetTinkerPopGraph(){
        Graph mg1 = tinkerGraphFactory.getTinkerPopGraph("name", null, null, false);
        assertThat(mg1, instanceOf(TinkerGraph.class));
    }
}