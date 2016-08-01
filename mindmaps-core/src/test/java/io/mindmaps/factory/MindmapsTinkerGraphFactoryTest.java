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

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;
import io.mindmaps.core.implementation.MindmapsTinkerTransaction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
        MindmapsGraph graph = tinkerGraphFactory.getGraph("test", null, null);
        MindmapsTransaction transaction = graph.newTransaction();
        assertThat(graph, instanceOf(MindmapsTinkerGraph.class));
        assertThat(transaction, instanceOf(MindmapsTinkerTransaction.class));;

        try {
            transaction.close();
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFactoryMap(){
        MindmapsGraph graph1 = tinkerGraphFactory.getGraph("graph1", null, null);
        MindmapsGraph graph2 = tinkerGraphFactory.getGraph("graph2", null, null);
        MindmapsGraph graph1_copy = tinkerGraphFactory.getGraph("graph1", null, null);

        assertNotEquals(graph1, graph2);
        assertEquals(graph1, graph1_copy);
    }
}