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

package ai.grakn;

import ai.grakn.graph.internal.MindmapsTinkerGraph;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class MindmapsTest {

    @Test
    public void testInMemory(){
        assertThat(Mindmaps.factory(Mindmaps.IN_MEMORY, "test").getGraph(), instanceOf(MindmapsTinkerGraph.class));
    }

    @Test
    public void testInMemorySingleton(){
        MindmapsGraph test1 = Mindmaps.factory(Mindmaps.IN_MEMORY, "test1").getGraph();
        MindmapsGraph test11 = Mindmaps.factory(Mindmaps.IN_MEMORY, "test1").getGraph();
        MindmapsGraph test2 = Mindmaps.factory(Mindmaps.IN_MEMORY, "test2").getGraph();

        assertEquals(test1, test11);
        assertNotEquals(test1, test2);
    }

    @Test
    public void testInMemoryClear(){
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.IN_MEMORY, "default").getGraph();
        graph.clear();
        graph =  Mindmaps.factory(Mindmaps.IN_MEMORY, "default").getGraph();
        graph.putEntityType("A thing");
        assertNotNull(graph.getEntityType("A thing"));
    }

    @Test
    public void testComputer(){
        assertThat(Mindmaps.factory(Mindmaps.IN_MEMORY, "bob").getGraphComputer(), instanceOf(MindmapsComputer.class));
    }
}