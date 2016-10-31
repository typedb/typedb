package io.grakn;

import io.grakn.graph.internal.MindmapsTinkerGraph;
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