package io.mindmaps;

import io.mindmaps.graph.internal.MindmapsTinkerGraph;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class MindmapsTest {

    @Test
    public void testInMemory(){
        assertThat(Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("test"), instanceOf(MindmapsTinkerGraph.class));
    }

    @Test
    public void testInMemorySingleton(){
        MindmapsGraph test1 = Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("test1");
        MindmapsGraph test11 = Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("test1");
        MindmapsGraph test2 = Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("test2");

        assertEquals(test1, test11);
        assertNotEquals(test1, test2);
    }

    @Test
    public void testInMemoryClear(){
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("default");
        graph.clear();
        graph =  Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("default");
        graph.putEntityType("A thing");
        assertNotNull(graph.getEntityType("A thing"));
    }
}