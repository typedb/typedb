package ai.grakn;

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
}