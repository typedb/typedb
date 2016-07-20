package io.mindmaps.core.implementation;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class MindmapsTinkerTransactionTest {
    MindmapsGraph mindmapsGraph;

    @Before
    public void setup(){
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testRefresh() throws Exception {
        mindmapsGraph.newTransaction().refresh();
    }

    @Test
    public void testGetRootGraph() throws Exception {
        MindmapsTinkerTransaction tinkerTransaction = (MindmapsTinkerTransaction) mindmapsGraph.newTransaction();
        assertThat(tinkerTransaction.getRootGraph(), instanceOf(MindmapsTinkerGraph.class));
    }
}