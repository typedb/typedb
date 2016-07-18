package io.mindmaps.core.implementation;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class MindmapsTinkerGraph implements MindmapsGraph {

    private TinkerGraph tinkerGraph;

    public MindmapsTinkerGraph(){
        tinkerGraph = TinkerGraph.open();
        new MindmapsTinkerTransaction(this).initialiseMetaConcepts();
    }

    @Override
    public MindmapsTransaction newTransaction() {
        if(tinkerGraph == null){
            tinkerGraph = TinkerGraph.open();
        }
        return new MindmapsTinkerTransaction(this);
    }

    @Override
    public void close() {
        clear();
        tinkerGraph = null;
    }

    @Override
    public void clear() {
        tinkerGraph.close();
    }

    @Override
    public Graph getGraph() {
        return tinkerGraph;
    }
}
