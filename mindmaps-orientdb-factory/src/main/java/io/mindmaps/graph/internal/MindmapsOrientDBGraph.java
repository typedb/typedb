package io.mindmaps.graph.internal;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;

public class MindmapsOrientDBGraph extends AbstractMindmapsGraph<OrientGraph> {
    public MindmapsOrientDBGraph(OrientGraph graph, String name, String engineUrl, boolean batchLoading){
        super(graph, name, engineUrl, batchLoading);
    }

    @Override
    public void clear() {
        getTinkerPopGraph().traversal().V().drop().iterate();
    }

    @Override
    protected void commitTx(){
        getTinkerPopGraph().commit();
    }
}
