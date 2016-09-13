package io.mindmaps.graph.internal;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;

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

    @Override
    public GraphTraversalSource getTinkerTraversal(){
        return getTinkerPopGraph().traversal().withStrategies(ReadOnlyStrategy.instance());
    }
}
