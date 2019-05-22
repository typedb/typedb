package grakn.core.graql.gremlin.spanningtree.graph;

import grakn.core.server.session.TransactionOLTP;

public class EdgeNode extends Node {
    private static final int EDGE_NODE_PRIORITY = 2;

    public EdgeNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        // edge nodes for now return 1 so we don't affect the multiplication of other vertices' counts
        return 1;
    }

    @Override
    public int getNodeTypePriority() {
        return EDGE_NODE_PRIORITY;
    }
}
