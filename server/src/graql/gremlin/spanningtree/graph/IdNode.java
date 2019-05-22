package grakn.core.graql.gremlin.spanningtree.graph;

import grakn.core.server.session.TransactionOLTP;

public class IdNode extends Node {
    private static final int ID_NODE_PRIORITY = 0;

    public IdNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        // only 1 node matches a node with an ID
        return 1;
    }

    @Override
    public int getNodeTypePriority() {
        return ID_NODE_PRIORITY;
    }
}
