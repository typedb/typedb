package grakn.core.graql.gremlin.spanningtree.graph;

import grakn.core.server.session.TransactionOLTP;

public class SchemaNode extends Node {

    private static final int SCHEMA_NODE_PRIORITY = 1;

    public SchemaNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        // only 1 node ever matches a schema node
        return 1;
    }

    @Override
    public int getNodeTypePriority() {
        return SCHEMA_NODE_PRIORITY;
    }
}
