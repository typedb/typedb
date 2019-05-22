package grakn.core.graql.gremlin.spanningtree.graph;

import grakn.core.concept.Label;
import grakn.core.server.session.TransactionOLTP;

public class InstanceNode extends Node {

    private static final int INSTANCE_NODE_PRIORTIY = 3;
    // null instance type label indicates we have no information and we the total of all instance counts;
    private Label instanceTypeLabel = null;

    public InstanceNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        if (instanceTypeLabel == null) {
            // upper bound for now until we can efficiently retrieve the total of all things efficiently
            return 100000L;
        } else {
            return tx.session().keyspaceStatistics().count(tx, instanceTypeLabel);
        }
    }

    @Override
    public int getNodeTypePriority() {
        return INSTANCE_NODE_PRIORTIY;
    }

    public void setInstanceLabel(Label instanceTypeLabel) {
        this.instanceTypeLabel = instanceTypeLabel;
    }

    public Label getInstanceLabel() {
        return instanceTypeLabel;
    }

}

