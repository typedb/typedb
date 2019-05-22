/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.gremlin.spanningtree.graph;

import grakn.core.concept.Label;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.server.session.TransactionOLTP;

import java.util.HashSet;
import java.util.Set;

/**
 * An node in a directed graph.
 *
 */
public class Node {

    private final NodeId nodeId;
    private boolean isValidStartingPoint = true;
    private double fixedFragmentCost = 0;

    private Double nodeWeight = null;
    private Double branchWeight = null;

    private Set<Fragment> fragmentsWithoutDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependencyVisited = new HashSet<>();
    private Set<Fragment> dependants = new HashSet<>();

    // state used by QP planning & statistics
    public enum NodeType {
        // lower is better - a lower ordering indicates the node is more specific
        // ie. has fewer matches in the graph
        ID_NODE(1),     // an ID always matches exactly one node
        SCHEMA_NODE(2),     // a schema node only has 1 node as well, arbitrary ordering between ID and this
        EDGE_NODE(3),       // We currently weight edge nodes as 1 always, but this may change with better stats
        INSTANCE_NODE(4);   // instance nodes are the worst, always matching many vertices

        private int ordering;
        NodeType(int relativeOrdering) {
            this.ordering = relativeOrdering;
        }
        public int getRelativeOrdering() {
            return ordering;
        }

    }
    private NodeType nodeType;
    // null instance type label indicates we have no information and we the total of all instance counts;
    private Label instanceTypeLabel = null;

    public Node(NodeId nodeId, NodeType nodeType) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }
    public void setInstanceLabel(Label label) {
        instanceTypeLabel = label;
    }

    public Set<Fragment> getFragmentsWithoutDependency() {
        return fragmentsWithoutDependency;
    }

    public Set<Fragment> getFragmentsWithDependency() {
        return fragmentsWithDependency;
    }

    public Set<Fragment> getFragmentsWithDependencyVisited() {
        return fragmentsWithDependencyVisited;
    }

    public Set<Fragment> getDependants() {
        return dependants;
    }

    public boolean isValidStartingPoint() {
        return isValidStartingPoint;
    }

    public void setInvalidStartingPoint() {
        isValidStartingPoint = false;
    }

    public double getFixedFragmentCost() {
        return fixedFragmentCost;
    }

    public void setFixedFragmentCost(double fixedFragmentCost) {
        if (this.fixedFragmentCost < fixedFragmentCost) {
            this.fixedFragmentCost = fixedFragmentCost;
        }
    }

    public Double getNodeWeight() {
        return nodeWeight;
    }

    public void setNodeWeight(Double nodeWeight) {
        this.nodeWeight = nodeWeight;
    }

    public Double getBranchWeight() {
        return branchWeight;
    }

    public void setBranchWeight(Double branchWeight) {
        this.branchWeight = branchWeight;
    }

    /**
     * Calculate the expected number of vertices in the graph that match this node, using the information
     * available to this node. Without further refinement, this only consumes the node type and if it's an
     * instance node then returns the total count of the graph vertices
     * @param tx
     * @return estimated number nodes in the graph that may match this node
     */
    public long nodeQuantityEstimate(TransactionOLTP tx) {
        if (nodeType.equals(NodeType.INSTANCE_NODE)) {
            if (instanceTypeLabel == null) {
                // upper bound for now until we can efficiently retrieve the total of all things efficiently
                return 100000L;
            } else {
                return tx.session().keyspaceStatistics().count(tx, instanceTypeLabel);
            }
        } else {
            // there's exactly 1 node for an ID
            // there's exactly 1 schema node for a label
            // we assume nodes representing edges have cardinality 1 for now
            return 1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node that = (Node) o;
        return nodeId.equals(that.nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        return nodeId.toString();
    }

}
