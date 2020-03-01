/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.kb.graql.planning.spanningtree.graph;

import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.keyspace.KeyspaceStatistics;

import java.util.HashSet;
import java.util.Set;

/**
 * An node in a directed graph.
 */
public abstract class Node {

    private final NodeId nodeId;
    private boolean isValidStartingPoint = true;
    private double fixedFragmentCost = 0;

    private Double nodeWeight = null;
    private Double branchWeight = null;

    private Set<Fragment> fragmentsWithoutDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependencyVisited = new HashSet<>();
    private Set<Fragment> dependants = new HashSet<>();


    public Node(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public NodeId getNodeId() {
        return nodeId;
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
     * instance node then returns the total count of the graph vertices using labels to refine if available
     *
     * @return estimated number nodes in the graph that may match this node (aiming for an upper bound)
     */
    public abstract long matchingElementsEstimate(ConceptManager conceptManager, KeyspaceStatistics statistics);

    /**
     * Lower is a more specific and therefore a more desirable node type
     * @return the node priority - lower is better
     */
    public abstract int getNodeTypePriority();

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
