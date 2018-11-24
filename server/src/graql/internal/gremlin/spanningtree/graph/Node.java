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

package grakn.core.graql.internal.gremlin.spanningtree.graph;

import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.internal.gremlin.fragment.Fragment;

import java.util.HashSet;
import java.util.Map;
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

    private Node(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public static Node addIfAbsent(NodeId.NodeType nodeType, Var var, Map<NodeId, Node> nodes) {
        NodeId nodeId = new NodeId(nodeType, var);
        if (!nodes.containsKey(nodeId)) {
            nodes.put(nodeId, new Node(nodeId));
        }
        return nodes.get(nodeId);
    }

    public static Node addIfAbsent(NodeId.NodeType nodeType, Set<Var> vars, Map<NodeId, Node> nodes) {
        NodeId nodeId = new NodeId(nodeType, vars);
        if (!nodes.containsKey(nodeId)) {
            nodes.put(nodeId, new Node(nodeId));
        }
        return nodes.get(nodeId);
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
