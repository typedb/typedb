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

import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.fragment.Fragment;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An node in a directed graph.
 *
 */
public class Node implements Comparable {

    private final NodeId nodeId;
    private boolean isValidStartingPoint = true;
    private double fixedFragmentCost = 0;

    private Double nodeWeight = null;
    private Double branchWeight = null;

    private Set<Fragment> fragmentsWithoutDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependencyVisited = new HashSet<>();
    private Set<Fragment> dependants = new HashSet<>();

    // this is a special hash used for ordering nodes, and is computed externally and saved into the node
    private int neighborhoodAwareHash;

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


    public int localHashCode() {
        // a sort of hash based on this node's fragments' internal hash codes
        Set<Fragment> internalFragments = Sets.newHashSet(fragmentsWithoutDependency);
        internalFragments.addAll(fragmentsWithDependency);
        internalFragments.addAll(fragmentsWithDependencyVisited);
        internalFragments.addAll(dependants);

        // sort into a list based on the fragments' hashes
        Integer[] orderedInternalFragmentHashes = internalFragments.stream()
                .map(Fragment::variableAgnosticHash)
                .sorted()
                .collect(Collectors.toList())
                .toArray(new Integer[] {});

        return Objects.hashCode(orderedInternalFragmentHashes);
    }

    public void setGlobalHash(int neighborhoodAwareHash) {
        this.neighborhoodAwareHash = neighborhoodAwareHash;
    }

    public int globalCode() {
        return neighborhoodAwareHash;
    }

    /**
     * We make Node comparable because we can then deterministically order and sort them
     * leading to deterministic query plans
     */
    @Override
    public int compareTo(Object o) {
        if (o instanceof Node) {
            return nodeId.hashCode() - o.hashCode();
        }
        throw new ClassCastException("Cannot compare Node class and " + o.getClass().toString());
    }
}
