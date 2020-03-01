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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SparseWeightedGraph extends WeightedGraph {
    final private Set<Node> nodes;
    final private Map<Node, Map<Node, Weighted<DirectedEdge>>> incomingEdges;

    private SparseWeightedGraph(Set<Node> nodes, Map<Node, Map<Node, Weighted<DirectedEdge>>> incomingEdges) {
        this.nodes = nodes;
        this.incomingEdges = incomingEdges;
    }

    public static SparseWeightedGraph from(Iterable<Node> nodes, Iterable<Weighted<DirectedEdge>> edges) {
        final Map<Node, Map<Node, Weighted<DirectedEdge>>> incomingEdges = Maps.newHashMap();
        for (Weighted<DirectedEdge> edge : edges) {
            if (!incomingEdges.containsKey(edge.val.destination)) {
                incomingEdges.put(edge.val.destination, Maps.newHashMap());
            }
            incomingEdges.get(edge.val.destination).put(edge.val.source, edge);
        }
        return new SparseWeightedGraph(ImmutableSet.copyOf(nodes), incomingEdges);
    }

    public static  SparseWeightedGraph from(Iterable<Weighted<DirectedEdge>> edges) {
        final Set<Node> nodes = Sets.newHashSet();
        for (Weighted<DirectedEdge> edge : edges) {
            nodes.add(edge.val.source);
            nodes.add(edge.val.destination);
        }
        return SparseWeightedGraph.from(nodes, edges);
    }

    @Override
    public Collection<Node> getNodes() {
        return nodes;
    }

    @Override
    public double getWeightOf(Node source, Node dest) {
        if (!incomingEdges.containsKey(dest)) return Double.NEGATIVE_INFINITY;
        final Map<Node, Weighted<DirectedEdge>> inEdges = incomingEdges.get(dest);
        if (!inEdges.containsKey(source)) return Double.NEGATIVE_INFINITY;
        return inEdges.get(source).weight;
    }

    @Override
    public Collection<Weighted<DirectedEdge>> getIncomingEdges(Node destinationNode) {
        if (!incomingEdges.containsKey(destinationNode)) return ImmutableSet.of();
        return incomingEdges.get(destinationNode).values();
    }
}
