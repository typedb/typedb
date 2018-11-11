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

import grakn.core.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @param <V> the type of the nodes stored
 * @author Jason Liu
 */
public class SparseWeightedGraph<V> extends WeightedGraph<V> {
    final private Set<V> nodes;
    final private Map<V, Map<V, Weighted<DirectedEdge<V>>>> incomingEdges;

    private SparseWeightedGraph(Set<V> nodes, Map<V, Map<V, Weighted<DirectedEdge<V>>>> incomingEdges) {
        this.nodes = nodes;
        this.incomingEdges = incomingEdges;
    }

    public static <T> SparseWeightedGraph<T> from(Iterable<T> nodes, Iterable<Weighted<DirectedEdge<T>>> edges) {
        final Map<T, Map<T, Weighted<DirectedEdge<T>>>> incomingEdges = Maps.newHashMap();
        for (Weighted<DirectedEdge<T>> edge : edges) {
            if (!incomingEdges.containsKey(edge.val.destination)) {
                incomingEdges.put(edge.val.destination, Maps.newHashMap());
            }
            incomingEdges.get(edge.val.destination).put(edge.val.source, edge);
        }
        return new SparseWeightedGraph<>(ImmutableSet.copyOf(nodes), incomingEdges);
    }

    public static <T> SparseWeightedGraph<T> from(Iterable<Weighted<DirectedEdge<T>>> edges) {
        final Set<T> nodes = Sets.newHashSet();
        for (Weighted<DirectedEdge<T>> edge : edges) {
            nodes.add(edge.val.source);
            nodes.add(edge.val.destination);
        }
        return SparseWeightedGraph.from(nodes, edges);
    }

    @Override
    public Collection<V> getNodes() {
        return nodes;
    }

    @Override
    public double getWeightOf(V source, V dest) {
        if (!incomingEdges.containsKey(dest)) return Double.NEGATIVE_INFINITY;
        final Map<V, Weighted<DirectedEdge<V>>> inEdges = incomingEdges.get(dest);
        if (!inEdges.containsKey(source)) return Double.NEGATIVE_INFINITY;
        return inEdges.get(source).weight;
    }

    @Override
    public Collection<Weighted<DirectedEdge<V>>> getIncomingEdges(V destinationNode) {
        if (!incomingEdges.containsKey(destinationNode)) return ImmutableSet.of();
        return incomingEdges.get(destinationNode).values();
    }
}
