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

package ai.grakn.graql.internal.gremlin.spanningtree.graph;

import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.Range.closedOpen;

/**
 * @param <V> the type of the nodes stored
 * @author Jason Liu
 */
public class DenseWeightedGraph<V> extends WeightedGraph<V> {
    final private ArrayList<V> nodes;
    final private Map<V, Integer> indexOf;
    final private double[][] weights;

    private DenseWeightedGraph(ArrayList<V> nodes, Map<V, Integer> indexOf, double[][] weights) {
        this.nodes = nodes;
        this.indexOf = indexOf;
        this.weights = weights;
    }

    public static <V> DenseWeightedGraph<V> from(Iterable<V> nodes, double[][] weights) {
        final ArrayList<V> nodeList = Lists.newArrayList(nodes);
        Preconditions.checkArgument(nodeList.size() == weights.length);
        final Map<V, Integer> indexOf = Maps.newHashMap();
        for (int i = 0; i < nodeList.size(); i++) {
            indexOf.put(nodeList.get(i), i);
        }
        return new DenseWeightedGraph<>(nodeList, indexOf, weights);
    }

    public static DenseWeightedGraph<Integer> from(double[][] weights) {
        final Set<Integer> nodes = ContiguousSet.create(closedOpen(0, weights.length), integers());
        return DenseWeightedGraph.from(nodes, weights);
    }

    @Override
    public Collection<V> getNodes() {
        return nodes;
    }

    @Override
    public double getWeightOf(V source, V dest) {
        if (!indexOf.containsKey(source) || !indexOf.containsKey(dest)) return Double.NEGATIVE_INFINITY;
        return weights[indexOf.get(source)][indexOf.get(dest)];
    }

    @Override
    public Collection<Weighted<DirectedEdge<V>>> getIncomingEdges(V destinationNode) {
        if (!indexOf.containsKey(destinationNode)) return Collections.emptySet();
        final int dest = indexOf.get(destinationNode);
        List<Weighted<DirectedEdge<V>>> results = Lists.newArrayList();
        for (int src = 0; src < nodes.size(); src++) {
            results.add(Weighted.weighted(DirectedEdge.from(nodes.get(src)).to(destinationNode), weights[src][dest]));
        }
        return results;
    }
}
