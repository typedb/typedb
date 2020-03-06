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
 */

package grakn.core.graql.planning.spanningtree.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.WeightedGraph;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Used for testing
 */
public class DenseWeightedGraph extends WeightedGraph {
    final private ArrayList<Node> nodes;
    final private Map<Node, Integer> indexOf;
    final private double[][] weights;

    private DenseWeightedGraph(ArrayList<Node> nodes, Map<Node, Integer> indexOf, double[][] weights) {
        this.nodes = nodes;
        this.indexOf = indexOf;
        this.weights = weights;
    }

    public static DenseWeightedGraph from(Iterable<Node> nodes, double[][] weights) {
        final ArrayList<Node> nodeList = Lists.newArrayList(nodes);
        Preconditions.checkArgument(nodeList.size() == weights.length);
        final Map<Node, Integer> indexOf = Maps.newHashMap();
        for (int i = 0; i < nodeList.size(); i++) {
            indexOf.put(nodeList.get(i), i);
        }
        return new DenseWeightedGraph(nodeList, indexOf, weights);
    }

    @Override
    public Collection<Node> getNodes() {
        return nodes;
    }

    @Override
    public double getWeightOf(Node source, Node dest) {
        if (!indexOf.containsKey(source) || !indexOf.containsKey(dest)) return Double.NEGATIVE_INFINITY;
        return weights[indexOf.get(source)][indexOf.get(dest)];
    }

    @Override
    public Collection<Weighted<DirectedEdge>> getIncomingEdges(Node destinationNode) {
        if (!indexOf.containsKey(destinationNode)) return Collections.emptySet();
        final int dest = indexOf.get(destinationNode);
        List<Weighted<DirectedEdge>> results = Lists.newArrayList();
        for (int src = 0; src < nodes.size(); src++) {
            results.add(Weighted.weighted(DirectedEdge.from(nodes.get(src)).to(destinationNode), weights[src][dest]));
        }
        return results;
    }
}
