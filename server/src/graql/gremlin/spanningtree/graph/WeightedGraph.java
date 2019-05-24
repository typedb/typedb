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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import grakn.core.graql.gremlin.spanningtree.util.Weighted;

import java.util.Collection;
import java.util.List;

/**
 */
public abstract class WeightedGraph {
    public abstract Collection<Node> getNodes();

    public abstract double getWeightOf(Node source, Node dest);

    public abstract Collection<Weighted<DirectedEdge>> getIncomingEdges(Node destinationNode);

    public WeightedGraph filterEdges(Predicate<DirectedEdge> predicate) {
        final List<Weighted<DirectedEdge>> allEdges = Lists.newArrayList();
        for (Node node : getNodes()) {
            final Collection<Weighted<DirectedEdge>> incomingEdges = getIncomingEdges(node);
            for (Weighted<DirectedEdge> edge : incomingEdges) {
                if (predicate.apply(edge.val)) {
                    allEdges.add(edge);
                }
            }
        }
        return SparseWeightedGraph.from(getNodes(), allEdges);
    }
}
