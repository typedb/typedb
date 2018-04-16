/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.spanningtree.graph;

import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * @param <V> the type of the nodes stored
 * @author Jason Liu
 */
public abstract class WeightedGraph<V> {
    public abstract Collection<V> getNodes();

    public abstract double getWeightOf(V source, V dest);

    public abstract Collection<Weighted<DirectedEdge<V>>> getIncomingEdges(V destinationNode);

    public WeightedGraph<V> filterEdges(Predicate<DirectedEdge<V>> predicate) {
        final List<Weighted<DirectedEdge<V>>> allEdges = Lists.newArrayList();
        for (V node : getNodes()) {
            final Collection<Weighted<DirectedEdge<V>>> incomingEdges = getIncomingEdges(node);
            for (Weighted<DirectedEdge<V>> edge : incomingEdges) {
                if (predicate.apply(edge.val)) {
                    allEdges.add(edge);
                }
            }
        }
        return SparseWeightedGraph.from(getNodes(), allEdges);
    }
}
