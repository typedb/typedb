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

package ai.grakn.graql.internal.gremlin.spanningtree;

import ai.grakn.graql.internal.gremlin.spanningtree.datastructure.FibonacciQueue;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import ai.grakn.graql.internal.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A priority queue of incoming edges for each strongly connected component that we haven't chosen
 * an incoming edge for yet.
 *
 * @param <V> the type of the nodes stored
 * @author Jason Liu
 */

class EdgeQueueMap<V> {
    final Partition<V> partition;
    public final Map<V, EdgeQueue<V>> queueByDestination;

    public static class EdgeQueue<V> {
        private final V component;
        public final FibonacciQueue<ExclusiveEdge<V>> edges;
        private final Partition<V> partition;

        private EdgeQueue(V component, Partition<V> partition) {
            this.component = component;
            this.edges = FibonacciQueue.create(Collections.reverseOrder()); // highest weight edges first
            this.partition = partition;
        }

        public static <T> EdgeQueue<T> create(T component, Partition<T> partition) {
            return new EdgeQueue<>(component, partition);
        }

        public void addEdge(ExclusiveEdge<V> exclusiveEdge) {
            // only add if source is external to SCC
            if (partition.componentOf(exclusiveEdge.edge.source).equals(component)) return;
            edges.add(exclusiveEdge);
        }

        /**
         * Always breaks ties in favor of edges in bestArborescence
         */
        public Optional<ExclusiveEdge<V>> popBestEdge(Arborescence<V> bestArborescence) {
            if (edges.isEmpty()) return Optional.empty();
            final LinkedList<ExclusiveEdge<V>> candidates = Lists.newLinkedList();
            do {
                candidates.addFirst(edges.poll());
            } while (!edges.isEmpty()
                    && edges.comparator().compare(candidates.getFirst(), edges.peek()) == 0  // has to be tied for best
                    && !bestArborescence.contains(candidates.getFirst().edge));   // break if we've found one in `bestArborescence`
            // at this point all edges in `candidates` have equal weight, and if one of them is in `bestArborescence`
            // it will be first
            final ExclusiveEdge<V> bestEdge = candidates.removeFirst();
//            edges.addAll(candidates); // add back all the edges we looked at but didn't pick
            edges.addAll(candidates);
            return Optional.of(bestEdge);
        }
    }

    @AutoValue
    abstract static class QueueAndReplace<V> {
        abstract EdgeQueue<V> queue();
        abstract Weighted<DirectedEdge<V>> replace();

        static <V> QueueAndReplace<V> of(EdgeQueueMap.EdgeQueue<V> queue, Weighted<DirectedEdge<V>> replace) {
            return new AutoValue_EdgeQueueMap_QueueAndReplace<>(queue, replace);
        }
    }

    EdgeQueueMap(Partition<V> partition) {
        this.partition = partition;
        this.queueByDestination = Maps.newHashMap();
    }

    public void addEdge(Weighted<DirectedEdge<V>> edge) {
        final V destination = partition.componentOf(edge.val.destination);
        if (!queueByDestination.containsKey(destination)) {
            queueByDestination.put(destination, EdgeQueue.create(destination, partition));
        }
        final List<DirectedEdge<V>> replaces = Lists.newLinkedList();
        queueByDestination.get(destination).addEdge(ExclusiveEdge.of(edge.val, replaces, edge.weight));
    }

    /**
     * Always breaks ties in favor of edges in best
     */
    public Optional<ExclusiveEdge<V>> popBestEdge(V component, Arborescence<V> best) {
        if (!queueByDestination.containsKey(component)) return Optional.empty();
        return queueByDestination.get(component).popBestEdge(best);
    }

    public EdgeQueue merge(V component, Iterable<QueueAndReplace<V>> queuesToMerge) {
        final EdgeQueue<V> result = EdgeQueue.create(component, partition);
        for (QueueAndReplace<V> queueAndReplace : queuesToMerge) {
            final EdgeQueue<V> queue = queueAndReplace.queue();
            final Weighted<DirectedEdge<V>> replace = queueAndReplace.replace();
            for (ExclusiveEdge<V> wEdgeAndExcluded : queue.edges) {
                final List<DirectedEdge<V>> replaces = wEdgeAndExcluded.excluded;
                replaces.add(replace.val);
                result.addEdge(ExclusiveEdge.of(
                        wEdgeAndExcluded.edge,
                        replaces,
                        wEdgeAndExcluded.weight - replace.weight));
            }
        }
        queueByDestination.put(component, result);
        return result;
    }
}
