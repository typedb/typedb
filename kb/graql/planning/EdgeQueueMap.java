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

package grakn.core.kb.graql.planning;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import grakn.core.common.util.Partition;
import grakn.core.kb.graql.planning.spanningtree.datastructure.FibonacciQueue;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A priority queue of incoming edges for each strongly connected component that we haven't chosen
 * an incoming edge for yet.
 */

class EdgeQueueMap {
    final Partition<Node> partition;
    public final Map<Node, EdgeQueue> queueByDestination;

    public static class EdgeQueue {
        private final Node component;
        public final FibonacciQueue<ExclusiveEdge> edges;
        private final Partition<Node> partition;

        private EdgeQueue(Node component, Partition<Node> partition) {
            this.component = component;
            this.edges = FibonacciQueue.create(Collections.reverseOrder()); // highest weight edges first
            this.partition = partition;
        }

        public static EdgeQueue create(Node component, Partition<Node> partition) {
            return new EdgeQueue(component, partition);
        }

        public void addEdge(ExclusiveEdge exclusiveEdge) {
            // only add if source is external to SCC
            if (partition.componentOf(exclusiveEdge.edge.source).equals(component)) return;
            edges.add(exclusiveEdge);
        }

        /**
         * Always breaks ties in favor of edges in bestArborescence
         */
        public Optional<ExclusiveEdge> popBestEdge(Arborescence<Node> bestArborescence) {
            if (edges.isEmpty()) return Optional.empty();
            final LinkedList<ExclusiveEdge> candidates = Lists.newLinkedList();
            do {
                candidates.addFirst(edges.poll());
            } while (!edges.isEmpty()
                    && edges.comparator().compare(candidates.getFirst(), edges.peek()) == 0  // has to be tied for best
                    && !bestArborescence.contains(candidates.getFirst().edge));   // break if we've found one in `bestArborescence`
            // at this point all edges in `candidates` have equal weight, and if one of them is in `bestArborescence`
            // it will be first
            final ExclusiveEdge bestEdge = candidates.removeFirst();
//            edges.addAll(candidates); // add back all the edges we looked at but didn't pick
            edges.addAll(candidates);
            return Optional.of(bestEdge);
        }
    }

    static class QueueAndReplace {
        private final EdgeQueue queue;
        private final Weighted<DirectedEdge> replace;

        public EdgeQueue queue() {
            return queue;
        }

        public Weighted<DirectedEdge> replace(){
            return replace;
        }

        QueueAndReplace(EdgeQueue queue, Weighted<DirectedEdge> replace) {
            this.queue = queue;
            this.replace = replace;
        }

        static QueueAndReplace of(EdgeQueueMap.EdgeQueue queue, Weighted<DirectedEdge> replace) {
            return new QueueAndReplace(queue, replace);
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            } else if (!(o instanceof QueueAndReplace)) {
                return false;
            } else {
                QueueAndReplace that = (QueueAndReplace)o;
                return this.queue.equals(that.queue()) && this.replace.equals(that.replace());
            }
        }

        public int hashCode() {
            return Objects.hash(queue, replace);
        }
    }

    EdgeQueueMap(Partition<Node> partition) {
        this.partition = partition;
        this.queueByDestination = Maps.newHashMap();
    }

    public void addEdge(Weighted<DirectedEdge> edge) {
        final Node destination = partition.componentOf(edge.val.destination);
        if (!queueByDestination.containsKey(destination)) {
            queueByDestination.put(destination, EdgeQueue.create(destination, partition));
        }
        final List<DirectedEdge> replaces = Lists.newLinkedList();
        queueByDestination.get(destination).addEdge(ExclusiveEdge.of(edge.val, replaces, edge.weight));
    }

    /**
     * Always breaks ties in favor of edges in best
     */
    public Optional<ExclusiveEdge> popBestEdge(Node component, Arborescence<Node> best) {
        if (!queueByDestination.containsKey(component)) return Optional.empty();
        return queueByDestination.get(component).popBestEdge(best);
    }

    public EdgeQueue merge(Node component, Iterable<QueueAndReplace> queuesToMerge) {
        final EdgeQueue result = EdgeQueue.create(component, partition);
        for (QueueAndReplace queueAndReplace : queuesToMerge) {
            final EdgeQueue queue = queueAndReplace.queue();
            final Weighted<DirectedEdge> replace = queueAndReplace.replace();
            for (ExclusiveEdge wEdgeAndExcluded : queue.edges) {
                final List<DirectedEdge> replaces = wEdgeAndExcluded.excluded;
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
