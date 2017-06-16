/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.spanningtree;

import ai.grakn.graql.internal.gremlin.spanningtree.graph.Edge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.WeightedGraph;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Pair;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.spanningtree.ChuLiuEdmonds.PartialSolution;
import static ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted.weighted;
import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singleton;

/**
 * Finds the k best arborescences of a complete, directed graph in O(n^2 log n) time.
 * Based on "The k best spanning arborescences of a network." Camerini et al. 1980
 *
 * @author sthomson@cs.cmu.edu, swabha@cs.cmu.edu
 */
public class KBestArborescences {
    /**
     * Find the k-best arborescences of `graph`, rooted in the given node `root`.
     */
    public static <V> List<Weighted<Arborescence<V>>> getKBestArborescences(WeightedGraph<V> graph, V root, int k) {
        // remove all edges incoming to `root`. resulting arborescence is then forced to be rooted at `root`.
        return getKBestArborescences(graph.filterEdges(not(Edge.hasDestination(root))), k);
    }

    /**
     * Find the k-best arborescences of `graph`.
     * Equivalent to the RANK function in Camerini et al. 1980.
     */
    private static <V> List<Weighted<Arborescence<V>>> getKBestArborescences(WeightedGraph<V> graph, int k) {
        final List<Weighted<Arborescence<V>>> results = Lists.newArrayList();
        if (k < 1) return results;
        // 1-best
        final Weighted<Arborescence<V>> best = ChuLiuEdmonds.getMaxArborescence(graph);
        results.add(best);
        if (k < 2) return results;
        final PriorityQueue<Weighted<SubsetOfSolutions<V>>> queue = Queues.newPriorityQueue();
        // find the edge you need to ban to get the 2nd best
        final Set<Edge<V>> empty = ImmutableSet.of();
        queue.addAll(scoreSubsetOfSolutions(graph, empty, empty, best).asSet());
        for (int j = 2; j <= k && !queue.isEmpty(); j++) {
            final Weighted<SubsetOfSolutions<V>> wItem = queue.poll();
            final SubsetOfSolutions<V> item = wItem.val;
            // divide this subset into 2: things that have `edgeToBan`, and those that don't.
            // We have already pre-calculated that `jthBest` will not contain `edgeToBan`
            final Set<Edge<V>> newBanned = copyOf(concat(item.banned, singleton(item.edgeToBan)));
            final Weighted<Arborescence<V>> jthBest =
                    ChuLiuEdmonds.getMaxArborescence(graph, item.required, newBanned);
            assert jthBest.weight == wItem.weight;
            results.add(jthBest);
            // subset of solutions in item that *don't* have `edgeToBan`, except `jthBest`
            queue.addAll(scoreSubsetOfSolutions(graph, item.required, newBanned, jthBest).asSet());
            // subset of solutions in item that *do* have `edgeToBan`, except `bestArborescence`
            final Set<Edge<V>> newRequired = copyOf(concat(item.required, singleton(item.edgeToBan)));
            queue.addAll(scoreSubsetOfSolutions(graph, newRequired, item.banned, item.bestArborescence).asSet());
        }
        return results;
    }

    static <V> Optional<Weighted<SubsetOfSolutions<V>>>
    scoreSubsetOfSolutions(WeightedGraph<V> graph,
                           Set<Edge<V>> required,
                           Set<Edge<V>> banned,
                           Weighted<Arborescence<V>> wBestArborescence) {
        final WeightedGraph<V> filtered =
                graph.filterEdges(and(not(Edge.competesWith(required)), not(Edge.isIn(banned))));
        final Optional<Pair<Edge<V>, Double>> oEdgeToBanAndDiff =
                getNextBestArborescence(filtered, wBestArborescence.val);
        if (oEdgeToBanAndDiff.isPresent()) {
            final Pair<Edge<V>, Double> edgeToBanAndDiff = oEdgeToBanAndDiff.get();
            return Optional.of(weighted(
                    new SubsetOfSolutions<V>(
                            edgeToBanAndDiff.first,
                            wBestArborescence,
                            required,
                            banned),
                    wBestArborescence.weight - edgeToBanAndDiff.second
            ));
        } else {
            return Optional.absent();
        }
    }

    /**
     * Finds the edge you need to ban in order to get the second best solution (and how much worse that
     * second best solution will be)
     * Corresponds to the NEXT function in Camerini et al. 1980
     */
    private static <V> Optional<Pair<Edge<V>, Double>> getNextBestArborescence(WeightedGraph<V> graph,
                                                                               Arborescence<V> bestArborescence) {
        final PartialSolution<V> partialSolution =
                PartialSolution.initialize(graph.filterEdges(not(Edge.isAutoCycle())));
        // In the beginning, subgraph has no edges, so no SCC has in-edges.
        final Queue<V> componentsWithNoInEdges = Lists.newLinkedList(graph.getNodes());

        double bestDifference = Double.POSITIVE_INFINITY;
        Optional<ExclusiveEdge<V>> bestEdgeToKickOut = Optional.absent();

        // Work our way through all componentsWithNoInEdges, in no particular order
        while (!componentsWithNoInEdges.isEmpty()) {
            final V component = componentsWithNoInEdges.poll();
            // find maximum edge entering 'component' from the outside.
            // break ties in favor of edges in bestArborescence
            final Optional<ExclusiveEdge<V>> oMaxInEdge = partialSolution.popBestEdge(component, bestArborescence);
            if (!oMaxInEdge.isPresent()) continue; // No in-edges left to consider for this component. Done with it!
            final ExclusiveEdge<V> maxInEdge = oMaxInEdge.get();
            if (bestArborescence.parents.get(maxInEdge.edge.destination).equals(maxInEdge.edge.source)) {
                final Optional<ExclusiveEdge<V>> oAlternativeEdge =
                        seek(maxInEdge, bestArborescence, partialSolution.unseenIncomingEdges.queueByDestination.get(component));
                if (oAlternativeEdge.isPresent()) {
                    final ExclusiveEdge<V> alternativeEdge = oAlternativeEdge.get();
                    final double difference = maxInEdge.weight - alternativeEdge.weight;
                    if (difference < bestDifference) {
                        bestDifference = difference;
                        bestEdgeToKickOut = Optional.of(maxInEdge);
                    }
                }
            }
            // add the new edge to subgraph, merging SCCs if necessary
            final Optional<V> newComponent = partialSolution.addEdge(maxInEdge);
            if (newComponent.isPresent()) {
                // addEdge created a cycle, which means the new cycle doesn't have any incoming edges
                componentsWithNoInEdges.add(newComponent.get());
            }
        }
        if (bestEdgeToKickOut.isPresent()) {
            return Optional.of(Pair.of(bestEdgeToKickOut.get().edge, bestDifference));
        } else {
            return Optional.absent();
        }
    }

    /**
     * Determines whether `potentialAncestor` is an ancestor of `node` in `bestArborescence`
     */
    private static <V> boolean isAncestor(V node, V potentialAncestor, Arborescence<V> bestArborescence) {
        V currentNode = node;
        while (bestArborescence.parents.containsKey(currentNode)) {
            currentNode = bestArborescence.parents.get(currentNode);
            if (currentNode.equals(potentialAncestor)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds `nextBestEdge`, the next best alternative to `maxInEdge` for which the tail of
     * `maxInEdge` is not an ancestor of the source of `nextBestEdge` in `bestArborescence`
     */
    public static <V> Optional<ExclusiveEdge<V>> seek(ExclusiveEdge<V> maxInEdge,
                                                      Arborescence<V> bestArborescence,
                                                      EdgeQueueMap.EdgeQueue<V> edgeQueue) {
        Optional<ExclusiveEdge<V>> oNextBestEdge = edgeQueue.popBestEdge();
        while (oNextBestEdge.isPresent()) {
            final ExclusiveEdge<V> nextBestEdge = oNextBestEdge.get();
            if (!isAncestor(nextBestEdge.edge.source, maxInEdge.edge.destination, bestArborescence)) {
                edgeQueue.addEdge(nextBestEdge);
                return oNextBestEdge;
            } else {
                oNextBestEdge = edgeQueue.popBestEdge();
            }
        }
        return Optional.absent();
    }

    /**
     * Represents a subset of all possible arborescences: those that contain all of `required` and
     * none of `banned`.
     * `bestArborescence` is the best arborescence in this subset.
     * `weightOfNextBest` is the score of the second best.
     * `edgeToBan` is the edge you need to ban in order to get the second best (i.e. the 2nd best in this subset
     * will be the 1st best arborescence with all of `required`, and none of `banned` U {`edgeToBan`}.
     * The assumption is that `bestArborescence` has already been added
     * to the k-best list, and we're trying to decide whether the next best in this subset is k+1-best overall.
     */
    static class SubsetOfSolutions<V> {
        final Edge<V> edgeToBan;
        final Weighted<Arborescence<V>> bestArborescence;
        final Set<Edge<V>> required;
        final Set<Edge<V>> banned;

        public SubsetOfSolutions(Edge<V> edgeToBan,
                                 Weighted<Arborescence<V>> bestArborescence,
                                 Set<Edge<V>> required,
                                 Set<Edge<V>> banned) {
            this.edgeToBan = edgeToBan;
            this.bestArborescence = bestArborescence;
            this.required = required;
            this.banned = banned;
        }
    }
}
