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

import com.google.common.collect.ImmutableMap;
import grakn.core.common.util.Partition;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.WeightedGraph;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;
import static grakn.core.kb.graql.planning.spanningtree.util.Weighted.weighted;


/**
 * Chu-Liu-Edmonds' algorithm for finding a maximum branching in a complete, directed graph in O(n^2) time.
 * Implementation is based on Tarjan's "Finding Optimum Branchings" paper:
 * http://cw.felk.cvut.cz/lib/exe/fetch.php/courses/a4m33pal/cviceni/tarjan-finding-optimum-branchings.pdf
 *
 */
public class ChuLiuEdmonds {
    /**
     * Represents the subgraph that gets iteratively built up in the CLE algorithm.
     */
    static class PartialSolution {
        // Partition representing the strongly connected components (SCCs).
        private final Partition<Node> stronglyConnected;
        // Partition representing the weakly connected components (WCCs).
        private final Partition<Node> weaklyConnected;
        // An invariant of the CLE algorithm is that each SCC always has at most one incoming edge.
        // You can think of these edges as implicitly defining a graph with SCCs as nodes.
        private final Map<Node, Weighted<DirectedEdge>> incomingEdgeByScc;
        // History of edges we've added, and for each, a list of edges it would exclude.
        // More recently added edges get priority over less recently added edges when reconstructing the final tree.
        private final Deque<ExclusiveEdge> edgesAndWhatTheyExclude;
        // a priority queue of incoming edges for each SCC that we haven't chosen an incoming edge for yet.
        final EdgeQueueMap unseenIncomingEdges;
        // running sum of weights.
        // edge weights are adjusted as we go to take into account the fact that we have an extra edge in each cycle
        private double score;

        private PartialSolution(Partition<Node> stronglyConnected,
                                Partition<Node> weaklyConnected,
                                Map<Node, Weighted<DirectedEdge>> incomingEdgeByScc,
                                Deque<ExclusiveEdge> edgesAndWhatTheyExclude,
                                EdgeQueueMap unseenIncomingEdges,
                                double score) {
            this.stronglyConnected = stronglyConnected;
            this.weaklyConnected = weaklyConnected;
            this.incomingEdgeByScc = incomingEdgeByScc;
            this.edgesAndWhatTheyExclude = edgesAndWhatTheyExclude;
            this.unseenIncomingEdges = unseenIncomingEdges;
            this.score = score;
        }

        public static PartialSolution initialize(WeightedGraph graph) {
            final Partition<Node> stronglyConnected = Partition.singletons(graph.getNodes());
            final HashMap<Node, Weighted<DirectedEdge>> incomingByScc = new HashMap<>();
            final Deque<ExclusiveEdge> exclusiveEdges = new ArrayDeque<>();
            // group edges by their destination component
            final EdgeQueueMap incomingEdges = new EdgeQueueMap(stronglyConnected);
            for (Node destinationNode : graph.getNodes()) {
                for (Weighted<DirectedEdge> inEdge : graph.getIncomingEdges(destinationNode)) {
                    if (inEdge.weight != Double.NEGATIVE_INFINITY) {
                        incomingEdges.addEdge(inEdge);
                    }
                }
            }
            return new PartialSolution(
                    stronglyConnected,
                    Partition.singletons(graph.getNodes()),
                    incomingByScc,
                    exclusiveEdges,
                    incomingEdges,
                    0.0
            );
        }

        public Set<Node> getNodes() {
            return stronglyConnected.getNodes();
        }

        /**
         * Given an edge that completes a cycle, merge all SCCs on that cycle into one SCC.
         * Returns the new component.
         */
        private Node merge(Weighted<DirectedEdge> newEdge, EdgeQueueMap unseenIncomingEdges) {
            // Find edges connecting SCCs on the path from newEdge.destination to newEdge.source
            final List<Weighted<DirectedEdge>> cycle = getCycle(newEdge);
            // build up list of queues that need to be merged, with the edge they would exclude
            final List<EdgeQueueMap.QueueAndReplace> queuesToMerge = new ArrayList<>(cycle.size());
            for (Weighted<DirectedEdge> currentEdge : cycle) {
                final Node destination = stronglyConnected.componentOf(currentEdge.val.destination);
                final EdgeQueueMap.EdgeQueue queue = unseenIncomingEdges.queueByDestination.get(destination);
                // if we choose an edge in `queue`, we'll have to throw out `currentEdge` at the end
                // (each SCC can have only one incoming edge).
                queuesToMerge.add(EdgeQueueMap.QueueAndReplace.of(queue, currentEdge));
                unseenIncomingEdges.queueByDestination.remove(destination);
            }
            // Merge all SCCs on the cycle into one
            for (Weighted<DirectedEdge> e : cycle) {
                stronglyConnected.merge(e.val.source, e.val.destination);
            }
            Node component = stronglyConnected.componentOf(newEdge.val.destination);
            // merge the queues and put the merged queue back into our map under the new component
            unseenIncomingEdges.merge(component, queuesToMerge);
            // keep our implicit graph of SCCs up to date:
            // we just created a cycle, so all in-edges have sources inside the new component
            // i.e. there is no edge with source outside component, and destination inside component
            incomingEdgeByScc.remove(component);
            return component;
        }

        /**
         * Gets the cycle of edges between SCCs that newEdge creates
         */
        private List<Weighted<DirectedEdge>> getCycle(Weighted<DirectedEdge> newEdge) {
            final List<Weighted<DirectedEdge>> cycle = new ArrayList<>();
            // circle around backward until you get back to where you started
            Weighted<DirectedEdge> edge = newEdge;
            cycle.add(edge);
            while (!stronglyConnected.sameComponent(edge.val.source, newEdge.val.destination)) {
                edge = incomingEdgeByScc.get(stronglyConnected.componentOf(edge.val.source));
                cycle.add(edge);
            }
            return cycle;
        }

        /**
         * Adds the given edge to this subgraph, merging SCCs if necessary
         *
         * @return the new SCC if adding edge created a cycle
         */
        public Optional<Node> addEdge(ExclusiveEdge wEdgeAndExcludes) {
            final DirectedEdge edge = wEdgeAndExcludes.edge;
            final double weight = wEdgeAndExcludes.weight;
            final Weighted<DirectedEdge> wEdge = weighted(edge, weight);
            score += weight;
            final Node destinationScc = stronglyConnected.componentOf(edge.destination);
            edgesAndWhatTheyExclude.addFirst(wEdgeAndExcludes);
            incomingEdgeByScc.put(destinationScc, wEdge);
            if (!weaklyConnected.sameComponent(edge.source, edge.destination)) {
                // Edge connects two different WCCs. Including it won't create a new cycle
                weaklyConnected.merge(edge.source, edge.destination);
                return Optional.empty();
            } else {
                // Edge is contained within one WCC. Including it will create a new cycle.
                return Optional.of(merge(wEdge, unseenIncomingEdges));
            }
        }

        /**
         * Recovers the optimal arborescence.
         * <p>
         * Each SCC can only have 1 edge entering it: the edge that we added most recently.
         * So we work backwards, adding edges unless they conflict with edges we've already added.
         * Runtime is O(n^2) in the worst case.
         */
        private Weighted<Arborescence<Node>> recoverBestArborescence() {
            final ImmutableMap.Builder<Node, Node> parents = ImmutableMap.builder();
            final Set<DirectedEdge> excluded = new HashSet<>();
            // start with the most recent
            while (!edgesAndWhatTheyExclude.isEmpty()) {
                final ExclusiveEdge edgeAndWhatItExcludes = edgesAndWhatTheyExclude.pollFirst();
                final DirectedEdge edge = edgeAndWhatItExcludes.edge;
                if (!excluded.contains(edge)) {
                    excluded.addAll(edgeAndWhatItExcludes.excluded);
                    parents.put(edge.destination, edge.source);
                }
            }
            return weighted(Arborescence.of(parents.build()), score);
        }

        public Optional<ExclusiveEdge> popBestEdge(Node component) {
            return popBestEdge(component, Arborescence.empty());
        }

        /**
         * Always breaks ties in favor of edges in `best`
         */
        public Optional<ExclusiveEdge> popBestEdge(Node component, Arborescence<Node> best) {
            return unseenIncomingEdges.popBestEdge(component, best);
        }
    }

    /**
     * Find an optimal arborescence of the given graph `graph`, rooted in the given node `root`.
     */
    public static Weighted<Arborescence<Node>> getMaxArborescence(WeightedGraph graph, Node root) {
        // remove all edges incoming to `root`. resulting arborescence is then forced to be rooted at `root`.
        return getMaxArborescence(graph.filterEdges(not(DirectedEdge.hasDestination(root))));
    }

    public static Weighted<Arborescence<Node>> getMaxArborescence(WeightedGraph graph,
                                                            Set<DirectedEdge> required,
                                                            Set<DirectedEdge> banned) {
        return getMaxArborescence(graph.filterEdges(and(not(DirectedEdge.competesWith(required)), not(DirectedEdge.isIn(banned)))));
    }

    /**
     * Find an optimal arborescence of the given graph.
     */
    public static Weighted<Arborescence<Node>> getMaxArborescence(WeightedGraph graph) {
        final PartialSolution partialSolution =
                PartialSolution.initialize(graph.filterEdges(not(DirectedEdge.isAutoCycle())));
        // In the beginning, subgraph has no edges, so no SCC has in-edges.
        final Deque<Node> componentsWithNoInEdges = new ArrayDeque<>(partialSolution.getNodes());

        // Work our way through all componentsWithNoInEdges, in no particular order
        while (!componentsWithNoInEdges.isEmpty()) {
            final Node component = componentsWithNoInEdges.poll();
            // find maximum edge entering 'component' from outside 'component'.
            final Optional<ExclusiveEdge> oMaxInEdge = partialSolution.popBestEdge(component);
            if (!oMaxInEdge.isPresent()) continue; // No in-edges left to consider for this component. Done with it!
            final ExclusiveEdge maxInEdge = oMaxInEdge.get();
            // add the new edge to subgraph, merging SCCs if necessary
            final Optional<Node> newComponent = partialSolution.addEdge(maxInEdge);
            if (newComponent.isPresent()) {
                // addEdge created a cycle/component, which means the new component doesn't have any incoming edges
                componentsWithNoInEdges.add(newComponent.get());
            }
        }
        // Once no component has incoming edges left to consider, it's time to recover the optimal branching.
        return partialSolution.recoverBestArborescence();
    }
}
