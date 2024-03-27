/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class HybridAStarBeamSearch {

    // Changing the search algorithm by changing the parameters:
    //      heuristic() returns 0           : UCS search
    //      SWITCHOVER_FACTOR = 0           : Pure beam search
    //          MAX_BEAM_SIZE = 1           : Greedy search
    //      SWITCHOVER_FACTOR = infinity    : Pure A* (or UCS if heuristic is 0)

    private static final int MAX_BEAM_SIZE = 100; // Consider making this a function of the number of resolvables.
    private static final float SWITCHOVER_FACTOR = 10;

    private final RecursivePlanner planner;
    private final ReasonerPlanner.CallMode callMode;

    private final ConjunctionGraph.ConjunctionNode conjunctionNode;
    private final Set<Resolvable<?>> resolvables;
    private final Set<Concludable> cyclicConcludables;
    private final Map<Resolvable<?>, Set<Variable>> dependencies;

    private final Map<Set<Pair<Concludable, Set<Variable>>>, FringeElement> completedSignatures;
    private final Set<Resolvable<?>> valueIdentityResolvables;
    private final Map<Resolvable<?>, Double> resolvableCostForHeuristic;

    public HybridAStarBeamSearch(RecursivePlanner planner, ReasonerPlanner.CallMode callMode) {
        this.planner = planner;
        this.callMode = callMode;
        this.conjunctionNode = planner.conjunctionGraph.conjunctionNode(callMode.conjunction);
        this.resolvables = planner.logicMgr.compile(callMode.conjunction);
        this.cyclicConcludables = conjunctionNode.cyclicConcludables();
        this.dependencies = ReasonerPlanner.dependencies(resolvables);
        this.valueIdentityResolvables = iterate(this.resolvables).filter(resolvable -> !resolvable.isNegated()).filter(r ->
                iterate(ReasonerPlanner.estimateableVariables(r.variables())).flatMap(v -> iterate(v.constraints()))
                        .anyMatch(constraint ->
                                constraint.isThing() && constraint.asThing().isPredicate() && constraint.asThing().asPredicate().predicate().isValueIdentity())).toSet();

        this.resolvableCostForHeuristic = planner.orderingCoster.resolvableHeuristics(callMode, conjunctionNode, resolvables);
        this.completedSignatures = new HashMap<>();
    }

    public Set<OrderingCoster.LocalAllCallsCosting> search() {
        PriorityQueue<FringeElement> fringe = new PriorityQueue<>();
        fringe.add(initialFringeElement());
        fringe = aStarSearch(fringe, new HashSet<>());
        fringe = beamSearch(fringe, new HashSet<>()); // Start with a fresh seen set.
        assert fringe.isEmpty(); // For now, we terminate on an empty fringe.
        return iterate(completedSignatures.values())
                .map(fringeElement -> fringeElement.costingBuilder.build())
                .map(singleCallCosting -> planner.orderingCoster.createAllCallsCosting(singleCallCosting.callMode, singleCallCosting.ordering, singleCallCosting.cyclicConcludableModes))
                .toSet();
    }

    private PriorityQueue<FringeElement> aStarSearch(PriorityQueue<FringeElement> fringe, Set<SearchNode> seenNodes) {
        while (!fringe.isEmpty()) {
            FringeElement top = fringe.poll();
            if (isCompletePlan(top.node)) {
                if (!completedSignatures.containsKey(top.node.cyclicConcludableModes)) {
                    completedSignatures.put(top.node.cyclicConcludableModes, top);
                } // else, do nothing.
            } else if (checkRedundant(seenNodes, top.node)) continue;
            seenNodes.add(top.node);

            List<SearchNode> successors = successors(top).toList();
            Set<Variable> currentBoundVars = top.bounds();
            List<SearchNode> connectedSuccessors = iterate(successors)
                    .filter(node ->
                            valueIdentityResolvables.contains(node.lastResolvable()) ||
                            iterate(ReasonerPlanner.estimateableVariables(node.lastResolvable().variables()))
                                    .anyMatch(currentBoundVars::contains))
                    .toList();

            if (!connectedSuccessors.isEmpty()) successors = connectedSuccessors;

            for (SearchNode succ : successors) {
                if (!checkRedundant(seenNodes, succ)) {
                    fringe.add(extendFringeTowards(top, succ));
                }
            }
            if (fringe.size() >= MAX_BEAM_SIZE * SWITCHOVER_FACTOR) break;
        }
        return fringe;
    }

    private PriorityQueue<FringeElement> beamSearch(PriorityQueue<FringeElement> fringe, Set<SearchNode> seenNodes) {
        // We do beam-search lazily - We add everything to the beam, but only consider the top <beam_size> elements
        while (!fringe.isEmpty()) {
            PriorityQueue<FringeElement> nextFringe = new PriorityQueue<>();
            int maxToPoll = MAX_BEAM_SIZE;
            while (maxToPoll > 0 && !fringe.isEmpty()) {
                maxToPoll -= 1;
                FringeElement top = fringe.poll();
                if (isCompletePlan(top.node)) {
                    if (!completedSignatures.containsKey(top.node.cyclicConcludableModes)) {
                        completedSignatures.put(top.node.cyclicConcludableModes, top);
                    } // else, do nothing.
                } else if (checkRedundant(seenNodes, top.node)) continue;
                seenNodes.add(top.node);

                List<SearchNode> successors = successors(top).toList();
                Set<Variable> currentBoundVars = top.bounds();
                List<SearchNode> connectedSuccessors = iterate(successors)
                        .filter(node -> valueIdentityResolvables.contains(node.lastResolvable()) ||
                                iterate(ReasonerPlanner.estimateableVariables(node.lastResolvable().variables())).anyMatch(currentBoundVars::contains))
                        .toList();

                if (!connectedSuccessors.isEmpty()) successors = connectedSuccessors;

                for (SearchNode succ : successors) {
                    if (!checkRedundant(seenNodes, succ)) {
                        nextFringe.add(extendFringeTowards(top, succ));
                    }
                }
            }
            fringe = nextFringe;
        }
        return fringe;
    }

    private FringeElement extendFringeTowards(FringeElement prevElement, SearchNode succNode) {
        OrderingCoster.SingleCallCostingBuilder nextEstimator = prevElement.costingBuilder().clone();
        Resolvable<?> lastResolvable = succNode.lastResolvable();
        planner.initialiseResolvableDependencies(conjunctionNode, lastResolvable, Collections.intersection(prevElement.bounds(), ReasonerPlanner.estimateableVariables(lastResolvable.variables())));
        nextEstimator.extend(succNode.lastResolvable());
        assert nextEstimator.currentOrdering().equals(succNode.resolvableOrder);
        assert nextEstimator.currentCyclicConcludableModes().equals(succNode.cyclicConcludableModes);
        return new FringeElement(succNode, nextEstimator);
    }

    private FringeElement initialFringeElement() {
        return new FringeElement(
                new SearchNode(new ArrayList<>(), new HashSet<>()),
                planner.orderingCoster.createSingleCallCostingBuilder(callMode));
    }

    private boolean isCompletePlan(SearchNode node) {
        return node.resolvables.size() == resolvables.size();
    }

    private FunctionalIterator<SearchNode> successors(FringeElement fringeElement) {
        FunctionalIterator<Resolvable<?>> next = iterate(resolvables)
                .filter(res -> !fringeElement.node.resolvables.contains(res))
                .filter(res -> ReasonerPlanner.dependenciesSatisfied(res, fringeElement.bounds(), dependencies));

        return next.map(res -> fringeElement.node.extend(res, res.isConcludable() && cyclicConcludables.contains(res.asConcludable()), fringeElement.bounds()));
    }

    private boolean checkRedundant(Set<SearchNode> seenNodes, SearchNode searchNode) {
        return seenNodes.contains(searchNode) || completedSignatures.containsKey(searchNode.cyclicConcludableModes);
    }

    private class FringeElement implements Comparable<FringeElement> {
        private final SearchNode node;
        private final OrderingCoster.SingleCallCostingBuilder costingBuilder;
        private final double cost;

        private FringeElement(SearchNode node, OrderingCoster.SingleCallCostingBuilder costingBuilder) {
            this.node = node;
            this.costingBuilder = costingBuilder;
            this.cost = costingBuilder.currentCost() + heuristic();
        }

        private double heuristic() {
            // TODO: Can subtract from total
            return iterate(resolvables).filter(res -> !node.resolvables.contains(res))
                    .map(resolvableCostForHeuristic::get).reduce(0.0, Double::sum);
        }

        private Set<Variable> bounds() {
            return costingBuilder.currentBounds();
        }

        private double cost() {
            return cost;
        }

        private OrderingCoster.SingleCallCostingBuilder costingBuilder() {
            return costingBuilder;
        }

        @Override
        public int compareTo(FringeElement other) {
            return Double.compare(cost(), other.cost());
        }

    }

    private static class SearchNode {
        private final Set<Resolvable<?>> resolvables;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        private final int hash;

        private final List<Resolvable<?>> resolvableOrder; // unhashed and not considered in equals

        private SearchNode(List<Resolvable<?>> resolvableOrder, Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes) {
            this.resolvableOrder = resolvableOrder;
            this.cyclicConcludableModes = cyclicConcludableModes;
            this.resolvables = new HashSet<>(resolvableOrder);
            this.hash = Objects.hash(resolvables, cyclicConcludableModes);
        }

        public Resolvable<?> lastResolvable() {
            return resolvableOrder.get(resolvableOrder.size() - 1);
        }

        public SearchNode extend(Resolvable<?> res, boolean isCyclicConcludable, Set<Variable> presentBounds) {
            List<Resolvable<?>> nextResolvables = new ArrayList<>(this.resolvableOrder);
            nextResolvables.add(res);
            Set<Pair<Concludable, Set<Variable>>> nextCyclicConcludables;
            if (isCyclicConcludable) {
                nextCyclicConcludables = new HashSet<>(this.cyclicConcludableModes);
                nextCyclicConcludables.add(new Pair<>(res.asConcludable(), Collections.intersection(presentBounds, ReasonerPlanner.estimateableVariables(res.variables()))));
            } else {
                nextCyclicConcludables = this.cyclicConcludableModes;
            }
            return new SearchNode(nextResolvables, nextCyclicConcludables);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SearchNode that = (SearchNode) o;
            return resolvables.equals(that.resolvables) && cyclicConcludableModes.equals(that.cyclicConcludableModes);
        }
    }
}
