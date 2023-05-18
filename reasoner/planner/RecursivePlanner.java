/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph.ConjunctionNode;
import com.vaticle.typedb.core.reasoner.planner.OrderingCoster.LocalAllCallsCosting;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class RecursivePlanner extends ReasonerPlanner {

    final AnswerCountEstimator answerCountEstimator;
    final ConjunctionGraph conjunctionGraph;
    final OrderingCoster orderingCoster;
    private final Map<CallMode, Set<LocalAllCallsCosting>> callModeCostings;

    protected RecursivePlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr, boolean explain) {
        super(traversalEng, conceptMgr, logicMgr, explain);
        this.conjunctionGraph = new ConjunctionGraph(logicMgr);
        this.answerCountEstimator = new AnswerCountEstimator(logicMgr, traversalEng.graph(), this.conjunctionGraph);
        this.callModeCostings = new HashMap<>();
        this.orderingCoster = new OrderingCoster(this, answerCountEstimator, conjunctionGraph);
    }

    public static RecursivePlanner create(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr, boolean explain) {
        return new RecursivePlanner(traversalEng, conceptMgr, logicMgr, explain);
    }

    @Override
    Plan computePlan(CallMode callMode) {
        recursivelyGenerateCostingsWithGuard(callMode);
        planMutuallyRecursiveSubgraph(callMode);
        assert planCache.getIfPresent(callMode) != null;
        return planCache.getIfPresent(callMode);
    }

    // Conjunctions which call each other must be planned together
    private void planMutuallyRecursiveSubgraph(CallMode callMode) {
        Map<CallMode, LocalAllCallsCosting> costings = new HashMap<>();
        Set<CallMode> pendingModes = new HashSet<>();
        pendingModes.add(callMode);
        SubgraphPlan bestPlan = subgraphPlanSearch(callMode, pendingModes, costings);

        for (LocalAllCallsCosting bestCostingForCall : bestPlan.costings.values()) {
            Plan plan = new Plan(bestCostingForCall.ordering, bestCostingForCall.callMode,
                    Math.round(Math.ceil(bestPlan.cost(bestCostingForCall.callMode, 1.0))),
                    bestPlan.cyclicScalingFactorSum.get(bestCostingForCall.callMode));
            planCache.put(bestCostingForCall.callMode, plan);
        }
    }

    private SubgraphPlan subgraphPlanSearch(CallMode root, Set<CallMode> pendingCallModes, Map<CallMode, LocalAllCallsCosting> callModeCostings) { // The value contains the scaling factors needed for the other conjunctions in the globalCost.
        // Pick a choice of ordering, expand dependencies, recurse ; backtrack over choices
        if (pendingCallModes.isEmpty()) { // All modes have an ordering chosen -> we have a complete candidate plan for the subgraph
            return SubgraphPlan.fromCostings(this, callModeCostings);
        }

        CallMode mode = iterate(pendingCallModes).next();
        pendingCallModes.remove(mode);
        assert !callModeCostings.containsKey(mode); // Should not have been added to pending

        ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(mode.conjunction);
        SubgraphPlan bestPlan = null; // for the branch of choices committed to so far.
        double bestPlanCost = Double.MAX_VALUE;
        for (LocalAllCallsCosting localAllCallsCosting : this.callModeCostings.get(mode)) {
            callModeCostings.put(mode, localAllCallsCosting);

            Set<CallMode> nextPendingModes = new HashSet<>(pendingCallModes);
            Set<CallMode> triggeredCalls = new HashSet<>();
            iterate(localAllCallsCosting.cyclicModes).forEachRemaining(concludableMode -> {
                triggeredCalls.addAll(triggeredCalls(concludableMode.first(), concludableMode.second(), conjunctionNode.cyclicDependencies(concludableMode.first())));
            });
            iterate(triggeredCalls).filter(call -> !callModeCostings.containsKey(call)).forEachRemaining(nextPendingModes::add);
            SubgraphPlan newPlan = subgraphPlanSearch(root, nextPendingModes, callModeCostings);

            double newPlanCost = newPlan.cost(root, 1.0/callModeCostings.get(root).answersToMode);
            if (bestPlan == null || newPlanCost < bestPlanCost) {
                bestPlan = newPlan;
                bestPlanCost = newPlanCost;
            }

            callModeCostings.remove(mode);
        }
        assert bestPlan != null;
        return bestPlan;
    }

    private void recursivelyGenerateCostingsWithGuard(CallMode callMode) {
        if (!callModeCostings.containsKey(callMode)) {
            callModeCostings.put(callMode, null); // Guard
            HybridAStarBeamSearch orderingSearch = createOrderingSearch(callMode);
            Set<LocalAllCallsCosting> costings = orderingSearch.search();
            callModeCostings.put(callMode, costings);
        }
    }

    void initialiseResolvableDependencies(ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableMode) {
        if (resolvable.isConcludable()) {
            Set<ResolvableConjunction> cyclicDependencies = conjunctionNode.cyclicDependencies(resolvable.asConcludable());
            for (CallMode callMode : triggeredCalls(resolvable.asConcludable(), resolvableMode, null)) {
                recursivelyGenerateCostingsWithGuard(callMode);
                if (!cyclicDependencies.contains(callMode.conjunction)) {
                    plan(callMode); // Acyclic dependencies can be fully planned
                }
            }
        } else if (resolvable.isNegated()) {
            iterate(resolvable.asNegated().disjunction().conjunctions()).forEachRemaining(conjunction -> {
                Set<Variable> branchVariables = Collections.intersection(estimateableVariables(conjunction.pattern().variables()), resolvableMode);
                CallMode callMode = new CallMode(conjunction, branchVariables);
                recursivelyGenerateCostingsWithGuard(callMode);
                plan(callMode);
            });
        }
    }

    HybridAStarBeamSearch createOrderingSearch(CallMode callMode) {
        return new HybridAStarBeamSearch(this, callMode);
    }

    private static class SubgraphPlan {
        private final Map<CallMode, LocalAllCallsCosting> costings;
        private final Map<CallMode, Double> cyclicScalingFactorSum;

        private SubgraphPlan(Map<CallMode, LocalAllCallsCosting> costings, Map<CallMode, Double> cyclicScalingFactorSum) {
            this.costings = new HashMap<>(costings);
            this.cyclicScalingFactorSum = new HashMap<>(cyclicScalingFactorSum);
            this.costings.keySet().forEach(callMode -> this.cyclicScalingFactorSum.putIfAbsent(callMode, 0.0));
        }

        private double cost(CallMode root, double rootScalingFactor) {
            double cycleCost = 0L;
            for (LocalAllCallsCosting localAllCallsCosting : costings.values()) {
                double scalingFactor = localAllCallsCosting.callMode.equals(root) ?
                        Math.min(1.0, rootScalingFactor + cyclicScalingFactorSum.get(localAllCallsCosting.callMode)) :
                        cyclicScalingFactorSum.get(localAllCallsCosting.callMode);
                cycleCost += localAllCallsCosting.allCallsConnectedAcyclicCost * scalingFactor + localAllCallsCosting.allCallsDisconnectedAcyclicCost;
            }

            return cycleCost;
        }

        private static SubgraphPlan fromCostings(RecursivePlanner planner, Map<CallMode, LocalAllCallsCosting> costings) {
            Map<CallMode, Double> scalingFactorSum = new HashMap<>();
            for (LocalAllCallsCosting localAllCallsCosting : costings.values()) {
                ConjunctionNode conjunctionNode = planner.conjunctionGraph.conjunctionNode(localAllCallsCosting.callMode.conjunction);
                localAllCallsCosting.cyclicModes.forEach(concludableMode -> {
                    iterate(planner.triggeredCalls(concludableMode.first(), concludableMode.second(), conjunctionNode.cyclicDependencies(concludableMode.first())))
                            .forEachRemaining(callMode -> {
                                scalingFactorSum.put(callMode, Math.min(1.0,
                                        scalingFactorSum.getOrDefault(callMode, 0.0) + localAllCallsCosting.cyclicScalingFactors.get(concludableMode.first())));
                            });
                });
            }
            return new SubgraphPlan(costings, scalingFactorSum);
        }
    }
}
