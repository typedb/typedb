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
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph.ConjunctionNode;
import com.vaticle.typedb.core.reasoner.planner.OrderingCoster.LocalAllCallsCosting;
import com.vaticle.typedb.core.reasoner.planner.OrderingCoster.LocalSingleCallCosting;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class RecursivePlanner extends ReasonerPlanner {
    // Inaccuracies:
    //      retrieval costs are treated the same as reasoning-overhead cost (Both approximated as the number of answers retrieved for all variables)
    //      Excess calls are not penalised, since the scaling factor is capped at one (Solve using (calls + scaling-factor * rec-cost?) )
    //      Acyclic dependencies are counted once for each occurence
    //          (Can be fixed by including the optimal plans for the bound in the globalPlan chosenSummaries - The scaling factors will be capped at 1 then)
    // So far, these are acceptable because the difference in cost is some constant factor, and our aim is to avoid horrible plans.
    //
    //      !!! The cost does not depend on the binding mode !!! because of the formulation - Handle this with connectedness restriction when generating orders?

    private final AnswerCountEstimator answerCountEstimator;
    private final Map<CallMode, Set<LocalAllCallsCosting>> callModeCostings;
    private final ConjunctionGraph conjunctionGraph;
    private final OrderingCoster orderingCoster;

    public RecursivePlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
        this.conjunctionGraph = new ConjunctionGraph(logicMgr);
        this.answerCountEstimator = new AnswerCountEstimator(logicMgr, traversalEng.graph(), this.conjunctionGraph);
        this.callModeCostings = new HashMap<>();
        this.orderingCoster = new OrderingCoster(this, answerCountEstimator, conjunctionGraph);
    }

    @Override
    Plan computePlan(CallMode callMode) {
        recursivelyGenerateCostings(callMode);
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

    private void recursivelyGenerateCostings(CallMode callMode) {
        if (!callModeCostings.containsKey(callMode)) {
            callModeCostings.put(callMode, null); // Guard
            ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(callMode.conjunction);
            answerCountEstimator.buildConjunctionModel(callMode.conjunction);

            Map<Set<Pair<Concludable, Set<Variable>>>, LocalSingleCallCosting> bestCostings = new HashMap<>();
            PartialOrderReductionSearch porSearch = new PartialOrderReductionSearch(logicMgr.compile(callMode.conjunction), callMode.mode);
            for (List<Resolvable<?>> ordering : porSearch.allOrderings()) {
                initialiseOrderingDependencies(conjunctionNode, ordering, callMode.mode);
                LocalSingleCallCosting localSingleCallCosting = orderingCoster.createSingleCallCosting(callMode, ordering);

                // Two orderings for the same CallMode with identical cyclic-concludable modes are interchangeable in the subgraph-plan
                //      -> We only need to keep the cheaper one.
                if (!bestCostings.containsKey(localSingleCallCosting.cyclicConcludableModes) ) {
                    bestCostings.put(localSingleCallCosting.cyclicConcludableModes, localSingleCallCosting);
                } else {
                    LocalSingleCallCosting existingSummary = bestCostings.get(localSingleCallCosting.cyclicConcludableModes);
                    if (localSingleCallCosting.singleCallAcyclicCost < existingSummary.singleCallAcyclicCost) {
                        bestCostings.put(localSingleCallCosting.cyclicConcludableModes, localSingleCallCosting);
                    }
                }
            }
            this.callModeCostings.put(callMode, iterate(bestCostings.values()).map(singleCall -> orderingCoster.createAllCallsCosting(singleCall.callMode, singleCall.ordering, singleCall.cyclicConcludableModes)).toSet());
        }
    }

    private void initialiseOrderingDependencies(ConjunctionNode conjunctionNode, List<Resolvable<?>> ordering, Set<Variable> mode) {
        Set<Variable> currentBoundVars = new HashSet<>(mode);
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableMode = Collections.intersection(estimateableVariables(resolvable.variables()), currentBoundVars);
            initialiseResolvableDependencies(conjunctionNode, resolvable, resolvableMode);
            if (!resolvable.isNegated()) currentBoundVars.addAll(estimateableVariables(resolvable.variables()));
        }
    }

    private void initialiseResolvableDependencies(ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableMode) {
        if (resolvable.isConcludable()) {
            Set<ResolvableConjunction> cyclicDependencies = conjunctionNode.cyclicDependencies(resolvable.asConcludable());
            for (CallMode callMode : triggeredCalls(resolvable.asConcludable(), resolvableMode, null)) {
                recursivelyGenerateCostings(callMode);
                if (!cyclicDependencies.contains(callMode.conjunction)) {
                    plan(callMode); // Acyclic dependencies can be fully planned
                }
            }
        } else if (resolvable.isNegated()) {
            iterate(resolvable.asNegated().disjunction().conjunctions()).forEachRemaining(conjunction -> {
                Set<Variable> branchVariables = Collections.intersection(estimateableVariables(conjunction.pattern().variables()), resolvableMode);
                CallMode callMode = new CallMode(conjunction, branchVariables);
                recursivelyGenerateCostings(callMode);
                plan(callMode);
            });
        }
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

    static class PartialOrderReductionSearch {
        private final Set<Resolvable<?>> resolvables;
        private final Set<Variable> mode;
        private final Map<Resolvable<?>, Set<Variable>> dependencies;

        // TODO: Combine these when we fix ReasonerPlanner.dependencies
        PartialOrderReductionSearch(Set<Resolvable<?>> resolvables, Set<Variable> mode) {
            this(resolvables, mode, ReasonerPlanner.dependencies(resolvables));
        }

        PartialOrderReductionSearch(Set<Resolvable<?>> resolvables, Set<Variable> mode, Map<Resolvable<?>, Set<Variable>> dependencies) {
            this.resolvables = resolvables;
            this.mode = mode;
            this.dependencies = dependencies;
        }

        List<List<Resolvable<?>>> allOrderings() {
            Set<Resolvable<?>> remaining = new HashSet<>(resolvables);
            Set<Variable> restrictedVars = new HashSet<>(mode);
            iterate(resolvables).filter(resolvable -> !resolvable.isNegated()).flatMap(r -> iterate(estimateableVariables(r.variables())))
                    .filter(v -> iterate(v.constraints()).anyMatch(constraint ->
                            constraint.isThing() && constraint.asThing().isValue() && constraint.asThing().asValue().isValueIdentity()))
                    .forEachRemaining(restrictedVars::add);

            List<List<Resolvable<?>>> orderings = new ArrayList<>();
            porDFS(new Stack<>(), restrictedVars, remaining, new HashSet<>(), orderings);
            assert !orderings.isEmpty();
            return orderings; // TODO: If this causes a memory blow-up, POR can "generate" one at a time.
        }

        private void porDFS(Stack<Resolvable<?>> currentPath, Set<Variable> currentBoundVars,
                            Set<Resolvable<?>> remaining, Set<Resolvable<?>> sleepSet, List<List<Resolvable<?>>> orderings) {
            if (remaining.isEmpty()) {
                orderings.add(new ArrayList<>(currentPath));
                return;
            }

            List<Resolvable<?>> enabled = iterate(remaining)
                    .filter(r -> ReasonerPlanner.dependenciesSatisfied(r, currentBoundVars, dependencies) && !sleepSet.contains(r))
                    .toList();

            if (enabled.isEmpty()) {
                return; // All enabled are sleeping
            }

            // Restrict further to only the connected ones
            List<Resolvable<?>> connectedEnabled = iterate(enabled)
                    .filter(r -> iterate(estimateableVariables(r.variables())).anyMatch(currentBoundVars::contains)).toList();

            if (!connectedEnabled.isEmpty()) {
                enabled = connectedEnabled;
            } else if (connectedEnabled.isEmpty() && !sleepSet.isEmpty()) {
                return; // We should have tried the disconnected down a path that's now sleeping.
            } // else enabled = enabled; and carry on

            Set<Resolvable<?>> newSleepSet = new HashSet<>(sleepSet);
            for (Resolvable<?> next : enabled) {
                Set<Resolvable<?>> awaken = iterate(newSleepSet)
                        .filter(r -> iterate(r.variables()).anyMatch(next.variables()::contains)) // not necessarily newly bound
                        .toSet();

                Set<Resolvable<?>> nextSleepSet = iterate(newSleepSet).filter(r -> !awaken.contains(r)).toSet();
                Set<Variable> newlyBoundVars = next.isNegated() ?
                        new HashSet<>() :
                        iterate(next.variables()).filter(v -> !currentBoundVars.contains(v)).toSet();

                currentPath.add(next);
                remaining.remove(next);
                currentBoundVars.addAll(newlyBoundVars);
                porDFS(currentPath, currentBoundVars, remaining, nextSleepSet, orderings);
                currentBoundVars.removeAll(newlyBoundVars);
                remaining.add(next);
                currentPath.remove(currentPath.size() - 1);

                newSleepSet.add(next);
            }
        }
    }
}
