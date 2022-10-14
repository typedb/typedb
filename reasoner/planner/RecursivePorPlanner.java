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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionSummarizer.ConjunctionSummary;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class RecursivePorPlanner extends ReasonerPlanner {
    // Inaccuracies:
    //      retrieval costs are treated the same as reasoning-overhead cost (Both approximated as the number of answers retrieved for all variables)
    //      Excess calls are not penalized, since the scaling factor is capped at one (Solve using (calls + scaling-factor * rec-cost?) )
    //      Acyclic dependencies are counted once for each occurence
    //          (Can be fixed by including the optimal plans for the bound in the globalPlan chosenSummaries - The scaling factors will be capped at 1 then)
    // So far, these are acceptable because the difference in cost is some constant factor, and our aim is to avoid horrible plans.
    //
    //      !!! The cost does not depend on the binding mode !!! because of the formulation - Handle this with connectedness restriction when generating orders?

    final AnswerCountEstimator answerCountEstimator;
    private final Map<CallKey, Set<OrderingSummary>> candidateOrderings;
    private final ConjunctionSummarizer conjunctionSummarizer;

    public RecursivePorPlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
        this.conjunctionSummarizer = new ConjunctionSummarizer(logicMgr);
        this.answerCountEstimator = new AnswerCountEstimator(logicMgr, traversalEng.graph(), this.conjunctionSummarizer);
        this.candidateOrderings = new HashMap<>();
    }

    private static Set<Variable> consideredVariables(Resolvable<?> resolvable) {
        return iterate(resolvable.variables()).filter(Variable::isThing).toSet();
    }

    @Override
    Plan computePlan(CallKey callKey) {
        recursivelySummarizeOrderings(callKey);
        computeOrderingsForCycles(callKey);
        assert planCache.getIfPresent(callKey) != null;
        return planCache.getIfPresent(callKey);
    }

    // plan-search: combined plan-search for all nodes in a cycle
    private void computeOrderingsForCycles(CallKey callKey) {
        // This should be called only from conjunctions not on cycle-paths or at the lasso-knot FOR THE CURRENT LASSO ROPE
        Map<CallKey, OrderingSummary> chosenSummaries = new HashMap<>();
        Set<CallKey> pendingKeys = new HashSet<>();
        pendingKeys.add(callKey);
        Pair<List<OrderingSummary>, Double> bestPlan = cyclePlanSearch(callKey, pendingKeys, chosenSummaries);

        long bestGlobalCost = Math.round(Math.ceil(bestPlan.second()));
        for (OrderingSummary bestPlanForCall : bestPlan.first()) {
            // Caching all should be fine - scaling factors aren't propagated so we have the best plan for a scaling-factor 1.
            // So a call from a different location would receive the cost it would expect?
            planCache.put(bestPlanForCall.callKey, new Plan(bestPlanForCall.ordering, bestGlobalCost));
        }
    }

    private Pair<List<OrderingSummary>, Double> cyclePlanSearch(CallKey root, Set<CallKey> pendingKeys, Map<CallKey, OrderingSummary> chosenSummaries) { // The value contains the scaling factors needed for the other conjunctions in the globalCost.
        // Walk the cycle, backtrack over choices of orders.
        if (pendingKeys.isEmpty()) {
            return new Pair<>(new ArrayList<>(chosenSummaries.values()), cyclePlanCost(root, chosenSummaries));
        }

        CallKey key = pendingKeys.stream().findAny().get();
        pendingKeys.remove(key);
        assert !chosenSummaries.containsKey(key); // Should not have been added to pending

        ConjunctionSummary conjunctionSummary = conjunctionSummarizer.conjunctionSummary(key.conjunction);
        Pair<List<OrderingSummary>, Double> bestPlan = null; // for this branch
        for (OrderingSummary orderingSummary : candidateOrderings.get(key)) {
            chosenSummaries.put(key, orderingSummary);

            Set<CallKey> nextPendingKeys = new HashSet<>(pendingKeys);
            Set<CallKey> triggeredCalls = new HashSet<>();
            iterate(orderingSummary.cyclicConcludableBounds).forEachRemaining(concludableBounds -> {
                triggeredCalls.addAll(triggeredCalls(concludableBounds.first(), concludableBounds.second(), Optional.of(conjunctionSummary.cyclicDependencies(concludableBounds.first()))));
            });
            iterate(triggeredCalls).filter(call -> !chosenSummaries.containsKey(call)).forEachRemaining(nextPendingKeys::add);
            Pair<List<OrderingSummary>, Double> newPlan = cyclePlanSearch(root, nextPendingKeys, chosenSummaries);
            bestPlan = (bestPlan == null || newPlan.second() < bestPlan.second()) ? newPlan : bestPlan;

            chosenSummaries.remove(key);
        }
        assert bestPlan != null;
        return bestPlan;
    }

    // ordering-generation: recursively generate candidate-orderings for each (conjunction,mode)
    private void initializeOrderingDependencies(ConjunctionSummary conjunctionSummary, List<Resolvable<?>> ordering, Set<Variable> inputBounds) {
        Set<Variable> currentBounds = new HashSet<>(inputBounds);
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableBounds = iterate(consideredVariables(resolvable)).filter(currentBounds::contains).toSet();
            initializeDependencies(conjunctionSummary, resolvable, resolvableBounds);
            currentBounds.addAll(consideredVariables(resolvable));
        }
    }

    private void initializeDependencies(ConjunctionSummary conjunctionSummary, Resolvable<?> resolvable, Set<Variable> resolvableBounds) {
        if (resolvable.isConcludable()) {
            Set<ResolvableConjunction> cyclicDependencies = conjunctionSummary.cyclicDependencies(resolvable.asConcludable());
            for (CallKey callKey : triggeredCalls(resolvable.asConcludable(), resolvableBounds, Optional.empty())) {
                recursivelySummarizeOrderings(callKey);
                if (!cyclicDependencies.contains(callKey.conjunction)) {
                    plan(callKey); // Acyclic dependencies can be fully planned
                }
            }
        } else if (resolvable.isNegated()) {
            iterate(resolvable.asNegated().disjunction().conjunctions()).forEachRemaining(conjunction -> {
                CallKey callKey = new CallKey(conjunction, resolvableBounds);
                recursivelySummarizeOrderings(callKey);
                plan(callKey);
            });
        }
    }

    private void recursivelySummarizeOrderings(CallKey callKey) {
        if (!candidateOrderings.containsKey(callKey)) {
            candidateOrderings.put(callKey, null); // Guard
            ConjunctionSummary conjunctionSummary = conjunctionSummarizer.conjunctionSummary(callKey.conjunction);
            answerCountEstimator.buildConjunctionModel(callKey.conjunction);

            Map<Set<Pair<Concludable, Set<Variable>>>, OrderingSummary> bestOrderingSummary = new HashMap<>();
            PartialOrderReductionSearch porSearch = new PartialOrderReductionSearch(logicMgr.compile(callKey.conjunction), callKey.bounds);
            for (List<Resolvable<?>> ordering : porSearch.allOrderings()) {
                initializeOrderingDependencies(conjunctionSummary, ordering, callKey.bounds);
                OrderingSummary orderingSummary = summarizeOrdering(conjunctionSummary, ordering, callKey.bounds);
                if (!bestOrderingSummary.containsKey(orderingSummary.cyclicConcludableBounds) ||
                        bestOrderingSummary.get(orderingSummary.cyclicConcludableBounds).acyclicCost > orderingSummary.acyclicCost) {
                    bestOrderingSummary.put(orderingSummary.cyclicConcludableBounds, orderingSummary);
                }
            }
            candidateOrderings.put(callKey, new HashSet<>(bestOrderingSummary.values()));
        }
    }

    private OrderingSummary summarizeOrdering(ConjunctionSummary conjunctionSummary, List<Resolvable<?>> ordering, Set<Variable> inputBounds) {
        Set<Variable> runningBounds = new HashSet<>(inputBounds);
        Set<Variable> runningRestrictedBounds = new HashSet<>(); // Restricted by preceding resolvables
        double acyclicCost = 0L;

        Map<Concludable, Double> scalingFactors = new HashMap<>();
        Set<Pair<Concludable, Set<Variable>>> cyclicConcludableBounds = new HashSet<>();
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunctionSummary.conjunction());
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableBounds = Collections.intersection(consideredVariables(resolvable), runningBounds);
            Set<Variable> restrictedResolvableBounds = Collections.intersection(consideredVariables(resolvable), runningRestrictedBounds);

            AnswerCountEstimator.IncrementalEstimator thisResolvableOnlyEstimator = answerCountEstimator.createIncrementalEstimator(conjunctionSummary.conjunction());
            thisResolvableOnlyEstimator.extend(resolvable);
            double allPossibleBounds = thisResolvableOnlyEstimator.answerEstimate(restrictedResolvableBounds);
            double boundsFromPrefix = estimator.answerEstimate(restrictedResolvableBounds);
            double scalingFactor = Math.min(1, boundsFromPrefix/allPossibleBounds);
            acyclicCost += scalingFactor * acyclicCost(conjunctionSummary, resolvable, resolvableBounds);
            if (resolvable.isConcludable() && conjunctionSummary.cyclicConcludables().contains(resolvable.asConcludable())) {
                scalingFactors.put(resolvable.asConcludable(), scalingFactor);
                cyclicConcludableBounds.add(new Pair<>(resolvable.asConcludable(), resolvableBounds));
            }

            estimator.extend(resolvable);
            runningBounds.addAll(consideredVariables(resolvable));
            runningRestrictedBounds.addAll(consideredVariables(resolvable));
        }

        CallKey callKey = new CallKey(conjunctionSummary.conjunction(), new HashSet<>(inputBounds));
        return new OrderingSummary(callKey, ordering, acyclicCost, cyclicConcludableBounds, scalingFactors);
    }

    private long retrievalCost(ResolvableConjunction conjunction, Resolvable<?> resolvable, Set<Variable> inputBounds) {
        // Inaccurate because retrievables traversals work differently.
        // Also inaccurate because it considers inferred answers for concludables? We could rename to computeLocalCost.
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunction);
        estimator.extend(resolvable);
        return estimator.answerEstimate(consideredVariables(resolvable));
    }

    private double acyclicCost(ConjunctionSummary conjunctionSummary, Resolvable<?> resolvable, Set<Variable> resolvableBounds) {
        double cost = 0.0;
        if (resolvable.isRetrievable()) {
            cost += retrievalCost(conjunctionSummary.conjunction(), resolvable, resolvableBounds);
        } else if (resolvable.isConcludable()) {
            cost += retrievalCost(conjunctionSummary.conjunction(), resolvable, resolvableBounds);
            Set<CallKey> acyclicCalls = triggeredCalls(resolvable.asConcludable(), resolvableBounds, Optional.of(conjunctionSummary.acyclicDependencies(resolvable.asConcludable())));
            cost += iterate(acyclicCalls).map(acylcicCall -> getPlan(acylcicCall).cost()).reduce(0L, Long::sum);    // Assumption: Decompose the global planning problems to SCCs
        } else if (resolvable.isNegated()) {
            cost += iterate(resolvable.asNegated().disjunction().conjunctions())
                    .map(conjunction -> getPlan(new CallKey(conjunction, resolvableBounds)).cost())
                    .reduce(0L, Long::sum);
        } else throw TypeDBException.of(ILLEGAL_STATE);
        return cost;
    }

    private double cyclePlanCost(CallKey root, Map<CallKey, OrderingSummary> chosenSummaries) {
        // Collect scaling factors and acyclic-costs
        Map<CallKey, Double> scalingFactorSum = new HashMap<>();
        scalingFactorSum.put(root, 1.0); // root is unscaled
        for (OrderingSummary orderingSummary : chosenSummaries.values()) {
            ConjunctionSummary conjunctionSummary = conjunctionSummarizer.conjunctionSummary(orderingSummary.callKey.conjunction);
            orderingSummary.cyclicConcludableBounds.forEach(concludableBounds -> {
                iterate(triggeredCalls(concludableBounds.first(), concludableBounds.second(), Optional.of(conjunctionSummary.cyclicDependencies(concludableBounds.first()))))
                        .forEachRemaining(callKey -> {
                            scalingFactorSum.put(callKey, Math.min(1.0,
                                    scalingFactorSum.getOrDefault(callKey, 0.0) + orderingSummary.scalingFactors.get(concludableBounds.first())));
                        });
            });
        }

        double cycleCost = 0L;
        for (OrderingSummary orderingSummary : chosenSummaries.values()) {
            cycleCost += orderingSummary.acyclicCost * scalingFactorSum.get(orderingSummary.callKey);
        }

        return cycleCost;
    }

    static class PartialOrderReductionSearch {
        private final Set<Resolvable<?>> resolvables;
        private final Set<Variable> inputBounds;
        private final Map<Resolvable<?>, Set<Variable>> dependencies;
        private final boolean enableConnectednessRestriction;

        // TODO: Combine these when we fix ReasonerPlanner.dependencies
        PartialOrderReductionSearch(Set<Resolvable<?>> resolvables, Set<Variable> inputBounds) {
            this(resolvables, inputBounds, ReasonerPlanner.dependencies(resolvables), true);
        }

        PartialOrderReductionSearch(Set<Resolvable<?>> resolvables, Set<Variable> inputBounds, Map<Resolvable<?>, Set<Variable>> dependencies, boolean enableConnectednessRestriction) {
            this.resolvables = resolvables;
            this.inputBounds = inputBounds;
            this.dependencies = dependencies;
            this.enableConnectednessRestriction = enableConnectednessRestriction;
        }

        List<List<Resolvable<?>>> allOrderings() {
            Set<Variable> bounds = new HashSet<>(inputBounds);
            Set<Resolvable<?>> remaining = new HashSet<>(resolvables);

            List<List<Resolvable<?>>> orderings = new ArrayList<>();
            porDFS(new Stack<>(), bounds, remaining, new HashSet<>(), orderings);
            return orderings; // TODO: If this causes a memory blow-up, POR can "generate" one at a time.
        }

        private void porDFS(Stack<Resolvable<?>> currentPath, Set<Variable> currentBounds,
                            Set<Resolvable<?>> remaining, Set<Resolvable<?>> sleepSet, List<List<Resolvable<?>>> orderings) {

            if (remaining.isEmpty()) {
                orderings.add(new ArrayList<>(currentPath));
                return;
            }

            List<Resolvable<?>> enabled = iterate(remaining)
                    .filter(r -> ReasonerPlanner.dependenciesSatisfied(r, currentBounds, dependencies) && !sleepSet.contains(r))
                    .toList();

            if (enabled.isEmpty()) {
                enabled = iterate(remaining).filter(r -> !r.isNegated() && !sleepSet.contains(r)).toList();
            }

            if (enabled.isEmpty()) {
                return; // All enabled are sleeping
            }

            if (enableConnectednessRestriction) {
                List<Resolvable<?>> connectedEnabled = iterate(enabled)
                        .filter(r -> iterate(consideredVariables(r)).anyMatch(currentBounds::contains)).toList();
                if (!connectedEnabled.isEmpty()) {
                    enabled = connectedEnabled;
                }
            }

            Set<Resolvable<?>> newSleepSet = new HashSet<>(sleepSet);
            for (Resolvable<?> next : enabled) {
                Set<Resolvable<?>> awaken = iterate(newSleepSet) //  Don't need to iterate(remaining), do I?
                        .filter(r -> iterate(r.variables()).anyMatch(next.variables()::contains)) // not necessarily newly bound
                        .toSet();

                Set<Resolvable<?>> nextSleepSet = iterate(newSleepSet).filter(r -> !awaken.contains(r)).toSet();
                Set<Variable> newlyBoundVars = next.isNegated() ? new HashSet<>() : iterate(next.variables()).filter(v -> !currentBounds.contains(v)).toSet();

                currentPath.add(next);
                remaining.remove(next);
                currentBounds.addAll(newlyBoundVars);
                porDFS(currentPath, currentBounds, remaining, nextSleepSet, orderings);
                currentBounds.removeAll(newlyBoundVars);
                remaining.add(next);
                currentPath.remove(currentPath.size() - 1);

                newSleepSet.add(next);
            }
        }
    }

    static class OrderingSummary {
        final CallKey callKey;
        final double acyclicCost;
        final Map<Concludable, Double> scalingFactors;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableBounds;
        final List<Resolvable<?>> ordering;

        private OrderingSummary(CallKey callKey, List<Resolvable<?>> ordering, double acyclicCost,
                                Set<Pair<Concludable, Set<Variable>>> cyclicConcludableBounds, Map<Concludable, Double> scalingFactors) {
            this.callKey = callKey;
            this.ordering = ordering;
            this.acyclicCost = acyclicCost;
            this.cyclicConcludableBounds = cyclicConcludableBounds;
            this.scalingFactors = scalingFactors;
        }
    }
}
