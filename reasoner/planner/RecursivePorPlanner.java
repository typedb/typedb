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
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph.ConjunctionNode;
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
    //      Excess calls are not penalised, since the scaling factor is capped at one (Solve using (calls + scaling-factor * rec-cost?) )
    //      Acyclic dependencies are counted once for each occurence
    //          (Can be fixed by including the optimal plans for the bound in the globalPlan chosenSummaries - The scaling factors will be capped at 1 then)
    // So far, these are acceptable because the difference in cost is some constant factor, and our aim is to avoid horrible plans.
    //
    //      !!! The cost does not depend on the binding mode !!! because of the formulation - Handle this with connectedness restriction when generating orders?

    final AnswerCountEstimator answerCountEstimator;
    private final Map<CallMode, Set<OrderingChoice>> candidateOrderings;
    private final ConjunctionGraph conjunctionGraph;
    private final Map<CallMode, Double> cyclicScalingFactors;

    public RecursivePorPlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
        this.conjunctionGraph = new ConjunctionGraph(logicMgr);
        this.answerCountEstimator = new AnswerCountEstimator(logicMgr, traversalEng.graph(), this.conjunctionGraph);
        this.candidateOrderings = new HashMap<>();
        this.cyclicScalingFactors = new HashMap<>();
    }

    private static Set<Variable> consideredVariables(Resolvable<?> resolvable) {
        return iterate(resolvable.variables()).filter(Variable::isThing).toSet();
    }

    @Override
    Plan computePlan(CallMode callMode) {
        recursivelyGenerateOrderingChoices(callMode);
        planComponent(callMode);
        assert planCache.getIfPresent(callMode) != null;
        return planCache.getIfPresent(callMode);
    }

    // combined planning for all nodes in a strongly-connected-component
    private void planComponent(CallMode callMode) {
        Map<CallMode, OrderingChoice> chosenSummaries = new HashMap<>();
        Set<CallMode> pendingKeys = new HashSet<>();
        pendingKeys.add(callMode);
        ComponentPlan bestPlan = componentPlanSearch(callMode, pendingKeys, chosenSummaries);

        for (OrderingChoice bestPlanForCall : bestPlan.orderingChoices.values()) {
            planCache.put(bestPlanForCall.callMode, new Plan(bestPlanForCall.ordering, Math.round(Math.ceil(bestPlan.cost(bestPlanForCall.callMode, 1.0)))));
            cyclicScalingFactors.put(bestPlanForCall.callMode, bestPlan.cyclicScalingFactorSum.get(bestPlanForCall.callMode));
        }
    }

    private ComponentPlan componentPlanSearch(CallMode root, Set<CallMode> pendingCallModes, Map<CallMode, OrderingChoice> choices) { // The value contains the scaling factors needed for the other conjunctions in the globalCost.
        // Pick a choice of ordering, expand dependencies, recurse ; backtrack over choices
        if (pendingCallModes.isEmpty()) { // All modes have an ordering chosen -> we have a complete candidate plan
            return createComponentPlan(choices);
        }

        CallMode mode = iterate(pendingCallModes).next();
        pendingCallModes.remove(mode);
        assert !choices.containsKey(mode); // Should not have been added to pending

        ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(mode.conjunction);
        ComponentPlan bestPlan = null; // for the branch of choices committed to so far.
        double bestPlanCost = Double.MAX_VALUE;
        for (OrderingChoice orderingChoice : candidateOrderings.get(mode)) {
            choices.put(mode, orderingChoice);

            Set<CallMode> nextPendingKeys = new HashSet<>(pendingCallModes);
            Set<CallMode> triggeredCalls = new HashSet<>();
            iterate(orderingChoice.cyclicConcludableBounds).forEachRemaining(concludableBounds -> {
                triggeredCalls.addAll(triggeredCalls(concludableBounds.first(), concludableBounds.second(), Optional.of(conjunctionNode.cyclicDependencies(concludableBounds.first()))));
            });
            iterate(triggeredCalls).filter(call -> !choices.containsKey(call)).forEachRemaining(nextPendingKeys::add);
            ComponentPlan newPlan = componentPlanSearch(root, nextPendingKeys, choices);

            double newPlanCost = newPlan.cost(root, 1.0);
            if (bestPlan == null || newPlanCost < bestPlanCost) {
                bestPlan = newPlan;
                bestPlanCost = newPlanCost;
            }

            choices.remove(mode);
        }
        assert bestPlan != null;
        return bestPlan;
    }

    private void initialiseOrderingDependencies(ConjunctionNode conjunctionNode, List<Resolvable<?>> ordering, Set<Variable> inputBounds) {
        Set<Variable> currentBounds = new HashSet<>(inputBounds);
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableBounds = iterate(consideredVariables(resolvable)).filter(currentBounds::contains).toSet();
            initialiseDependencies(conjunctionNode, resolvable, resolvableBounds);
            currentBounds.addAll(consideredVariables(resolvable));
        }
    }

    private void initialiseDependencies(ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableBounds) {
        if (resolvable.isConcludable()) {
            Set<ResolvableConjunction> cyclicDependencies = conjunctionNode.cyclicDependencies(resolvable.asConcludable());
            for (CallMode callMode : triggeredCalls(resolvable.asConcludable(), resolvableBounds, Optional.empty())) {
                recursivelyGenerateOrderingChoices(callMode);
                if (!cyclicDependencies.contains(callMode.conjunction)) {
                    plan(callMode); // Acyclic dependencies can be fully planned
                }
            }
        } else if (resolvable.isNegated()) {
            iterate(resolvable.asNegated().disjunction().conjunctions()).forEachRemaining(conjunction -> {
                CallMode callMode = new CallMode(conjunction, resolvableBounds);
                recursivelyGenerateOrderingChoices(callMode);
                plan(callMode);
            });
        }
    }

    private void recursivelyGenerateOrderingChoices(CallMode callMode) {
        if (!candidateOrderings.containsKey(callMode)) {
            candidateOrderings.put(callMode, null); // Guard
            ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(callMode.conjunction);
            answerCountEstimator.buildConjunctionModel(callMode.conjunction);

            // Two orderings for the same CallMode with identical cyclic-concludable modes are interchangeable in the component-plan
            //      -> We only need to keep the cheaper one.
            Map<Set<Pair<Concludable, Set<Variable>>>, OrderingChoice> bestChoice = new HashMap<>();
            PartialOrderReductionSearch porSearch = new PartialOrderReductionSearch(logicMgr.compile(callMode.conjunction), callMode.bounds);
            for (List<Resolvable<?>> ordering : porSearch.allOrderings()) {
                initialiseOrderingDependencies(conjunctionNode, ordering, callMode.bounds);
                OrderingChoice orderingChoice = summariseOrdering(conjunctionNode, ordering, callMode.bounds);
                if (!bestChoice.containsKey(orderingChoice.cyclicConcludableBounds) ||
                        bestChoice.get(orderingChoice.cyclicConcludableBounds).acyclicCost > orderingChoice.acyclicCost) {
                    bestChoice.put(orderingChoice.cyclicConcludableBounds, orderingChoice);
                }
            }
            candidateOrderings.put(callMode, new HashSet<>(bestChoice.values()));
        }
    }

    private OrderingChoice summariseOrdering(ConjunctionNode conjunctionNode, List<Resolvable<?>> ordering, Set<Variable> inputBounds) {
        Set<Variable> bounds = new HashSet<>(inputBounds);
        Set<Variable> restrictedBounds = new HashSet<>(); // Restricted by preceding resolvables
        double acyclicCost = 0L;

        Map<Concludable, Double> scalingFactors = new HashMap<>();
        Set<Pair<Concludable, Set<Variable>>> cyclicConcludableBounds = new HashSet<>();
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableBounds = Collections.intersection(consideredVariables(resolvable), bounds);
            Set<Variable> restrictedResolvableBounds = Collections.intersection(consideredVariables(resolvable), restrictedBounds);

            double boundsFromPrefix = estimator.answerEstimate(restrictedResolvableBounds);

            if (resolvable.isNegated()) {
                acyclicCost += iterate(resolvable.asNegated().disjunction().conjunctions()).map(conj -> {
                    double allPossibleBounds = answerCountEstimator.estimateAnswers(conj, restrictedResolvableBounds);
                    double scalingFactor = Math.min(1, boundsFromPrefix / allPossibleBounds);
                    return scaledCallCost(scalingFactor, new CallMode(conj, resolvableBounds));
                }).reduce(0.0, Double::sum);

            } else {
                AnswerCountEstimator.IncrementalEstimator thisResolvableOnlyEstimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
                thisResolvableOnlyEstimator.extend(resolvable);
                double allPossibleBounds = thisResolvableOnlyEstimator.answerEstimate(restrictedResolvableBounds);
                double scalingFactor = Math.min(1, boundsFromPrefix / allPossibleBounds);
                acyclicCost += scaledAcyclicCost(scalingFactor, conjunctionNode, resolvable, resolvableBounds);

                if (resolvable.isConcludable() && conjunctionNode.cyclicConcludables().contains(resolvable.asConcludable())) {

                    Set<Variable> nonInputRestrictedBounds = iterate(restrictedResolvableBounds).filter(v -> !inputBounds.contains(v)).toSet();
//                    double cyclicScalingFactor = nonInputRestrictedBounds.isEmpty() ? 0.0 : 1.0;
                    // Approximation: This severely underestimates the number of cyclic-calls generated in the case where a mix of input and local variables are arguments to the call.
                    double cyclicScalingFactor = nonInputRestrictedBounds.isEmpty() ? 0.0 : (double) estimator.answerEstimate(nonInputRestrictedBounds) / thisResolvableOnlyEstimator.answerEstimate(resolvableBounds);
                    scalingFactors.put(resolvable.asConcludable(), cyclicScalingFactor);
                    cyclicConcludableBounds.add(new Pair<>(resolvable.asConcludable(), resolvableBounds));
                }
            }

            estimator.extend(resolvable);
            bounds.addAll(consideredVariables(resolvable));
            restrictedBounds.addAll(consideredVariables(resolvable));
        }

        CallMode callMode = new CallMode(conjunctionNode.conjunction(), new HashSet<>(inputBounds));
        return new OrderingChoice(callMode, ordering, acyclicCost, cyclicConcludableBounds, scalingFactors);
    }

    private long retrievalCost(ResolvableConjunction conjunction, Resolvable<?> resolvable, Set<Variable> inputBounds) {
        // Inaccurate because retrievables traversals work differently.
        // Also inaccurate because it considers inferred answers for concludables? We could rename to computeLocalCost.
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunction);
        estimator.extend(resolvable);
        return estimator.answerEstimate(consideredVariables(resolvable));
    }

    private double scaledAcyclicCost(double scalingFactor, ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableBounds) {
        assert !resolvable.isNegated();
        double cost = 0.0;
        if (resolvable.isRetrievable()) {
            cost += scalingFactor * retrievalCost(conjunctionNode.conjunction(), resolvable, resolvableBounds);
        } else if (resolvable.isConcludable()) {
            cost += scalingFactor * retrievalCost(conjunctionNode.conjunction(), resolvable, resolvableBounds);
            Set<CallMode> acyclicCalls = triggeredCalls(resolvable.asConcludable(), resolvableBounds, Optional.of(conjunctionNode.acyclicDependencies(resolvable.asConcludable())));
            cost += iterate(acyclicCalls).map(acylcicCall -> scaledCallCost(scalingFactor, acylcicCall)).reduce(0.0, Double::sum);    // Assumption: Decompose the global planning problems to SCCs
        } else throw TypeDBException.of(ILLEGAL_STATE);
        return cost;
    }

    private double scaledCallCost(double scalingFactor, CallMode callMode) {
        return getPlan(callMode).cost() * Math.min(1.0, scalingFactor + cyclicScalingFactors.get(callMode));
    }

    private ComponentPlan createComponentPlan(Map<CallMode, OrderingChoice> chosenSummaries) {
        Map<CallMode, Double> scalingFactorSum = new HashMap<>();
        for (OrderingChoice orderingChoice : chosenSummaries.values()) {
            ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(orderingChoice.callMode.conjunction);
            orderingChoice.cyclicConcludableBounds.forEach(concludableBounds -> {
                iterate(triggeredCalls(concludableBounds.first(), concludableBounds.second(), Optional.of(conjunctionNode.cyclicDependencies(concludableBounds.first()))))
                        .forEachRemaining(callMode -> {
                            scalingFactorSum.put(callMode, Math.min(1.0,
                                    scalingFactorSum.getOrDefault(callMode, 0.0) + orderingChoice.scalingFactors.get(concludableBounds.first())));
                        });
            });
        }
        return new ComponentPlan(chosenSummaries, scalingFactorSum);
    }

    private static class ComponentPlan {
        private final Map<CallMode, OrderingChoice> orderingChoices;
        private final Map<CallMode, Double> cyclicScalingFactorSum;

        private ComponentPlan(Map<CallMode, OrderingChoice> orderingChoices, Map<CallMode, Double> cyclicScalingFactorSum) {
            this.orderingChoices = new HashMap<>(orderingChoices);
            this.cyclicScalingFactorSum = new HashMap<>(cyclicScalingFactorSum);
            this.orderingChoices.keySet().forEach(callMode -> this.cyclicScalingFactorSum.putIfAbsent(callMode, 0.0));
        }

        private double cost(CallMode root, double rootScalingFactor) {
            double cycleCost = 0L;
            for (OrderingChoice orderingChoice : orderingChoices.values()) {
                double scalingFactor = cyclicScalingFactorSum.get(orderingChoice.callMode) + (orderingChoice.callMode.equals(root) ? rootScalingFactor : 0.0);
                cycleCost += orderingChoice.acyclicCost * scalingFactor;
            }

            return cycleCost;
        }
    }

    static class OrderingChoice {
        final CallMode callMode;
        final double acyclicCost;
        final Map<Concludable, Double> scalingFactors;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableBounds;
        final List<Resolvable<?>> ordering;

        private OrderingChoice(CallMode callMode, List<Resolvable<?>> ordering, double acyclicCost,
                               Set<Pair<Concludable, Set<Variable>>> cyclicConcludableBounds, Map<Concludable, Double> scalingFactors) {
            this.callMode = callMode;
            this.ordering = ordering;
            this.acyclicCost = acyclicCost;
            this.cyclicConcludableBounds = cyclicConcludableBounds;
            this.scalingFactors = scalingFactors;
        }
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
            Set<Resolvable<?>> remaining = new HashSet<>(resolvables);
            Set<Variable> bounds = new HashSet<>(inputBounds);
            iterate(resolvables).flatMap(r -> iterate(consideredVariables(r)))
                    .filter(v -> iterate(v.constraints()).anyMatch(constraint -> constraint.isThing() && constraint.asThing().isValue() && constraint.asThing().asValue().isValueIdentity()))
                    .forEachRemaining(bounds::add);

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
}
