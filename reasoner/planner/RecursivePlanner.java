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

public class RecursivePlanner extends ReasonerPlanner {
    // Inaccuracies:
    //      retrieval costs are treated the same as reasoning-overhead cost (Both approximated as the number of answers retrieved for all variables)
    //      Excess calls are not penalised, since the scaling factor is capped at one (Solve using (calls + scaling-factor * rec-cost?) )
    //      Acyclic dependencies are counted once for each occurence
    //          (Can be fixed by including the optimal plans for the bound in the globalPlan chosenSummaries - The scaling factors will be capped at 1 then)
    // So far, these are acceptable because the difference in cost is some constant factor, and our aim is to avoid horrible plans.
    //
    //      !!! The cost does not depend on the binding mode !!! because of the formulation - Handle this with connectedness restriction when generating orders?

    final AnswerCountEstimator answerCountEstimator;
    private final Map<CallMode, Set<PlanChoice>> planChoices;
    private final ConjunctionGraph conjunctionGraph;
    private final Map<CallMode, Double> cyclicScalingFactors;

    public RecursivePlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
        this.conjunctionGraph = new ConjunctionGraph(logicMgr);
        this.answerCountEstimator = new AnswerCountEstimator(logicMgr, traversalEng.graph(), this.conjunctionGraph);
        this.planChoices = new HashMap<>();
        this.cyclicScalingFactors = new HashMap<>();
    }

    @Override
    Plan computePlan(CallMode callMode) {
        recursivelyGeneratePlanChoices(callMode);
        planMutuallyRecursiveSubgraph(callMode);
        assert planCache.getIfPresent(callMode) != null;
        return planCache.getIfPresent(callMode);
    }

    // Conjunctions which call each other must be planned together
    private void planMutuallyRecursiveSubgraph(CallMode callMode) {
        Map<CallMode, PlanChoice> choices = new HashMap<>();
        Set<CallMode> pendingModes = new HashSet<>();
        pendingModes.add(callMode);
        SubgraphPlan bestPlan = subgraphPlanSearch(callMode, pendingModes, choices);

        for (PlanChoice bestPlanForCall : bestPlan.choices.values()) {
            planCache.put(bestPlanForCall.callMode, new Plan(bestPlanForCall.ordering, Math.round(Math.ceil(bestPlan.cost(bestPlanForCall.callMode)))));
            cyclicScalingFactors.put(bestPlanForCall.callMode, bestPlan.cyclicScalingFactorSum.get(bestPlanForCall.callMode));
        }
    }

    private SubgraphPlan subgraphPlanSearch(CallMode root, Set<CallMode> pendingCallModes, Map<CallMode, PlanChoice> choices) { // The value contains the scaling factors needed for the other conjunctions in the globalCost.
        // Pick a choice of ordering, expand dependencies, recurse ; backtrack over choices
        if (pendingCallModes.isEmpty()) { // All modes have an ordering chosen -> we have a complete candidate plan for the subgraph
            return createSubgraphPlan(choices);
        }

        CallMode mode = iterate(pendingCallModes).next();
        pendingCallModes.remove(mode);
        assert !choices.containsKey(mode); // Should not have been added to pending

        ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(mode.conjunction);
        SubgraphPlan bestPlan = null; // for the branch of choices committed to so far.
        double bestPlanCost = Double.MAX_VALUE;
        for (PlanChoice planChoice : planChoices.get(mode)) {
            choices.put(mode, planChoice);

            Set<CallMode> nextPendingModes = new HashSet<>(pendingCallModes);
            Set<CallMode> triggeredCalls = new HashSet<>();
            iterate(planChoice.cyclicConcludableModes).forEachRemaining(concludableMode -> {
                triggeredCalls.addAll(triggeredCalls(concludableMode.first(), concludableMode.second(), conjunctionNode.cyclicDependencies(concludableMode.first())));
            });
            iterate(triggeredCalls).filter(call -> !choices.containsKey(call)).forEachRemaining(nextPendingModes::add);
            SubgraphPlan newPlan = subgraphPlanSearch(root, nextPendingModes, choices);

            double newPlanCost = newPlan.cost(root);
            if (bestPlan == null || newPlanCost < bestPlanCost) {
                bestPlan = newPlan;
                bestPlanCost = newPlanCost;
            }

            choices.remove(mode);
        }
        assert bestPlan != null;
        return bestPlan;
    }

    private void initialiseOrderingDependencies(ConjunctionNode conjunctionNode, List<Resolvable<?>> ordering, Set<Variable> mode) {
        Set<Variable> currentBounds = new HashSet<>(mode);
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableMode = Collections.intersection(estimateableVariables(resolvable.variables()), currentBounds);
            initialiseDependencies(conjunctionNode, resolvable, resolvableMode);
            if (!resolvable.isNegated()) currentBounds.addAll(estimateableVariables(resolvable.variables()));
        }
    }

    private void initialiseDependencies(ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableMode) {
        if (resolvable.isConcludable()) {
            Set<ResolvableConjunction> cyclicDependencies = conjunctionNode.cyclicDependencies(resolvable.asConcludable());
            for (CallMode callMode : triggeredCalls(resolvable.asConcludable(), resolvableMode, null)) {
                recursivelyGeneratePlanChoices(callMode);
                if (!cyclicDependencies.contains(callMode.conjunction)) {
                    plan(callMode); // Acyclic dependencies can be fully planned
                }
            }
        } else if (resolvable.isNegated()) {
            iterate(resolvable.asNegated().disjunction().conjunctions()).forEachRemaining(conjunction -> {
                CallMode callMode = new CallMode(conjunction, resolvableMode);
                recursivelyGeneratePlanChoices(callMode);
                plan(callMode);
            });
        }
    }

    private void recursivelyGeneratePlanChoices(CallMode callMode) {
        if (!planChoices.containsKey(callMode)) {
            planChoices.put(callMode, null); // Guard
            ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(callMode.conjunction);
            answerCountEstimator.buildConjunctionModel(callMode.conjunction);

            Map<Set<Pair<Concludable, Set<Variable>>>, PlanChoice> planChoices = new HashMap<>();
            PartialOrderReductionSearch porSearch = new PartialOrderReductionSearch(logicMgr.compile(callMode.conjunction), callMode.mode);
            for (List<Resolvable<?>> ordering : porSearch.allOrderings()) {
                initialiseOrderingDependencies(conjunctionNode, ordering, callMode.mode);
                PlanChoice planChoice = summarisePlanChoice(conjunctionNode, ordering, callMode.mode);

                // Two orderings for the same CallMode with identical cyclic-concludable modes are interchangeable in the subgraph-plan
                //      -> We only need to keep the cheaper one.
                if (!planChoices.containsKey(planChoice.cyclicConcludableModes) ||
                        planChoices.get(planChoice.cyclicConcludableModes).acyclicCost > planChoice.acyclicCost) {
                    planChoices.put(planChoice.cyclicConcludableModes, planChoice);
                }
            }
            this.planChoices.put(callMode, new HashSet<>(planChoices.values()));
        }
    }

    private PlanChoice summarisePlanChoice(ConjunctionNode conjunctionNode, List<Resolvable<?>> ordering, Set<Variable> mode) {
        Set<Variable> bounds = new HashSet<>(mode);
        Set<Variable> restrictedBounds = new HashSet<>(); // Restricted by preceding resolvables
        double acyclicCost = 0L;

        Map<Concludable, Double> scalingFactors = new HashMap<>();
        Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes = new HashSet<>();
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> estimateableVars = estimateableVariables(resolvable.variables());
            Set<Variable> resolvableMode = Collections.intersection(estimateableVars, bounds);
            Set<Variable> restrictedResolvableBounds = Collections.intersection(estimateableVars, restrictedBounds);

            double boundsFromPrefix = estimator.answerEstimate(restrictedResolvableBounds);

            if (resolvable.isNegated()) {
                acyclicCost += iterate(resolvable.asNegated().disjunction().conjunctions()).map(conj -> {
                    double allPossibleBounds = answerCountEstimator.estimateAnswers(conj, restrictedResolvableBounds);
                    double scalingFactor = Math.min(1, boundsFromPrefix / allPossibleBounds);
                    return scaledCallCost(scalingFactor, new CallMode(conj, resolvableMode));
                }).reduce(0.0, Double::sum);
            } else {
                AnswerCountEstimator.IncrementalEstimator thisResolvableOnlyEstimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
                thisResolvableOnlyEstimator.extend(resolvable);
                double allPossibleBounds = thisResolvableOnlyEstimator.answerEstimate(restrictedResolvableBounds);
                double scalingFactor = Math.min(1, boundsFromPrefix / allPossibleBounds);
                acyclicCost += scaledAcyclicCost(scalingFactor, conjunctionNode, resolvable, resolvableMode);

                if (resolvable.isConcludable() && conjunctionNode.cyclicConcludables().contains(resolvable.asConcludable())) {
                    Set<Variable> nonInputRestrictedBounds = iterate(restrictedResolvableBounds).filter(v -> !mode.contains(v)).toSet();
                    // Approximation: This severely underestimates the number of cyclic-calls generated in the case where a mix of input and local variables are arguments to the call.
                    double cyclicScalingFactor = nonInputRestrictedBounds.isEmpty() ? 0.0 :
                            (double) estimator.answerEstimate(nonInputRestrictedBounds) / thisResolvableOnlyEstimator.answerEstimate(resolvableMode);
                    scalingFactors.put(resolvable.asConcludable(), cyclicScalingFactor);
                    cyclicConcludableModes.add(new Pair<>(resolvable.asConcludable(), resolvableMode));
                }
            }

            estimator.extend(resolvable);
            if (!resolvable.isNegated()) {
                bounds.addAll(estimateableVars);
                restrictedBounds.addAll(estimateableVars);
            }
        }

        CallMode callMode = new CallMode(conjunctionNode.conjunction(), new HashSet<>(mode));
        return new PlanChoice(callMode, ordering, acyclicCost, cyclicConcludableModes, scalingFactors);
    }

    private long retrievalCost(ResolvableConjunction conjunction, Resolvable<?> resolvable, Set<Variable> mode) {
        // Inaccurate because retrievables traversals work differently.
        // Also inaccurate because it considers inferred answers for concludables? We could rename to computeLocalCost.
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunction);
        estimator.extend(resolvable);
        return estimator.answerEstimate(estimateableVariables(resolvable.variables()));
    }

    private double scaledAcyclicCost(double scalingFactor, ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableMode) {
        assert !resolvable.isNegated();
        double cost = 0.0;
        if (resolvable.isRetrievable()) {
            cost += scalingFactor * retrievalCost(conjunctionNode.conjunction(), resolvable, resolvableMode);
        } else if (resolvable.isConcludable()) {
            cost += scalingFactor * retrievalCost(conjunctionNode.conjunction(), resolvable, resolvableMode);
            Set<CallMode> acyclicCalls = triggeredCalls(resolvable.asConcludable(), resolvableMode, conjunctionNode.acyclicDependencies(resolvable.asConcludable()));
            cost += iterate(acyclicCalls).map(acylcicCall -> scaledCallCost(scalingFactor, acylcicCall)).reduce(0.0, Double::sum);    // Assumption: Decompose the global planning problems to SCCs
        } else throw TypeDBException.of(ILLEGAL_STATE);
        return cost;
    }

    private double scaledCallCost(double scalingFactor, CallMode callMode) {
        return getPlan(callMode).cost() * Math.min(1.0, scalingFactor + cyclicScalingFactors.get(callMode));
    }

    private SubgraphPlan createSubgraphPlan(Map<CallMode, PlanChoice> chosenSummaries) {
        Map<CallMode, Double> scalingFactorSum = new HashMap<>();
        for (PlanChoice planChoice : chosenSummaries.values()) {
            ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(planChoice.callMode.conjunction);
            planChoice.cyclicConcludableModes.forEach(concludableMode -> {
                iterate(triggeredCalls(concludableMode.first(), concludableMode.second(), conjunctionNode.cyclicDependencies(concludableMode.first())))
                        .forEachRemaining(callMode -> {
                            scalingFactorSum.put(callMode, Math.min(1.0,
                                    scalingFactorSum.getOrDefault(callMode, 0.0) + planChoice.scalingFactors.get(concludableMode.first())));
                        });
            });
        }
        return new SubgraphPlan(chosenSummaries, scalingFactorSum);
    }

    private static class SubgraphPlan {
        private final Map<CallMode, PlanChoice> choices;
        private final Map<CallMode, Double> cyclicScalingFactorSum;

        private SubgraphPlan(Map<CallMode, PlanChoice> choices, Map<CallMode, Double> cyclicScalingFactorSum) {
            this.choices = new HashMap<>(choices);
            this.cyclicScalingFactorSum = new HashMap<>(cyclicScalingFactorSum);
            this.choices.keySet().forEach(callMode -> this.cyclicScalingFactorSum.putIfAbsent(callMode, 0.0));
        }

        private double cost(CallMode root) {
            double cycleCost = 0L;
            for (PlanChoice planChoice : choices.values()) {
                double scalingFactor = planChoice.callMode.equals(root) ? 1.0 : cyclicScalingFactorSum.get(planChoice.callMode);
                cycleCost += planChoice.acyclicCost * scalingFactor;
            }

            return cycleCost;
        }
    }

    static class PlanChoice {
        final CallMode callMode;
        final List<Resolvable<?>> ordering;
        final double acyclicCost;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        final Map<Concludable, Double> scalingFactors;

        private PlanChoice(CallMode callMode, List<Resolvable<?>> ordering, double acyclicCost,
                           Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes, Map<Concludable, Double> scalingFactors) {
            this.callMode = callMode;
            this.ordering = ordering;
            this.acyclicCost = acyclicCost;
            this.cyclicConcludableModes = cyclicConcludableModes;
            this.scalingFactors = scalingFactors;
        }
    }

    static class PartialOrderReductionSearch {
        private final Set<Resolvable<?>> resolvables;
        private final Set<Variable> mode;
        private final Map<Resolvable<?>, Set<Variable>> dependencies;
        private final boolean enableConnectednessRestriction;

        // TODO: Combine these when we fix ReasonerPlanner.dependencies
        PartialOrderReductionSearch(Set<Resolvable<?>> resolvables, Set<Variable> mode) {
            this(resolvables, mode, ReasonerPlanner.dependencies(resolvables), true);
        }

        PartialOrderReductionSearch(Set<Resolvable<?>> resolvables, Set<Variable> mode, Map<Resolvable<?>, Set<Variable>> dependencies, boolean enableConnectednessRestriction) {
            this.resolvables = resolvables;
            this.mode = mode;
            this.dependencies = dependencies;
            this.enableConnectednessRestriction = enableConnectednessRestriction;
        }

        List<List<Resolvable<?>>> allOrderings() {
            Set<Resolvable<?>> remaining = new HashSet<>(resolvables);
            Set<Variable> bounds = new HashSet<>(mode);
            iterate(resolvables).filter(resolvable -> !resolvable.isNegated()).flatMap(r -> iterate(estimateableVariables(r.variables())))
                    .filter(v -> iterate(v.constraints()).anyMatch(constraint ->
                            constraint.isThing() && constraint.asThing().isValue() && constraint.asThing().asValue().isValueIdentity()))
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
                        .filter(r -> iterate(estimateableVariables(r.variables())).anyMatch(currentBounds::contains)).toList();
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
                Set<Variable> newlyBoundVars = next.isNegated() ?
                        new HashSet<>() :
                        iterate(next.variables()).filter(v -> !currentBounds.contains(v)).toSet();

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
