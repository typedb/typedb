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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner.estimateableVariables;

public class OrderingCoster {
    private final ReasonerPlanner planner;
    private final AnswerCountEstimator answerCountEstimator;
    private final ConjunctionGraph conjunctionGraph;

    public OrderingCoster(ReasonerPlanner planner, AnswerCountEstimator answerCountEstimator, ConjunctionGraph conjunctionGraph) {
        this.planner = planner;
        this.answerCountEstimator = answerCountEstimator;
        this.conjunctionGraph = conjunctionGraph;
    }

    LocalSingleCallCosting createSingleCallCosting(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering) {
        SingleCallCostingBuilder estimator = new SingleCallCostingBuilder(callMode);
        for (Resolvable<?> resolvable : ordering) {
            estimator.extend(resolvable);
        }
        return estimator.build();
    }

    LocalAllCallsCosting createAllCallsCosting(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering, Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes) {
        ConjunctionGraph.ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(callMode.conjunction);

        Set<Variable> boundVars = new HashSet<>(callMode.mode);  // bound -> in input mode or restricted locally
        Set<Variable> restrictedVars = new HashSet<>(); // restricted -> Restricted by preceding resolvables
        Set<Variable> inputConnectedVars = new HashSet<>(callMode.mode);
        double acyclicCost = 0.0;
        double disconnectedCost = 0.0;

        Map<Concludable, Double> cylicScalingFactors = new HashMap<>();
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
        for (Resolvable<?> resolvable : ordering) {
            Set<Variable> resolvableVars = estimateableVariables(resolvable.variables());
            Set<Variable> resolvableMode = Collections.intersection(resolvableVars, boundVars);
            Set<Variable> restrictedResolvableVars = Collections.intersection(resolvableVars, restrictedVars);

            double resolvableCost = scaledAcyclicCost(conjunctionNode, estimator, resolvable, restrictedResolvableVars, resolvableMode);

            if (resolvable.isConcludable() && conjunctionNode.cyclicConcludables().contains(resolvable.asConcludable())) {
                // Question: Do we project onto all the restrictedVars, or only those not in the mode?
                // Including those in the mode leads to double-scaling. Excluding them leads to an overestimate if the bounds are not restrictive.
                // I lean towards the overestimate.
                Set<Variable> projectionVars = iterate(restrictedResolvableVars).filter(v -> !callMode.mode.contains(v)).toSet();
                double allAnswersForUnrestrictedMode = answerCountEstimator.localEstimate(conjunctionNode.conjunction(), resolvable, resolvableMode);
                double cyclicScalingFactor = projectionVars.isEmpty() && allAnswersForUnrestrictedMode != 0 ? 0.0 :
                        (double) estimator.answerEstimate(projectionVars) / allAnswersForUnrestrictedMode;
                cylicScalingFactors.put(resolvable.asConcludable(), cyclicScalingFactor);
            }

            estimator.extend(resolvable);

            boolean isConnectedToInput = !Collections.intersection(resolvableMode, inputConnectedVars).isEmpty();
            if (isConnectedToInput) {
                acyclicCost += resolvableCost;
            } else {
                disconnectedCost += resolvableCost;
            }

            if (!resolvable.isNegated()) {
                boundVars.addAll(resolvableVars);
                restrictedVars.addAll(resolvableVars);
                if (isConnectedToInput) inputConnectedVars.addAll(resolvableVars);
            }
        }

        double answersToMode = answerCountEstimator.estimateAnswers(callMode.conjunction, callMode.mode);
        return new LocalAllCallsCosting(callMode, ordering, cyclicConcludableModes, cylicScalingFactors, acyclicCost, disconnectedCost, answersToMode);
    }

    private double retrievalCost(ResolvableConjunction conjunction, Resolvable<?> resolvable, Set<Variable> mode) {
        // Inaccurate because retrievables traversals work differently.
        // Also inaccurate because it considers inferred answers for concludables? We could rename to computeLocalCost.
        return answerCountEstimator.localEstimate(conjunction, resolvable, estimateableVariables(resolvable.variables()));
    }

    double scaledAcyclicCost(ConjunctionGraph.ConjunctionNode conjunctionNode, AnswerCountEstimator.IncrementalEstimator scaledEstimator,
                             Resolvable<?> resolvable, Set<Variable> restrictedResolvableMode, Set<Variable> resolvableMode) {
        if (resolvable.isNegated()) {
            return iterate(resolvable.asNegated().disjunction().conjunctions()).map(conj -> {
                Set<Variable> conjVariables = estimateableVariables(conj.pattern().variables());
                Set<Variable> nestedMode = Collections.intersection(conjVariables, restrictedResolvableMode);
                double answersForModeFromPrefix = scaledEstimator.answerEstimate(nestedMode);
                double allAnswersForMode = answerCountEstimator.estimateAnswers(conj, nestedMode);
                double scalingFactor = allAnswersForMode != 0 ? Math.min(1, answersForModeFromPrefix / allAnswersForMode) : 0;
                return scaledCallCost(scalingFactor, new ReasonerPlanner.CallMode(conj, Collections.intersection(conjVariables, resolvableMode)));
            }).reduce(0.0, Double::sum);
        } else {
            double answersForModeFromPrefix = scaledEstimator.answerEstimate(restrictedResolvableMode);
            double allAnswersForMode = answerCountEstimator.localEstimate(conjunctionNode.conjunction(), resolvable, restrictedResolvableMode);
            double scalingFactor = allAnswersForMode != 0 ? Math.min(1, answersForModeFromPrefix / allAnswersForMode) : 0;
            double cost = 0.0;
            if (resolvable.isRetrievable()) {
                cost += scalingFactor * retrievalCost(conjunctionNode.conjunction(), resolvable, resolvableMode);
            } else if (resolvable.isConcludable()) {
                cost += scalingFactor * retrievalCost(conjunctionNode.conjunction(), resolvable, resolvableMode);
                Set<ReasonerPlanner.CallMode> acyclicCalls = planner.triggeredCalls(resolvable.asConcludable(), resolvableMode, conjunctionNode.acyclicDependencies(resolvable.asConcludable()));
                cost += iterate(acyclicCalls).map(acylcicCall -> scaledCallCost(scalingFactor, acylcicCall)).reduce(0.0, Double::sum);    // Assumption: Decompose the global planning problems to SCCs
            } else throw TypeDBException.of(ILLEGAL_STATE);
            return cost;
        }
    }

    private double scaledCallCost(double scalingFactor, ReasonerPlanner.CallMode callMode) {
        ReasonerPlanner.Plan plan = planner.getPlan(callMode);
        return plan.allCallsCost() * Math.min(1.0, scalingFactor + plan.cyclicScalingFactor);
    }

    public class SingleCallCostingBuilder {
        private final ReasonerPlanner.CallMode callMode;
        private final ConjunctionGraph.ConjunctionNode conjunctionNode;
        private final AnswerCountEstimator.IncrementalEstimator estimator;

        private final List<Resolvable<?>> ordering;
        private final Set<Variable> boundVars;
        private final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        private double singlyBoundCost;

        private boolean finalised;

        SingleCallCostingBuilder(ReasonerPlanner.CallMode callMode) {
            this.callMode = callMode;
            this.conjunctionNode = conjunctionGraph.conjunctionNode(callMode.conjunction);
            this.estimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction(), callMode.mode);

            this.ordering = new ArrayList<>();
            this.boundVars = new HashSet<>(callMode.mode);
            this.cyclicConcludableModes = new HashSet<>();
            this.singlyBoundCost = 0.0;
            this.finalised = false;
        }

        void extend(Resolvable<?> resolvable) {
            assert !finalised;
            ordering.add(resolvable);
            Set<Variable> resolvableVars = estimateableVariables(resolvable.variables());
            Set<Variable> resolvableMode = Collections.intersection(resolvableVars, boundVars);
            double resolvableCost = scaledAcyclicCost(conjunctionNode, estimator, resolvable, resolvableMode, resolvableMode);

            if (resolvable.isConcludable() && conjunctionNode.cyclicConcludables().contains(resolvable.asConcludable())) {
                cyclicConcludableModes.add(new Pair<>(resolvable.asConcludable(), resolvableMode));
            }

            estimator.extend(resolvable);
            singlyBoundCost += resolvableCost;

            if (!resolvable.isNegated()) {
                boundVars.addAll(resolvable.variables()); // We need non-estimateable variables too.
            }
        }

        LocalSingleCallCosting build() {
            assert !finalised;
            this.finalised = true;
            return new LocalSingleCallCosting(callMode, ordering, cyclicConcludableModes, singlyBoundCost);
        }
    }


    static class LocalSingleCallCosting {

        final ReasonerPlanner.CallMode callMode;
        final List<Resolvable<?>> ordering;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        final double singleCallAcyclicCost;

        LocalSingleCallCosting(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering, Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes, double singleCallAcyclicCost) {
            this.callMode = callMode;
            this.ordering = ordering;
            this.cyclicConcludableModes = cyclicConcludableModes;
            this.singleCallAcyclicCost = singleCallAcyclicCost;
        }
    }

    /**
     * Represents the all-calls cost excluding cycles, and records the local concludables that cause
     * cycles along with their call modes.
     * Also records the scaling factors of the local concludables and their call modesl
     */
    static class LocalAllCallsCosting {
        final ReasonerPlanner.CallMode callMode;
        final List<Resolvable<?>> ordering;
        final Set<Pair<Concludable, Set<Variable>>> cyclicModes;
        final Map<Concludable, Double> cyclicScalingFactors;
        final double allCallsConnectedAcyclicCost;
        final double allCallsDisconnectedAcyclicCost;
        final double answersToMode;

        private LocalAllCallsCosting(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering,
                                     Set<Pair<Concludable, Set<Variable>>> cyclicModes,
                                     Map<Concludable, Double> cyclicScalingFactors, double allCallsConnectedAcyclicCost, double unscalableCost, double answersToMode) {
            this.callMode = callMode;
            this.ordering = ordering;
            this.allCallsConnectedAcyclicCost = allCallsConnectedAcyclicCost;
            this.cyclicModes = cyclicModes;
            this.cyclicScalingFactors = cyclicScalingFactors;
            this.allCallsDisconnectedAcyclicCost = unscalableCost;
            this.answersToMode = answersToMode;
        }
    }
}
