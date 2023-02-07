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

public class OrderingCostEstimator {
    private final ReasonerPlanner planner;
    private final AnswerCountEstimator answerCountEstimator;
    private final ConjunctionGraph conjunctionGraph;

    public OrderingCostEstimator(ReasonerPlanner planner, AnswerCountEstimator answerCountEstimator, ConjunctionGraph conjunctionGraph) {
        this.planner = planner;
        this.answerCountEstimator = answerCountEstimator;
        this.conjunctionGraph = conjunctionGraph;
    }

    SingleCallEstimateBuilder createSingleCallSummaryBuilder(ReasonerPlanner.CallMode callMode) {
        return new SingleCallEstimateBuilder(callMode);
    }

    SingleCallSummary createSingleCallSummary(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering) {
        SingleCallEstimateBuilder estimator = createSingleCallSummaryBuilder(callMode);
        for (Resolvable<?> resolvable : ordering) {
            estimator.extend(resolvable);
        }
        return estimator.build();
    }

    OrderingChoice createOrderingChoice(SingleCallSummary summary) {
        ConjunctionGraph.ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(summary.callMode.conjunction);

        Set<Variable> boundVars = new HashSet<>(summary.callMode.mode);  // bound -> in input mode or restricted locally
        Set<Variable> restrictedVars = new HashSet<>(); // restricted -> Restricted by preceding resolvables
        Set<Variable> inputConnectedVars = new HashSet<>(summary.callMode.mode);
        double acyclicCost = 0.0;
        double unscalableCost = 0.0;

        Map<Concludable, Double> scalingFactors = new HashMap<>();
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
        for (Resolvable<?> resolvable : summary.ordering) {
            Set<Variable> resolvableVars = estimateableVariables(resolvable.variables());
            Set<Variable> resolvableMode = Collections.intersection(resolvableVars, boundVars);
            Set<Variable> restrictedResolvableVars = Collections.intersection(resolvableVars, restrictedVars);

            double answersForModeFromPrefix = estimator.answerEstimate(restrictedResolvableVars);
            double resolvableCost;
            if (resolvable.isNegated()) {
                resolvableCost = iterate(resolvable.asNegated().disjunction().conjunctions()).map(conj -> {
                    Set<Variable> conjVariables = estimateableVariables(conj.pattern().variables());
                    double allAnswersForMode = answerCountEstimator.estimateAnswers(conj, Collections.intersection(conjVariables, restrictedResolvableVars));
                    double scalingFactor = allAnswersForMode !=0 ? Math.min(1, answersForModeFromPrefix / allAnswersForMode) : 0;
                    return scaledCallCost(scalingFactor, new ReasonerPlanner.CallMode(conj, Collections.intersection(conjVariables, resolvableMode)));
                }).reduce(0.0, Double::sum);
            } else {
                double allAnswersForMode = answerCountEstimator.localEstimate(conjunctionNode.conjunction(), resolvable, restrictedResolvableVars);
                double scalingFactor = allAnswersForMode != 0 ? Math.min(1, answersForModeFromPrefix / allAnswersForMode) : 0;
                resolvableCost = scaledAcyclicCost(scalingFactor, conjunctionNode, resolvable, resolvableMode);
            }

            if (resolvable.isConcludable() && conjunctionNode.cyclicConcludables().contains(resolvable.asConcludable())) {
                // Question: Do we project onto all the restrictedVars, or only those not in the mode?
                // Including those in the mode leads to double-scaling. Excluding them leads to an overestimate if the bounds are not restrictive.
                // I lean towards the overestimate.
                Set<Variable> projectionVars = iterate(restrictedResolvableVars).filter(v -> !summary.callMode.mode.contains(v)).toSet();
                long allAnswersForUnrestrictedMode = answerCountEstimator.localEstimate(conjunctionNode.conjunction(), resolvable, resolvableMode);
                double cyclicScalingFactor = projectionVars.isEmpty() && allAnswersForUnrestrictedMode != 0 ? 0.0 :
                        (double) estimator.answerEstimate(projectionVars) / allAnswersForUnrestrictedMode;
                scalingFactors.put(resolvable.asConcludable(), cyclicScalingFactor);
            }

            estimator.extend(resolvable);

            boolean isConnectedToInput = !Collections.intersection(resolvableMode, inputConnectedVars).isEmpty();
            if (isConnectedToInput) {
                acyclicCost += resolvableCost;
            } else {
                unscalableCost += resolvableCost;
            }

            if (!resolvable.isNegated()) {
                boundVars.addAll(resolvableVars);
                restrictedVars.addAll(resolvableVars);
                if (isConnectedToInput) inputConnectedVars.addAll(resolvableVars);
            }
        }

        double answersToMode = answerCountEstimator.estimateAnswers(summary.callMode.conjunction, summary.callMode.mode);
        return new OrderingChoice(summary.callMode, summary.ordering, summary.cyclicConcludableModes, scalingFactors, acyclicCost, unscalableCost, answersToMode);
    }

    private long retrievalCost(ResolvableConjunction conjunction, Resolvable<?> resolvable, Set<Variable> mode) {
        // Inaccurate because retrievables traversals work differently.
        // Also inaccurate because it considers inferred answers for concludables? We could rename to computeLocalCost.
        AnswerCountEstimator.IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunction);
        estimator.extend(resolvable);
        return estimator.answerEstimate(estimateableVariables(resolvable.variables()));
    }

    private double scaledAcyclicCost(double scalingFactor, ConjunctionGraph.ConjunctionNode conjunctionNode, Resolvable<?> resolvable, Set<Variable> resolvableMode) {
        assert !resolvable.isNegated();
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

    private double scaledCallCost(double scalingFactor, ReasonerPlanner.CallMode callMode) {
        ReasonerPlanner.Plan plan = planner.getPlan(callMode);
        return plan.cost() * Math.min(1.0, scalingFactor + plan.cyclicScalingFactor);
    }

    class SingleCallEstimateBuilder {
        private final ReasonerPlanner.CallMode callMode;
        private final ConjunctionGraph.ConjunctionNode conjunctionNode;
        private final AnswerCountEstimator.IncrementalEstimator estimator;

        private final List<Resolvable<?>> ordering;
        private final Set<Variable> boundVars;
        private final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        private double singlyBoundCost;

        private boolean finalised;

        SingleCallEstimateBuilder(ReasonerPlanner.CallMode callMode) {
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
            double answersForModeFromPrefix = estimator.answerEstimate(resolvableMode);
            double resolvableCost;
            if (resolvable.isNegated()) {
                resolvableCost = iterate(resolvable.asNegated().disjunction().conjunctions()).map(conj -> {
                    Set<Variable> conjVariables = estimateableVariables(conj.pattern().variables());
                    double allAnswersForMode = answerCountEstimator.estimateAnswers(conj, Collections.intersection(conjVariables, resolvableMode));
                    double scalingFactor = allAnswersForMode !=0 ? Math.min(1, answersForModeFromPrefix / allAnswersForMode) : 0;
                    return scaledCallCost(scalingFactor, new ReasonerPlanner.CallMode(conj, Collections.intersection(conjVariables, resolvableMode)));
                }).reduce(0.0, Double::sum);
            } else {
                double allAnswersForMode = answerCountEstimator.localEstimate(callMode.conjunction, resolvable, resolvableMode);
                double scalingFactor = allAnswersForMode != 0 ? Math.min(1, answersForModeFromPrefix / allAnswersForMode) : 0;
                resolvableCost = scaledAcyclicCost(scalingFactor, conjunctionNode, resolvable, resolvableMode);
            }

            if (resolvable.isConcludable() && conjunctionNode.cyclicConcludables().contains(resolvable.asConcludable())) {
                cyclicConcludableModes.add(new Pair<>(resolvable.asConcludable(), resolvableMode));
            }

            estimator.extend(resolvable);
            singlyBoundCost += resolvableCost;

            if (!resolvable.isNegated()) {
                boundVars.addAll(resolvableVars);
            }
        }

        SingleCallSummary build() {
            assert !finalised;
            this.finalised = true;
            return new SingleCallSummary(callMode, ordering, cyclicConcludableModes, singlyBoundCost);
        }
    }

    static class SingleCallSummary {
        // Created for every resolvable-ordering of a CallMode
        final ReasonerPlanner.CallMode callMode;
        final List<Resolvable<?>> ordering;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        final double singlyBoundCost;

        SingleCallSummary(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering, Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes, double singlyBoundCost) {
            this.callMode = callMode;
            this.ordering = ordering;
            this.cyclicConcludableModes = cyclicConcludableModes;
            this.singlyBoundCost = singlyBoundCost;
        }
    }

    static class OrderingChoice {
        // Created for one ordering per (CallMode, CyclicConcludableModes) - the one with lowest SinglyBoundCost.
        final ReasonerPlanner.CallMode callMode;
        final List<Resolvable<?>> ordering;
        final double acyclicCost;
        final Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes;
        final Map<Concludable, Double> scalingFactors;
        final double unscalableCost;
        final double answersToMode;

        private OrderingChoice(ReasonerPlanner.CallMode callMode, List<Resolvable<?>> ordering,
                               Set<Pair<Concludable, Set<Variable>>> cyclicConcludableModes,
                               Map<Concludable, Double> scalingFactors, double acyclicCost, double unscalableCost, double answersToMode) {
            this.callMode = callMode;
            this.ordering = ordering;
            this.acyclicCost = acyclicCost;
            this.cyclicConcludableModes = cyclicConcludableModes;
            this.scalingFactors = scalingFactors;
            this.unscalableCost = unscalableCost;
            this.answersToMode = answersToMode;
        }
    }
}
