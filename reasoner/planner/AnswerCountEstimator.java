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
 */
package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class AnswerCountEstimator {
    private final LogicManager logicMgr;
    private final Map<ResolvableConjunction, ConjunctionAnswerCountEstimator> estimators;

    // Cycle handling
    private final ArrayList<ResolvableConjunction> initializationStack;
    private final Set<ResolvableConjunction> cycleHeads;
    private final Set<ResolvableConjunction> toReset;

    public AnswerCountEstimator(LogicManager logicMgr) {
        this.logicMgr = logicMgr;
        this.estimators = new HashMap<>();
        this.initializationStack = new ArrayList<>();
        this.cycleHeads = new HashSet<>();
        this.toReset = new HashSet<>();
    }

    public long estimateAllAnswers(ResolvableConjunction conjunction) {
        registerConjunctionAndInitialize(conjunction);
        return estimators.get(conjunction).estimateAllAnswers();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter) {
        return estimateAnswers(conjunction, variableFilter, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
        registerConjunctionAndInitialize(conjunction);
        return estimators.get(conjunction).estimateAnswers(variableFilter, includedResolvables);
    }

    private void registerConjunctionAndInitialize(ResolvableConjunction conjunction) {
        if (!estimators.containsKey(conjunction)) {
            estimators.put(conjunction, new ConjunctionAnswerCountEstimator(this, conjunction));
        }

        if (estimators.get(conjunction).mayInitialize()) {
            initializationStack.add(conjunction);
            estimators.get(conjunction).initialize();
            assert initializationStack.get(initializationStack.size()-1) == conjunction;
            initializationStack.remove(initializationStack.size()-1);

            // In cyclic paths
            if (cycleHeads.size() == 1 && cycleHeads.contains(conjunction)) {
                // We can reset and recompute all!
                for (ResolvableConjunction conjunctionToReset: toReset) {
                    estimators.get(conjunctionToReset).reset();
                }
                estimators.get(conjunction).initialize(); // no loop needed. This is the maximal cycle.
            }
        }
    }

    private void reportInitializationCycle(ResolvableConjunction conjunction) {
        cycleHeads.add(conjunction);
        for (int i=initializationStack.size()-1; initializationStack.get(i) != conjunction ; i--) {
            toReset.add(initializationStack.get(i));
        }
    }

    private static class ConjunctionAnswerCountEstimator {

        private final ResolvableConjunction conjunction;
        private final AnswerCountEstimator answerCountEstimator;
        private final LogicManager logicMgr;
        private Map<Variable, LocalEstimate> unaryEstimateCover;
        private Map<Resolvable<?>, List<LocalEstimate>> retrievableEstimates;
        private Map<Resolvable<?>, List<LocalEstimate>> inferrableEstimates;
        private long fullAnswerCount;
        private long negatedsCost;


        private enum InitializationStatus {NOT_STARTED, RESET, IN_PROGRESS, COMPLETE}
        private InitializationStatus initializationStatus;

        public ConjunctionAnswerCountEstimator(AnswerCountEstimator answerCountEstimator, ResolvableConjunction conjunction) {
            this.answerCountEstimator = answerCountEstimator;
            this.logicMgr = answerCountEstimator.logicMgr;
            this.conjunction = conjunction;
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
            this.initializationStatus = InitializationStatus.NOT_STARTED;
        }

        // <editor-fold desc="utils">
        private FunctionalIterator<Resolvable<?>> resolvables() {
            return iterate(logicMgr.compile(conjunction));
        }

        private boolean isInferrable(Resolvable<?> resolvable) {
            return resolvable.isConcludable() &&
                    !logicMgr.applicableRules(resolvable.asConcludable()).isEmpty();
        }

        private FunctionalIterator<Resolvable<?>> resolvablesGenerating(ThingVariable generatedVariable) {
            return resolvables()
                    .filter(resolvable -> isInferrable(resolvable) && resolvable.generating().isPresent() &&
                            resolvable.generating().get().equals(generatedVariable));
        }

        private Set<Variable> allVariables() {
            return iterate(conjunction.pattern().variables())
                    .filter(v -> v.isThing())
                    .toSet();
        }

        private List<LocalEstimate> multivarEstimate(Resolvable<?> resolvable){
            assert resolvables().toSet().contains(resolvable);
            if (resolvable.isNegated()) return new ArrayList<>();
            if (retrievableEstimates.containsKey(resolvable)) return retrievableEstimates.get(resolvable);
            else if (inferrableEstimates == null) {
                assert this.initializationStatus == InitializationStatus.IN_PROGRESS || this.initializationStatus == InitializationStatus.RESET;
                return new ArrayList<>();
            } else {
                return inferrableEstimates.get(resolvable);
            }
        }
        // </editor-fold>

        // <editor-fold desc="query">
        public long estimateAllAnswers() {
            assert this.fullAnswerCount >= 0;
            return this.fullAnswerCount;
        }

        public long estimateAnswers(Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
            if (this.initializationStatus == InitializationStatus.IN_PROGRESS) {
                answerCountEstimator.reportInitializationCycle(this.conjunction);
            }

            List<LocalEstimate> includedEstimates = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(multivarEstimate(resolvable)))
                    .toList();
            Map<Variable, LocalEstimate> costCover = computeCheapestEstimateCoverForVariables(variableFilter, includedEstimates);
            long ret = costOfEstimateCover(variableFilter, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        private Map<Variable, LocalEstimate> computeCheapestEstimateCoverForVariables(Set<Variable> variableFilter, List<LocalEstimate> includedEstimates) {
            // Does a greedy set cover
            Map<Variable, LocalEstimate> costCover = new HashMap<>(unaryEstimateCover);
            includedEstimates.sort(Comparator.comparing(x -> x.answerEstimate(variableFilter)));
            for (LocalEstimate estimate : includedEstimates) {
                Set<Variable> filteredVariablesInEstimate = estimate.variables.stream()
                        .filter(variableFilter::contains).collect(Collectors.toSet());

                long currentCostToCover = costOfEstimateCover(filteredVariablesInEstimate, costCover);
                if (currentCostToCover > estimate.answerEstimate(filteredVariablesInEstimate)) {
                    filteredVariablesInEstimate.forEach(v -> costCover.put(v, estimate));
                }
            }
            return costCover;
        }

        private static long costOfEstimateCover(Set<Variable> variablesToConsider, Map<Variable, LocalEstimate> coverMap) {
            Set<LocalEstimate> subsetCoveredBy = coverMap.keySet().stream().filter(variablesToConsider::contains)
                    .map(coverMap::get).collect(Collectors.toSet());
            return subsetCoveredBy.stream().map(estimate -> estimate.answerEstimate(variablesToConsider)).reduce(1L, (x, y) -> x * y);
        }
        // </editor-fold>

        // <editor-fold desc="initialization, recursive initialization & cycle-handling">
        public boolean mayInitialize() {
            return initializationStatus == InitializationStatus.NOT_STARTED || initializationStatus == InitializationStatus.RESET;
        }

        private void reset() {
            assert this.initializationStatus == InitializationStatus.COMPLETE;
            this.initializationStatus = InitializationStatus.RESET;
            this.inferrableEstimates = null;
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
        }

        private void registerTriggeredRules() {
            resolvables().filter(this::isInferrable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
                Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
                iterate(unifiers.keySet()).forEachRemaining(rule -> {
                    answerCountEstimator.registerConjunctionAndInitialize(rule.condition().conjunction());
                });
            });
        }

        private void initialize() {
            if (!this.mayInitialize()) return;
            //TODO: How does traversal handle type Variables?

            initializationStatus = InitializationStatus.IN_PROGRESS;
            // Create a baseline estimate from type information
            if (unaryEstimateCover == null) unaryEstimateCover = computeUnaryEstimateCover(false);

            if (retrievableEstimates == null) retrievableEstimates = deriveEstimatesFromRetrievables();

            registerTriggeredRules();

            if (inferrableEstimates == null) {
                inferrableEstimates = deriveEstimatesFromInferrables();
                unaryEstimateCover = computeUnaryEstimateCover(true); // recompute with inferred
            }

            if (this.negatedsCost == -1) this.negatedsCost = computeNegatedsCost();

            List<LocalEstimate> allEstimates = resolvables().flatMap(resolvable -> iterate(multivarEstimate(resolvable))).toList();
            Map<Variable, LocalEstimate> fullCostCover = computeCheapestEstimateCoverForVariables(allVariables(), allEstimates);
            this.fullAnswerCount = costOfEstimateCover(allVariables(), fullCostCover) + this.negatedsCost;

            // Can we prune the multiVarEstimates stored?
            // Not if we want to order-resolvables. Else, yes based on queryableVariables.
            initializationStatus = InitializationStatus.COMPLETE;
        }
        // </editor-fold>

        // <editor-fold desc="Derive LocalEstimate objects from conjunction ">
        private Map<Variable, LocalEstimate> computeUnaryEstimateCover(boolean considerInferrable) {
            Map<Variable, LocalEstimate> unaryEstimateCover =new HashMap<>();
            List<LocalEstimate> unaryEstimates = new ArrayList<>();
            iterate(allVariables()).forEachRemaining(v -> {
                unaryEstimates.add(estimateFromTypes(v.asThing(), considerInferrable));
            });

            unaryEstimates.sort(Comparator.comparing(x -> x.answerEstimate(new HashSet<>(x.variables))));
            for (LocalEstimate unaryEstimate : unaryEstimates) {
                unaryEstimateCover.putIfAbsent(unaryEstimate.variables.get(0), unaryEstimate);
            }

            return unaryEstimateCover;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> deriveEstimatesFromRetrievables() {
            Map<Resolvable<?>, List<LocalEstimate>> retrievableEstimates = new HashMap<>();
            resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                retrievableEstimates.put(retrievable, new ArrayList<>());
                Set<Concludable> concludablesInRetrievable = ResolvableConjunction.of(retrievable.pattern()).positiveConcludables();
                iterate(concludablesInRetrievable).forEachRemaining(concludable -> {
                    retrievableEstimates.get(retrievable).addAll(deriveEstimatesFromConcludable(concludable));
                });
            });

            return retrievableEstimates;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> deriveEstimatesFromInferrables() {
            Map<Resolvable<?>, List<LocalEstimate>> inferrableEstimates = new HashMap<>();
            iterate(resolvables()).filter(this::isInferrable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
                inferrableEstimates.put(concludable, deriveEstimatesFromConcludable(concludable));
            });
            return inferrableEstimates;
        }

        private List<LocalEstimate> deriveEstimatesFromConcludable(Concludable concludable) {
            if (concludable.isHas()) {
                return list(estimatesFromHasEdges(concludable.asHas()));
            } else if (concludable.isRelation()) {
                return list(estimatesFromRolePlayersAssumingEvenDistribution(concludable.asRelation()));
            } else {
                return list();
            }
        }

        private long computeNegatedsCost() {
            return resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                    .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .map(negatedConjunction -> {
                        answerCountEstimator.registerConjunctionAndInitialize(negatedConjunction);
                        return answerCountEstimator.estimateAllAnswers(negatedConjunction);
                    }).reduce(0L, Long::sum);
        }
        // </editor-fold>

        // <editor-fold desc="Answer estimate computation">
        private long estimateInferredAnswerCount(Concludable concludable, Set<Variable> variableFilter) {
            Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
            long inferredEstimate = 0;
            for (Rule rule : unifiers.keySet()) {
                for (Unifier unifier : unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variableFilter)
                            .flatMap(v -> iterate(unifier.mapping().get(v.id()))).toSet();
                    Set<Variable> ruleSideVariables = iterate(ruleSideIds).map(id -> rule.condition().conjunction().pattern().variable(id))
                            .toSet();
                    inferredEstimate += answerCountEstimator.estimateAnswers(rule.condition().conjunction(), ruleSideVariables);
                }
            }
            return inferredEstimate;
        }

        private long computeInferredUnaryEstimates(ThingVariable generatedVariable) {
            List<Concludable> relevantConcludables = resolvablesGenerating(generatedVariable)
                    .map(Resolvable::asConcludable)
                    .toList();

            if (relevantConcludables.isEmpty()) {
                return 0L;
            }

            long mostConstrainedAnswer = Long.MAX_VALUE;
            for (Concludable concludable : relevantConcludables) {
                Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
                long answersGeneratedByThisResolvable = iterate(unifiers.keySet()).map(rule -> {
                    Optional<Unifier> firstUnifier = unifiers.get(rule).stream().findFirst();
                    // Increment the estimates for variables generated by the rule
                    assert rule.conclusion().generating().isPresent() && firstUnifier.isPresent();

                    if (rule.conclusion().isRelation()) { // Is a relation
                        Set<Variable> answerVariables = iterate(firstUnifier.get().mapping().values()).flatMap(Iterators::iterate)
                                .map(v -> rule.conclusion().pattern().variable(v))
                                .toSet();
                        // The answers to the generated variable = answers to all variables
                        if (answerVariables.contains(rule.conclusion().generating().get())) {
                            answerVariables = new HashSet<>(rule.conclusion().pattern().variables());
                            answerVariables.remove(rule.conclusion().generating().get());
                        }
                        return answerCountEstimator.estimateAnswers(rule.condition().conjunction(), answerVariables);

                    } else if (rule.conclusion().isExplicitHas()) {
                        return 1L;
                    } else {
                        assert rule.conclusion().isVariableHas();
                        return 0L;
                    }
                }).reduce(0L, Long::sum);

                mostConstrainedAnswer = Math.min(mostConstrainedAnswer, answersGeneratedByThisResolvable);
            }
            assert mostConstrainedAnswer != Long.MAX_VALUE;
            return mostConstrainedAnswer;
        }

        private LocalEstimate estimateFromTypes(ThingVariable var, boolean considerInferrable) {
            long estimate;
            if (var.iid().isPresent()) {
                estimate = 1;
            }
            else {
                estimate = countPersistedThingsMatchingType(var.asThing());
                estimate += considerInferrable ? computeInferredUnaryEstimates(var.asThing()) : 0;
                if (estimate == 0 && resolvablesGenerating(var.asThing()).hasNext()) estimate = 1; // When it's purely inferrable
            }

            return new LocalEstimate.SimpleEstimate(list(var), estimate);
        }

        private LocalEstimate estimatesFromHasEdges(Concludable.Has concludableHas) {

            long estimate = countPersistedHasEdges(concludableHas.asHas().owner().inferredTypes(), concludableHas.asHas().attribute().inferredTypes()) +
                    estimateInferredAnswerCount(concludableHas, set(concludableHas.asHas().owner(), concludableHas.asHas().attribute()));

            return new LocalEstimate.SimpleEstimate(list(concludableHas.asHas().owner(), concludableHas.asHas().attribute()), estimate);
        }

        private LocalEstimate estimatesFromRolePlayersAssumingEvenDistribution(Concludable.Relation concludable) {
            // Small inaccuracy: We double count duplicate roles (r:$a, r:$b)
            // counts the case where r:$a=r:$b, which TypeDB wouldn't return
            Set<RelationConstraint.RolePlayer> rolePlayers = concludable.relation().players();
            List<Variable> constrainedVars = new ArrayList<>();
            Map<TypeVariable, Long> rolePlayerEstimates = new HashMap<>();
            Map<TypeVariable, Integer> rolePlayerCounts = new HashMap<>();

            double relationTypeEstimate = countPersistedThingsMatchingType(concludable.relation().owner());
            constrainedVars.add(concludable.relation().owner());
            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                constrainedVars.add(rp.player());
                TypeVariable key = rp.roleType().orElse(null);
                rolePlayerCounts.put(key, rolePlayerCounts.getOrDefault(key, 0) + 1);
                if (!rolePlayerEstimates.containsKey(key)) {
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp));
                }
            }

            // TODO: Can improve estimate by collecting List<List<LocalEstimate>> from the triggered rules and doign sum(costCover).
            long inferredRelationsEstimate = estimateInferredAnswerCount(concludable, new HashSet<>(constrainedVars));
            return new LocalEstimate.CoPlayerEstimate(constrainedVars, relationTypeEstimate, rolePlayerEstimates, rolePlayerCounts, inferredRelationsEstimate);

        }
        // </editor-fold>

        // <editor-fold desc="stats">
        private long countPersistedRolePlayers(RelationConstraint.RolePlayer rolePlayer) {
            return logicMgr.graph().data().stats().thingVertexSum(rolePlayer.inferredRoleTypes());
        }

        private long countPersistedThingsMatchingType(ThingVariable thingVar) {
            return logicMgr.graph().data().stats().thingVertexSum(thingVar.inferredTypes());
        }

        private long countPersistedHasEdges(Set<Label> ownerTypes, Set<Label> attributeTypes) {
            return ownerTypes.stream().map(ownerType ->
                    attributeTypes.stream()
                            .map(attrType -> logicMgr.graph().data().stats().hasEdgeCount(ownerType, attrType))
                            .reduce(0L, Long::sum)
            ).reduce(0L, Long::sum);
        }
        // </editor-fold>

        private static abstract class LocalEstimate {

            final List<Variable> variables;

            private LocalEstimate(List<Variable> variables) {
                this.variables = variables;
            }

            abstract long answerEstimate(Set<Variable> variableFilter);

            private static class SimpleEstimate extends LocalEstimate {
                private final long staticEstimate;

                private SimpleEstimate(List<Variable> variables, long estimate) {
                    super(variables);
                    this.staticEstimate = estimate;
                }

                @Override
                long answerEstimate(Set<Variable> variableFilter) {
                    return staticEstimate;
                }
            }

            public static class CoPlayerEstimate extends LocalEstimate {

                private final Map<TypeVariable, Integer> rolePlayerCounts;
                private final Map<TypeVariable, Long> rolePlayerEstimates;
                private final double relationTypeEstimate;
                private final long inferredRelationEstimate;

                public CoPlayerEstimate(List<Variable> variables, double relationTypeEstimate,
                                        Map<TypeVariable, Long> rolePlayerEstimates, Map<TypeVariable, Integer> rolePlayerCounts,
                                        long inferredRelationEstimate) {
                    super(variables);
                    this.relationTypeEstimate = relationTypeEstimate;
                    this.rolePlayerEstimates = rolePlayerEstimates;
                    this.rolePlayerCounts = rolePlayerCounts;
                    this.inferredRelationEstimate = inferredRelationEstimate;
                }

                @Override
                long answerEstimate(Set<Variable> variableFilter) {
                    long singleRelationEstimate = 1L;
                    for (TypeVariable key : rolePlayerCounts.keySet()) {
                        assert rolePlayerEstimates.containsKey(key);
                        long avgRolePlayers = Double.valueOf(Math.ceil(rolePlayerEstimates.get(key) / relationTypeEstimate)).longValue();
                        singleRelationEstimate *= nPermuteKforSmallK(avgRolePlayers, rolePlayerCounts.get(key));
                    }

                    long fullEstimate = Double.valueOf(Math.ceil(relationTypeEstimate * singleRelationEstimate)).longValue() + inferredRelationEstimate;
                    return fullEstimate;
                }

                private long nPermuteKforSmallK(long n, long k) {
                    long ans = 1;
                    for (int i = 0; i < k; i++) ans *= n - i;
                    return ans;
                }
            }
        }
    }
}
