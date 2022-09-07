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

import com.vaticle.typedb.core.common.exception.TypeDBException;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class CostEstimator {
    private final LogicManager logicMgr;
    private final Map<ResolvableConjunction, ConjunctionAnswerCountEstimator> costEstimators;

    // Cycle handling
    private final ArrayList<ResolvableConjunction> initializationStack;
    private final Set<ResolvableConjunction> cycleHeads;
    private final Set<ResolvableConjunction> toReset;

    public CostEstimator(LogicManager logicMgr) {
        this.logicMgr = logicMgr;
        this.costEstimators = new HashMap<>();
        this.initializationStack = new ArrayList<>();
        this.cycleHeads = new HashSet<>();
        this.toReset = new HashSet<>();
    }

    public long estimateAllAnswers(ResolvableConjunction conjunction) {
        registerConjunction(conjunction);
        return costEstimators.get(conjunction).estimateAllAnswers();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variablesOfInterest) {
        return estimateAnswers(conjunction, variablesOfInterest, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variablesOfInterest, Set<Resolvable<?>> includedResolvables) {
        registerConjunction(conjunction);
        return costEstimators.get(conjunction).estimateAnswers(variablesOfInterest, includedResolvables);
    }

    private void registerConjunction(ResolvableConjunction conjunction) {
        if (!costEstimators.containsKey(conjunction)) {
            costEstimators.put(conjunction, new ConjunctionAnswerCountEstimator(this, conjunction));
        }

        if (costEstimators.get(conjunction).mayInitialize()) {
            initializationStack.add(conjunction);
            costEstimators.get(conjunction).initializeOnce();
            assert initializationStack.get(initializationStack.size()-1) == conjunction;
            initializationStack.remove(initializationStack.size()-1);

            // In cyclic paths
            if (cycleHeads.size() == 1 && cycleHeads.contains(conjunction)) {
                // We can reset and recompute all!
                for (ResolvableConjunction conjunctionToReset: toReset) {
                    costEstimators.get(conjunctionToReset).reset();
                }
                costEstimators.get(conjunction).initializeOnce(); // no loop needed. This is the maximal cycle.
            }
        }
    }

    public void reportInitializationCycle(ResolvableConjunction conjunction) {
        cycleHeads.add(conjunction);
        for (int i=initializationStack.size()-1; initializationStack.get(i) != conjunction ; i--) {
            toReset.add(initializationStack.get(i));
        }
    }

    private static class ConjunctionAnswerCountEstimator {

        private final ResolvableConjunction conjunction;
        private final CostEstimator costEstimator;
        private final LogicManager logicMgr;
        private Map<Variable, CostCover> unaryCostCover;
        private Map<Resolvable<?>, List<LocalEstimate>> retrievableEstimates;
        private Map<Resolvable<?>, List<LocalEstimate>> inferrableEstimates;
        private long fullAnswerCount;
        private long negatedsCost;

        public boolean mayInitialize() {
            return initializationStatus == InitializationStatus.NOT_STARTED || initializationStatus == InitializationStatus.RESET;
        }

        private enum InitializationStatus {NOT_STARTED, RESET, IN_PROGRESS, COMPLETE};
        private InitializationStatus initializationStatus;

        public ConjunctionAnswerCountEstimator(CostEstimator costEstimator, ResolvableConjunction conjunction) {
            this.costEstimator = costEstimator;
            this.logicMgr = costEstimator.logicMgr;
            this.conjunction = conjunction;
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
            this.initializationStatus = InitializationStatus.NOT_STARTED;
        }

        private void reset() {
            assert this.initializationStatus == InitializationStatus.COMPLETE;
            this.initializationStatus = InitializationStatus.RESET;
            this.inferrableEstimates = null;
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
        }

        public long estimateAllAnswers() {
            assert this.fullAnswerCount >= 0;
            return this.fullAnswerCount;
        }

        public long estimateAnswers(Set<Variable> projectToVariables, Set<Resolvable<?>> includedResolvables) {
            if (this.initializationStatus == InitializationStatus.IN_PROGRESS) {
                costEstimator.reportInitializationCycle(this.conjunction);
            }

            List<LocalEstimate> enabledEstimates = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(multivarEstimate(resolvable)))
                    .toList();
            Map<Variable, CostCover> costCover = computeCostCover(enabledEstimates, projectToVariables);
            long ret = CostCover.costToCover(projectToVariables, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        private Map<Variable, CostCover> computeCostCover(List<LocalEstimate> enabledEstimates, Set<Variable> projectToVariables) {
            // Does a greedy set cover
            Map<Variable, CostCover> costCover = new HashMap<>(unaryCostCover);
            enabledEstimates.sort(Comparator.comparing(x -> x.answerEstimate(this, projectToVariables)));
            for (LocalEstimate enabledEstimate : enabledEstimates) {
                Set<Variable> interestingSubsetOfVariables = enabledEstimate.variables.stream()
                        .filter(projectToVariables::contains).collect(Collectors.toSet());

                long currentCostToCover = CostCover.costToCover(interestingSubsetOfVariables, costCover);
                if (currentCostToCover > enabledEstimate.answerEstimate(this, interestingSubsetOfVariables)) {
                    CostCover variablesNowCoveredBy = new CostCover(enabledEstimate.answerEstimate(this, interestingSubsetOfVariables));
                    interestingSubsetOfVariables.forEach(v -> costCover.put(v, variablesNowCoveredBy));
                }
            }
            return costCover;
        }

        private void registerTriggeredRules() {
            resolvables().filter(this::isInferrable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
                Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
                iterate(unifiers.keySet()).forEachRemaining(rule -> {
                    costEstimator.registerConjunction(rule.condition().conjunction());
                });
            });
        }

        private boolean isInferrable(Resolvable<?> resolvable) {
            return resolvable.isConcludable() &&
                    !logicMgr.applicableRules(resolvable.asConcludable()).isEmpty();
        }

        private FunctionalIterator<Resolvable<?>> resolvables() {
            return iterate(logicMgr.compile(conjunction));
        }

        private Set<Variable> allVariables() {
            return iterate(conjunction.pattern().variables())
                    .filter(v -> v.isThing())
                    .toSet();
        }

        private FunctionalIterator<Resolvable<?>> resolvablesGenerating(ThingVariable generatedVariable) {
            return resolvables()
                    .filter(resolvable -> isInferrable(resolvable) && resolvable.generating().isPresent() &&
                            resolvable.generating().get().equals(generatedVariable));
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

        private void initializeOnce() {
            if (!this.mayInitialize()) return;

            initializationStatus = InitializationStatus.IN_PROGRESS;
            // TODO: Move this after the retrievable estimates, so cycle-head estimates are easy.
            if (unaryCostCover == null) {
                unaryCostCover = new HashMap<>();
                // Create a baseline estimate from type information
                List<LocalEstimate> unaryEstimates = new ArrayList<>();
                iterate(allVariables()).forEachRemaining(v -> {
                    unaryEstimates.add(estimateFromTypes(v.asThing(), false));
                });

                unaryEstimates.sort(Comparator.comparing(x -> x.answerEstimate(this, new HashSet<>(x.variables))));
                for (LocalEstimate unaryEstimate : unaryEstimates) {
                    if (!unaryCostCover.containsKey(unaryEstimate.variables.get(0))) {
                        unaryCostCover.put(unaryEstimate.variables.get(0), new CostCover(unaryEstimate.answerEstimate(this, new HashSet<>(unaryEstimate.variables))));
                    }
                }
            }

            if (retrievableEstimates == null) {
                retrievableEstimates = new HashMap<>();
                //TODO: How does traversal handle type Variables?
                resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                    retrievableEstimates.put(retrievable, new ArrayList<>());
                    Set<Concludable> concludablesInRetrievable = ResolvableConjunction.of(retrievable.pattern()).positiveConcludables();
                    iterate(concludablesInRetrievable).forEachRemaining(concludable -> {
                        retrievableEstimates.get(retrievable).addAll(computeEstimatesFromConcludable(concludable));
                    });
                });
            }

            registerTriggeredRules();

            if (inferrableEstimates == null) {
                HashMap<Resolvable<?>, List<LocalEstimate>> tempInferrableEstimates = new HashMap<>();
                iterate(resolvables()).filter(this::isInferrable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
                    tempInferrableEstimates.put(concludable, computeEstimatesFromConcludable(concludable));
                });
                inferrableEstimates = tempInferrableEstimates;

                // We also have to recompute the unaryCostCover now.
                unaryCostCover = new HashMap<>();
                // Create a baseline estimate from type information
                List<LocalEstimate> unaryEstimates = new ArrayList<>();
                iterate(allVariables()).forEachRemaining(v -> {
                    unaryEstimates.add(estimateFromTypes(v.asThing(), true));
                });

                unaryEstimates.sort(Comparator.comparing(x -> x.answerEstimate(this, new HashSet<>(x.variables))));
                for (LocalEstimate unaryEstimate : unaryEstimates) {
                    if (!unaryCostCover.containsKey(unaryEstimate.variables.get(0))) {
                        unaryCostCover.put(unaryEstimate.variables.get(0), new CostCover(unaryEstimate.answerEstimate(this, new HashSet<>(unaryEstimate.variables))));
                    }
                }
            }

            if (this.negatedsCost == -1) {
                this.negatedsCost = resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .map(negatedConjunction -> {
                            costEstimator.registerConjunction(negatedConjunction);
                            return costEstimator.estimateAllAnswers(negatedConjunction);
                        }).reduce(0L, Long::sum);
            }

            List<LocalEstimate> allEstimates = resolvables().flatMap(resolvable -> iterate(multivarEstimate(resolvable))).toList();
            Map<Variable, CostCover> fullCostCover = computeCostCover(allEstimates, allVariables());
            this.fullAnswerCount = CostCover.costToCover(allVariables(), fullCostCover) + this.negatedsCost;

            // Can we prune the multiVarEstimates stored?
            // Not if we want to order-resolvables. Else, yes based on queryableVariables.
            initializationStatus = InitializationStatus.COMPLETE;
        }

        private List<LocalEstimate> computeEstimatesFromConcludable(Concludable concludable) {
            if (concludable.isHas()) {
                return list(estimatesFromHasEdges(concludable.asHas()));
            } else if (concludable.isRelation()) {
                return list(estimatesFromRolePlayersAssumingEvenDistribution(concludable.asRelation()));
            } else {
                return list();
            }
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
                AtomicLong nInferred = new AtomicLong(0L);
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
                        return costEstimator.estimateAnswers(rule.condition().conjunction(), answerVariables);

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

        private long estimateInferredAnswerCount(Concludable concludable, Set<Variable> variablesOfInterest) {
            Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
            long inferredEstimate = 0;
            for (Rule rule : unifiers.keySet()) {
                for (Unifier unifier : unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variablesOfInterest)
                            .flatMap(v -> iterate(unifier.mapping().get(v.id()))).toSet();
                    Set<Variable> ruleSideVariables = iterate(ruleSideIds).map(id -> rule.condition().conjunction().pattern().variable(id))
                            .toSet();
                    inferredEstimate += costEstimator.estimateAnswers(rule.condition().conjunction(), ruleSideVariables);
                }
            }
            return inferredEstimate;
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

        private static abstract class LocalEstimate {

            final List<Variable> variables;

            private LocalEstimate(List<Variable> variables) {
                this.variables = variables;
            }

            abstract long answerEstimate(ConjunctionAnswerCountEstimator costEstimator, Set<Variable> variablesOfInterest);

            private static class SimpleEstimate extends LocalEstimate {
                private final long staticEstimate;

                private SimpleEstimate(List<Variable> variables, long estimate) {
                    super(variables);
                    this.staticEstimate = estimate;
                }

                @Override
                long answerEstimate(ConjunctionAnswerCountEstimator costEstimator, Set<Variable> variablesOfInterest) {
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
                long answerEstimate(ConjunctionAnswerCountEstimator costEstimator, Set<Variable> variablesOfInterest) {
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

        private static class CostCover {
            private final long cost;

            private CostCover(long cost) {
                this.cost = cost;
            }

            private static long costToCover(Collection<Variable> subset, Map<Variable, CostCover> coverMap) {
                Set<CostCover> subsetCoveredBy = coverMap.keySet().stream().filter(subset::contains)
                        .map(coverMap::get).collect(Collectors.toSet());
                return subsetCoveredBy.stream().map(cc -> cc.cost).reduce(1L, (x, y) -> x * y);
            }
        }
    }
}
