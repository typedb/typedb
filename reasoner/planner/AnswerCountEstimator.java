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
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class AnswerCountEstimator {
    private final LogicManager logicMgr;
    private final Map<ResolvableConjunction, ConjunctionAnswerCountEstimator> estimators;

    // Cycle handling
    private final ArrayList<ResolvableConjunction> initializationStack;
    private final AnswerCountModel answerCountModel;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph) {
        this.logicMgr = logicMgr;
        this.answerCountModel = new AnswerCountModel(this, graph);
        this.estimators = new HashMap<>();
        this.initializationStack = new ArrayList<>();
    }

    public void registerAndInitializeConjunction(ResolvableConjunction conjunction) {
        registerConjunction(conjunction);
        initializeConjunction(conjunction);
    }

    private boolean registerConjunction(ResolvableConjunction conjunction) {
        if (!estimators.containsKey(conjunction)) {
            estimators.put(conjunction, new ConjunctionAnswerCountEstimator(this, conjunction));
        }

        // TODO: Improve cycle-detection using caching to avoid re-traversing the graph?
        if (initializationStack.contains(conjunction)) return true;

        initializationStack.add(conjunction);
        boolean onCycle = estimators.get(conjunction).registerDependencies();
        initializationStack.remove(initializationStack.size() - 1);
        return onCycle;
    }

    private void initializeConjunction(ResolvableConjunction conjunction) {
        estimators.get(conjunction).initialize();
    }

    public long estimateAllAnswers(ResolvableConjunction conjunction) {
        return estimators.get(conjunction).estimateAllAnswers();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter) {
        return estimateAnswers(conjunction, variableFilter, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
        return estimators.get(conjunction).estimateAnswers(variableFilter, includedResolvables);
    }

    private static class ConjunctionAnswerCountEstimator {

        private final ResolvableConjunction conjunction;
        private final AnswerCountEstimator answerCountEstimator;
        private final LogicManager logicMgr;
        private final AnswerCountModel answerCountModel;

        private Map<Variable, LocalEstimate> unaryEstimateCover;
        private long fullAnswerCount;
        private long negatedsCost;

        private Set<Concludable> cyclicConcludables;
        private Set<Concludable> acyclicConcludables;
        private final HashMap<Resolvable<?>, List<LocalEstimate>> estimatesFromResolvable;

        private enum InitializationStatus {NOT_STARTED, REGISTERED, ACYCLIC_ESTIMATES, COMPLETE}

        private InitializationStatus initializationStatus;

        public ConjunctionAnswerCountEstimator(AnswerCountEstimator answerCountEstimator, ResolvableConjunction conjunction) {
            this.answerCountEstimator = answerCountEstimator;
            this.answerCountModel = answerCountEstimator.answerCountModel;
            this.logicMgr = answerCountEstimator.logicMgr;
            this.conjunction = conjunction;

            this.estimatesFromResolvable = new HashMap<>();
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
            this.initializationStatus = InitializationStatus.NOT_STARTED;
        }

        private FunctionalIterator<Resolvable<?>> resolvables() {
            return iterate(logicMgr.compile(conjunction));
        }

        private Set<Variable> allVariables() {
            return iterate(conjunction.pattern().variables())
                    .filter(Variable::isThing)
                    .toSet();
        }

        public long estimateAllAnswers() {
            assert this.initializationStatus == InitializationStatus.COMPLETE && this.fullAnswerCount >= 0;
            return this.fullAnswerCount;
        }

        public long estimateAnswers(Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
            assert this.initializationStatus == InitializationStatus.ACYCLIC_ESTIMATES || this.initializationStatus == InitializationStatus.COMPLETE;
            List<LocalEstimate> includedEstimates = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(estimatesFromResolvable.get(resolvable)))
                    .toList();
            Map<Variable, LocalEstimate> costCover = computeGreedyEstimateCoverForVariables(variableFilter, includedEstimates);
            long ret = costOfEstimateCover(variableFilter, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        private Map<Variable, LocalEstimate> computeGreedyEstimateCoverForVariables(Set<Variable> variableFilter, List<LocalEstimate> includedEstimates) {
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

        private FunctionalIterator<ResolvableConjunction> dependencies(Concludable concludable) {
            return iterate(logicMgr.applicableRules(concludable).keySet())
                    .map(rule -> rule.condition().conjunction());
        }

        public boolean registerDependencies() {
            if (initializationStatus == InitializationStatus.NOT_STARTED) {
                cyclicConcludables = new HashSet<>();
                acyclicConcludables = new HashSet<>();
                resolvables().filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                        .forEachRemaining(concludable -> {
                            dependencies(concludable).forEachRemaining(dependency -> {
                                if (answerCountEstimator.registerConjunction(dependency)) {
                                    cyclicConcludables.add(concludable);
                                }
                            });
                            if (!cyclicConcludables.contains(concludable)) {
                                acyclicConcludables.add(concludable);
                            }
                        });

                resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .forEachRemaining(answerCountEstimator::registerConjunction);

                initializationStatus = InitializationStatus.REGISTERED;
            }
            return !cyclicConcludables.isEmpty();
        }

        private void initialize() {
            assert initializationStatus != InitializationStatus.NOT_STARTED;
            if (initializationStatus == InitializationStatus.REGISTERED) {
                // Acyclic estimates
                resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated-> iterate(negated.disjunction().conjunctions()))
                        .forEachRemaining(answerCountEstimator::initializeConjunction);
                iterate(acyclicConcludables).flatMap(this::dependencies).forEachRemaining(answerCountEstimator::initializeConjunction);
                initializeAcyclicEstimates();
                initializationStatus = InitializationStatus.ACYCLIC_ESTIMATES;

                // cyclic calls to this estimator will answer based on acyclic estimates.
                iterate(cyclicConcludables).flatMap(this::dependencies).forEachRemaining(answerCountEstimator::initializeConjunction);
                initializeCyclicEstimates();

                // Compute costs
                this.negatedsCost = resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .map(answerCountEstimator::estimateAllAnswers).reduce(0L, Long::sum);

                this.fullAnswerCount = estimateAnswers(allVariables(), resolvables().toSet()) + this.negatedsCost;

                initializationStatus = InitializationStatus.COMPLETE;
            }
        }

        private void initializeAcyclicEstimates() {
            this.estimatesFromResolvable.putAll(initializeEstimatesFromRetrievables());
            this.estimatesFromResolvable.putAll(initializeEstimatesFromNegations());
            this.estimatesFromResolvable.putAll(initializeEstimatesFromAcyclicConcludables());
            iterate(cyclicConcludables).forEachRemaining(concludable -> this.estimatesFromResolvable.put(concludable, list()));
            this.unaryEstimateCover = computeUnaryEstimateCover(new HashMap<>());
        }

        private void initializeCyclicEstimates() {
            Map<Resolvable<?>, List<LocalEstimate>> estimatesFromCyclicConcludables = initializeEstimatesFromCyclicConcludables();
            iterate(cyclicConcludables).forEachRemaining(concludable -> this.estimatesFromResolvable.remove(concludable));
            this.estimatesFromResolvable.putAll(estimatesFromCyclicConcludables);

            Map<Resolvable<?>, List<LocalEstimate>> unaryEstimates = initializeUnaryEstimatesWithInference();
            iterate(unaryEstimates.keySet()).forEachRemaining(resolvable -> {
                this.estimatesFromResolvable.put(resolvable, list(this.estimatesFromResolvable.get(resolvable), unaryEstimates.get(resolvable)));
            });
            this.unaryEstimateCover = computeUnaryEstimateCover(unaryEstimates); // recompute with inferred
        }

        private Map<Resolvable<?>, List<LocalEstimate>> initializeEstimatesFromRetrievables() {
            Map<Resolvable<?>, List<LocalEstimate>> estimatesFromRetrievable = new HashMap<>();
            resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                List<LocalEstimate> retrievableEstimates = new ArrayList<>();
                Set<Concludable> concludablesInRetrievable = ResolvableConjunction.of(retrievable.pattern()).positiveConcludables();
                iterate(concludablesInRetrievable).forEachRemaining(concludable -> {
                    retrievableEstimates.add(deriveEstimateFromConcludable(concludable));
                });
                estimatesFromRetrievable.put(retrievable, retrievableEstimates);
            });
            return estimatesFromRetrievable;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> initializeEstimatesFromNegations() {
            Map<Resolvable<?>, List<LocalEstimate>> estimatesFromNegateds = new HashMap<>();
            Set<Negated> negateds = resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated).toSet();
            iterate(negateds).forEachRemaining(negated -> estimatesFromNegateds.put(negated, list()));
            return estimatesFromNegateds;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> initializeEstimatesFromAcyclicConcludables() {
            Map<Resolvable<?>, List<LocalEstimate>> estimatesFromAcyclicConcludables = new HashMap<>();
            iterate(acyclicConcludables).forEachRemaining(concludable -> {
                estimatesFromAcyclicConcludables.put(concludable, list(deriveEstimateFromConcludable(concludable)));
            });

            return estimatesFromAcyclicConcludables;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> initializeEstimatesFromCyclicConcludables() {
            Map<Resolvable<?>, List<LocalEstimate>> estimatesFromCyclicConcludables = new HashMap<>();
            iterate(cyclicConcludables).forEachRemaining(concludable -> {
                estimatesFromCyclicConcludables.put(concludable, list(deriveEstimateFromConcludable(concludable)));
            });

            return estimatesFromCyclicConcludables;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> initializeUnaryEstimatesWithInference() {
            Map<Resolvable<?>, List<LocalEstimate>> unaryEstimates = new HashMap<>();
            resolvables().filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        ThingVariable v = concludable.generating().get();
                        long persistedAnswerCount = answerCountModel.countPersistedThingsMatchingType(v.asThing());
                        long inferredAnswerCount = (concludable.isHas() || concludable.isAttribute()) ?
                                answerCountModel.attributesCreatedByExplicitHas(concludable) :
                                answerCountModel.estimateInferredAnswerCount(concludable, set(v));
                        unaryEstimates.put(concludable, list(new LocalEstimate.SimpleEstimate(list(v), persistedAnswerCount + inferredAnswerCount)));
                    });
            return unaryEstimates;
        }

        private Map<Variable, LocalEstimate> computeUnaryEstimateCover(Map<Resolvable<?>, List<LocalEstimate>> unaryEstimates) {
            Map<Variable, LocalEstimate> newUnaryEstimateCover = new HashMap<>();
            iterate(allVariables()).map(Variable::asThing).forEachRemaining(v -> { // baseline
                newUnaryEstimateCover.put(v, new LocalEstimate.SimpleEstimate(list(v), answerCountModel.countPersistedThingsMatchingType(v)));
            });

            iterate(unaryEstimates.values()).flatMap(Iterators::iterate)
                    .forEachRemaining(estimate -> {
                        Variable v = estimate.variables.get(0);
                        long existingEstimate = newUnaryEstimateCover.get(v).answerEstimate(set(v));
                        long newEstimate = estimate.answerEstimate(set(v));
                        if (newEstimate > existingEstimate) { // Biggest one serves as baseline.
                            newUnaryEstimateCover.put(v, estimate);
                        }
                    });

            return newUnaryEstimateCover;
        }

        private LocalEstimate deriveEstimateFromConcludable(Concludable concludable) {
            if (concludable.isHas()) {
                return answerCountModel.estimatesFromHasEdges(concludable.asHas());
            } else if (concludable.isRelation()) {
                return answerCountModel.estimatesFromRolePlayersAssumingEvenDistribution(concludable.asRelation());
            } else if (concludable.isIsa()) {
                assert concludable.generating().isPresent();
                ThingVariable v = concludable.generating().get();
                return new LocalEstimate.SimpleEstimate(list(v),
                        answerCountModel.countPersistedThingsMatchingType(v) + answerCountModel.estimateInferredAnswerCount(concludable, set(v)));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

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

                    return Double.valueOf(Math.ceil(relationTypeEstimate * singleRelationEstimate)).longValue() + inferredRelationEstimate;
                }

                private long nPermuteKforSmallK(long n, long k) {
                    long ans = 1;
                    for (int i = 0; i < k; i++) ans *= n - i;
                    return ans;
                }
            }
        }
    }

    private static class AnswerCountModel {
        private final AnswerCountEstimator answerCountEstimator;
        private final GraphManager graphMgr;

        public AnswerCountModel(AnswerCountEstimator answerCountEstimator, GraphManager graphMgr) {
            this.answerCountEstimator = answerCountEstimator;
            this.graphMgr = graphMgr;
        }

        private long estimateInferredAnswerCount(Concludable concludable, Set<Variable> variableFilter) {
            Map<Rule, Set<Unifier>> unifiers = answerCountEstimator.logicMgr.applicableRules(concludable);
            long inferredEstimate = 0;
            for (Rule rule : unifiers.keySet()) {
                for (Unifier unifier : unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variableFilter)
                            .flatMap(v -> iterate(unifier.mapping().get(v.id()))).toSet();
                    Set<Variable> ruleSideVariables = iterate(ruleSideIds).map(id -> rule.conclusion().conjunction().pattern().variable(id))
                            .toSet();
                    if (rule.conclusion().generating().isPresent() && ruleSideVariables.contains(rule.conclusion().generating().get())) {
                        // There is one generated variable per combination of ALL variables in the conclusion
                        ruleSideVariables = rule.conclusion().pattern().variables().stream().filter(Variable::isThing).collect(Collectors.toSet());
                    }
                    inferredEstimate += answerCountEstimator.estimateAnswers(rule.condition().conjunction(), ruleSideVariables);
                }
            }
            return inferredEstimate;
        }

        private ConjunctionAnswerCountEstimator.LocalEstimate estimatesFromHasEdges(Concludable.Has concludableHas) {
            long estimate = countPersistedHasEdges(concludableHas.asHas().owner().inferredTypes(), concludableHas.asHas().attribute().inferredTypes()) +
                    estimateInferredAnswerCount(concludableHas, set(concludableHas.asHas().owner(), concludableHas.asHas().attribute()));

            return new ConjunctionAnswerCountEstimator.LocalEstimate.SimpleEstimate(list(concludableHas.asHas().owner(), concludableHas.asHas().attribute()), estimate);
        }

        private ConjunctionAnswerCountEstimator.LocalEstimate estimatesFromRolePlayersAssumingEvenDistribution(Concludable.Relation concludable) {
            // Small inaccuracy: We double count duplicate roles (r:$a, r:$b)
            // counts the case where r:$a=r:$b, which TypeDB wouldn't return
            Set<RelationConstraint.RolePlayer> rolePlayers = concludable.relation().players();
            List<Variable> constrainedVars = new ArrayList<>();
            Map<TypeVariable, Long> rolePlayerEstimates = new HashMap<>();
            Map<TypeVariable, Integer> rolePlayerCounts = new HashMap<>();

            double relationTypeEstimate = countPersistedThingsMatchingType(concludable.relation().owner());

            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                constrainedVars.add(rp.player());
                TypeVariable key = rp.roleType().orElse(null);
                rolePlayerCounts.put(key, rolePlayerCounts.getOrDefault(key, 0) + 1);
                if (!rolePlayerEstimates.containsKey(key)) {
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp));
                }
            }

            // TODO: Can improve estimate by collecting List<List<LocalEstimate>> from the triggered rules and doign sum(costCover).
            // Consider owner in the inferred estimate call only if it's not anonymous
            if (concludable.relation().owner().id().isName()) {
                constrainedVars.add(concludable.relation().owner());
            }
            long inferredRelationsEstimate = estimateInferredAnswerCount(concludable, new HashSet<>(constrainedVars));
            constrainedVars.add(concludable.relation().owner()); // Now add it.

            return new ConjunctionAnswerCountEstimator.LocalEstimate.CoPlayerEstimate(constrainedVars, relationTypeEstimate, rolePlayerEstimates, rolePlayerCounts, inferredRelationsEstimate);
        }

        public long attributesCreatedByExplicitHas(Concludable concludable) {
            return iterate(answerCountEstimator.logicMgr.applicableRules(concludable).keySet())
                    .filter(rule -> rule.conclusion().isExplicitHas())
                    .map(rule -> rule.conclusion().asExplicitHas().value().value())
                    .toSet().size();
        }

        private long countPersistedRolePlayers(RelationConstraint.RolePlayer rolePlayer) {
            return graphMgr.data().stats().thingVertexSum(rolePlayer.inferredRoleTypes());
        }

        private long countPersistedThingsMatchingType(ThingVariable thingVar) {
            return graphMgr.data().stats().thingVertexSum(thingVar.inferredTypes());
        }

        private long countPersistedHasEdges(Set<Label> ownerTypes, Set<Label> attributeTypes) {
            return ownerTypes.stream().map(ownerType ->
                    attributeTypes.stream()
                            .map(attrType -> graphMgr.data().stats().hasEdgeCount(ownerType, attrType))
                            .reduce(0L, Long::sum)
            ).reduce(0L, Long::sum);
        }
    }
}
