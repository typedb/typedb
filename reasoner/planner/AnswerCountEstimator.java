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
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final Map<ResolvableConjunction, Set<ResolvableConjunction>> cyclePathToHead;
    private final Map<ResolvableConjunction, Set<ResolvableConjunction>> cycleHeadToPaths;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph) {
        this.logicMgr = logicMgr;
        this.answerCountModel = new AnswerCountModel(this, graph);
        this.estimators = new HashMap<>();
        this.initializationStack = new ArrayList<>();
        this.cycleHeadToPaths = new HashMap<>();
        this.cyclePathToHead = new HashMap<>();
    }

    public long estimateAllAnswers(ResolvableConjunction conjunction) {
        registerAndInitializeConjunction(conjunction);
        return estimators.get(conjunction).estimateAllAnswers();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter) {
        return estimateAnswers(conjunction, variableFilter, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
        registerAndInitializeConjunction(conjunction);
        return estimators.get(conjunction).estimateAnswers(variableFilter, includedResolvables);
    }

    void registerAndInitializeConjunction(ResolvableConjunction conjunction) {
        if (!estimators.containsKey(conjunction)) {
            ConjunctionAnswerCountEstimator estimator = new ConjunctionAnswerCountEstimator(this, conjunction);
            estimator.initializeNonInferredLocalEstimates();
            estimators.put(conjunction, estimator);
        }

        if (estimators.get(conjunction).localEstimateStatus == ConjunctionAnswerCountEstimator.LocalEstimateStatus.COMPLETE) {
            return;
        }


        // cycle-detection
        if (initializationStack.contains(conjunction) || (cyclePathToHead.containsKey(conjunction) && !cyclePathToHead.get(conjunction).isEmpty())) {
            Set<ResolvableConjunction> cycleHeads = new HashSet<>();
            if (initializationStack.contains(conjunction)) {
                cycleHeads.add(conjunction);
            }
            if (cyclePathToHead.containsKey(conjunction) && !cyclePathToHead.get(conjunction).isEmpty()) {
                iterate(cyclePathToHead.get(conjunction))
                        .filter(initializationStack::contains) // Should be redundant
                        .forEachRemaining(cycleHeads::add);
            }

            if (!cycleHeads.isEmpty()) {
                for (ResolvableConjunction cycleHead : cycleHeads) {
                    cycleHeadToPaths.putIfAbsent(cycleHead, new HashSet<>());
                    for (int i = initializationStack.size() - 1; initializationStack.get(i) != cycleHead; i--) {
                        ResolvableConjunction pathNode = initializationStack.get(i);
                        cycleHeadToPaths.get(cycleHead).add(pathNode);
                        cyclePathToHead.putIfAbsent(pathNode, new HashSet<>());
                        cyclePathToHead.get(pathNode).add(cycleHead);
                    }
                }
            }
        } else {
            // recurse
            initializationStack.add(conjunction);

            Set<ResolvableConjunction> dependencies = estimators.get(conjunction).dependencies();
            dependencies.forEach(this::registerAndInitializeConjunction);
            estimators.get(conjunction).initializeInferredLocalEstimates();

            // Reset cycles ending here.
            if (cycleHeadToPaths.containsKey(conjunction)) {
                Set<ResolvableConjunction> directDependenciesReset = new HashSet<>();
                for (ResolvableConjunction pathNode : cycleHeadToPaths.get(conjunction)) {
                    estimators.get(pathNode).resetInferrableEstimates();
                    if (dependencies.contains(pathNode)) directDependenciesReset.add(pathNode);
                    cyclePathToHead.get(pathNode).remove(conjunction);
                }
                cycleHeadToPaths.remove(conjunction);
                directDependenciesReset.forEach(this::registerAndInitializeConjunction);
            }
            initializationStack.remove(initializationStack.size() - 1);

            assert estimators.get(conjunction).localEstimateStatus == ConjunctionAnswerCountEstimator.LocalEstimateStatus.COMPLETE;
            assert initializationStack.size() > 0 || // TODO: Remove
                    estimators.values().stream().allMatch(estimator -> estimator.localEstimateStatus == ConjunctionAnswerCountEstimator.LocalEstimateStatus.COMPLETE);
        }
    }

    private static class ConjunctionAnswerCountEstimator {

        private final ResolvableConjunction conjunction;
        private final AnswerCountEstimator answerCountEstimator;
        private final LogicManager logicMgr;
        private final AnswerCountModel answerCountModel;
        private Map<Variable, LocalEstimate> unaryEstimateCover;
        private Map<Resolvable<?>, List<LocalEstimate>> retrievableEstimates;
        private Map<Resolvable<?>, LocalEstimate> inferrableEstimates;
        private Map<Resolvable<?>, LocalEstimate> unaryEstimates;

        private long fullAnswerCount;
        private long negatedsCost;

        private enum LocalEstimateStatus {EMPTY, NON_INFERRED, INFERRED_IN_PROGRESS, RESET, COMPLETE}

        private LocalEstimateStatus localEstimateStatus;

        public ConjunctionAnswerCountEstimator(AnswerCountEstimator answerCountEstimator, ResolvableConjunction conjunction) {
            this.answerCountEstimator = answerCountEstimator;
            this.answerCountModel = answerCountEstimator.answerCountModel;
            this.logicMgr = answerCountEstimator.logicMgr;
            this.conjunction = conjunction;
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
            this.localEstimateStatus = LocalEstimateStatus.EMPTY;
        }

        private FunctionalIterator<Resolvable<?>> resolvables() {
            return iterate(logicMgr.compile(conjunction));
        }

        private Set<Variable> allVariables() {
            return iterate(conjunction.pattern().variables())
                    .filter(Variable::isThing)
                    .toSet();
        }

        private List<LocalEstimate> estimatesFromResolvable(Resolvable<?> resolvable) {
            List<LocalEstimate> results = new ArrayList<>();
            assert retrievableEstimates != null;
            assert inferrableEstimates != null ||
                    this.localEstimateStatus == LocalEstimateStatus.NON_INFERRED || this.localEstimateStatus == LocalEstimateStatus.INFERRED_IN_PROGRESS ||
                    this.localEstimateStatus == LocalEstimateStatus.RESET;

            if (retrievableEstimates.containsKey(resolvable)) results.addAll(retrievableEstimates.get(resolvable));
            if (unaryEstimates != null && unaryEstimates.containsKey(resolvable))
                results.add(unaryEstimates.get(resolvable));
            if (inferrableEstimates != null && inferrableEstimates.containsKey(resolvable))
                results.add(inferrableEstimates.get(resolvable));
            return results;
        }

        public long estimateAllAnswers() {
            assert this.fullAnswerCount >= 0;
            return this.fullAnswerCount;
        }

        public long estimateAnswers(Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
            List<LocalEstimate> includedEstimates = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(estimatesFromResolvable(resolvable)))
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

        private void resetInferrableEstimates() {
            assert this.localEstimateStatus == LocalEstimateStatus.COMPLETE || this.localEstimateStatus == LocalEstimateStatus.RESET;
            this.localEstimateStatus = LocalEstimateStatus.RESET;
            this.inferrableEstimates = null;
            this.unaryEstimates = null;
            this.fullAnswerCount = -1;
        }

        public Set<ResolvableConjunction> dependencies() {
            return resolvables().filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .flatMap(concludable -> iterate(logicMgr.applicableRules(concludable).keySet()))
                    .map(rule -> rule.condition().conjunction())
                    .toSet();
        }

        private void initializeNonInferredLocalEstimates() {
            assert this.localEstimateStatus == LocalEstimateStatus.EMPTY;
            assert unaryEstimateCover == null && retrievableEstimates == null;
            unaryEstimateCover = computeNonInferredUnaryEstimateCover();
            retrievableEstimates = deriveEstimatesFromRetrievables();
            negatedsCost = computeNegatedsCost(); // Can't be involved in cycles because stratified-negation
            this.localEstimateStatus = LocalEstimateStatus.NON_INFERRED;
        }

        private void initializeInferredLocalEstimates() {
            assert localEstimateStatus != LocalEstimateStatus.EMPTY;
            if (localEstimateStatus != LocalEstimateStatus.COMPLETE) {
                assert inferrableEstimates == null && unaryEstimates == null;
                localEstimateStatus = LocalEstimateStatus.INFERRED_IN_PROGRESS;
                inferrableEstimates = deriveEstimatesFromInferrables();
                unaryEstimates = computeUnaryEstimatesWithInference(unaryEstimateCover);
                unaryEstimateCover = computeFinalUnaryEstimateCover(unaryEstimateCover); // recompute with inferred

                List<LocalEstimate> allEstimates = resolvables().flatMap(resolvable -> iterate(estimatesFromResolvable(resolvable))).toList();
                Map<Variable, LocalEstimate> fullCostCover = computeGreedyEstimateCoverForVariables(allVariables(), allEstimates);
                this.fullAnswerCount = costOfEstimateCover(allVariables(), fullCostCover) + this.negatedsCost;
                // Can we prune the multiVarEstimates stored?
                // Not if we want to order-resolvables. Else, yes based on queryableVariables.
                localEstimateStatus = LocalEstimateStatus.COMPLETE;
            }
        }

        private Map<Resolvable<?>, LocalEstimate> computeUnaryEstimatesWithInference(Map<Variable, LocalEstimate> nonInferredCover) {
            assert inferrableEstimates != null;
            Map<Resolvable<?>, LocalEstimate> unaryEstimates = new HashMap<>();
            iterate(inferrableEstimates.keySet()).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        ThingVariable v = concludable.generating().get();
                        long inferredAnswerCount = (concludable.isHas() || concludable.isAttribute()) ?
                                answerCountModel.attributesCreatedByExplicitHas(concludable) :
                                answerCountModel.estimateInferredAnswerCount(concludable, set(v));
                        unaryEstimates.put(concludable, new LocalEstimate.SimpleEstimate(list(v), nonInferredCover.get(v).answerEstimate(set(v)) + inferredAnswerCount));
                    });
            return unaryEstimates;
        }

        private Map<Variable, LocalEstimate> computeNonInferredUnaryEstimateCover() {
            Map<Variable, LocalEstimate> unaryEstimateCover = new HashMap<>();
            iterate(allVariables()).forEachRemaining(v -> {
                unaryEstimateCover.put(v.asThing(), new LocalEstimate.SimpleEstimate(list(v.asThing()), answerCountModel.countPersistedThingsMatchingType(v.asThing())));
            });
            return unaryEstimateCover;
        }

        private Map<Variable, LocalEstimate> computeFinalUnaryEstimateCover(Map<Variable, LocalEstimate> nonInferredUnaryEstimateCover) {
            Map<Variable, LocalEstimate> newUnaryEstimateCover = new HashMap<>(nonInferredUnaryEstimateCover);
            iterate(unaryEstimates.values()).forEachRemaining(estimate -> {
                Variable v = estimate.variables.get(0);
                long existingEstimate = newUnaryEstimateCover.get(v).answerEstimate(set(v));
                long newEstimate = estimate.answerEstimate(set(v));
                if (newEstimate > existingEstimate) { // Biggest one serves as baseline.
                    newUnaryEstimateCover.put(v, estimate);
                }
            });
            return newUnaryEstimateCover;
        }

        private Map<Resolvable<?>, List<LocalEstimate>> deriveEstimatesFromRetrievables() {
            Map<Resolvable<?>, List<LocalEstimate>> retrievableEstimates = new HashMap<>();
            resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                retrievableEstimates.put(retrievable, new ArrayList<>());
                Set<Concludable> concludablesInRetrievable = ResolvableConjunction.of(retrievable.pattern()).positiveConcludables();
                iterate(concludablesInRetrievable).forEachRemaining(concludable -> {
                    deriveEstimatesFromConcludable(concludable)
                            .ifPresent(estimate -> retrievableEstimates.get(retrievable).add(estimate));
                });
            });

            return retrievableEstimates;
        }

        private Map<Resolvable<?>, LocalEstimate> deriveEstimatesFromInferrables() {
            Map<Resolvable<?>, LocalEstimate> inferrableEstimates = new HashMap<>();
            iterate(resolvables()).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        deriveEstimatesFromConcludable(concludable)
                                .ifPresent(estimate -> inferrableEstimates.put(concludable, estimate));
                    });
            return inferrableEstimates;
        }

        private Optional<LocalEstimate> deriveEstimatesFromConcludable(Concludable concludable) {
            if (concludable.isHas()) {
                return Optional.of(answerCountModel.estimatesFromHasEdges(concludable.asHas()));
            } else if (concludable.isRelation()) {
                return Optional.of(answerCountModel.estimatesFromRolePlayersAssumingEvenDistribution(concludable.asRelation()));
            } else if (concludable.isIsa()) {
                assert concludable.generating().isPresent();
                ThingVariable v = concludable.generating().get();
                return Optional.of(new LocalEstimate.SimpleEstimate(list(v),
                        unaryEstimateCover.get(v).answerEstimate(set(v)) + answerCountModel.estimateInferredAnswerCount(concludable, set(v))));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

        private long computeNegatedsCost() {
            return resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                    .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .map(negatedConjunction -> {
                        answerCountEstimator.registerAndInitializeConjunction(negatedConjunction);
                        return answerCountEstimator.estimateAllAnswers(negatedConjunction);
                    }).reduce(0L, Long::sum);
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
