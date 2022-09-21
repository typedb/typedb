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
    private final Map<ResolvableConjunction, ConjunctionModel> conjunctionModels;

    // Cycle handling
    private final ConcludableModel concludableModel;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph) {
        this.logicMgr = logicMgr;
        this.concludableModel = new ConcludableModel(this, graph);
        this.conjunctionModels = new HashMap<>();
    }

    public void registerConjunctionAndBuildModel(ResolvableConjunction conjunction) {
        registerConjunction(conjunction, new HashSet<>());
        buildConjunctionModel(conjunction);
    }

    private boolean registerConjunction(ResolvableConjunction conjunction, Set<ResolvableConjunction> dependencyPath) {
        if (!conjunctionModels.containsKey(conjunction)) {
            conjunctionModels.put(conjunction, new ConjunctionModel(this, conjunction));
        }

        // TODO: Improve cycle-detection using caching to avoid re-traversing the graph?
        if (dependencyPath.contains(conjunction)) return true;

        dependencyPath.add(conjunction);
        boolean onCycle = conjunctionModels.get(conjunction).registerDependencies(dependencyPath);
        dependencyPath.remove(conjunction);
        return onCycle;
    }

    private void buildConjunctionModel(ResolvableConjunction conjunction) {
        conjunctionModels.get(conjunction).buildModel();
    }

    public long estimateAllAnswers(ResolvableConjunction conjunction) {
        return conjunctionModels.get(conjunction).estimateAllAnswers();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter) {
        return estimateAnswers(conjunction, variableFilter, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
        return conjunctionModels.get(conjunction).estimateAnswers(variableFilter, includedResolvables);
    }

    private static class ConjunctionModel {

        private final ResolvableConjunction conjunction;
        private final AnswerCountEstimator answerCountEstimator;
        private final LogicManager logicMgr;
        private final ConcludableModel concludableModel;

        private Map<Variable, ResolvableModel> baselineCover;
        private long fullAnswerCount;
        private long negatedsCost;

        private Set<Concludable> cyclicConcludables;
        private Set<Concludable> acyclicConcludables;
        private final HashMap<Resolvable<?>, List<ResolvableModel>> resolvableModels;

        private enum ModelStatus {NOT_STARTED, REGISTERED, ACYCLIC_ESTIMATE, COMPLETE}

        private ModelStatus modelStatus;

        private ConjunctionModel(AnswerCountEstimator answerCountEstimator, ResolvableConjunction conjunction) {
            this.answerCountEstimator = answerCountEstimator;
            this.concludableModel = answerCountEstimator.concludableModel;
            this.logicMgr = answerCountEstimator.logicMgr;
            this.conjunction = conjunction;

            this.resolvableModels = new HashMap<>();
            this.fullAnswerCount = -1;
            this.negatedsCost = -1;
            this.modelStatus = ModelStatus.NOT_STARTED;
        }

        private FunctionalIterator<Resolvable<?>> resolvables() {
            return iterate(logicMgr.compile(conjunction));
        }

        private Set<Variable> allVariables() {
            return iterate(conjunction.pattern().variables())
                    .filter(Variable::isThing)
                    .toSet();
        }

        private long estimateAllAnswers() {
            assert this.modelStatus == ModelStatus.COMPLETE && this.fullAnswerCount >= 0;
            return this.fullAnswerCount;
        }

        private long estimateAnswers(Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
            assert this.modelStatus == ModelStatus.ACYCLIC_ESTIMATE || this.modelStatus == ModelStatus.COMPLETE;
            List<ResolvableModel> includedResolvableModels = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(resolvableModels.get(resolvable)))
                    .toList();
            Map<Variable, ResolvableModel> costCover = computeGreedyResolvableCoverForVariables(variableFilter, includedResolvableModels);
            long ret = costOfResolvableCover(variableFilter, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        private Map<Variable, ResolvableModel> computeGreedyResolvableCoverForVariables(Set<Variable> variableFilter, List<ResolvableModel> includedResolvableModels) {
            // Does a greedy set cover
            Map<Variable, ResolvableModel> currentCover = new HashMap<>(baselineCover);
            includedResolvableModels.sort(Comparator.comparing(x -> x.estimateAnswers(variableFilter)));
            for (ResolvableModel model : includedResolvableModels) {
                Set<Variable> filteredVariablesInResolvable = model.variables.stream()
                        .filter(variableFilter::contains).collect(Collectors.toSet());

                long currentCostToCover = costOfResolvableCover(filteredVariablesInResolvable, currentCover);
                if (currentCostToCover > model.estimateAnswers(filteredVariablesInResolvable)) {
                    filteredVariablesInResolvable.forEach(v -> currentCover.put(v, model));
                }
            }
            return currentCover;
        }

        private static long costOfResolvableCover(Set<Variable> variablesToConsider, Map<Variable, ResolvableModel> coverMap) {
            Set<ResolvableModel> subsetCoveredBy = coverMap.keySet().stream().filter(variablesToConsider::contains)
                    .map(coverMap::get).collect(Collectors.toSet());
            return subsetCoveredBy.stream().map(model -> model.estimateAnswers(variablesToConsider)).reduce(1L, (x, y) -> x * y);
        }

        private FunctionalIterator<ResolvableConjunction> dependencies(Concludable concludable) {
            return iterate(logicMgr.applicableRules(concludable).keySet())
                    .map(rule -> rule.condition().conjunction());
        }

        private boolean registerDependencies(Set<ResolvableConjunction> dependencyPath) {
            if (modelStatus == ModelStatus.NOT_STARTED) {
                cyclicConcludables = new HashSet<>();
                acyclicConcludables = new HashSet<>();
                resolvables().filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                        .forEachRemaining(concludable -> {
                            dependencies(concludable).forEachRemaining(dependency -> {
                                if (answerCountEstimator.registerConjunction(dependency, dependencyPath)) {
                                    cyclicConcludables.add(concludable);
                                }
                            });
                            if (!cyclicConcludables.contains(concludable)) {
                                acyclicConcludables.add(concludable);
                            }
                        });

                resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .forEachRemaining(dependency -> answerCountEstimator.registerConjunction(dependency, dependencyPath));

                modelStatus = ModelStatus.REGISTERED;
            }
            return !cyclicConcludables.isEmpty();
        }

        private void buildModel() {
            assert modelStatus != ModelStatus.NOT_STARTED;
            if (modelStatus == ModelStatus.REGISTERED) {
                // Acyclic estimates
                resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .forEachRemaining(answerCountEstimator::buildConjunctionModel);
                iterate(acyclicConcludables).flatMap(this::dependencies).forEachRemaining(answerCountEstimator::buildConjunctionModel);
                buildAcyclicModel();
                modelStatus = ModelStatus.ACYCLIC_ESTIMATE;

                // cyclic calls to this model will answer based on the acyclic model.
                iterate(cyclicConcludables).flatMap(this::dependencies).forEachRemaining(answerCountEstimator::buildConjunctionModel);
                buildCyclicModel();

                // Compute costs
                this.negatedsCost = resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated)
                        .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .map(answerCountEstimator::estimateAllAnswers).reduce(0L, Long::sum);

                this.fullAnswerCount = estimateAnswers(allVariables(), resolvables().toSet()) + this.negatedsCost;

                modelStatus = ModelStatus.COMPLETE;
            }
        }

        private void buildAcyclicModel() {
            this.resolvableModels.putAll(buildModelsForRetrievables());
            this.resolvableModels.putAll(buildModelsForNegateds());
            this.resolvableModels.putAll(buildModelsForAcyclicConcludables());
            iterate(cyclicConcludables).forEachRemaining(concludable -> this.resolvableModels.put(concludable, list()));
            this.baselineCover = computeBaselineVariableCover(new HashMap<>());
        }

        private void buildCyclicModel() {
            this.resolvableModels.putAll(buildResolvableModelsForCyclicConcludables());
            Map<Resolvable<?>, List<ResolvableModel>> unaryModels = resolvableModelsForSingleVariables();
            iterate(unaryModels.keySet()).forEachRemaining(resolvable -> {
                this.resolvableModels.put(resolvable, list(this.resolvableModels.get(resolvable), unaryModels.get(resolvable)));
            });
            this.baselineCover = computeBaselineVariableCover(unaryModels); // recompute with inferred
        }

        private Map<Resolvable<?>, List<ResolvableModel>> buildModelsForRetrievables() {
            Map<Resolvable<?>, List<ResolvableModel>> modelsForRetrievables = new HashMap<>();
            resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                Set<Concludable> concludablesInRetrievable = ResolvableConjunction.of(retrievable.pattern()).positiveConcludables();
                modelsForRetrievables.put(retrievable, iterate(concludablesInRetrievable).map(this::buildConcludableModel).toList());
            });
            return modelsForRetrievables;
        }

        private Map<Resolvable<?>, List<ResolvableModel>> buildModelsForNegateds() {
            Map<Resolvable<?>, List<ResolvableModel>> modelsForNegateds = new HashMap<>();
            Set<Negated> negateds = resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated).toSet();
            iterate(negateds).forEachRemaining(negated -> modelsForNegateds.put(negated, list()));
            return modelsForNegateds;
        }

        private Map<Resolvable<?>, List<ResolvableModel>> buildModelsForAcyclicConcludables() {
            Map<Resolvable<?>, List<ResolvableModel>> modelsForAcyclicConcludables = new HashMap<>();
            iterate(acyclicConcludables).forEachRemaining(concludable -> {
                modelsForAcyclicConcludables.put(concludable, list(buildConcludableModel(concludable)));
            });
            return modelsForAcyclicConcludables;
        }

        private Map<Resolvable<?>, List<ResolvableModel>> buildResolvableModelsForCyclicConcludables() {
            Map<Resolvable<?>, List<ResolvableModel>> modelsForCyclicConcludables = new HashMap<>();
            iterate(cyclicConcludables).forEachRemaining(concludable -> {
                modelsForCyclicConcludables.put(concludable, list(buildConcludableModel(concludable)));
            });
            return modelsForCyclicConcludables;
        }

        private Map<Resolvable<?>, List<ResolvableModel>> resolvableModelsForSingleVariables() {
            Map<Resolvable<?>, List<ResolvableModel>> singleVariableModels = new HashMap<>();
            resolvables().filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        ThingVariable v = concludable.generating().get();
                        long persistedAnswerCount = concludableModel.countPersistedThingsMatchingType(v.asThing());
                        long inferredAnswerCount = (concludable.isHas() || concludable.isAttribute()) ?
                                concludableModel.attributesCreatedByExplicitHas(concludable) :
                                concludableModel.estimateInferredAnswerCount(concludable, set(v));
                        singleVariableModels.put(concludable, list(new ResolvableModel.StaticModel(list(v), persistedAnswerCount + inferredAnswerCount)));
                    });
            return singleVariableModels;
        }

        private Map<Variable, ResolvableModel> computeBaselineVariableCover(Map<Resolvable<?>, List<ResolvableModel>> unaryModels) {
            Map<Variable, ResolvableModel> newUnaryModelCover = new HashMap<>();
            iterate(allVariables()).map(Variable::asThing).forEachRemaining(v -> { // baseline
                newUnaryModelCover.put(v, new ResolvableModel.StaticModel(list(v), concludableModel.countPersistedThingsMatchingType(v)));
            });

            iterate(unaryModels.values()).flatMap(Iterators::iterate)
                    .forEachRemaining(model -> {
                        Variable v = model.variables.get(0);
                        long existingEstimate = newUnaryModelCover.get(v).estimateAnswers(set(v));
                        long newEstimate = model.estimateAnswers(set(v));
                        if (newEstimate > existingEstimate) { // Biggest one serves as baseline.
                            newUnaryModelCover.put(v, model);
                        }
                    });

            return newUnaryModelCover;
        }

        private ResolvableModel buildConcludableModel(Concludable concludable) {
            if (concludable.isHas()) {
                return concludableModel.modelForHas(concludable.asHas());
            } else if (concludable.isRelation()) {
                return concludableModel.modelForRelation(concludable.asRelation());
            } else if (concludable.isIsa()) {
                assert concludable.generating().isPresent();
                ThingVariable v = concludable.generating().get();
                return new ResolvableModel.StaticModel(list(v),
                        concludableModel.countPersistedThingsMatchingType(v) + concludableModel.estimateInferredAnswerCount(concludable, set(v)));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

        private static abstract class ResolvableModel {

            final List<Variable> variables;

            private ResolvableModel(List<Variable> variables) {
                this.variables = variables;
            }

            abstract long estimateAnswers(Set<Variable> variableFilter);

            private static class StaticModel extends ResolvableModel {
                private final long staticEstimate;

                private StaticModel(List<Variable> variables, long estimate) {
                    super(variables);
                    this.staticEstimate = estimate;
                }

                @Override
                long estimateAnswers(Set<Variable> variableFilter) {
                    return staticEstimate;
                }
            }

            private static class RelationModel extends ResolvableModel {

                private final Map<TypeVariable, Integer> rolePlayerCounts;
                private final Map<TypeVariable, Long> rolePlayerEstimates;
                private final double relationTypeEstimate;
                private final long inferredRelationEstimate;

                private RelationModel(List<Variable> variables, double relationTypeEstimate,
                                      Map<TypeVariable, Long> rolePlayerEstimates, Map<TypeVariable, Integer> rolePlayerCounts,
                                      long inferredRelationEstimate) {
                    super(variables);
                    this.relationTypeEstimate = relationTypeEstimate;
                    this.rolePlayerEstimates = rolePlayerEstimates;
                    this.rolePlayerCounts = rolePlayerCounts;
                    this.inferredRelationEstimate = inferredRelationEstimate;
                }

                @Override
                long estimateAnswers(Set<Variable> variableFilter) {
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

    private static class ConcludableModel {
        private final AnswerCountEstimator answerCountEstimator;
        private final GraphManager graphMgr;

        private ConcludableModel(AnswerCountEstimator answerCountEstimator, GraphManager graphMgr) {
            this.answerCountEstimator = answerCountEstimator;
            this.graphMgr = graphMgr;
        }

        private ConjunctionModel.ResolvableModel modelForHas(Concludable.Has concludableHas) {
            long estimate = countPersistedHasEdges(concludableHas.asHas().owner().inferredTypes(), concludableHas.asHas().attribute().inferredTypes()) +
                    estimateInferredAnswerCount(concludableHas, set(concludableHas.asHas().owner(), concludableHas.asHas().attribute()));

            return new ConjunctionModel.ResolvableModel.StaticModel(list(concludableHas.asHas().owner(), concludableHas.asHas().attribute()), estimate);
        }

        private ConjunctionModel.ResolvableModel modelForRelation(Concludable.Relation concludable) {
            // Assumes role-players are evenly distributed.
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

            // TODO: Can improve estimate by collecting List<List<ResolvableModel>> from the triggered rules and doing sum(costCover).
            // Consider owner in the inferred estimate call only if it's not anonymous
            if (concludable.relation().owner().id().isName()) {
                constrainedVars.add(concludable.relation().owner());
            }
            long inferredRelationsEstimate = estimateInferredAnswerCount(concludable, new HashSet<>(constrainedVars));
            constrainedVars.add(concludable.relation().owner()); // Now add it.

            return new ConjunctionModel.ResolvableModel.RelationModel(constrainedVars, relationTypeEstimate, rolePlayerEstimates, rolePlayerCounts, inferredRelationsEstimate);
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

        private long attributesCreatedByExplicitHas(Concludable concludable) {
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
