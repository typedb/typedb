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
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ThingConstraint;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class AnswerCountEstimator {
    private final LogicManager logicMgr;
    private final Map<ResolvableConjunction, ConjunctionModel> conjunctionModels;

    // Cycle handling
    private final ConstraintModelBuilder constraintModelBuilder;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph) {
        this.logicMgr = logicMgr;
        this.constraintModelBuilder = new ConstraintModelBuilder(this, graph);
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
        private final ConstraintModelBuilder constraintModelBuilder;

        private Map<Variable, ConstraintModel> baselineCover;
        private long fullAnswerCount;
        private long negatedsCost;

        private Set<Concludable> cyclicConcludables;
        private Set<Concludable> acyclicConcludables;
        private final HashMap<Resolvable<?>, List<ConstraintModel>> constraintModels;

        private enum ModelStatus {NOT_STARTED, REGISTERED, ACYCLIC_ESTIMATE, COMPLETE}

        private ModelStatus modelStatus;

        private ConjunctionModel(AnswerCountEstimator answerCountEstimator, ResolvableConjunction conjunction) {
            this.answerCountEstimator = answerCountEstimator;
            this.constraintModelBuilder = answerCountEstimator.constraintModelBuilder;
            this.logicMgr = answerCountEstimator.logicMgr;
            this.conjunction = conjunction;

            this.constraintModels = new HashMap<>();
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
            List<ConstraintModel> includedConstraintModels = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(constraintModels.get(resolvable)))
                    .toList();
            Map<Variable, ConstraintModel> costCover = computeGreedyResolvableCoverForVariables(variableFilter, includedConstraintModels);
            long ret = costOfResolvableCover(variableFilter, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        public Set<Constraint> extractConstraintsToModel(Retrievable retrievable) {
            Set<Constraint> constraints = new HashSet<>();
            iterate(retrievable.pattern().variables()).flatMap(v -> iterate(v.constraints())).filter(this::isModellable).forEachRemaining(constraints::add);
            return constraints;
        }

        public Constraint extractConstraintToModel(Concludable concludable) {
            if (concludable.isHas()) {
                return concludable.asHas().has();
            } else if (concludable.isRelation()) {
                return concludable.asRelation().relation();
            } else if (concludable.isIsa()) {
                return concludable.asIsa().isa();
            } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }

        private boolean isModellable(Constraint constraint) {
            return constraint.isThing() &&
                    (constraint.asThing().isRelation() || constraint.asThing().isHas() || constraint.asThing().isIsa());
        }

        private Map<Variable, ConstraintModel> computeGreedyResolvableCoverForVariables(Set<Variable> variableFilter, List<ConstraintModel> includedConstraintModels) {
            // Does a greedy set cover
            Map<Variable, ConstraintModel> currentCover = new HashMap<>(baselineCover);
            includedConstraintModels.sort(Comparator.comparing(x -> x.estimateAnswers(variableFilter)));
            for (ConstraintModel model : includedConstraintModels) {
                Set<Variable> filteredVariablesInResolvable = model.variables.stream()
                        .filter(variableFilter::contains).collect(Collectors.toSet());

                long currentCostToCover = costOfResolvableCover(filteredVariablesInResolvable, currentCover);
                if (currentCostToCover > model.estimateAnswers(filteredVariablesInResolvable)) {
                    filteredVariablesInResolvable.forEach(v -> currentCover.put(v, model));
                }
            }
            return currentCover;
        }

        private static long costOfResolvableCover(Set<Variable> variablesToConsider, Map<Variable, ConstraintModel> coverMap) {
            Set<ConstraintModel> subsetCoveredBy = coverMap.keySet().stream().filter(variablesToConsider::contains)
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
            this.constraintModels.putAll(buildModelsForRetrievables());
            this.constraintModels.putAll(buildModelsForNegateds());
            this.constraintModels.putAll(buildModelsForAcyclicConcludables());
            iterate(cyclicConcludables).forEachRemaining(concludable -> this.constraintModels.put(concludable, list()));
            this.baselineCover = computeBaselineVariableCover(new HashMap<>());
        }

        private void buildCyclicModel() {
            this.constraintModels.putAll(buildModelsForCyclicConcludables());
            Map<Resolvable<?>, List<ConstraintModel>> generatedVariableModels = constraintModelsForGeneratedVariables();
            iterate(generatedVariableModels.keySet()).forEachRemaining(resolvable -> {
                this.constraintModels.put(resolvable, list(this.constraintModels.get(resolvable), generatedVariableModels.get(resolvable)));
            });
            this.baselineCover = computeBaselineVariableCover(generatedVariableModels); // recompute with inferred
        }

        private Map<Resolvable<?>, List<ConstraintModel>> buildModelsForRetrievables() {
            Map<Resolvable<?>, List<ConstraintModel>> modelsForRetrievables = new HashMap<>();
            resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                List<ConstraintModel> constraintModels = iterate(extractConstraintsToModel(retrievable))
                        .map(constraint -> buildConstraintModel(constraint, Optional.empty()))
                        .toList();
                modelsForRetrievables.put(retrievable, constraintModels);
            });
            return modelsForRetrievables;
        }

        private Map<Resolvable<?>, List<ConstraintModel>> buildModelsForNegateds() {
            Map<Resolvable<?>, List<ConstraintModel>> modelsForNegateds = new HashMap<>();
            Set<Negated> negateds = resolvables().filter(Resolvable::isNegated).map(Resolvable::asNegated).toSet();
            iterate(negateds).forEachRemaining(negated -> modelsForNegateds.put(negated, list()));
            return modelsForNegateds;
        }

        private Map<Resolvable<?>, List<ConstraintModel>> buildModelsForAcyclicConcludables() {
            Map<Resolvable<?>, List<ConstraintModel>> modelsForAcyclicConcludables = new HashMap<>();
            iterate(acyclicConcludables).forEachRemaining(concludable -> {
                modelsForAcyclicConcludables.put(concludable, list(buildConstraintModel(extractConstraintToModel(concludable), Optional.of(concludable))));
            });
            return modelsForAcyclicConcludables;
        }

        private Map<Resolvable<?>, List<ConstraintModel>> buildModelsForCyclicConcludables() {
            Map<Resolvable<?>, List<ConstraintModel>> modelsForCyclicConcludables = new HashMap<>();
            iterate(cyclicConcludables).forEachRemaining(concludable -> {
                modelsForCyclicConcludables.put(concludable, list(buildConstraintModel(extractConstraintToModel(concludable), Optional.of(concludable))));
            });
            return modelsForCyclicConcludables;
        }

        private Map<Resolvable<?>, List<ConstraintModel>> constraintModelsForGeneratedVariables() {
            Map<Resolvable<?>, List<ConstraintModel>> generatedVariableModels = new HashMap<>();
            resolvables().filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        ThingVariable v = concludable.generating().get();
                        long persistedAnswerCount = constraintModelBuilder.countPersistedThingsMatchingType(v.asThing());
                        long inferredAnswerCount = (concludable.isHas() || concludable.isAttribute()) ?
                                constraintModelBuilder.attributesCreatedByExplicitHas(concludable) :
                                constraintModelBuilder.estimateInferredAnswerCount(concludable, set(v));
                        generatedVariableModels.put(concludable, list(new ConstraintModel.StaticModel(list(v), persistedAnswerCount + inferredAnswerCount)));
                    });
            return generatedVariableModels;
        }

        private Map<Variable, ConstraintModel> computeBaselineVariableCover(Map<Resolvable<?>, List<ConstraintModel>> generatedVariableModels) {
            Map<Variable, ConstraintModel> newVariableCover = new HashMap<>();
            iterate(allVariables()).map(Variable::asThing).forEachRemaining(v -> { // baseline
                newVariableCover.put(v, new ConstraintModel.StaticModel(list(v), constraintModelBuilder.countPersistedThingsMatchingType(v)));
            });

            iterate(generatedVariableModels.values()).flatMap(Iterators::iterate)
                    .forEachRemaining(model -> {
                        Variable v = model.variables.get(0);
                        long existingEstimate = newVariableCover.get(v).estimateAnswers(set(v));
                        long newEstimate = model.estimateAnswers(set(v));
                        if (newEstimate > existingEstimate) { // Biggest one serves as baseline.
                            newVariableCover.put(v, model);
                        }
                    });

            return newVariableCover;
        }

        private ConstraintModel buildConstraintModel(Constraint constraint, Optional<Concludable> correspondingConcludable) {
            if (constraint.isThing()) {
                ThingConstraint asThingConstraint = constraint.asThing();
                if (asThingConstraint.isHas()) {
                    return constraintModelBuilder.modelForHas(asThingConstraint.asHas(), correspondingConcludable);
                } else if (asThingConstraint.isRelation()) {
                    return constraintModelBuilder.modelForRelation(asThingConstraint.asRelation(), correspondingConcludable);
                } else if (asThingConstraint.isIsa()) {
                    return constraintModelBuilder.modelForIsa(asThingConstraint.asIsa(), correspondingConcludable);
                } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
            } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }

        private static abstract class ConstraintModel {

            final List<Variable> variables;

            private ConstraintModel(List<Variable> variables) {
                this.variables = variables;
            }

            abstract long estimateAnswers(Set<Variable> variableFilter);

            private static class StaticModel extends ConstraintModel {
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

            private static class RelationModel extends ConstraintModel {

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

    private static class ConstraintModelBuilder {
        private final AnswerCountEstimator answerCountEstimator;
        private final GraphManager graphMgr;

        private ConstraintModelBuilder(AnswerCountEstimator answerCountEstimator, GraphManager graphMgr) {
            this.answerCountEstimator = answerCountEstimator;
            this.graphMgr = graphMgr;
        }

        private ConjunctionModel.ConstraintModel modelForHas(HasConstraint hasConstraint, Optional<Concludable> correspondingConcludable) {
            long estimate = countPersistedHasEdges(hasConstraint.owner().inferredTypes(), hasConstraint.attribute().inferredTypes());
            if (correspondingConcludable.isPresent()) {
                estimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(hasConstraint.owner(), hasConstraint.attribute()));
            }
            return new ConjunctionModel.ConstraintModel.StaticModel(list(hasConstraint.owner(), hasConstraint.attribute()), estimate);
        }

        private ConjunctionModel.ConstraintModel modelForRelation(RelationConstraint relationConstraint, Optional<Concludable> correspondingConcludable) {
            // Assumes role-players are evenly distributed.
            // Small inaccuracy: We double count duplicate roles (r:$a, r:$b)
            // counts the case where r:$a=r:$b, which TypeDB wouldn't return
            Set<RelationConstraint.RolePlayer> rolePlayers = relationConstraint.players();
            List<Variable> constrainedVars = new ArrayList<>();
            Map<TypeVariable, Long> rolePlayerEstimates = new HashMap<>();
            Map<TypeVariable, Integer> rolePlayerCounts = new HashMap<>();

            double relationTypeEstimate = countPersistedThingsMatchingType(relationConstraint.owner());

            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                constrainedVars.add(rp.player());
                TypeVariable key = rp.roleType().orElse(null);
                rolePlayerCounts.put(key, rolePlayerCounts.getOrDefault(key, 0) + 1);
                if (!rolePlayerEstimates.containsKey(key)) {
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp));
                }
            }

            // TODO: Can improve estimate by collecting List<List<ConstraintModel>> from the triggered rules and doing sum(costCover).
            // Consider owner in the inferred estimate call only if it's not anonymous
            if (relationConstraint.owner().id().isName()) {
                constrainedVars.add(relationConstraint.owner());
            }
            long inferredRelationsEstimate = correspondingConcludable.isPresent() ?
                    estimateInferredAnswerCount(correspondingConcludable.get(), new HashSet<>(constrainedVars)) :
                    0L;
            constrainedVars.add(relationConstraint.owner()); // Now add it.

            return new ConjunctionModel.ConstraintModel.RelationModel(constrainedVars, relationTypeEstimate, rolePlayerEstimates, rolePlayerCounts, inferredRelationsEstimate);
        }

        public ConjunctionModel.ConstraintModel modelForIsa(IsaConstraint isaConstraint, Optional<Concludable> correspondingConcludable) {
            ThingVariable v = isaConstraint.owner();
            long estimate = countPersistedThingsMatchingType(v);
            if (correspondingConcludable.isPresent()) {
                estimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(v));
            }
            return new ConjunctionModel.ConstraintModel.StaticModel(list(v), estimate);
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
