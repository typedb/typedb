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
    private final Map<ResolvableConjunction, ConjunctionModelBuilder> conjunctionModelBuilders;

    private final ConstraintModelFactory constraintModelFactory;
    private final Map<ResolvableConjunction, ConjunctionModel> conjunctionModels;
    private final Map<ResolvableConjunction, Set<Concludable>> cyclicConcludables;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph) {
        this.logicMgr = logicMgr;
        this.constraintModelFactory = new ConstraintModelFactory(this, graph);
        this.conjunctionModelBuilders = new HashMap<>();
        this.conjunctionModels = new HashMap<>();
        this.cyclicConcludables = new HashMap<>();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter) {
        return estimateAnswers(conjunction, variableFilter, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
        if (!conjunctionModels.containsKey(conjunction)) registerConjunctionAndBuildModel(conjunction);
        return conjunctionModels.get(conjunction).estimateAnswers(variableFilter, includedResolvables);
    }

    public void registerConjunctionAndBuildModel(ResolvableConjunction conjunction) {
        registerConjunction(conjunction, new ArrayList<>(), new HashMap<>());
        buildConjunctionModel(conjunction);
    }

    private void registerConjunction(ResolvableConjunction conjunction, ArrayList<ResolvableConjunction> registrationStack, Map<ResolvableConjunction, Concludable> concludableBeingProcessed) {
        if (!this.cyclicConcludables.containsKey(conjunction)) {
            this.cyclicConcludables.put(conjunction, new HashSet<>());
        }

        if (concludableBeingProcessed.containsKey(conjunction)) {
            this.cyclicConcludables.get(conjunction).add(concludableBeingProcessed.get(conjunction));
            for (int i = registrationStack.size() - 1; registrationStack.get(i) != conjunction; i--) {
                this.cyclicConcludables.get(conjunction).add(concludableBeingProcessed.get(conjunction));
            }
        } else {
            registrationStack.add(conjunction);
            // TODO: FIX! THIS DETECTS LASSOS AS CYCLES!
            Set<Resolvable<?>> resolvables = logicMgr.compile(conjunction);

            iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated)
                    .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(dependency -> registerConjunction(dependency, new ArrayList<>(), new HashMap<>())); // Stratified negation -> Fresh set


            iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        iterate(dependencies(concludable)).forEachRemaining(dependency -> {
                            concludableBeingProcessed.put(conjunction, concludable);
                            registerConjunction(dependency, registrationStack, concludableBeingProcessed);
                            concludableBeingProcessed.remove(conjunction);
                        });
                    });
            assert registrationStack.get(registrationStack.size() - 1) == conjunction;
            registrationStack.remove(registrationStack.size() - 1);
        }
    }

    private Set<ResolvableConjunction> dependencies(Concludable concludable) {
        return iterate(logicMgr.applicableRules(concludable).keySet()).map(rule -> rule.condition().conjunction()).toSet();
    }

    private void buildConjunctionModel(ResolvableConjunction conjunction) {
        if (!conjunctionModelBuilders.containsKey(conjunction)) {
            ConjunctionContext conjunctionContext = new ConjunctionContext(conjunction, logicMgr.compile(conjunction), cyclicConcludables.get(conjunction));
            ConjunctionModelBuilder conjunctionModelBuilder = new ConjunctionModelBuilder(this, conjunctionContext);
            conjunctionModelBuilders.put(conjunction, conjunctionModelBuilder);

            // Acyclic estimates
            Set<Resolvable<?>> resolvables = logicMgr.compile(conjunction);
            iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated)
                    .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(this::buildConjunctionModel);

            iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .filter(concludable -> !this.cyclicConcludables.get(conjunction).contains(concludable))
                    .flatMap(concludable -> iterate(dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);

            conjunctionModels.put(conjunction, conjunctionModelBuilder.buildAcyclicModel());

            // cyclic calls to this model will answer based on the acyclic model.
            iterate(conjunctionContext.cyclicConcludables)
                    .flatMap(concludable -> iterate(dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);

            conjunctionModels.put(conjunction, conjunctionModelBuilder.buildCyclicModel());
        }

    }

    private static class ConjunctionContext {
        private final ResolvableConjunction conjunction;
        private final Set<Resolvable<?>> resolvables;
        private final Set<Variable> consideredVariables;
        private Set<Concludable> cyclicConcludables;
        private Set<Concludable> acyclicConcludables;
        
        private ConjunctionContext(ResolvableConjunction conjunction, Set<Resolvable<?>> resolvables, Set<Concludable> cyclicConcludables) {
            this.conjunction = conjunction;
            this.resolvables = resolvables;
            this.consideredVariables = iterate(conjunction.pattern().variables()).filter(Variable::isThing).toSet();
            this.cyclicConcludables = cyclicConcludables;
            this.acyclicConcludables = iterate(resolvables)
                    .filter(resolvable -> resolvable.isConcludable() && !cyclicConcludables.contains(resolvable))
                    .map(Resolvable::asConcludable).toSet();
        }
    }

        private static class ConjunctionModel {
        private final ConjunctionContext conjunctionContext;
        private final Map<Variable, AnswerCountEstimator.LocalModel> variableModels;
        private final HashMap<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> constraintModels;
        private final boolean isCyclic;

        private ConjunctionModel(ConjunctionContext conjunctionContext,
                                 Map<Variable, LocalModel> variableModels, HashMap<Resolvable<?>, List<LocalModel>> constraintModels,
                                 AnswerCountEstimator answerCountEstimator, boolean isCyclic) {
            this.conjunctionContext = conjunctionContext;
            this.variableModels = variableModels;
            this.constraintModels = constraintModels;
            this.isCyclic = isCyclic;
        }

        private long estimateAnswers(Set<Variable> variableFilter, Set<Resolvable<?>> includedResolvables) {
            List<AnswerCountEstimator.LocalModel> includedConstraintModels = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(constraintModels.get(resolvable)))
                    .toList();

            assert variableFilter.stream().allMatch(v -> conjunctionContext.consideredVariables.contains(v)); // TODO: Remove assert
            Set<Variable> validVariableFilter = iterate(variableFilter).filter(conjunctionContext.consideredVariables::contains).toSet();

            Map<Variable, AnswerCountEstimator.LocalModel> costCover = computeGreedyResolvableCoverForVariables(validVariableFilter, includedConstraintModels);
            long ret = costOfVariableCover(validVariableFilter, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        private Map<Variable, AnswerCountEstimator.LocalModel> computeGreedyResolvableCoverForVariables(Set<Variable> variableFilter, List<AnswerCountEstimator.LocalModel> includedConstraintModels) {
            // Does a greedy set cover
            Map<Variable, AnswerCountEstimator.LocalModel> currentCover = new HashMap<>();
            iterate(variableFilter).forEachRemaining(v -> currentCover.put(v, variableModels.get(v)));

            includedConstraintModels.sort(Comparator.comparing(x -> x.estimateAnswers(variableFilter)));
            for (AnswerCountEstimator.LocalModel model : includedConstraintModels) {
                Set<Variable> filteredVariablesInResolvable = model.variables.stream()
                        .filter(variableFilter::contains).collect(Collectors.toSet());

                long currentCostToCover = costOfVariableCover(filteredVariablesInResolvable, currentCover);
                if (currentCostToCover > model.estimateAnswers(filteredVariablesInResolvable)) {
                    filteredVariablesInResolvable.forEach(v -> currentCover.put(v, model));
                }
            }
            return currentCover;
        }

        private static long costOfVariableCover(Set<Variable> variablesToConsider, Map<Variable, AnswerCountEstimator.LocalModel> coverMap) {
            Set<AnswerCountEstimator.LocalModel> subsetCoveredBy = coverMap.keySet().stream().filter(variablesToConsider::contains)
                    .map(coverMap::get).collect(Collectors.toSet());
            return subsetCoveredBy.stream().map(model -> model.estimateAnswers(variablesToConsider)).reduce(1L, (x, y) -> x * y);
        }

    }

    private static class ConjunctionModelBuilder {

        private final ConjunctionContext conjunctionContext;
        private final AnswerCountEstimator answerCountEstimator;
        private final ConstraintModelFactory constraintModelFactory;

        // TODO: these maps should contain VariableModel and ConstraintModel
        private Map<Variable, AnswerCountEstimator.LocalModel> variableModels;
        private final HashMap<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> constraintModels;

        private enum ModelStatus {NOT_STARTED, ACYCLIC_MODEL, COMPLETE}

        private ModelStatus modelStatus;

        private ConjunctionModelBuilder(AnswerCountEstimator answerCountEstimator, ConjunctionContext conjunctionContext) {
            this.answerCountEstimator = answerCountEstimator;
            this.constraintModelFactory = answerCountEstimator.constraintModelFactory;
            this.conjunctionContext = conjunctionContext;

            this.constraintModels = new HashMap<>();
            this.modelStatus = ModelStatus.NOT_STARTED;
        }

        private ConjunctionModel buildAcyclicModel() {
            this.constraintModels.putAll(buildModelsForRetrievables());
            this.constraintModels.putAll(buildModelsForNegateds());
            this.constraintModels.putAll(buildModelsForAcyclicConcludables());
            iterate(conjunctionContext.cyclicConcludables).forEachRemaining(concludable -> this.constraintModels.put(concludable, list()));
            this.variableModels = computeBaselineVariableCover(new HashMap<>());

            // Compute costs
            assert !iterate(conjunctionContext.resolvables).filter(resolvable -> !constraintModels.containsKey(resolvable)).hasNext();
            modelStatus = ModelStatus.ACYCLIC_MODEL;
            return new ConjunctionModel(conjunctionContext, variableModels, constraintModels, answerCountEstimator, false);
        }

        private ConjunctionModel buildCyclicModel() {
            assert this.modelStatus == ModelStatus.ACYCLIC_MODEL;
            Map<Resolvable<?>, List<LocalModel>> modelsForCyclicConcludables = buildModelsForCyclicConcludables();
            Map<Resolvable<?>, List<LocalModel.VariableModel>> generatedVariableModels = constraintModelsForGeneratedVariables();
            iterate(conjunctionContext.cyclicConcludables).forEachRemaining(concludable -> {
                List<LocalModel> combinedModels = new ArrayList<>(modelsForCyclicConcludables.get(concludable));
                if (generatedVariableModels.containsKey(concludable)) {
                    combinedModels.addAll(generatedVariableModels.get(concludable));
                }
                this.constraintModels.put(concludable, combinedModels);
            });
            this.variableModels = computeBaselineVariableCover(generatedVariableModels); // recompute with inferred

            modelStatus = ModelStatus.COMPLETE;
            return new ConjunctionModel(conjunctionContext, variableModels, constraintModels, answerCountEstimator, true);
        }

        private Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> buildModelsForRetrievables() {
            Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> modelsForRetrievables = new HashMap<>();
            iterate(conjunctionContext.resolvables).filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                List<AnswerCountEstimator.LocalModel> constraintModels = iterate(extractConstraintsToModel(retrievable))
                        .map(constraint -> buildConstraintModel(constraint, Optional.empty()))
                        .toList();
                modelsForRetrievables.put(retrievable, constraintModels);
            });
            return modelsForRetrievables;
        }

        private Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> buildModelsForNegateds() {
            Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> modelsForNegateds = new HashMap<>();
            Set<Negated> negateds = iterate(conjunctionContext.resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated).toSet();
            iterate(negateds).forEachRemaining(negated -> modelsForNegateds.put(negated, list()));
            return modelsForNegateds;
        }

        private Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> buildModelsForAcyclicConcludables() {
            Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> modelsForAcyclicConcludables = new HashMap<>();
            iterate(conjunctionContext.acyclicConcludables).forEachRemaining(concludable -> {
                modelsForAcyclicConcludables.put(concludable, list(buildConstraintModel(extractConstraintToModel(concludable), Optional.of(concludable))));
            });
            return modelsForAcyclicConcludables;
        }

        private Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> buildModelsForCyclicConcludables() {
            Map<Resolvable<?>, List<AnswerCountEstimator.LocalModel>> modelsForCyclicConcludables = new HashMap<>();
            iterate(conjunctionContext.cyclicConcludables).forEachRemaining(concludable -> {
                modelsForCyclicConcludables.put(concludable, list(buildConstraintModel(extractConstraintToModel(concludable), Optional.of(concludable))));
            });
            return modelsForCyclicConcludables;
        }

        private Map<Resolvable<?>, List<LocalModel.VariableModel>> constraintModelsForGeneratedVariables() {
            Map<Resolvable<?>, List<LocalModel.VariableModel>> generatedVariableModels = new HashMap<>();
            iterate(conjunctionContext.resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        ThingVariable v = concludable.generating().get();
                        long persistedAnswerCount = constraintModelFactory.countPersistedThingsMatchingType(v.asThing());
                        long inferredAnswerCount = (concludable.isHas() || concludable.isAttribute()) ?
                                constraintModelFactory.attributesCreatedByExplicitHas(concludable) :
                                constraintModelFactory.estimateInferredAnswerCount(concludable, set(v));
                        generatedVariableModels.put(concludable, list(new LocalModel.VariableModel(list(v), persistedAnswerCount + inferredAnswerCount)));
                    });
            return generatedVariableModels;
        }

        private Map<Variable, AnswerCountEstimator.LocalModel> computeBaselineVariableCover(Map<Resolvable<?>, List<LocalModel.VariableModel>> generatedVariableModels) {
            Map<Variable, AnswerCountEstimator.LocalModel> newVariableCover = new HashMap<>();
            iterate(conjunctionContext.consideredVariables).map(Variable::asThing).forEachRemaining(v -> { // baseline
                newVariableCover.put(v, new LocalModel.VariableModel(list(v), constraintModelFactory.countPersistedThingsMatchingType(v)));
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

            assert conjunctionContext.consideredVariables.stream().allMatch(newVariableCover::containsKey);
            return newVariableCover;
        }

        private AnswerCountEstimator.LocalModel buildConstraintModel(Constraint constraint, Optional<Concludable> correspondingConcludable) {
            if (constraint.isThing()) {
                ThingConstraint asThingConstraint = constraint.asThing();
                if (asThingConstraint.isHas()) {
                    return constraintModelFactory.modelForHas(asThingConstraint.asHas(), correspondingConcludable);
                } else if (asThingConstraint.isRelation()) {
                    return constraintModelFactory.modelForRelation(asThingConstraint.asRelation(), correspondingConcludable);
                } else if (asThingConstraint.isIsa()) {
                    return constraintModelFactory.modelForIsa(asThingConstraint.asIsa(), correspondingConcludable);
                } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
            } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }

        private Set<Constraint> extractConstraintsToModel(Retrievable retrievable) {
            Set<Constraint> constraints = new HashSet<>();
            iterate(retrievable.pattern().variables()).flatMap(v -> iterate(v.constraints())).filter(this::isModellable).forEachRemaining(constraints::add);
            return constraints;
        }

        private Constraint extractConstraintToModel(Concludable concludable) {
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
    }

    private static class ConstraintModelFactory {
        private final AnswerCountEstimator answerCountEstimator;
        private final GraphManager graphMgr;

        private ConstraintModelFactory(AnswerCountEstimator answerCountEstimator, GraphManager graphMgr) {
            this.answerCountEstimator = answerCountEstimator;
            this.graphMgr = graphMgr;
        }

        private LocalModel modelForRelation(RelationConstraint relationConstraint, Optional<Concludable> correspondingConcludable) {
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

            // TODO: Can improve estimate by collecting List<List<LocalModel>> from the triggered rules and doing sum(costCover).
            // Consider owner in the inferred estimate call only if it's not anonymous
            if (relationConstraint.owner().id().isName()) {
                constrainedVars.add(relationConstraint.owner());
            }
            long inferredRelationsEstimate = correspondingConcludable.isPresent() ?
                    estimateInferredAnswerCount(correspondingConcludable.get(), new HashSet<>(constrainedVars)) :
                    0L;
            constrainedVars.add(relationConstraint.owner());

            return new LocalModel.RelationModel(relationConstraint, constrainedVars, relationTypeEstimate, rolePlayerEstimates, inferredRelationsEstimate);
        }

        private LocalModel modelForHas(HasConstraint hasConstraint, Optional<Concludable> correspondingConcludable) {
            long hasEdgeEstimate = countPersistedHasEdges(hasConstraint.owner().inferredTypes(), hasConstraint.attribute().inferredTypes());
            if (correspondingConcludable.isPresent()) {
                hasEdgeEstimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(hasConstraint.owner(), hasConstraint.attribute()));
            }
            return new LocalModel.HasModel(hasConstraint, hasEdgeEstimate);
        }

        private LocalModel modelForIsa(IsaConstraint isaConstraint, Optional<Concludable> correspondingConcludable) {
            ThingVariable v = isaConstraint.owner();
            long estimate = countPersistedThingsMatchingType(v);
            if (correspondingConcludable.isPresent()) {
                estimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(v));
            }
            return new LocalModel.IsaModel(isaConstraint, estimate);
        }

        private long estimateInferredAnswerCount(Concludable concludable, Set<Variable> variableFilter) {
            Map<Rule, Set<Unifier>> unifiers = answerCountEstimator.logicMgr.applicableRules(concludable);
            long inferredEstimate = 0;
            for (Rule rule : unifiers.keySet()) {
                for (Unifier unifier : unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variableFilter)
                            .flatMap(v -> iterate(unifier.mapping().get(v.id()))).toSet();
                    Set<Variable> ruleSideVariables;
                    if (rule.conclusion().generating().isPresent() && ruleSideIds.contains(rule.conclusion().generating().get().id())) {
                        // There is one generated variable per combination of ALL variables in the conclusion
                        ruleSideVariables = iterate(rule.conclusion().pattern().variables())
                                .filter(v -> v.isThing() && v != rule.conclusion().generating().get())
                                .toSet();
                    } else {
                        ruleSideVariables = iterate(ruleSideIds).map(id -> rule.conclusion().conjunction().pattern().variable(id)).toSet();
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

    private static abstract class LocalModel {

        final List<Variable> variables;

        private LocalModel(List<Variable> variables) {
            this.variables = variables;
        }

        abstract long estimateAnswers(Set<Variable> variableFilter);

        private abstract static class StaticModel extends LocalModel {
            private final long staticEstimate;

            private StaticModel(List<Variable> variables, long staticEstimate) {
                super(variables);
                this.staticEstimate = staticEstimate;
            }

            @Override
            long estimateAnswers(Set<Variable> variableFilter) {
                return staticEstimate;
            }

        }

        private static class RelationModel extends LocalModel {
            private final RelationConstraint relation;
            private final Map<TypeVariable, Long> rolePlayerEstimates;
            private final double relationTypeEstimate;
            private final long inferredRelationEstimate;
            private final Map<ThingVariable, TypeVariable> rolePlayerTypes;

            private RelationModel(RelationConstraint relation, List<Variable> variables, double relationTypeEstimate,
                                  Map<TypeVariable, Long> rolePlayerEstimates, long inferredRelationEstimate) {
                super(variables); // TODO: Replace with role-players and owner?
                this.relation = relation;
                this.relationTypeEstimate = relationTypeEstimate;
                this.rolePlayerEstimates = rolePlayerEstimates;
                this.inferredRelationEstimate = inferredRelationEstimate;
                this.rolePlayerTypes = new HashMap<>();
                relation.players().forEach(player -> {
                    // Error: null is a valid role-type, but two unspecified roles are not necessarily interchangable.
                    TypeVariable roleType = player.roleType().isPresent() ? player.roleType().get() : null;
                    this.rolePlayerTypes.put(player.player(), roleType);
                });
            }

            @Override
            long estimateAnswers(Set<Variable> variableFilter) {
                long singleRelationEstimate = 1L;
                Map<TypeVariable, Integer> queriedRolePlayerCounts = new HashMap<>();
                for (Variable v : variableFilter) {
                    if (rolePlayerTypes.containsKey(v)) {
                        TypeVariable vType = this.rolePlayerTypes.get(v);
                        queriedRolePlayerCounts.put(vType, 1 + queriedRolePlayerCounts.getOrDefault(vType, 0));
                    }
                }

                if (relationTypeEstimate > 0) {
                    for (TypeVariable key : queriedRolePlayerCounts.keySet()) {
                        assert rolePlayerEstimates.containsKey(key);
                        long avgRolePlayers = Double.valueOf(Math.ceil(rolePlayerEstimates.get(key) / relationTypeEstimate)).longValue();
                        singleRelationEstimate *= nPermuteKforSmallK(avgRolePlayers, queriedRolePlayerCounts.get(key));
                    }
                }

                return Double.valueOf(Math.ceil(relationTypeEstimate * singleRelationEstimate)).longValue() + inferredRelationEstimate;
            }

            private long nPermuteKforSmallK(long n, long k) {
                long ans = 1;
                for (int i = 0; i < k; i++) ans *= n - i;
                return ans;
            }
        }

        private static class HasModel extends StaticModel {
            private final HasConstraint has;

            private HasModel(HasConstraint has, long hasEdgeEstimate) {
                super(list(has.owner(), has.attribute()), hasEdgeEstimate);
                this.has = has;
            }
        }

        private static class IsaModel extends StaticModel {
            private final IsaConstraint isa;

            private IsaModel(IsaConstraint isa, long estimate) {
                super(list(isa.owner()), estimate);
                this.isa = isa;
            }
        }

        private static class VariableModel extends StaticModel {
            private VariableModel(List<Variable> variables, long estimate) {
                super(variables, estimate);
            }
        }
    }
}
