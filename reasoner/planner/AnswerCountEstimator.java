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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
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
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionSummarizer.ConjunctionSummary;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class AnswerCountEstimator {
    private final LogicManager logicMgr;
    private final ConjunctionModelFactory conjunctionModelFactory;
    private final ConjunctionSummarizer conjunctionSummarizer;
    private final Map<ResolvableConjunction, ConjunctionModel> conjunctionModels;
    private final Map<ResolvableConjunction, IncrementalEstimator> fullConjunctionEstimators;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph, ConjunctionSummarizer conjunctionSummarizer) {
        this.logicMgr = logicMgr;
        this.conjunctionModelFactory = new ConjunctionModelFactory(new LocalModelFactory(this, graph));
        this.conjunctionSummarizer = conjunctionSummarizer;
        this.conjunctionModels = new HashMap<>();
        this.fullConjunctionEstimators = new HashMap<>();
    }

    public void buildConjunctionModel(ResolvableConjunction conjunction) {
        if (!conjunctionModels.containsKey(conjunction)) {
            // Acyclic estimates
            ConjunctionSummary conjunctionSummary = conjunctionSummarizer.conjunctionSummary(conjunction);
            iterate(conjunctionSummary.negateds()).flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(this::buildConjunctionModel);
            iterate(conjunctionSummary.acyclicConcludables()).flatMap(concludable -> iterate(conjunctionSummarizer.dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);
            // Don't recurse into acyclic dependencies of cyclic concludables. That overconstrains the disjunction b/w acyclic & cyclic.
            // TODO: ^ Does it though?
            AnswerCountEstimator.ConjunctionModel acyclicModel = conjunctionModelFactory.buildAcyclicModel(conjunctionSummary);
            conjunctionModels.put(conjunction, acyclicModel);

            // cyclic calls to this model will answer based on the acyclic model.
            iterate(conjunctionSummary.cyclicConcludables()).flatMap(concludable -> iterate(conjunctionSummarizer.dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);
            AnswerCountEstimator.ConjunctionModel cyclicModel = conjunctionModelFactory.buildCyclicModel(conjunctionSummary, acyclicModel);
            conjunctionModels.put(conjunction, cyclicModel);
        }
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variables) {
        if (!conjunctionModels.containsKey(conjunction)) buildConjunctionModel(conjunction);
        if (!fullConjunctionEstimators.containsKey(conjunction)) {
            IncrementalEstimator incrementalEstimator = createIncrementalEstimator(conjunction);
            conjunctionModels.get(conjunction).conjunctionSummary.resolvables().forEach(incrementalEstimator::extend);
            fullConjunctionEstimators.put(conjunction, incrementalEstimator);
        }
        IncrementalEstimator estimator = fullConjunctionEstimators.get(conjunction);
        Set<Variable> estimateableVariables = ReasonerPlanner.estimateableVariables(variables);
        return estimator.answerEstimate(estimateableVariables);
    }

    public IncrementalEstimator createIncrementalEstimator(ResolvableConjunction conjunction) {
        if (!conjunctionModels.containsKey(conjunction)) buildConjunctionModel(conjunction);
        return new IncrementalEstimator(conjunctionModels.get(conjunction));
    }

    public static class IncrementalEstimator {
        private final ConjunctionModel conjunctionModel;
        private final Map<LocalModel, Pair<Double, Optional<Variable>>> modelScale;
        private final Map<Variable, Double> minVariableEstimate;
        private final Map<Variable, Set<LocalModel>> affectedModels;

        private IncrementalEstimator(ConjunctionModel conjunctionModel) {
            this.conjunctionModel = conjunctionModel;
            this.modelScale = new HashMap<>();
            this.minVariableEstimate = new HashMap<>();
            this.affectedModels = new HashMap<>();
        }

        public void extend(Resolvable<?> resolvable) {
            Map<Variable, Double> improvedVariableEstimates = new HashMap<>();
            List<LocalModel> models = conjunctionModel.modelsForResolvable(resolvable);
            assert models.stream().allMatch(model -> model.variables.size() > 0);

            iterate(models).flatMap(model -> iterate(model.variables)).forEachRemaining(v -> affectedModels.computeIfAbsent(v, v1 -> new HashSet<>()));

            iterate(models).filter(model -> model.variables.size() == 1).forEachRemaining(model -> {
                Variable v = model.variables.stream().findFirst().get();
                double newEstimate = model.estimateAnswers(model.variables);
                if (newEstimate < minVariableEstimate.getOrDefault(v, Double.MAX_VALUE)) {
                    improvedVariableEstimates.merge(v, newEstimate, Math::min);
                } // Optimisation: Single variable models can only improve estimates once.
            });

            iterate(models).filter(model -> model.variables.size() > 1).forEachRemaining(model -> {
                model.variables.forEach(v -> affectedModels.get(v).add(model));
                modelScale.put(model, new Pair<>(1.0, Optional.empty()));
                Map<Variable, Double> scaledEstimates = applyScaling(model);
                scaledEstimates.forEach((k,v) -> improvedVariableEstimates.merge(k, v, Math::min));
            });

            propagate(improvedVariableEstimates);
        }

        private Map<Variable,Double> applyScaling(LocalModel model) {
            Map<Variable, Double> improvedVariableEstimates = new HashMap<>();
            Pair<Double, Optional<Variable>> scale = modelScale.get(model);
            double bestScaler = scale.first();
            Variable bestScalingVar = scale.second().orElse(null);
            // Find scaling factor
            for (Variable v : model.variables) {
                double ans = (double) model.estimateAnswers(set(v));
                if (minVariableEstimate.containsKey(v) && minVariableEstimate.get(v) / ans < bestScaler) {
                    bestScaler = minVariableEstimate.get(v) / ans;
                    bestScalingVar = v;
                } else if (ans <  minVariableEstimate.getOrDefault(v, Double.MAX_VALUE)) {
                    improvedVariableEstimates.put(v, ans);
                }
            }
            // Find cascading effects of scaling on other vars (to propagate)
            if (bestScalingVar != null) {
                modelScale.put(model, new Pair<>(bestScaler, Optional.of(bestScalingVar)));
                for (Variable v : model.variables) {
                    if (v == bestScalingVar) continue;
                    double newEstimate = scaledEstimate(model, modelScale.get(model), set(v));
                    if (newEstimate < minVariableEstimate.getOrDefault(v, Double.MAX_VALUE)) {
                        improvedVariableEstimates.merge(v, newEstimate, Math::min);
                    }
                }
            }
            return improvedVariableEstimates;
        }

        private void propagate(Map<Variable, Double> minVarUpdates) {
            // Ideally, we'd just remove and add each model again till unaryUpdates is empty.
            int maxIters = Math.max(1, 2 * modelScale.size()); // TODO: Consider pruning out small changes
            Map<Variable, Double> updatesToApply = minVarUpdates;
            while (!updatesToApply.isEmpty() && maxIters > 0) {
                assert iterate(updatesToApply.entrySet()).allMatch(update -> update.getValue() < minVariableEstimate.getOrDefault(update.getKey(), Double.MAX_VALUE));
                minVariableEstimate.putAll(updatesToApply);
                Map<Variable, Double> improvedVariableEstimates = new HashMap<>();
                iterate(updatesToApply.keySet()).flatMap(v -> iterate(this.affectedModels.get(v))).distinct().forEachRemaining(model -> {
                    Map<Variable, Double> scaledEstimates = applyScaling(model);
                    scaledEstimates.forEach((k,v) -> improvedVariableEstimates.merge(k, v, Math::min));
                });
                updatesToApply = improvedVariableEstimates;
                maxIters--;
            }
        }

        public long answerEstimate(Set<Variable> variables) {
            List<LocalModel> relevantModels = iterate(modelScale.keySet())
                    .filter(model -> model.variables.stream().anyMatch(variables::contains))
                    .toList();

            Map<Variable, CoverElement> cover = new HashMap<>();
            iterate(variables).forEachRemaining(v -> cover.put(v, new CoverElement(minVariableEstimate.get(v))));

            Map<LocalModel, Pair<Set<Variable>, Double>> scaledEstimates = new HashMap<>();
            relevantModels.forEach(model -> {
                Set<Variable> modelledVars = intersection(model.variables, variables);
                scaledEstimates.put(model, new Pair<>(modelledVars, scaledEstimate(model, modelScale.get(model), variables)));
            });
            relevantModels.sort(Comparator.comparing(model -> scaledEstimates.get(model).second()));
            for (LocalModel model : relevantModels) {
                if (scaledEstimates.get(model).second() < answerEstimateFromCover(scaledEstimates.get(model).first(), cover)) {
                    CoverElement coverElement = new CoverElement(scaledEstimates.get(model).second()); // Same instance for all keys
                    scaledEstimates.get(model).first().forEach(v -> cover.put(v, coverElement));
                }
            }
            return answerEstimateFromCover(variables, cover);
        }

        private static double scaledEstimate(LocalModel model, Pair<Double, Optional<Variable>> scale, Set<Variable> estimateVariables) {
            assert scale.second().isPresent() || scale.first() == 1.0;
            Set<Variable> variables = new HashSet<>();
            iterate(estimateVariables).filter(model.variables::contains).forEachRemaining(variables::add);
            double ans = model.estimateAnswers(variables);
            if (scale.second().isPresent()) {
                assert scale.first() <= 1.0;
                variables.add(scale.second().get());
                double scaledAns = scale.first() * model.estimateAnswers(variables);
                ans = Math.min(ans, scaledAns);
            }
            return ans;
        }

        private static long answerEstimateFromCover(Set<Variable> variables, Map<Variable, CoverElement> coverMap) {
            double estimate = iterate(variables).map(coverMap::get).distinct()
                    .map(coverElement -> coverElement.estimate).reduce(1.0, (x, y) -> x * y);
            return Math.round(Math.ceil(estimate));
        }

        private static class CoverElement {
            private final double estimate;

            private CoverElement(double estimate) {
                this.estimate = estimate;
            }
        }
    }

    private static class ConjunctionModel {
        private final ConjunctionSummary conjunctionSummary;
        private final Map<Resolvable<?>, List<LocalModel>> constraintModels;
        private final boolean isCyclic;

        private ConjunctionModel(ConjunctionSummary conjunctionSummary, Map<Resolvable<?>, List<LocalModel>> constraintModels,
                                 boolean isCyclic) {
            this.conjunctionSummary = conjunctionSummary;
            this.constraintModels = constraintModels;
            this.isCyclic = isCyclic;
        }

        private List<LocalModel> modelsForResolvable(Resolvable<?> resolvable) {
            return constraintModels.get(resolvable);
        }
    }

    private static class ConjunctionModelFactory {
        private final LocalModelFactory localModelFactory;

        private ConjunctionModelFactory(LocalModelFactory localModelFactory) {
            this.localModelFactory = localModelFactory;
        }

        private ConjunctionModel buildAcyclicModel(ConjunctionSummary conjunctionSummary) {
            Map<Resolvable<?>, List<LocalModel>> models = new HashMap<>();
            iterate(conjunctionSummary.acyclicConcludables())
                    .forEachRemaining(concludable -> models.put(concludable, buildModelsForConcludable(concludable)));

            iterate(conjunctionSummary.retrievables())
                    .forEachRemaining(retrievable -> models.put(retrievable, buildModelsForRetrievable(retrievable)));
            iterate(conjunctionSummary.negateds())
                    .forEachRemaining(negated -> models.put(negated, buildModelsForNegated(negated)));
            // Should we recurse into the acyclic dependencies? Don't think so - Would over-constrain
            iterate(conjunctionSummary.cyclicConcludables())
                    .forEachRemaining(concludable -> models.put(concludable, buildVariableModelsForConcludable(concludable)));

            assert !iterate(conjunctionSummary.resolvables()).filter(resolvable -> !models.containsKey(resolvable)).hasNext();
            return new ConjunctionModel(conjunctionSummary, models, false);
        }

        private ConjunctionModel buildCyclicModel(ConjunctionSummary conjunctionSummary, ConjunctionModel acyclicModel) {
            assert acyclicModel.conjunctionSummary == conjunctionSummary;
            assert !acyclicModel.isCyclic;
            Map<Resolvable<?>, List<LocalModel>> models = new HashMap<>(acyclicModel.constraintModels);
            iterate(conjunctionSummary.cyclicConcludables())
                    .forEachRemaining(concludable -> models.put(concludable, buildModelsForConcludable(concludable)));

            assert !iterate(conjunctionSummary.resolvables()).filter(resolvable -> !models.containsKey(resolvable)).hasNext();
            return new ConjunctionModel(conjunctionSummary, models, true);
        }

        private List<LocalModel> buildModelsForRetrievable(Retrievable retrievable) {
            return iterate(extractConstraints(retrievable))
                    .map(constraint -> buildConstraintModel(constraint, Optional.empty()))
                    .toList();
        }

        // INACCURACY: We don't consider negations, which can reduce the number of answers retrieved.
        private List<LocalModel> buildModelsForNegated(Negated negated) {
            return list();
        }

        private List<LocalModel> buildModelsForConcludable(Concludable concludable) {
            return iterate(concludable.concludableConstraints()).filter(this::isModellable)
                    .map(constraint -> buildConstraintModel(constraint, Optional.of(concludable)))
                    .toList();
        }

        private List<LocalModel> buildVariableModelsForConcludable(Concludable concludable) {
            List<LocalModel> models = new ArrayList<>();
            iterate(concludable.variables()).filter(Variable::isThing).map(v -> localModelFactory.modelForVariable(v, Optional.empty())).forEachRemaining(models::add);
            return models;
        }

        private LocalModel buildConstraintModel(Constraint constraint, Optional<Concludable> correspondingConcludable) {
            if (constraint.isThing()) {
                ThingConstraint asThingConstraint = constraint.asThing();
                if (asThingConstraint.isHas()) {
                    return localModelFactory.modelForHas(asThingConstraint.asHas(), correspondingConcludable);
                } else if (asThingConstraint.isRelation()) {
                    return localModelFactory.modelForRelation(asThingConstraint.asRelation(), correspondingConcludable);
                } else if (asThingConstraint.isIsa()) {
                    return localModelFactory.modelForIsa(asThingConstraint.asIsa(), correspondingConcludable);
                } else if (asThingConstraint.isValue() && asThingConstraint.asValue().isValueIdentity()) {
                    return localModelFactory.modelForValue(asThingConstraint.asValue(), correspondingConcludable);
                } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
            } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }

        private Set<Constraint> extractConstraints(Retrievable retrievable) {
            Set<Constraint> constraints = new HashSet<>();
            iterate(retrievable.pattern().variables()).flatMap(v -> iterate(v.constraints())).filter(this::isModellable).forEachRemaining(constraints::add);
            return constraints;
        }

        private boolean isModellable(Constraint constraint) {
            return constraint.isThing() &&
                    (constraint.asThing().isRelation() || constraint.asThing().isHas() || constraint.asThing().isIsa() ||
                            (constraint.asThing().isValue() && constraint.asThing().asValue().isValueIdentity()));
        }
    }

    private static abstract class LocalModel {

        final Set<Variable> variables;

        private LocalModel(Set<Variable> variables) {
            this.variables = variables;
        }

        abstract long estimateAnswers(Set<Variable> variableFilter);

        public boolean isRelation() { return false; }
        public boolean isHas() { return false; }
        public boolean isIsa() { return false; }
        public boolean isVariable() { return false; }

        private abstract static class StaticModel extends LocalModel {
            private final long staticEstimate;

            private StaticModel(Set<Variable> variables, long staticEstimate) {
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
            private final Map<TypeVariable, Long> typeMaximums;
            private final long relationTypeEstimate;
            private final Map<ThingVariable, TypeVariable> rolePlayerTypes;

            private RelationModel(RelationConstraint relation, long relationTypeEstimate,
                                  Map<TypeVariable, Long> typeMaximums, Map<TypeVariable, Long> rolePlayerEstimates) {
                super(iterate(relation.variables()).filter(Variable::isThing).toSet());
                this.relation = relation;
                this.relationTypeEstimate = relationTypeEstimate;
                this.rolePlayerEstimates = rolePlayerEstimates;
                this.typeMaximums = typeMaximums;
                this.rolePlayerTypes = new HashMap<>();

                relation.players().forEach(player -> {
                    // Error: null is a valid role-type, but two unspecified roles are not necessarily interchangable.
                    TypeVariable roleType = player.roleType().isPresent() ? player.roleType().get() : null;
                    this.rolePlayerTypes.put(player.player(), roleType);
                });
            }

            public boolean isRelation() { return true; }

            @Override
            long estimateAnswers(Set<Variable> variableFilter) {
                long singleRelationEstimate = 1L;
                Map<TypeVariable, Integer> queriedRolePlayerCounts = new HashMap<>();
                for (Variable v : variableFilter) {
                    if (v.isThing() && rolePlayerTypes.containsKey(v.asThing())) {
                        TypeVariable vType = this.rolePlayerTypes.get(v);
                        queriedRolePlayerCounts.put(vType, 1 + queriedRolePlayerCounts.getOrDefault(vType, 0));
                    }
                }

                long typeBasedUpperBound = 1L;
                long typeBasedUpperBoundFromRelations = relationTypeEstimate;
                if (relationTypeEstimate > 0) {
                    for (TypeVariable key : queriedRolePlayerCounts.keySet()) {
                        assert rolePlayerEstimates.containsKey(key);
                        long avgRolePlayers = Double.valueOf(Math.ceil((double) rolePlayerEstimates.get(key) / relationTypeEstimate)).longValue();
                        singleRelationEstimate *= nPermuteKforSmallK(avgRolePlayers, queriedRolePlayerCounts.get(key));
                        typeBasedUpperBoundFromRelations *= nPermuteKforSmallK(avgRolePlayers, queriedRolePlayerCounts.get(key));
                        typeBasedUpperBound *= nPermuteKforSmallK(typeMaximums.get(key), queriedRolePlayerCounts.get(key));
                    }
                }

                // How do you correctly query $q($x) from $r($a, $b) ?
                if (variableFilter.contains(relation.owner()))
                    typeBasedUpperBound = typeBasedUpperBoundFromRelations; // We need the type based upper bound for the relationEstimate too

                return Math.min(typeBasedUpperBound, Double.valueOf(Math.ceil(relationTypeEstimate * singleRelationEstimate)).longValue());
            }

            private long nPermuteKforSmallK(long n, long k) {
                long ans = 1;
                for (int i = 0; i < k; i++) ans *= n - i;
                return ans;
            }

            @Override
            public String toString() {
                return "RelationModel[" + relation.toString() + "]";
            }
        }

        private static class HasModel extends LocalModel {
            private final HasConstraint has;
            private final long hasEdgeEstimate;
            private final long ownerEstimate;
            private final long attributeEstimate;

            private HasModel(HasConstraint has, long hasEdgeEstimate, long ownerEstimate, long attributeEstimate) {
                super(set(has.owner(), has.attribute()));
                this.has = has;
                this.hasEdgeEstimate = hasEdgeEstimate;
                this.ownerEstimate = ownerEstimate;
                this.attributeEstimate = attributeEstimate;
            }

            public boolean isHas() { return true; }

            @Override
            long estimateAnswers(Set<Variable> variableFilter) {
                long answerEstimate = 1;
                if (variableFilter.contains(has.owner())) answerEstimate *= ownerEstimate;
                if (variableFilter.contains(has.attribute())) answerEstimate *= attributeEstimate;
                return Math.min(answerEstimate, this.hasEdgeEstimate);
            }

            @Override
            public String toString() {
                return "HasModel[" + has.toString() + "]";
            }
        }

        private static class IsaModel extends StaticModel {
            private final IsaConstraint isa;

            private IsaModel(IsaConstraint isa, long estimate) {
                super(set(isa.owner()), estimate);
                this.isa = isa;
            }

            public boolean isIsa() { return true; }

            @Override
            public String toString() {
                return "IsaModel[" + isa.toString() + "]";
            }
        }

        private static class VariableModel extends StaticModel {
            private VariableModel(Set<Variable> variables, long estimate) {
                super(variables, estimate);
            }

            public boolean isVariable() { return true; }

            @Override
            public String toString() {
                return "VariableModel[" + variables.stream().findAny() + "=" + estimateAnswers(set()) + "]";
            }
        }
    }

    private static class LocalModelFactory {
        private final AnswerCountEstimator answerCountEstimator;
        private final GraphManager graphMgr;

        private LocalModelFactory(AnswerCountEstimator answerCountEstimator, GraphManager graphMgr) {
            this.answerCountEstimator = answerCountEstimator;
            this.graphMgr = graphMgr;
        }

        private LocalModel.RelationModel modelForRelation(RelationConstraint relationConstraint, Optional<Concludable> correspondingConcludable) {
            // Assumes role-players are evenly distributed.
            // Small inaccuracy: We double count duplicate roles (r:$a, r:$b)
            // counts the case where r:$a=r:$b, which TypeDB wouldn't return
            Set<RelationConstraint.RolePlayer> rolePlayers = relationConstraint.players();
            Map<TypeVariable, Long> rolePlayerEstimates = new HashMap<>();
            Map<TypeVariable, Integer> rolePlayerCounts = new HashMap<>();

            Map<TypeVariable, Long> typeMaximums = new HashMap<>();
            long relationUpperBound = 1L;
            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                TypeVariable key = rp.roleType().orElse(null);
                if (!typeMaximums.containsKey(key)) {
                    typeMaximums.put(key, countPersistedThingsMatchingType(rp.player()));
                }
                relationUpperBound *= typeMaximums.get(key);
            }

            long persistedRelationEstimate = countPersistedThingsMatchingType(relationConstraint.owner());
            // TODO: Can improve estimate by collecting List<List<LocalModel>> from the triggered rules and doing sum(costCover).
            long inferredRelationsEstimate = 0L;
            if (correspondingConcludable.isPresent()) {
                List<Variable> constrainedVars = new ArrayList<>();
                iterate(relationConstraint.players()).forEachRemaining(player -> constrainedVars.add(player.player()));
                if (relationConstraint.owner().id().isName()) {
                    constrainedVars.add(relationConstraint.owner());
                }
                inferredRelationsEstimate = estimateInferredAnswerCount(correspondingConcludable.get(), new HashSet<>(constrainedVars));
            }

            inferredRelationsEstimate = Math.min(inferredRelationsEstimate, relationUpperBound - persistedRelationEstimate);

            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                TypeVariable key = rp.roleType().orElse(null);
                rolePlayerCounts.put(key, rolePlayerCounts.getOrDefault(key, 0) + 1);
                if (!rolePlayerEstimates.containsKey(key)) {
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp) + inferredRelationsEstimate);
                }
            }

            return new LocalModel.RelationModel(relationConstraint, inferredRelationsEstimate + persistedRelationEstimate, typeMaximums, rolePlayerEstimates);
        }

        private LocalModel.HasModel modelForHas(HasConstraint hasConstraint, Optional<Concludable> correspondingConcludable) {
            long hasEdgeEstimate = countPersistedHasEdges(hasConstraint.owner().inferredTypes(), hasConstraint.attribute().inferredTypes());
            long ownerEstimate = countPersistedThingsMatchingType(hasConstraint.owner());
            long attributeEstimate = countPersistedThingsMatchingType(hasConstraint.attribute());
            if (correspondingConcludable.isPresent()) {
                hasEdgeEstimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(hasConstraint.owner(), hasConstraint.attribute()));
                attributeEstimate += attributesCreatedByExplicitHas(correspondingConcludable.get());
                if (ownerEstimate < hasEdgeEstimate / attributeEstimate) {
                    boolean rulesConcludeOwner = iterate(hasConstraint.owner().inferredTypes()).flatMap(ownerType -> iterate(answerCountEstimator.logicMgr.rulesConcluding(ownerType))).hasNext();
                    if (rulesConcludeOwner) ownerEstimate = hasEdgeEstimate / attributeEstimate;
                }
            }
            return new LocalModel.HasModel(hasConstraint, hasEdgeEstimate, ownerEstimate, attributeEstimate);
        }

        private LocalModel.IsaModel modelForIsa(IsaConstraint isaConstraint, Optional<Concludable> correspondingConcludable) {
            ThingVariable owner = isaConstraint.owner();
            long estimate = countPersistedThingsMatchingType(owner);
            if (correspondingConcludable.isPresent()) {
                estimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(owner));
            }
            return new LocalModel.IsaModel(isaConstraint, estimate);
        }

        public LocalModel modelForValue(ValueConstraint<?> asValue, Optional<Concludable> correspondingConcludable) {
            if (asValue.isValueIdentity()) {
                return new LocalModel.VariableModel(set(asValue.owner()), 1L);
            } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }

        private LocalModel.VariableModel modelForVariable(Variable v, Optional<Concludable> concludable) {
            long persistedAnswerCount = countPersistedThingsMatchingType(v.asThing());
            long inferredAnswerCount = 0;
            Concludable sourceConcludable;
            if (concludable.isPresent() && v == concludable.get().generating().orElse(null)) {
                sourceConcludable = concludable.get();
                inferredAnswerCount = (sourceConcludable.isHas() || sourceConcludable.isAttribute()) ?
                        attributesCreatedByExplicitHas(sourceConcludable) :
                        estimateInferredAnswerCount(sourceConcludable, set(v));
            }
            return new LocalModel.VariableModel(set(v), persistedAnswerCount + inferredAnswerCount);
        }

        private long estimateInferredAnswerCount(Concludable concludable, Set<Variable> variableFilter) {
            Map<Rule, Set<Unifier>> unifiers = answerCountEstimator.logicMgr.applicableRules(concludable);
            long inferredEstimate = 0;
            for (Rule rule : unifiers.keySet()) {
                for (Unifier unifier : unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variableFilter).filter(v -> v.id().isRetrievable())
                            .flatMap(v -> iterate(unifier.mapping().get(v.id().asRetrievable()))).toSet();
                    Set<Variable> ruleSideVariables;
                    if ((concludable.isRelation() || concludable.isIsa())
                            && rule.conclusion().generating().isPresent() && ruleSideIds.contains(rule.conclusion().generating().get().id())) {
                        // There is one generated variable per combination of ALL variables in the conclusion
                        ruleSideIds = new HashSet<>(rule.conclusion().pattern().retrieves());
                    }

                    ruleSideVariables = iterate(ruleSideIds)
                            .filter(id -> rule.condition().conjunction().pattern().retrieves().contains(id)) // avoids constant has
                            .map(id -> rule.condition().conjunction().pattern().variable(id)).toSet();
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
            return iterate(ownerTypes).map(ownerType ->
                    iterate(attributeTypes)
                            .map(attrType -> graphMgr.data().stats().hasEdgeCount(ownerType, attrType))
                            .reduce(0L, Long::sum)
            ).reduce(0L, Long::sum);
        }
    }
}
