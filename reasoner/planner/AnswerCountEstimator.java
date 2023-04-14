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
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph.ConjunctionNode;
import com.vaticle.typedb.core.traversal.common.Identifier;

import javax.annotation.Nullable;
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
import static com.vaticle.typedb.core.reasoner.planner.RecursivePlanner.estimateableVariables;

public class AnswerCountEstimator {
    private final LogicManager logicMgr;
    private final ConjunctionModelFactory conjunctionModelFactory;
    private final ConjunctionGraph conjunctionGraph;
    private final Map<ResolvableConjunction, ConjunctionModel> conjunctionModels;
    private final Map<ResolvableConjunction, IncrementalEstimator> fullConjunctionEstimators;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph, ConjunctionGraph conjunctionGraph) {
        this.logicMgr = logicMgr;
        this.conjunctionModelFactory = new ConjunctionModelFactory(new LocalModelFactory(this, graph));
        this.conjunctionGraph = conjunctionGraph;
        this.conjunctionModels = new HashMap<>();
        this.fullConjunctionEstimators = new HashMap<>();
    }

    public void buildConjunctionModel(ResolvableConjunction conjunction) {
        if (!conjunctionModels.containsKey(conjunction)) {
            ConjunctionNode conjunctionNode = conjunctionGraph.conjunctionNode(conjunction);
            iterate(conjunctionNode.negateds()).flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(this::buildConjunctionModel);
            iterate(conjunctionNode.acyclicConcludables()).flatMap(concludable -> iterate(conjunctionGraph.dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);
            AnswerCountEstimator.ConjunctionModel acyclicModel = conjunctionModelFactory.buildAcyclicModel(conjunctionNode);
            conjunctionModels.put(conjunction, acyclicModel);

            iterate(conjunctionNode.cyclicConcludables()).flatMap(concludable -> iterate(conjunctionGraph.dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);
            AnswerCountEstimator.ConjunctionModel cyclicModel = conjunctionModelFactory.buildCyclicModel(conjunctionNode, acyclicModel);
            conjunctionModels.put(conjunction, cyclicModel);
        }
    }

    public double estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variables) {
        if (!conjunctionModels.containsKey(conjunction)) buildConjunctionModel(conjunction);
        if (!fullConjunctionEstimators.containsKey(conjunction)) {
            IncrementalEstimator incrementalEstimator = createIncrementalEstimator(conjunction);
            conjunctionModels.get(conjunction).conjunctionNode.resolvables().forEach(incrementalEstimator::extend);
            fullConjunctionEstimators.put(conjunction, incrementalEstimator);
        }
        IncrementalEstimator estimator = fullConjunctionEstimators.get(conjunction);
        Set<Variable> estimateableVariables = ReasonerPlanner.estimateableVariables(variables);
        return estimator.answerEstimate(estimateableVariables);
    }

    public double localEstimate(ResolvableConjunction conjunction, Resolvable<?> resolvable, Set<Variable> variables) {
        return conjunctionModels.get(conjunction).localEstimate(this, resolvable, variables);
    }

    public IncrementalEstimator createIncrementalEstimator(ResolvableConjunction conjunction) {
        return createIncrementalEstimator(conjunction, set());
    }

    public IncrementalEstimator createIncrementalEstimator(ResolvableConjunction conjunction, Set<Variable> mode) {
        if (!conjunctionModels.containsKey(conjunction)) buildConjunctionModel(conjunction);
        return new IncrementalEstimator(conjunctionModels.get(conjunction), conjunctionModelFactory.localModelFactory, mode);
    }

    public static class IncrementalEstimator {
        private final ConjunctionModel conjunctionModel;
        private final LocalModelFactory localModelFactory;
        private final Map<LocalModel, Pair<Double, Optional<Variable>>> modelScale;
        private final Map<Variable, Double> minVariableEstimate;
        private final Map<Variable, Set<LocalModel>> affectedModels;

        private IncrementalEstimator(ConjunctionModel conjunctionModel, LocalModelFactory localModelFactory, Set<Variable> initialBounds) {
            this.conjunctionModel = conjunctionModel;
            this.localModelFactory = localModelFactory;
            this.modelScale = new HashMap<>();
            this.minVariableEstimate = new HashMap<>();
            initialBounds.forEach(v -> this.minVariableEstimate.put(v, 1.0));
            this.affectedModels = new HashMap<>();
        }

        public void extend(Resolvable<?> resolvable) {
            assert conjunctionModel.conjunctionNode.resolvables().contains(resolvable);
            Map<Variable, Double> improvedVariableEstimates = new HashMap<>();
            List<LocalModel> models = conjunctionModel.modelsForResolvable(resolvable);
            iterate(models).flatMap(model -> iterate(model.variables)).forEachRemaining(v -> affectedModels.computeIfAbsent(v, v1 -> new HashSet<>()));

            iterate(models).filter(model -> model.variables.size() == 1).forEachRemaining(model -> {
                Variable v = iterate(model.variables).next();
                double newEstimate = model.estimateAnswers(model.variables);
                if (newEstimate < minVariableEstimate.getOrDefault(v, Double.MAX_VALUE)) {
                    improvedVariableEstimates.merge(v, newEstimate, Math::min);
                } // Optimisation: Single variable models can only improve estimates once.
            });

            iterate(models).filter(model -> model.variables.size() > 1).forEachRemaining(model -> {
                model.variables.forEach(v -> affectedModels.get(v).add(model));
                modelScale.put(model, new Pair<>(1.0, Optional.empty()));
                Map<Variable, Double> scaledEstimates = applyScaling(model);
                scaledEstimates.forEach((k, v) -> improvedVariableEstimates.merge(k, v, Math::min));
            });

            propagate(improvedVariableEstimates);
            // Lazy-fix: Give variables not included in any constraint a pessimistic estimate (Inaccuracy: We don't consider inferred values)
            iterate(estimateableVariables(resolvable.variables())).filter(v -> !minVariableEstimate.containsKey(v))
                    .forEachRemaining(v -> minVariableEstimate.put(v, (double) localModelFactory.countPersistedThingsMatchingType(v.asThing())));
        }

        private Map<Variable, Double> applyScaling(LocalModel model) {
            Map<Variable, Double> improvedVariableEstimates = new HashMap<>();
            Pair<Double, Optional<Variable>> scale = modelScale.get(model);
            double bestScaler = scale.first();
            Variable bestScalingVar = scale.second().orElse(null);
            // Find scaling factor
            for (Variable v : model.variables) {
                double ans = (double) Math.max(1, model.estimateAnswers(set(v)));
                if (minVariableEstimate.containsKey(v) && minVariableEstimate.get(v) / ans < bestScaler) {
                    bestScaler = minVariableEstimate.get(v) / ans;
                    bestScalingVar = v;
                } else if (ans < minVariableEstimate.getOrDefault(v, Double.MAX_VALUE)) {
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
            int maxIters = Math.max(1, 2 * modelScale.size()); // TODO: Ignoring small changes can force early convergence
            Map<Variable, Double> updatesToApply = minVarUpdates;
            while (!updatesToApply.isEmpty() && maxIters > 0) {
                assert iterate(updatesToApply.entrySet()).allMatch(update -> update.getValue() < minVariableEstimate.getOrDefault(update.getKey(), Double.MAX_VALUE));
                minVariableEstimate.putAll(updatesToApply);
                Map<Variable, Double> improvedVariableEstimates = new HashMap<>();
                iterate(updatesToApply.keySet()).flatMap(v -> iterate(this.affectedModels.get(v))).distinct().forEachRemaining(model -> {
                    Map<Variable, Double> scaledEstimates = applyScaling(model);
                    scaledEstimates.forEach((k, v) -> improvedVariableEstimates.merge(k, v, Math::min));
                });
                updatesToApply = improvedVariableEstimates;
                maxIters--;
            }
        }

        public double answerEstimate(Set<Variable> variables) {
            List<LocalModel> relevantModels = iterate(modelScale.keySet())
                    .filter(model -> iterate(model.variables).anyMatch(variables::contains))
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
                    CoverElement coverElement = new CoverElement(scaledEstimates.get(model).second());
                    scaledEstimates.get(model).first().forEach(v -> cover.put(v, coverElement));
                }
            }

            assert !variables.isEmpty() || answerEstimateFromCover(variables, cover) == 1;
            return Math.round(Math.ceil(answerEstimateFromCover(variables, cover)));
        }

        public double answerSetSize() {
            // TODO: ignore the anonymous variables iff/when ConjunctionController can ignore them when branching CompoundStreams
            return answerEstimate(minVariableEstimate.keySet());
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

        private static double answerEstimateFromCover(Set<Variable> variables, Map<Variable, CoverElement> coverMap) {
            double estimate = iterate(variables).map(coverMap::get).distinct()
                    .map(coverElement -> coverElement.estimate).reduce(1.0, (x, y) -> x * y);
            return Math.round(Math.ceil(estimate));
        }

        public IncrementalEstimator clone() {
            // initialbounds = set() is ok, the next steps will take care of it.
            IncrementalEstimator cloned = new IncrementalEstimator(conjunctionModel, localModelFactory, set());
            cloned.minVariableEstimate.putAll(this.minVariableEstimate);
            cloned.modelScale.putAll(this.modelScale);
            this.affectedModels.forEach((k, v) -> cloned.affectedModels.put(k, new HashSet<>(v)));
            return cloned;
        }

        private static class CoverElement {
            private final double estimate;

            private CoverElement(double estimate) {
                this.estimate = estimate;
            }
        }
    }

    private static class ConjunctionModel {
        private final ConjunctionNode conjunctionNode;
        private final Map<Resolvable<?>, List<LocalModel>> constraintModels;
        private final boolean isCyclic;

        private final Map<Resolvable<?>, IncrementalEstimator> resolvableEstimators;

        private ConjunctionModel(ConjunctionNode conjunctionNode, Map<Resolvable<?>, List<LocalModel>> constraintModels,
                                 boolean isCyclic) {
            this.conjunctionNode = conjunctionNode;
            this.constraintModels = constraintModels;
            this.isCyclic = isCyclic;
            this.resolvableEstimators = new HashMap<>();
        }

        private List<LocalModel> modelsForResolvable(Resolvable<?> resolvable) {
            return constraintModels.get(resolvable);
        }

        private double localEstimate(AnswerCountEstimator answerCountEstimator, Resolvable<?> resolvable, Set<Variable> variables) {
            return resolvableEstimators.computeIfAbsent(resolvable, res -> {
                IncrementalEstimator estimator = answerCountEstimator.createIncrementalEstimator(conjunctionNode.conjunction());
                estimator.extend(res);
                return estimator;
            }).answerEstimate(variables);
        }
    }

    private static class ConjunctionModelFactory {
        private final LocalModelFactory localModelFactory;

        private ConjunctionModelFactory(LocalModelFactory localModelFactory) {
            this.localModelFactory = localModelFactory;
        }

        private ConjunctionModel buildAcyclicModel(ConjunctionNode conjunctionNode) {
            Map<Resolvable<?>, List<LocalModel>> models = new HashMap<>();
            iterate(conjunctionNode.acyclicConcludables())
                    .forEachRemaining(concludable -> models.put(concludable, buildModelsForConcludable(concludable)));

            iterate(conjunctionNode.retrievables())
                    .forEachRemaining(retrievable -> models.put(retrievable, buildModelsForRetrievable(retrievable)));
            iterate(conjunctionNode.negateds())
                    .forEachRemaining(negated -> models.put(negated, buildModelsForNegated(negated)));
            // Should we recurse into the acyclic dependencies? Don't think so - Would over-constrain
            iterate(conjunctionNode.cyclicConcludables())
                    .forEachRemaining(concludable -> models.put(concludable, buildVariableModelsForConcludable(concludable)));

            assert !iterate(conjunctionNode.resolvables()).filter(resolvable -> !models.containsKey(resolvable)).hasNext();
            return new ConjunctionModel(conjunctionNode, models, false);
        }

        private ConjunctionModel buildCyclicModel(ConjunctionNode conjunctionNode, ConjunctionModel acyclicModel) {
            assert acyclicModel.conjunctionNode == conjunctionNode;
            assert !acyclicModel.isCyclic;
            Map<Resolvable<?>, List<LocalModel>> models = new HashMap<>(acyclicModel.constraintModels);
            iterate(conjunctionNode.cyclicConcludables())
                    .forEachRemaining(concludable -> models.put(concludable, buildModelsForConcludable(concludable)));

            assert !iterate(conjunctionNode.resolvables()).filter(resolvable -> !models.containsKey(resolvable)).hasNext();
            return new ConjunctionModel(conjunctionNode, models, true);
        }

        private List<LocalModel> buildModelsForRetrievable(Retrievable retrievable) {
            return iterate(extractConstraints(retrievable))
                    .map(constraint -> buildConstraintModel(constraint, null))
                    .toList();
        }

        // INACCURACY: We don't consider negations, which can reduce the number of answers retrieved.
        private List<LocalModel> buildModelsForNegated(Negated negated) {
            return list();
        }

        private List<LocalModel> buildModelsForConcludable(Concludable concludable) {
            return iterate(concludable.concludableConstraints()).filter(this::isModellable)
                    .map(constraint -> buildConstraintModel(constraint, concludable))
                    .toList();
        }

        private List<LocalModel> buildVariableModelsForConcludable(Concludable concludable) {
            List<LocalModel> models = new ArrayList<>();
            iterate(concludable.variables()).filter(Variable::isThing).map(v -> localModelFactory.modelForVariable(v, null)).forEachRemaining(models::add);
            return models;
        }

        private LocalModel buildConstraintModel(Constraint constraint, @Nullable Concludable correspondingConcludable) {
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

        abstract double estimateAnswers(Set<Variable> variableFilter);

        public boolean isRelation() {
            return false;
        }

        public boolean isHas() {
            return false;
        }

        public boolean isIsa() {
            return false;
        }

        public boolean isVariable() {
            return false;
        }

        private abstract static class StaticModel extends LocalModel {
            private final double staticEstimate;

            private StaticModel(Set<Variable> variables, double staticEstimate) {
                super(variables);
                this.staticEstimate = staticEstimate;
            }

            @Override
            double estimateAnswers(Set<Variable> variableFilter) {
                return staticEstimate;
            }
        }

        private static class RelationModel extends LocalModel {
            private final RelationConstraint relation;
            private final Map<Label, Long> rolePlayerEstimates;
            private final Map<ThingVariable, Long> typeMaximums;
            private final double relationTypeEstimate;
            private final Map<ThingVariable, Label> rolePlayerTypes;

            private RelationModel(RelationConstraint relation, double relationTypeEstimate,
                                  Map<ThingVariable, Long> typeMaximums, Map<Label, Long> rolePlayerEstimates) {
                super(iterate(relation.variables()).filter(Variable::isThing).toSet());
                this.relation = relation;
                this.relationTypeEstimate = relationTypeEstimate;
                this.rolePlayerEstimates = rolePlayerEstimates;
                this.typeMaximums = typeMaximums;
                this.rolePlayerTypes = new HashMap<>();

                relation.players().forEach(player -> {
                    // Inaccuracy: null is a valid role-type, but two unspecified roles are not necessarily interchangable.
                    TypeVariable roleType = player.roleType().isPresent() ? player.roleType().get() : null;
                    this.rolePlayerTypes.put(player.player(), getRoleType(player));
                });
            }

            static Label getRoleType(RelationConstraint.RolePlayer player) {
                if (player.roleType().isPresent() && player.roleType().get().label().isPresent()) {
                    return player.roleType().get().label().get().properLabel();
                } else if (player.inferredRoleTypes().size() == 1) return iterate(player.inferredRoleTypes()).next();
                else return null;
            }

            public boolean isRelation() {
                return true;
            }

            @Override
            double estimateAnswers(Set<Variable> variableFilter) {
                double singleRelationEstimate = 1L;
                Map<Label, Integer> queriedRolePlayerCounts = new HashMap<>();
                double typeBasedUpperBound = 1L;
                for (Variable v : variableFilter) {
                    if (v.isThing() && rolePlayerTypes.containsKey(v.asThing())) {
                        Label role = this.rolePlayerTypes.get(v);
                        queriedRolePlayerCounts.put(role, 1 + queriedRolePlayerCounts.getOrDefault(role, 0));
                        typeBasedUpperBound *= typeMaximums.get(v);
                    }
                }

                double typeBasedUpperBoundFromRelations = relationTypeEstimate;
                if (relationTypeEstimate > 0) {
                    for (Label key : queriedRolePlayerCounts.keySet()) {
                        assert rolePlayerEstimates.containsKey(key);
                        long avgRolePlayers = Double.valueOf(Math.ceil((double) rolePlayerEstimates.get(key) / relationTypeEstimate)).longValue();
                        singleRelationEstimate *= nPermuteKforSmallK(avgRolePlayers, queriedRolePlayerCounts.get(key));
                        typeBasedUpperBoundFromRelations *= nPermuteKforSmallK(avgRolePlayers, queriedRolePlayerCounts.get(key));
                    }
                }

                // How do you correctly query $q($x) from $r($a, $b) ?
                if (variableFilter.contains(relation.owner()))
                    typeBasedUpperBound = typeBasedUpperBoundFromRelations; // We need the type based upper bound for the relationEstimate too

                return Math.min(typeBasedUpperBound, Double.valueOf(Math.ceil(relationTypeEstimate * singleRelationEstimate)).doubleValue());
            }

            private double nPermuteKforSmallK(long n, long k) {
                assert n >= k;
                n = Math.max(n, k); // Guard against 0. (Although 0 is the right answer it's not useful)
                double ans = 1;
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
            private final double hasEdgeEstimate;
            private final double ownerEstimate;
            private final double attributeEstimate;

            private HasModel(HasConstraint has, double hasEdgeEstimate, double ownerEstimate, double attributeEstimate) {
                super(set(has.owner(), has.attribute()));
                this.has = has;
                this.hasEdgeEstimate = hasEdgeEstimate;
                this.ownerEstimate = ownerEstimate;
                this.attributeEstimate = attributeEstimate;
            }

            public boolean isHas() {
                return true;
            }

            @Override
            double estimateAnswers(Set<Variable> variableFilter) {
                double answerEstimate = 1;
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

            private IsaModel(IsaConstraint isa, double estimate) {
                super(set(isa.owner()), estimate);
                this.isa = isa;
            }

            public boolean isIsa() {
                return true;
            }

            @Override
            public String toString() {
                return "IsaModel[" + isa.toString() + "]";
            }
        }

        private static class VariableModel extends StaticModel {
            private VariableModel(Set<Variable> variables, double estimate) {
                super(variables, estimate);
            }

            public boolean isVariable() {
                return true;
            }

            @Override
            public String toString() {
                return "VariableModel[" + iterate(variables).next() + "=" + estimateAnswers(set()) + "]";
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

        private LocalModel.RelationModel modelForRelation(RelationConstraint relationConstraint, @Nullable Concludable correspondingConcludable) {
            // Assumes role-players are evenly distributed.
            // Small inaccuracy: We double count duplicate roles (r:$a, r:$b)
            // counts the case where r:$a=r:$b, which TypeDB wouldn't return
            Set<RelationConstraint.RolePlayer> rolePlayers = relationConstraint.players();
            Map<Label, Long> rolePlayerEstimates = new HashMap<>();
            Map<Label, Integer> rolePlayerCounts = new HashMap<>();

            Map<ThingVariable, Long> typeMaximums = new HashMap<>();
            double typeBasedUpperBound = 1L;
            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                typeMaximums.put(rp.player(), countPersistedThingsMatchingType(rp.player()));
                typeBasedUpperBound *= typeMaximums.get(rp.player());
            }

            double persistedRelationEstimate = countPersistedThingsMatchingType(relationConstraint.owner());
            // TODO: Can improve estimate by collecting List<List<LocalModel>> from the triggered rules and doing sum(costCover).
            double inferredRelationsEstimate = 0L;
            if (correspondingConcludable != null) {
                List<Variable> constrainedVars = new ArrayList<>();
                iterate(relationConstraint.players()).forEachRemaining(player -> constrainedVars.add(player.player()));
                if (relationConstraint.owner().id().isName()) {
                    constrainedVars.add(relationConstraint.owner());
                }
                inferredRelationsEstimate = estimateInferredAnswerCount(correspondingConcludable, new HashSet<>(constrainedVars));
            }

            inferredRelationsEstimate = Math.min(inferredRelationsEstimate, typeBasedUpperBound - persistedRelationEstimate);

            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                Label key = LocalModel.RelationModel.getRoleType(rp);
                rolePlayerCounts.put(key, rolePlayerCounts.getOrDefault(key, 0) + 1);
                if (!rolePlayerEstimates.containsKey(key)) {
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp));
                }
            }

            for (Label key : rolePlayerCounts.keySet()) {
                rolePlayerEstimates.put(key, rolePlayerEstimates.get(key) +
                        rolePlayerCounts.get(key) * Double.valueOf(Math.ceil(inferredRelationsEstimate)).longValue());
            }

            return new LocalModel.RelationModel(relationConstraint, inferredRelationsEstimate + persistedRelationEstimate, typeMaximums, rolePlayerEstimates);
        }

        private LocalModel.HasModel modelForHas(HasConstraint hasConstraint, @Nullable Concludable correspondingConcludable) {
            double hasEdgeEstimate = countPersistedHasEdges(hasConstraint.owner().inferredTypes(), hasConstraint.attribute().inferredTypes());
            double ownerEstimate = countPersistedThingsMatchingType(hasConstraint.owner());
            double attributeEstimate = countPersistedThingsMatchingType(hasConstraint.attribute());
            if (correspondingConcludable != null) {
                hasEdgeEstimate += estimateInferredAnswerCount(correspondingConcludable, set(hasConstraint.owner(), hasConstraint.attribute()));
                attributeEstimate += attributesCreatedByExplicitHas(correspondingConcludable);
                if (attributeEstimate != 0 && ownerEstimate < hasEdgeEstimate / attributeEstimate) {
                    boolean rulesConcludeOwner = iterate(hasConstraint.owner().inferredTypes()).flatMap(ownerType -> iterate(answerCountEstimator.logicMgr.rulesConcluding(ownerType))).hasNext();
                    if (rulesConcludeOwner) ownerEstimate = hasEdgeEstimate / attributeEstimate;
                }
            }
            return new LocalModel.HasModel(hasConstraint, hasEdgeEstimate, ownerEstimate, attributeEstimate);
        }

        private LocalModel.IsaModel modelForIsa(IsaConstraint isaConstraint, @Nullable Concludable correspondingConcludable) {
            ThingVariable owner = isaConstraint.owner();
            double estimate = countPersistedThingsMatchingType(owner);
            if (correspondingConcludable != null) {
                estimate += estimateInferredAnswerCount(correspondingConcludable, set(owner));
            }
            return new LocalModel.IsaModel(isaConstraint, estimate);
        }

        public LocalModel modelForValue(ValueConstraint<?> asValue, @Nullable Concludable correspondingConcludable) {
            if (asValue.isValueIdentity()) {
                return new LocalModel.VariableModel(set(asValue.owner()), 1L);
            } else throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }

        private LocalModel.VariableModel modelForVariable(Variable v, @Nullable Concludable concludable) {
            double persistedAnswerCount = countPersistedThingsMatchingType(v.asThing());
            double inferredAnswerCount = 0;
            if (concludable != null && v == concludable.generating().orElse(null)) {
                inferredAnswerCount = (concludable.isHas() || concludable.isAttribute()) ?
                        attributesCreatedByExplicitHas(concludable) :
                        estimateInferredAnswerCount(concludable, set(v));
            }
            return new LocalModel.VariableModel(set(v), persistedAnswerCount + inferredAnswerCount);
        }

        private double estimateInferredAnswerCount(Concludable concludable, Set<Variable> variableFilter) {
            Map<Rule, Set<Unifier>> unifiers = answerCountEstimator.logicMgr.applicableRules(concludable);
            double inferredEstimate = 0;
            for (Rule rule : unifiers.keySet()) {
                for (Unifier unifier : unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variableFilter).filter(v -> v.id().isRetrievable())
                            .flatMap(v -> iterate(unifier.mapping().get(v.id().asRetrievable()))).toSet();
                    if ((concludable.isRelation() || concludable.isIsa())
                            && rule.conclusion().generating().isPresent() && ruleSideIds.contains(rule.conclusion().generating().get().id())) {
                        // There is one generated variable per combination of ALL variables in the conclusion
                        ruleSideIds = new HashSet<>(rule.conclusion().conjunction().pattern().retrieves());
                    }

                    for (Rule.Condition.ConditionBranch conditionBranch : rule.condition().branches()) {
                        Set<Variable> ruleSideVariables = iterate(ruleSideIds)
                                .filter(id -> conditionBranch.conjunction().pattern().retrieves().contains(id))
                                .map(id -> conditionBranch.conjunction().pattern().variable(id)).toSet();
                        double inferredEstimateForThisBranch = answerCountEstimator.estimateAnswers(conditionBranch.conjunction(), ruleSideVariables);

                        Set<Concludable> cyclicConcludables = answerCountEstimator.conjunctionGraph.conjunctionNode(conditionBranch.conjunction()).cyclicConcludables();
                        if (concludable.isRelation() && inferredEstimateForThisBranch > 0 && !cyclicConcludables.isEmpty()) {
                            Set<Variable> recursivePlayers = iterate(cyclicConcludables).flatMap(c -> iterate(c.variables()))
                                    .filter(v -> v.isThing() && ruleSideVariables.contains(v))
                                    .toSet();
                            Set<Variable> nonRecursivePlayers = iterate(ruleSideVariables).filter(v -> v.isThing() && !recursivePlayers.contains(v)).toSet();

                            Map<Variable, Double> estimateForRecursivePlayer = new HashMap<>();
                            // TODO: we might get better estimates by only considering non-recursive cases, but getting it right may be more complicated.
                            recursivePlayers.forEach(v -> estimateForRecursivePlayer.put(v, answerCountEstimator.estimateAnswers(conditionBranch.conjunction(), set(v))));

                            // All possible combinations (of non-recursive players) assumes full-connectivity, producing an unrealistically large/worst-case estimate.
                            // Instead, we model each role-player as being recursively related to sqrt(|r_i|) instances of each other role-player r_i.
                            double rootOfAll = Math.max(1.0, Math.sqrt(iterate(estimateForRecursivePlayer.values()).reduce(1.0, (x,y) -> x * y)));
                            double recursivelyConnectedEstimate = iterate(estimateForRecursivePlayer.values()).map(x -> rootOfAll * Math.max(1.0, Math.sqrt(x))).reduce(0.0, Double::sum);

                            double estimateForNonRecursivePlayers = answerCountEstimator.estimateAnswers(conditionBranch.conjunction(), nonRecursivePlayers);
                            double heuristicUpperBound = Math.ceil(estimateForNonRecursivePlayers * recursivelyConnectedEstimate);
                            inferredEstimate += Math.min(inferredEstimateForThisBranch, heuristicUpperBound);
                        } else {
                            inferredEstimate += inferredEstimateForThisBranch;
                        }
                    }
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
