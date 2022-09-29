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
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
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

import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class AnswerCountEstimator {
    private final LogicManager logicMgr;
    private final ConjunctionModelFactory conjunctionModelFactory;

    private final Map<ResolvableConjunction, ConjunctionModel> conjunctionModels;
    private final Map<ResolvableConjunction, Set<Concludable>> cyclicConcludables;

    public AnswerCountEstimator(LogicManager logicMgr, GraphManager graph) {
        this.logicMgr = logicMgr;
        this.conjunctionModelFactory = new ConjunctionModelFactory(new LocalModelFactory(this, graph));
        this.conjunctionModels = new HashMap<>();
        this.cyclicConcludables = new HashMap<>();
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variables) {
        return estimateAnswers(conjunction, variables, logicMgr.compile(conjunction));
    }

    public long estimateAnswers(ResolvableConjunction conjunction, Set<Variable> variables, Set<Resolvable<?>> resolvables) {
        if (!conjunctionModels.containsKey(conjunction)) registerAndBuildModel(conjunction);
        assert conjunction.pattern().variables().containsAll(variables);
        assert logicMgr.compile(conjunction).containsAll(resolvables);
        return conjunctionModels.get(conjunction).estimateAnswers(variables, resolvables);
    }

    private void registerAndBuildModel(ResolvableConjunction conjunction) {
        registerCycles(conjunction, new ConjunctionConcludableStack());
        buildConjunctionModel(conjunction);
    }

    private void registerCycles(ResolvableConjunction conjunction, ConjunctionConcludableStack stack) {
        if (stack.contains(conjunction)) {
            stack.peekUntil(conjunction).forEach(conj ->
                    cyclicConcludables.computeIfAbsent(conj, c -> new HashSet<>()).add(stack.getConcludable(conj))
            );
        } else {
            stack.add(conjunction);
            Set<Resolvable<?>> resolvables = logicMgr.compile(conjunction);
            iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated)
                    .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(dependency -> registerCycles(dependency, new ConjunctionConcludableStack())); // Stratified negation -> Fresh set
            iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        stack.setConcludable(conjunction, concludable);
                        iterate(dependencies(concludable)).forEachRemaining(dependency -> registerCycles(dependency, stack));
                    });
            assert stack.last() == conjunction;
            stack.pop();
        }
    }

    private void buildConjunctionModel(ResolvableConjunction conjunction) {
        if (!conjunctionModels.containsKey(conjunction)) {
            ConjunctionContext conjunctionContext = new ConjunctionContext(conjunction, logicMgr.compile(conjunction), cyclicConcludables.getOrDefault(conjunction, new HashSet<>()));

            // Acyclic estimates
            iterate(conjunctionContext.negateds()).flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(this::buildConjunctionModel);
            iterate(conjunctionContext.acyclicConcludables()).flatMap(concludable -> iterate(dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);

            ConjunctionModel acyclicModel = conjunctionModelFactory.buildAcyclicModel(conjunctionContext);
            conjunctionModels.put(conjunction, acyclicModel);

            // cyclic calls to this model will answer based on the acyclic model.
            iterate(conjunctionContext.cyclicConcludables()).flatMap(concludable -> iterate(dependencies(concludable)))
                    .forEachRemaining(this::buildConjunctionModel);
            ConjunctionModel cyclicModel = conjunctionModelFactory.buildCyclicModel(conjunctionContext, acyclicModel);
            conjunctionModels.put(conjunction, cyclicModel);
        }
    }

    private Set<ResolvableConjunction> dependencies(Concludable concludable) {
        return iterate(logicMgr.applicableRules(concludable).keySet()).map(rule -> rule.condition().conjunction()).toSet();
    }

    private static class ConjunctionConcludableStack {

        ArrayList<ResolvableConjunction> stack;
        Map<ResolvableConjunction, Concludable> concludables;

        ConjunctionConcludableStack() {
            this.stack = new ArrayList<>();
            this.concludables = new HashMap<>();
        }

        private void add(ResolvableConjunction conjunction) {
            stack.add(conjunction);
            concludables.put(conjunction, null);
        }

        private ResolvableConjunction last() {
            return stack.get(stack.size() - 1);
        }

        private boolean contains(ResolvableConjunction conjunction) {
            return concludables.containsKey(conjunction);
        }

        private void pop() {
            ResolvableConjunction conjunction = stack.remove(stack.size() - 1);
            concludables.remove(conjunction);
        }

        private void setConcludable(ResolvableConjunction conjunction, Concludable concludable) {
            assert stack.contains(conjunction);
            concludables.put(conjunction, concludable);
        }

        private Concludable getConcludable(ResolvableConjunction conjunction) {
            return concludables.get(conjunction);
        }

        public List<ResolvableConjunction> peekUntil(ResolvableConjunction untilInclusive) {
            assert stack.contains(untilInclusive);
            List<ResolvableConjunction> subList = new ArrayList<>();
            for (int i = stack.size() - 1; stack.get(i) != untilInclusive; i--) {
                subList.add(stack.get(i));
            }
            subList.add(untilInclusive);
            return subList;
        }
    }

    private static class ConjunctionContext {
        private final ResolvableConjunction conjunction;
        private final Set<Resolvable<?>> resolvables;
        private final Set<Variable> estimateableVars;
        private final Set<Concludable> cyclicConcludables;
        private final Set<Concludable> acyclicConcludables;

        private ConjunctionContext(ResolvableConjunction conjunction, Set<Resolvable<?>> resolvables, Set<Concludable> cyclicConcludables) {
            this.conjunction = conjunction;
            this.resolvables = resolvables;
            this.estimateableVars = iterate(conjunction.pattern().variables()).filter(Variable::isThing).toSet();
            this.cyclicConcludables = cyclicConcludables;
            this.acyclicConcludables = iterate(resolvables)
                    .filter(resolvable -> resolvable.isConcludable() && !cyclicConcludables.contains(resolvable.asConcludable()))
                    .map(Resolvable::asConcludable).toSet();
        }

        public FunctionalIterator<Resolvable<?>> resolvables() {
            return iterate(resolvables);
        }

        public FunctionalIterator<Retrievable> retrievables() {
            return iterate(resolvables).filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable);
        }

        public FunctionalIterator<Negated> negateds() {
            return iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated);
        }

        public FunctionalIterator<Concludable> cyclicConcludables() {
            return iterate(cyclicConcludables);
        }

        public FunctionalIterator<Concludable> acyclicConcludables() {
            return iterate(acyclicConcludables);
        }
    }

    private static class ConjunctionModel {
        private final ConjunctionContext conjunctionContext;
        private final Map<Variable, LocalModel.VariableModel> variableModels;
        private final Map<Resolvable<?>, List<LocalModel>> constraintModels;
        private final boolean isCyclic;

        private ConjunctionModel(ConjunctionContext conjunctionContext,
                                 Map<Variable, LocalModel.VariableModel> variableModels, Map<Resolvable<?>, List<LocalModel>> constraintModels,
                                 boolean isCyclic) {
            this.conjunctionContext = conjunctionContext;
            this.variableModels = variableModels;
            this.constraintModels = constraintModels;
            this.isCyclic = isCyclic;
        }

        private long estimateAnswers(Set<Variable> variables, Set<Resolvable<?>> resolvables) {
            List<LocalModel> models = iterate(resolvables)
                    .flatMap(resolvable -> iterate(constraintModels.get(resolvable)))
                    .toList();

            Set<Variable> estimateableVars = intersection(variables, conjunctionContext.estimateableVars);
            Map<Variable, LocalModel> costCover = greedyCover(estimateableVars, variableModels, models);
            long ret = answerEstimateFromCover(estimateableVars, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
        }

        private static Map<Variable, LocalModel> greedyCover(Set<Variable> variables, Map<Variable, LocalModel.VariableModel> variableModels, List<LocalModel> constraintModels) {
            // Does a greedy set cover
            Map<Variable, LocalModel> cover = new HashMap<>();
            iterate(variables).forEachRemaining(v -> cover.put(v, variableModels.get(v)));

            constraintModels.sort(Comparator.comparing(x -> x.estimateAnswers(variables)));
            for (LocalModel model : constraintModels) {
                Set<Variable> resolvableVars = intersection(model.variables, variables);
                if (answerEstimateFromCover(resolvableVars, cover) > model.estimateAnswers(resolvableVars)) {
                    resolvableVars.forEach(v -> cover.put(v, model));
                }
            }
            return cover;
        }

        private static long answerEstimateFromCover(Set<Variable> variables, Map<Variable, LocalModel> coverMap) {
            Set<LocalModel> models = iterate(intersection(variables, coverMap.keySet())).map(coverMap::get).toSet();
            return iterate(models).map(model -> model.estimateAnswers(variables)).reduce(1L, (x, y) -> x * y);
        }
    }

    private static class ConjunctionModelFactory {
        private final LocalModelFactory localModelFactory;

        private ConjunctionModelFactory(LocalModelFactory localModelFactory) {
            this.localModelFactory = localModelFactory;
        }

        private ConjunctionModel buildAcyclicModel(ConjunctionContext conjunctionContext) {
            Map<Resolvable<?>, List<LocalModel>> models = new HashMap<>();
            List<LocalModel.VariableModel> generatedVariableModels = new ArrayList<>();
            iterate(conjunctionContext.acyclicConcludables())
                    .forEachRemaining(concludable -> {
                        List<LocalModel.VariableModel> generatedVariableModelsForConcludable = modelsForGeneratedVariable(concludable);
                        generatedVariableModels.addAll(generatedVariableModelsForConcludable);
                        ArrayList<LocalModel> combinedModels = new ArrayList<>();
                        combinedModels.addAll(generatedVariableModelsForConcludable);
                        combinedModels.addAll(buildModelsForConcludable(concludable));
                        models.put(concludable, combinedModels);
                    });

            Map<Variable, LocalModel.VariableModel> variableModels = computeBaselineCover(conjunctionContext.estimateableVars, generatedVariableModels);

            iterate(conjunctionContext.retrievables())
                    .forEachRemaining(retrievable -> models.put(retrievable, buildModelsForRetrievable(retrievable)));
            iterate(conjunctionContext.negateds())
                    .forEachRemaining(negated -> models.put(negated, buildModelsForNegated(negated)));
            iterate(conjunctionContext.cyclicConcludables()).forEachRemaining(concludable -> models.put(concludable, list()));

            // EdgeCase: Efficiently handle inferred `$x has $a` when $x is inferred in the body of the rule.
            iterate(conjunctionContext.acyclicConcludables()).filter(Concludable::isHas).map(Concludable::asHas)
                    .forEachRemaining(concludable -> {
                        if (variableModels.get(concludable.owner()).estimateAnswers(set(concludable.owner())) == 0) {
                            long attrCount = Math.max(1L, variableModels.get(concludable.attribute()).estimateAnswers(set(concludable.attribute())));
                            long hasEdgeCount = iterate(models.get(concludable))
                                    .map(localModel -> localModel.estimateAnswers(set(concludable.owner(), concludable.attribute())))
                                    .reduce(1L, Long::sum);
                            variableModels.put(concludable.owner(), new LocalModel.VariableModel(set(concludable.owner()), hasEdgeCount / attrCount));
                        }
                    });

            assert !iterate(conjunctionContext.estimateableVars).filter(variable -> !variableModels.containsKey(variable)).hasNext();
            assert !iterate(conjunctionContext.resolvables()).filter(resolvable -> !models.containsKey(resolvable)).hasNext();
            return new ConjunctionModel(conjunctionContext, variableModels, models, false);
        }

        private ConjunctionModel buildCyclicModel(ConjunctionContext conjunctionContext, ConjunctionModel acyclicModel) {
            assert acyclicModel.conjunctionContext == conjunctionContext;
            assert !acyclicModel.isCyclic;
            Map<Resolvable<?>, List<LocalModel>> models = new HashMap<>(acyclicModel.constraintModels);
            List<LocalModel.VariableModel> generatedVariableModels = new ArrayList<>(acyclicModel.variableModels.values());
            iterate(conjunctionContext.cyclicConcludables())
                    .forEachRemaining(concludable -> {
                        List<LocalModel.VariableModel> generatedVariableModelsForConcludable = modelsForGeneratedVariable(concludable);
                        generatedVariableModels.addAll(generatedVariableModelsForConcludable);
                        ArrayList<LocalModel> combinedModels = new ArrayList<>();
                        combinedModels.addAll(generatedVariableModelsForConcludable);
                        combinedModels.addAll(buildModelsForConcludable(concludable));
                        models.put(concludable, combinedModels);
                    });
            Map<Variable, LocalModel.VariableModel> variableModels = computeBaselineCover(conjunctionContext.estimateableVars, generatedVariableModels);

            assert !iterate(conjunctionContext.estimateableVars).filter(variable -> !variableModels.containsKey(variable)).hasNext();
            assert !iterate(conjunctionContext.resolvables()).filter(resolvable -> !models.containsKey(resolvable)).hasNext();
            return new ConjunctionModel(conjunctionContext, variableModels, models, true);
        }

        private List<LocalModel> buildModelsForRetrievable(Retrievable retrievable) {
            return iterate(extractConstraints(retrievable))
                    .map(constraint -> buildConstraintModel(constraint, Optional.empty()))
                    .toList();
        }

        private List<LocalModel> buildModelsForNegated(Negated negated) {
            return list();
        }

        private List<LocalModel> buildModelsForConcludable(Concludable concludable) {
            return iterate(concludable.concludableConstraints()).filter(this::isModellable)
                    .map(constraint -> buildConstraintModel(constraint, Optional.of(concludable)))
                    .toList();
        }

        private List<LocalModel.VariableModel> modelsForGeneratedVariable(Concludable concludable) {
            return list(localModelFactory.modelForVariable(concludable.generating().get(), concludable));
        }

        private Map<Variable, LocalModel.VariableModel> computeBaselineCover(Set<Variable> variablesToCover, List<LocalModel.VariableModel> generatedVariableModels) {
            Map<Variable, LocalModel.VariableModel> newVariableCover = new HashMap<>();
            iterate(variablesToCover).map(Variable::asThing).forEachRemaining(v -> { // baseline
                newVariableCover.put(v, new LocalModel.VariableModel(set(v), localModelFactory.countPersistedThingsMatchingType(v)));
            });

            iterate(generatedVariableModels).flatMap(Iterators::iterate)
                    .forEachRemaining(model -> {
                        Variable v = model.variables.stream().findAny().get();
                        long existingEstimate = newVariableCover.get(v).estimateAnswers(set(v));
                        long newEstimate = model.estimateAnswers(set(v));
                        if (newEstimate > existingEstimate) { // Biggest one serves as baseline.
                            newVariableCover.put(v, model);
                        }
                    });

            assert variablesToCover.stream().allMatch(newVariableCover::containsKey);
            return newVariableCover;
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
            private final double relationTypeEstimate;
            private final long inferredRelationEstimate;
            private final Map<ThingVariable, TypeVariable> rolePlayerTypes;

            private RelationModel(RelationConstraint relation, double relationTypeEstimate,
                                  Map<TypeVariable, Long> rolePlayerEstimates, long inferredRelationEstimate) {
                super(iterate(relation.variables()).filter(Variable::isThing).toSet());
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
                    if (v.isThing() && rolePlayerTypes.containsKey(v.asThing())) {
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
                super(set(has.owner(), has.attribute()), hasEdgeEstimate);
                this.has = has;
            }
        }

        private static class IsaModel extends StaticModel {
            private final IsaConstraint isa;

            private IsaModel(IsaConstraint isa, long estimate) {
                super(set(isa.owner()), estimate);
                this.isa = isa;
            }
        }

        private static class VariableModel extends StaticModel {
            private VariableModel(Set<Variable> variables, long estimate) {
                super(variables, estimate);
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

            double relationTypeEstimate = countPersistedThingsMatchingType(relationConstraint.owner());

            for (RelationConstraint.RolePlayer rp : rolePlayers) {
                TypeVariable key = rp.roleType().orElse(null);
                rolePlayerCounts.put(key, rolePlayerCounts.getOrDefault(key, 0) + 1);
                if (!rolePlayerEstimates.containsKey(key)) {
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp));
                }
            }

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

            return new LocalModel.RelationModel(relationConstraint, relationTypeEstimate, rolePlayerEstimates, inferredRelationsEstimate);
        }

        private LocalModel.HasModel modelForHas(HasConstraint hasConstraint, Optional<Concludable> correspondingConcludable) {
            long hasEdgeEstimate = countPersistedHasEdges(hasConstraint.owner().inferredTypes(), hasConstraint.attribute().inferredTypes());
            if (correspondingConcludable.isPresent()) {
                hasEdgeEstimate += estimateInferredAnswerCount(correspondingConcludable.get(), set(hasConstraint.owner(), hasConstraint.attribute()));
            }
            return new LocalModel.HasModel(hasConstraint, hasEdgeEstimate);
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

        private LocalModel.VariableModel modelForVariable(Variable v, Concludable sourceConcludable) {
            long persistedAnswerCount = countPersistedThingsMatchingType(v.asThing());
            long inferredAnswerCount = 0;
            if (v == sourceConcludable.generating().get()) {
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
//                        ruleSideVariables = iterate(rule.conclusion().pattern().variables())
//                                .filter(v -> v.isThing() && v != rule.conclusion().generating().get())
//                                .toSet();
//                    } else {
//                        ruleSideVariables = iterate(ruleSideIds)
//                                .filter(id -> rule.condition().conjunction().pattern().variables().contains(id)) // avoids constant has
//                                .map(id -> rule.conclusion().conjunction().pattern().variable(id)).toSet();
//                    }
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
