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
import com.vaticle.typedb.core.logic.resolvable.*;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class CostEstimator {
    private final LogicManager logicMgr;
    private final Map<ResolvableConjunction, ConjunctionAnswerCountEstimator> costEstimators;

    public CostEstimator(LogicManager logicMgr) {
        this.logicMgr = logicMgr;
        this.costEstimators = new HashMap<>();
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
            costEstimators.get(conjunction).initializeOnce();
        }
    }

    private static class ConjunctionAnswerCountEstimator {

        private final ResolvableConjunction conjunction;
        private final CostEstimator costEstimator;
        private final LogicManager logicMgr;
        private Map<Variable, CostCover> unaryCostCover;
        private Map<Resolvable<?>, List<LocalEstimate>> multivarEstimates;
        private long fullAnswerCount;

        public ConjunctionAnswerCountEstimator(CostEstimator costEstimator, ResolvableConjunction conjunction) {
            this.costEstimator = costEstimator;
            this.logicMgr = costEstimator.logicMgr;
            this.conjunction = conjunction;
            this.fullAnswerCount = -1;
        }

        public long estimateAllAnswers() {
            return this.fullAnswerCount;
        }

        public long estimateAnswers(Set<Variable> projectToVariables, Set<Resolvable<?>> includedResolvables) {
            List<LocalEstimate> enabledEstimates = iterate(includedResolvables)
                    .flatMap(resolvable -> iterate(multivarEstimates.get(resolvable)))
                    .toList();
            return computeCostCover(enabledEstimates, projectToVariables);
        }

        private long computeCostCover(List<LocalEstimate> multivarEstimates, Set<Variable> projectToVariables) {
            // Does a greedy set cover
            Map<Variable, CostCover> costCover = new HashMap<>(unaryCostCover);
            multivarEstimates.sort(Comparator.comparing(x -> x.answerEstimate(this, projectToVariables)));
            for (LocalEstimate multivarEstimate : multivarEstimates) {
                Set<Variable> interestingSubsetOfVariables = multivarEstimate.variables.stream()
                        .filter(projectToVariables::contains).collect(Collectors.toSet());

                long currentCostToCover = CostCover.costToCover(interestingSubsetOfVariables, costCover);
                if (currentCostToCover > multivarEstimate.answerEstimate(this, interestingSubsetOfVariables)) {
                    CostCover variablesNowCoveredBy = new CostCover(multivarEstimate.answerEstimate(this, interestingSubsetOfVariables));
                    interestingSubsetOfVariables.forEach(v -> costCover.put(v, variablesNowCoveredBy));
                }
            }

            long ret = CostCover.costToCover(projectToVariables, costCover);
            assert ret > 0;             // Flag in tests if it happens.
            return Math.max(ret, 1);    // Don't do stupid stuff in prod when it happens.
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


        private void initializeOnce() {
            // TODO: Move this after the retrievable estimates, so cycle-head estimates are easy.
            registerTriggeredRules();

            assert unaryCostCover == null;
            unaryCostCover = new HashMap<>();
            {
                // Create a baseline estimate from type information
                List<LocalEstimate> unaryEstimates = new ArrayList<>();
                iterate(conjunction.pattern().variables()).forEachRemaining(v -> {
                    unaryEstimates.add(estimateFromTypes(v));
                });

                unaryEstimates.sort(Comparator.comparing(x -> x.answerEstimate(this, new HashSet<>(x.variables))));
                for (LocalEstimate unaryEstimate : unaryEstimates) {
                    if (!unaryCostCover.containsKey(unaryEstimate.variables.get(0))) {
                        unaryCostCover.put(unaryEstimate.variables.get(0), new CostCover(unaryEstimate.answerEstimate(this, new HashSet<>(unaryEstimate.variables))));
                    }
                }
            }

            assert multivarEstimates == null;
            multivarEstimates = new HashMap<>();
            resolvables().forEachRemaining(resolvable -> multivarEstimates.put(resolvable, new ArrayList<>()));

            //TODO: How does traversal handle type Variables?

            resolvables().filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(retrievable -> {
                Set<Concludable> concludablesInRetrievable = ResolvableConjunction.of(retrievable.pattern()).positiveConcludables();
                iterate(concludablesInRetrievable).forEachRemaining(concludable -> {
                    multivarEstimates.get(retrievable).addAll(computeEstimatesFromConcludable(concludable));
                });
            });

            iterate(resolvables()).filter(this::isInferrable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
                multivarEstimates.get(concludable).addAll(computeEstimatesFromConcludable(concludable));
            });

            // TODO: Negateds for the total -cost?
            // TODO: can we prune the multiVarEstimates stored?

            this.fullAnswerCount = computeCostCover(iterate(multivarEstimates.values()).flatMap(Iterators::iterate).toList(), conjunction.pattern().variables());
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
            List<Concludable> relevantConcludables = resolvables()
                    .filter(resolvable -> isInferrable(resolvable) && resolvable.generating().isPresent() &&
                        resolvable.generating().get().equals(generatedVariable))
                    .map(Resolvable::asConcludable)
                    .toList();

            if (relevantConcludables.isEmpty()) {
                return 0L;
            }

            long mostConstrainedAnswer = Long.MAX_VALUE;
            for (Concludable concludable: relevantConcludables) {
                Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
                AtomicLong nInferred = new AtomicLong(0L);
                long answersGeneratedByThisResolvable = iterate(unifiers.keySet()).map(rule -> {
                    Optional<Unifier> firstUnifier = unifiers.get(rule).stream().findFirst();
                    // Increment the estimates for variables generated by the rule
                    assert rule.conclusion().generating().isPresent() && firstUnifier.isPresent();

                    if (rule.conclusion().isRelation()) { // Is a relation
                        Set<Variable> answerVariables = iterate(rule.conclusion().pattern().variables())
                                .filter(v -> rule.condition().pattern().variables().contains(v)).toSet();
                        return costEstimator.estimateAnswers(rule.condition().conjunction(), answerVariables);
                    } else if (rule.conclusion().isExplicitHas()) {
                        return 1L;
                    }  else {
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
            for (Rule rule: unifiers.keySet()) {
                for(Unifier unifier: unifiers.get(rule)) {
                    Set<Identifier.Variable> ruleSideIds = iterate(variablesOfInterest)
                            .flatMap(v -> iterate(unifier.mapping().get(v.id()))).toSet();
                    Set<Variable> ruleSideVariables =iterate(ruleSideIds).map(id -> rule.condition().conjunction().pattern().variable(id))
                            .toSet();
                    inferredEstimate += costEstimator.estimateAnswers(rule.condition().conjunction(), ruleSideVariables);
                }
            }
            return inferredEstimate;
        }

        private LocalEstimate estimateFromTypes(Variable var) {
            long estimate;
            if (var.isThing()) {
                estimate = (var.asThing().iid().isPresent()) ? 1 :
                        countPersistedThingsMatchingType(var.asThing()) + computeInferredUnaryEstimates(var.asThing());

            } else if (var.isType()) {
                estimate = logicMgr.graph().schema().stats().typeCount(); // TODO: Refine
            } else throw TypeDBException.of(ILLEGAL_STATE);

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
                if (!rolePlayerEstimates.containsKey(key)){
                    rolePlayerEstimates.put(key, countPersistedRolePlayers(rp));
                }
            }

            return new LocalEstimate.CoPlayerEstimate(concludable, constrainedVars, relationTypeEstimate, rolePlayerEstimates, rolePlayerCounts);

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

            private LocalEstimate(List<Variable> variables) { this.variables = variables; }

            abstract long answerEstimate(ConjunctionAnswerCountEstimator costEstimator, Set<Variable> variablesOfInterest);

            private static class SimpleEstimate extends LocalEstimate {
                private final long staticEstimate;

                private SimpleEstimate(List<Variable> variables, long estimate) {
                    super(variables);
                    this.staticEstimate = estimate;
                }

                @Override
                long answerEstimate(ConjunctionAnswerCountEstimator costEstimator, Set<Variable> variablesOfInterest) { return staticEstimate; }
            }

            public static class CoPlayerEstimate extends LocalEstimate {

                private final Map<TypeVariable, Integer> rolePlayerCounts;
                private final Map<TypeVariable, Long> rolePlayerEstimates;
                private final double relationTypeEstimate;
                private final Concludable concludable;

                public CoPlayerEstimate(Concludable concludable, List<Variable> variables, double relationTypeEstimate,
                                        Map<TypeVariable, Long> rolePlayerEstimates, Map<TypeVariable, Integer> rolePlayerCounts) {
                    super(variables);
                    this.concludable = concludable;
                    this.relationTypeEstimate = relationTypeEstimate;
                    this.rolePlayerEstimates = rolePlayerEstimates;
                    this.rolePlayerCounts = rolePlayerCounts;
                }

                @Override
                long answerEstimate(ConjunctionAnswerCountEstimator costEstimator, Set<Variable> variablesOfInterest) {
                    long singleRelationEstimate = 1L;
                    for (TypeVariable key : rolePlayerCounts.keySet()) {
                        assert rolePlayerEstimates.containsKey(key);
                        long avgRolePlayers = Double.valueOf(Math.ceil(rolePlayerEstimates.get(key)/relationTypeEstimate)).longValue();
                        singleRelationEstimate *= nPermuteKforSmallK(avgRolePlayers, rolePlayerCounts.get(key));
                    }

                    long fullEstimate = Double.valueOf(Math.ceil(relationTypeEstimate * singleRelationEstimate)).longValue() +
                            costEstimator.estimateInferredAnswerCount(concludable, variablesOfInterest.stream().filter(this.variables::contains).collect(Collectors.toSet()));
                    return fullEstimate;
                }

                private long nPermuteKforSmallK(long n, long k) {
                    long ans = 1;
                    for(int i=0; i < k; i++)    ans *= n-i;
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
