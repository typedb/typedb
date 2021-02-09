/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import graql.lang.pattern.variable.Reference;
import graql.lang.pattern.variable.Reference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class Planner {
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;

    Planner(ConceptManager conceptMgr, LogicManager logicMgr) {
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
    }

    public List<Resolvable<?>> plan(Set<Resolvable<?>> resolvables, Set<Reference.Name> bound) {
        return new Plan(resolvables, bound).plan;
    }

    class Plan {
        private final List<Resolvable<?>> plan;
        private final Map<Resolvable<?>, Set<Reference.Name>> dependencies;
        private final Set<Reference.Name> varsAnswered;
        private final Set<Resolvable<?>> remaining;

        Plan(Set<Resolvable<?>> resolvables, Set<Reference.Name> boundVars) {
            assert resolvables.size() > 0;
            this.plan = new ArrayList<>();
            this.varsAnswered = new HashSet<>(boundVars);
            this.dependencies = dependencies(resolvables);
            this.remaining = new HashSet<>(resolvables);
            computePlan();
            assert plan.size() == resolvables.size();
            assert set(plan).equals(resolvables);
        }

        private void add(Resolvable<?> resolvable) {
            plan.add(resolvable);
            varsAnswered.addAll(resolvable.variableNames());
            remaining.remove(resolvable);
        }

        private void computePlan() {
            while (remaining.size() != 0) {
                Optional<Concludable> concludable;
                Optional<Retrievable> retrievable;

                // Retrievable where:
                // all of it's dependencies are already satisfied,
                // which will answer the most variables
                retrievable = mostAnsweredVars(dependenciesSatisfied(hasAnsweredVar(remaining.stream().filter(Resolvable::isRetrievable))))
                        .map(Resolvable::asRetrievable);
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // all of it's dependencies are already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = mostAnsweredVars(dependenciesSatisfied(hasAnsweredVar(remaining.stream().filter(Resolvable::isConcludable))))
                        .map(Resolvable::asConcludable);
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Retrievable where:
                // all of it's dependencies are already satisfied (should be moot),
                // it can be disconnected
                // which will answer the most variables
                retrievable = mostUnansweredVars(dependenciesSatisfied(remaining.stream().filter(Resolvable::isRetrievable)))
                        .map(Resolvable::asRetrievable);
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // it can be disconnected
                // all of it's dependencies are already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = fewestRules(dependenciesSatisfied(remaining.stream().filter(Resolvable::isConcludable)));
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Concludable where:
                // it can be disconnected
                // all of it's dependencies are NOT already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = fewestRules(remaining.stream().filter(Resolvable::isConcludable));
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private Stream<Resolvable<?>> dependenciesSatisfied(Stream<Resolvable<?>> resolvableStream) {
            return resolvableStream.filter(c -> varsAnswered.containsAll(dependencies.get(c)));
        }

        private Stream<Resolvable<?>> hasAnsweredVar(Stream<Resolvable<?>> resolvableStream) {
            return resolvableStream.filter(r -> !Collections.disjoint(r.variableNames(), varsAnswered));
        }

        private Optional<Concludable> fewestRules(Stream<Resolvable<?>> resolvableStream) {
            // TODO: Tie-break for Concludables with the same number of applicable rules
            return resolvableStream.map(Resolvable::asConcludable)
                    .min(Comparator.comparingInt(c -> (int) c.getApplicableRules(conceptMgr, logicMgr).count()));
        }

        private Optional<Resolvable<?>> mostAnsweredVars(Stream<Resolvable<?>> resolvables) {
            return resolvables.max(Comparator.comparingInt(r -> iterate(r.variableNames())
                            .filter(varsAnswered::contains).toSet().size()));
        }

        private Optional<Resolvable<?>> mostUnansweredVars(Stream<Resolvable<?>> resolvableStream) {
            return resolvableStream.max(Comparator.comparingInt(r -> iterate(r.variableNames())
                    .filter(var -> !varsAnswered.contains(var)).toSet().size()));
        }

        /**
         * Determine the resolvables that are dependent upon the generation of each variable
         * @return
         */
        private Map<Resolvable<?>, Set<Reference.Name>> dependencies(Set<Resolvable<?>> resolvables) {
            Map<Resolvable<?>, Set<Reference.Name>> deps = new HashMap<>();
            Set<Reference.Name> generatedVars = iterate(resolvables).map(Resolvable::generating).filter(Optional::isPresent)
                    .map(Optional::get).filter(v -> v.reference().isName()).map(v -> v.reference().asName()).toSet();
            for (Resolvable<?> resolvable : resolvables) {
                Optional<Reference.Name> generating = resolvable.generating().filter(v -> v.reference().isName())
                        .map(v -> v.reference().asName());
                for (Reference.Name v : resolvable.variableNames()) {
                    deps.putIfAbsent(resolvable, new HashSet<>());
                    if (generatedVars.contains(v) && !(generating.isPresent() && generating.get().equals(v))) {
                        // TODO: Should this rule the Resolvable<?> out if generates it's own dependency?
                        deps.get(resolvable).add(v);
                    }
                }
            }
            return deps;
        }

    }
}
