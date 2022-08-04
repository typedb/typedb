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

package com.vaticle.typedb.core.reasoner.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

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

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Planner {

    public static List<Resolvable<?>> plan(Set<Resolvable<?>> resolvables,
                                           Map<Resolvable<?>, Integer> visitCounts,
                                           Set<Retrievable> boundVariables) {
//        assert false; // TODO: Delete class?
        return new Plan(resolvables, visitCounts, boundVariables).plan;
    }

    private static class Plan {
        private final List<Resolvable<?>> plan;
        private final Map<Resolvable<?>, Set<Retrievable>> dependencies;
        private final Set<Retrievable> boundVariables;
        private final Set<Resolvable<?>> unplanned;
        private final Map<Resolvable<?>, Integer> visitCounts;

        private Plan(Set<Resolvable<?>> resolvables, Map<Resolvable<?>, Integer> visitCounts,
                     Set<Retrievable> boundVariables) {
            assert resolvables.size() > 0;
            this.unplanned = new HashSet<>(resolvables);
            this.visitCounts = visitCounts;
            this.boundVariables = new HashSet<>(boundVariables);
            this.dependencies = dependencies(resolvables);
            this.plan = new ArrayList<>();
            computePlan();
            assert plan.size() == resolvables.size();
            assert set(plan).equals(resolvables);
        }

        private void add(Resolvable<?> resolvable) {
            plan.add(resolvable);
            iterate(resolvable.retrieves()).forEachRemaining(boundVariables::add);
            unplanned.remove(resolvable);
        }

        private void computePlan() {
            while (!unplanned.isEmpty()) {
                Optional<Concludable> concludable;
                Optional<com.vaticle.typedb.core.logic.resolvable.Retrievable> retrievable;

                // Retrievable where:
                // it is connected
                // all of it's dependencies are already satisfied,
                retrievable = sortedByVisitCount(dependenciesSatisfied(hasAnsweredVar(unplanned.stream().filter(Resolvable::isRetrievable))))
                        .map(Resolvable::asRetrievable);
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // it is connected
                // all of it's dependencies are already satisfied,
                concludable = sortedByVisitCount(dependenciesSatisfied(hasAnsweredVar(unplanned.stream().filter(Resolvable::isConcludable))))
                        .map(Resolvable::asConcludable);
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Retrievable where:
                // it can be disconnected
                // all of it's dependencies are already satisfied (should be moot),
                // it can be disconnected
                retrievable = sortedByVisitCount(dependenciesSatisfied(unplanned.stream().filter(Resolvable::isRetrievable)))
                        .map(Resolvable::asRetrievable);
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // it can be disconnected
                // all of it's dependencies are already satisfied
                concludable = sortedByVisitCount(dependenciesSatisfied(unplanned.stream().filter(Resolvable::isConcludable)))
                        .map(Resolvable::asConcludable);
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Concludable where:
                // it can be disconnected
                // all of it's dependencies are NOT already satisfied
                concludable = sortedByVisitCount(unplanned.stream().filter(Resolvable::isConcludable))
                        .map(Resolvable::asConcludable);
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private Stream<Resolvable<?>> dependenciesSatisfied(Stream<Resolvable<?>> resolvableStream) {
            return resolvableStream.filter(c -> boundVariables.containsAll(dependencies.get(c)));
        }

        private Stream<Resolvable<?>> hasAnsweredVar(Stream<Resolvable<?>> resolvableStream) {
            return resolvableStream.filter(r -> !Collections.disjoint(r.retrieves(), boundVariables));
        }

        private Optional<Resolvable<?>> sortedByVisitCount(Stream<Resolvable<?>> resolvables) {
            return resolvables.max(Comparator.comparingInt(r -> visitCounts.getOrDefault(r, 0)));
        }

        /**
         * Determine the resolvables that are dependent upon the generation of each variable
         */
        private static Map<Resolvable<?>, Set<Retrievable>> dependencies(Set<Resolvable<?>> resolvables) {
            Map<Resolvable<?>, Set<Retrievable>> deps = new HashMap<>();
            Set<Retrievable> generated = iterate(resolvables).map(Resolvable::generating).filter(Optional::isPresent)
                    .map(Optional::get).map(ThingVariable::id).toSet();
            for (Resolvable<?> resolvable : resolvables) {
                Optional<ThingVariable> generating = resolvable.generating();
                deps.putIfAbsent(resolvable, new HashSet<>());
                for (Retrievable v : resolvable.retrieves()) {
                    if (generated.contains(v) && !(generating.isPresent() && generating.get().id().equals(v))) {
                        // TODO: Should this rule the Resolvable<?> out if generates it's own dependency?
                        deps.get(resolvable).add(v);
                    }
                }
            }
            return deps;
        }

    }
}
