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

package grakn.core.logic.resolvable;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;

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

import static grakn.common.util.Objects.className;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class Resolvable {

    private final Conjunction conjunction;

    public Resolvable(Conjunction conjunction) {
        this.conjunction = conjunction;
    }

    public static List<Resolvable> plan(Set<Resolvable> resolvables, ConceptManager conceptMgr, LogicManager logicMgr) {
        return new Plan(resolvables, conceptMgr, logicMgr).plan();
    }

    public Conjunction conjunction() {
        return conjunction;
    }

    public boolean isRetrievable() {
        return false;
    }

    public boolean isConcludable() {
        return false;
    }

    public Retrievable asRetrievable() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Retrievable.class));
    }

    public Concludable asConcludable() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Concludable.class));
    }

    public static class Plan {
        private final List<Resolvable> plan;
        private final Map<Resolvable, Set<Variable>> dependencies;
        private final Map<Variable, Set<Resolvable>> connectedByVariable;
        private final ConceptManager conceptMgr;
        private final LogicManager logicMgr;
        private Set<Variable> varsAnswered;
        private Set<Resolvable> remaining;

        Plan(Set<Resolvable> resolvables, ConceptManager conceptMgr, LogicManager logicMgr) {
            this.conceptMgr = conceptMgr;
            this.logicMgr = logicMgr;
            assert resolvables.size() > 0;
            plan = new ArrayList<>();
            varsAnswered = new HashSet<>();
            connectedByVariable = connections(resolvables);
            dependencies = dependencies(resolvables);
            remaining = new HashSet<>(resolvables);

            planning();

            assert plan.size() == resolvables.size();
            assert set(plan).equals(resolvables);
        }

        private void add(Resolvable resolvable) {
            plan.add(resolvable);
            varsAnswered.addAll(resolvable.conjunction().variables());
            remaining.remove(resolvable);
        }

        private void planning() {
            while (remaining.size() != 0) {
                Optional<Concludable> concludable;
                Optional<Resolvable> retrievable;

                // Retrievable where:
                // all of it's dependencies are already satisfied,
                // which will answer the most variables
                retrievable = mostUnansweredVars(dependenciesSatisfied(connected(remaining.stream().filter(Resolvable::isRetrievable))));
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // all of it's dependencies are already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = fewestRules(dependenciesSatisfied(connected(remaining.stream().filter(Resolvable::isConcludable))));
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Retrievable where:
                // all of it's dependencies are already satisfied (should be moot),
                // it can be disconnected
                // which will answer the most variables
                retrievable = mostUnansweredVars(dependenciesSatisfied(remaining.stream().filter(Resolvable::isRetrievable)));
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
                assert false;
            }
        }

        private Stream<Resolvable> dependenciesSatisfied(Stream<Resolvable> resolvableStream) {
            return resolvableStream.filter(c -> varsAnswered.containsAll(dependencies.get(c)));
        }

        private Stream<Resolvable> connected(Stream<Resolvable> resolvableStream) {
            return resolvableStream.filter(r -> !Collections.disjoint(r.conjunction().variables(), varsAnswered));
        }

        private Optional<Concludable> fewestRules(Stream<Resolvable> resolvableStream) {
            // TODO How to do a tie-break for Concludables with the same number of applicable rules?
            return resolvableStream.map(Resolvable::asConcludable)
                    .min(Comparator.comparingInt(c -> c.getApplicableRules(conceptMgr, logicMgr).toSet().size()));
        }

        private Optional<Resolvable> mostUnansweredVars(Stream<Resolvable> resolvableStream) {
            return resolvableStream.max(Comparator.comparingInt(r -> {
                HashSet<Variable> s = new HashSet<>(r.conjunction().variables());
                s.removeAll(varsAnswered);
                return s.size();
            }));
        }



        public List<Resolvable> plan() {
            return plan;
        }

        /**
         * Determine which resolvables are connected by the same variable
         */
        private Map<Variable, Set<Resolvable>> connections(Set<Resolvable> resolvables) {
            Map<Variable, Set<Resolvable>> connections = new HashMap<>();
            for (Resolvable resolvable : resolvables) {
                for (Variable variable : resolvable.conjunction().variables()) {
                    connections.putIfAbsent(variable, new HashSet<>());
                    connections.get(variable).add(resolvable);
                }
            }
            return connections;
        }

        /**
         * Determine the resolvables that are dependent upon the generation of each variable
         */
        private Map<Resolvable, Set<Variable>> dependencies(Set<Resolvable> resolvables) {
            Map<Resolvable, Set<Variable>> deps = new HashMap<>();
            Set<Variable> generatedVars = iterate(resolvables).filter(Resolvable::isConcludable)
                    .map(Resolvable::asConcludable).map(Concludable::generating).toSet();
            for (Resolvable resolvable : resolvables) {
                for (Variable v : resolvable.conjunction().variables()) {
                    deps.putIfAbsent(resolvable, new HashSet<>());
                    if (generatedVars.contains(v) && !(resolvable.isConcludable() && resolvable.asConcludable().generating().equals(v))) {
                        // TODO Should this rule the Resolvable out if generates it's own dependency?
                        deps.get(resolvable).add(v);
                    }
                }
            }
            return deps;
        }
    }
}
