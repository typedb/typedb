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
import grakn.core.common.iterator.Iterators;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class Resolvable {

    private final Conjunction conjunction;

    public Resolvable(Conjunction conjunction) {
        this.conjunction = conjunction;
    }

    public static List<Resolvable> plan(Set<Resolvable> resolvables) {
        return new Plan(resolvables).plan();
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
        private final Map<Variable, Set<Resolvable>> dependentOnGenerated;
        private final Map<Variable, Set<Resolvable>> connectedByVariable;

        Plan(Set<Resolvable> resolvables) {
            assert resolvables.size() > 0;
            plan = new ArrayList<>();
            connectedByVariable = connections(resolvables);
            dependentOnGenerated = dependencies(resolvables);

            Set<Resolvable> dependents = iterate(dependentOnGenerated.values()).flatMap(Iterators::iterate).toSet();
            Set<Resolvable> independents = new HashSet<>(resolvables);
            independents.removeAll(dependents);

            Resolvable startingPoint = pickIndependentStartingPoint(independents).orElseGet(() -> pickDependentStartingPoint(dependents));
            addToPlanBasedOnGenerated(startingPoint);

            assert plan.size() == resolvables.size();
            assert set(plan).equals(resolvables);
        }

        private void addToPlanBasedOnGenerated(Resolvable resolvable) {
            plan.add(resolvable);
            if (resolvable.isConcludable()){
                for (Resolvable nextResolvable : dependentOnGenerated.getOrDefault(resolvable.asConcludable().generating(), set())) {
                    if (!plan.contains(nextResolvable)) {
                        addToPlanBasedOnGenerated(nextResolvable);
                    }
                }
            }
            resolvable.conjunction().variables().stream().flatMap(var -> connectedByVariable.get(var).stream()).filter(r -> !plan.contains(r)).forEach(this::addToPlanBasedOnGenerated);
        }

        public List<Resolvable> plan() {
            return plan;
        }

        private Optional<Resolvable> pickIndependentStartingPoint(Set<Resolvable> independent) {
            if (independent.size() > 0) {
                Optional<Resolvable> startingPoint = independent.stream().filter(Resolvable::isRetrievable)
                        .max(Comparator.comparingInt(r -> r.conjunction().variables().size()));
                if (startingPoint.isPresent()) return startingPoint;
                return independent.stream().filter(Resolvable::isConcludable).findFirst();
            }
            return Optional.empty();
        }

        private Resolvable pickDependentStartingPoint(Set<Resolvable> dependents) {
            assert dependents.size() > 0;
            return dependents.iterator().next();
        }

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

        private Map<Variable, Set<Resolvable>> dependencies(Set<Resolvable> resolvables) {
            Map<Variable, Set<Resolvable>> deps = new HashMap<>();
            Set<Variable> generatedVars = iterate(resolvables).filter(Resolvable::isConcludable)
                    .map(Resolvable::asConcludable).map(Concludable::generating).toSet();
            for (Resolvable resolvable : resolvables) {
                for (Variable v : resolvable.conjunction().variables()) {
                    if (generatedVars.contains(v) && !(resolvable.isConcludable() && resolvable.asConcludable().generating().equals(v))) {
                        deps.putIfAbsent(v, new HashSet<>());
                        deps.get(v).add(resolvable);
                    }
                }
            }
            return deps;
        }
    }
}
