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

    Set<Variable> generatedVars() {
        return null;
    }

    public static class Plan {
        private final Map<Resolvable, Set<Variable>> resolvableVars;
        private final List<Resolvable> plan;
        private final Map<Variable, Resolvable> dependentOn;

        Plan(Set<Resolvable> resolvables) {
            assert resolvables.size() > 0;
            resolvableVars = new HashMap<>();
            plan = new ArrayList<>();
            dependentOn = dependencies(resolvables);

            Set<Resolvable> dependents = set(dependentOn.values());
            Set<Resolvable> independents = new HashSet<>(resolvables);
            independents.removeAll(dependents);

            Resolvable startingPoint = pickIndependentStartingPoint(independents).orElse(pickDependentStartingPoint(dependents));
            addToPlanBasedOnGenerated(startingPoint);

            assert plan.size() == resolvables.size();
            assert set(plan).equals(resolvables);
        }

        private void addToPlanBasedOnGenerated(Resolvable resolvable) {
            plan.add(resolvable);
            iterate(resolvable.generatedVars()).map(dependentOn::get).forEachRemaining(this::addToPlanBasedOnGenerated);
        }

        public List<Resolvable> plan() {
            return plan;
        }

        private Optional<Resolvable> pickIndependentStartingPoint(Set<Resolvable> independent) {
            if (independent.size() > 0) {
                Resolvable startingPoint = independent.stream().filter(Resolvable::isRetrievable).sorted(
                        Comparator.comparingInt(r -> r.conjunction().variables().size())).findFirst().orElse(
                        independent.stream().filter(Resolvable::isConcludable).findFirst().get());
                return Optional.of(startingPoint);
            }
            return Optional.empty();
        }

        private Resolvable pickDependentStartingPoint(Set<Resolvable> dependents) {
            return dependents.iterator().next();
        }

        private Map<Variable, Resolvable> dependencies(Set<Resolvable> resolvables) {
            Map<Variable, Resolvable> deps = new HashMap<>();
            final Set<Variable> generatedVars = iterate(resolvables).flatMap(r -> iterate(r.generatedVars())).toSet();
            for (Resolvable resolvable : resolvables) {
                for (Variable v : resolvable.conjunction().variables()) {
                    if (generatedVars.contains(v)) {
                        deps.put(v, resolvable);
                    }
                }
            }
            return deps;
        }
    }
}
