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
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.util.Objects.className;
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
//        private final Set<Resolvable> resolvables;
        private final Map<Resolvable, Set<Variable>> resolvableVars;
        private final List<Resolvable> plan;
        private Map<Variable, Resolvable> dependentOn;

        Plan(Set<Resolvable> resolvables) {
            assert resolvables.size() > 0;
//            this.resolvables = resolvables;
            resolvableVars = new HashMap<>();
            plan = new ArrayList<>();

            // Find a valid starting point
            // Ideally a retrievable (must have no dependencies)

            Map<Resolvable, Set<Variable>> deps = dependencies();
            Resolvable startingPoint = pickStartingPoint(deps);
            addToPlanBasedOnGenerated(startingPoint);
        }

        private void addToPlanBasedOnGenerated(Resolvable resolvable) {
            plan.add(resolvable);
            iterate(resolvable.generatedVars()).map(var -> dependentOn.get(var)).forEachRemaining(this::addToPlanBasedOnGenerated);
        }

        public List<Resolvable> plan() {
            return plan;
        }

        private Resolvable pickStartingPoint(Map<Resolvable, Set<Variable>> deps) {
            Resolvable startingPoint;
            Set<Resolvable> independent = iterate(deps.keySet()).filter(r -> deps.get(r).size() == 0).toSet();
            if (independent.size() > 0) {
                startingPoint = independent.stream().filter(Resolvable::isRetrievable).sorted(
                        Comparator.comparingInt(r -> r.conjunction().variables().size())).findFirst().orElse(
                        independent.stream().filter(Resolvable::isConcludable).findFirst().get());
            } else {
                // Pick any starting point arbitrarily
                startingPoint = deps.keySet().iterator().next();
            }
            return startingPoint;
        }

        private Map<Resolvable, Set<Variable>> dependencies() {
            return null;
        }
    }
}
