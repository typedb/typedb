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
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ThingConstraint;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class GreedyCostSearch extends ReasonerPlanner {

    public GreedyCostSearch(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
    }

    @Override
    Plan computePlan(CallMode callMode) {
        return computePlan(logicMgr.compile(callMode.conjunction), callMode.mode);
    }

    Plan computePlan(Set<Resolvable<?>> resolvables, Set<Variable> inputBounds) {
        Set<Variable> bounds = new HashSet<>(inputBounds);
        Set<Resolvable<?>> remaining = new HashSet<>(resolvables);
        long cost = 0;
        List<Resolvable<?>> orderedResolvables = new ArrayList<>();
        Map<Resolvable<?>, Set<Variable>> dependencies = dependencies(resolvables);
        boolean boundsExtended = false;
        while (!remaining.isEmpty()) {
            Pair<? extends Resolvable<?>, Long> nextResolvableCost = remaining.stream()
                    .filter(r -> dependenciesSatisfied(r, bounds, dependencies))
                    .map(r -> new Pair<>(r, estimateCost(r, bounds)))
                    .min(Comparator.comparing(Pair::second))
                    .orElse(null);

            // If the choice is disconnected, try extending bounds from IID/valueIdentity constraints.
            if (!boundsExtended && (nextResolvableCost == null || nextResolvableCost.second() > 10)) {
                boundsExtended = true;
                iterate(remaining).filter(Resolvable::isRetrievable).flatMap(resolvable -> iterate(retrievedVariables(resolvable)))
                        .flatMap(variable -> iterate(variable.constraints()))
                        .filter(Constraint::isThing).map(Constraint::asThing)
                        .filter(thingConstraint -> (thingConstraint.isIID() || (thingConstraint.isValue() && thingConstraint.asValue().isValueIdentity())))
                        .map(ThingConstraint::owner)
                        .forEachRemaining(bounds::add);
                continue;
            }

            if (nextResolvableCost == null) {
                nextResolvableCost = remaining.stream()
                        .filter(r -> !dependenciesSatisfied(r, bounds, dependencies) && !r.isNegated())
                        .map(r -> new Pair<>(r, estimateCost(r, bounds)))
                        .min(Comparator.comparing(Pair::second))
                        .orElse(null);
            }
            assert nextResolvableCost != null;
            Resolvable<?> nextResolvable = nextResolvableCost.first();
            cost += nextResolvableCost.second();
            orderedResolvables.add(nextResolvable);
            remaining.remove(nextResolvable);
            if (!nextResolvable.isNegated()) {
                bounds.addAll(retrievedVariables(nextResolvable));
            }
        }
        assert resolvables.size() == orderedResolvables.size() && iterate(orderedResolvables).allMatch(resolvables::contains);
        return new Plan(orderedResolvables, cost);
    }

    abstract long estimateCost(Resolvable<?> resolvable, Set<Variable> bounds);

    public static class HeuristicPlannerEmulator extends GreedyCostSearch {

        public HeuristicPlannerEmulator(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
            super(traversalEng, conceptMgr, logicMgr);
        }

        @Override
        long estimateCost(Resolvable<?> r, Set<Variable> bounds) {
            long cost = 0;
            cost += iterate(r.variables()).anyMatch(bounds::contains) ? 0 : 10; // Connected:disconnected
            if (r.isRetrievable()) {
                cost += 1;
            } else if (r.isConcludable()) {
                cost += 2;
            } else if (r.isNegated()) { // always at the end
                cost += 13;
            }
            return cost;
        }
    }
}
