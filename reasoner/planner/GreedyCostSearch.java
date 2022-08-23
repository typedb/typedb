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

import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class GreedyCostSearch extends ReasonerPlanner {

    public GreedyCostSearch(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
    }

    /* TODO:
     * AnswerSize -> For each resolvable: Add the number of calls + the answers produced
     * AnswersProduced? TraversalEngine tells us this based on constraints in the rule-body
     * #calls? Product of the cardinality of each variable in the bounds (This number reduces as you evaluate a new resolvable which further constrains the variable - but min or less?)ssssssss
     * Effect? Schedule stronger constraints first.
     */
    @Override
    protected Plan<Resolvable<?>> planConjunction(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> inputBounds) {
        Set<Identifier.Variable.Retrievable> bounds = new HashSet<>(inputBounds);
        Set<Resolvable<?>> resolvables = compile(conjunction);

        Set<Resolvable<?>> remaining = new HashSet<>(resolvables);
        long cost = 0;
        List<Resolvable<?>> orderedResolvables = new ArrayList<>();
        Map<Resolvable<?>, Set<Identifier.Variable.Retrievable>> dependencies = dependencies(resolvables);

        while (!remaining.isEmpty()) {
            Optional<Resolvable<?>> nextResolvableOpt = remaining.stream()
                    .filter(r -> dependenciesSatisfied(r, bounds, dependencies))
                    .min(Comparator.comparing(r -> estimateCost(r, bounds)));

            if (nextResolvableOpt.isEmpty()) {
                nextResolvableOpt = remaining.stream()
                        .filter(r -> !dependenciesSatisfied(r, bounds, dependencies))
                        .min(Comparator.comparing(r -> estimateCost(r, bounds)));
            }
            assert nextResolvableOpt.isPresent();
            Resolvable<?> nextResolvable = nextResolvableOpt.get();
            cost += estimateCost(nextResolvable, bounds); // TODO: Eliminate double work
            orderedResolvables.add(nextResolvable);
            remaining.remove(nextResolvable);
            bounds.addAll(nextResolvable.retrieves());
        }
        assert resolvables.size() == orderedResolvables.size() && iterate(orderedResolvables).allMatch(r -> resolvables.contains(r));
        return new Plan<>(orderedResolvables, cost);
    }

    abstract long estimateCost(Resolvable<?> resolvable, Set<Identifier.Variable.Retrievable> bounds);

    public static class OldPlannerEmulator extends GreedyCostSearch {

        public OldPlannerEmulator(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
            super(traversalEng, conceptMgr, logicMgr);
        }

        @Override
        long estimateCost(Resolvable<?> r, Set<Identifier.Variable.Retrievable> bounds) {
            long cost = 0;
            cost += r.retrieves().stream().anyMatch(v -> bounds.contains(v)) ? 0 : 10; // Connected:disconnected
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
