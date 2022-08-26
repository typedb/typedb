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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ReasonerPlanner {
    protected final Map<Pair<ResolvableConjunction, Set<Identifier.Variable.Retrievable>>, Plan<Resolvable<?>>> planCache;
    protected final ConceptManager conceptMgr;
    protected final TraversalEngine traversalEng;
    protected final LogicManager logicMgr;

    public ReasonerPlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        planCache = new HashMap<>();
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
    }

    public static ReasonerPlanner create(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        return new GreedyCostSearch.OldPlannerEmulator(traversalEng, conceptMgr, logicMgr);
    }

    public Plan<Resolvable<?>> plan(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> bounds) {
        Pair<ResolvableConjunction, Set<Identifier.Variable.Retrievable>> plannableKey = new Pair<>(conjunction, bounds);
        if (!planCache.containsKey(plannableKey)) {
            synchronized (planCache) {
                if (!planCache.containsKey(plannableKey)) {
                    planCache.put(plannableKey, planConjunction(conjunction, bounds));
                }
            }
        }
        return planCache.get(plannableKey);
    }

    Plan<Resolvable<?>> planConjunction(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> inputBounds) {
        return planResolvableOrdering(logicMgr.compile(conjunction), inputBounds);
    }

    abstract Plan<Resolvable<?>> planResolvableOrdering(Set<Resolvable<?>> resolvables, Set<Identifier.Variable.Retrievable> bounds);

    protected boolean dependenciesSatisfied(Resolvable<?> resolvable, Set<Identifier.Variable.Retrievable> bounds, Map<Resolvable<?>, Set<Identifier.Variable.Retrievable>> dependencies) {
        return bounds.containsAll(dependencies.get(resolvable));
    }

    /**
     * Determine the resolvables that are dependent upon the generation of each variable
     */
    protected static Map<Resolvable<?>, Set<Identifier.Variable.Retrievable>> dependencies(Set<Resolvable<?>> resolvables) {
        Map<Resolvable<?>, Set<Identifier.Variable.Retrievable>> deps = new HashMap<>();
        Set<Identifier.Variable.Retrievable> generated = iterate(resolvables).map(Resolvable::generating).filter(Optional::isPresent)
                .map(Optional::get).map(ThingVariable::id).toSet();

        Map<Identifier.Variable.Retrievable, Integer> unnegatedRefCount = new HashMap<>();
        for (Resolvable<?> resolvable : resolvables) {
            Optional<ThingVariable> generating = resolvable.generating();
            deps.putIfAbsent(resolvable, new HashSet<>());
            for (Identifier.Variable.Retrievable v : resolvable.retrieves()) {
                if (generated.contains(v) && !(generating.isPresent() && generating.get().id().equals(v))) {
                    // TODO: Should this rule the Resolvable<?> out if generates it's own dependency?
                    deps.get(resolvable).add(v);
                }
                if (!resolvable.isNegated()) {
                    unnegatedRefCount.put(v, 1 + unnegatedRefCount.getOrDefault(v, 0));
                }
            }
        }

        for (Resolvable<?> resolvable : resolvables) {
            if (resolvable.isNegated()) {
                iterate(resolvable.retrieves())
                    .filter(v -> unnegatedRefCount.getOrDefault(v, 0) > 0)
                    .forEachRemaining(v -> deps.get(resolvable).add(v));
            }
        }

        return deps;
    }

    public static class Plan<PLANELEMENT> {
        private final List<PLANELEMENT> elementOrder;
        private final long cost;

        public Plan(List<PLANELEMENT> elementOrder, long cost) {
            this.elementOrder = elementOrder;
            this.cost = cost;
        }

        public List<PLANELEMENT> planOrder() { return elementOrder; }
        
        public long cost() { return cost; }
    }
}
