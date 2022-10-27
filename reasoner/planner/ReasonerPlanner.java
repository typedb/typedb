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

import com.vaticle.typedb.core.common.cache.CommonCache;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ReasonerPlanner {
    final ConceptManager conceptMgr;
    final TraversalEngine traversalEng;
    final LogicManager logicMgr;
    final CommonCache<CallMode, Plan> planCache;

    public ReasonerPlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.planCache = new CommonCache<>();
    }

    public static ReasonerPlanner create(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        return new RecursivePlanner(traversalEng, conceptMgr, logicMgr);
    }

    static Set<Variable> estimateableVariables(Set<Variable> variables) {
        return iterate(variables).filter(Variable::isThing).toSet();
    }

    static Set<Variable> estimateableVariables(Resolvable<?> resolvable) {
        return iterate(resolvable.variables()).filter(Variable::isThing).toSet();
    }

    static Set<Variable> retrievedVariables(Resolvable<?> resolvable) {
        return iterate(resolvable.variables()).filter(v -> v.id().isRetrievable()).toSet();
    }

    static boolean dependenciesSatisfied(Resolvable<?> resolvable, Set<Variable> bounds, Map<Resolvable<?>, Set<Variable>> dependencies) {
        return bounds.containsAll(dependencies.get(resolvable));
    }

    public void plan(ResolvableConjunction conjunction, Set<Variable> boundVariables) {
        plan(new CallMode(conjunction, estimateableVariables(boundVariables)));
    }

    public void planAllDependencies(Concludable concludable, Set<Variable> boundVariables) {
        triggeredCalls(concludable, estimateableVariables(boundVariables), Optional.empty())
                .forEach(callMode -> plan(callMode.conjunction, callMode.bounds));
    }

    public Plan getPlan(ResolvableConjunction conjunction, Set<Variable> boundVariables) {
        return getPlan(new CallMode(conjunction, estimateableVariables(boundVariables)));
    }

    void plan(CallMode callMode) {
        synchronized (planCache) {
            if (planCache.getIfPresent(callMode) == null) {
                planCache.put(callMode, computePlan(callMode));
            }
        }
    }

    Plan getPlan(CallMode callMode) {
        assert planCache.getIfPresent(callMode) != null;
        return planCache.getIfPresent(callMode);
    }

    abstract Plan computePlan(CallMode callMode);

    /**
     * Determine the resolvables that are dependent upon the generation of each variable
     */
    static Map<Resolvable<?>, Set<Variable>> dependencies(Set<Resolvable<?>> resolvables) {
        // TODO: There may not be any generated->used dependencies since every use either triggers the rule or is unsatisfiable
        Map<Resolvable<?>, Set<Variable>> deps = new HashMap<>();
        Set<Variable> generated = new HashSet<>(iterate(resolvables)
                .map(Resolvable::generating).filter(Optional::isPresent)
                .map(Optional::get).toSet());

        Map<Variable, Integer> unnegatedRefCount = new HashMap<>();
        for (Resolvable<?> resolvable : resolvables) {
            Optional<ThingVariable> generating = resolvable.generating();
            deps.putIfAbsent(resolvable, new HashSet<>());

            for (Variable v : retrievedVariables(resolvable)) {
                if (generated.contains(v) && !(generating.isPresent() && generating.get().equals(v))) {
                    deps.get(resolvable).add(v);
                }
                if (!resolvable.isNegated()) {
                    unnegatedRefCount.put(v, 1 + unnegatedRefCount.getOrDefault(v, 0));
                }
            }
        }

        for (Resolvable<?> resolvable : resolvables) {
            if (resolvable.isNegated()) {
                iterate(retrievedVariables(resolvable))
                        .filter(v -> unnegatedRefCount.getOrDefault(v, 0) > 0)
                        .forEachRemaining(v -> deps.get(resolvable).add(v));
            }
        }

        return deps;
    }

    public Set<CallMode> triggeredCalls(Concludable concludable, Set<Variable> concludableBounds, Optional<Set<ResolvableConjunction>> dependencyFilter) {
        Set<CallMode> calls = new HashSet<>();
        for (Map.Entry<Rule, Set<Unifier>> entry : logicMgr.applicableRules(concludable).entrySet()) {
            ResolvableConjunction ruleConjunction = entry.getKey().condition().conjunction();
            if (dependencyFilter.isPresent() && !dependencyFilter.get().contains(ruleConjunction)) {
                continue;
            }
            for (Unifier unifier : entry.getValue()) {
                assert iterate(concludableBounds).allMatch(v -> v.id().isRetrievable());
                Set<Identifier.Variable.Retrievable> ruleSideIds = iterate(concludableBounds)
                        .flatMap(v -> iterate(unifier.mapping().get(v.id().asRetrievable())))
                        .map(Identifier.Variable::asRetrievable)
                        .toSet();
                Set<Variable> ruleSideBounds = iterate(ruleSideIds)
                        .filter(id -> ruleConjunction.pattern().retrieves().contains(id))
                        .map(id -> ruleConjunction.pattern().variable(id)).toSet();
                calls.add(new CallMode(ruleConjunction, ruleSideBounds));
            }
        }
        return calls;
    }

    static class CallMode {
        final ResolvableConjunction conjunction;
        final Set<Variable> bounds;
        private final int hash;

        CallMode(ResolvableConjunction conjunction, Set<Variable> bounds) {
            this.conjunction = conjunction;
            this.bounds = bounds;
            this.hash = Objects.hash(conjunction, bounds);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CallMode that = (CallMode) o;
            return conjunction.equals(that.conjunction) && bounds.equals(that.bounds);
        }

        @Override
        public String toString() {
            return conjunction + "::{" + iterate(bounds).reduce("", (x, y) -> (x + ", " + y)) + "}";
        }
    }

    public static class Plan {
        private final List<Resolvable<?>> order;
        private final long cost;

        public Plan(List<Resolvable<?>> resolvableOrder, long cost) {
            this.order = resolvableOrder;
            this.cost = cost;
        }

        public List<Resolvable<?>> plan() {
            return order;
        }

        public long cost() {
            return cost;
        }
    }
}
