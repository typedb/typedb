package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.*;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.*;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class PlanSearch {
    protected final Map<Plannable, Plan> planCache;
    protected final ConceptManager conceptMgr;
    protected final TraversalEngine traversalEng;
    protected final LogicManager logicMgr;
    private final HashMap<ResolvableConjunction, Pair<Set<Concludable>, Set<Retrievable>>> compiled;

    public PlanSearch(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        planCache = new HashMap<>();
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.compiled = new HashMap<>();
    }

    public Pair<Set<Concludable>, Set<Retrievable>> compile(ResolvableConjunction conjunction){
        synchronized (compiled) {
            return compiled.computeIfAbsent(conjunction, conj -> {
                Set<Concludable> concludablesTriggeringRules = iterate(conjunction.positiveConcludables()).filter(concludable -> !logicMgr.applicableRules(concludable).isEmpty()).toSet();
                return new Pair(concludablesTriggeringRules, Retrievable.extractFrom(conjunction.pattern(), concludablesTriggeringRules));
            });
        }
    }

    public Plan getPlan(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> bounds) {
        return getPlan(Plannable.ofConjunction(conjunction, bounds));
    }

    private Plan getPlan(Plannable plannableKey) {
        Plan plan = null;
        if (!planCache.containsKey(plannableKey)) {
            plan = plannableKey.callPlannerFunction(this);
            synchronized (planCache) {
                planCache.put(plannableKey, plan);
            }
        } else {
            synchronized (planCache) {
                plan = planCache.get(plannableKey);
            }
        }
        return plan;
    }

    protected abstract Plan<Resolvable<?>> planConjunction(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> bounds);

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

        Map<Identifier.Variable.Retrievable, Integer> refCounts = new HashMap<>();
        for (Resolvable<?> resolvable : resolvables) {
            Optional<ThingVariable> generating = resolvable.generating();
            deps.putIfAbsent(resolvable, new HashSet<>());
            for (Identifier.Variable.Retrievable v : resolvable.retrieves()) {
                refCounts.put(v, 1 + refCounts.getOrDefault(v, 0));
                if (generated.contains(v) && !(generating.isPresent() && generating.get().id().equals(v))) {
                    // TODO: Should this rule the Resolvable<?> out if generates it's own dependency?
                    deps.get(resolvable).add(v);
                }
            }
        }

        for (Resolvable<?> resolvable : resolvables) {
            if (resolvable.isNegated()) {
                for (Identifier.Variable.Retrievable v : resolvable.retrieves()) {
                    if (refCounts.get(v) > 1) {
                        deps.get(resolvable).add(v);
                    }
                }
            }
        }

        return deps;
    }

    public static class Plan<PLANELEMENT> {
        List<PLANELEMENT> elementOrder;
        long cost;

        Plan(List<PLANELEMENT> elementOrder, long cost) {
            this.elementOrder = elementOrder;
            this.cost = cost;
        }

        public List<PLANELEMENT> resolvableOrder() {
            return elementOrder;
        }
    }
}
