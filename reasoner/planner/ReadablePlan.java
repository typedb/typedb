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

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ReadablePlan {

    public final String label;
    private final ReasonerPlanner.CallMode callMode;
    public final Map<Resolvable<?>, ReadablePlan> nested;
    public final Map<Resolvable<?>, Set<ReadablePlan>> triggeredCalls;
    public final List<Resolvable<?>> evaluationOrder;
    public final Map<Resolvable<?>, Set<Variable>> modes;
    private final double cost;

    public ReadablePlan(String label, ReasonerPlanner.CallMode callMode, List<Resolvable<?>> evaluationOrder, double cost) {
        this.label = label;
        this.callMode = callMode;
        this.evaluationOrder = evaluationOrder;
        this.cost = cost;
        this.nested = new HashMap<>();
        this.triggeredCalls = new HashMap<>();
        this.modes = new HashMap<>();
    }

    static Set<ReadablePlan> summarise(ReasonerPlanner planner, Set<ResolvableConjunction> rootConjunctions) {
        return new Summariser(planner).summarise(rootConjunctions);
    }

    public static String prettyString(Set<ReadablePlan> rootPlans) {
        StringBuilder sb = new StringBuilder();
        prettyString(rootPlans, new HashSet<>(), sb);
        return sb.toString();
    }

    private static void prettyString(Set<ReadablePlan> thisLevel, Set<ReadablePlan> seen, StringBuilder sb) {
        Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> byLabelMode = new HashMap<>();
        thisLevel.forEach(plan -> {
            byLabelMode.computeIfAbsent(new Pair<>(plan.label, plan.callMode.mode), key -> new HashSet<>()).add(plan);
        });


        Set<ReadablePlan> nextLevel = new HashSet<>();
        byLabelMode.forEach((labelMode, plans) -> {
            sb.append("--\t").append(labelMode.first()).append("::").append(labelMode.second()).append("\t--\n");
            plans.forEach(singlePlan -> {
                prettyString(singlePlan, sb, "");
                singlePlan.triggeredCalls.values().forEach(calls -> nextLevel.addAll(calls));
            });

            seen.addAll(plans);
        });
        nextLevel.removeAll(seen);

        if (!nextLevel.isEmpty()) prettyString(nextLevel, seen, sb);
    }

    private static void prettyString(ReadablePlan toPrint, StringBuilder sb, String nesting) {
        sb.append("Cost: ").append(toPrint.cost).append("\n");
        for (int i = 0; i < toPrint.evaluationOrder.size(); i++) {
            Resolvable<?> res = toPrint.evaluationOrder.get(i);
            Set<Variable> bounds = toPrint.modes.get(res);
            if (res.isRetrievable()) {
                appendHeader(sb, nesting, i, "RET", bounds);
                appendPattern(sb, nesting, res.asRetrievable().pattern());
            } else if (res.isConcludable()) {
                appendHeader(sb, nesting, i, "CON", bounds);
                appendPattern(sb, nesting, res.asConcludable().pattern());
                toPrint.triggeredCalls.get(res).forEach(call -> {
                    sb.append(nesting).append("\t- ").append(call.label).append("::").append(call.callMode.mode).append("\n");
                });
            } else if (res.isNegated()) {
                appendHeader(sb, nesting, i, "NEG", bounds);
                prettyString(toPrint.nested.get(res), sb, nesting + "\t");
                sb.append(nesting).append("}\n");
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }
        sb.append("---\n");
    }

    private static void appendPattern(StringBuilder sb, String nesting, Conjunction pattern) {
        sb.append(nesting).append(pattern.toString().replace('\n', ' ')).append('\n');
    }

    private static void appendHeader(StringBuilder sb, String nesting, int resolvableIndex, String resolvableType, Set<Variable> bounds) {
        sb.append(String.format("%s[%d] %s [bounds: %s]\n", nesting, resolvableIndex, resolvableType,
                String.join(", ", bounds.stream().map(v -> v.id().toString()).collect(Collectors.toList()))));
    }

    private static class Summariser {
        private final ReasonerPlanner planner;
        private final Map<ReasonerPlanner.CallMode, ReadablePlan> done;
        private final Map<ResolvableConjunction, Rule> fromRule;

        public Summariser(ReasonerPlanner planner) {
            this.planner = planner;
            this.done = new HashMap<>();
            this.fromRule = new HashMap<>();
            planner.logicMgr.rules().forEachRemaining(rule -> {
                rule.condition().disjunction().conjunctions().forEach(conj -> {
                    fromRule.put(conj, rule);
                });
            });
        }

        public Set<ReadablePlan> summarise(Set<ResolvableConjunction> rootConjunctions) {
            Set<ReadablePlan> rootPlans = new HashSet<>();
            rootConjunctions.forEach(conj -> {
                ReasonerPlanner.CallMode callMode = new ReasonerPlanner.CallMode(conj, set());
                ReadablePlan rootPlan = summarise(callMode, "<user>");
                rootPlans.add(rootPlan);
            });
            return rootPlans;
        }

        private ReadablePlan summarise(ReasonerPlanner.CallMode callMode, String label) {
            if (done.containsKey(callMode)) return done.get(callMode);


            ReasonerPlanner.Plan plan = planner.getPlan(callMode);
            ReadablePlan readablePlan = new ReadablePlan(label, callMode, plan.plan(), plan.allCallsCost());
            done.put(callMode, readablePlan);

            Set<Variable> runningBounds = new HashSet<>(callMode.mode);
            for (Resolvable<?> res : plan.plan()) {
                readablePlan.modes.put(res, runningBounds);
                if (res.isNegated()) {
                    res.asNegated().disjunction().conjunctions().forEach(nestedConjunction -> {
                        ReasonerPlanner.CallMode nestedCallMode = new ReasonerPlanner.CallMode(nestedConjunction, Collections.intersection(nestedConjunction.pattern().variables(), runningBounds));
                        readablePlan.nested.put(res, summarise(nestedCallMode, "<negated>"));
                    });
                } else if (res.isConcludable()) {
                    Set<ReasonerPlanner.CallMode> triggeredCallModes = planner.triggeredCalls(res.asConcludable(), Collections.intersection(runningBounds, res.asConcludable().variables()), null);
                    readablePlan.triggeredCalls.put(res, iterate(triggeredCallModes).map(cm -> summarise(cm, fromRule.get(cm.conjunction).getLabel())).toSet());
                }

                if (!res.isNegated()) runningBounds.addAll(ReasonerPlanner.estimateableVariables(res.variables()));
            }

            return readablePlan;
        }
    }
}
