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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.ArrayList;
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
    private final ReasonerPlanner.CallMode callMode; // Essentially the branch.
    private final double cost;
    public List<ResolvableSummary> resolvableSummaries;

    public ReadablePlan(String label, ReasonerPlanner.CallMode callMode, double cost) {
        this.label = label;
        this.callMode = callMode;
        this.resolvableSummaries = null;
        this.cost = cost;
    }

    public void setResolvableSummaries(List<ResolvableSummary> resolvableSummaries) {
        assert this.resolvableSummaries == null;
        this.resolvableSummaries = resolvableSummaries;
    }

    static Set<ReadablePlan> summarise(ReasonerPlanner planner, Set<ResolvableConjunction> rootConjunctions) {
        return new Summariser(planner).summarise(rootConjunctions);
    }

    public static String prettyString(Set<ReadablePlan> rootPlans) {
        StringBuilder sb = new StringBuilder();
        prettyString(Map.of(new Pair<>("<user>", set()), rootPlans), new HashSet<>(), sb);
        return sb.toString();
    }

    private static void prettyString(Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> byLabelMode, Set<Pair<String, Set<Variable>>> seen, StringBuilder sb) {
        Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> nextLevel = new HashMap<>();

        byLabelMode.forEach((labelMode, plans) -> {
            sb.append("--------------------\t\t").append(labelMode.first()).append("::").append(labelMode.second()).append("\t\t--------------------\n");
            List<ReadablePlan> planList = new ArrayList<>(plans);
            planList.forEach(branch -> {
                prettyString(branch, sb, "");
                if (branch != planList.get(planList.size() - 1)) {
                    sb.append("- - - - - - - - - - - - - - -NEXT BRANCH- - - - - - - - - - - - - - - - - - - - \n\n");
                }
                iterate(branch.resolvableSummaries).forEachRemaining(resolvableSummary -> {
                    resolvableSummary.triggeredCalls().forEach((k, v) -> {
                        if (!nextLevel.containsKey(k)) nextLevel.put(k, v);
                        assert nextLevel.get(k).equals(v);
                    });
                });
            });
            sb.append("\n========================================================================================================================\n");
            seen.add(labelMode);
        });
        seen.forEach(nextLevel::remove);
        if (!nextLevel.isEmpty()) prettyString(nextLevel, seen, sb);
    }

    private static void prettyString(ReadablePlan toPrint, StringBuilder sb, String nesting) {
        sb.append(nesting).append("Cost: ").append(toPrint.cost).append("\n");
        for (int i = 0; i < toPrint.resolvableSummaries.size(); i++) {
            ResolvableSummary summary = toPrint.resolvableSummaries.get(i);
            Resolvable<?> res = summary.resolvable;
            Set<Variable> bounds = summary.mode;
            if (res.isRetrievable()) {
                appendHeader(sb, nesting, i, "RET", bounds);
                appendPattern(sb, nesting, res.asRetrievable().pattern());
            } else if (res.isConcludable()) {
                appendHeader(sb, nesting, i, "CON", bounds);
                appendPattern(sb, nesting, res.asConcludable().pattern());
                summary.triggeredCalls().forEach((labelMode, readablePlanSet) -> {
                    assert labelMode.second().equals(readablePlanSet.stream().findAny().get().callMode.mode);
                    sb.append(nesting).append("\t\t- ").append(labelMode.first()).append("::").append(labelMode.second()).append("\n");
                });
            } else if (res.isNegated()) {
                appendHeader(sb, nesting, i, "NEG", bounds);
                sb.append(nesting).append("{");
                prettyString(((ResolvableSummary.NegatedSummary) summary).negatedPlan, sb, nesting + "\t");
                sb.append(nesting).append("}\n");
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private static void appendPattern(StringBuilder sb, String nesting, Conjunction pattern) {
        sb.append(nesting).append('\t').append(pattern.toString().replace('\n', ' ')).append('\n');
    }

    private static void appendHeader(StringBuilder sb, String nesting, int resolvableIndex, String resolvableType, Set<Variable> bounds) {
        sb.append(String.format("%s[%d] %s {%s}\n", nesting, resolvableIndex, resolvableType,
                bounds.stream().map(v -> v.id().toString()).collect(Collectors.joining(", "))));
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
                rule.condition().disjunction().conjunctions().forEach(conj -> fromRule.put(conj, rule));
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
            ReadablePlan readablePlan = new ReadablePlan(label, callMode, plan.allCallsCost());
            done.put(callMode, readablePlan);

            List<ResolvableSummary> resolvableSummaries = new ArrayList<>();
            Set<Variable> runningBounds = new HashSet<>(callMode.mode);
            for (Resolvable<?> res : plan.plan()) {
                Set<Variable> resolvableMode = Collections.intersection(runningBounds, res.variables());
                if (res.isNegated()) {
                    res.asNegated().disjunction().conjunctions().forEach(nestedConjunction -> {
                        ReasonerPlanner.CallMode nestedCallMode = new ReasonerPlanner.CallMode(nestedConjunction, resolvableMode);
                        ReadablePlan nestedPlan = summarise(nestedCallMode, "<negated>");
                        resolvableSummaries.add(new ResolvableSummary.NegatedSummary(res.asNegated(), resolvableMode, nestedPlan));
                    });
                } else if (res.isConcludable()) {
                    Set<ReasonerPlanner.CallMode> triggeredCallModes = planner.triggeredCalls(res.asConcludable(), resolvableMode, null);
                    Set<ReadablePlan> triggeredCalls = iterate(triggeredCallModes).map(cm -> summarise(cm, fromRule.get(cm.conjunction).getLabel())).toSet();
                    resolvableSummaries.add(new ResolvableSummary.ConcludableSummary(res.asConcludable(), resolvableMode, triggeredCalls));
                } else if (res.isRetrievable()) {
                    resolvableSummaries.add(new ResolvableSummary.RetrievableSummary(res.asRetrievable(), resolvableMode));
                } else throw TypeDBException.of(ILLEGAL_STATE);

                if (!res.isNegated()) runningBounds.addAll(ReasonerPlanner.estimateableVariables(res.variables()));
            }
            readablePlan.setResolvableSummaries(resolvableSummaries);

            return readablePlan;
        }
    }

    private static class ResolvableSummary {
        private final Resolvable<?> resolvable;
        private final Set<Variable> mode;

        public ResolvableSummary(Resolvable<?> resolvable, Set<Variable> mode) {
            this.resolvable = resolvable;
            this.mode = mode;
        }

        public Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> triggeredCalls() {
            return java.util.Collections.EMPTY_MAP;
        }

        public static class ConcludableSummary extends ResolvableSummary {
            private final Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> triggeredCalls;

            public ConcludableSummary(Concludable resolvable, Set<Variable> mode, Set<ReadablePlan> triggeredCalls) {
                super(resolvable, mode);
                this.triggeredCalls = byLabelMode(triggeredCalls);
            }

            private static Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> byLabelMode(Set<ReadablePlan> triggeredCalls) {
                Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> byRule = new HashMap<>();
                triggeredCalls.forEach(r -> byRule.computeIfAbsent(new Pair<>(r.label, r.callMode.mode), l -> new HashSet<>()).add(r));
                return byRule;
            }

            @Override
            public Map<Pair<String, Set<Variable>>, Set<ReadablePlan>> triggeredCalls() {
                return triggeredCalls;
            }
        }

        public static class NegatedSummary extends ResolvableSummary {
            private final ReadablePlan negatedPlan;

            public NegatedSummary(Negated negated, Set<Variable> resolvableMode, ReadablePlan negatedPlan) {
                super(negated, resolvableMode);
                this.negatedPlan = negatedPlan;
            }
        }

        public static class RetrievableSummary extends ResolvableSummary {
            public RetrievableSummary(Retrievable retrievable, Set<Variable> resolvableMode) {
                super(retrievable, resolvableMode);
            }
        }
    }
}
