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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;

public class ReasonerPlanPrinter {

    private final ReasonerPlanner planner;
    private final StringBuilder sb;
    private final Set<CallGroup> printed;

    private ReasonerPlanPrinter(ReasonerPlanner planner) {
        this.planner = planner;
        sb = new StringBuilder();
        printed = new HashSet<>();
    }

    public static String print(ReasonerPlanner planner, Set<ResolvableConjunction> roots, Set<Variable> mode) {
        return new ReasonerPlanPrinter(planner).print(set(new CallGroup("(root-query)", mode, roots)));
    }

    private String print(Set<CallGroup> roots) {
        sb.append("================================================= Start reasoner plans =================================================\n");
        appendRecursive(roots);
        sb.append("=================================================  End reasoner plans  =================================================\n");
        return sb.toString();
    }

    private void appendRecursive(Set<CallGroup> callGroups) {
        Set<CallGroup> triggered = new HashSet<>();
        callGroups.forEach(call -> appendCallGroup(call, triggered));
        triggered.removeAll(printed);
        if (!triggered.isEmpty()) appendRecursive(triggered);
    }

    private void appendCallGroup(CallGroup callGroup, Set<CallGroup> triggered) {
        appendCallGroupHeader(callGroup);
        for (int i = 0; i < callGroup.conjunctions.size(); i++) {
            sb.append("Branch ").append(i+1).append(":").append("\n");
            appendCallMode(callGroup.conjunctions.get(i), callGroup.mode, triggered, "|\t");
            sb.append("\n");
        }
        appendCallGroupSeparator();
        printed.add(callGroup);
    }

    private void appendCallMode(ResolvableConjunction conjunction, Set<Variable> mode, Set<CallGroup> triggered, String nesting) {
        ReasonerPlanner.Plan plan = planner.getPlan(conjunction, mode);
        Set<Variable> runningBounds = new HashSet<>(mode);
        sb.append(nesting).append("Cost for all ").append(mode).append(":\t").append(plan.allCallsCost()).append("\n");
        for (int i = 0; i < plan.plan().size(); i++) {
            Resolvable<?> res = plan.plan().get(i);
            if (res.isNegated()) {
                final int index = i;
                res.asNegated().disjunction().conjunctions().forEach(nestedConjunction -> {
                    Set<Variable> negationMode = Collections.intersection(runningBounds, ReasonerPlanner.estimateableVariables(nestedConjunction.pattern().variables()));
                    appendResolvableHeader(nesting, index, "NEGATION", negationMode);
                    appendCallMode(nestedConjunction, negationMode, triggered, nesting + "|\t");
                });
            } else {
                Set<Variable> resolvableMode = Collections.intersection(runningBounds, ReasonerPlanner.estimateableVariables(res.variables()));
                appendResolvable(res, i, resolvableMode, triggered, nesting);
            }
            if (!res.isNegated()) runningBounds.addAll(ReasonerPlanner.estimateableVariables(res.variables()));
            sb.append(nesting).append("\n");
        }
    }

    private void appendResolvable(Resolvable<?> res, int index, Set<Variable> mode, Set<CallGroup> triggered, String nesting) {
        if (res.isRetrievable()) {
            appendResolvableHeader(nesting, index, "RETRIEVABLE", mode);
            appendResolvablePattern(nesting, res.asRetrievable().pattern());
        } else if (res.isConcludable()) {
            appendResolvableHeader(nesting, index, "CONCLUDABLE", mode);
            appendResolvablePattern(nesting, res.asConcludable().pattern());
            triggeredCallGroups(res.asConcludable(), mode).forEach((callGroup) -> {
                triggered.add(callGroup);
                sb.append(nesting).append("\t\t- ").append(callGroup.label).append("::").append(callGroup.mode).append("\n");
            });
        }
    }

    private void appendCallGroupHeader(CallGroup callGroup) {
        sb.append(callGroup.label).append("::").append(callGroup.mode).append("\n\n");
    }

    private void appendResolvableHeader(String nesting, int resolvableIndex, String resolvableType, Set<Variable> bounds) {
        sb.append(String.format("%s[%d]\t%s\t%s\n", nesting, resolvableIndex, resolvableType, bounds));
    }

    private void appendResolvablePattern(String nesting, Conjunction pattern) {
        sb.append(nesting).append('\t').append(pattern.toString().replace('\n', ' ')).append('\n');
    }

    private void appendCallGroupSeparator() {
        sb.append("\n------------------------------------------------------------------------------------------------------------------------\n");
    }

    private Set<CallGroup> triggeredCallGroups(Concludable concludable, Set<Variable> mode) {
        Map<ResolvableConjunction, String> labels = new HashMap<>();
        planner.logicMgr.applicableRules(concludable).keySet().forEach(rule -> {
            rule.condition().disjunction().conjunctions().forEach(conjunction -> labels.put(conjunction, rule.getLabel()));
        });

        Map<Pair<String, Set<Variable>>, Set<ResolvableConjunction>> builder = new HashMap<>();
        planner.triggeredCalls(concludable, mode, null).forEach(callMode -> {
            builder.computeIfAbsent(new Pair<>(labels.get(callMode.conjunction), callMode.mode), labelMode -> new HashSet<>()).add(callMode.conjunction);
        });

        Set<CallGroup> triggeredCallGroups = new HashSet<>();
        builder.forEach((labelMode, conjunctions) -> triggeredCallGroups.add(new CallGroup(labelMode.first(), labelMode.second(), conjunctions)));
        return triggeredCallGroups;
    }

    private static class CallGroup {
        private final String label;
        private final Set<Variable> mode;
        private final List<ResolvableConjunction> conjunctions;
        private final int hash;

        private CallGroup(String label, Set<Variable> mode, Set<ResolvableConjunction> conjunctions) {
            this.label = label;
            this.mode = mode;
            this.conjunctions = new ArrayList<>(conjunctions);
            this.hash = Objects.hash(label, mode);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CallGroup that = (CallGroup) o;
            return label.equals(that.label) && mode.equals(that.mode);
        }
    }
}
