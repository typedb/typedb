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
    private final Set<RuleMode> printed;

    private ReasonerPlanPrinter(ReasonerPlanner planner) {
        this.planner = planner;
        sb = new StringBuilder();
        printed = new HashSet<>();
    }

    public static String print(ReasonerPlanner planner, Set<ResolvableConjunction> roots, Set<Variable> mode) {
        return new ReasonerPlanPrinter(planner).print(set(new RuleMode("<user>", mode, roots)));
    }

    private String print(Set<RuleMode> roots) {
        sb.append("================================================= Start reasoner plans =================================================\n");
        appendRecursive(roots);
        sb.append("=================================================  End reasoner plans  =================================================\n");
        return sb.toString();
    }

    private void appendRecursive(Set<RuleMode> ruleModes) {
        Set<RuleMode> triggered = new HashSet<>();
        ruleModes.forEach(call -> appendRuleMode(call, triggered));
        triggered.removeAll(printed);
        if (!triggered.isEmpty()) appendRecursive(triggered);
    }

    private void appendRuleMode(RuleMode ruleMode, Set<RuleMode> triggered) {
        appendRuleModeHeader(ruleMode);
        for (int i = 0; i < ruleMode.conjunctions.size(); i++) {
            appendCallMode(ruleMode.conjunctions.get(i), ruleMode.mode, triggered, "");
            if (ruleMode.conjunctions.size() - i > 1) appendBranchSeparator();
        }
        appendRuleModeSeparator();
        printed.add(ruleMode);
    }

    private void appendCallMode(ResolvableConjunction conjunction, Set<Variable> mode, Set<RuleMode> triggered, String nesting) {
        ReasonerPlanner.Plan plan = planner.getPlan(conjunction, mode);
        Set<Variable> runningBounds = new HashSet<>(mode);

        sb.append(nesting).append("Cost: ").append(plan.allCallsCost()).append("\n");
        for (int i = 0; i < plan.plan().size(); i++) {
            Resolvable<?> res = plan.plan().get(i);
            if (res.isNegated()) {
                final int index = i;
                res.asNegated().disjunction().conjunctions().forEach(nestedConjunction -> {
                    Set<Variable> negationMode = Collections.intersection(runningBounds, ReasonerPlanner.estimateableVariables(nestedConjunction.pattern().variables()));
                    appendResolvableHeader(nesting, index, "NEG", negationMode);
                    sb.append(nesting).append("{");
                    sb.append(nesting).append("}\n");
                    appendCallMode(nestedConjunction, negationMode, triggered, nesting + "\t");
                });
            } else {
                Set<Variable> resolvableMode = Collections.intersection(runningBounds, ReasonerPlanner.estimateableVariables(res.variables()));
                appendResolvable(res, i, resolvableMode, triggered, nesting);
            }
            if (!res.isNegated()) runningBounds.addAll(ReasonerPlanner.estimateableVariables(res.variables()));
        }
    }

    private void appendResolvable(Resolvable<?> res, int index, Set<Variable> mode, Set<RuleMode> triggered, String nesting) {
        if (res.isRetrievable()) {
            appendResolvableHeader(nesting, index, "RET", mode);
            appendResolvablePattern(nesting, res.asRetrievable().pattern());
        } else if (res.isConcludable()) {
            appendResolvableHeader(nesting, index, "CON", mode);
            appendResolvablePattern(nesting, res.asConcludable().pattern());
            triggeredRuleModes(res.asConcludable(), mode).forEach((ruleMode) -> {
                triggered.add(ruleMode);
                sb.append(nesting).append("\t\t- ").append(ruleMode.label).append("::").append(ruleMode.mode).append("\n");
            });
        }
    }

    private void appendRuleModeHeader(RuleMode ruleMode) {
        sb.append("--------------------------------\t\t").append(ruleMode.label).append("::").append(ruleMode.mode).append("\t\t--------------------------------\n");
    }

    private void appendResolvableHeader(String nesting, int resolvableIndex, String resolvableType, Set<Variable> bounds) {
        sb.append(String.format("%s[%d] %s {%s}\n", nesting, resolvableIndex, resolvableType,
                bounds.stream().map(v -> v.id().toString()).collect(Collectors.joining(", "))));
    }

    private void appendResolvablePattern(String nesting, Conjunction pattern) {
        sb.append(nesting).append('\t').append(pattern.toString().replace('\n', ' ')).append('\n');
    }

    private void appendBranchSeparator() {
        sb.append("- - - - - - - - - - - - - - - - - - - - - - - - NEXT BRANCH  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n\n");
    }

    private void appendRuleModeSeparator() {
        sb.append("\n========================================================================================================================\n");
    }

    private Set<RuleMode> triggeredRuleModes(Concludable concludable, Set<Variable> mode) {
        Map<ResolvableConjunction, String> labels = new HashMap<>();
        planner.logicMgr.applicableRules(concludable).keySet().forEach(rule -> {
            rule.condition().disjunction().conjunctions().forEach(conjunction -> labels.put(conjunction, rule.getLabel()));
        });

        Map<Pair<String, Set<Variable>>, Set<ResolvableConjunction>> builder = new HashMap<>();
        planner.triggeredCalls(concludable, mode, null).forEach(callMode -> {
            builder.computeIfAbsent(new Pair<>(labels.get(callMode.conjunction), callMode.mode), labelMode -> new HashSet<>()).add(callMode.conjunction);
        });

        Set<RuleMode> triggeredRuleModes = new HashSet<>();
        builder.forEach((labelMode, conjunctions) -> triggeredRuleModes.add(new RuleMode(labelMode.first(), labelMode.second(), conjunctions)));
        return triggeredRuleModes;
    }

    private static class RuleMode {
        private final String label;
        private final Set<Variable> mode;
        private final List<ResolvableConjunction> conjunctions;
        private final int hash;

        private RuleMode(String label, Set<Variable> mode, Set<ResolvableConjunction> conjunctions) {
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
            RuleMode that = (RuleMode) o;
            return label.equals(that.label) && mode.equals(that.mode);
        }
    }
}
