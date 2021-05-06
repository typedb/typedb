/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.util.StringBuilders;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.logic.tool.TypeResolver;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.CONTRADICTORY_RULE_CYCLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.logic.LogicManager.RuleExporter.writeRule;
import static java.util.Comparator.comparing;

public class LogicManager {

    private final GraphManager graphMgr;
    private final ConceptManager conceptMgr;
    private final TypeResolver typeResolver;
    private LogicCache logicCache;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.logicCache = logicCache;
        this.typeResolver = new TypeResolver(logicCache, traversalEng, conceptMgr);
    }


    GraphManager graph() { return graphMgr; }

    public TypeResolver typeResolver() {
        return typeResolver;
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        Rule rule = getRule(label);
        if (rule != null) {
            rule.delete();
            logicCache.rule().invalidate(label);
        }
        return logicCache.rule().get(label, l -> Rule.of(label, when, then, graphMgr, conceptMgr, this));
    }

    public Rule getRule(String label) {
        Rule rule = logicCache.rule().getIfPresent(label);
        if (rule != null) return rule;
        RuleStructure structure = graphMgr.schema().rules().get(label);
        if (structure != null) return logicCache.rule().get(structure.label(), l -> Rule.of(this, structure));
        return null;
    }

    public FunctionalIterator<Rule> rules() {
        return graphMgr.schema().rules().all().map(this::fromStructure);
    }

    public FunctionalIterator<Rule> rulesConcluding(Label type) {
        return graphMgr.schema().rules().conclusions().concludesVertex(graphMgr.schema().getType(type)).map(this::fromStructure);
    }

    public FunctionalIterator<Rule> rulesConcludingHas(Label attributeType) {
        return graphMgr.schema().rules().conclusions().concludesEdgeTo(graphMgr.schema().getType(attributeType)).map(this::fromStructure);
    }

    private FunctionalIterator<Rule> rulesWithNegations() {
        return rules().filter(rule -> !rule.when().negations().isEmpty());
    }

    /**
     * On commit we must clear the rule cache and revalidate rules - this will force re-running type resolution
     * when we re-load the Rule objects
     * Rule indexes should also be deleted and regenerated as needed
     * Note: does not need to be synchronized as only called by one schema transaction at a time
     */
    public void revalidateAndReindexRules() {
        logicCache.rule().clear();

        // re-validate all rules are valid
        rules().forEachRemaining(rule -> rule.validate(this, conceptMgr));

        // re-index if rules are valid and satisfiable
        if (graphMgr.schema().rules().conclusions().isOutdated()) {
            graphMgr.schema().rules().all().forEachRemaining(s -> fromStructure(s).reindex());
            graphMgr.schema().rules().conclusions().outdated(false);
        }

        // using the new index, validate new rules are stratifiable (eg. do not cause cycles through a negation)
        validateCyclesThroughNegations(conceptMgr, this);
    }

    private Rule fromStructure(RuleStructure ruleStructure) {
        return logicCache.rule().get(ruleStructure.label(), l -> Rule.of(this, ruleStructure));
    }

    private void validateCyclesThroughNegations(ConceptManager conceptMgr, LogicManager logicMgr) {
        Set<Rule> negationRulesTriggeringRules = logicMgr.rulesWithNegations()
                .filter(rule -> !rule.condition().negatedConcludablesTriggeringRules(conceptMgr, logicMgr).isEmpty())
                .toSet();

        for (Rule negationRule : negationRulesTriggeringRules) {
            Map<Rule, RuleDependency> visitedDependentRules = new HashMap<>();
            visitedDependentRules.put(negationRule, RuleDependency.of(negationRule, null));
            List<RuleDependency> frontier = new LinkedList<>(ruleDependencies(negationRule, conceptMgr, logicMgr));
            RuleDependency dependency;
            while (!frontier.isEmpty()) {
                dependency = frontier.remove(0);
                if (negationRule.equals(dependency.recursiveRule)) {
                    List<Rule> cycle = findCycle(dependency, visitedDependentRules);
                    String readableCycle = cycle.stream().map(Rule::getLabel).collect(Collectors.joining(" -> \n", "\n", "\n"));
                    throw TypeDBException.of(CONTRADICTORY_RULE_CYCLE, readableCycle);
                } else {
                    visitedDependentRules.put(dependency.recursiveRule, dependency);
                    Set<RuleDependency> recursive = ruleDependencies(dependency.recursiveRule, conceptMgr, logicMgr);
                    recursive.removeAll(visitedDependentRules.values());
                    frontier.addAll(recursive);
                }
            }
        }
    }

    private Set<RuleDependency> ruleDependencies(Rule rule, ConceptManager conceptMgr, LogicManager logicMgr) {
        return link(iterate(rule.condition().concludablesTriggeringRules(conceptMgr, logicMgr)),
                    iterate(rule.condition().negatedConcludablesTriggeringRules(conceptMgr, logicMgr)))
                .flatMap(concludable -> concludable.getApplicableRules(conceptMgr, logicMgr))
                .map(recursiveRule -> RuleDependency.of(recursiveRule, rule)).toSet();
    }

    private List<Rule> findCycle(RuleDependency dependency, Map<Rule, RuleDependency> visitedDependentRules) {
        List<Rule> cycle = new LinkedList<>();
        cycle.add(dependency.recursiveRule);
        Rule triggeringRule = dependency.triggeringRule;
        while (!cycle.contains(triggeringRule)) {
            cycle.add(triggeringRule);
            triggeringRule = visitedDependentRules.get(triggeringRule).triggeringRule;
        }
        cycle.add(triggeringRule);
        return cycle;
    }

    public void exportRules(StringBuilder builder) {
        rules().stream().sorted(comparing(Rule::getLabel)).forEach(x -> writeRule(builder, x));
    }

    private static class RuleDependency {

        final Rule recursiveRule;
        final Rule triggeringRule;

        private RuleDependency(Rule recursiveRule, @Nullable Rule triggeringRule) {
            this.recursiveRule = recursiveRule;
            this.triggeringRule = triggeringRule;
        }

        public static RuleDependency of(Rule rule, Rule triggeredFrom) {
            return new RuleDependency(rule, triggeredFrom);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RuleDependency that = (RuleDependency) o;
            return Objects.equals(recursiveRule, that.recursiveRule) && Objects.equals(triggeringRule, that.triggeringRule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recursiveRule, triggeringRule);
        }
    }

    // TODO: This class should be dissolved and its logic should be moved to Rules and Patterns
    static class RuleExporter {

        static void writeRule(StringBuilder builder, Rule rule) {
            builder.append(String.format("rule %s:\n", rule.getLabel()))
                    .append(StringBuilders.indent(1))
                    .append("when\n")
                    .append(getPatternString(wrapConjunction(rule.getWhenPreNormalised()), 1))
                    .append("\n")
                    .append(StringBuilders.indent(1))
                    .append("then\n")
                    .append(getPatternString(wrapConjunction(rule.getThenPreNormalised()), 1))
                    .append(StringBuilders.SEMICOLON_NEWLINE_X2);
        }

        static String getPatternString(Pattern pattern, int indent) {
            if (pattern.isVariable()) {
                return StringBuilders.indent(indent) + pattern.asVariable().toString();
            } else if (pattern.isConjunction()) {
                StringBuilder builder = new StringBuilder()
                        .append(StringBuilders.indent(indent))
                        .append("{\n");
                pattern.asConjunction().patterns().forEach(p -> builder
                        .append(getPatternString(p, indent + 1))
                        .append(";\n"));
                builder.append(StringBuilders.indent(indent))
                        .append("}");
                return builder.toString();
            } else if (pattern.isDisjunction()) {
                return pattern.asDisjunction().patterns().stream()
                        .map(p -> getPatternString(wrapConjunction(p), indent))
                        .collect(Collectors.joining("\n" + StringBuilders.indent(indent) + "or\n"));
            } else if (pattern.isNegation()) {
                return StringBuilders.indent(indent) + "not\n" + getPatternString(wrapConjunction(pattern.asNegation().pattern()), indent);
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        static Pattern wrapConjunction(Pattern pattern) {
            return pattern.isConjunction() ? pattern : new Conjunction<>(list(pattern));
        }
    }
}
