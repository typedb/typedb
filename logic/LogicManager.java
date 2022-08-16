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

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.util.StringBuilders;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.logic.tool.TypeInference;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
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
    private final TypeInference typeInference;
    private final LogicCache logicCache;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.logicCache = logicCache;
        this.typeInference = new TypeInference(logicCache, traversalEng, graphMgr);
    }

    GraphManager graph() { return graphMgr; }

    public TypeInference typeInference() {
        return typeInference;
    }

    public void deleteAndInvalidateRule(Rule rule) {
        rule.delete();
        logicCache.rule().invalidate(rule.getLabel());
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        Rule rule = getRule(label);
        if (rule != null) deleteAndInvalidateRule(rule);
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

    public FunctionalIterator<Unifier> unifiers(Concludable concludable, Rule rule) {
        return iterate(applicableRules(concludable).getOrDefault(rule, new HashSet<>()));
    }

    public Map<Rule, Set<Unifier>> applicableRules(Concludable concludable) {
        return logicCache.applicableRules().get(concludable, c -> c.applicableRules(conceptMgr, this));
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
        validateCyclesThroughNegations();
    }

    private Rule fromStructure(RuleStructure ruleStructure) {
        return logicCache.rule().get(ruleStructure.label(), l -> Rule.of(this, ruleStructure));
    }

    private void validateCyclesThroughNegations() {
        Set<Rule> negationRulesTriggeringRules = this.rulesWithNegations()
                .filter(rule -> negatedRuleDependencies(rule).hasNext())
                .toSet();

        for (Rule negationRule : negationRulesTriggeringRules) {
            Map<Rule, RuleDependency> visitedDependentRules = new HashMap<>();
            LinkedList<RuleDependency> frontier = new LinkedList<>(negatedRuleDependencies(negationRule).toList());
            while (!frontier.isEmpty()) {
                RuleDependency dependency = frontier.removeFirst();
                visitedDependentRules.put(dependency.recursiveRule, dependency);
                if (negationRule.equals(dependency.recursiveRule)) {
                    List<Rule> cycle = findCycle(dependency, visitedDependentRules);
                    String readableCycle = cycle.stream().map(Rule::getLabel).collect(Collectors.joining(" -> \n", "\n", "\n"));
                    throw TypeDBException.of(CONTRADICTORY_RULE_CYCLE, readableCycle);
                } else {
                    ruleDependencies(dependency.recursiveRule)
                            .filter(rule -> !visitedDependentRules.containsKey(rule.recursiveRule))
                            .forEachRemaining(frontier::add);
                }
            }
        }
    }

    private FunctionalIterator<RuleDependency> ruleDependencies(Rule rule) {
        return link(iterate(negatedRuleDependencies(rule)),
                    iterate(rule.condition().conjunction().concludables()).flatMap(c -> iterate(applicableRules(c).keySet()))
                        .map(recursiveRule -> RuleDependency.of(recursiveRule, rule))
        );
    }

    private FunctionalIterator<RuleDependency> negatedRuleDependencies(Rule rule) {
        return iterate(rule.condition().conjunction().negations())
                .flatMap(neg -> iterate(neg.disjunction().conjunctions()))
                .flatMap(conj -> iterate(conj.concludables()))
                .flatMap(concludable -> iterate(applicableRules(concludable).keySet()))
                .map(recursiveRule -> RuleDependency.of(recursiveRule, rule));
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

    public String rulesSyntax() {
        StringBuilder builder = new StringBuilder();
        rules().stream().sorted(comparing(Rule::getLabel)).forEach(x -> writeRule(builder, x));
        return builder.toString();
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
