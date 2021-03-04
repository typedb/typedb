/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.logic;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.tool.TypeResolver;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.CONTRADICTORY_RULE_CYCLE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static graql.lang.common.GraqlToken.Char.CURLY_CLOSE;
import static graql.lang.common.GraqlToken.Char.CURLY_OPEN;
import static graql.lang.common.GraqlToken.Char.NEW_LINE;
import static graql.lang.common.GraqlToken.Char.SEMICOLON;
import static graql.lang.common.GraqlToken.Char.SPACE;

public class LogicManager {

    private final GraphManager graphMgr;
    private final ConceptManager conceptMgr;
    private final TypeResolver typeResolver;
    private LogicCache logicCache;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.logicCache = logicCache;
        this.typeResolver = new TypeResolver(conceptMgr, traversalEng, logicCache);
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        RuleStructure structure = graphMgr.schema().rules().get(label);
        if (structure != null) {
            // overwriting a rule means we purge it and re-create the rule
            structure.delete();
            logicCache.rule().invalidate(label);
        }
        return logicCache.rule().get(label, l -> Rule.of(graphMgr, this, label, when, then));
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

        // validate all rules are valid and satisfiable
        rules().forEachRemaining(Rule::validateSatisfiable);

        // re-index if rules are valid and satisfiable
        if (graphMgr.schema().rules().conclusions().isOutdated()) {
            graphMgr.schema().rules().all().forEachRemaining(s -> fromStructure(s).reindex());
            graphMgr.schema().rules().conclusions().outdated(false);
        }

        // using the new index, validate new rules are stratifiable (eg. do not cause cycles through a negation)
        validateCycles(conceptMgr, this);
    }


    private void validateCycles(ConceptManager conceptMgr, LogicManager logicMgr) {
        Set<Rule> negationRulesTriggeringRules = logicMgr.rulesWithNegations()
                .filter(rule -> !rule.condition().negatedConcludablesTriggeringRules(conceptMgr, logicMgr).isEmpty())
                .toSet();

        for (Rule negationRule : negationRulesTriggeringRules) {
            Map<Rule, RuleDependency> visitedRecursiveRules = new HashMap<>();
            visitedRecursiveRules.put(negationRule, RuleDependency.of(negationRule, null));
            List<RuleDependency> frontier = new LinkedList<>(recursiveRules(negationRule, conceptMgr, logicMgr));
            RuleDependency visiting;
            while (!frontier.isEmpty()) {
                visiting = frontier.remove(0);
                if (visitedRecursiveRules.containsKey(visiting.rule)) {
                    List<Rule> cycle = buildCycle(visiting, visitedRecursiveRules);
                    String readableCycle = cycle.stream().map(Rule::getLabel).collect(Collectors.joining(" -> \n"));
                    throw GraknException.of(CONTRADICTORY_RULE_CYCLE, "\n" + readableCycle);
                } else {
                    visitedRecursiveRules.put(visiting.rule, visiting);
                    Set<RuleDependency> recursive = recursiveRules(visiting.rule, conceptMgr, logicMgr);
                    recursive.removeAll(visitedRecursiveRules.values());
                    frontier.addAll(recursive);
                }
            }
        }
    }

    private List<Rule> buildCycle(RuleDependency dependency, Map<Rule, RuleDependency> visitedRecursiveRules) {
        List<Rule> cycle = new LinkedList<>();
        cycle.add(dependency.rule);
        Rule triggeringRule = dependency.triggeringRule;
        while (!cycle.contains(triggeringRule)) {
            cycle.add(triggeringRule);
            triggeringRule = visitedRecursiveRules.get(triggeringRule).triggeringRule;
        }
        cycle.add(triggeringRule);
        return cycle;
    }

    private Set<RuleDependency> recursiveRules(Rule rule, ConceptManager conceptMgr, LogicManager logicMgr) {
        return link(iterate(rule.condition().concludablesTriggeringRules(conceptMgr, logicMgr)),
                    iterate(rule.condition().negatedConcludablesTriggeringRules(conceptMgr, logicMgr)))
                .flatMap(concludable -> concludable.getApplicableRules(conceptMgr, logicMgr))
                .map(recursiveRule -> RuleDependency.of(recursiveRule, rule)).toSet();
    }

    private static class RuleDependency {
        final Rule rule;
        final Rule triggeringRule;

        private RuleDependency(Rule rule, @Nullable Rule triggeredFrom) {
            this.rule = rule;
            this.triggeringRule = triggeredFrom;
        }

        public static RuleDependency of(Rule rule, Rule triggeredFrom) {
            return new RuleDependency(rule, triggeredFrom);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RuleDependency that = (RuleDependency) o;
            return Objects.equals(rule, that.rule) && Objects.equals(triggeringRule, that.triggeringRule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rule, triggeringRule);
        }
    }

    private Rule fromStructure(RuleStructure ruleStructure) {
        return logicCache.rule().get(ruleStructure.label(), l -> Rule.of(this, ruleStructure));
    }


    public TypeResolver typeResolver() {
        return typeResolver;
    }

    GraphManager graph() { return graphMgr; }

}
