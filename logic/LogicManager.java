/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.logic.tool.ExpressionResolver;
import com.vaticle.typedb.core.logic.tool.TypeInference;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.CONTRADICTORY_RULE_CYCLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Comparator.comparing;

public class LogicManager {

    private final GraphManager graphMgr;
    private final ConceptManager conceptMgr;
    private final TypeInference typeInference;
    private final ExpressionResolver expressionResolver;
    private final LogicCache logicCache;
    private final Map<ResolvableConjunction, Set<Resolvable<?>>> compiledConjunctions;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.logicCache = logicCache;
        this.typeInference = new TypeInference(logicCache, traversalEng, graphMgr);
        this.expressionResolver = new ExpressionResolver(graphMgr);
        this.compiledConjunctions = new HashMap<>();
    }

    GraphManager graph() {
        return graphMgr;
    }

    public TypeInference typeInference() {
        return typeInference;
    }

    public ExpressionResolver expressionResolver() {
        return expressionResolver;
    }

    public void deleteAndInvalidateRule(Rule rule) {
        rule.delete();
        logicCache.rule().invalidate(rule.getLabel());
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingStatement<?> then) {
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
        return rules().filter(rule -> iterate(rule.condition().branches()).anyMatch(condition -> !condition.conjunction().negations().isEmpty()));
    }

    public Map<Rule, Set<Unifier>> applicableRules(Concludable concludable) {
        return logicCache.unifiers().get(concludable, c -> c.computeApplicableRules(conceptMgr, this));
    }

    public Set<Resolvable<?>> compile(ResolvableConjunction conjunction) {
        if (!compiledConjunctions.containsKey(conjunction)) {
            synchronized (compiledConjunctions) {
                if (!compiledConjunctions.containsKey(conjunction)) {
                    Set<Concludable> concludablesTriggeringRules = iterate(conjunction.positiveConcludables())
                            .filter(concludable -> !applicableRules(concludable).isEmpty())
                            .toSet();
                    Set<Resolvable<?>> resolvables = new HashSet<>();
                    resolvables.addAll(concludablesTriggeringRules);
                    resolvables.addAll(Retrievable.extractFrom(conjunction.pattern(), concludablesTriggeringRules));
                    resolvables.addAll(conjunction.negations());
                    compiledConjunctions.put(conjunction, resolvables);
                }
            }
        }
        return compiledConjunctions.get(conjunction);
    }

    /**
     * On commit we must clear the rule cache and revalidate rules - this will force re-running type inference
     * when we re-load the Rule objects
     * Rule indexes should also be deleted and regenerated as needed
     * Note: does not need to be synchronized as only called by one schema transaction at a time
     */
    public void revalidateAndReindexRules() {
        logicCache.rule().clear();
        logicCache.unifiers().clear();

        if (graphMgr.schema().hasModifiedTypes()) {
            // re-validate all rules are valid
            rules().stream().parallel().forEach(rule -> rule.validate(this, conceptMgr));

            // recreate rule index conclusions
            graphMgr.schema().rules().all().forEachRemaining(s -> fromStructure(s).conclusion().reindex());
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
        return iterate(rule.condition().branches()).flatMap(condition -> condition.conjunction().allConcludables())
                .flatMap(c -> iterate(applicableRules(c).keySet()))
                .map(recursiveRule -> RuleDependency.of(recursiveRule, rule));
    }

    private FunctionalIterator<RuleDependency> negatedRuleDependencies(Rule rule) {
        assert iterate(rule.condition().branches()).flatMap(condition -> iterate(condition.conjunction().negations()))
                .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                .allMatch(conj -> conj.negations().isEmpty()); // Revise when we support nested negations in rules
        return iterate(rule.condition().branches()).flatMap(condition -> iterate(condition.conjunction().negations()))
                .flatMap(neg -> iterate(neg.disjunction().conjunctions()))
                .flatMap(ResolvableConjunction::allConcludables)
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
        rules().stream().sorted(comparing(Rule::getLabel)).forEach(rule -> rule.getSyntax(builder));
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
}
