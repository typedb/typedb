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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.complete;

import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.TypeQLHelpers;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class Completer {

    private final Session session;
    private Set<Rule> rules;
    private final RuleResolutionBuilder ruleResolutionBuilder = new RuleResolutionBuilder();

    public Completer(Session session) {
        this.session = session;
    }

    public void loadRules(Set<com.vaticle.typedb.core.logic.Rule> typeDBRules) {
        Set<Rule> rules = new HashSet<>();
        for (com.vaticle.typedb.core.logic.Rule typeDBRule : typeDBRules) {
            rules.add(new Rule(typeDBRule.when(), typeDBRule.then(), typeDBRule.getLabel()));
        }
        this.rules = rules;
    }

    public void complete() {
        boolean allRulesRerun = true;

        while (allRulesRerun) {
            allRulesRerun = false;
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                for (Rule rule : rules) {
                    allRulesRerun = allRulesRerun | completeRule(tx, rule);
                }
                tx.commit();
            }
        }
    }

    private boolean completeRule(Transaction tx, Rule rule) {

        AtomicBoolean foundResult = new AtomicBoolean(false);
        // TODO When making match queries be careful that user-provided rules could trigger due to elements of the
        //  completion schema. These results should be filtered out.

        // Use the DNF so that we can know each `when` if free of disjunctions. Disjunctions in the `when` will otherwise complicate things significantly
        Set<Conjunction<Pattern>> disjunctiveWhens = rule.when.normalise().patterns();
        for (Conjunction<Pattern> when : disjunctiveWhens) {
            // Get all the places where the `when` of the rule is satisfied, but the `then` is not
            List<ConceptMap> inferredConcepts = tx.query().match(TypeQL.match(when, TypeQL.not(rule.then)).insert(rule.then.variables()));
            if (inferredConcepts.isEmpty()) {
                continue;
            }

            // We already know that the rule doesn't contain any disjunctions as we previously used negationDNF,
            // now we make sure negation blocks are removed, so that we know it must be a conjunct set of variables
            PatternVisitor.NegationRemovalVisitor negationRemover = new PatternVisitor.NegationRemovalVisitor();
            Pattern ruleResolutionConjunction = negationRemover.visitPattern(ruleResolutionBuilder.ruleResolutionConjunction(tx, rule.when, rule.then, rule.label));

            // Record how the inference was made
            List<ConceptMap> inserted = tx.query().match(TypeQL.match(rule.when, rule.then, TypeQL.not(ruleResolutionConjunction)).insert(ruleResolutionConjunction.variables()));
            assert inserted.size() >= 1;
            foundResult.set(true);
        }
        return foundResult.get();
    }

    private static class Rule {
        private final Pattern when;
        private final Pattern then;
        private String label;

        Rule(Pattern when, Pattern then, String label) {
            PatternVisitor.VariableVisitor visitor = new PatternVisitor.VariableVisitor(TypeQLHelpers::makeAnonVarsExplicit);
            this.when = visitor.visitPattern(when);
            this.then = visitor.visitPattern(then);
            this.label = label;
        }
    }
}
