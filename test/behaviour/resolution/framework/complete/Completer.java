/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.test.behaviour.resolution.framework.complete;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.common.GraqlHelpers;
import grakn.core.test.behaviour.resolution.framework.common.NegationRemovalVisitor;
import grakn.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import grakn.core.test.behaviour.resolution.framework.common.StatementVisitor;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class Completer {

    private int numInferredConcepts;
    private final Session session;
    private Set<Rule> rules;
    private RuleResolutionBuilder ruleResolutionBuilder = new RuleResolutionBuilder();

    public Completer(Session session) {
        this.session = session;
    }

    public void loadRules(Set<grakn.core.kb.concept.api.Rule> graknRules) {
        Set<Rule> rules = new HashSet<>();
        for (grakn.core.kb.concept.api.Rule graknRule : graknRules) {
            rules.add(new Rule(Objects.requireNonNull(graknRule.when()), Objects.requireNonNull(graknRule.then()), graknRule.label().toString()));
        }
        this.rules = rules;
    }

    public int complete() {
        boolean allRulesRerun = true;

        while (allRulesRerun) {
            allRulesRerun = false;
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                for (Rule rule : rules) {
                    allRulesRerun = allRulesRerun | completeRule(tx, rule);
                }
                tx.commit();
            }
        }
        return numInferredConcepts;
    }

    private boolean completeRule(Transaction tx, Rule rule) {

        AtomicBoolean foundResult = new AtomicBoolean(false);
        // TODO When making match queries be careful that user-provided rules could trigger due to elements of the
        //  completion schema. These results should be filtered out.

        // Use the DNF so that we can know each `when` if free of disjunctions. Disjunctions in the `when` will otherwise complicate things significantly
        Set<Conjunction<Pattern>> disjunctiveWhens = rule.when.getNegationDNF().getPatterns();
        for (Conjunction<Pattern> when : disjunctiveWhens) {
            // Get all the places where the `when` of the rule is satisfied, but the `then` is not
            List<ConceptMap> inferredConcepts = tx.execute(Graql.match(when, Graql.not(rule.then)).insert(rule.then.statements()));
            if (inferredConcepts.isEmpty()) {
                continue;
            }

            // We already know that the rule doesn't contain any disjunctions as we previously used negationDNF,
            // now we make sure negation blocks are removed, so that we know it must be a conjunct set of statements
            NegationRemovalVisitor negationRemover = new NegationRemovalVisitor();
            Pattern ruleResolutionConjunction = negationRemover.visitPattern(ruleResolutionBuilder.ruleResolutionConjunction(tx, rule.when, rule.then, rule.label));

            numInferredConcepts += inferredConcepts.size();

            // Record how the inference was made
            List<ConceptMap> inserted = tx.execute(Graql.match(rule.when, rule.then, Graql.not(ruleResolutionConjunction)).insert(ruleResolutionConjunction.statements()));
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
            StatementVisitor visitor = new StatementVisitor(GraqlHelpers::makeAnonVarsExplicit);
            this.when = visitor.visitPattern(when);
            this.then = visitor.visitPattern(then);
            this.label = label;
        }
    }
}
