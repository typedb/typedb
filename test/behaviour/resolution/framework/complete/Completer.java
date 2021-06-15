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
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.VarNameGenerator;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class Completer {

    private final Session session;
    private Set<Rule> rules;
    private final RuleResolutionBuilder ruleResolutionBuilder;

    public Completer(Session session) {
        this.session = session;
        this.ruleResolutionBuilder = new RuleResolutionBuilder();
    }

    public void loadRules(Set<com.vaticle.typedb.core.logic.Rule> typeDBRules) {
        Set<Rule> rules = new HashSet<>();
        for (com.vaticle.typedb.core.logic.Rule typeDBRule : typeDBRules) {
            rules.add(new Rule(typeDBRule.getWhenPreNormalised(), typeDBRule.getThenPreNormalised(), typeDBRule.getLabel()));
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

        // Get all the places where the `when` of the rule is satisfied, but the `then` is not
        List<ConceptMap> inferredConcepts = tx.query().insert(TypeQL.match(rule.when, TypeQL.not(rule.then)).insert(rule.then)).toList();
        Conjunction<ThingVariable<?>> ruleResolutionConjunction = ruleResolutionBuilder.ruleResolutionConjunction(rule.when, rule.then, rule.label);

        // Record how the inference was made
        // TODO: This looks incorrect - it could add resolution between inserted facts not inferred ones. This can be
        //  fixed by adding the inferred concepts into the match by iid. Or possibly by changing the initial
        //  insertion of the inferred concepts to include the derivation.
        List<ConceptMap> inserted = tx.query().insert(TypeQL.match(
                rule.when, rule.then, TypeQL.not(ruleResolutionConjunction)
        ).insert(ruleResolutionConjunction.patterns())).toList();
        assert inserted.size() >= 1;
        foundResult.set(true);

        return foundResult.get();
    }

    private static class Rule {
        private final Conjunction<Conjunctable> when;
        private final ThingVariable<?> then;
        private final String label;

        public Rule(Conjunction<? extends Pattern> whenPreNormalised, ThingVariable<?> thenPreNormalised, String label) {
            PatternVisitor.VariableVisitor visitor = new PatternVisitor.VariableVisitor(new VarNameGenerator().deanonymiseIfAnon());
            List<Conjunction<Conjunctable>> whenConjunctions = whenPreNormalised.normalise().patterns();
            assert whenConjunctions.size() == 1;
            this.when = visitor.visitConjunction(whenConjunctions.get(0));
            this.then = visitor.visitVariable(thenPreNormalised.normalise()).asThing();
            this.label = label;
        }
    }
}
