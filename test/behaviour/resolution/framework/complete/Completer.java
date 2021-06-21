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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;


public class Completer {

    private final RocksSession session;
    private final Set<CompletionRule> rules;

    public Completer(RocksSession session) {
        this.session = session;
        this.rules = new HashSet<>();
    }

    public void loadRules(Set<com.vaticle.typedb.core.logic.Rule> typeDBRules) {
        for (com.vaticle.typedb.core.logic.Rule typeDBRule : typeDBRules) {
            rules.add(new CompletionRule(typeDBRule));
        }
    }

    public void complete() {
        boolean allRulesRerun = true;
        while (allRulesRerun) {
            allRulesRerun = false;
            try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                for (CompletionRule rule : rules) {
                    rule.resetRequiresReiteration();
                    runRule(tx, rule);
                    allRulesRerun = allRulesRerun || rule.requiresReiteration();
                }
            }
        }
    }

    private static void runRule(RocksTransaction tx, CompletionRule completionRule) {
        // Get all the places where the `when` of the rule is satisfied and materialise for each
        tx.reasoner().executeTraversal(
                new Disjunction(Collections.singletonList(completionRule.rule().when())),
                new Context.Query(tx.context(), new Options.Query().infer(false)),
                filterRetrievableVars(completionRule.rule().when().identifiers()))
                .forEachRemaining(whenConcepts -> completionRule.rule().conclusion()
                        .materialise(whenConcepts, tx.traversal(), tx.concepts())
                        .map(thenConcepts -> new ConceptMap(filterRetrievableVars(thenConcepts)))
                        .filter(thenConceptMap -> completionRule.thenConcludable().isInferredAnswer(thenConceptMap))
                        .forEachRemaining(ans -> completionRule.addLineages(whenConcepts, ans)));
    }

    private static Set<Variable.Retrievable> filterRetrievableVars(Set<Variable> vars) {
        return iterate(vars).filter(var -> var.isName() || var.isAnonymous()).map(var -> {
            if (var.isName()) return var.asName();
            if (var.isAnonymous()) return var.asAnonymous();
            throw TypeDBException.of(ILLEGAL_STATE);
        }).toSet();
    }

    private static Map<Variable.Retrievable, Concept> filterRetrievableVars(Map<Variable, Concept> concepts) {
        Map<Variable.Retrievable, Concept> newMap = new HashMap<>();
        concepts.forEach((var, concept) -> {
            if (var.isName()) newMap.put(var.asName(), concept);
            else if (var.isAnonymous()) newMap.put(var.asAnonymous(), concept);
        });
        return newMap;
    }

    private static class CompletionRule {
        private final Rule rule;
        private final Concludable thenConcludable;
        private final Set<Lineage> lineages;
        private boolean requiresReiteration;

        public CompletionRule(Rule typeDBRule) {
            this.rule = typeDBRule;
            this.lineages = new HashSet<>();
            this.requiresReiteration = false;

            Set<Concludable> concludables = Concludable.create(this.rule.then());
            assert concludables.size() == 1;
            FunctionalIterator<Concludable> iterator = iterate(concludables);
            this.thenConcludable = iterator.next();
            iterator.recycle();
        }

        public Rule rule() {
            return rule;
        }

        public Concludable thenConcludable() {
            return thenConcludable;
        }

        public void addLineages(ConceptMap whenConcepts, ConceptMap thenConcepts) {
            Lineage newLineage = new Lineage(whenConcepts, thenConcepts);
            if (!lineages.contains(newLineage)) {
                // TODO: New lineage found, trigger another iteration of the rules
                this.requiresReiteration = true;
                lineages.add(newLineage);
            }
        }

        public boolean requiresReiteration() {
            return requiresReiteration;
        }

        public void resetRequiresReiteration() {
            requiresReiteration = false;
        }

        private static class Lineage {
            private final ConceptMap whenConceptMap;
            private final ConceptMap thenConceptMap;

            Lineage(ConceptMap whenConceptMap, ConceptMap thenConceptMap) {
                this.whenConceptMap = whenConceptMap;
                this.thenConceptMap = thenConceptMap;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Lineage lineage = (Lineage) o;
                return whenConceptMap.equals(lineage.whenConceptMap) &&
                        thenConceptMap.equals(lineage.thenConceptMap);
            }

            @Override
            public int hashCode() {
                return Objects.hash(whenConceptMap, thenConceptMap);
            }
        }
    }
}
