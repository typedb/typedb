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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.reference;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Collections.singletonList;


public class Reasoner {

    private final Map<Rule, RuleRecorder> ruleRecorders;

    public Reasoner() {
        this.ruleRecorders = new HashMap<>();
    }

    public Map<Rule, RuleRecorder> ruleRecorderMap() {
        return ruleRecorders;
    }

    public void run(RocksSession session) {
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            tx.logic().rules().forEachRemaining(r -> this.ruleRecorders.put(r, new RuleRecorder(r)));
            boolean reiterateRules = true;
            while (reiterateRules) {
                reiterateRules = false;
                for (RuleRecorder rule : this.ruleRecorders.values()) {
                    rule.resetRequiresReiteration();
                    runRule(tx, rule);
                    reiterateRules = reiterateRules || rule.requiresReiteration();
                }
            }
            // Let the transaction close, therefore deleting the materialised concepts
        }
    }

    private static void runRule(RocksTransaction tx, RuleRecorder ruleRecorder) {
        // Get all the places where the `when` of the rule is satisfied and materialise for each
        tx.reasoner().executeTraversal(
                new Disjunction(singletonList(ruleRecorder.rule().when())),
                new Context.Query(tx.context(), new Options.Query().infer(false)),
                filterRetrievableVars(ruleRecorder.rule().when().identifiers()))
                .forEachRemaining(whenConcepts -> ruleRecorder.rule().conclusion()
                        .materialise(whenConcepts, tx.traversal(), tx.concepts())
                        .map(thenConcepts -> new ConceptMap(filterRetrievableVars(thenConcepts)))
                        .filter(ruleRecorder::isInferredAnswer)
                        .forEachRemaining(ans -> ruleRecorder.recordApplication(whenConcepts, ans)));
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

    public static class RuleRecorder {
        private final Rule rule;
        private final Concludable thenConcludable;
        private final Set<RuleApplication> ruleApplications;  // TODO: Can we remove this and just use a map from when to then?
        private boolean requiresReiteration;
        private final Map<ConceptMap, ConceptMap> whenToThenBindings;

        public RuleRecorder(Rule typeDBRule) {
            this.rule = typeDBRule;
            this.ruleApplications = new HashSet<>();
            this.requiresReiteration = false;
            this.whenToThenBindings = new HashMap<>();

            Set<Concludable> concludables = Concludable.create(this.rule.then());
            assert concludables.size() == 1;
            FunctionalIterator<Concludable> iterator = iterate(concludables);
            // Use a concludable for the `then` as a convenient way to check if an answer is inferred
            this.thenConcludable = iterator.next();
            iterator.recycle();
        }

        public Map<ConceptMap, ConceptMap> whenToThenBindings() {
            return whenToThenBindings;
        }

        public Rule rule() {
            return rule;
        }

        public boolean isInferredAnswer(ConceptMap thenConceptMap) {
            return thenConcludable.isInferredAnswer(thenConceptMap);
        }

        public void recordApplication(ConceptMap whenConceptMap, ConceptMap thenConceptMap) {
            RuleApplication newApplication = new RuleApplication(whenConceptMap, thenConceptMap);
            if (!ruleApplications.contains(newApplication)) {
                this.requiresReiteration = true;
                ruleApplications.add(newApplication);
                whenToThenBindings.put(whenConceptMap, thenConceptMap);
            }
        }

        public boolean requiresReiteration() {
            return requiresReiteration;
        }

        public void resetRequiresReiteration() {
            requiresReiteration = false;
        }

        private static class RuleApplication {
            private final ConceptMap whenConceptMap;
            private final ConceptMap thenConceptMap;

            RuleApplication(ConceptMap whenConceptMap, ConceptMap thenConceptMap) {
                this.whenConceptMap = whenConceptMap;
                this.thenConceptMap = thenConceptMap;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                RuleApplication ruleApplication = (RuleApplication) o;
                return whenConceptMap.equals(ruleApplication.whenConceptMap) &&
                        thenConceptMap.equals(ruleApplication.thenConceptMap);
            }

            @Override
            public int hashCode() {
                return Objects.hash(whenConceptMap, thenConceptMap);
            }
        }
    }
}
