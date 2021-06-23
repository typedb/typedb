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
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Utils;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Collections.singletonList;


public class Reasoner {

    // TODO: Needs strings as keys rather than rules, only for testing the framework itself
    private final Map<String, RuleRecorder> ruleRecorders;

    public Reasoner() {
        this.ruleRecorders = new HashMap<>();
    }

    public Map<String, RuleRecorder> ruleRecorderMap() {
        return ruleRecorders;
    }

    public void run(RocksSession session) {
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE,
                                                       new Options.Transaction().infer(false))) {
            tx.logic().rules().forEachRemaining(r -> this.ruleRecorders.put(r.getLabel(), new RuleRecorder(r)));
            boolean reiterateRules = true;
            while (reiterateRules) {
                reiterateRules = false;
                for (RuleRecorder rule : this.ruleRecorders.values()) {
                    rule.resetRequiresReiteration();
                    runRule(tx, rule);
                    reiterateRules = reiterateRules || rule.requiresReiteration();
                }
            }
            // Let the transaction close, therefore deleting the materialised concepts. The inferences recorded are
            // held in memory instead.
        }
    }

    private static void runRule(RocksTransaction tx, RuleRecorder ruleRecorder) {
        // Get all the places where the rule condition is satisfied and materialise for each
        tx.reasoner().executeTraversal(
                new Disjunction(singletonList(ruleRecorder.rule().when())),
                new Context.Query(tx.context(), new Options.Query()),
                Utils.filterRetrievableVars(ruleRecorder.rule().when().identifiers())
        ).forEachRemaining(whenConcepts -> ruleRecorder.rule().conclusion()
                .materialise(whenConcepts, tx.traversal(), tx.concepts())
                .map(thenConcepts -> new ConceptMap(filterRetrievableVars(thenConcepts)))
                .filter(ruleRecorder::isInferredAnswer)
                .forEachRemaining(ans -> ruleRecorder.recordInference(whenConcepts, ans)));
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
        private boolean requiresReiteration;
        // Inferences from condition answer to conclusion answer
        private final Map<ConceptMap, ConceptMap> inferencesByCondition;
        private final Map<ConceptMap, ConceptMap> inferencesByConclusion;

        public RuleRecorder(Rule typeDBRule) {
            this.rule = typeDBRule;
            this.requiresReiteration = false;
            this.inferencesByCondition = new HashMap<>();
            this.inferencesByConclusion = new HashMap<>();

            Set<Concludable> concludables = Concludable.create(this.rule.then());
            assert concludables.size() == 1;
            // Use a concludable for the `then` as a convenient way to check if an answer is inferred
            this.thenConcludable = iterate(concludables).next();
        }

        public Map<ConceptMap, ConceptMap> inferencesByCondition() {
            return inferencesByCondition;
        }

        public Map<ConceptMap, ConceptMap> inferencesByConclusion() {
            return inferencesByConclusion;
        }

        public Rule rule() {
            return rule;
        }

        public boolean isInferredAnswer(ConceptMap thenConceptMap) {
            return thenConcludable.isInferredAnswer(thenConceptMap);
        }

        public void recordInference(ConceptMap whenConceptMap, ConceptMap thenConceptMap) {
            if (!inferencesByCondition.containsKey(whenConceptMap)) {
                requiresReiteration = true;
                inferencesByCondition.put(whenConceptMap, thenConceptMap);
                inferencesByConclusion.put(thenConceptMap, whenConceptMap);
            } else {
                assert inferencesByCondition.get(whenConceptMap).equals(thenConceptMap);
            }
        }

        public boolean requiresReiteration() {
            return requiresReiteration;
        }

        public void resetRequiresReiteration() {
            requiresReiteration = false;
        }

    }
}
