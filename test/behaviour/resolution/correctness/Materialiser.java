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

package com.vaticle.typedb.core.test.behaviour.resolution.correctness;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Collections.singletonList;

public class Materialiser {

    private final Map<Rule, RuleMaterialisationRecorder> ruleRecorders;
    private final RocksTransaction tx;

    private Materialiser(RocksSession session) {
        this.ruleRecorders = new HashMap<>();
        this.tx = session.transaction(Arguments.Transaction.Type.WRITE, new Options.Transaction().infer(false));
    }

    public static Materialiser materialiseAll(RocksSession session) {
        Materialiser materialiser = new Materialiser(session);
        materialiser.materialiseAll();
        return materialiser;
    }

    private void materialiseAll() {
        tx.logic().rules().forEachRemaining(rule -> this.ruleRecorders.put(rule, new RuleMaterialisationRecorder(rule)));
        boolean reiterateRules = true;
        while (reiterateRules) {
            reiterateRules = false;
            for (RuleMaterialisationRecorder ruleRecorder : this.ruleRecorders.values()) {
                ruleRecorder.requiresReiteration = false;
                materialiseFromRule(ruleRecorder);
                reiterateRules = reiterateRules || ruleRecorder.requiresReiteration;
            }
        }
    }

    private void materialiseFromRule(RuleMaterialisationRecorder ruleRecorder) {
        // Get all the places where the rule condition is satisfied and materialise for each
        traverse(ruleRecorder.rule.when())
                .forEachRemaining(whenConcepts -> ruleRecorder.rule.conclusion()
                .materialise(whenConcepts, tx.traversal(), tx.concepts())
                .map(thenConcepts -> new ConceptMap(filterRetrievableVars(thenConcepts)))
                .filter(ruleRecorder::isInferredAnswer)
                .forEachRemaining(ans -> ruleRecorder.recordMaterialisation(whenConcepts, ans)));
    }

    private static Map<Variable.Retrievable, Concept> filterRetrievableVars(Map<Variable, Concept> concepts) {
        Map<Variable.Retrievable, Concept> newMap = new HashMap<>();
        concepts.forEach((var, concept) -> {
            if (var.isName()) newMap.put(var.asName(), concept);
            else if (var.isAnonymous()) newMap.put(var.asAnonymous(), concept);
        });
        return newMap;
    }

    private FunctionalIterator<ConceptMap> traverse(Conjunction conjunction) {
        return tx.reasoner().executeTraversal(
                new Disjunction(singletonList(conjunction)),
                new Context.Query(tx.context(), new Options.Query()),
                filterRetrievableVars(conjunction.identifiers())
        );
    }

    private static Set<Identifier.Variable.Retrievable> filterRetrievableVars(Set<Identifier.Variable> vars) {
        return iterate(vars).filter(var -> var.isName() || var.isAnonymous()).map(var -> {
            if (var.isName()) return var.asName();
            if (var.isAnonymous()) return var.asAnonymous();
            throw TypeDBException.of(ILLEGAL_STATE);
        }).toSet();
    }

    void close() {
        tx.close();
    }

    public Map<Conjunction, FunctionalIterator<ConceptMap>> query(TypeQLMatch inferenceQuery) {
        // TODO: How do we handle disjunctions inside negations and negations in general?
        Disjunction disjunction = Disjunction.create(inferenceQuery.conjunction().normalise());
        tx.logic().typeResolver().resolve(disjunction);
        HashMap<Conjunction, FunctionalIterator<ConceptMap>> conjunctionAnswers = new HashMap<>();
        disjunction.conjunctions().forEach(conjunction -> conjunctionAnswers.put(conjunction, traverse(conjunction)));
        return conjunctionAnswers;
    }

    FunctionalIterator<Materialisation> explainConjunction(ConceptMap answer,
                                                                  Conjunction conjunction) {
        return iterate(Concludable.create(conjunction)).flatMap(concludable -> iterate(
                concludable.applicableRules(tx.concepts(), tx.logic()).entrySet()
        ).flatMap(applicableRule -> iterate(applicableRule.getValue())
                .map(unifier -> unifier.unify(answer.filter(concludable.retrieves()))
                        .flatMap(unified -> materialisationForConclusion(applicableRule.getKey(),
                                                                         unified.first())))
                .filter(Optional::isPresent).map(Optional::get)
        ));
    }

    private Optional<Materialisation> materialisationForConclusion(Rule rule, ConceptMap conclusionAnswer) {
        if (!ruleRecorders.containsKey(rule)) return Optional.empty();
        if (!ruleRecorders.get(rule).materialisationsByConclusion.containsKey(conclusionAnswer)) return Optional.empty();
        return Optional.of(ruleRecorders.get(rule).materialisationsByConclusion.get(conclusionAnswer));
    }

    Optional<Materialisation> materialisationForCondition(Rule rule, ConceptMap conditionAnswer) {
        if (!ruleRecorders.containsKey(rule)) return Optional.empty();
        if (!ruleRecorders.get(rule).materialisationsByCondition.containsKey(conditionAnswer)) return Optional.empty();
        return Optional.of(ruleRecorders.get(rule).materialisationsByCondition.get(conditionAnswer));
    }

    static class Materialisation {

        private final Rule rule;
        private final ConceptMap conditionAnswer;
        private final ConceptMap conclusionAnswer;

        Materialisation(Rule rule, ConceptMap conditionAnswer, ConceptMap conclusionAnswer) {
            this.rule = rule;
            this.conditionAnswer = conditionAnswer;
            this.conclusionAnswer = conclusionAnswer;
        }

        ConceptMap conditionAnswer() {
            return conditionAnswer;
        }

        ConceptMap conclusionAnswer() {
            return conclusionAnswer;
        }

        Rule rule() {
            return rule;
        }

    }

    private static class RuleMaterialisationRecorder {
        private final Rule rule;
        private final Concludable thenConcludable;
        private boolean requiresReiteration;
        private final Map<ConceptMap, Materialiser.Materialisation> materialisationsByCondition;
        private final Map<ConceptMap, Materialiser.Materialisation> materialisationsByConclusion;

        private RuleMaterialisationRecorder(Rule typeDBRule) {
            this.rule = typeDBRule;
            this.requiresReiteration = false;
            this.materialisationsByCondition = new HashMap<>();
            this.materialisationsByConclusion = new HashMap<>();

            Set<Concludable> concludables = Concludable.create(this.rule.then());
            assert concludables.size() == 1;
            // Use a concludable for the conclusion for the convenience of its isInferredAnswer method
            this.thenConcludable = iterate(concludables).next();
        }

        private boolean isInferredAnswer(ConceptMap thenConceptMap) {
            return thenConcludable.isInferredAnswer(thenConceptMap);
        }

        private void recordMaterialisation(ConceptMap whenConceptMap, ConceptMap thenConceptMap) {
            if (!materialisationsByCondition.containsKey(whenConceptMap)) {
                Materialiser.Materialisation materialisation =
                        new Materialiser.Materialisation(rule, whenConceptMap, thenConceptMap);
                requiresReiteration = true;
                materialisationsByCondition.put(whenConceptMap, materialisation);
                materialisationsByConclusion.put(thenConceptMap, materialisation);
            } else {
                assert materialisationsByCondition.get(whenConceptMap).conclusionAnswer().equals(thenConceptMap);
            }
        }

    }

}
