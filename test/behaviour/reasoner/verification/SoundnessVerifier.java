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

package com.vaticle.typedb.core.test.behaviour.reasoner.verification;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier.SoundnessException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class SoundnessVerifier {

    private final Materialiser materialiser;
    private final TypeDB.Session session;
    private final Map<Concept, Concept> inferredConceptMapping;
    private final Set<Explanation> verifiedExplanations;

    private SoundnessVerifier(Materialiser materialiser, TypeDB.Session session) {
        this.materialiser = materialiser;
        this.session = session;
        this.inferredConceptMapping = new HashMap<>();
        this.verifiedExplanations = new HashSet<>();
    }

    static SoundnessVerifier create(Materialiser materialiser, TypeDB.Session session) {
        return new SoundnessVerifier(materialiser, session);
    }

    void verifyQuery(TypeQLMatch inferenceQuery) {
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                  new Options.Transaction().infer(true).explain(true))) {
            tx.query().match(inferenceQuery).forEachRemaining(ans -> verifyAnswer(ans, tx));
        }
    }

    private void verifyAnswer(ConceptMap answer, Transaction tx) {
        answer.explainables().iterator().forEachRemaining(explainable -> {
            tx.query().explain(explainable.id()).forEachRemaining(explanation -> {
                // This check is valid given that there is no mechanism for recursion termination given by the UX of
                // explanations, so we do it ourselves
                if (verifiedExplanations.contains(explanation)) return;
                else verifiedExplanations.add(explanation);
                verifyAnswer(explanation.conditionAnswer(), tx);
                verifyExplanation(explanation);
            });
        });
    }

    private void verifyExplanation(Explanation explanation) {
        ConceptMap recordedWhen = mapInferredConcepts(explanation.conditionAnswer());
        Optional<ConceptMap> recordedThen = materialiser
                .conditionMaterialisations(explanation.rule(), recordedWhen)
                .map(materialisation -> materialisation.boundConclusion().pattern().bounds());
        if (recordedThen.isPresent()) {
            // Update the inferred variables mapping between the two reasoners
            assert recordedThen.get().concepts().keySet().equals(
                    iterate(explanation.conclusionAnswer().keySet())
                            .filter(Identifier::isRetrievable)
                            .map(Identifier.Variable::asRetrievable).toSet()
            );  // TODO: use unfiltered set of variables
            recordedThen.get().concepts().forEach((var, recordedConcept) -> {
                Concept inferredConcept = explanation.conclusionAnswer().get(var);
                if (inferredConceptMapping.containsKey(inferredConcept)) {
                    // Check that the mapping stored is one-to-one
                    assert inferredConceptMapping.get(inferredConcept).equals(recordedConcept);
                } else {
                    inferredConceptMapping.put(inferredConcept, recordedConcept);
                }
            });
        } else {
            throw new SoundnessException(String.format("Soundness testing found an answer within an explanation that " +
                                                               "should not be present for rule \"%s\"" +
                                                               ".\nAnswer:\n%s\nIncorrectly derived from " +
                                                               "condition:\n%s",
                                                       explanation.rule().getLabel(), explanation.conclusionAnswer(),
                                                       explanation.conditionAnswer()));
        }
    }

    private ConceptMap mapInferredConcepts(ConceptMap conditionAnswer) {
        Map<Retrievable, Concept> substituted = new HashMap<>();
        conditionAnswer.concepts().forEach((var, concept) -> {
            substituted.put(var, inferredConceptMapping.getOrDefault(concept, concept));
        });
        return new ConceptMap(substituted);
    }

}
