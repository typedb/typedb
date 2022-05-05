/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMap.Explainable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.BoundPattern.BoundConcludable;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.BoundPattern.BoundConjunction;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier.SoundnessException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class SoundnessVerifier {

    private final ForwardChainingMaterialiser materialiser;
    private final TypeDB.Session session;
    private final Map<Concept, Concept> inferredConceptMapping;
    private final Set<Explanation> collectedExplanations;

    private SoundnessVerifier(ForwardChainingMaterialiser materialiser, TypeDB.Session session) {
        this.materialiser = materialiser;
        this.session = session;
        this.inferredConceptMapping = new HashMap<>();
        this.collectedExplanations = new HashSet<>();
    }

    static SoundnessVerifier create(ForwardChainingMaterialiser materialiser, TypeDB.Session session) {
        return new SoundnessVerifier(materialiser, session);
    }

    void verifyQuery(TypeQLMatch inferenceQuery) {
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ,
                new Options.Transaction().infer(true).explain(true))) {
            collectedExplanations.clear();
            // recursively collects explanations, partially verifies the answer.
            tx.query().match(inferenceQuery).forEachRemaining(ans -> verifyAnswerAndCollectExplanations(inferenceQuery, ans, tx));

            // We can only verify an explanation once all concepts in it's condition have been "mapped"
            // Concepts are mapped when an explanation containing them in the conclusion is verified.
            // Too slow? Create a dependency graph, retry only those from which a newly verified explanation is reachable
            Set<Explanation> unverifiedExplanations = collectedExplanations;
            Set<Explanation> dependentUnverifiedExplanations = new HashSet<>();
            boolean madeProgress = true;
            while (!unverifiedExplanations.isEmpty() && madeProgress) {
                madeProgress = false;
                for (Explanation e : unverifiedExplanations) {
                    if (canExplanationBeVerified(e)) {
                        verifyExplanationAndMapConcepts(e);
                        madeProgress = true;
                    } else {
                        dependentUnverifiedExplanations.add(e);
                    }
                }
                unverifiedExplanations = dependentUnverifiedExplanations;
                dependentUnverifiedExplanations = new HashSet<>();
            }

            if (!unverifiedExplanations.isEmpty()) {
                throw new SoundnessException("SoundnessVerifier could not verify the soundness" +
                        " of all generated explanations");
            }
            collectedExplanations.clear();
        }
    }

    private boolean canExplanationBeVerified(Explanation explanation) {
        return iterate(explanation.conditionAnswer().concepts().values())
                .filter(c -> c.asThing().isInferred() && !inferredConceptMapping.containsKey(c))
                .first().isEmpty();
    }

    private void verifyAnswerAndCollectExplanations(TypeQLMatch inferenceQuery, ConceptMap answer, Transaction tx) {
        verifyExplainableVars(answer);
        answer.explainables().iterator().forEachRemaining(explainable -> {
            List<Explanation> explanations = tx.query().explain(explainable.id()).toList();
            iterate(explanations).forEachRemaining(explanation -> {
                // This check is valid given that there is no mechanism for recursion termination given by the UX of
                // explanations, so we do it ourselves
                if (collectedExplanations.contains(explanation)) return;
                else collectedExplanations.add(explanation);
                verifyAnswerAndCollectExplanations(inferenceQuery, explanation.conditionAnswer(), tx);
                verifyVariableMapping(answer, explainable, explanation);
            });
            verifyNumberOfExplanations(tx, inferenceQuery, answer, explainable, explanations);
        });
    }

    private void verifyNumberOfExplanations(Transaction tx, TypeQLMatch inferenceQuery, ConceptMap answer, Explainable explainable, List<Explanation> explanations) {
        Set<BoundConcludable> boundConcludable = BoundConjunction.create(
                explainable.conjunction(), mapInferredConcepts(answer.filter(explainable.conjunction().retrieves()))
        ).boundConcludables();
        assert boundConcludable.size() == 1;
        boundConcludable.forEach(bc -> {
            bc.concludable().getApplicableRules(tx.concepts(), tx.logic());
            AtomicInteger numExplanationsExpected = new AtomicInteger();
            materialiser.concludableMaterialisations(bc).forEachRemaining(materialisation -> {
                bc.concludable().getUnifiers(materialisation.boundConclusion().conclusion().rule()).forEachRemaining(unifier -> {
                    Optional<Pair<ConceptMap, Unifier.Requirements.Instance>> boundsAndRequirements = unifier.unify(bc.pattern().bounds());
                    assert boundsAndRequirements.isPresent();
                    numExplanationsExpected.getAndAdd(unifier.unUnify(materialisation.boundConclusion().bounds(), boundsAndRequirements.get().second()).toSet().size());
                });
            });
            if (explanations.size() != numExplanationsExpected.get()) {
                throw new SoundnessException(String.format(
                        "Soundness testing found an incorrect number of explanations for query \"%s\" for explainable" +
                                " \"%s\".\nExplanations expected: %s\nExplanations found: %s",
                        inferenceQuery, explainable.conjunction(), numExplanationsExpected, explanations.size()
                ));
            }
        });
    }

    // TODO: Duplicate code from ExplanationTest
    private static void verifyVariableMapping(ConceptMap ans, Explainable explainable, Explanation explanation) {
        assert explanation.variableMapping().keySet().equals(explainable.conjunction().retrieves());

        Map<Retrievable, Set<Identifier.Variable>> mapping = explanation.variableMapping();
        Map<Retrievable, Set<Retrievable>> retrievableMapping = new HashMap<>();
        mapping.forEach((k, v) -> retrievableMapping.put(
                k, iterate(v).filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet()
        ));
        ConceptMap projected = applyMapping(retrievableMapping, ans);
        projected.concepts().forEach((var, concept) -> {
            assert explanation.conclusionAnswer().concepts().containsKey(var);
            assert explanation.conclusionAnswer().concepts().get(var).equals(concept);
        });
    }

    // TODO: Duplicate code from ExplanationTest
    private static ConceptMap applyMapping(Map<Retrievable, Set<Retrievable>> mapping, ConceptMap completeMap) {
        Map<Retrievable, Concept> concepts = new HashMap<>();
        mapping.forEach((from, tos) -> {
            assert completeMap.contains(from);
            Concept concept = completeMap.get(from);
            tos.forEach(mapped -> {
                assert !concepts.containsKey(mapped) || concepts.get(mapped).equals(concept);
                concepts.put(mapped, concept);
            });
        });
        return new ConceptMap(concepts);
    }

    // TODO: Duplicate code from ExplanationTest
    private static void verifyExplainableVars(ConceptMap ans) {
        ans.explainables().relations().keySet().forEach(v -> {
            assert ans.contains(v);
        });
        ans.explainables().attributes().keySet().forEach(v -> {
            assert ans.contains(v);
        });
        ans.explainables().ownerships().keySet().forEach(
                pair -> {
                    assert ans.contains(pair.first()) && ans.contains(pair.second());
                });
    }

    private void verifyExplanationAndMapConcepts(Explanation explanation) {
        ConceptMap recordedWhen = mapInferredConcepts(explanation.conditionAnswer());
        Optional<ConceptMap> recordedThen = materialiser
                .conditionMaterialisations(explanation.rule(), recordedWhen)
                .map(materialisation -> materialisation.boundConclusion().pattern().bounds());
        if (recordedThen.isPresent()) {
            // Update the inferred variables mapping between the two reasoners
            assert recordedThen.get().concepts().keySet().equals(
                    iterate(explanation.conclusionAnswer().concepts().keySet())
                            .filter(Identifier::isRetrievable)
                            .map(Identifier.Variable::asRetrievable).toSet()
            );  // TODO: use unfiltered set of variables
            recordedThen.get().concepts().forEach((var, recordedConcept) -> {
                Concept inferredConcept = explanation.conclusionAnswer().concepts().get(var);
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

    private ConceptMap mapInferredConcepts(ConceptMap inferredAnswer) {
        Map<Retrievable, Concept> substituted = new HashMap<>();
        inferredAnswer.concepts().forEach((var, concept) -> {
            if (inferredConceptMapping.containsKey(concept)) {
                substituted.put(var, inferredConceptMapping.get(concept));
            } else {
                assert !concept.asThing().isInferred();
                substituted.put(var, concept);
            }
        });
        return new ConceptMap(substituted);
    }

}
