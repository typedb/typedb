package com.vaticle.typedb.core.test.behaviour.resolution.framework.soundness;

import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.HashMap;
import java.util.Map;

public class SoundnessChecker {

    private final Reasoner referenceReasoner;
    private final Transaction tx;
    private final Map<Concept, Concept> inferredConceptMapping;

    private SoundnessChecker(Reasoner referenceReasoner, Transaction tx) {
        this.referenceReasoner = referenceReasoner;
        this.tx = tx;
        this.inferredConceptMapping = new HashMap<>();
    }

    public static SoundnessChecker create(Reasoner referenceReasoner, Transaction tx) {
        return new SoundnessChecker(referenceReasoner, tx);
    }

    public void check(TypeQLMatch inferenceQuery) {
        tx.query().match(inferenceQuery).forEachRemaining(this::checkAnswer);
    }

    private void checkAnswer(ConceptMap answer) {
        answer.explainables().iterator().forEachRemaining(explainable -> {
            tx.query().explain(explainable.id()).forEachRemaining(explanation -> {
                checkAnswer(explanation.conditionAnswer());
                checkExplanation(explanation);
            });
        });
    }

    private void checkExplanation(Explanation explanation) {
        Reasoner.RuleRecorder recorder = referenceReasoner.ruleRecorderMap().get(explanation.rule());
        ConceptMap recordedWhen = substituteInferredVarsForReferenceVars(explanation.conditionAnswer());
        if (recorder.whenToThenBindings().containsKey(recordedWhen)) {
            // Update the inferred variables mapping between the two reasoners
            ConceptMap recordedThen = recorder.whenToThenBindings().get(recordedWhen);
            assert recordedThen.concepts().keySet().equals(explanation.conclusionAnswer().concepts());
            recordedThen.concepts().forEach((var, recordedConcept) -> {
                Concept inferredConcept = explanation.conclusionAnswer().concepts().get(var);
                if (inferredConceptMapping.containsKey(inferredConcept)) {
                    // Check that the mapping stored is one-to-one
                    assert inferredConceptMapping.get(inferredConcept).equals(recordedConcept);
                } else {
                    inferredConceptMapping.put(inferredConcept, recordedConcept);
                }
            });
        } else {
            // We have detected an answer in the explanations that shouldn't be there!
            throw new SoundnessException(String.format("Found an answer in the explanation given that should not be " +
                                                               "present for rule %s", explanation.rule().getLabel()));
        }
    }

    private ConceptMap substituteInferredVarsForReferenceVars(ConceptMap conditionAnswer) {
        Map<Retrievable, Concept> substituted = new HashMap<>();
        conditionAnswer.concepts().forEach((var, concept) -> {
            substituted.put(var, inferredConceptMapping.getOrDefault(concept, concept));
        });
        return new ConceptMap(substituted);
    }

    public static class SoundnessException extends RuntimeException {
        public SoundnessException(String message) {
            super(message);
        }
    }

}
