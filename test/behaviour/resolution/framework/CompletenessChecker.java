package com.vaticle.typedb.core.test.behaviour.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.CorrectnessChecker.CompletenessException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

class CompletenessChecker {

    private final Materialiser referenceReasoner;
    private final RocksSession session;

    private CompletenessChecker(Materialiser referenceReasoner, RocksSession session) {
        this.referenceReasoner = referenceReasoner;
        this.session = session;
    }

    static CompletenessChecker create(Materialiser referenceReasoner, RocksSession session) {
        return new CompletenessChecker(referenceReasoner, session);
    }

    void checkQuery(TypeQLMatch inferenceQuery) {
        referenceReasoner.query(inferenceQuery).forEach((conjunction, answers) -> {
            answers.forEachRemaining(answer -> checkConjunction(conjunction, answer));
        });
    }

    private void checkConjunction(Conjunction inferred, ConceptMap answer) {
        referenceReasoner.explainConjunction(answer, inferred).forEachRemaining(materialisation -> {
            checkConjunction(materialisation.rule().when(), materialisation.conditionAnswer());
            checkConclusion(materialisation.rule().conclusion(), materialisation.conclusionAnswer());
        });
    }

    private void checkConclusion(Rule.Conclusion conclusion, ConceptMap conclusionAnswer) {
        ConceptMap conclusionBounds = removeGeneratingBound(conclusion, conclusionAnswer);
        validateNonInferred(conclusionBounds);
        Conjunction conclusionWithIIDs = constrainByIIDs(conclusion, conclusionAnswer);
        validateInference(conclusion, conclusionWithIIDs);
    }

    private void validateInference(Rule.Conclusion conclusion, Conjunction conclusionWithIIDs) {
        // Validate that this conclusion can be reached using the production reasoner
        Disjunction concludableQuery = new Disjunction(Collections.singletonList(conclusionWithIIDs));
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                       new Options.Transaction().infer(true))) {
            int numAnswers = tx.reasoner().executeReasoner(
                    concludableQuery,
                    conclusion.retrievableIds(),
                    new Context.Query(tx.context(), new Options.Query())
            ).toList().size();
            if (numAnswers == 0) {
                throw new CompletenessException(String.format("Completeness testing found an answer which is expected" +
                                                                      " but is not present.\nExpected exactly one " +
                                                                      "answer for the query:\n%s",
                                                              concludableQuery.toString()));
            } else if (numAnswers > 1) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }

    private static Conjunction constrainByIIDs(Rule.Conclusion conclusion, ConceptMap conclusionBounds) {
        Conjunction constrainedByIIDs = conclusion.conjunction().clone();
        conclusionBounds.concepts().forEach((identifier, concept) -> {
            if (concept.isThing()) {
                assert constrainedByIIDs.variable(identifier).isThing();
                // Make sure this isn't the generating variable, we want to leave that unconstrained.
                constrainedByIIDs.variable(identifier).asThing().iid(concept.asThing().getIID());
            }
        });
        return constrainedByIIDs;
    }

    private static ConceptMap removeGeneratingBound(Rule.Conclusion conclusion, ConceptMap conclusionBounds) {
        // Remove the bound for the variable that the conclusion may generate
        Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(conclusion.retrievableIds());
        if (conclusion.generating().isPresent()) nonGenerating.remove(conclusion.generating().get().id());
        return conclusionBounds.filter(nonGenerating);
    }

    private static void validateNonInferred(ConceptMap conclusionAnswer) {
        for (Concept concept : conclusionAnswer.concepts().values()) {
            if (concept.isThing() && concept.asThing().isInferred()) {
                throw new UnsupportedOperationException("Completeness testing does not yet support non-generated " +
                                                                "inferred concepts in rule conclusions.");
            }
        }
    }

}
