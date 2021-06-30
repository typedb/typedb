package com.vaticle.typedb.core.test.behaviour.resolution.correctness;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.correctness.CorrectnessChecker.CompletenessException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class CompletenessChecker {

    private final Materialiser materialiser;
    private final RocksSession session;
    private final Set<Pair<Conjunction, ConceptMap>> checked;

    private CompletenessChecker(Materialiser materialiser, RocksSession session) {
        this.materialiser = materialiser;
        this.session = session;
        this.checked = new HashSet<>();
    }

    static CompletenessChecker create(Materialiser materialiser, RocksSession session) {
        return new CompletenessChecker(materialiser, session);
    }

    void checkQuery(TypeQLMatch inferenceQuery) {
        materialiser.query(inferenceQuery).forEach((conjunction, answers) -> {
            answers.forEachRemaining(answer -> checkConjunction(conjunction, answer));
        });
    }

    private void checkConjunction(Conjunction inferred, ConceptMap answer) {
        iterate(Concludable.create(inferred)).forEachRemaining(concludable -> {
            if (concludable.isInferredAnswer(answer)) {
                materialiser.materialisationsForConcludable(answer, concludable).forEachRemaining(materialisation -> {
                    Pair<Conjunction, ConceptMap> check = new Pair<>(materialisation.rule().when(),
                                                                     materialisation.conditionAnswer());
                    // Materialisations record all possible paths that could be taken to infer a fact, so can contain
                    // cycles. When we detect we have explored the same state before, stop.
                    if (checked.contains(check)) return;
                    else checked.add(check);
                    checkConjunction(materialisation.rule().when(), materialisation.conditionAnswer());
                    verifyConclusionReasoning(materialisation.rule().conclusion(), materialisation.conclusionAnswer());
                });
                verifyConcludableReasoning(concludable, answer);
            }
        });
    }

    private void verifyConcludableReasoning(Concludable concludable, ConceptMap concludableAnswer) {
        ConceptMap concludableBounds = removeInferredBound(concludable, concludableAnswer);
        validateNonInferred(concludableBounds, concludable.pattern());
        Conjunction concludableWithIIDs = constrainByIIDs(concludable.pattern(), concludableBounds);
        validateNumConcludableAnswers(concludableWithIIDs, concludable);
    }

    private void verifyConclusionReasoning(Rule.Conclusion conclusion, ConceptMap conclusionAnswer) {
        ConceptMap conclusionBounds = removeInferredBound(conclusion, conclusionAnswer);
        validateNonInferred(conclusionBounds, conclusion.conjunction());
        Conjunction conclusionWithIIDs = constrainByIIDs(conclusion.conjunction(), conclusionBounds);
        validateNumConclusionAnswers(conclusionWithIIDs, conclusion);
    }

    private void validateNumConclusionAnswers(Conjunction IIDBoundConjunction, Rule.Conclusion conclusion) {
        int numAnswers = numReasonedAnswers(IIDBoundConjunction, conclusion.retrievableIds());
        if (numAnswers == 0) {
            throw new CompletenessException(String.format("Completeness testing found a missing answer.\nExpected " +
                                                                  "exactly one answer for the rule conclusion (bound " +
                                                                  "with IIDs):\n%s\n for rule \"%s\"",
                                                          IIDBoundConjunction.toString(), conclusion.rule().getLabel()));
        } else if (numAnswers > 1) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private void validateNumConcludableAnswers(Conjunction IIDBoundConjunction, Concludable concludable) {
        if (numReasonedAnswers(IIDBoundConjunction, concludable.retrieves()) == 0) {
            throw new CompletenessException(String.format("Completeness testing found a missing answer.\nExpected " +
                                                                  "one or more answers for the concludable (bound " +
                                                                  "with IIDs):\n%s\noriginal:\n%s",
                                                          IIDBoundConjunction.toString(),
                                                          concludable.pattern().toString()));
        }
    }

    private int numReasonedAnswers(Conjunction IIDBoundConjunction, Set<Identifier.Variable.Retrievable> filter) {
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                       new Options.Transaction().infer(true))) {
            return tx.reasoner().executeReasoner(
                    new Disjunction(Collections.singletonList(IIDBoundConjunction)),
                    filter, new Context.Query(tx.context(), new Options.Query())).toList().size();
        }
    }

    private static Conjunction constrainByIIDs(Conjunction conjunction, ConceptMap conclusionBounds) {
        Conjunction constrainedByIIDs = conjunction.clone();
        conclusionBounds.concepts().forEach((identifier, concept) -> {
            if (concept.isThing()) {
                assert constrainedByIIDs.variable(identifier).isThing();
                // Make sure this isn't the generating variable, we want to leave that unconstrained.
                constrainedByIIDs.variable(identifier).asThing().iid(concept.asThing().getIID());
            }
        });
        return constrainedByIIDs;
    }

    private static ConceptMap removeInferredBound(Rule.Conclusion conclusion, ConceptMap conclusionBounds) {
        // Remove the bound for the variable that the conclusion may generate
        Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(conclusion.retrievableIds());
        if (conclusion.generating().isPresent()) nonGenerating.remove(conclusion.generating().get().id());
        return conclusionBounds.filter(nonGenerating);
    }

    private static ConceptMap removeInferredBound(Resolvable<?> concludable, ConceptMap concludableBounds) {
        // Remove the bound for the variable that the conclusion may generate
        Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(concludable.retrieves());
        if (concludable.generating().isPresent()) nonGenerating.remove(concludable.generating().get().id());
        return concludableBounds.filter(nonGenerating);
    }

    private static void validateNonInferred(ConceptMap answer, Conjunction conjunction) {
        for (Concept concept : answer.concepts().values()) {
            if (concept.isThing() && concept.asThing().isInferred()) {
                throw new UnsupportedOperationException(
                        String.format("Completeness testing does not yet support more than one inferred concept in a " +
                                              "query tested against the reasoner. It becomes too computationally " +
                                              "expensive to verify the history of all inferences. Encountered when " +
                                              "querying:\n%s", conjunction));
            }
        }
    }

}
