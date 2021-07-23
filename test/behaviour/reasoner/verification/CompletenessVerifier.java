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

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.BoundPattern.BoundCondition;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.BoundPattern.BoundConjunction;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier.CompletenessException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class CompletenessVerifier {

    private final Materialiser materialiser;
    private final RocksSession session;
    private final Set<BoundCondition> verifiedConditions;

    private CompletenessVerifier(Materialiser materialiser, RocksSession session) {
        this.materialiser = materialiser;
        this.session = session;
        this.verifiedConditions = new HashSet<>();
    }

    static CompletenessVerifier create(Materialiser materialiser, RocksSession session) {
        return new CompletenessVerifier(materialiser, session);
    }

    void verifyQuery(TypeQLMatch inferenceQuery) {
        materialiser.query(inferenceQuery).forEach((conjunction, answers) -> {
            answers.forEachRemaining(answer -> verifyConjunction(BoundConjunction.create(conjunction, answer)));
        });
    }

    private void verifyConjunction(BoundConjunction boundConjunction) {
        iterate(boundConjunction.boundConcludables()).forEachRemaining(boundConcludable -> {
            if (boundConcludable.isInferredAnswer()) {
                materialiser.concludableMaterialisations(boundConcludable).forEachRemaining(materialisation -> {
                    // Materialisations record all possible paths that could be taken to infer a fact, so can contain
                    // cycles. When we detect we have explored the same state before, stop.
                    if (verifiedConditions.contains(materialisation.boundCondition())) return;
                    else verifiedConditions.add(materialisation.boundCondition());
                    verifyConjunction(materialisation.boundCondition().conjunction());
                    verifyConclusionReasoning(materialisation.boundConclusion());
                });
                verifyConcludableReasoning(boundConcludable);
            }
        });
    }

    private void verifyConcludableReasoning(BoundPattern.BoundConcludable boundConcludable) {
        BoundPattern.BoundConcludable boundNonInferred = boundConcludable.removeInferredBound();
        validateNonInferred(boundNonInferred.pattern());
        validateNumConcludableAnswers(boundNonInferred.pattern(), boundConcludable.concludable());
    }

    private void verifyConclusionReasoning(BoundPattern.BoundConclusion boundConclusion) {
        BoundPattern.BoundConclusion boundNonInferred = boundConclusion.removeInferredBound();
        validateNonInferred(boundNonInferred.pattern());
        validateNumConclusionAnswers(boundNonInferred.pattern(), boundConclusion.conclusion());
    }

    private static void validateNonInferred(BoundConjunction boundConjunction) {
        for (Concept concept : boundConjunction.bounds().concepts().values()) {
            if (concept.isThing() && concept.asThing().isInferred()) {
                throw new UnsupportedOperationException(
                        String.format("Completeness testing does not yet support more than one inferred concept " +
                                              "in a query tested against the reasoner. It becomes too " +
                                              "computationally expensive to verify the history of all inferences." +
                                              " Encountered when querying:\n%s", boundConjunction.conjunction()));
            }
        }
    }

    private void validateNumConcludableAnswers(BoundConjunction boundConjunction, Concludable concludable) {
        if (numReasonedAnswers(boundConjunction, concludable.retrieves()) == 0) {
            throw new CompletenessException(String.format("Completeness testing found a missing answer.\nExpected " +
                                                                  "one or more answers for the concludable (bound " +
                                                                  "with IIDs):\n%s\noriginal:\n%s",
                                                          boundConjunction.toString(),
                                                          concludable.pattern().toString()));
        }
    }

    private void validateNumConclusionAnswers(BoundConjunction boundConjunction, Rule.Conclusion conclusion) {
        int numAnswers = numReasonedAnswers(boundConjunction, conclusion.retrievableIds());
        if (numAnswers == 0) {
            throw new CompletenessException(String.format("Completeness testing found a missing answer.\nExpected " +
                                                                  "exactly one answer for the rule conclusion (bound " +
                                                                  "with IIDs):\n%s\n for rule \"%s\"",
                                                          boundConjunction.toString(), conclusion.rule().getLabel()));
        } else if (numAnswers > 1) {
            // TODO: We would like to differentiate between the valid case vs failure case. We can do so by analysing
            //  whether the answers are relations with the exact same roleplayers. If not then this is permissable.
            throw new CompletenessException(String.format(
                    "Completeness testing found %d answers for query:\n%s\nThis implies that more than one inferred " +
                            "fact was materialised by the rule %s.\nThe only valid cause is that there are two rules " +
                            "where one infers the same as another but with fewer roleplayers. This otherwise suggests" +
                            " an error in materialisation.", numAnswers, boundConjunction.toString(),
                    conclusion.rule().getLabel()));
        }
    }

    private int numReasonedAnswers(BoundConjunction boundConjunction, Set<Identifier.Variable.Retrievable> filter) {
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                       new Options.Transaction().infer(true))) {
            return tx.reasoner().executeReasoner(
                    new Disjunction(Collections.singletonList(boundConjunction.conjunction())),
                    filter, new Context.Query(tx.context(), new Options.Query())).toList().size();
        }
    }

}
